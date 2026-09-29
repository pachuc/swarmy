use jiff::Timestamp;
use swarmy_bus::Bus;
use swarmy_core::{AgentRecord, Event, SessionId, SessionState, WakeReply, WakeRequest};
use swarmy_store::{MAX_SCAN_LIMIT, RouteCache, Store, StoreError, runnable_partition};
use tokio::time::MissedTickBehavior;

use crate::config::Config;

pub(crate) struct Scheduler {
    store: Store,
    bus: Bus,
    config: Config,
}

impl Scheduler {
    pub(crate) fn new(store: Store, bus: Bus, config: Config) -> Self {
        Self { store, bus, config }
    }

    pub(crate) async fn run(&self) -> anyhow::Result<()> {
        tokio::select! {
            () = self.scan_loop() => {},
            () = self.reaper_loop() => {},
            () = self.timer_loop() => {},
            result = self.bus.serve_wake_requests(|request| self.wake(request)) => result?,
        }
        Ok(())
    }

    /// Resolve the session route against its breaker records. A usable step
    /// lets the session run; if every step is open, wait for the earliest retry.
    /// Picking starts at the session attempt position and wraps to a recovered
    /// earlier step, so the scheduler and worker agree on the next attempt.
    /// Resolve the session provider override directly so sessions on the same
    /// agent with different overrides do not share a snapshot.
    async fn breaker_park(
        &self,
        session: &swarmy_core::SessionRecord,
        agent: Option<&AgentRecord>,
        cache: &mut RouteCache,
    ) -> Result<Option<(Timestamp, String)>, StoreError> {
        let snapshot = self
            .store
            .route_pick_with_cache(
                session,
                agent,
                self.config.default_route.as_deref(),
                &self.config.provider,
                Timestamp::now(),
                cache,
            )
            .await?;
        if snapshot.pick(session.route_step).is_some() {
            Ok(None)
        } else {
            Ok(snapshot.earliest())
        }
    }

    async fn nudge(&self, session_id: SessionId, force: bool, breakers: &mut RouteCache) {
        let partition = runnable_partition(session_id);
        if !self.config.partitions.contains(&partition) {
            return;
        }
        let result = async {
            // The agent arrives in the same transaction so route assignment
            // costs no extra transaction per session.
            let (session, agent) = self
                .store
                .fetch_session_with_agent(session_id)
                .await?
                .ok_or(StoreError::Domain(swarmy_store::DomainError::SessionMissing))?;
            if session.state == SessionState::Runnable {
                if session.interrupt_requested {
                    if self.store.finish_runnable_interrupt(session_id).await? {
                        let head = self.store.fetch_session(session_id).await?
                            .ok_or(StoreError::Domain(swarmy_store::DomainError::SessionMissing))?.head_seq;
                        if let Some(event) = self.store.read_events(session_id, head - 1, 1).await?.pop() {
                            self.bus.publish_live(swarmy_bus::LiveFeed::SessionEvents(session_id), &event).await?;
                        }
                    }
                    return Ok(());
                }
                if let Some((until, reason)) =
                    self.breaker_park(&session, agent.as_ref(), breakers).await?
                {
                    let wait = self.store.inference_wait(session_id).await?;
                    let failure_pending = if session.head_seq == 0 {
                        false
                    } else {
                        self.store
                            .read_events(session_id, session.head_seq - 1, 1)
                            .await?
                            .first()
                            .is_some_and(|event| {
                                matches!(event, Event::InferenceFailed { retryable: true, seq, .. }
                                    if wait.as_ref().is_none_or(|wait| wait.last_failure_seq != *seq))
                            })
                    };
                    let wait_expired = wait.as_ref().is_some_and(|wait| {
                        wait.since
                            .checked_add(self.config.max_inference_wait)
                            .is_ok_and(|limit| limit <= Timestamp::now())
                    });
                    if !failure_pending
                        && !wait_expired
                        && self
                            .store
                            .park_runnable_for_breaker(
                                session_id,
                                &reason,
                                until,
                                Timestamp::now(),
                                self.config.max_inference_wait,
                            )
                            .await?
                    {
                        return Ok(());
                    }
                }
            }
            let turn = self.store.turn_id(session_id).await?;
            self.bus
                .nudge(
                    session_id,
                    session.head_seq,
                    turn,
                    self.config.resend_interval,
                    force,
                )
                .await?;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(error) = result {
            tracing::warn!(%session_id, partition, %error, "nudge failed");
        }
    }

    async fn scan_loop(&self) {
        let mut interval = tokio::time::interval(self.config.scan_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let mut breakers = RouteCache::default();
            for &partition in &self.config.partitions {
                if let Err(error) = self.scan_partition(partition, &mut breakers).await {
                    tracing::warn!(partition, %error, "runnable scan failed; will retry");
                }
            }
        }
    }

    async fn timer_loop(&self) {
        let mut interval = tokio::time::interval(self.config.scan_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if let Err(error) = self.scan_timers().await {
                tracing::warn!(%error, "timer scan failed; will retry");
            }
        }
    }

    async fn scan_timers(&self) -> Result<(), StoreError> {
        let now = Timestamp::now();
        for id in self.store.scan_due_inference_waits(now).await? {
            if self.store.wake_inference_wait(id, now).await? {
                self.nudge(id, false, &mut RouteCache::default()).await;
            }
        }
        let mut cursor = None;
        loop {
            let page = self.store.scan_due_timers(now, cursor.as_ref()).await?;
            for timer in &page {
                match self
                    .store
                    .fire_timer(timer.agent_id, timer.timer_id, now)
                    .await
                {
                    Ok(Some((id, event))) => {
                        if let Err(error) = self
                            .bus
                            .publish_live(swarmy_bus::LiveFeed::SessionEvents(id), &event)
                            .await
                        {
                            tracing::warn!(%error, "timer event notification failed");
                        }
                        self.nudge(id, false, &mut RouteCache::default()).await;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        tracing::warn!(timer_id = %timer.timer_id, %error, "timer append failed; will retry");
                    }
                }
            }
            if page.len() < MAX_SCAN_LIMIT {
                return Ok(());
            }
            cursor = page.last().cloned();
        }
    }

    async fn scan_partition(
        &self,
        partition: u16,
        breakers: &mut RouteCache,
    ) -> Result<(), StoreError> {
        let mut cursor = None;
        loop {
            let page = self
                .store
                .scan_runnable(partition, cursor.as_ref(), MAX_SCAN_LIMIT)
                .await?;
            for entry in &page {
                // Priority sorts before time, so a future entry cannot end the scan.
                if entry.wake_at <= Timestamp::now() {
                    self.nudge(entry.session_id, false, breakers).await;
                }
            }
            if page.len() < MAX_SCAN_LIMIT {
                return Ok(());
            }
            cursor = page.last().cloned();
        }
    }

    async fn reaper_loop(&self) {
        let mut interval = tokio::time::interval(self.config.scan_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if let Err(error) = self.reap().await {
                tracing::warn!(%error, "lease scan failed; will retry");
            }
        }
    }

    async fn reap(&self) -> Result<(), StoreError> {
        let now = Timestamp::now();
        let mut cursor = None;
        loop {
            let page = self
                .store
                .scan_expired_leases(now, cursor.as_ref(), MAX_SCAN_LIMIT)
                .await?;
            for (session_id, lease) in &page {
                if !self
                    .config
                    .partitions
                    .contains(&runnable_partition(*session_id))
                {
                    continue;
                }
                match self.store.reap_lease(*session_id, lease, now).await {
                    Ok(()) => {
                        tracing::info!(%session_id, owner = %lease.owner, "reaped expired lease");
                        self.nudge(*session_id, true, &mut RouteCache::default())
                            .await;
                    }
                    // Another reaper or a renewal can win after the scan.
                    Err(StoreError::Fence(swarmy_store::FenceError::LeaseMismatch)) => {}
                    Err(error) => tracing::warn!(%session_id, %error, "lease reaping failed"),
                }
            }
            if page.len() < MAX_SCAN_LIMIT {
                return Ok(());
            }
            cursor = page.last().cloned();
        }
    }

    async fn wake(&self, WakeRequest { session_id }: WakeRequest) -> WakeReply {
        match self.store.wake_session(session_id, Timestamp::now()).await {
            Ok(SessionState::Idle) => {
                tracing::info!(%session_id, "woke idle session");
                // If another instance owns this partition, its scan will nudge it.
                self.nudge(session_id, true, &mut RouteCache::default())
                    .await;
                WakeReply::Runnable
            }
            Ok(SessionState::Runnable) => WakeReply::Runnable,
            Ok(state) => {
                tracing::debug!(%session_id, ?state, "wake left session unchanged");
                WakeReply::Unchanged(state)
            }
            Err(StoreError::Domain(swarmy_store::DomainError::SessionMissing)) => {
                WakeReply::NotFound
            }
            Err(error) => {
                tracing::warn!(%session_id, %error, "wake failed");
                WakeReply::Failed(error.to_string())
            }
        }
    }
}
