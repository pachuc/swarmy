use std::collections::HashMap;

use jiff::Timestamp;
use swarmy_bus::Bus;
use swarmy_core::{AgentRecord, Event, SessionId, SessionState, WakeReply, WakeRequest};
use swarmy_store::{
    MAX_SCAN_LIMIT, PoolEntry, RouteStepStatus, Store, StoreError, runnable_partition,
};
use tokio::time::MissedTickBehavior;

use crate::config::Config;

/// One cached breaker state: the open retry time, if the breaker is open,
/// with its stored reason.
type BreakerState = (Option<Timestamp>, Option<String>);

/// Per-tick caches for route resolution. A scan costs one transaction per
/// distinct route, provider pool, and breaker step instead of one snapshot
/// per session, so a swarm with hundreds of agents sharing routes runs a
/// handful of transactions per scan. Nothing outlives the tick, so route
/// edits apply on the next pass.
#[derive(Default)]
struct TickCache {
    routes: HashMap<String, Option<swarmy_core::RouteRecord>>,
    pools: HashMap<String, Vec<PoolEntry>>,
    breakers: HashMap<(String, Option<String>), BreakerState>,
}

pub struct Scheduler {
    store: Store,
    bus: Bus,
    config: Config,
}

impl Scheduler {
    pub fn new(store: Store, bus: Bus, config: Config) -> Self {
        Self { store, bus, config }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        tokio::select! {
            () = self.scan_loop() => {},
            () = self.reaper_loop() => {},
            () = self.timer_loop() => {},
            result = self.bus.serve_place_requests(|request| async move {
                match self.store.place_sandbox(request.session_id, Timestamp::now()).await {
                    Ok(record) => swarmy_core::PlaceReply::Placed(record),
                    Err(error) => swarmy_core::PlaceReply::Failed(error.to_string()),
                }
            }) => result?,
            result = self.bus.serve_wake_requests(|request| self.wake(request)) => result?,
        }
        Ok(())
    }

    /// Resolve the session's route behind its steps' breaker records. A
    /// usable step means the session can run; when every step is open the
    /// session waits for the earliest retry among them. The scan starts at
    /// the session's attempt position and wraps to a recovered earlier step,
    /// so the scheduler and the worker agree on which step serves the next
    /// attempt. Resolution reads the session's provider override directly,
    /// so two sessions on one agent with different providers never share a
    /// snapshot.
    async fn breaker_park(
        &self,
        session: &swarmy_core::SessionRecord,
        agent: Option<&AgentRecord>,
        cache: &mut TickCache,
    ) -> Result<Option<(Timestamp, String)>, StoreError> {
        let now = Timestamp::now();
        let agent_route = agent.and_then(|agent| agent.route.as_deref());
        let agent_provider = agent.and_then(|agent| agent.provider.as_deref());
        let name = session
            .route
            .as_deref()
            .or(agent_route)
            .or(self.config.default_route.as_deref());
        let provider = session
            .inference
            .provider
            .as_deref()
            .or(agent_provider)
            .unwrap_or(&self.config.provider);
        let record = match name {
            Some(name) => {
                if let Some(cached) = cache.routes.get(name) {
                    cached.clone()
                } else {
                    let record = self.store.get_route(name).await?;
                    cache.routes.insert(name.to_owned(), record.clone());
                    record
                }
            }
            None => None,
        };
        let mut providers: Vec<String> = record
            .as_ref()
            .map(|record| {
                record
                    .steps
                    .iter()
                    .map(|step| step.provider.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        providers.push(provider.to_owned());
        providers.sort();
        providers.dedup();
        let missing: Vec<String> = providers
            .into_iter()
            .filter(|provider| !cache.pools.contains_key(provider))
            .collect();
        if !missing.is_empty() {
            for (provider, pool) in self.store.route_pools(&missing, now).await? {
                cache.pools.insert(provider, pool);
            }
        }
        // The same pure expansion the worker's transaction uses, so both
        // agree on the chain including skipped missing or unready entries.
        let chain = Store::expand_chain(record.as_ref(), &cache.pools, provider);
        let missing: Vec<(String, Option<String>)> = chain
            .steps
            .iter()
            .map(|step| (step.provider.clone(), step.label.clone()))
            .filter(|key| !cache.breakers.contains_key(key))
            .collect();
        if !missing.is_empty() {
            for (key, state) in missing
                .iter()
                .zip(self.store.breaker_states(&missing, now).await?)
            {
                cache.breakers.insert(key.clone(), state);
            }
        }
        let mut steps = Vec::with_capacity(chain.steps.len());
        for step in chain.steps {
            let (open_until, reason) = cache
                .breakers
                .get(&(step.provider.clone(), step.label.clone()))
                .cloned()
                .unwrap_or((None, None));
            steps.push(RouteStepStatus {
                provider: step.provider,
                label: step.label,
                model: step.model,
                reason,
                open_until,
            });
        }
        let snapshot = swarmy_store::RouteSnapshot {
            name: chain.name,
            steps,
            skipped: chain.skipped,
        };
        if snapshot.pick(session.route_step).is_some() {
            return Ok(None);
        }
        Ok(snapshot.earliest())
    }

    async fn nudge(&self, session_id: SessionId, force: bool, breakers: &mut TickCache) {
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
                .ok_or(StoreError::SessionMissing)?;
            if session.state == SessionState::Runnable {
                if session.interrupt_requested {
                    if self.store.finish_runnable_interrupt(session_id).await? {
                        let head = self.store.fetch_session(session_id).await?
                            .ok_or(StoreError::SessionMissing)?.head_seq;
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
            let mut breakers = TickCache::default();
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
                self.nudge(id, false, &mut TickCache::default()).await;
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
                        self.nudge(id, false, &mut TickCache::default()).await;
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
        breakers: &mut TickCache,
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
                        self.nudge(*session_id, true, &mut TickCache::default())
                            .await;
                    }
                    // Another reaper or a renewal can win after the scan.
                    Err(StoreError::LeaseMismatch) => {}
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
                self.nudge(session_id, true, &mut TickCache::default())
                    .await;
                WakeReply::Runnable
            }
            Ok(SessionState::Runnable) => WakeReply::Runnable,
            Ok(state) => {
                tracing::debug!(%session_id, ?state, "wake left session unchanged");
                WakeReply::Unchanged(state)
            }
            Err(StoreError::SessionMissing) => WakeReply::NotFound,
            Err(error) => {
                tracing::warn!(%session_id, %error, "wake failed");
                WakeReply::Failed(error.to_string())
            }
        }
    }
}
