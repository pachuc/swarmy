use jiff::Timestamp;
use swarmy_bus::Bus;
use swarmy_core::{SessionId, SessionState, WakeReply, WakeRequest};
use swarmy_store::{MAX_SCAN_LIMIT, Store, StoreError, runnable_partition};
use tokio::time::MissedTickBehavior;

use crate::config::Config;

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

    async fn nudge(&self, session_id: SessionId, force: bool) {
        let partition = runnable_partition(session_id);
        if !self.config.partitions.contains(&partition) {
            return;
        }
        let result = async {
            let session = self
                .store
                .fetch_session(session_id)
                .await?
                .ok_or(StoreError::SessionMissing)?;
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
            for &partition in &self.config.partitions {
                if let Err(error) = self.scan_partition(partition).await {
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
                        self.nudge(id, false).await;
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

    async fn scan_partition(&self, partition: u16) -> Result<(), StoreError> {
        let mut cursor = None;
        loop {
            let page = self
                .store
                .scan_runnable(partition, cursor.as_ref(), MAX_SCAN_LIMIT)
                .await?;
            for entry in &page {
                // Priority sorts before time, so a future entry cannot end the scan.
                if entry.wake_at <= Timestamp::now() {
                    self.nudge(entry.session_id, false).await;
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
                        self.nudge(*session_id, true).await;
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
                self.nudge(session_id, true).await;
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
