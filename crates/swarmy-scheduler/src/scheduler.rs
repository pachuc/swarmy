use std::{collections::HashMap, time::Instant};

use jiff::Timestamp;
use swarmy_bus::{Bus, WorkQueue};
use swarmy_core::{Nudge, SessionId, SessionState, WakeReply, WakeRequest};
use swarmy_store::{MAX_SCAN_LIMIT, Store, StoreError, runnable_partition};
use tokio::{sync::Mutex, time::MissedTickBehavior};

use crate::config::Config;

pub struct Scheduler {
    store: Store,
    bus: Bus,
    config: Config,
    recent: Mutex<HashMap<SessionId, Instant>>,
}

impl Scheduler {
    pub fn new(store: Store, bus: Bus, config: Config) -> Self {
        Self {
            store,
            bus,
            config,
            recent: Mutex::new(HashMap::new()),
        }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        tokio::select! {
            () = self.scan_loop() => {},
            () = self.reaper_loop() => {},
            result = self.bus.serve_wake_requests(|request| self.wake(request)) => result?,
        }
        Ok(())
    }

    async fn nudge(&self, session_id: SessionId, force: bool) {
        let partition = runnable_partition(session_id);
        if !self.config.partitions.contains(&partition) {
            return;
        }
        // Serialize publications so a wake and a scan share the resend deadline.
        // Record only acknowledged publications; a failed send stays retryable.
        let mut recent = self.recent.lock().await;
        if !force
            && recent
                .get(&session_id)
                .is_some_and(|sent| sent.elapsed() < self.config.resend_interval)
        {
            return;
        }
        match self
            .bus
            .publish_work(&WorkQueue::Runnable(partition), &Nudge { session_id })
            .await
        {
            Ok(()) => {
                recent.insert(session_id, Instant::now());
                tracing::info!(%session_id, partition, "nudged runnable session");
            }
            Err(error) => tracing::warn!(%session_id, partition, %error, "nudge failed"),
        }
    }

    async fn scan_loop(&self) {
        let mut interval = tokio::time::interval(self.config.scan_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            self.recent
                .lock()
                .await
                .retain(|_, sent| sent.elapsed() < self.config.resend_interval);
            for &partition in &self.config.partitions {
                if let Err(error) = self.scan_partition(partition).await {
                    tracing::warn!(partition, %error, "runnable scan failed; will retry");
                }
            }
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
