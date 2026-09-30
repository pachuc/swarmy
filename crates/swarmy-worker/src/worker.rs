use std::{collections::HashMap, fmt::Write, sync::Arc};

use anyhow::{Context, Result, ensure};
use jiff::Timestamp;
use swarmy_bus::{Bus, LiveFeed, SubjectToken, WorkMessage, WorkQueue};
use swarmy_core::{
    Event, InflightRecord, Lease, LeaseOwnerId, ManifestId, MessageId, Nudge, RequestId,
    SandboxArguments, SessionId, SessionRecord, SessionState, SnapshotRef, ToolCallRecord, ToolJob,
    TurnStage, decode, encode,
};
use swarmy_harness::{Action, Snapshot, execution_result};
use swarmy_llm::{InferenceJob, InferenceJobRef};
use swarmy_store::{
    FailoverAction, MAX_SCAN_LIMIT, Store, StoreError, SubmitInferenceOptions, blob::BlobStore,
    runnable_partition,
};
use tokio::{
    sync::Mutex,
    time::{Instant, MissedTickBehavior, interval, interval_at},
};
use ulid::Ulid;

use crate::config::{Config, KillPoint};

/// A step lease shared with its heartbeat. Release is explicit only after a
/// successful fenced store transition.
struct HeldLease(Mutex<Option<Lease>>);

impl HeldLease {
    fn new(lease: Lease) -> Self {
        Self(Mutex::new(Some(lease)))
    }

    async fn lock(&self) -> HeldLeaseGuard<'_> {
        HeldLeaseGuard(self.0.lock().await)
    }

    async fn is_held(&self) -> bool {
        self.0.lock().await.is_some()
    }

    async fn release(&self) {
        self.0.lock().await.take();
    }

    async fn renew(
        &self,
        store: &Store,
        id: SessionId,
        now: Timestamp,
        duration: std::time::Duration,
    ) -> Result<()> {
        let mut token = self.0.lock().await;
        if let Some(current) = token.as_ref() {
            *token = Some(
                store
                    .renew_lease(id, current, now, now.checked_add(duration)?)
                    .await?,
            );
        }
        Ok(())
    }
}

struct HeldLeaseGuard<'a>(tokio::sync::MutexGuard<'a, Option<Lease>>);
impl HeldLeaseGuard<'_> {
    fn as_ref(&self) -> Option<&Lease> {
        self.0.as_ref()
    }

    fn release(&mut self) {
        self.0.take();
    }
}

/// One route-selection group for every snapshot and failover call: the
/// session override, the swarm default, and the clock.
fn route_selection<'a>(
    session: &'a SessionRecord,
    config: &'a Config,
    now: Timestamp,
) -> swarmy_store::RouteSelection<'a> {
    swarmy_store::RouteSelection {
        session_route: session.route.as_deref(),
        session_provider: session.inference.provider.as_deref(),
        default_route: config.default_route.as_deref(),
        default_provider: &config.provider,
        now,
    }
}

/// Snapshots cached by content-addressed key alongside the claim that uses
/// them. Display flags cached by the image manifest that determines them,
/// with a per-session index so hits need no database reads. Caches
/// clear instead of evicting entries because a worker handles few distinct
/// keys and a full clear keeps the bound with no per-entry bookkeeping.
const SNAPSHOT_CACHE_SIZE: usize = 16;
const DISPLAY_CACHE_SIZE: usize = 64;

/// Display flag per image manifest, indexed by session. A worker restart
/// drops this cache; restart workers after changing an agent's image.
#[derive(Default)]
struct DisplayCache {
    by_image: HashMap<ManifestId, bool>,
    by_session: HashMap<SessionId, bool>,
}

impl DisplayCache {
    fn get(&self, session: SessionId) -> Option<bool> {
        self.by_session.get(&session).copied()
    }

    fn get_image(&self, manifest: ManifestId) -> Option<bool> {
        self.by_image.get(&manifest).copied()
    }

    fn insert(&mut self, session: SessionId, manifest: Option<ManifestId>, display: bool) {
        if self.by_image.len() >= DISPLAY_CACHE_SIZE || self.by_session.len() >= DISPLAY_CACHE_SIZE
        {
            self.by_image.clear();
            self.by_session.clear();
        }
        if let Some(manifest) = manifest {
            self.by_image.insert(manifest, display);
        }
        self.by_session.insert(session, display);
    }
}

pub(crate) struct Worker {
    store: Store,
    bus: Bus,
    blobs: Arc<dyn BlobStore>,
    config: Config,
    placements: crate::placement::Cache,
    snapshots: Mutex<HashMap<String, Snapshot>>,
    display: Mutex<DisplayCache>,
    pub owner: LeaseOwnerId,
    clock: Arc<dyn Fn() -> Timestamp + Send + Sync>,
}

impl Worker {
    pub(crate) fn new(store: Store, bus: Bus, blobs: Arc<dyn BlobStore>, config: Config) -> Self {
        Self {
            store,
            bus,
            blobs,
            config,
            placements: crate::placement::Cache::default(),
            snapshots: Mutex::default(),
            display: Mutex::default(),
            owner: LeaseOwnerId::from_ulid(Ulid::generate()),
            clock: Arc::new(Timestamp::now),
        }
    }

    /// Observe the worker's clock. Recovery and placement resolution read
    /// this instead of the wall clock so tests advance one shared clock past
    /// lease expiry instead of sleeping out real leases; production keeps the
    /// default wall clock.
    #[cfg(test)]
    pub(crate) fn with_clock(mut self, clock: impl Fn() -> Timestamp + Send + Sync + 'static) -> Self {
        self.clock = Arc::new(clock);
        self
    }

    fn now(&self) -> Timestamp {
        (self.clock)()
    }

    fn kill(&self, point: KillPoint) {
        // Production leaves kill_point unset; the chaos harness opts in at runtime.
        if self.config.kill_point == Some(point) {
            tracing::warn!(point = point.as_str(), "instrumented worker kill");
            if let Err(error) = rustix::process::kill_process(
                rustix::process::getpid(),
                rustix::process::Signal::KILL,
            ) {
                tracing::warn!(%error, "chaos kill signal failed; aborting instead");
            }
            std::process::abort();
        }
    }

    pub(crate) async fn handle(&self, message: &WorkMessage<Nudge>) -> Result<()> {
        let id = message.value.session_id;
        let (lease, session, turn, events) = match self
            .store
            .claim_step_with_tail(
                id,
                self.owner,
                self.now().checked_add(self.config.lease_duration)?,
            )
            .await
        {
            Ok(lease) => lease,
            Err(StoreError::Domain(
                swarmy_store::DomainError::UnexpectedSessionState
                | swarmy_store::DomainError::InterruptPending,
            )) => {
                return Ok(message.acknowledge().await?);
            }
            Err(error) => return Err(error.into()),
        };
        tracing::info!(session_id = %id, owner = %lease.owner, step = lease.seq, "claimed step");
        if let Some(turn) = turn {
            let event = Bus::turn_event(id, turn, TurnStage::Claimed, None);
            self.bus.record_turn(&event).await;
            self.store.observe_turn_stage(event);
        }
        self.kill(KillPoint::AfterClaim);
        let mut ctx = step::StepContext {
            session: session.clone(),
            lease: Arc::new(HeldLease::new(lease)),
            snapshot: None,
            events,
            turn,
        };
        let heartbeat_lease = ctx.lease.clone();
        tokio::select! {
            result = self.step(&mut ctx) => {
                if result.is_err() { self.placements.invalidate(session.agent_id).await; }
                result?;
            },
            result = self.heartbeat(id, &heartbeat_lease, message) => result?,
        }
        message.acknowledge().await?;
        Ok(())
    }

    async fn heartbeat(
        &self,
        id: SessionId,
        lease: &HeldLease,
        message: &WorkMessage<Nudge>,
    ) -> Result<()> {
        let period = (self.config.lease_duration / 3).min(self.config.bus.ack_wait / 3);
        let mut ticks = interval_at(Instant::now() + period, period);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticks.tick().await;
            message.extend_deadline().await?;
            if lease.is_held().await {
                lease
                    .renew(&self.store, id, self.now(), self.config.lease_duration)
                    .await?;
            }
        }
    }
}

mod inference;
mod inflight;
mod overflow;
mod step;
mod summarize;
#[cfg(test)]
mod tests;
mod tools;
