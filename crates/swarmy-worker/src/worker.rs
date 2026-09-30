use std::{collections::HashMap, fmt::Write, sync::Arc};

use anyhow::{Context, Result, ensure};
use jiff::Timestamp;
use swarmy_bus::{Bus, LiveFeed, SubjectToken, WorkMessage, WorkQueue};
use swarmy_core::{
    Event, ImageRecord, ImageTag, InflightRecord, Lease, LeaseOwnerId, ManifestId, MessageId,
    Nudge, RequestId, SandboxArguments, SessionId, SessionRecord, SessionState, SnapshotRef,
    ToolCallRecord, ToolJob, TurnStage, decode, encode,
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
        duration: std::time::Duration,
    ) -> Result<()> {
        let mut token = self.0.lock().await;
        if let Some(current) = token.as_ref() {
            let now = Timestamp::now();
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

/// Display flag per image, indexed by session. The store reads the flag by
/// the full image identity (name, tag, manifest), so the cache keys the
/// same way: two tags pointing at one manifest can still differ. A worker
/// restart drops this cache; restart workers after changing an agent's
/// image.
#[derive(Clone, PartialEq, Eq, Hash)]
struct ImageKey {
    name: String,
    tag: ImageTag,
    manifest: ManifestId,
}

impl From<&ImageRecord> for ImageKey {
    fn from(image: &ImageRecord) -> Self {
        Self {
            name: image.name.clone(),
            tag: image.tag.clone(),
            manifest: image.manifest_id,
        }
    }
}

#[derive(Default)]
struct DisplayCache {
    by_image: HashMap<ImageKey, bool>,
    by_session: HashMap<SessionId, bool>,
}

impl DisplayCache {
    fn get(&self, session: SessionId) -> Option<bool> {
        self.by_session.get(&session).copied()
    }

    fn get_image(&self, image: &ImageKey) -> Option<bool> {
        self.by_image.get(image).copied()
    }

    fn insert(&mut self, session: SessionId, image: Option<ImageKey>, display: bool) {
        if self.by_image.len() >= DISPLAY_CACHE_SIZE || self.by_session.len() >= DISPLAY_CACHE_SIZE
        {
            self.by_image.clear();
            self.by_session.clear();
        }
        if let Some(image) = image {
            self.by_image.insert(image, display);
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
        }
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
                Timestamp::now().checked_add(self.config.lease_duration)?,
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
                    .renew(&self.store, id, self.config.lease_duration)
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
