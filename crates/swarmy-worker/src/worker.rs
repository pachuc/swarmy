use std::{collections::HashMap, fmt::Write, sync::Arc};

use anyhow::{Context, Result, ensure};
use jiff::Timestamp;
use swarmy_bus::{Bus, LiveFeed, SubjectToken, WorkMessage, WorkQueue};
use swarmy_core::{
    AgentId, Event, InflightRecord, Lease, LeaseOwnerId, MessageId, Nudge, RequestId,
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

use crate::config::Config;

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

/// Chaos kill points crash the worker at fixed step boundaries. The config
/// carries the selected point as a string so chaos runs can set it without
/// rebuilding; the enum keeps every call site and the matching check in one
/// place instead of scattering string literals.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KillPoint {
    AfterClaim,
    BeforeRelease,
    AfterRequestEvent,
    AfterRelease,
    AfterAdvance,
}

impl KillPoint {
    const fn as_str(self) -> &'static str {
        match self {
            Self::AfterClaim => "after_claim",
            Self::BeforeRelease => "before_release",
            Self::AfterRequestEvent => "after_request_event",
            Self::AfterRelease => "after_release",
            Self::AfterAdvance => "after_advance",
        }
    }
}

/// Snapshots cached by content-addressed key alongside the claim that uses
/// them. Display flags cached by the agent and image pair that determines
/// them, with a per-session index so hits need no database reads. Caches
/// clear instead of evicting entries because a worker handles few distinct
/// keys and a full clear keeps the bound with no per-entry bookkeeping.
const SNAPSHOT_CACHE_SIZE: usize = 16;
const DISPLAY_CACHE_SIZE: usize = 64;

/// Display flag per (agent, image), indexed by session. A worker restart
/// drops this cache; restart workers after changing an agent's image.
#[derive(Default)]
struct DisplayCache {
    by_image: HashMap<(AgentId, String), bool>,
    by_session: HashMap<SessionId, (AgentId, String)>,
}

impl DisplayCache {
    fn get(&self, session: SessionId) -> Option<bool> {
        let key = self.by_session.get(&session)?;
        self.by_image.get(key).copied()
    }

    fn insert(&mut self, session: SessionId, agent: AgentId, image: String, display: bool) {
        if self.by_image.len() >= DISPLAY_CACHE_SIZE || self.by_session.len() >= DISPLAY_CACHE_SIZE
        {
            self.by_image.clear();
            self.by_session.clear();
        }
        self.by_image.insert((agent, image.clone()), display);
        self.by_session.insert(session, (agent, image));
    }
}

pub struct Worker {
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
    pub fn new(store: Store, bus: Bus, blobs: Arc<dyn BlobStore>, config: Config) -> Self {
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
        if self.config.kill_point.as_deref() == Some(point.as_str()) {
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

    pub async fn handle(&self, message: &WorkMessage<Nudge>) -> Result<()> {
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
            let event = Bus::turn_event(id, turn, swarmy_core::TurnStage::Claimed, None);
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
