use std::{collections::HashMap, fmt::Write, sync::Arc};

use anyhow::{Context, Result, ensure};
use jiff::Timestamp;
use swarmy_bus::{Bus, LiveFeed, SubjectToken, WorkMessage, WorkQueue};
use swarmy_core::{
    Event, InflightRecord, Lease, LeaseOwnerId, MessageId, Nudge, RequestId, SandboxArguments,
    SessionId, SessionRecord, SessionState, SnapshotRef, ToolCallRecord, ToolJob, TurnStage,
    decode, encode,
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
impl std::ops::Deref for HeldLeaseGuard<'_> {
    type Target = Option<Lease>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for HeldLeaseGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl HeldLeaseGuard<'_> {
    fn release(&mut self) {
        self.0.take();
    }
}

type ActiveLease = HeldLease;

/// A resolved route step and its metering identity.
struct ResolvedAttempt {
    provider: String,
    entry: Option<String>,
    route: Option<String>,
    route_step: u32,
    snapshot: swarmy_store::RouteSnapshot,
}

impl Worker {
    async fn route_snapshot(&self, session: &SessionRecord) -> Result<swarmy_store::RouteSnapshot> {
        Ok(self
            .store
            .route_snapshot(
                session.agent_id,
                session.route.as_deref(),
                session.inference.provider.as_deref(),
                self.config.default_route.as_deref(),
                &self.config.provider,
                Timestamp::now(),
            )
            .await?)
    }
}

fn warn_on_route_fallback(session: &SessionRecord, resolved: Option<&str>, skipped: &[String]) {
    // A deleted or renamed route falls back to the implicit chain; say so
    // once per resolution so the operator can fix the assignment. A route
    // whose named steps are all unready falls back the same way, but the
    // route itself exists, so name the skipped steps instead.
    let requested = session.route.as_deref();
    if requested.is_some() && resolved != requested {
        if skipped.is_empty() {
            tracing::warn!(
                session_id = %session.session_id,
                route = requested,
                "assigned route is missing; using the implicit provider chain",
            );
        } else {
            tracing::warn!(
                session_id = %session.session_id,
                route = requested,
                skipped = skipped.join("; "),
                "assigned route has no usable step; using the implicit provider chain",
            );
        }
    }
}

pub struct Worker {
    store: Store,
    bus: Bus,
    blobs: Arc<dyn BlobStore>,
    config: Config,
    placements: crate::placement::Cache,
    snapshots: Mutex<HashMap<String, Snapshot>>,
    display_by_session: Mutex<HashMap<SessionId, bool>>,
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
            display_by_session: Mutex::default(),
            owner: LeaseOwnerId::from_ulid(Ulid::generate()),
        }
    }

    fn kill(&self, point: &str) {
        if self.config.kill_point.as_deref() == Some(point) {
            tracing::warn!(point, "instrumented worker exit");
            std::process::exit(137);
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
            Err(StoreError::InvalidState) => {
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
        self.kill("after_claim");
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
        lease: &ActiveLease,
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
mod recovery;
mod step;
mod summarize;
#[cfg(test)]
mod tests;
mod tools;
