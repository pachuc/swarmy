//! Gateway dispatch: serving loop, provider advertisement, and delivery handling.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use crate::{Error, Result};
use futures::StreamExt;
use jiff::Timestamp;
use swarmy_bus::{Bus, WorkMessage, WorkQueue};
use swarmy_core::{Event, IdempotencyState, LeaseOwnerId, MessageId, MessageRole, RequestId};
use swarmy_llm::{InferenceJob, InferenceJobRef};
use swarmy_store::{
    GatewayProvider, InferenceClaim, ServiceDetail, ServiceHeartbeat, ServiceRole, Store,
    blob::BlobStore,
};
use tokio::{
    sync::Semaphore,
    task::JoinSet,
    time::{Instant, interval_at, sleep},
};
use tracing::{error, info, warn};
use ulid::Ulid;

use crate::{
    attempt::Delivery,
    config,
    providers::{ProviderChanges, Providers},
};

/// The gateway engine: claims inference work, streams provider responses,
/// and commits terminal events. Constructed once in `main` and served until
/// shutdown; every stage below is a method here or on the attempt and commit
/// modules.
pub struct Gateway {
    pub(crate) store: Store,
    pub(crate) blobs: Arc<dyn BlobStore>,
    pub(crate) bus: Bus,
    pub(crate) providers: Providers,
    pub(crate) default_provider: String,
    pub(crate) summarize_at_tokens: Option<u64>,
    pub(crate) model_context_window_tokens: Option<u64>,
    pub(crate) ack_wait: Duration,
    pub(crate) max_deliver: i64,
    pub(crate) resend_interval: Duration,
    pub(crate) max_backoff: Duration,
    pub(crate) health_id: String,
    pub(crate) started_at: Timestamp,
}

impl Gateway {
    /// Assemble the engine from its connections and the service config.
    pub fn new(
        store: Store,
        blobs: Arc<dyn BlobStore>,
        bus: Bus,
        providers: Providers,
        config: &config::Config,
    ) -> Self {
        Self {
            store,
            blobs,
            bus,
            providers,
            default_provider: config.settings.selection.provider.clone(),
            summarize_at_tokens: config
                .settings
                .context
                .summarize_at
                .map(std::num::NonZeroU64::get),
            model_context_window_tokens: config
                .settings
                .context
                .context_window
                .map(std::num::NonZeroU64::get),
            ack_wait: config.bus.ack_wait,
            max_deliver: config.bus.max_deliver,
            resend_interval: config.resend_interval,
            max_backoff: config.settings.inference.max_backoff_secs,
            health_id: Ulid::generate().to_string(),
            started_at: Timestamp::now(),
        }
    }

    /// Serve provider work until the work stream ends or a shutdown signal
    /// arrives, then drain queued turn metrics before exit so shutdown keeps
    /// every write. The flush runs on every exit path, including startup,
    /// delivery, and semaphore failures, so a failing serve still keeps the
    /// metrics it queued.
    /// # Errors
    /// Returns store, transport, or decoding failures; per-delivery inference
    /// failures are committed to the session log instead.
    pub async fn serve(self: Arc<Self>, concurrency: usize) -> Result<()> {
        let outcome = self.serve_inner(concurrency).await;
        // Drain queued turn metrics before exit so shutdown keeps every write.
        if let Err(error) = self.store.flush_turn_metrics().await {
            warn!(error = %swarmy_core::error_chain(&error), "gateway metric flush failed");
        }
        outcome
    }

    async fn serve_inner(self: &Arc<Self>, concurrency: usize) -> Result<()> {
        let mut messages = futures::stream::SelectAll::new();
        let mut subscriptions = BTreeSet::new();
        let mut worker = DispatchWorker::new(concurrency);
        let mut ticks = tokio::time::interval(swarmy_config::SERVICE_HEALTH_INTERVAL);
        refresh(self, &mut messages, &mut subscriptions).await?;
        ticks.tick().await;
        info!(concurrency, "gateway ready");
        loop {
            worker.reap()?;
            let delivery = tokio::select! {
                _ = ticks.tick() => {
                    if let Err(error) = refresh(self, &mut messages, &mut subscriptions).await {
                        warn!(error = %swarmy_core::error_chain(&error), "provider refresh failed; retaining the previous advertisement");
                    }
                    continue;
                }
                delivery = messages.next(), if !subscriptions.is_empty() => delivery,
                // Break out to flush queued turn metrics in `serve`.
                () = swarmy_config::shutdown_signal() => break,
            };
            let Some(delivery) = delivery else {
                return Err(Error::Internal("work stream ended"));
            };
            worker.dispatch(self, delivery).await?;
        }
        Ok(())
    }
}

/// In-flight delivery tasks and their concurrency bound. Draining finished
/// tasks and decoding the next delivery are separate steps so the serving
/// loop reads as the lifecycle it drives.
struct DispatchWorker {
    semaphore: Arc<Semaphore>,
    tasks: JoinSet<()>,
}

impl DispatchWorker {
    fn new(concurrency: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(concurrency)),
            tasks: JoinSet::new(),
        }
    }

    fn reap(&mut self) -> Result<()> {
        while let Some(result) = self.tasks.try_join_next() {
            result?;
        }
        Ok(())
    }

    async fn dispatch(
        &mut self,
        gateway: &Arc<Gateway>,
        delivery: std::result::Result<WorkMessage<InferenceJobRef>, swarmy_bus::Error>,
    ) -> Result<()> {
        let message = match delivery {
            Ok(message) => message,
            Err(error) => {
                warn!(error = %swarmy_core::error_chain(&error), "cannot decode work delivery");
                return Ok(());
            }
        };
        let permit = self.semaphore.clone().acquire_owned().await?;
        let gateway = Arc::clone(gateway);
        self.tasks.spawn(async move {
            let _permit = permit;
            if let Err(error) = gateway.handle(&message).await {
                error!(error = %swarmy_core::error_chain(&error), "delivery left unacknowledged");
            }
        });
        Ok(())
    }
}

const ADVERTISEMENT_TTL: Duration = Duration::from_secs(90);

async fn advertise(store: &Store, served: &[String]) -> Result<()> {
    let record = GatewayProvider {
        expires_at: Timestamp::now().checked_add(ADVERTISEMENT_TTL)?,
        reason: "credentials resolved".into(),
    };
    let entries = match swarmy_config::Keyring::load() {
        Ok(keyring) => {
            store
                .credentials(keyring)
                .list_entries(swarmy_core::CredentialScope::Cluster)
                .await?
        }
        Err(error) => {
            warn!(error = %swarmy_core::error_chain(&error), "keyring unavailable; advertising no credential entries");
            Vec::new()
        }
    };
    for provider in served {
        store.put_gateway_provider(provider, &record).await?;
        for entry in entries.iter().filter(|entry| {
            &entry.provider == provider && entry.status == swarmy_core::CredentialStatus::Ready
        }) {
            store
                .put_gateway_entry(provider, &entry.label, &record)
                .await?;
        }
    }
    Ok(())
}

async fn refresh(
    gateway: &Gateway,
    messages: &mut futures::stream::SelectAll<
        futures::stream::BoxStream<
            'static,
            std::result::Result<WorkMessage<InferenceJobRef>, swarmy_bus::Error>,
        >,
    >,
    subscriptions: &mut BTreeSet<String>,
) -> Result<()> {
    let changes = gateway.providers.refresh().await?;
    subscribe_new(gateway, messages, subscriptions, &changes.served).await?;
    log_changes(&changes);
    record_unserved(gateway, &changes).await?;
    advertise_health(gateway, &changes).await?;
    Ok(())
}

/// Subscribe to each newly served provider queue. Queues that vanished keep
/// their subscription: redelivery, not resubscription, recovers their work.
async fn subscribe_new(
    gateway: &Gateway,
    messages: &mut futures::stream::SelectAll<
        futures::stream::BoxStream<
            'static,
            std::result::Result<WorkMessage<InferenceJobRef>, swarmy_bus::Error>,
        >,
    >,
    subscriptions: &mut BTreeSet<String>,
    served: &[String],
) -> Result<()> {
    for id in served {
        if subscriptions.contains(id) {
            continue;
        }
        let queue = WorkQueue::Inference(swarmy_bus::SubjectToken::new(id)?);
        gateway.bus.setup(std::slice::from_ref(&queue)).await?;
        let mut stream = gateway.bus.consume::<InferenceJobRef>(&queue).await?;
        messages.push(Box::pin(async_stream::stream! {
            while let Some(message) = stream.next().await { yield message; }
        }));
        subscriptions.insert(id.clone());
    }
    Ok(())
}

fn log_changes(changes: &ProviderChanges) {
    for id in &changes.added {
        info!(provider = %id, "serving provider");
    }
    for id in &changes.removed {
        info!(provider = %id, "provider no longer served");
    }
    for id in &changes.rotated {
        info!(provider = %id, "provider credential changed");
    }
}

/// Record why each unserved provider stays out, so `doctor` can report it.
/// Skipped providers that never served get an immediate row; removed ones
/// fall back to "provider unavailable" when no skip reason exists.
async fn record_unserved(gateway: &Gateway, changes: &ProviderChanges) -> Result<()> {
    for (provider, reason) in &changes.skipped {
        if !gateway.store.gateway_serves(provider).await? {
            gateway
                .store
                .put_gateway_provider(
                    provider,
                    &GatewayProvider {
                        expires_at: Timestamp::now(),
                        reason: reason.clone(),
                    },
                )
                .await?;
        }
    }
    for id in &changes.removed {
        let reason = changes
            .skipped
            .get(id)
            .cloned()
            .unwrap_or_else(|| "provider unavailable".into());
        gateway
            .store
            .put_gateway_provider(
                id,
                &GatewayProvider {
                    expires_at: Timestamp::now(),
                    reason,
                },
            )
            .await?;
    }
    Ok(())
}

async fn advertise_health(gateway: &Gateway, changes: &ProviderChanges) -> Result<()> {
    advertise(&gateway.store, &changes.served).await?;
    gateway
        .store
        .put_service_heartbeat(&ServiceHeartbeat {
            role: ServiceRole::Gateway,
            instance_id: gateway.health_id.clone(),
            version: env!("CARGO_PKG_VERSION").into(),
            host: swarmy_config::service_hostname(),
            started_at: gateway.started_at,
            last_seen: Timestamp::now(),
            detail: ServiceDetail::Providers(changes.served.clone()),
        })
        .await?;
    Ok(())
}
/// Whether a claim attempt won the inference lease or found the work already
/// done. Helpers return this instead of `()` so `handle` and the drive loop
/// can stop after acknowledging a completed job rather than re-driving it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimOutcome {
    Claimed,
    Acknowledged,
}

impl Gateway {
    async fn completed(&self, request: RequestId) -> Result<bool> {
        Ok(self
            .store
            .get_idempotency(request)
            .await?
            .is_some_and(|record| record.state == IdempotencyState::Completed))
    }

    pub(crate) async fn handle(&self, message: &WorkMessage<InferenceJobRef>) -> Result<()> {
        let job = &message.value;
        if job.request_id != RequestId::for_step(job.session_id, job.step) {
            // Invalid jobs cannot identify legitimate work to fail in the session log.
            warn!(request_id = %job.request_id, "rejecting mismatched request id");
            return Ok(message.acknowledge().await?);
        }
        let mut claim = InferenceClaim {
            session_id: job.session_id,
            request_id: job.request_id,
            owner: LeaseOwnerId::from_ulid(Ulid::generate()),
            expires_at: Timestamp::now(),
        };
        match self.acquire_claim(message, job, &mut claim).await? {
            ClaimOutcome::Acknowledged => return Ok(()),
            ClaimOutcome::Claimed => {}
        }
        let Some(stored) = self.load_stored_job(job).await? else {
            // Nothing can serve a reference whose request is gone; the worker's
            // recovery scan republishes live work with its request stored.
            warn!(request_id = %job.request_id, "terminating job with no stored request");
            return Ok(message.terminate().await?);
        };
        self.drive_work(message, job, &claim, &stored).await
    }

    /// Hold the inference claim until the store accepts it, backing off on
    /// the ack deadline while another owner finishes or the job completes.
    async fn acquire_claim(
        &self,
        message: &WorkMessage<InferenceJobRef>,
        job: &InferenceJobRef,
        claim: &mut InferenceClaim,
    ) -> Result<ClaimOutcome> {
        loop {
            let now = Timestamp::now();
            claim.expires_at = now.checked_add(self.ack_wait)?;
            if self.store.start_inference(claim, now).await? {
                return Ok(ClaimOutcome::Claimed);
            }
            if self.completed(job.request_id).await? {
                message.acknowledge().await?;
                return Ok(ClaimOutcome::Acknowledged);
            }
            message.extend_deadline().await?;
            sleep(self.ack_wait / 3).await;
        }
    }

    /// Load the stored request a delivery refers to and check it still
    /// matches the delivered selection. Returns `None` when the request is
    /// gone and the caller must terminate the job.
    async fn load_stored_job(&self, job: &InferenceJobRef) -> Result<Option<InferenceJob>> {
        let Some(request) = self
            .store
            .get_inference_request::<swarmy_llm::Request>(job.request_id)
            .await?
        else {
            return Ok(None);
        };
        if request.settings != job.selection {
            return Err(Error::Internal(
                "stored inference selection differs from delivery",
            ));
        }
        Ok(Some(InferenceJob {
            summary: job.summary,
            summary_prefix: job.summary_prefix,
            summary_cut: None,
            summary_recovery: false,
            session_id: job.session_id,
            step: job.step,
            request_id: job.request_id,
            provider: job.provider.clone(),
            entry: job.entry.clone(),
            route: job.route.clone(),
            route_step: job.route_step,
            request,
        }))
    }

    /// Drive one claimed delivery to completion, extending the ack deadline
    /// and renewing the claim until the work finishes or another owner wins.
    async fn drive_work(
        &self,
        message: &WorkMessage<InferenceJobRef>,
        job: &InferenceJobRef,
        claim: &InferenceClaim,
        stored: &InferenceJob,
    ) -> Result<()> {
        let work = self.process(message, claim, stored);
        tokio::pin!(work);
        let period = self.ack_wait / 3;
        let mut heartbeat = interval_at(Instant::now() + period, period);
        loop {
            tokio::select! {
                result = &mut work => return result,
                _ = heartbeat.tick() => {
                    message.extend_deadline().await?;
                    match self.renew_claim(message, job, claim).await? {
                        ClaimOutcome::Acknowledged => return Ok(()),
                        ClaimOutcome::Claimed => {}
                    }
                }
            }
        }
    }

    /// Renew the inference claim after extending the deadline. A lost claim
    /// for a completed job just acknowledges; any other loss fails the
    /// delivery so the worker's recovery scan republishes it.
    async fn renew_claim(
        &self,
        message: &WorkMessage<InferenceJobRef>,
        job: &InferenceJobRef,
        claim: &InferenceClaim,
    ) -> Result<ClaimOutcome> {
        let now = Timestamp::now();
        let renewal = InferenceClaim {
            expires_at: now.checked_add(self.ack_wait)?,
            ..claim.clone()
        };
        if self.store.start_inference(&renewal, now).await? {
            return Ok(ClaimOutcome::Claimed);
        }
        if self.completed(job.request_id).await? {
            message.acknowledge().await?;
            return Ok(ClaimOutcome::Acknowledged);
        }
        Err(Error::Internal("inference claim was replaced"))
    }

    fn turn_id(job: &InferenceJob) -> Option<MessageId> {
        job.request
            .messages
            .iter()
            .rev()
            .find(|message| message.role == MessageRole::User)
            .map(|message| message.id)
    }

    pub(crate) async fn observe_inference_stage(
        &self,
        job: &InferenceJob,
        turn: Option<MessageId>,
        stage: swarmy_core::TurnStage,
    ) {
        if let Some(turn) = turn {
            let event = Bus::turn_event(job.session_id, turn, stage, Some(job.request_id));
            self.bus.record_turn(&event).await;
            self.store.observe_turn_stage(event);
        }
    }

    pub(crate) fn observe_wait(
        &self,
        job: &InferenceJob,
        turn: Option<MessageId>,
        kind: swarmy_store::WaitKind,
    ) {
        if let Some(turn) = turn {
            self.store.observe_turn_metric(
                job.session_id,
                turn,
                swarmy_store::MetricPatch::Wait {
                    request_id: job.request_id.to_string(),
                    kind,
                },
            );
        }
    }

    pub(crate) fn observe_terminal_metric(
        &self,
        job: &InferenceJob,
        turn: Option<MessageId>,
        provider: &str,
        event: &Event,
        streamed: Option<bool>,
    ) {
        let Some(turn) = turn else { return };
        let patch = match event {
            Event::InferenceCompleted { completion, .. } => Some(
                swarmy_store::MetricPatch::Inference(swarmy_store::InferenceMetric {
                    request_id: job.request_id.to_string(),
                    provider: provider.to_owned(),
                    model: job.request.settings.model.clone(),
                    input_tokens: completion.usage.input_tokens,
                    cached_input_tokens: completion.usage.cached_input_tokens,
                    output_tokens: completion.usage.output_tokens,
                    reasoning_tokens: completion.usage.reasoning_output_tokens,
                    cost_micros: completion.cost_micros,
                    streamed,
                    ..Default::default()
                }),
            ),
            Event::InferenceFailed {
                error,
                retryable: false,
                ..
            } => Some(swarmy_store::MetricPatch::Inference(
                swarmy_store::InferenceMetric {
                    request_id: job.request_id.to_string(),
                    provider: provider.to_owned(),
                    model: job.request.settings.model.clone(),
                    error: Some(error.chars().take(512).collect()),
                    ..Default::default()
                },
            )),
            Event::InferenceFailed { .. } => None,
            _ => unreachable!("gateway terminal event"),
        };
        if let Some(patch) = patch {
            self.store.observe_turn_metric(job.session_id, turn, patch);
        }
        // The turn-level error drives the session rollup alongside the
        // per-request error above; both land as independent patches.
        if let Event::InferenceFailed {
            error,
            retryable: false,
            ..
        } = event
        {
            self.store.observe_turn_metric(
                job.session_id,
                turn,
                swarmy_store::MetricPatch::Error(error.clone()),
            );
        }
    }

    pub(crate) async fn process(
        &self,
        message: &WorkMessage<InferenceJobRef>,
        claim: &InferenceClaim,
        job: &InferenceJob,
    ) -> Result<()> {
        let provider = if job.provider.is_empty() {
            &self.default_provider
        } else {
            &job.provider
        };
        let delivery = Delivery {
            message,
            claim,
            job,
            provider,
            turn: Self::turn_id(job),
        };
        let effort = self.effort_for(job, provider);
        self.observe_inference_stage(job, delivery.turn, swarmy_core::TurnStage::InferenceStarted)
            .await;
        let outcome = self
            .attempt_provider(
                job,
                provider,
                effort.used,
                delivery.turn,
                job.entry.as_deref(),
            )
            .await?;
        self.observe_inference_stage(
            job,
            delivery.turn,
            swarmy_core::TurnStage::InferenceFinished,
        )
        .await;
        let Some(outcome) = self.retry_or_continue(&delivery, outcome).await? else {
            return Ok(());
        };
        self.commit_terminal(&delivery, &effort, outcome).await
    }
}
