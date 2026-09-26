use swarmy_gateway::{config, cost::cost_micros, providers::Providers};

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use jiff::Timestamp;
use swarmy_bus::{Bus, LiveFeed, WorkMessage, WorkQueue};
use swarmy_core::{
    Event, IdempotencyState, LeaseOwnerId, Message, MessageId, MessageRole, RequestId,
};
use swarmy_llm::{Delta, InferenceJob, InferenceJobRef, Response};
use swarmy_store::{
    CredentialKey, GatewayProvider, InferenceClaim, InferenceCompletion, ServiceDetail,
    ServiceHeartbeat, ServiceRole, Store,
    blob::{BlobStore, ObjectBlobStore},
};
use tokio::{
    sync::Semaphore,
    task::JoinSet,
    time::{Instant, interval_at, sleep},
};
use tracing::{error, info, warn};
use ulid::Ulid;

struct Gateway {
    store: Store,
    blobs: Arc<dyn BlobStore>,
    bus: Bus,
    providers: Providers,
    default_provider: String,
    ack_wait: Duration,
    max_deliver: i64,
    resend_interval: Duration,
    max_backoff: Duration,
    health_id: String,
    started_at: Timestamp,
}

fn retryable_error(error: &swarmy_llm::Error) -> (bool, Option<Duration>) {
    use swarmy_llm::Error;
    match error {
        Error::Retryable {
            status,
            retry_after,
        } => (
            *status == reqwest::StatusCode::TOO_MANY_REQUESTS
                || status.is_server_error()
                || matches!(status.as_u16(), 408 | 409),
            *retry_after,
        ),
        Error::ProviderResponse {
            status,
            message,
            retry_after,
        } => {
            let text = message.to_ascii_lowercase();
            (
                *status == reqwest::StatusCode::TOO_MANY_REQUESTS
                    || status.is_server_error()
                    || matches!(status.as_u16(), 408 | 409)
                    || text.contains("usage_limit")
                    || text.contains("usage limit")
                    || text.contains("rate_limit")
                    || text.contains("rate limit"),
                *retry_after,
            )
        }
        Error::Status(status) => (
            *status == reqwest::StatusCode::TOO_MANY_REQUESTS
                || status.is_server_error()
                || matches!(status.as_u16(), 408 | 409),
            None,
        ),
        Error::Http(error) => (error.is_connect() || error.is_timeout(), None),
        _ => (false, None),
    }
}

/// The fake provider yields whole parts without streaming text deltas, so a
/// completed part is the first observable content for those turns.
fn part_has_content(part: &swarmy_core::Part) -> bool {
    match part {
        swarmy_core::Part::Text { text } | swarmy_core::Part::Reasoning { text, .. } => {
            !text.is_empty()
        }
        swarmy_core::Part::ToolCall { .. } | swarmy_core::Part::Image { .. } => true,
        swarmy_core::Part::ToolResult { .. } => false,
    }
}

/// First observable model content in a stream delta. Text, reasoning, and
/// tool-argument deltas count when nonempty; completed parts count too so the
/// fake provider's `PartDone` stream starts the first-token clock.
fn is_first_content(delta: &swarmy_llm::Delta) -> bool {
    match delta {
        swarmy_llm::Delta::Text { text, .. } | swarmy_llm::Delta::Reasoning { text, .. } => {
            !text.is_empty()
        }
        swarmy_llm::Delta::ToolArguments { arguments, .. } => !arguments.is_empty(),
        swarmy_llm::Delta::PartDone { part, .. } => part_has_content(part),
        swarmy_llm::Delta::Completed(_) => false,
    }
}

/// One streaming chunk toward the `streamed` flag. Only incremental text,
/// reasoning, and tool-argument deltas count: every real client emits one
/// `PartDone` per part after the incremental deltas, so counting `PartDone`
/// would label every single-chunk response as streamed.
fn is_stream_chunk(delta: &swarmy_llm::Delta) -> bool {
    match delta {
        swarmy_llm::Delta::Text { text, .. } | swarmy_llm::Delta::Reasoning { text, .. } => {
            !text.is_empty()
        }
        swarmy_llm::Delta::ToolArguments { arguments, .. } => !arguments.is_empty(),
        swarmy_llm::Delta::PartDone { .. } | swarmy_llm::Delta::Completed(_) => false,
    }
}

/// Whether a response with this many streaming chunks counts as streamed.
/// Zero or one chunk means the provider delivered the whole response at once
/// (the fake provider yields no incremental deltas at all); the store divides
/// throughput by the whole request in that case instead of by a millisecond
/// streaming tail.
fn is_streamed_response(content_chunks: u32) -> bool {
    content_chunks > 1
}

/// A 5xx, 408, or 409 is a provider failure, not a rate limit. Only 429 or an
/// explicit retry-after means the provider asked for a slower pace.
fn rate_limited(error: &swarmy_llm::Error) -> bool {
    use swarmy_llm::Error;
    match error {
        Error::Retryable {
            status,
            retry_after,
        }
        | Error::ProviderResponse {
            status,
            retry_after,
            ..
        } => *status == reqwest::StatusCode::TOO_MANY_REQUESTS || retry_after.is_some(),
        Error::Status(status) => *status == reqwest::StatusCode::TOO_MANY_REQUESTS,
        _ => false,
    }
}

fn permanent_error(error: &swarmy_llm::Error) -> bool {
    use swarmy_llm::Error;
    if retryable_error(error).0 {
        return false;
    }
    matches!(
        error,
        Error::UnknownModel { .. }
            | Error::Unsupported(_)
            | Error::ContextOverflow(_)
            | Error::Credentials(_)
            | Error::NeedsLogin(_)
    ) || matches!(error, Error::ProviderResponse { status, .. } | Error::Status(status)
            if matches!(status.as_u16(), 401 | 403 | 404))
}

// Boot before the runtime so the network guard outlives all database tasks.
fn main() -> Result<()> {
    swarmy_version::parse::<swarmy_version::ServiceArgs>("swarmy-gateway")?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let config = config::Config::from_env()?;
    let _network = swarmy_store::boot();
    tokio::runtime::Runtime::new()?.block_on(async {
        tokio::select! {
            result = run(config) => result,
            result = tokio::signal::ctrl_c() => Ok(result?),
        }
    })
}

async fn run(config: config::Config) -> Result<()> {
    let blobs = Arc::new(ObjectBlobStore::from_env()?);
    let store = Store::open(
        Some(&config.cluster),
        Some(&config.directory),
        blobs.clone(),
    )
    .await?;
    let providers = Providers::discover(store.clone(), &config.settings).await?;
    let bus = Bus::connect(&config.nats, config.bus.clone()).await?;
    let mut messages = futures::stream::SelectAll::new();
    let mut subscriptions = BTreeSet::new();
    let gateway = Arc::new(Gateway {
        store,
        blobs,
        bus,
        providers,
        default_provider: config.settings.provider,
        ack_wait: config.bus.ack_wait,
        max_deliver: config.bus.max_deliver,
        resend_interval: config.resend_interval,
        max_backoff: Duration::from_secs(config.settings.inference.max_backoff_seconds.get()),
        health_id: Ulid::generate().to_string(),
        started_at: Timestamp::now(),
    });
    let semaphore = Arc::new(Semaphore::new(config.concurrency));
    let mut tasks = JoinSet::new();
    let mut ticks = tokio::time::interval(ADVERTISEMENT_INTERVAL);
    refresh(&gateway, &mut messages, &mut subscriptions).await?;
    ticks.tick().await;
    info!(concurrency = config.concurrency, "gateway ready");
    loop {
        while let Some(result) = tasks.try_join_next() {
            result?;
        }
        let delivery = tokio::select! {
            _ = ticks.tick() => {
                if let Err(error) = refresh(&gateway, &mut messages, &mut subscriptions).await {
                    warn!(%error, "provider refresh failed; retaining the previous advertisement");
                }
                continue;
            }
            delivery = messages.next(), if !subscriptions.is_empty() => delivery,
        };
        let Some(delivery) = delivery else {
            bail!("work stream ended");
        };
        let message = match delivery {
            Ok(message) => message,
            Err(error) => {
                warn!(%error, "cannot decode work delivery");
                continue;
            }
        };
        let gateway = gateway.clone();
        let permit = semaphore.clone().acquire_owned().await?;
        tasks.spawn(async move {
            let _permit = permit;
            if let Err(error) = gateway.handle(&message).await {
                error!(%error, "delivery left unacknowledged");
            }
        });
    }
}

/// Workers route to a provider only while a gateway advertisement is unexpired.
const ADVERTISEMENT_INTERVAL: Duration = Duration::from_secs(30);
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
        Err(_) => Vec::new(),
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
            Result<WorkMessage<InferenceJobRef>, swarmy_bus::Error>,
        >,
    >,
    subscriptions: &mut BTreeSet<String>,
) -> Result<()> {
    let changes = gateway.providers.refresh().await?;
    for id in &changes.served {
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
    for id in &changes.added {
        info!(provider = %id, "serving provider");
    }
    for id in &changes.removed {
        info!(provider = %id, "provider no longer served");
    }
    for id in &changes.rotated {
        info!(provider = %id, "provider credential changed");
    }
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
    advertise(&gateway.store, &changes.served).await?;
    gateway
        .store
        .put_service_heartbeat(&ServiceHeartbeat {
            role: ServiceRole::Gateway,
            instance_id: gateway.health_id.clone(),
            version: env!("CARGO_PKG_VERSION").into(),
            host: std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into()),
            started_at: gateway.started_at,
            last_seen: Timestamp::now(),
            detail: ServiceDetail::Providers(changes.served),
        })
        .await?;
    Ok(())
}

struct CompletionAttribution {
    entry: Option<String>,
    entry_kind: Option<String>,
    quota_remaining: std::collections::BTreeMap<String, u64>,
    quota_resets: std::collections::BTreeMap<String, u64>,
}

struct TerminalInput<'a> {
    job: &'a InferenceJob,
    provider: &'a str,
    model: Option<&'a swarmy_llm::catalog::ModelInfo>,
    effort_used: Option<swarmy_core::ReasoningEffort>,
    effort_requested: Option<swarmy_core::ReasoningEffort>,
    effort_clamped: bool,
    retryable: bool,
    retry_at: Option<jiff::Timestamp>,
    result: &'a std::result::Result<Response, swarmy_llm::Error>,
    entry: Option<String>,
    route: Option<String>,
    route_step: Option<u32>,
}

impl Gateway {
    async fn completed(&self, request: RequestId) -> Result<bool> {
        Ok(self
            .store
            .get_idempotency(request)
            .await?
            .is_some_and(|record| record.state == IdempotencyState::Completed))
    }

    async fn handle(&self, message: &WorkMessage<InferenceJobRef>) -> Result<()> {
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
        loop {
            let now = Timestamp::now();
            claim.expires_at = now.checked_add(self.ack_wait)?;
            if self.store.start_inference(&claim, now).await? {
                break;
            }
            if self.completed(job.request_id).await? {
                return Ok(message.acknowledge().await?);
            }
            message.extend_deadline().await?;
            sleep(self.ack_wait / 3).await;
        }
        let Some(request) = self
            .store
            .get_inference_request::<swarmy_llm::Request>(job.request_id)
            .await?
        else {
            // Nothing can serve a reference whose request is gone; the worker's
            // recovery scan republishes live work with its request stored.
            warn!(request_id = %job.request_id, "terminating job with no stored request");
            return Ok(message.terminate().await?);
        };
        anyhow::ensure!(
            request.settings == job.selection,
            "stored inference selection differs from delivery"
        );
        let stored = InferenceJob {
            session_id: job.session_id,
            step: job.step,
            request_id: job.request_id,
            provider: job.provider.clone(),
            entry: job.entry.clone(),
            route: job.route.clone(),
            route_step: job.route_step,
            request,
        };
        let work = self.process(message, &claim, &stored);
        tokio::pin!(work);
        let period = self.ack_wait / 3;
        let mut heartbeat = interval_at(Instant::now() + period, period);
        loop {
            tokio::select! {
                result = &mut work => return result,
                _ = heartbeat.tick() => {
                    message.extend_deadline().await?;
                    let now = Timestamp::now();
                    let renewal = InferenceClaim { expires_at: now.checked_add(self.ack_wait)?, ..claim.clone() };
                    if !self.store.start_inference(&renewal, now).await? {
                        if self.completed(job.request_id).await? { return Ok(message.acknowledge().await?); }
                        bail!("inference claim was replaced");
                    }
                }
            }
        }
    }

    async fn stream(
        &self,
        client: &Arc<dyn swarmy_llm::Provider>,
        job: &InferenceJob,
        effort: Option<swarmy_core::ReasoningEffort>,
        turn: Option<MessageId>,
    ) -> Result<(Response, Option<bool>), swarmy_llm::Error> {
        let mut request = job.request.clone();
        request.settings.reasoning_effort = effort;
        let mut stream = client.request_for_session(request, job.session_id);
        let mut response = None;
        let turn_id = turn.map_or_else(|| job.request_id.to_string(), |id| id.to_string());
        let mut token_position = 0_u64;
        let mut first_token = false;
        // Count streaming chunks: more than one means the provider streamed
        // the response, while zero or one means the whole response arrived in
        // a single chunk (the fake provider yields no incremental deltas at
        // all, and single-chunk OpenRouter responses yield one). `PartDone`
        // is excluded from the count but still starts the first-token clock
        // below so the fake provider reports a first token.
        let mut content_chunks = 0_u32;
        while let Some(delta) = stream.next().await {
            let delta = delta?;
            if is_stream_chunk(&delta) {
                content_chunks = content_chunks.saturating_add(1);
            }
            if is_first_content(&delta) && !first_token {
                first_token = true;
                if let Some(turn) = turn {
                    let event = Bus::turn_event(
                        job.session_id,
                        turn,
                        swarmy_core::TurnStage::FirstToken,
                        Some(job.request_id),
                    );
                    self.store.observe_turn_stage(event.clone());
                    let bus = self.bus.clone();
                    tokio::spawn(async move {
                        bus.record_turn(&event).await;
                    });
                }
            }
            if response.is_some() {
                return Err(swarmy_llm::Error::Protocol("delta after completion".into()));
            }
            // Live feeds are ephemeral. An unavailable observer path must not lose
            // a provider result that can still be committed to the durable log.
            if let Err(error) = self
                .bus
                .publish_live(LiveFeed::ModelDeltas(job.session_id), &delta)
                .await
            {
                warn!(%error, "live delta publication failed");
            }
            if let Delta::Text { text, .. } = &delta {
                let live = swarmy_core::LiveTokenDelta {
                    turn_id: turn_id.clone(),
                    position: token_position,
                    text: text.clone(),
                };
                token_position = token_position.saturating_add(text.len() as u64);
                if let Err(error) = self
                    .bus
                    .publish_live(LiveFeed::ApiTokenDeltas(job.session_id), &live)
                    .await
                {
                    warn!(%error, "api token publication failed");
                }
            }
            if let Delta::Completed(completed) = delta {
                response = Some(completed);
            }
        }
        let response = response
            .ok_or_else(|| swarmy_llm::Error::Protocol("stream ended without completion".into()))?;
        Ok((response, Some(is_streamed_response(content_chunks))))
    }

    fn turn_id(job: &InferenceJob) -> Option<MessageId> {
        job.request
            .messages
            .iter()
            .rev()
            .find(|message| message.role == MessageRole::User)
            .map(|message| message.id)
    }

    async fn observe_inference_stage(
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

    fn observe_wait(
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

    fn observe_terminal_metric(
        &self,
        job: &InferenceJob,
        turn: Option<MessageId>,
        provider: &str,
        event: &Event,
        streamed: Option<bool>,
    ) {
        let Some(turn) = turn else { return };
        let patch = match event {
            Event::InferenceCompleted {
                usage, cost_micros, ..
            } => Some(swarmy_store::MetricPatch::Inference(
                swarmy_api_types::InferenceMetric {
                    request_id: job.request_id.to_string(),
                    provider: provider.to_owned(),
                    model: job.request.settings.model.clone(),
                    input_tokens: usage.input_tokens,
                    cached_input_tokens: usage.cached_input_tokens,
                    output_tokens: usage.output_tokens,
                    reasoning_tokens: usage.reasoning_output_tokens,
                    cost_micros: *cost_micros,
                    streamed,
                    ..Default::default()
                },
            )),
            Event::InferenceFailed {
                error,
                retryable: false,
                ..
            } => Some(swarmy_store::MetricPatch::Inference(
                swarmy_api_types::InferenceMetric {
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

    async fn process(
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
        let (model, effort_used, effort_clamped, effort_requested) =
            Self::effort_for(self, job, provider);
        let turn = Self::turn_id(job);
        self.observe_inference_stage(job, turn, swarmy_core::TurnStage::InferenceStarted)
            .await;
        let (result, streamed, blocked, entry, entry_kind) = self
            .attempt_provider(job, provider, effort_used, turn, job.entry.as_deref())
            .await?;
        self.observe_inference_stage(job, turn, swarmy_core::TurnStage::InferenceFinished)
            .await;
        let Some((result, streamed)) = self
            .retry_or_continue(message, claim, job, turn, result, streamed, blocked)
            .await?
        else {
            return Ok(());
        };
        self.commit_terminal(
            message,
            claim,
            job,
            provider,
            model,
            turn,
            effort_used,
            effort_requested,
            effort_clamped,
            result,
            streamed,
            blocked,
            entry,
            entry_kind,
        )
        .await
    }

    fn effort_for(
        &self,
        job: &InferenceJob,
        provider: &str,
    ) -> (
        Option<&swarmy_llm::catalog::ModelInfo>,
        Option<swarmy_core::ReasoningEffort>,
        bool,
        Option<swarmy_core::ReasoningEffort>,
    ) {
        let model = self
            .providers
            .catalog
            .model(provider, &job.request.settings.model);
        let effort_requested = job.request.settings.reasoning_effort;
        let (effort_used, effort_clamped) =
            model
                .zip(effort_requested)
                .map_or((effort_requested, false), |(model, requested)| {
                    let (used, clamped) = model.clamp_effort(requested);
                    (Some(used), clamped)
                });
        (model, effort_used, effort_clamped, effort_requested)
    }

    // Retry routing carries the claim, job, result, streaming flag, and block
    // state together; splitting would separate the retry decision from the
    // streamed flag it preserves.
    #[allow(clippy::too_many_arguments)]
    async fn retry_or_continue(
        &self,
        message: &WorkMessage<InferenceJobRef>,
        claim: &InferenceClaim,
        job: &InferenceJob,
        turn: Option<MessageId>,
        result: std::result::Result<Response, swarmy_llm::Error>,
        streamed: Option<bool>,
        blocked: bool,
    ) -> Result<
        Option<(
            std::result::Result<Response, swarmy_llm::Error>,
            Option<bool>,
        )>,
    > {
        match result {
            Ok(response) => Ok(Some((Ok(response), streamed))),
            Err(error)
                if !blocked
                    && !permanent_error(&error)
                    && !retryable_error(&error).0
                    && message.delivery_count()? < self.max_deliver =>
            {
                warn!(%error, request_id = %job.request_id, "provider failed; retrying");
                self.observe_wait(job, turn, swarmy_store::WaitKind::Retry);
                self.observe_wait(job, turn, swarmy_store::WaitKind::ProviderFailure);
                self.store.release_inference(claim).await?;
                let exponent = u32::try_from(message.delivery_count()?.saturating_sub(1).min(5))?;
                message
                    .negative_acknowledge(Some(Duration::from_millis(100) * 2_u32.pow(exponent)))
                    .await?;
                Ok(None)
            }
            Err(error) => Ok(Some((Err(error), streamed))),
        }
    }

    // Terminal commit carries breaker, event, attribution, and ack state
    // together; splitting would separate the atomic usage write from its fan-out.
    #[allow(clippy::too_many_arguments)]
    async fn commit_terminal(
        &self,
        message: &WorkMessage<InferenceJobRef>,
        claim: &InferenceClaim,
        job: &InferenceJob,
        provider: &str,
        model: Option<&swarmy_llm::catalog::ModelInfo>,
        turn: Option<MessageId>,
        effort_used: Option<swarmy_core::ReasoningEffort>,
        effort_requested: Option<swarmy_core::ReasoningEffort>,
        effort_clamped: bool,
        result: std::result::Result<Response, swarmy_llm::Error>,
        streamed: Option<bool>,
        blocked: bool,
        entry: Option<String>,
        entry_kind: Option<String>,
    ) -> Result<()> {
        let (retryable, retry_at) = self
            .record_breaker(provider, entry.as_deref(), job, &result, blocked)
            .await?;
        if let Err(error) = &result {
            let kind = if rate_limited(error) {
                swarmy_store::WaitKind::RateLimit
            } else {
                swarmy_store::WaitKind::ProviderFailure
            };
            self.observe_wait(job, turn, kind);
            if retryable {
                self.observe_wait(job, turn, swarmy_store::WaitKind::Retry);
            }
        }
        let event = Self::terminal_event(&TerminalInput {
            job,
            provider,
            model,
            effort_used,
            effort_requested,
            effort_clamped,
            retryable,
            retry_at,
            result: &result,
            entry: entry.clone(),
            route: job.route.clone(),
            route_step: Some(job.route_step),
        });
        let attribution = Self::attribution_for(&result, entry, entry_kind);
        self.touch_entry(provider, attribution.entry.as_deref())
            .await;
        let stored_result = result.map_err(|error| error.to_string());
        self.persist_response(job, claim, event.clone(), &stored_result, turn, attribution)
            .await?;
        self.observe_terminal_metric(job, turn, provider, &event, streamed);
        if stored_result.is_ok() {
            self.store.clear_inference_wait(job.session_id).await?;
        }
        message.acknowledge().await?;
        Ok(())
    }

    fn attribution_for(
        result: &std::result::Result<Response, swarmy_llm::Error>,
        entry: Option<String>,
        entry_kind: Option<String>,
    ) -> CompletionAttribution {
        let (quota_remaining, quota_resets) = result.as_ref().map_or_else(
            |_| {
                (
                    std::collections::BTreeMap::new(),
                    std::collections::BTreeMap::new(),
                )
            },
            |response| {
                (
                    response.quota_remaining.clone(),
                    response.quota_resets.clone(),
                )
            },
        );
        CompletionAttribution {
            entry,
            entry_kind,
            quota_remaining,
            quota_resets,
        }
    }

    fn terminal_event(input: &TerminalInput<'_>) -> Event {
        match input.result {
            Ok(response) => Event::InferenceCompleted {
                provider: input.provider.to_owned(),
                model: input.job.request.settings.model.clone(),
                effort_used: input.effort_used,
                usage: response.usage.clone(),
                cost_micros: input
                    .model
                    .map_or(0, |model| cost_micros(&model.cost, &response.usage)),
                effort_requested: input.effort_requested,
                effort_clamped: input.effort_clamped,
                seq: 0,
                request_id: input.job.request_id,
                entry: input.entry.clone(),
                route: input.route.clone(),
                route_step: input.route_step,
                message: Message {
                    id: MessageId::from_ulid(Ulid::generate()),
                    role: MessageRole::Assistant,
                    parts: response.parts.clone(),
                },
            },
            Err(error) => Event::InferenceFailed {
                seq: 0,
                request_id: input.job.request_id,
                error: error.to_string(),
                retryable: input.retryable,
                retry_at: input.retry_at,
            },
        }
    }

    async fn touch_entry(&self, provider: &str, entry: Option<&str>) {
        let Some(label) = entry else {
            return;
        };
        if let Ok(keyring) = swarmy_config::Keyring::load()
            && let Err(error) = self
                .store
                .credentials(keyring)
                .touch_entry(swarmy_core::CredentialScope::Cluster, provider, label)
                .await
        {
            warn!(%error, "credential last-use update failed");
        }
    }

    async fn attempt_provider(
        &self,
        job: &InferenceJob,
        provider: &str,
        effort: Option<swarmy_core::ReasoningEffort>,
        turn: Option<MessageId>,
        pinned: Option<&str>,
    ) -> Result<(
        std::result::Result<Response, swarmy_llm::Error>,
        Option<bool>,
        bool,
        Option<String>,
        Option<String>,
    )> {
        // Resolve the entry first so the breaker check and the call use the
        // same key. Resolution already skips entries with open breakers; the
        // claim below serializes the remaining race to a single probe. A
        // pinned route step selects exactly its entry instead of the pool.
        let model = self
            .providers
            .catalog
            .model(provider, &job.request.settings.model);
        let Some(model) = model else {
            return Ok((
                Err(swarmy_llm::Error::UnknownModel {
                    provider: provider.into(),
                    model: job.request.settings.model.clone(),
                }),
                None,
                false,
                None,
                None,
            ));
        };
        let (client, entry, entry_kind) =
            match self.providers.client_pinned(provider, model, pinned).await {
                Ok(resolved) => resolved,
                Err(error) => {
                    return Ok((Err(error), None, false, pinned.map(str::to_owned), None));
                }
            };
        let key = CredentialKey::for_label(provider, entry.clone());
        if let Some(until) = self.store.claim_entry(&key, Timestamp::now()).await? {
            let reason = self
                .store
                .entry_reason(&key)
                .await?
                .unwrap_or_else(|| "provider temporarily unavailable".into());
            return Ok((
                Err(swarmy_llm::Error::ProviderResponse {
                    status: reqwest::StatusCode::TOO_MANY_REQUESTS,
                    message: reason,
                    retry_after: Some(
                        std::time::Duration::try_from(until - Timestamp::now()).unwrap_or_default(),
                    ),
                }),
                None,
                true,
                entry,
                entry_kind,
            ));
        }
        let (result, streamed) = match self.stream(&client, job, effort, turn).await {
            Ok((response, streamed)) => (Ok(response), streamed),
            Err(error) => (Err(error), None),
        };
        Ok((result, streamed, false, entry, entry_kind))
    }

    async fn record_breaker(
        &self,
        provider: &str,
        entry: Option<&str>,
        job: &InferenceJob,
        result: &std::result::Result<Response, swarmy_llm::Error>,
        blocked: bool,
    ) -> Result<(bool, Option<Timestamp>)> {
        let key = CredentialKey::for_label(provider, entry.map(str::to_owned));
        let Some(error) = result.as_ref().err() else {
            if !blocked {
                self.store.entry_success(&key).await?;
            }
            return Ok((false, None));
        };
        let (retryable, retry_after) = retryable_error(error);
        if !retryable {
            if !blocked {
                self.store.entry_success(&key).await?;
            }
            return Ok((false, None));
        }
        let failures = self.store.entry_failures(&key).await?;
        let delay = retry_after.unwrap_or_else(|| {
            let exponent = failures.min(8);
            let base = Duration::from_secs(1_u64 << exponent).min(self.max_backoff);
            let jitter = u64::from(job.request_id.as_bytes()[0]) * 1000 / 255;
            base.saturating_add(Duration::from_millis(jitter))
                .min(self.max_backoff)
        });
        let until = Timestamp::now().checked_add(delay)?;
        if !blocked {
            // Name the entry in the stored reason so waiting sessions show
            // which key is limited; the unlabeled record keeps the raw error.
            let reason = entry.map_or_else(
                || error.to_string(),
                |label| format!("{provider}/{label}: {error}"),
            );
            self.store.entry_failure(&key, until, &reason).await?;
        }
        Ok((true, Some(until)))
    }

    // The loop retries inside this transaction: a stale head adopts the actual
    // head from the error without an extra fetch, and any other store error
    // backs off and retries with the same head, so a store outage never drops
    // the finished provider response or spends another provider call.
    async fn persist_response(
        &self,
        job: &InferenceJob,
        claim: &InferenceClaim,
        mut event: Event,
        result: &std::result::Result<Response, String>,
        turn: Option<MessageId>,
        attribution: CompletionAttribution,
    ) -> Result<()> {
        // Keep the finished response and renew the deadline during store outages.
        // Retrying only this transaction avoids spending another provider call.
        // Entry attribution and quota observation commit atomically with usage.
        let mut expected_head = job.step;
        loop {
            let completion = InferenceCompletion {
                claim: claim.clone(),
                expected_head,
                event: event.clone(),
                now: Timestamp::now(),
                entry: attribution.entry.clone(),
                entry_kind: attribution.entry_kind.clone(),
                quota_remaining: attribution.quota_remaining.clone(),
                quota_resets: attribution.quota_resets.clone(),
            };
            let snapshot = match self.terminal_snapshot(job, &completion).await {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    warn!(%error, "retrying terminal snapshot upload");
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let committed = if let Some(snapshot) = snapshot.as_ref() {
                self.store
                    .complete_inference_and_idle(&completion, result, snapshot)
                    .await
            } else {
                self.store.complete_inference(&completion, result).await
            };
            match committed {
                Ok(false) => break,
                Ok(true) => {
                    event.set_seq(expected_head + 1);
                    let session = self.store.fetch_session(job.session_id).await?;
                    let committed_snapshot = snapshot.as_ref().filter(|reference| {
                        session.as_ref().is_some_and(|session| {
                            session.state == swarmy_core::SessionState::Idle
                                && session.snapshot_ref.as_ref() == Some(*reference)
                        })
                    });
                    self.notify_completion(job.session_id, &event, turn, committed_snapshot)
                        .await;
                    break;
                }
                Err(swarmy_store::StoreError::StaleSequence { actual, .. }) => {
                    expected_head = actual;
                }
                Err(error) => {
                    warn!(%error, "retrying terminal store update");
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
        Ok(())
    }

    async fn terminal_snapshot(
        &self,
        job: &InferenceJob,
        completion: &InferenceCompletion,
    ) -> Result<Option<swarmy_core::SnapshotRef>> {
        // Main sessions need a worker turn-end step to decide whether to summarize.
        if let Some(session) = self.store.fetch_session(job.session_id).await?
            && matches!(session.kind, swarmy_core::SessionKind::Named { .. })
            && let Some(agent) = self.store.get_agent(session.agent_id).await?
            && agent.main_session == Some(job.session_id)
        {
            return Ok(None);
        }
        // A concurrent log append is not in the immutable request. Let the
        // worker replay it instead of advancing a snapshot over unseen events.
        if completion.expected_head != job.step {
            return Ok(None);
        }
        let Event::InferenceCompleted { message, .. } = &completion.event else {
            return Ok(None);
        };
        if message
            .parts
            .iter()
            .any(|part| matches!(part, swarmy_core::Part::ToolCall { .. }))
        {
            return Ok(None);
        }
        // The worker's immutable request contains the complete replayed message
        // history. Replay the response with the same harness that resumes it.
        let mut events: Vec<_> = job
            .request
            .messages
            .iter()
            .cloned()
            .map(|message| Event::MessageAppended { seq: 0, message })
            .collect();
        events.push(completion.event.clone());
        let bytes = swarmy_core::encode(&swarmy_harness::Snapshot::default().replay(&events))?;
        let snapshot = swarmy_core::SnapshotRef {
            seq: completion
                .expected_head
                .checked_add(2)
                .context("sequence overflow")?,
            object_key: format!("blobs/{}", blake3::hash(&bytes).to_hex()),
        };
        self.blobs.put(&snapshot.object_key, bytes.into()).await?;
        Ok(Some(snapshot))
    }

    async fn notify_completion(
        &self,
        id: swarmy_core::SessionId,
        event: &Event,
        turn: Option<MessageId>,
        snapshot: Option<&swarmy_core::SnapshotRef>,
    ) {
        if let Err(error) = self
            .bus
            .publish_live(LiveFeed::SessionEvents(id), event)
            .await
        {
            warn!(%error, "completion event publication failed; client will catch up");
        }
        if let Some(snapshot) = snapshot {
            if let Some(turn) = turn {
                let event = Bus::turn_event(id, turn, swarmy_core::TurnStage::Idle, None);
                self.bus.record_turn(&event).await;
                // The idle anchor is the turn's wall time. Record it in the
                // store as well as on the bus so a turn of any length keeps
                // its duration even when the worker never sees this turn end.
                self.store.observe_turn_stage(event);
            }
            if let Err(error) = self
                .bus
                .publish_live(
                    LiveFeed::SessionEvents(id),
                    &Event::StateChanged {
                        seq: snapshot.seq,
                        from: swarmy_core::SessionState::WaitingInference,
                        to: swarmy_core::SessionState::Idle,
                    },
                )
                .await
            {
                warn!(%error, "idle event publication failed; client will catch up");
            }
            return;
        }
        if let Err(error) = self
            .bus
            .nudge(id, event.seq(), turn, self.resend_interval, false)
            .await
        {
            warn!(%error, "completion nudge failed; scheduler will recover");
        }
    }
}

#[cfg(test)]
mod retry_tests {
    use super::*;

    #[test]
    fn rate_limits_outages_and_permanent_errors_are_distinct() {
        let rate_limit = swarmy_llm::Error::ProviderResponse {
            status: reqwest::StatusCode::TOO_MANY_REQUESTS,
            message: "quota exceeded".into(),
            retry_after: Some(Duration::from_secs(2)),
        };
        assert_eq!(
            retryable_error(&rate_limit),
            (true, Some(Duration::from_secs(2)))
        );
        let usage_limit = swarmy_llm::Error::ProviderResponse {
            status: reqwest::StatusCode::FORBIDDEN,
            message: "usage_limit_reached".into(),
            retry_after: None,
        };
        assert!(retryable_error(&usage_limit).0);
        assert!(!permanent_error(&usage_limit));
        let outage = swarmy_llm::Error::Status(reqwest::StatusCode::BAD_GATEWAY);
        assert!(retryable_error(&outage).0);
        let auth = swarmy_llm::Error::ProviderResponse {
            status: reqwest::StatusCode::UNAUTHORIZED,
            message: "invalid token".into(),
            retry_after: None,
        };
        assert!(!retryable_error(&auth).0);
        assert!(permanent_error(&auth));
        assert!(permanent_error(&swarmy_llm::Error::ContextOverflow(
            "too long".into()
        )));
    }

    #[test]
    fn only_429_or_retry_after_counts_as_rate_limit() {
        let limited = swarmy_llm::Error::ProviderResponse {
            status: reqwest::StatusCode::TOO_MANY_REQUESTS,
            message: "slow down".into(),
            retry_after: None,
        };
        assert!(rate_limited(&limited));
        let delayed = swarmy_llm::Error::ProviderResponse {
            status: reqwest::StatusCode::BAD_GATEWAY,
            message: "outage".into(),
            retry_after: Some(Duration::from_secs(1)),
        };
        assert!(rate_limited(&delayed));
        let outage = swarmy_llm::Error::Status(reqwest::StatusCode::BAD_GATEWAY);
        assert!(retryable_error(&outage).0);
        assert!(!rate_limited(&outage));
        let conflict = swarmy_llm::Error::Status(reqwest::StatusCode::from_u16(409).unwrap());
        assert!(retryable_error(&conflict).0);
        assert!(!rate_limited(&conflict));
    }

    #[test]
    fn completed_parts_count_as_first_content() {
        use futures::StreamExt as _;
        use swarmy_core::Part;
        assert!(part_has_content(&Part::Text { text: "hi".into() }));
        assert!(!part_has_content(&Part::Text {
            text: String::new()
        }));
        assert!(part_has_content(&Part::ToolCall {
            call_id: swarmy_core::ToolCallId("c".into()),
            tool: "bash".into(),
            input: serde_json::json!({}),
        }));
        // Drive the real fake provider: its stream yields PartDone deltas
        // without any text deltas, so the first delta must start the
        // first-token clock or append-to-first-token stays null on the dev
        // stack.
        let provider = swarmy_llm::fake::FakeProvider::default();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let request = swarmy_llm::Request {
                system_prompt: String::new(),
                messages: Vec::new(),
                tools: Vec::new(),
                settings: swarmy_llm::GenerationSettings::default(),
            };
            let mut stream = {
                use swarmy_llm::Provider as _;
                provider.request(request)
            };
            // An unscripted turn errors before yielding content.
            assert!(stream.next().await.unwrap().is_err());
        });
        let mut scripted = swarmy_llm::fake::FakeProvider::default();
        scripted.responses.insert(
            0,
            swarmy_llm::Response {
                parts: vec![Part::Text {
                    text: "done".into(),
                }],
                stop_reason: swarmy_llm::StopReason::EndTurn,
                usage: swarmy_llm::TokenUsage::default(),
                quota_remaining: std::collections::BTreeMap::new(),
                quota_resets: std::collections::BTreeMap::new(),
            },
        );
        runtime.block_on(async {
            use swarmy_llm::Provider as _;
            let request = swarmy_llm::Request {
                system_prompt: String::new(),
                messages: Vec::new(),
                tools: Vec::new(),
                settings: swarmy_llm::GenerationSettings::default(),
            };
            let deltas: Vec<_> = scripted
                .request(request)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .map(|result| result.unwrap())
                .collect();
            assert!(matches!(deltas[0], Delta::PartDone { .. }));
            let first = deltas.iter().position(is_first_content);
            assert_eq!(first, Some(0));
        });
    }

    #[test]
    fn single_chunk_fake_response_is_unstreamed_with_request_duration_throughput() {
        use futures::StreamExt as _;
        use swarmy_core::Part;
        // The fake provider delivers the whole response in one chunk: a
        // single `PartDone` delta with the completed part, then completion.
        let mut scripted = swarmy_llm::fake::FakeProvider::default();
        scripted.responses.insert(
            0,
            swarmy_llm::Response {
                parts: vec![Part::Text {
                    text: "done".into(),
                }],
                stop_reason: swarmy_llm::StopReason::EndTurn,
                usage: swarmy_llm::TokenUsage {
                    input_tokens: 12,
                    output_tokens: 363,
                    ..Default::default()
                },
                quota_remaining: std::collections::BTreeMap::new(),
                quota_resets: std::collections::BTreeMap::new(),
            },
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            use swarmy_llm::Provider as _;
            let request = swarmy_llm::Request {
                system_prompt: String::new(),
                messages: Vec::new(),
                tools: Vec::new(),
                settings: swarmy_llm::GenerationSettings::default(),
            };
            let deltas: Vec<_> = scripted
                .request(request)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .map(|result| result.unwrap())
                .collect();
            // The fake provider yields no incremental deltas, only `PartDone`:
            // zero streaming chunks still counts as a single-chunk response.
            let chunks = deltas.iter().filter(|delta| is_stream_chunk(delta)).count();
            assert_eq!(chunks, 0);
            assert!(!is_streamed_response(u32::try_from(chunks).unwrap()));
        });
        // Throughput divides by the whole request (first byte to completion):
        // 363 tokens over a 1001 ms request reports about 363 tokens per
        // second instead of dividing by the 1 ms streaming tail.
        let mut turn = swarmy_api_types::TurnMetrics::default();
        let row = |stage: &str, ns: u64, request: Option<&str>| swarmy_api_types::StageTiming {
            stage: stage.into(),
            request_id: request.map(str::to_owned),
            clock_id: "boot".into(),
            monotonic_ns: ns,
            unix_ns: i64::try_from(ns).unwrap(),
        };
        turn.stages.push(row("appended", 1_000_000_000, None));
        turn.stages
            .push(row("inference_started", 2_000_000_000, Some("r")));
        turn.stages
            .push(row("first_token", 3_000_000_000, Some("r")));
        turn.stages
            .push(row("inference_finished", 3_001_000_000, Some("r")));
        turn.inference.push(swarmy_api_types::InferenceMetric {
            request_id: "r".into(),
            output_tokens: 363,
            streamed: Some(false),
            ..Default::default()
        });
        turn.derive();
        let request = &turn.inference[0];
        assert_eq!(request.streamed, Some(false));
        assert_eq!(request.request_duration_ms, Some(1001.0));
        let expected = 363.0 * 1000.0 / 1001.0;
        assert!((request.output_tokens_per_second.unwrap() - expected).abs() < 1.0);
    }

    #[test]
    fn part_done_does_not_count_as_a_stream_chunk() {
        use swarmy_core::Part;
        let text = swarmy_llm::Delta::Text {
            output_index: 0,
            text: "hi".into(),
        };
        let done = swarmy_llm::Delta::PartDone {
            output_index: 0,
            part: Part::Text { text: "hi".into() },
        };
        // `PartDone` still starts the first-token clock (the fake provider
        // relies on it) but never counts toward the streamed flag.
        assert!(is_first_content(&text));
        assert!(is_first_content(&done));
        assert!(is_stream_chunk(&text));
        assert!(!is_stream_chunk(&done));
        assert!(!is_stream_chunk(&swarmy_llm::Delta::Completed(
            swarmy_llm::Response {
                parts: Vec::new(),
                stop_reason: swarmy_llm::StopReason::EndTurn,
                usage: swarmy_llm::TokenUsage::default(),
                quota_remaining: std::collections::BTreeMap::new(),
                quota_resets: std::collections::BTreeMap::new(),
            }
        )));
    }

    /// Build a real `Gateway` against the dev stack so the stream tests below
    /// exercise `Gateway::stream` itself instead of copying its counting loop.
    /// Returns `None` (and the caller skips) when the stack is absent. The
    /// tests pass `turn: None`, so no turn rows are written; only ephemeral
    /// live publishes reach NATS.
    async fn stream_test_gateway() -> Option<Gateway> {
        use foundationdb::{Database, tuple::Subspace};
        use std::sync::OnceLock;
        static NETWORK: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();
        let Ok(cluster) = std::env::var("SWARMY_FDB_CLUSTER_FILE") else {
            eprintln!("skipping gateway stream test: SWARMY_FDB_CLUSTER_FILE unset");
            return None;
        };
        let Ok(nats_url) = std::env::var("SWARMY_NATS_URL") else {
            eprintln!("skipping gateway stream test: SWARMY_NATS_URL unset");
            return None;
        };
        NETWORK.get_or_init(swarmy_store::boot);
        let store = Store::with_subspace(
            Arc::new(Database::new(Some(&cluster)).unwrap()),
            Subspace::all().subspace(&("gateway-stream-tests", ulid::Ulid::generate().to_string())),
            Arc::new(swarmy_store::blob::MemoryBlobStore::default()),
        );
        let bus = Bus::connect(&nats_url, swarmy_bus::Config::default())
            .await
            .expect("dev NATS must be reachable for gateway stream tests");
        let settings = swarmy_config::Settings::default();
        let providers = Providers::discover(store.clone(), &settings)
            .await
            .expect("provider discovery must succeed for gateway stream tests");
        Some(Gateway {
            store,
            blobs: Arc::new(swarmy_store::blob::MemoryBlobStore::default()),
            bus,
            providers,
            default_provider: "fake".into(),
            ack_wait: Duration::from_secs(30),
            max_deliver: 5,
            resend_interval: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            health_id: "stream-test".into(),
            started_at: Timestamp::now(),
        })
    }

    struct ScriptedStream(Vec<swarmy_llm::Delta>);

    impl swarmy_llm::Provider for ScriptedStream {
        fn request(&self, _request: swarmy_llm::Request) -> swarmy_llm::ProviderStream {
            let deltas = self.0.clone();
            Box::pin(async_stream::try_stream! {
                for delta in deltas {
                    yield delta;
                }
            })
        }
    }

    fn stream_test_job() -> swarmy_llm::InferenceJob {
        let session_id = swarmy_core::SessionId::from_ulid(ulid::Ulid::generate());
        let step = 1;
        swarmy_llm::InferenceJob {
            session_id,
            step,
            request_id: swarmy_core::RequestId::for_step(session_id, step),
            request: swarmy_llm::Request {
                system_prompt: String::new(),
                messages: Vec::new(),
                tools: Vec::new(),
                settings: swarmy_llm::GenerationSettings::default(),
            },
            provider: "fake".into(),
            entry: None,
            route: None,
            route_step: 0,
        }
    }

    /// Drive `Gateway::stream` with one text delta plus the terminal
    /// `PartDone`: a single-chunk response reports `streamed == Some(false)`.
    #[tokio::test]
    async fn one_text_delta_plus_part_done_is_single_chunk() {
        use swarmy_core::Part;
        let Some(gateway) = stream_test_gateway().await else {
            return;
        };
        let completed = swarmy_llm::Response {
            parts: vec![Part::Text { text: "hi".into() }],
            stop_reason: swarmy_llm::StopReason::EndTurn,
            usage: swarmy_llm::TokenUsage::default(),
            quota_remaining: std::collections::BTreeMap::new(),
            quota_resets: std::collections::BTreeMap::new(),
        };
        let client: Arc<dyn swarmy_llm::Provider> = Arc::new(ScriptedStream(vec![
            swarmy_llm::Delta::Text {
                output_index: 0,
                text: "hi".into(),
            },
            swarmy_llm::Delta::PartDone {
                output_index: 0,
                part: Part::Text { text: "hi".into() },
            },
            swarmy_llm::Delta::Completed(completed),
        ]));
        let job = stream_test_job();
        let (response, streamed) = gateway.stream(&client, &job, None, None).await.unwrap();
        assert_eq!(streamed, Some(false));
        assert_eq!(response.parts.len(), 1);
    }

    /// Drive `Gateway::stream` with two incremental text deltas plus
    /// `PartDone`: more than one chunk reports `streamed == Some(true)`.
    #[tokio::test]
    async fn two_text_deltas_are_streamed() {
        use swarmy_core::Part;
        let Some(gateway) = stream_test_gateway().await else {
            return;
        };
        let completed = swarmy_llm::Response {
            parts: vec![Part::Text { text: "ab".into() }],
            stop_reason: swarmy_llm::StopReason::EndTurn,
            usage: swarmy_llm::TokenUsage::default(),
            quota_remaining: std::collections::BTreeMap::new(),
            quota_resets: std::collections::BTreeMap::new(),
        };
        let client: Arc<dyn swarmy_llm::Provider> = Arc::new(ScriptedStream(vec![
            swarmy_llm::Delta::Text {
                output_index: 0,
                text: "a".into(),
            },
            swarmy_llm::Delta::Text {
                output_index: 0,
                text: "b".into(),
            },
            swarmy_llm::Delta::PartDone {
                output_index: 0,
                part: Part::Text { text: "ab".into() },
            },
            swarmy_llm::Delta::Completed(completed),
        ]));
        let job = stream_test_job();
        let (response, streamed) = gateway.stream(&client, &job, None, None).await.unwrap();
        assert_eq!(streamed, Some(true));
        assert_eq!(response.parts.len(), 1);
    }

    /// Drive `Gateway::stream` with a failing provider stream. The stream
    /// itself errors, and the caller (`attempt_provider`) records
    /// `streamed: None` for such attempts so failed requests stay out of the
    /// single-chunk count.
    #[tokio::test]
    async fn failing_stream_errors_without_a_streamed_flag() {
        struct Failing;
        impl swarmy_llm::Provider for Failing {
            fn request(&self, _request: swarmy_llm::Request) -> swarmy_llm::ProviderStream {
                Box::pin(futures::stream::iter(vec![Err(
                    swarmy_llm::Error::Protocol("boom".into()),
                )]))
            }
        }
        let Some(gateway) = stream_test_gateway().await else {
            return;
        };
        let client: Arc<dyn swarmy_llm::Provider> = Arc::new(Failing);
        let job = stream_test_job();
        assert!(gateway.stream(&client, &job, None, None).await.is_err());
    }
}
