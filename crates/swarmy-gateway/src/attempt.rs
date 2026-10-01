//! Gateway attempts: streaming, effort selection, and provider calls.

use std::{sync::Arc, time::Duration};

use crate::Result;
use futures::StreamExt;
use jiff::Timestamp;
use swarmy_bus::{Bus, LiveFeed};
use swarmy_core::{CredentialEntryKind, MessageId};
use swarmy_llm::{Delta, InferenceJob, InferenceJobRef, Response};
use swarmy_store::CredentialKey;
use tracing::warn;

use crate::dispatch::Gateway;

fn part_has_content(part: &swarmy_core::Part) -> bool {
    match part {
        swarmy_core::Part::Text { text }
        | swarmy_core::Part::Reasoning { text, .. }
        | swarmy_core::Part::Notice { text, .. } => !text.is_empty(),
        swarmy_core::Part::ToolCall { .. } | swarmy_core::Part::Image { .. } => true,
        swarmy_core::Part::ToolResult { .. } => false,
    }
}

/// First observable model content in a stream delta. Text, reasoning, and
/// tool-argument deltas count when nonempty; completed parts count too so the
/// fake provider's `PartDone` stream starts the first-token clock.
fn is_first_content(delta: &Delta) -> bool {
    match delta {
        Delta::Text { text, .. } | Delta::Reasoning { text, .. } => !text.is_empty(),
        Delta::ToolArguments { arguments, .. } => !arguments.is_empty(),
        Delta::PartDone { part, .. } => part_has_content(part),
        Delta::Completed(_) => false,
    }
}

/// One streaming chunk toward the `streamed` flag. Only incremental text,
/// reasoning, and tool-argument deltas count: every real client emits one
/// `PartDone` per part after the incremental deltas, so counting `PartDone`
/// would label every single-chunk response as streamed.
fn is_stream_chunk(delta: &Delta) -> bool {
    match delta {
        Delta::Text { text, .. } | Delta::Reasoning { text, .. } => !text.is_empty(),
        Delta::ToolArguments { arguments, .. } => !arguments.is_empty(),
        Delta::PartDone { .. } | Delta::Completed(_) => false,
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

/// Effort selected for one provider attempt: the catalog model when known,
/// the effort actually sent, the effort the turn requested, and whether the
/// model clamp changed it.
pub(crate) struct EffortChoice<'a> {
    pub(crate) model: Option<&'a swarmy_llm::catalog::ModelInfo>,
    pub(crate) used: Option<swarmy_core::ReasoningEffort>,
    pub(crate) requested: Option<swarmy_core::ReasoningEffort>,
    pub(crate) clamped: bool,
}

/// One provider attempt: the response or failure, whether the body streamed,
/// whether a breaker blocked the call without spending it, and the entry
/// the call is attributed to.
pub(crate) struct AttemptOutcome {
    pub(crate) result: std::result::Result<Response, swarmy_llm::Error>,
    pub(crate) streamed: Option<bool>,
    pub(crate) blocked: bool,
    pub(crate) entry: Option<String>,
    pub(crate) entry_kind: Option<CredentialEntryKind>,
}

/// Stable per-delivery context threading through attempt, retry, and commit
/// so those stages take two or three arguments instead of seven or fourteen.
pub(crate) struct Delivery<'a> {
    pub(crate) message: &'a swarmy_bus::WorkMessage<InferenceJobRef>,
    pub(crate) claim: &'a swarmy_store::InferenceClaim,
    pub(crate) job: &'a InferenceJob,
    pub(crate) provider: &'a str,
    pub(crate) turn: Option<MessageId>,
}

/// Per-delta state while streaming one provider response: token positions,
/// chunk counts for the streamed verdict, the first-token flag, and the
/// terminal response. Each stage of a delta is its own method so the
/// streaming loop reads as the pipeline it drives.
struct DeltaFold {
    turn_id: String,
    token_position: u64,
    // Count streaming chunks: more than one means the provider streamed the
    // response, while zero or one means the whole response arrived in a
    // single chunk (the fake provider yields no incremental deltas at all,
    // and single-chunk OpenRouter responses yield one). `PartDone` is
    // excluded from the count but still starts the first-token clock below
    // so the fake provider reports a first token.
    content_chunks: u32,
    first_token: bool,
    response: Option<Response>,
}

impl DeltaFold {
    fn new(turn_id: String) -> Self {
        Self {
            turn_id,
            token_position: 0,
            content_chunks: 0,
            first_token: false,
            response: None,
        }
    }

    async fn accumulate(
        &mut self,
        gateway: &Gateway,
        job: &InferenceJob,
        turn: Option<MessageId>,
        delta: Delta,
    ) -> std::result::Result<(), swarmy_llm::Error> {
        if is_stream_chunk(&delta) {
            self.content_chunks = self.content_chunks.saturating_add(1);
        }
        self.observe_first_token(gateway, job, turn, &delta).await;
        if self.response.is_some() {
            return Err(swarmy_llm::Error::Protocol("delta after completion".into()));
        }
        self.publish_live(gateway, job, &delta).await;
        if let Delta::Completed(completed) = delta {
            self.response = Some(completed);
        }
        Ok(())
    }

    async fn observe_first_token(
        &mut self,
        gateway: &Gateway,
        job: &InferenceJob,
        turn: Option<MessageId>,
        delta: &Delta,
    ) {
        if !is_first_content(delta) || self.first_token {
            return;
        }
        self.first_token = true;
        if let Some(turn) = turn {
            let event = Bus::turn_event(
                job.session_id,
                turn,
                swarmy_core::TurnStage::FirstToken,
                Some(job.request_id),
            );
            gateway.store.observe_turn_stage(event.clone());
            gateway.bus.record_turn(&event).await;
        }
    }

    /// Publish one delta to both live feeds. Live feeds are ephemeral: an
    /// unavailable observer path must not lose a provider result that can
    /// still be committed to the durable log.
    async fn publish_live(&mut self, gateway: &Gateway, job: &InferenceJob, delta: &Delta) {
        if let Err(error) = gateway
            .bus
            .publish_live(LiveFeed::ModelDeltas(job.session_id), delta)
            .await
        {
            warn!(error = %swarmy_core::error_chain(&error), "live delta publication failed");
        }
        if let Delta::Text { text, .. } = delta {
            let live = swarmy_core::LiveTokenDelta {
                turn_id: self.turn_id.clone(),
                position: self.token_position,
                text: text.clone(),
            };
            self.token_position = self.token_position.saturating_add(text.len() as u64);
            if let Err(error) = gateway
                .bus
                .publish_live(LiveFeed::ApiTokenDeltas(job.session_id), &live)
                .await
            {
                warn!(error = %swarmy_core::error_chain(&error), "api token publication failed");
            }
        }
    }

    fn finish(self) -> std::result::Result<(Response, Option<bool>), swarmy_llm::Error> {
        let response = self
            .response
            .ok_or_else(|| swarmy_llm::Error::Protocol("stream ended without completion".into()))?;
        Ok((response, Some(is_streamed_response(self.content_chunks))))
    }
}

impl Gateway {
    pub(crate) async fn stream(
        &self,
        client: &Arc<dyn swarmy_llm::Provider>,
        job: &InferenceJob,
        effort: Option<swarmy_core::ReasoningEffort>,
        turn: Option<MessageId>,
    ) -> std::result::Result<(Response, Option<bool>), swarmy_llm::Error> {
        let mut request = job.request.clone();
        request.settings.reasoning_effort = effort;
        request.no_cache = job.summary;
        let mut stream = client.request_for_session(request, job.session_id);
        let turn_id = turn.map_or_else(|| job.request_id.to_string(), |id| id.to_string());
        let mut fold = DeltaFold::new(turn_id);
        while let Some(delta) = stream.next().await {
            fold.accumulate(self, job, turn, delta?).await?;
        }
        fold.finish()
    }

    pub(crate) fn effort_for(&self, job: &InferenceJob, provider: &str) -> EffortChoice<'_> {
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
        EffortChoice {
            model,
            used: effort_used,
            requested: effort_requested,
            clamped: effort_clamped,
        }
    }

    pub(crate) async fn attempt_provider(
        &self,
        job: &InferenceJob,
        provider: &str,
        effort: Option<swarmy_core::ReasoningEffort>,
        turn: Option<MessageId>,
        pinned: Option<&str>,
    ) -> Result<AttemptOutcome> {
        // Resolve the entry first so the breaker check and the call use the
        // same key. Resolution already skips entries with open breakers; the
        // claim below serializes the remaining race to a single probe. A
        // pinned route step selects exactly its entry instead of the pool.
        let model = self
            .providers
            .catalog
            .model(provider, &job.request.settings.model);
        let Some(model) = model else {
            return Ok(AttemptOutcome {
                result: Err(swarmy_llm::Error::UnknownModel {
                    provider: provider.into(),
                    model: job.request.settings.model.clone(),
                }),
                streamed: None,
                blocked: false,
                entry: None,
                entry_kind: None,
            });
        };
        let resolved = match self.providers.client_pinned(provider, model, pinned).await {
            Ok(resolved) => resolved,
            Err(error) => {
                return Ok(AttemptOutcome {
                    result: Err(error),
                    streamed: None,
                    blocked: false,
                    entry: pinned.map(str::to_owned),
                    entry_kind: None,
                });
            }
        };
        let crate::providers::ResolvedClient {
            client,
            entry,
            entry_kind,
        } = resolved;
        let key = CredentialKey::for_label(provider, entry.clone());
        if let Some(until) = self.store.claim_entry(&key, Timestamp::now()).await? {
            let reason = self
                .store
                .entry_reason(&key)
                .await?
                .unwrap_or_else(|| "provider temporarily unavailable".into());
            return Ok(AttemptOutcome {
                result: Err(swarmy_llm::Error::ProviderResponse {
                    reason: swarmy_llm::ProviderFailureReason::Other,
                    status: reqwest::StatusCode::TOO_MANY_REQUESTS,
                    message: reason,
                    retry_after: Some(
                        Duration::try_from(until - Timestamp::now()).unwrap_or_default(),
                    ),
                }),
                streamed: None,
                blocked: true,
                entry,
                entry_kind,
            });
        }
        let (result, streamed) = match self.stream(&client, job, effort, turn).await {
            Ok((response, streamed)) => (Ok(response), streamed),
            Err(error) => (Err(error), None),
        };
        Ok(AttemptOutcome {
            result,
            streamed,
            blocked: false,
            entry,
            entry_kind,
        })
    }

    pub(crate) async fn record_breaker(
        &self,
        provider: &str,
        entry: Option<&str>,
        job: &InferenceJob,
        result: &std::result::Result<Response, swarmy_llm::Error>,
        blocked: bool,
    ) -> Result<(Option<swarmy_llm::ErrorClass>, Option<Timestamp>)> {
        let key = CredentialKey::for_label(provider, entry.map(str::to_owned));
        let Some(error) = result.as_ref().err() else {
            if !blocked {
                self.store.entry_success(&key).await?;
            }
            return Ok((None, None));
        };
        let class = error.classify();
        let (retryable, retry_after) = (class.retryable, class.retry_after);
        if !retryable {
            if !blocked {
                self.store.entry_success(&key).await?;
            }
            return Ok((Some(class), None));
        }
        let failures = self.store.entry_failures(&key).await?;
        let delay = retry_after.unwrap_or_else(|| {
            let base = swarmy_core::backoff(Duration::from_secs(1), failures.saturating_add(1), 8)
                .min(self.max_backoff);
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
        Ok((Some(class), Some(until)))
    }

    // The loop retries inside this transaction: a stale head adopts the actual
    // head from the error without an extra fetch, and any other store error
    // backs off and retries with the same head, so a store outage never drops
    // the finished provider response or spends another provider call.
}

#[cfg(test)]
mod retry_tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use crate::{dispatch::Gateway, providers::Providers};
    use swarmy_store::Store;

    /// Build a real `Gateway` against the dev stack so the stream tests below
    /// exercise `Gateway::stream` itself instead of copying its counting loop.
    /// Returns `None` (and the caller skips) when the stack is absent. The
    /// tests pass `turn: None`, so no turn rows are written; only ephemeral
    /// live publishes reach NATS.
    async fn stream_test_gateway() -> Option<Gateway> {
        use foundationdb::{Database, tuple::Subspace};
        let cluster = swarmy_testkit::require_stack("SWARMY_FDB_CLUSTER_FILE")?;
        let nats_url = swarmy_testkit::require_stack("SWARMY_NATS_URL")?;
        swarmy_testkit::boot_fdb();
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
            summarize_at_tokens: None,
            model_context_window_tokens: None,
            ack_wait: Duration::from_secs(30),
            max_deliver: 5,
            resend_interval: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            health_id: "stream-test".into(),
            started_at: Timestamp::now(),
        })
    }

    struct ScriptedStream(Vec<Delta>);

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

    fn stream_test_job() -> InferenceJob {
        let session_id = swarmy_core::SessionId::from_ulid(ulid::Ulid::generate());
        let step = 1;
        InferenceJob {
            summary: false,
            summary_prefix: false,
            summary_cut: None,
            summary_recovery: false,
            session_id,
            step,
            request_id: swarmy_core::RequestId::for_step(session_id, step),
            request: swarmy_llm::Request {
                no_cache: false,
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
        let completed = Response {
            parts: vec![Part::Text { text: "hi".into() }],
            stop_reason: swarmy_llm::StopReason::EndTurn,
            usage: swarmy_llm::TokenUsage::default(),
            quota_remaining: std::collections::BTreeMap::new(),
            quota_resets: std::collections::BTreeMap::new(),
        };
        let client: Arc<dyn swarmy_llm::Provider> = Arc::new(ScriptedStream(vec![
            Delta::Text {
                output_index: 0,
                text: "hi".into(),
            },
            Delta::PartDone {
                output_index: 0,
                part: Part::Text { text: "hi".into() },
            },
            Delta::Completed(completed),
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
        let completed = Response {
            parts: vec![Part::Text { text: "ab".into() }],
            stop_reason: swarmy_llm::StopReason::EndTurn,
            usage: swarmy_llm::TokenUsage::default(),
            quota_remaining: std::collections::BTreeMap::new(),
            quota_resets: std::collections::BTreeMap::new(),
        };
        let client: Arc<dyn swarmy_llm::Provider> = Arc::new(ScriptedStream(vec![
            Delta::Text {
                output_index: 0,
                text: "a".into(),
            },
            Delta::Text {
                output_index: 0,
                text: "b".into(),
            },
            Delta::PartDone {
                output_index: 0,
                part: Part::Text { text: "ab".into() },
            },
            Delta::Completed(completed),
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
