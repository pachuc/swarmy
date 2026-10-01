//! Gateway commit: retry routing and terminal persistence.

use std::time::Duration;

use crate::Result;
use jiff::Timestamp;
use swarmy_bus::{Bus, LiveFeed};
use swarmy_core::{CredentialEntryKind, Event, Message, MessageId, MessageRole};
use swarmy_llm::{InferenceJob, Response, cost::cost_micros};
use swarmy_store::{InferenceClaim, InferenceCompletion};
use tokio::time::sleep;
use tracing::warn;
use ulid::Ulid;

use crate::{
    attempt::{AttemptOutcome, Delivery, EffortChoice},
    dispatch::Gateway,
};

struct CompletionAttribution {
    entry: Option<String>,
    entry_kind: Option<CredentialEntryKind>,
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
    retry_at: Option<Timestamp>,
    result: &'a std::result::Result<Response, swarmy_llm::Error>,
    entry: Option<String>,
    route: Option<String>,
    route_step: Option<u32>,
}

impl Gateway {
    pub(crate) async fn retry_or_continue(
        &self,
        delivery: &Delivery<'_>,
        outcome: AttemptOutcome,
    ) -> Result<Option<AttemptOutcome>> {
        let AttemptOutcome {
            result,
            streamed,
            blocked,
            entry,
            entry_kind,
        } = outcome;
        let job = delivery.job;
        let turn = delivery.turn;
        match result {
            Ok(response) => Ok(Some(AttemptOutcome {
                result: Ok(response),
                streamed,
                blocked,
                entry,
                entry_kind,
            })),
            Err(error)
                if !blocked
                    && {
                        let class = error.classify();
                        !class.permanent && !class.retryable
                    }
                    // The worker, not the transport queue, owns the single
                    // compact-and-retry attempt for context overflow.
                    && !matches!(error, swarmy_llm::Error::ContextOverflow(_)) =>
            {
                // Recovery may republish the reference with a new stream sequence.
                // Its delivery count starts at one, so it cannot bound calls.
                let retry = self
                    .store
                    .record_inference_retry(delivery.claim, Timestamp::now())
                    .await?;
                if i64::from(retry.attempts) >= self.max_deliver {
                    warn!(error = %swarmy_core::error_chain(&error), attempts = retry.attempts, request_id = %job.request_id, "provider retries exhausted");
                    return Ok(Some(AttemptOutcome {
                        result: Err(error),
                        streamed,
                        blocked,
                        entry,
                        entry_kind,
                    }));
                }
                warn!(error = %swarmy_core::error_chain(&error), attempts = retry.attempts, request_id = %job.request_id, "provider failed; retrying");
                self.observe_wait(job, turn, swarmy_store::WaitKind::Retry);
                self.observe_wait(job, turn, swarmy_store::WaitKind::ProviderFailure);
                self.store.release_inference(delivery.claim).await?;
                delivery
                    .message
                    .negative_acknowledge(Some(retry.delay))
                    .await?;
                Ok(None)
            }
            Err(error) => Ok(Some(AttemptOutcome {
                result: Err(error),
                streamed,
                blocked,
                entry,
                entry_kind,
            })),
        }
    }

    pub(crate) async fn commit_terminal(
        &self,
        delivery: &Delivery<'_>,
        effort: &EffortChoice<'_>,
        outcome: AttemptOutcome,
    ) -> Result<()> {
        let AttemptOutcome {
            result,
            streamed,
            blocked,
            entry,
            entry_kind,
        } = outcome;
        let (class, retry_at) = self
            .record_breaker(
                delivery.provider,
                entry.as_deref(),
                delivery.job,
                &result,
                blocked,
            )
            .await?;
        if result.is_err() {
            let kind = if class.is_some_and(|class| class.rate_limited) {
                swarmy_store::WaitKind::RateLimit
            } else {
                swarmy_store::WaitKind::ProviderFailure
            };
            self.observe_wait(delivery.job, delivery.turn, kind);
            if class.is_some_and(|class| class.retryable) {
                self.observe_wait(delivery.job, delivery.turn, swarmy_store::WaitKind::Retry);
            }
        }
        let event = Self::terminal_event(&TerminalInput {
            job: delivery.job,
            provider: delivery.provider,
            model: effort.model,
            effort_used: effort.used,
            effort_requested: effort.requested,
            effort_clamped: effort.clamped,
            retryable: class.is_some_and(|class| class.retryable),
            retry_at,
            result: &result,
            entry: entry.clone(),
            route: delivery.job.route.clone(),
            route_step: Some(delivery.job.route_step),
        });
        let attribution = Self::attribution_for(&result, entry, entry_kind);
        self.touch_entry(delivery.provider, attribution.entry.as_deref())
            .await;
        // The terminal error text is the durable String-schema record the model resumes from; the store layer cannot name the provider error type.
        // ast-grep-ignore: no-stringified-errors
        let stored_result = result.map_err(|error| error.to_string());
        self.persist_response(
            delivery.job,
            delivery.claim,
            event.clone(),
            &stored_result,
            delivery.turn,
            attribution,
        )
        .await?;
        self.observe_terminal_metric(
            delivery.job,
            delivery.turn,
            delivery.provider,
            &event,
            streamed,
        );
        if stored_result.is_ok() {
            self.store
                .clear_inference_wait(delivery.job.session_id)
                .await?;
        }
        delivery.message.acknowledge().await?;
        Ok(())
    }

    fn attribution_for(
        result: &std::result::Result<Response, swarmy_llm::Error>,
        entry: Option<String>,
        entry_kind: Option<CredentialEntryKind>,
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
                seq: 0,
                request_id: input.job.request_id,
                completion: swarmy_core::InferenceCompletion {
                    message: Message {
                        id: MessageId::from_ulid(Ulid::generate()),
                        role: MessageRole::Assistant,
                        parts: response.parts.clone(),
                    },
                    provider: input.provider.to_owned(),
                    model: input.job.request.settings.model.clone(),
                    effort_used: input.effort_used,
                    usage: response.usage.clone(),
                    cost_micros: input
                        .model
                        .map_or(0, |model| cost_micros(&model.cost, &response.usage)),
                    effort_requested: input.effort_requested,
                    effort_clamped: input.effort_clamped,
                    entry: input.entry.clone(),
                    route: input.route.clone(),
                    route_step: input.route_step,
                },
            },
            Err(error) => Event::InferenceFailed {
                seq: 0,
                request_id: input.job.request_id,
                error: error.to_string(),
                retryable: input.retryable,
                retry_at: input.retry_at,
                failure_kind: if matches!(error, swarmy_llm::Error::ContextOverflow(_)) {
                    swarmy_core::FailureKind::ContextOverflow
                } else {
                    swarmy_core::FailureKind::Provider
                },
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
            warn!(error = %swarmy_core::error_chain(&error), "credential last-use update failed");
        }
    }

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
                entry_kind: attribution.entry_kind,
                quota_remaining: attribution.quota_remaining.clone(),
                quota_resets: attribution.quota_resets.clone(),
            };
            let snapshot = match self.terminal_snapshot(job, &completion, result).await {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    warn!(error = %swarmy_core::error_chain(&error), "retrying terminal snapshot upload");
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
                Err(swarmy_store::StoreError::Fence(swarmy_store::FenceError::StaleSequence {
                    actual,
                    ..
                })) => {
                    expected_head = actual;
                }
                Err(error) => {
                    warn!(error = %swarmy_core::error_chain(&error), "retrying terminal store update");
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
        Ok(())
    }

    fn side_summarization_threshold(&self, provider: &str, model: &str) -> u64 {
        self.summarize_at_tokens
            .or_else(|| {
                self.model_context_window_tokens
                    .map(|context| context.saturating_sub(16_384))
            })
            .or_else(|| self.providers.catalog.summarize_at(provider, model))
            .unwrap_or(u64::MAX)
    }

    async fn terminal_snapshot(
        &self,
        job: &InferenceJob,
        completion: &InferenceCompletion,
        result: &std::result::Result<Response, String>,
    ) -> Result<Option<swarmy_core::SnapshotRef>> {
        // The worker must validate and archive a summary before the turn idles.
        if job.summary {
            return Ok(None);
        }
        // Named sessions need a worker step to decide whether to summarize.
        // Main sessions always take the slow path. Side sessions take it only
        // once the completion reaches the compaction threshold, so text-only turns
        // below that keep the single-transaction fast path instead of paying
        // for a scheduler nudge, a worker lease, and a snapshot upload.
        if let Some(session) = self.store.fetch_session(job.session_id).await?
            && matches!(session.kind, swarmy_core::SessionKind::Named { .. })
        {
            let main = self
                .store
                .get_agent(session.agent_id)
                .await?
                .is_some_and(|agent| agent.main_session == Some(job.session_id));
            if main {
                return Ok(None);
            }
            let provider = if job.provider.is_empty() {
                self.default_provider.as_str()
            } else {
                job.provider.as_str()
            };
            let input = match &completion.event {
                Event::InferenceCompleted { completion, .. } => completion.usage.input_tokens,
                Event::InferenceFailed { .. } => return Ok(None),
                _ => 0,
            };
            if matches!(&completion.event, Event::InferenceCompleted { completion, .. } if completion.usage.output_tokens < self.providers.catalog.model(provider, &job.request.settings.model).and_then(|model| model.limit.output).unwrap_or(0) && matches!(result, Ok(response) if response.stop_reason == swarmy_llm::StopReason::MaxOutputTokens))
            {
                return Ok(None);
            }
            if input >= self.side_summarization_threshold(provider, &job.request.settings.model) {
                return Ok(None);
            }
        }
        // A concurrent log append is not in the immutable request. Let the
        // worker replay it instead of advancing a snapshot over unseen events.
        if completion.expected_head != job.step {
            return Ok(None);
        }
        let Event::InferenceCompleted {
            completion: fields, ..
        } = &completion.event
        else {
            return Ok(None);
        };
        if fields
            .message
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
                .ok_or(crate::Error::Internal("sequence overflow"))?,
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
        self.publish_completion(id, event).await;
        if let Some(snapshot) = snapshot {
            self.publish_snapshot_idle(id, turn, snapshot).await;
        } else {
            self.nudge_completion(id, event, turn).await;
        }
    }

    /// Publish the terminal event to the session feed. A failed publish only
    /// warns: live clients catch up from the stored log.
    async fn publish_completion(&self, id: swarmy_core::SessionId, event: &Event) {
        if let Err(error) = self
            .bus
            .publish_live(LiveFeed::SessionEvents(id), event)
            .await
        {
            warn!(error = %swarmy_core::error_chain(&error), "completion event publication failed; client will catch up");
        }
    }

    /// Publish the idle turn and state change for a snapshot completion. The
    /// idle anchor is the turn's wall time, recorded in the store as well as
    /// on the bus so a turn of any length keeps its duration even when the
    /// worker never sees this turn end.
    async fn publish_snapshot_idle(
        &self,
        id: swarmy_core::SessionId,
        turn: Option<MessageId>,
        snapshot: &swarmy_core::SnapshotRef,
    ) {
        if let Some(turn) = turn {
            let event = Bus::turn_event(id, turn, swarmy_core::TurnStage::Idle, None);
            self.bus.record_turn(&event).await;
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
            warn!(error = %swarmy_core::error_chain(&error), "idle event publication failed; client will catch up");
        }
    }

    /// Nudge the session so the scheduler picks up the completed turn. A
    /// failed nudge only warns: the scheduler's resend recovers the work.
    async fn nudge_completion(
        &self,
        id: swarmy_core::SessionId,
        event: &Event,
        turn: Option<MessageId>,
    ) {
        if let Err(error) = self
            .bus
            .nudge(id, event.seq(), turn, self.resend_interval, false)
            .await
        {
            warn!(error = %swarmy_core::error_chain(&error), "completion nudge failed; scheduler will recover");
        }
    }
}
