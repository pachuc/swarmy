//! Context-overflow recovery: one compact-and-retry after overflow or an
//! early length stop. This is separate from `summarize`, which archives
//! summaries and compacts history; the archived predecessor records the
//! recovery attempt.
//!
//! Pi references below pin to earendil-works/pi@8eb2bcc (formerly
//! badlogic/pi-mono) so the cited line numbers stay checkable.

use super::{
    Context, Event, HeldLease, InferenceJob, MessageId, Result, SessionRecord, Snapshot, Ulid,
    Worker,
};

impl Worker {
    /// Pi agent-session.ts@8eb2bcc:2583-2696 allows one compact-and-retry after overflow
    /// or an early length stop. The archived predecessor records the attempt.
    pub(super) async fn recover_context_overflow(
        &self,
        session: &mut SessionRecord,
        lease: &HeldLease,
        snapshot: &Snapshot,
        events: &mut Vec<Event>,
        turn: Option<MessageId>,
    ) -> Result<bool> {
        if !matches!(session.kind, swarmy_core::SessionKind::Named { .. }) {
            return Ok(false);
        }
        // A later user turn must not recover a stale failed inference.
        let Some(last) = events.iter().rev().find(|event| {
            matches!(
                event,
                Event::InferenceCompleted { .. } | Event::InferenceFailed { .. }
            )
        }) else {
            return Ok(false);
        };
        if let Some(last_seq) = events.iter().rev().find_map(|event| match event {
            Event::InferenceCompleted { seq, .. } | Event::InferenceFailed { seq, .. } => Some(*seq),
            _ => None,
        }) && events.iter().any(|event| matches!(event, Event::MessageAppended { seq, message } if *seq > last_seq && message.role == swarmy_core::MessageRole::User)) {
            return Ok(false);
        }
        let job = match last {
            Event::InferenceFailed {
                request_id,
                failure_kind: swarmy_core::FailureKind::ContextOverflow,
                retryable: false,
                ..
            } => {
                self.store
                    .get_inference_input::<InferenceJob>(*request_id)
                    .await?
            }
            Event::InferenceCompleted { request_id, .. } => {
                let Some(job) = self
                    .store
                    .get_inference_input::<InferenceJob>(*request_id)
                    .await?
                else {
                    return Ok(false);
                };
                let response = self
                    .store
                    .get_inference_result::<Result<swarmy_llm::Response, String>>(*request_id)
                    .await?;
                if !response.is_some_and(|response| {
                    response.is_ok_and(|response| self.recoverable_length(&job, &response))
                }) {
                    return Ok(false);
                }
                Some(job)
            }
            _ => return Ok(false),
        };
        let Some(job) = job else { return Ok(false) };
        if job.summary {
            return Ok(false);
        }
        let stopped_on_length = matches!(last, Event::InferenceCompleted { .. });
        if self.recovery_already_attempted(session).await? {
            if stopped_on_length {
                // Pi keeps the reply but fails its calls rather than executing
                // potentially incomplete arguments. Persist results before the
                // notice and idle snapshot so every prompt sees the same history.
                self.fail_truncated_calls(session, lease, events).await?;
                self.recovery_notice(
                    session,
                    lease,
                    snapshot,
                    events,
                    turn,
                    (
                        "Truncated response recovery failed after one compact-and-retry attempt.",
                        false,
                    ),
                )
                .await?;
                return Ok(true);
            }
            self.recovery_notice(session, lease, snapshot, events, turn,
                ("Context overflow recovery failed after one compact-and-retry attempt. Try reducing context or switching to a larger-context model.", false)).await?;
            return Ok(true);
        }
        if self
            .issue_summary(session, lease, snapshot, events, &job, (&[], true))
            .await?
        {
            return Ok(true);
        }
        // Pi omits the failed attempt before trying to compact. Even if no
        // head remains, never execute a truncated tool call or replay it.
        self.finish_failed_recovery(session, lease, snapshot, events, turn, true)
            .await?;
        Ok(true)
    }

    async fn fail_truncated_calls(
        &self,
        session: &mut SessionRecord,
        lease: &HeldLease,
        events: &mut Vec<Event>,
    ) -> Result<()> {
        let Some((seq, message)) = events.iter().rev().find_map(|event| match event {
            Event::InferenceCompleted {
                seq, completion, ..
            } => Some((*seq, completion.message.clone())),
            _ => None,
        }) else {
            return Ok(());
        };
        let already_recorded: Vec<_> = events
            .iter()
            .filter(|event| event.seq() > seq)
            .filter_map(|event| {
                if let Event::MessageAppended { message, .. } = event {
                    Some(message.parts.as_slice())
                } else {
                    None
                }
            })
            .flatten()
            .filter_map(|part| {
                if let swarmy_core::Part::ToolResult { call_id, .. } = part {
                    Some(call_id.clone())
                } else {
                    None
                }
            })
            .collect();
        let batch: Vec<_> = message.parts.iter().filter_map(|part| {
            if let swarmy_core::Part::ToolCall { call_id, tool, .. } = part
                && !already_recorded.contains(call_id)
            {
                Some(Event::MessageAppended {
                    seq: 0,
                    message: swarmy_core::Message {
                        id: MessageId::from_ulid(Ulid::generate()),
                        role: swarmy_core::MessageRole::Tool,
                        parts: vec![swarmy_core::Part::ToolResult {
                            call_id: call_id.clone(),
                            result: swarmy_core::ToolResult::Error {
                                error: format!("Tool call \"{tool}\" was not executed: the response hit the output token limit, so its arguments may be truncated. Re-issue the tool call with complete arguments."),
                            },
                        }],
                    },
                })
            } else {
                None
            }
        }).collect();
        if !batch.is_empty() {
            self.append(session, lease, events, &batch).await?;
        }
        Ok(())
    }

    async fn recovery_notice(
        &self,
        session: &mut SessionRecord,
        lease: &HeldLease,
        snapshot: &Snapshot,
        events: &mut Vec<Event>,
        turn: Option<MessageId>,
        notice: (&str, bool),
    ) -> Result<()> {
        let (text, omit_attempt) = notice;
        self.append(
            session,
            lease,
            events,
            &[Event::MessageAppended {
                seq: 0,
                message: swarmy_core::Message {
                    id: MessageId::from_ulid(Ulid::generate()),
                    role: swarmy_core::MessageRole::System,
                    parts: vec![swarmy_core::Part::Text { text: text.into() }],
                },
            }],
        )
        .await?;
        self.finish_failed_recovery(session, lease, snapshot, events, turn, omit_attempt)
            .await
    }

    async fn recovery_already_attempted(&self, session: &SessionRecord) -> Result<bool> {
        // The worker's event tail may start after a snapshot. Read the whole
        // current session only on recovery, so a later user message is not
        // mistaken for part of the original retried turn.
        let events = self.tail(session.session_id, 0, session.head_seq).await?;
        // Pi agent-session.ts@8eb2bcc:901,952 resets the one-shot recovery guard on
        // new input or a successful assistant reply, not on every rollover.
        // The successor starts with a user-role summary and may replay the
        // active user request. Neither is a new user turn. Only messages
        // appended after its first inference reset the recovery guard.
        let first_inference = events.iter().find_map(|event| match event {
            Event::InferenceRequested { seq, .. } => Some(*seq),
            _ => None,
        });
        let new_user = first_inference.is_some_and(|first| events.iter().any(|event| {
            matches!(event, Event::MessageAppended { seq, message }
                if *seq > first && message.role == swarmy_core::MessageRole::User
                    && !message.parts.iter().any(|part| matches!(part, swarmy_core::Part::Text { text } if text.starts_with(swarmy_harness::COMPACTION_SUMMARY_PREFIX))))
        }));
        let mut successful_reply = false;
        if !new_user {
            for event in events {
                if let Event::InferenceCompleted { request_id, .. } = event
                    && let Some(Ok(response)) = self
                        .store
                        .get_inference_result::<Result<swarmy_llm::Response, String>>(request_id)
                        .await?
                    && response.stop_reason != swarmy_llm::StopReason::MaxOutputTokens
                {
                    successful_reply = true;
                    break;
                }
            }
        }
        if !new_user
            && !successful_reply
            && let Some(previous) = self.store.previous_session(session.session_id).await?
        {
            let previous_record = self
                .store
                .fetch_session(previous)
                .await?
                .context("missing predecessor session")?;
            let prior = self.tail(previous, 0, previous_record.head_seq).await?;
            if let Some(request_id) = prior.iter().rev().find_map(|event| match event {
                Event::InferenceRequested { request_id, .. } => Some(*request_id),
                _ => None,
            }) {
                return Ok(self
                    .store
                    .get_inference_input::<InferenceJob>(request_id)
                    .await?
                    .is_some_and(|job| job.summary_recovery));
            }
        }
        Ok(false)
    }

    fn recoverable_length(&self, job: &InferenceJob, response: &swarmy_llm::Response) -> bool {
        response.stop_reason == swarmy_llm::StopReason::MaxOutputTokens
            && response.usage.output_tokens
                < self
                    .config
                    .catalog
                    .model(self.job_provider(job), &job.request.settings.model)
                    .and_then(|model| model.limit.output)
                    .unwrap_or(0)
    }
}
