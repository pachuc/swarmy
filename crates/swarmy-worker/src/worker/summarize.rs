use std::fmt::Write as _;

use super::{
    Context, Event, HeldLease, InferenceJob, MessageId, Nudge, RequestId, Result, SessionId,
    SessionRecord, Snapshot, Timestamp, Ulid, WorkQueue, Worker, runnable_partition,
};

impl Worker {
    pub(super) async fn summary_completed(
        &self,
        session: &SessionRecord,
        events: &[Event],
    ) -> Result<bool> {
        if !matches!(session.kind, swarmy_core::SessionKind::Named { .. }) {
            return Ok(false);
        }
        let Some(request_id) = events
            .iter()
            .rev()
            .find(|event| {
                matches!(
                    event,
                    Event::InferenceCompleted { .. } | Event::InferenceFailed { .. }
                )
            })
            .and_then(|event| match event {
                Event::InferenceCompleted { request_id, .. }
                | Event::InferenceFailed {
                    request_id,
                    retryable: false,
                    ..
                } => Some(*request_id),
                _ => None,
            })
        else {
            return Ok(false);
        };
        Ok(self
            .store
            .get_inference_input::<InferenceJob>(request_id)
            .await?
            .is_some_and(|job| job.summary))
    }

    /// Pi agent-session.ts:2583-2696 allows one compact-and-retry after overflow
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
                // Pi agent-session.ts:2671-2690 fails the second truncated
                // attempt rather than accepting its partial assistant reply.
                let notice = swarmy_core::Message {
                    id: MessageId::from_ulid(Ulid::generate()),
                    role: swarmy_core::MessageRole::System,
                    parts: vec![swarmy_core::Part::Text {
                        text: "Truncated response recovery failed".into(),
                    }],
                };
                self.append(
                    session,
                    lease,
                    events,
                    &[Event::MessageAppended {
                        seq: 0,
                        message: notice,
                    }],
                )
                .await?;
                self.finish_failed_recovery(session, lease, snapshot, events, turn)
                    .await?;
                return Ok(true);
            }
            return Ok(false);
        }
        self.issue_summary(session, lease, snapshot, events, &job, (&[], true))
            .await
    }

    async fn recovery_already_attempted(&self, session: &SessionRecord) -> Result<bool> {
        // The worker's event tail may start after a snapshot. Read the whole
        // current session only on recovery, so a later user message is not
        // mistaken for part of the original retried turn.
        let events = self.tail(session.session_id, 0, session.head_seq).await?;
        // Pi agent-session.ts:901,952 resets the one-shot recovery guard on
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
            for event in prior.iter().rev() {
                match event {
                    Event::InferenceFailed {
                        failure_kind: swarmy_core::FailureKind::ContextOverflow,
                        ..
                    } => {
                        tracing::warn!(session_id = %session.session_id, "context recovery failed after one attempt");
                        return Ok(true);
                    }
                    Event::InferenceCompleted { request_id, .. } => {
                        if let Some(prior_job) = self
                            .store
                            .get_inference_input::<InferenceJob>(*request_id)
                            .await?
                        {
                            if prior_job.summary {
                                continue;
                            }
                            if let Some(Ok(response)) = self
                                .store
                                .get_inference_result::<Result<swarmy_llm::Response, String>>(
                                    *request_id,
                                )
                                .await?
                                && self.recoverable_length(&prior_job, &response)
                            {
                                tracing::warn!(session_id = %session.session_id, "length-stop recovery failed after one attempt");
                                return Ok(true);
                            }
                        }
                        break;
                    }
                    _ => {}
                }
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

    /// Mid-turn checks use event usage first, avoiding store reads on every
    /// tool round. Turn-end checks also handle the summary archive path.
    pub(super) async fn summarize(
        &self,
        session: &mut SessionRecord,
        lease: &HeldLease,
        snapshot: &Snapshot,
        events: &mut [Event],
        mid_turn: Option<&Event>,
    ) -> Result<bool> {
        if !matches!(session.kind, swarmy_core::SessionKind::Named { .. }) {
            return Ok(false);
        }
        if mid_turn.is_some() {
            let Some((mut provider, mut model, input)) = last_side_usage(events) else {
                return Ok(false);
            };
            if provider.is_empty() {
                provider = session
                    .inference
                    .provider
                    .clone()
                    .unwrap_or_else(|| self.config.provider.clone());
            }
            if model.is_empty() {
                model = session
                    .inference
                    .model
                    .clone()
                    .unwrap_or_else(|| self.config.harness.settings.model.clone());
            }
            if input < self.config.side_summarization_threshold(&provider, &model) {
                return Ok(false);
            }
        }
        let is_main = match self.store.get_agent(session.agent_id).await? {
            Some(agent) => agent.main_session == Some(session.session_id),
            None if mid_turn.is_some() => false,
            None => return Ok(false),
        };
        let Some((_request_id, job, response, message)) = self.last_inference(events).await? else {
            return Ok(false);
        };
        if job.summary {
            if mid_turn.is_some() {
                return Ok(false);
            }
            return self
                .finish_checkpoint(
                    session,
                    lease,
                    snapshot,
                    events,
                    &job,
                    (&response, message.as_ref()),
                )
                .await;
        }
        let provider = self.job_provider(&job);
        let model = &job.request.settings.model;
        if is_main {
            let tokens = response
                .usage
                .input_tokens
                .saturating_add(response.usage.output_tokens);
            if self
                .config
                .summarization_threshold(provider, model)
                .is_none_or(|threshold| tokens < threshold)
            {
                return Ok(false);
            }
        } else {
            // The stored job may reflect a newer agent model than the event
            // used to gate the mid-turn fast path.
            let threshold = self.config.side_summarization_threshold(provider, model);
            let input = response.usage.input_tokens;
            if input < threshold {
                return Ok(false);
            }
        }
        self.issue_summary(
            session,
            lease,
            snapshot,
            events,
            &job,
            (mid_turn.map_or(&[][..], std::slice::from_ref), false),
        )
        .await
    }

    async fn finish_checkpoint(
        &self,
        session: &mut SessionRecord,
        lease: &HeldLease,
        snapshot: &Snapshot,
        events: &[Event],
        job: &InferenceJob,
        result: (&swarmy_llm::Response, Option<&swarmy_core::Message>),
    ) -> Result<bool> {
        let (response, message) = result;
        if !job.summary_prefix {
            let history = self
                .compaction_history(snapshot, events, job.summary_recovery)
                .await?;
            let cut = job
                .summary_cut
                .map_or_else(
                    || raw_cut(&history),
                    |cut| usize::try_from(cut).unwrap_or(usize::MAX),
                )
                .min(history.len());
            if let Some(start) = split_turn_start(&history, cut) {
                if !message.as_ref().is_some_and(|message| {
                    valid_summary(&summary_text(message), message, Some(&Ok(response.clone())))
                }) {
                    return Ok(false);
                }
                let request = prefix_summary_request(
                    &self.config,
                    self.job_provider(job),
                    &history[start..cut],
                    job.request.settings.clone(),
                );
                if !summary_fits(&self.config, &request, self.job_provider(job)) {
                    return Ok(false);
                }
                self.build_inference_with_prefix(
                    session,
                    lease,
                    &[],
                    request,
                    true,
                    Some((cut, job.summary_recovery)),
                )
                .await?;
                return Ok(true);
            }
        }
        self.archive_summary(session, lease, snapshot, job, message, events)
            .await
    }

    /// Last stored inference input and output for the session tail.
    pub(super) async fn last_inference(
        &self,
        events: &[Event],
    ) -> Result<
        Option<(
            RequestId,
            InferenceJob,
            swarmy_llm::Response,
            Option<swarmy_core::Message>,
        )>,
    > {
        let Some((request_id, message)) = events.iter().rev().find_map(|event| match event {
            Event::InferenceCompleted {
                request_id,
                message,
                ..
            } => Some((*request_id, Some(message.clone()))),
            Event::InferenceFailed { request_id, .. } => Some((*request_id, None)),
            _ => None,
        }) else {
            return Ok(None);
        };
        let Some(job) = self
            .store
            .get_inference_input::<InferenceJob>(request_id)
            .await?
        else {
            return Ok(None);
        };
        let Some(Ok(response)) = self
            .store
            .get_inference_result::<Result<swarmy_llm::Response, String>>(request_id)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some((request_id, job, response, message)))
    }

    /// Build the summary inference for the replayed history plus any
    /// preceding events (mid-turn folded tool results), and submit it with
    /// those events in one transaction.
    pub(super) async fn issue_summary(
        &self,
        session: &mut SessionRecord,
        lease: &HeldLease,
        snapshot: &Snapshot,
        events: &[Event],
        job: &InferenceJob,
        context: (&[Event], bool),
    ) -> Result<bool> {
        let (preceding, recovery) = context;
        let mut history = snapshot.replay(events).messages().to_vec();
        for event in preceding {
            if let Event::MessageAppended { message, .. } = event {
                history.push(message.clone());
            }
        }
        // A failed attempt is not part of the context that Pi retries.
        if recovery
            && let Some(Event::InferenceCompleted { message, .. }) =
                events.iter().rev().find(|event| {
                    matches!(
                        event,
                        Event::InferenceCompleted { .. } | Event::InferenceFailed { .. }
                    )
                })
        {
            history.retain(|kept| kept.id != message.id);
        }
        let cut = raw_cut(&history);
        if cut == 0 {
            return Ok(false);
        }
        let split_start = split_turn_start(&history, cut);
        let request = if let Some(start) = split_start {
            if start == 0 || (start == 1 && previous_summary(&history).is_some()) {
                prefix_summary_request(
                    &self.config,
                    self.job_provider(job),
                    &history[start..cut],
                    job.request.settings.clone(),
                )
            } else {
                summary_request(
                    &self.config,
                    self.job_provider(job),
                    &history[..start],
                    job.request.settings.clone(),
                )
            }
        } else {
            summary_request(
                &self.config,
                self.job_provider(job),
                &history[..cut],
                job.request.settings.clone(),
            )
        };
        if !summary_fits(&self.config, &request, self.job_provider(job)) {
            tracing::warn!(
                session_id = %session.session_id,
                "summary prompt exceeds the model window; retaining current session"
            );
            return Ok(false);
        }
        self.build_inference_with_prefix(
            session,
            lease,
            preceding,
            request,
            split_start.is_some_and(|start| {
                start == 0 || (start == 1 && previous_summary(&history).is_some())
            }),
            Some((cut, recovery)),
        )
        .await?;
        Ok(true)
    }

    pub(super) async fn archive_summary(
        &self,
        session: &SessionRecord,
        lease: &HeldLease,
        snapshot: &Snapshot,
        job: &InferenceJob,
        message: Option<&swarmy_core::Message>,
        events: &[Event],
    ) -> Result<bool> {
        let Some(message) = message else {
            return Ok(false);
        };
        let text = summary_text(message);
        let is_main = self
            .store
            .get_agent(session.agent_id)
            .await?
            .is_some_and(|agent| agent.main_session == Some(session.session_id));
        let response = if let Some(id) = events.iter().rev().find_map(|event| match event {
            Event::InferenceCompleted { request_id, .. } => Some(*request_id),
            _ => None,
        }) {
            self.store
                .get_inference_result::<Result<swarmy_llm::Response, String>>(id)
                .await?
        } else {
            None
        };
        let valid = valid_summary(&text, message, response.as_ref());
        if !valid {
            tracing::warn!(session_id = %session.session_id, "invalid or truncated summary; retaining current session");
            return Ok(false);
        }
        let (text, history) = self
            .archived_history(snapshot, events, job, message, text)
            .await?;
        let cut = job
            .summary_cut
            .map_or_else(
                || raw_cut(&history),
                |cut| usize::try_from(cut).unwrap_or(usize::MAX),
            )
            .min(history.len());
        let recovery = job.summary_recovery;
        let file_lists = file_lists(&history[..cut]);
        let tail = history[cut..].to_vec();
        let opening = swarmy_core::Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role: swarmy_core::MessageRole::User,
            parts: vec![swarmy_core::Part::Text {
                text: format!(
                    "{}{text}{file_lists}{}",
                    swarmy_harness::COMPACTION_SUMMARY_PREFIX,
                    swarmy_harness::COMPACTION_SUMMARY_SUFFIX
                ),
            }],
        };
        let should_wake = tail
            .last()
            .is_some_and(|last| last.role == swarmy_core::MessageRole::Tool)
            || recovery;
        let mut token = lease.lock().await;
        let (successor, archived) = if is_main {
            self.store
                .summarize_main_session(
                    session.session_id,
                    session.head_seq,
                    token.as_ref().context("lease released")?,
                    &opening,
                    &tail,
                )
                .await?
        } else {
            // Carry recent tool rounds forward so the successor keeps
            // immediate context alongside the summary; the next request stays
            // small. A mid-task rollover replays the retained tool results
            // directly, without a synthetic user instruction.
            let (successor, archived_event) = self
                .store
                .summarize_side_session(
                    session.session_id,
                    session.head_seq,
                    token.as_ref().context("lease released")?,
                    &opening,
                    &tail,
                )
                .await?;
            token.release();
            self.publish_events(session.session_id, &[archived_event])
                .await?;
            // The successor shares the old session's runnable partition; wake
            // it so a mid-task rollover continues without waiting for input.
            // A chat-shaped successor replays to end-of-turn and idles again.
            self.wake_successor(session.session_id, successor).await;
            return Ok(true);
        };
        token.release();
        self.publish_events(session.session_id, &[archived]).await?;
        if should_wake {
            self.wake_successor(session.session_id, successor).await;
        }
        Ok(true)
    }
    async fn archived_history(
        &self,
        snapshot: &Snapshot,
        events: &[Event],
        job: &InferenceJob,
        message: &swarmy_core::Message,
        mut text: String,
    ) -> Result<(String, Vec<swarmy_core::Message>)> {
        let mut history = snapshot.replay(events).messages().to_vec();
        history.retain(|kept| kept.id != message.id);
        if job.summary_prefix {
            let prior = events.iter().rev().find_map(|event| match event {
                Event::InferenceCompleted {
                    request_id,
                    message,
                    ..
                } if *request_id != job.request_id => Some((*request_id, message)),
                _ => None,
            });
            let history_text = if let Some((request_id, prior_message)) = prior {
                if self
                    .store
                    .get_inference_input::<InferenceJob>(request_id)
                    .await?
                    .is_some_and(|prior_job| prior_job.summary)
                {
                    history.retain(|kept| kept.id != prior_message.id);
                    summary_text(prior_message)
                } else {
                    previous_summary(&history)
                        .unwrap_or("No prior history.")
                        .to_owned()
                }
            } else {
                previous_summary(&history)
                    .unwrap_or("No prior history.")
                    .to_owned()
            };
            text = format!("{history_text}\n\n---\n\n**Turn Context (split turn):**\n\n{text}");
        }
        history = self
            .compaction_history(snapshot, events, job.summary_recovery)
            .await?;
        Ok((text, history))
    }

    /// Compute the cut input once from the replay, excluding checkpoint replies
    /// and the abandoned length-stopped reply. All phases use this same view.
    async fn compaction_history(
        &self,
        snapshot: &Snapshot,
        events: &[Event],
        recovery: bool,
    ) -> Result<Vec<swarmy_core::Message>> {
        let mut history = snapshot.replay(events).messages().to_vec();
        let completions = events
            .iter()
            .filter_map(|event| match event {
                Event::InferenceCompleted {
                    request_id,
                    message,
                    ..
                } => Some((*request_id, message)),
                _ => None,
            })
            .collect::<Vec<_>>();
        let mut first_summary = None;
        for (index, (request_id, message)) in completions.iter().enumerate() {
            if self
                .store
                .get_inference_input::<InferenceJob>(*request_id)
                .await?
                .is_some_and(|job| job.summary)
            {
                first_summary.get_or_insert(index);
                history.retain(|kept| kept.id != message.id);
            }
        }
        if recovery
            && let Some(index) = first_summary
            && index > 0
        {
            // A length-stopped reply immediately before the checkpoint is not
            // replayed. An overflow has no completed assistant reply here.
            let (request_id, message) = completions[index - 1];
            let first_summary_seq = events.iter().find_map(|event| match event {
                Event::InferenceRequested {
                    seq,
                    request_id: id,
                    ..
                } if *id == completions[index].0 => Some(*seq),
                _ => None,
            });
            if first_summary_seq.is_some_and(|seq| events.iter().rev().find(|event| event.seq() < seq && matches!(event, Event::InferenceCompleted { .. } | Event::InferenceFailed { .. })).is_some_and(|event| matches!(event, Event::InferenceCompleted { request_id: id, .. } if *id == request_id)))
            {
                history.retain(|kept| kept.id != message.id);
            }
        }
        Ok(history)
    }

    /// Mark the successor runnable before nudging: idle nudges are dropped.
    pub(super) async fn wake_successor(&self, previous: SessionId, successor: SessionId) {
        if let Err(error) = self.store.wake_session(successor, Timestamp::now()).await {
            tracing::warn!(
                session_id = %previous,
                successor_id = %successor,
                %error,
                "successor wake failed; the scheduler scan still picks it up"
            );
            return;
        }
        if let Err(error) = self
            .bus
            .publish_work(
                &WorkQueue::Runnable(runnable_partition(successor)),
                &Nudge {
                    session_id: successor,
                },
            )
            .await
        {
            tracing::warn!(
                session_id = %previous,
                successor_id = %successor,
                %error,
                "successor wake failed; the scheduler scan still picks it up"
            );
        }
    }
}
fn summary_text(message: &swarmy_core::Message) -> String {
    message
        .parts
        .iter()
        .filter_map(|part| match part {
            swarmy_core::Part::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

fn raw_cut(history: &[swarmy_core::Message]) -> usize {
    select_side_tail(history)
        .first()
        .and_then(|first| history.iter().position(|message| message.id == first.id))
        .unwrap_or(history.len())
}

/// Pi compaction.ts:453-508 splits when the active turn began before the cut.
fn split_turn_start(history: &[swarmy_core::Message], cut: usize) -> Option<usize> {
    if cut >= history.len() || history[cut].role == swarmy_core::MessageRole::User {
        return None;
    }
    history[..cut].iter().rposition(|message| {
        message.role == swarmy_core::MessageRole::User
            && !message.parts.iter().any(|part| matches!(part, swarmy_core::Part::Text { text } if text.starts_with(swarmy_harness::COMPACTION_SUMMARY_PREFIX)))
    })
}

fn previous_summary(history: &[swarmy_core::Message]) -> Option<&str> {
    history.first()?.parts.first().and_then(|part| match part {
        swarmy_core::Part::Text { text } => text
            .strip_prefix(swarmy_harness::COMPACTION_SUMMARY_PREFIX)
            .and_then(|text| text.strip_suffix(swarmy_harness::COMPACTION_SUMMARY_SUFFIX)),
        _ => None,
    })
}

/// Pi utils.ts:607-622 rejects failed or length-stopped checkpoints.
fn valid_summary(
    text: &str,
    message: &swarmy_core::Message,
    response: Option<&Result<swarmy_llm::Response, String>>,
) -> bool {
    !text.trim().is_empty()
        && !message
            .parts
            .iter()
            .any(|part| matches!(part, swarmy_core::Part::ToolCall { .. }))
        && response.is_some_and(|response| {
            response.as_ref().is_ok_and(|response| {
                response.stop_reason != swarmy_llm::StopReason::MaxOutputTokens
            })
        })
}

/// Recent context retained in a side successor, in tokens.
const SIDE_TAIL_BUDGET_TOKENS: u64 = 20_000;

const SUMMARY_RESERVE_TOKENS: u64 = 16_384;

/// Rough token estimate for one message, chars divided by four like the Pi
/// and `OpenCode` heuristics. Images count as a fixed 4,800 chars.
/// Most recent completion usage, read from the in-memory event tail.
pub(super) fn last_side_usage(events: &[Event]) -> Option<(String, String, u64)> {
    events.iter().rev().find_map(|event| match event {
        Event::InferenceCompleted {
            provider,
            model,
            usage,
            ..
        } => Some((provider.clone(), model.clone(), usage.input_tokens)),
        _ => None,
    })
}

pub(super) fn estimate_message_tokens(message: &swarmy_core::Message) -> u64 {
    let mut chars = 0;
    for part in &message.parts {
        chars += match part {
            swarmy_core::Part::Text { text }
            | swarmy_core::Part::Reasoning { text, .. }
            | swarmy_core::Part::Notice { text, .. } => text.len(),
            swarmy_core::Part::ToolCall { tool, input, .. } => {
                tool.len() + serde_json::to_string(input).map_or(0, |json| json.len())
            }
            swarmy_core::Part::ToolResult { result, .. } => match result {
                swarmy_core::ToolResult::Completed { output, .. } => output.len(),
                swarmy_core::ToolResult::Error { error } => error.len(),
            },
            swarmy_core::Part::Image { .. } => 4_800,
        };
    }
    chars.div_ceil(4) as u64
}

/// Keep complete tool rounds in the side successor tail, even when the last
/// round alone exceeds the budget.
pub(super) fn select_side_tail(messages: &[swarmy_core::Message]) -> Vec<swarmy_core::Message> {
    use swarmy_core::MessageRole::Assistant;
    if messages.is_empty() {
        return Vec::new();
    }
    let mut total = 0;
    let mut start = messages.len();
    for (index, message) in messages.iter().enumerate().rev() {
        total += estimate_message_tokens(message);
        start = index;
        if total >= SIDE_TAIL_BUDGET_TOKENS {
            break;
        }
    }
    // Snap the cut forward to a safe boundary: the start, or just after a
    // tool-result message before an assistant message.
    let mut cut = start;
    while cut < messages.len() && !is_safe_tail_cut(messages, cut) {
        cut += 1;
    }
    // The budget bounds what comes before the last round, never the round
    // itself: a single oversized tool result still travels with its assistant
    // call instead of orphaning the call with a synthetic error.
    let round = last_tool_round(messages);
    if cut >= messages.len() {
        if let Some(round) = round {
            return messages[round..].to_vec();
        }
        return messages
            .iter()
            .rev()
            .find(|message| message.role == Assistant)
            .cloned()
            .map_or_else(Vec::new, |message| vec![message]);
    }
    if let Some(round) = round
        && cut > round
    {
        cut = round;
    }
    messages[cut..].to_vec()
}

/// Pi compaction.ts:453-508 cuts at user or assistant messages, never at
/// tool results. A tool call stays attached to the result that follows it.
pub(super) fn is_safe_tail_cut(messages: &[swarmy_core::Message], cut: usize) -> bool {
    use swarmy_core::MessageRole::{Assistant, User};
    cut == 0 || matches!(messages[cut].role, Assistant | User)
}

/// Start of the last complete tool round, if any: the latest assistant
/// message with tool calls that has tool results after it.
pub(super) fn last_tool_round(messages: &[swarmy_core::Message]) -> Option<usize> {
    use swarmy_core::MessageRole::Tool;
    let round = messages.iter().rposition(|message| {
        message
            .parts
            .iter()
            .any(|part| matches!(part, swarmy_core::Part::ToolCall { .. }))
    })?;
    messages
        .iter()
        .skip(round + 1)
        .any(|message| message.role == Tool)
        .then_some(round)
}

/// Summary request with bounded output: the smaller of the model's output
/// limit and the structured-summary cap, so the request fits providers that
/// reject oversized max-token values.
pub(super) fn summary_request(
    config: &crate::config::Config,
    provider: &str,
    messages: &[swarmy_core::Message],
    mut settings: swarmy_llm::GenerationSettings,
) -> swarmy_llm::Request {
    settings.max_output_tokens = Some(
        config
            .catalog
            .model(provider, &settings.model)
            .and_then(|model| model.limit.output)
            .unwrap_or(u64::MAX)
            .min(SUMMARY_RESERVE_TOKENS * 4 / 5),
    );
    let prompt = summary_prompt(messages);
    swarmy_llm::Request {
        system_prompt: swarmy_harness::SUMMARIZATION_SYSTEM_PROMPT.into(),
        messages: vec![swarmy_core::Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role: swarmy_core::MessageRole::User,
            parts: vec![swarmy_core::Part::Text { text: prompt }],
        }],
        tools: Vec::new(),
        settings,
        no_cache: true,
    }
}

/// Pi compaction.ts:1101-1140 gives the turn prefix half the reserve.
fn prefix_summary_request(
    config: &crate::config::Config,
    provider: &str,
    messages: &[swarmy_core::Message],
    settings: swarmy_llm::GenerationSettings,
) -> swarmy_llm::Request {
    let mut request = summary_request(config, provider, messages, settings);
    request.settings.max_output_tokens = request
        .settings
        .max_output_tokens
        .map(|limit| limit.min(SUMMARY_RESERVE_TOKENS / 2));
    request.messages[0].parts = vec![swarmy_core::Part::Text {
        text: format!(
            "# Conversation\n{}\n\n# Instructions\n{}",
            serialize_conversation(messages),
            swarmy_harness::TURN_PREFIX_SUMMARIZATION_PROMPT
        ),
    }];
    request
}

fn summary_prompt(messages: &[swarmy_core::Message]) -> String {
    let previous = messages
        .first()
        .and_then(|message| message.parts.first())
        .and_then(|part| match part {
            swarmy_core::Part::Text { text } => text
                .strip_prefix(swarmy_harness::COMPACTION_SUMMARY_PREFIX)
                .and_then(|text| text.strip_suffix(swarmy_harness::COMPACTION_SUMMARY_SUFFIX))
                // Old successors began with a system message containing JSON.
                // Carry that checkpoint opaquely; never parse or rewrite it.
                .or_else(|| {
                    text.starts_with("Conversation summarized. Previous session:")
                        .then_some(text.as_str())
                }),
            _ => None,
        });
    let conversation = serialize_conversation(if previous.is_some() {
        &messages[1..]
    } else {
        messages
    });
    let mut prompt = format!("<conversation>\n{conversation}\n</conversation>\n\n");
    if let Some(previous) = previous {
        write!(
            prompt,
            "<previous-summary>\n{previous}\n</previous-summary>\n\n"
        )
        .expect("write to String");
    }
    prompt.push_str(if previous.is_some() {
        swarmy_harness::UPDATE_SUMMARIZATION_PROMPT
    } else {
        swarmy_harness::SUMMARIZATION_PROMPT
    });
    prompt
}

/// Pi utils.ts:109-149: serialize old context as data, never as live chat turns.
pub(super) fn serialize_conversation(messages: &[swarmy_core::Message]) -> String {
    use swarmy_core::{MessageRole, Part, ToolResult};
    let mut lines = Vec::new();
    for message in messages {
        let text = message
            .parts
            .iter()
            .filter_map(|part| match part {
                Part::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        match message.role {
            MessageRole::User if !text.is_empty() => {
                lines.push(format!("[User]: {}", text.join("")));
            }
            MessageRole::Assistant => {
                let thinking = message
                    .parts
                    .iter()
                    .filter_map(|part| match part {
                        Part::Reasoning { text, .. } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                if !thinking.is_empty() {
                    lines.push(format!("[Assistant thinking]: {}", thinking.join("\n")));
                }
                if message
                    .parts
                    .iter()
                    .any(|part| matches!(part, Part::Text { .. }))
                {
                    lines.push(format!("[Assistant]: {}", text.join("\n")));
                }
                let calls = message
                    .parts
                    .iter()
                    .filter_map(|part| match part {
                        Part::ToolCall { tool, input, .. } => Some(format!(
                            "{tool}({})",
                            input
                                .as_object()
                                .map(|args| args
                                    .iter()
                                    .map(|(k, v)| format!("{k}={v}"))
                                    .collect::<Vec<_>>()
                                    .join(", "))
                                .unwrap_or_default()
                        )),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                if !calls.is_empty() {
                    lines.push(format!("[Assistant tool calls]: {}", calls.join("; ")));
                }
            }
            MessageRole::Tool => {
                for part in &message.parts {
                    if let Part::ToolResult { result, .. } = part {
                        let output = match result {
                            ToolResult::Completed { output, .. } => output,
                            ToolResult::Error { error } => error,
                        };
                        let mut truncated: String = output.chars().take(2_000).collect();
                        let omitted = output.chars().count().saturating_sub(2_000);
                        if omitted > 0 {
                            write!(truncated, "\n\n[... {omitted} more characters truncated]")
                                .expect("write to String");
                        }
                        if !output.is_empty() {
                            lines.push(format!("[Tool result]: {truncated}"));
                        }
                    }
                }
            }
            _ => {}
        }
    }
    lines.join("\n\n")
}

/// Whether the summary request fits the model window. The estimate covers
/// the replayed messages plus the prompt template, reserving the bounded
/// output. Unknown windows skip the check; the early threshold keeps the
/// prompt small in practice.
pub(super) fn summary_fits(
    config: &crate::config::Config,
    request: &swarmy_llm::Request,
    provider: &str,
) -> bool {
    let Some(context) = config
        .catalog
        .model(provider, &request.settings.model)
        .map(|model| model.limit.context)
        .or(config.model_context_window_tokens)
    else {
        return true;
    };
    let output = request.settings.max_output_tokens.unwrap_or(0);
    let mut chars: u64 =
        u64::try_from(swarmy_harness::SUMMARIZATION_SYSTEM_PROMPT.len()).unwrap_or(u64::MAX);
    for message in &request.messages {
        chars = chars.saturating_add(estimate_message_tokens(message).saturating_mul(4));
    }
    let estimated = chars.div_ceil(4);
    estimated.saturating_add(output) <= context
}

/// Pi utils.ts:25-94 and compaction.ts:1079-1080 track explicit file-tool paths.
fn file_lists(messages: &[swarmy_core::Message]) -> String {
    use std::collections::BTreeSet;
    let (mut read, mut modified) = (BTreeSet::new(), BTreeSet::new());
    for message in messages {
        for part in &message.parts {
            match part {
                swarmy_core::Part::ToolCall { tool, input, .. } => {
                    if let Some(path) = input.get("path").and_then(serde_json::Value::as_str) {
                        match tool.as_str() {
                            "read" => {
                                read.insert(path.to_owned());
                            }
                            "write" | "edit" => {
                                modified.insert(path.to_owned());
                            }
                            _ => {}
                        }
                    }
                }
                swarmy_core::Part::Text { text }
                    if text.starts_with(swarmy_harness::COMPACTION_SUMMARY_PREFIX) =>
                {
                    for (tag, paths) in
                        [("read-files", &mut read), ("modified-files", &mut modified)]
                    {
                        if let Some(body) = text
                            .split_once(&format!("<{tag}>\n"))
                            .and_then(|(_, rest)| rest.split_once(&format!("\n</{tag}>")))
                        {
                            paths.extend(body.0.lines().map(str::to_owned));
                        }
                    }
                }
                _ => {}
            }
        }
    }
    read.retain(|path| !modified.contains(path));
    let mut result = String::new();
    for (tag, paths) in [("read-files", &read), ("modified-files", &modified)] {
        if !paths.is_empty() {
            write!(
                result,
                "\n\n<{tag}>\n{}\n</{tag}>",
                paths.iter().cloned().collect::<Vec<_>>().join("\n")
            )
            .expect("write to String");
        }
    }
    result
}

#[cfg(test)]
mod pi_compaction_tests {
    use super::*;
    use std::collections::BTreeMap;
    use swarmy_core::{Message, MessageRole, Part, ToolCallId};
    use swarmy_llm::{Response, StopReason};

    fn text(role: MessageRole, value: &str) -> Message {
        Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role,
            parts: vec![Part::Text { text: value.into() }],
        }
    }

    fn answer(stop_reason: StopReason) -> Response {
        Response {
            parts: vec![Part::Text {
                text: "## Goal\nFinish".into(),
            }],
            stop_reason,
            usage: swarmy_core::TokenUsage::default(),
            quota_remaining: BTreeMap::default(),
            quota_resets: BTreeMap::default(),
        }
    }

    #[test]
    fn rejects_incomplete_empty_and_tool_calling_summaries() {
        let message = text(MessageRole::Assistant, "## Goal\nFinish");
        assert!(!valid_summary(
            "## Goal\nFinish",
            &message,
            Some(&Ok(answer(StopReason::MaxOutputTokens)))
        ));
        assert!(!valid_summary(
            " \n ",
            &message,
            Some(&Ok(answer(StopReason::EndTurn)))
        ));
        assert!(!valid_summary(
            "## Goal\nFinish",
            &message,
            Some(&Err("provider failed".into()))
        ));
        assert!(!valid_summary("## Goal\nFinish", &message, None));
        let mut tool = message.clone();
        tool.parts.push(Part::ToolCall {
            call_id: ToolCallId("read-1".into()),
            tool: "read".into(),
            input: serde_json::json!({"path": "src/main.rs"}),
        });
        assert!(!valid_summary(
            "## Goal\nFinish",
            &tool,
            Some(&Ok(answer(StopReason::EndTurn)))
        ));
        assert!(valid_summary(
            "## Goal\nFinish",
            &message,
            Some(&Ok(answer(StopReason::EndTurn)))
        ));
    }

    #[test]
    fn serializes_old_messages_as_data_and_truncates_tool_results() {
        let mut assistant = text(MessageRole::Assistant, "Working");
        assistant.parts.push(Part::ToolCall {
            call_id: ToolCallId("read-1".into()),
            tool: "read".into(),
            input: serde_json::json!({"path": "src/main.rs"}),
        });
        let tool = Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role: MessageRole::Tool,
            parts: vec![Part::ToolResult {
                call_id: ToolCallId("read-1".into()),
                result: swarmy_core::ToolResult::Completed {
                    output: "x".repeat(2_100),
                    title: String::new(),
                    metadata: BTreeMap::default(),
                },
            }],
        };
        let serialized =
            serialize_conversation(&[text(MessageRole::User, "Start"), assistant, tool]);
        assert!(serialized.starts_with("[User]: Start\n\n[Assistant]: Working"));
        assert!(serialized.contains("[Assistant tool calls]: read(path=\"src/main.rs\")"));
        assert!(serialized.contains(&format!("[Tool result]: {}", "x".repeat(2_000))));
        assert!(!serialized.contains(&"x".repeat(2_001)));
        assert!(serialized.contains("[... 100 more characters truncated]"));
    }

    #[test]
    fn split_turn_gets_a_separate_half_reserve_prompt() {
        let mut history = vec![
            text(MessageRole::User, "Earlier turn"),
            text(MessageRole::Assistant, "Done"),
            text(MessageRole::User, "Keep working"),
        ];
        for _ in 0..35 {
            history.push(text(MessageRole::Assistant, &"x".repeat(3_000)));
        }
        let cut = raw_cut(&history);
        let start = split_turn_start(&history, cut).expect("cut must split the active turn");
        assert_eq!(start, 2);
        assert!(cut > start);
        let settings = swarmy_llm::GenerationSettings::default();
        let config = crate::config::Config::from_env().unwrap();
        let prefix = prefix_summary_request(&config, "fake", &history[start..cut], settings);
        assert_eq!(prefix.settings.max_output_tokens, Some(8_192));
        let Part::Text { text } = &prefix.messages[0].parts[0] else {
            panic!("prefix text")
        };
        assert!(text.starts_with("# Conversation\n[User]: Keep working"));
        assert!(text.contains(swarmy_harness::TURN_PREFIX_SUMMARIZATION_PROMPT));
        assert!(prefix.tools.is_empty() && prefix.no_cache);
    }

    #[test]
    fn oversized_turn_splits_at_normal_cut_during_recovery() {
        let mut history = vec![text(MessageRole::User, "Retry this request")];
        history.extend((0..35).map(|_| text(MessageRole::Assistant, &"x".repeat(3_000))));
        let cut = raw_cut(&history);
        assert!(cut > 0 && cut < history.len());
        assert_eq!(split_turn_start(&history, cut), Some(0));
    }

    #[test]
    fn split_start_is_relative_to_cut_not_latest_user() {
        let history = vec![
            text(MessageRole::User, "old turn"),
            text(MessageRole::Assistant, "first"),
            text(MessageRole::User, "new turn"),
            text(MessageRole::Assistant, "second"),
        ];
        assert_eq!(split_turn_start(&history, 1), Some(0));
        assert_eq!(split_turn_start(&history, 2), None);
        assert_eq!(split_turn_start(&history, 3), Some(2));
    }

    #[test]
    fn serialization_keeps_empty_assistant_text_and_joins_thinking() {
        let mut assistant = text(MessageRole::Assistant, "");
        assistant.parts.push(Part::Reasoning {
            text: "first".into(),
            metadata: BTreeMap::new(),
        });
        assistant.parts.push(Part::Reasoning {
            text: "second".into(),
            metadata: BTreeMap::new(),
        });
        let empty_result = Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role: MessageRole::Tool,
            parts: vec![Part::ToolResult {
                call_id: ToolCallId("empty".into()),
                result: swarmy_core::ToolResult::Completed {
                    output: String::new(),
                    title: String::new(),
                    metadata: BTreeMap::new(),
                },
            }],
        };
        assert_eq!(
            serialize_conversation(&[assistant, empty_result]),
            "[Assistant thinking]: first\nsecond\n\n[Assistant]: "
        );
    }

    #[test]
    fn update_prompt_carries_previous_checkpoint_without_replaying_it() {
        let previous = text(
            MessageRole::User,
            &format!(
                "{}## Goal\nOld{}",
                swarmy_harness::COMPACTION_SUMMARY_PREFIX,
                swarmy_harness::COMPACTION_SUMMARY_SUFFIX
            ),
        );
        let prompt = summary_prompt(&[previous, text(MessageRole::User, "New task")]);
        assert!(prompt.contains("<conversation>\n[User]: New task\n</conversation>"));
        assert!(prompt.contains("<previous-summary>\n## Goal\nOld\n</previous-summary>"));
        assert!(prompt.contains(swarmy_harness::UPDATE_SUMMARIZATION_PROMPT));
    }
}
