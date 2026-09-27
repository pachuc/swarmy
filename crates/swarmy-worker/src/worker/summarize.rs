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
            .is_some_and(|job| job.request.system_prompt == swarmy_harness::SUMMARY_PROMPT))
    }

    /// Mid-turn checks use event usage first, avoiding store reads on every
    /// tool round. Turn-end checks also handle the summary archive path.
    pub(super) async fn summarize(
        &self,
        session: &mut SessionRecord,
        lease: &HeldLease,
        snapshot: &Snapshot,
        events: &mut Vec<Event>,
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
            if input < self.config.side_pressure_threshold(&provider, &model) {
                return Ok(false);
            }
        }
        let is_main = match self.store.get_agent(session.agent_id).await? {
            Some(agent) => agent.main_session == Some(session.session_id),
            None if mid_turn.is_some() => false,
            None => return Ok(false),
        };
        if mid_turn.is_some() && is_main {
            return Ok(false);
        }
        let Some((_request_id, job, response, message)) = self.last_inference(events).await? else {
            return Ok(false);
        };
        if job.request.system_prompt == swarmy_harness::SUMMARY_PROMPT {
            if mid_turn.is_some() {
                return Ok(false);
            }
            return self
                .archive_summary(session, lease, snapshot, message.as_ref(), events)
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
                let pressure = self.config.side_pressure_threshold(provider, model);
                if input >= pressure && !pressure_warned(snapshot, events) {
                    self.emit_pressure(session, lease, events, input, threshold)
                        .await?;
                }
                return Ok(false);
            }
        }
        self.issue_summary(
            session,
            lease,
            snapshot,
            events,
            &job,
            mid_turn.map_or(&[][..], std::slice::from_ref),
        )
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
        preceding: &[Event],
    ) -> Result<bool> {
        let mut history = snapshot.replay(events).messages().to_vec();
        for event in preceding {
            if let Event::MessageAppended { message, .. } = event {
                history.push(message.clone());
            }
        }
        let request = summary_request(
            &self.config,
            self.job_provider(job),
            &history,
            job.request.settings.clone(),
        );
        if !summary_fits(&self.config, &request, self.job_provider(job)) {
            tracing::warn!(
                session_id = %session.session_id,
                "summary prompt exceeds the model window; retaining current session"
            );
            return Ok(false);
        }
        self.build_inference(session, lease, preceding, request)
            .await?;
        Ok(true)
    }

    /// Append a `context_pressure` warning when input usage passes 75
    /// percent of the side threshold, so fleet status sees it coming.
    /// Callers check `pressure_warned` first; the append itself is best
    /// effort and never fails the step.
    pub(super) async fn emit_pressure(
        &self,
        session: &mut SessionRecord,
        lease: &HeldLease,
        events: &mut Vec<Event>,
        input_tokens: u64,
        threshold: u64,
    ) -> Result<()> {
        let message = swarmy_core::Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role: swarmy_core::MessageRole::System,
            parts: vec![swarmy_core::Part::Notice {
                kind: swarmy_core::NoticeKind::ContextPressure,
                text: format!(
                    "context_pressure: input {input_tokens} tokens at 75 percent of the {threshold} token side-session threshold. Summarization will archive this session soon; push work to keep it safe."
                ),
            }],
        };
        if let Err(error) = self
            .append(
                session,
                lease,
                events,
                &[Event::MessageAppended { seq: 0, message }],
            )
            .await
        {
            tracing::warn!(
                session_id = %session.session_id,
                %error,
                "context_pressure append failed"
            );
            return Err(error);
        }
        Ok(())
    }

    pub(super) async fn archive_summary(
        &self,
        session: &SessionRecord,
        lease: &HeldLease,
        snapshot: &Snapshot,
        message: Option<&swarmy_core::Message>,
        events: &[Event],
    ) -> Result<bool> {
        let Some(message) = message else {
            return Ok(false);
        };
        let text: String = message
            .parts
            .iter()
            .filter_map(|part| match part {
                swarmy_core::Part::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        let is_main = self
            .store
            .get_agent(session.agent_id)
            .await?
            .is_some_and(|agent| agent.main_session == Some(session.session_id));
        let summary = if let Some(summary) = parse_summary(&text) {
            summary
        } else if !is_main && !text.trim().is_empty() {
            let stop_reason = if let Some(Event::InferenceCompleted { request_id, .. }) = events
                .iter()
                .rev()
                .find(|event| matches!(event, Event::InferenceCompleted { .. }))
            {
                self.store
                    .get_inference_result::<Result<swarmy_llm::Response, String>>(*request_id)
                    .await?
                    .and_then(std::result::Result::ok)
                    .map_or_else(
                        || "unknown".into(),
                        |response| format!("{:?}", response.stop_reason),
                    )
            } else {
                "unknown".into()
            };
            tracing::warn!(session_id = %session.session_id, %stop_reason, "unparseable summary reply; rolling over side session with raw summary");
            swarmy_core::ConversationSummary {
                goals: String::new(),
                state_of_work: text,
                open_questions: String::new(),
                facts_to_keep: String::new(),
            }
        } else {
            tracing::warn!(session_id = %session.session_id, reason = "empty or invalid summary reply", "retaining current session");
            return Ok(false);
        };
        let opening = swarmy_core::Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role: swarmy_core::MessageRole::System,
            parts: vec![swarmy_core::Part::Text {
                text: format!(
                    "Conversation summarized. Previous session: {}. Its full transcript remains readable.\n{}",
                    session.session_id,
                    serde_json::to_string(&summary)?
                ),
            }],
        };
        let mut token = lease.lock().await;
        let (_, archived) = if is_main {
            self.store
                .summarize_main_session(
                    session.session_id,
                    session.head_seq,
                    token.as_ref().context("lease released")?,
                    &opening,
                )
                .await?
        } else {
            // Carry recent tool rounds forward so the successor keeps
            // immediate context alongside the summary; the next request stays
            // small. A mid-task rollover ends with a synthetic continue note
            // so the turn continues in the successor with the same pending
            // user intent; a chat-shaped rollover waits for input.
            let history = snapshot.replay(events).messages().to_vec();
            let tail = Self::side_successor_tail(&history, message);
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
        Ok(true)
    }
    /// Retain complete tool rounds and continue a mid-task successor without
    /// losing tool results awaiting the next inference.
    pub(super) fn side_successor_tail(
        history: &[swarmy_core::Message],
        summary: &swarmy_core::Message,
    ) -> Vec<swarmy_core::Message> {
        let mut tail = select_side_tail(history);
        tail.retain(|kept| kept.id != summary.id);
        if history
            .iter()
            .rev()
            .find(|kept| kept.id != summary.id)
            .is_some_and(|previous| previous.role == swarmy_core::MessageRole::Tool)
        {
            tail.push(continue_message());
        }
        tail
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
/// Recent context retained in a side successor, in tokens.
const SIDE_TAIL_BUDGET_TOKENS: u64 = 20_000;

fn parse_summary(text: &str) -> Option<swarmy_core::ConversationSummary> {
    let text = text.trim();
    let text = text.strip_prefix("```").map_or(text, |fenced| {
        fenced.split_once('\n').map_or(fenced, |(_, body)| body)
    });
    let text = text.strip_suffix("```").unwrap_or(text).trim();
    let json = text.get(text.find('{')?..=text.rfind('}')?)?;
    let value: serde_json::Value = serde_json::from_str(json).ok()?;
    let object = value.as_object()?;
    let field = |name: &str| match object.get(name) {
        Some(serde_json::Value::String(text)) => text.clone(),
        Some(value) => value.to_string(),
        None => String::new(),
    };
    Some(swarmy_core::ConversationSummary {
        goals: field("goals"),
        state_of_work: field("state_of_work"),
        open_questions: field("open_questions"),
        facts_to_keep: field("facts_to_keep"),
    })
}

const SUMMARY_OUTPUT_TOKENS: u64 = 4_096;

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
    // A stale pressure warning belongs to the archived session; the successor
    // warns again on its own usage, so drop it from the carried tail.
    let messages: Vec<swarmy_core::Message> = messages
        .iter()
        .filter(|message| !is_pressure_warning(message))
        .cloned()
        .collect();
    let messages = messages.as_slice();
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
    // Keep at least the last complete tool round: the latest assistant
    // message carrying tool calls plus the tool results after it.
    if let Some(round) = round
        && cut > round
    {
        cut = round;
    }
    messages[cut..].to_vec()
}

/// A tail cut is safe at the start, or between a completed tool result and
/// the next assistant message. Cuts never land between a tool call and its
/// result, and never inside an assistant message that carries reasoning.
pub(super) fn is_safe_tail_cut(messages: &[swarmy_core::Message], cut: usize) -> bool {
    use swarmy_core::MessageRole::{Assistant, Tool};
    cut == 0 || (messages[cut - 1].role == Tool && messages[cut].role == Assistant)
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

/// Whether a message is a `context_pressure` warning.
pub(super) fn is_pressure_warning(message: &swarmy_core::Message) -> bool {
    message.role == swarmy_core::MessageRole::System
        && message.parts.iter().any(|part| match part {
            swarmy_core::Part::Notice {
                kind: swarmy_core::NoticeKind::ContextPressure,
                ..
            } => true,
            swarmy_core::Part::Text { text } => text.starts_with("context_pressure"),
            _ => false,
        })
}

/// Check the replayed snapshot as well as the event tail to avoid warning
/// again after the snapshot cursor advances.
pub(super) fn pressure_warned(snapshot: &Snapshot, events: &[Event]) -> bool {
    snapshot
        .replay(events)
        .messages()
        .iter()
        .any(is_pressure_warning)
}

/// Continue a mid-task successor from its summary and retained tail.
pub(super) fn continue_message() -> swarmy_core::Message {
    use swarmy_core::{MessageId, MessageRole, Part};
    swarmy_core::Message {
        id: MessageId::from_ulid(Ulid::generate()),
        role: MessageRole::User,
        parts: vec![Part::Text {
            text: "Continue the summarized task from the recent tool context above. Do the next steps, or finish with a concise status when nothing remains."
                .into(),
        }],
    }
}

/// Summary request with bounded output: the smaller of the model's output
/// limit and the structured-summary cap, so the request fits providers that
/// reject oversized max-token values.
pub(super) fn summary_request(
    config: &crate::config::Config,
    provider: &str,
    messages: &[swarmy_core::Message],
    settings: swarmy_llm::GenerationSettings,
) -> swarmy_llm::Request {
    let mut settings = settings;
    // Keep reasoning from eating the summary's output cap, but only lower an
    // effort the session already asked for: a model without reasoning must
    // not start receiving an effort parameter.
    if matches!(
        settings.reasoning_effort,
        Some(
            swarmy_llm::ReasoningEffort::Medium
                | swarmy_llm::ReasoningEffort::High
                | swarmy_llm::ReasoningEffort::Xhigh
                | swarmy_llm::ReasoningEffort::Max
        )
    ) {
        settings.reasoning_effort = Some(swarmy_llm::ReasoningEffort::Low);
    }
    let cap = config
        .catalog
        .model(provider, &settings.model)
        .and_then(|model| model.limit.output)
        .map_or(SUMMARY_OUTPUT_TOKENS, |limit| {
            limit.min(SUMMARY_OUTPUT_TOKENS)
        });
    settings.max_output_tokens = Some(cap);
    swarmy_llm::Request {
        system_prompt: swarmy_harness::SUMMARY_PROMPT.into(),
        messages: messages.to_vec(),
        tools: Vec::new(),
        settings,
    }
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
    let mut chars: u64 = u64::try_from(swarmy_harness::SUMMARY_PROMPT.len()).unwrap_or(u64::MAX);
    for message in &request.messages {
        chars = chars.saturating_add(estimate_message_tokens(message).saturating_mul(4));
    }
    let estimated = chars.div_ceil(4);
    estimated.saturating_add(output) <= context
}

#[cfg(test)]
mod summary_parse_tests {
    use super::parse_summary;

    #[test]
    fn accepts_fences_arrays_missing_fields_and_preambles() {
        let fenced =
            parse_summary("```json\n{\"goals\":[\"one\",\"two\"],\"state_of_work\":\"done\"}\n```")
                .unwrap();
        assert_eq!(fenced.goals, "[\"one\",\"two\"]");
        assert_eq!(fenced.open_questions, "");
        assert_eq!(
            parse_summary("Here is the summary: {\"goals\":\"work\"}")
                .unwrap()
                .goals,
            "work"
        );
    }

    #[test]
    fn rejects_truncated_json() {
        assert!(parse_summary("{\"goals\": [\"unfinished\"").is_none());
    }
}
