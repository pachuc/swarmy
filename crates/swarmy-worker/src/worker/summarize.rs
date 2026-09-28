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
        events: &[Event],
    ) -> Result<bool> {
        if !matches!(session.kind, swarmy_core::SessionKind::Named { .. }) {
            return Ok(false);
        }
        let Some(last) = events.iter().rev().find(|event| {
            matches!(
                event,
                Event::InferenceCompleted { .. } | Event::InferenceFailed { .. }
            )
        }) else {
            return Ok(false);
        };
        let request_id = match last {
            Event::InferenceFailed {
                request_id,
                error,
                retryable: false,
                ..
            } if error.to_ascii_lowercase().contains("context overflow") => *request_id,
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
                    response.is_ok_and(|response| {
                        response.stop_reason == swarmy_llm::StopReason::MaxOutputTokens
                            && response.usage.output_tokens
                                < job
                                    .request
                                    .settings
                                    .max_output_tokens
                                    .or_else(|| {
                                        self.config
                                            .catalog
                                            .model(
                                                self.job_provider(&job),
                                                &job.request.settings.model,
                                            )
                                            .and_then(|model| model.limit.output)
                                    })
                                    .unwrap_or(0)
                    })
                }) {
                    return Ok(false);
                }
                *request_id
            }
            _ => return Ok(false),
        };
        let Some(job) = self
            .store
            .get_inference_input::<InferenceJob>(request_id)
            .await?
        else {
            return Ok(false);
        };
        if job.summary {
            return Ok(false);
        }
        if let Some(previous) = self.store.previous_session(session.session_id).await? {
            let prior = self.store.read_events(previous, 0, 10_000).await?;
            if prior.iter().any(|event| matches!(event, Event::InferenceFailed { error, .. } if error.to_ascii_lowercase().contains("context overflow"))) {
                tracing::warn!(session_id = %session.session_id, "context overflow recovery failed after one attempt");
                return Ok(false);
            }
        }
        self.issue_summary(session, lease, snapshot, events, &job, (&[], true))
            .await
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
        let tail = select_side_tail(&history);
        let cut = tail
            .first()
            .and_then(|first| history.iter().position(|message| message.id == first.id))
            .unwrap_or(history.len());
        let cut = if recovery { history.len() } else { cut };
        if cut == 0 {
            return Ok(false);
        }
        let request = summary_request(
            &self.config,
            self.job_provider(job),
            &history[..cut],
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
        let valid = !text.trim().is_empty()
            && !message
                .parts
                .iter()
                .any(|part| matches!(part, swarmy_core::Part::ToolCall { .. }))
            && if let Some(id) = events.iter().rev().find_map(|event| match event {
                Event::InferenceCompleted { request_id, .. } => Some(*request_id),
                _ => None,
            }) {
                self.store
                    .get_inference_result::<Result<swarmy_llm::Response, String>>(id)
                    .await?
                    .is_some_and(|response| {
                        response.is_ok_and(|response| {
                            response.stop_reason != swarmy_llm::StopReason::MaxOutputTokens
                        })
                    })
            } else {
                false
            };
        if !valid {
            tracing::warn!(session_id = %session.session_id, "invalid or truncated summary; retaining current session");
            return Ok(false);
        }
        let file_lists = file_lists(snapshot.replay(events).messages());
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
        let mut token = lease.lock().await;
        let (_, archived) = if is_main {
            self.store
                .summarize_main_session(
                    session.session_id,
                    session.head_seq,
                    token.as_ref().context("lease released")?,
                    &opening,
                    &Self::successor_tail(snapshot, events, message),
                )
                .await?
        } else {
            // Carry recent tool rounds forward so the successor keeps
            // immediate context alongside the summary; the next request stays
            // small. A mid-task rollover replays the retained tool results
            // directly, without a synthetic user instruction.
            let tail = Self::successor_tail(snapshot, events, message);
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
    fn successor_tail(
        snapshot: &Snapshot,
        events: &[Event],
        summary: &swarmy_core::Message,
    ) -> Vec<swarmy_core::Message> {
        // On overflow the old context cannot be replayed unchanged.
        if events.iter().any(|event| matches!(event, Event::InferenceFailed { error, .. } if error.to_ascii_lowercase().contains("context overflow"))) {
            return Vec::new();
        }
        Self::side_successor_tail(snapshot.replay(events).messages(), summary)
    }

    /// Retain complete tool rounds and continue a mid-task successor without
    /// losing tool results awaiting the next inference.
    pub(super) fn side_successor_tail(
        history: &[swarmy_core::Message],
        summary: &swarmy_core::Message,
    ) -> Vec<swarmy_core::Message> {
        let mut tail = select_side_tail(history);
        tail.retain(|kept| kept.id != summary.id);
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
    if cut == 0 && messages.len() > 1 && messages[1].role == Assistant {
        cut = 1;
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
    let previous = messages
        .first()
        .and_then(|message| message.parts.first())
        .and_then(|part| match part {
            swarmy_core::Part::Text { text } => text
                .strip_prefix(swarmy_harness::COMPACTION_SUMMARY_PREFIX)
                .and_then(|text| text.strip_suffix(swarmy_harness::COMPACTION_SUMMARY_SUFFIX)),
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
            .collect::<Vec<_>>()
            .join("\n");
        match message.role {
            MessageRole::User if !text.is_empty() => lines.push(format!("[User]: {text}")),
            MessageRole::Assistant => {
                for part in &message.parts {
                    if let Part::Reasoning { text, .. } = part {
                        lines.push(format!("[Assistant thinking]: {text}"));
                    }
                }
                if !text.is_empty() {
                    lines.push(format!("[Assistant]: {text}"));
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
                        let truncated: String = output.chars().take(2_000).collect();
                        lines.push(format!("[Tool result]: {truncated}"));
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
