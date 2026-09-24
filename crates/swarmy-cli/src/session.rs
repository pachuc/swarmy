use std::{collections::HashSet, io::Write, time::Duration};

use anyhow::{Context, Result, bail};
use swarmy_core::{Event, MessageId, MessageRole, Part, SessionId, SessionState, ToolResult};
use swarmy_llm::Delta;
use swarmy_store::MAX_SCAN_LIMIT;

use crate::conversation::{Conversation, Notification, Opened, TranscriptEvent, store};

pub use crate::session_command::Command;

pub async fn inspect(command: Command, json: bool) -> Result<()> {
    let store = store().await?;
    match command {
        Command::Interrupt { session_id } => {
            let id = SessionId::from_ulid(session_id);
            let result = store.interrupt_session(id).await?;
            if result == swarmy_store::InterruptResult::Finished {
                let session = store
                    .fetch_session(id)
                    .await?
                    .context("session not found")?;
                if let Some(event) = store.read_events(id, session.head_seq - 1, 1).await?.pop()
                    && let Ok(bus) = crate::conversation::bus().await
                {
                    let _ = bus
                        .publish_live(swarmy_bus::LiveFeed::SessionEvents(id), &event)
                        .await;
                }
            }
            let (status, message) = match result {
                swarmy_store::InterruptResult::Finished => {
                    ("finished", format!("Interrupted session {id}"))
                }
                swarmy_store::InterruptResult::Requested => {
                    ("requested", format!("Interrupt requested for session {id}"))
                }
            };
            crate::vol::output(
                &serde_json::json!({"event": "session_interrupt", "session_id": id, "result": status}),
                &message,
                json,
            )?;
        }
        Command::Close { session_id } => {
            let id = SessionId::from_ulid(session_id);
            store.close_session(id, jiff::Timestamp::now()).await?;
            crate::vol::output(
                &serde_json::json!({"event": "session_closed", "session_id": id}),
                &format!("Closed session {id}"),
                json,
            )?;
        }
        Command::Show { session_id } => {
            show_session(&store, SessionId::from_ulid(session_id), json).await?;
        }
        Command::List => {
            let mut after = None;
            loop {
                let sessions = store.list_sessions(after, MAX_SCAN_LIMIT).await?;
                if sessions.is_empty() {
                    break;
                }
                for session in sessions {
                    let agent = if matches!(session.kind, swarmy_core::SessionKind::Named { .. }) {
                        store.get_agent(session.agent_id).await?
                    } else {
                        None
                    };
                    let selection = crate::selection::resolved_session(&store, &session).await?;
                    let main = agent
                        .as_ref()
                        .is_some_and(|agent| agent.main_session == Some(session.session_id));
                    let name = agent.map(|agent| agent.name);
                    let mut value = serde_json::to_value(&session)?;
                    let successor = store.next_session(session.session_id).await?;
                    value["archived"] = successor.is_some().into();
                    value["next_session"] = serde_json::to_value(successor)?;
                    value["previous_session"] =
                        serde_json::to_value(store.previous_session(session.session_id).await?)?;
                    value["resolved_inference"] = serde_json::to_value(&selection)?;
                    value["main"] = main.into();
                    value["agent_name"] = serde_json::to_value(&name)?;
                    let kind = match session.kind {
                        swarmy_core::SessionKind::Ephemeral => "ephemeral",
                        swarmy_core::SessionKind::Named { .. } => "named",
                    };
                    crate::vol::output(
                        &value,
                        &format!(
                            "{} {:?} {}/{} kind={kind} agent={} head={} computer_deleted={} archived={} main={main}",
                            session.session_id,
                            session.state,
                            selection.provider,
                            selection.model,
                            name.as_deref().unwrap_or("-"),
                            session.head_seq,
                            session.computer_deleted,
                            successor.is_some()
                        ),
                        json,
                    )?;
                    after = Some(session.session_id);
                }
            }
        }
    }
    Ok(())
}

async fn show_session(store: &swarmy_store::Store, id: SessionId, json: bool) -> Result<()> {
    let session = store
        .fetch_session(id)
        .await?
        .context("session not found")?;
    let requirements = if let Some(agent) = store.get_agent(session.agent_id).await? {
        agent.requirements
    } else if let Some(image) = store.pinned_image(id).await? {
        swarmy_core::SandboxRequirements {
            memory_mib: store.image_memory(&image).await?.unwrap_or(768),
            gpu: swarmy_core::GpuRequirement::default(),
        }
    } else {
        swarmy_core::SandboxRequirements::default()
    };
    crate::vol::output(
        &serde_json::json!({"sandbox_requirements": requirements, "memory_limit_mib": requirements.memory_mib}),
        &format!(
            "Sandbox memory requirement and limit: {} MiB; GPU: {:?}",
            requirements.memory_mib, requirements.gpu
        ),
        json,
    )?;
    if let Some(placement) = store.get_by_agent(session.agent_id).await? {
        let address = store.placement_address(&placement).await?;
        crate::vol::output(
            &serde_json::json!({"placement": placement, "sandbox_address": address}),
            &format!(
                "Sandbox address: {}",
                address.map_or_else(|| "-".into(), |address| address.to_string())
            ),
            json,
        )?;
    } else {
        crate::vol::output(
            &serde_json::json!({"placement": null, "sandbox_status": "waiting_for_capacity_or_first_tool"}),
            "Sandbox not placed; pending tools wait for node memory capacity",
            json,
        )?;
    }
    show_selection(store, &session, json).await?;
    show_usage(&store.session_usage(id).await?, json);
    show_inference_wait(store, &session, json).await?;
    let mut after = 0;
    while after < session.head_seq {
        let events = store.read_events(id, after, MAX_SCAN_LIMIT).await?;
        if events.is_empty() {
            bail!("session log ended before its recorded head");
        }
        for event in events
            .iter()
            .take_while(|event| event.seq() <= session.head_seq)
        {
            if json {
                println!("{}", serde_json::to_string(event)?);
            } else {
                println!("{} {}", event.seq(), serde_json::to_string(event)?);
            }
            after = event.seq();
        }
    }
    Ok(())
}

async fn show_inference_wait(
    store: &swarmy_store::Store,
    session: &swarmy_core::SessionRecord,
    json: bool,
) -> Result<()> {
    if session.state == SessionState::Sleeping
        && let Some(wait) = store.inference_wait(session.session_id).await?
    {
        let value = serde_json::json!({"state": "waiting_for_inference", "wake_at": wait.wake_at, "reasons": wait.reasons});
        crate::vol::output(
            &value,
            &format!(
                "WaitingForInference until {}: {}",
                wait.wake_at,
                wait.reasons.join("; ")
            ),
            json,
        )?;
    }
    Ok(())
}

fn show_usage(totals: &swarmy_core::UsageTotals, json: bool) {
    if json {
        println!(
            "{}",
            serde_json::json!({"session_usage": totals, "cost_dollars": totals.dollars()})
        );
    } else {
        println!(
            "Usage: input={} cached={} cache_write={} output={} reasoning={} total={} cost=${}",
            totals.usage.input_tokens,
            totals.usage.cached_input_tokens,
            totals.usage.cache_write_input_tokens,
            totals.usage.output_tokens,
            totals.usage.reasoning_output_tokens,
            totals.usage.total_tokens,
            totals.dollars()
        );
    }
}

pub async fn run(
    prompt: String,
    image: Option<String>,
    agent: Option<String>,
    new: bool,
    selection: swarmy_core::InferenceSelection,
    json: bool,
) -> Result<()> {
    let mut conversation =
        Conversation::open(None, image.as_deref(), agent.as_deref(), new, selection).await?;
    announce(&conversation, json)?;
    conversation.send(prompt).await?;
    let outcome = until_idle(&mut conversation, json, true).await?;
    if json {
        match &outcome {
            RunOutcome::Completed => println!(
                "{}",
                serde_json::json!({"event": "run_outcome", "outcome": "completed"})
            ),
            RunOutcome::Failed(reason) => println!(
                "{}",
                serde_json::json!({"event": "run_outcome", "outcome": "failed", "reason": reason})
            ),
        }
    }
    match outcome {
        RunOutcome::Completed => Ok(()),
        RunOutcome::Failed(reason) => anyhow::bail!(reason.replace(['\r', '\n'], " ")),
    }
}

fn announce(conversation: &Conversation, json: bool) -> Result<()> {
    let event = match conversation.opened {
        Opened::Created => "session_created",
        Opened::Resumed => "session_opened",
    };
    if json {
        println!(
            "{}",
            serde_json::json!({"event": event, "session_id": conversation.id, "agent_name": conversation.agent_name})
        );
    } else {
        eprintln!("Session {}", conversation.id);
    }
    std::io::stdout().flush()?;
    Ok(())
}

/// JSON chat accepts one prompt per input line and emits the run event protocol.
pub async fn chat_json(
    id: Option<SessionId>,
    image: Option<String>,
    agent: Option<String>,
    new: bool,
    selection: swarmy_core::InferenceSelection,
) -> Result<()> {
    use tokio::io::AsyncBufReadExt;
    let mut conversation =
        Conversation::open(id, image.as_deref(), agent.as_deref(), new, selection).await?;
    announce(&conversation, true)?;
    until_idle(&mut conversation, true, false).await?;
    let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    while let Some(prompt) = lines.next_line().await? {
        conversation.send(prompt).await?;
        until_idle(&mut conversation, true, false).await?;
    }
    Ok(())
}

const WORKER_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Eq)]
enum RunOutcome {
    Completed,
    Failed(String),
}

#[derive(Default)]
struct TurnOutcome {
    worker_started: bool,
    assistant_reply: bool,
    failure: Option<String>,
}

impl TurnOutcome {
    fn record(&mut self, event: &Event) {
        match event {
            Event::MessageAppended { message, .. } if message.role == MessageRole::User => {
                *self = Self::default();
            }
            Event::StateChanged {
                to: SessionState::Leased,
                ..
            }
            | Event::InferenceRequested { .. } => self.worker_started = true,
            Event::InferenceFailed { error, .. } => self.failure = Some(error.clone()),
            Event::ToolCallCompleted {
                result: ToolResult::Error { error },
                ..
            } => {
                self.failure = Some(format!("tool failed: {error}"));
            }
            Event::InferenceCompleted { message, .. } | Event::MessageAppended { message, .. }
                if message.role == MessageRole::Assistant
                    && !message
                        .parts
                        .iter()
                        .any(|part| matches!(part, Part::ToolCall { .. })) =>
            {
                self.assistant_reply = true;
                self.failure = None;
            }
            _ => {}
        }
    }

    fn finish(self) -> RunOutcome {
        if let Some(reason) = self.failure {
            RunOutcome::Failed(reason)
        } else if self.assistant_reply {
            RunOutcome::Completed
        } else {
            RunOutcome::Failed("turn ended without a completed assistant reply".into())
        }
    }
}

async fn until_idle(
    conversation: &mut Conversation,
    json: bool,
    run_mode: bool,
) -> Result<RunOutcome> {
    let mut output = Output::new(json);
    output.report_errors = !run_mode;
    let mut idle_event = false;
    let mut outcome = TurnOutcome::default();
    let worker_deadline = tokio::time::Instant::now() + WORKER_WAIT_TIMEOUT;
    loop {
        let notification = if run_mode && !outcome.worker_started {
            if let Ok(result) = tokio::time::timeout_at(worker_deadline, conversation.next()).await
            {
                result?
            } else {
                output.finish_line();
                return Ok(RunOutcome::Failed(format!(
                    "worker did not pick up session {} within {} seconds",
                    conversation.id,
                    WORKER_WAIT_TIMEOUT.as_secs()
                )));
            }
        } else {
            conversation.next().await?
        };
        match notification {
            Notification::Log(event) => {
                outcome.record(&event);
                idle_event = matches!(
                    event,
                    Event::StateChanged {
                        to: SessionState::Idle,
                        ..
                    }
                );
                output.event(&event)?;
                if final_text(&event) {
                    conversation
                        .observe(swarmy_core::TurnStage::FinalTextRendered)
                        .await;
                }
            }
            Notification::Delta(delta) => {
                output.delta(&delta)?;
                if output.stream_finished(&delta) {
                    conversation
                        .observe(swarmy_core::TurnStage::FinalTextRendered)
                        .await;
                }
            }
            Notification::Transcript(TranscriptEvent::SessionIdle) => {
                conversation
                    .observe(swarmy_core::TurnStage::InputEnabled)
                    .await;
                if json && !idle_event {
                    println!(
                        "{}",
                        serde_json::json!({"event": "session_idle", "session_id": conversation.id})
                    );
                }
                break;
            }
            Notification::Transcript(TranscriptEvent::SessionChanged { previous, current }) => {
                if json {
                    println!(
                        "{}",
                        serde_json::json!({"event": "session_summarized", "previous_session_id": previous, "session_id": current})
                    );
                } else {
                    eprintln!(
                        "Conversation summarized. Session {previous} archived; continuing in {current}."
                    );
                }
            }
            Notification::Transcript(TranscriptEvent::State(SessionState::Completed)) => break,
            Notification::Transcript(_) => {}
        }
    }
    output.finish_line();
    Ok(outcome.finish())
}

pub(crate) struct Output {
    json: bool,
    report_errors: bool,
    streamed: String,
    messages: HashSet<MessageId>,
    mid_line: bool,
}

impl Output {
    pub(crate) fn new(json: bool) -> Self {
        Self {
            json,
            report_errors: true,
            streamed: String::new(),
            messages: HashSet::new(),
            mid_line: false,
        }
    }

    fn text(&mut self, text: &str) -> Result<()> {
        print!("{text}");
        if !text.is_empty() {
            self.mid_line = !text.ends_with('\n');
        }
        std::io::stdout().flush()?;
        Ok(())
    }

    fn finish_line(&mut self) {
        if self.mid_line {
            println!();
            self.mid_line = false;
        }
    }

    fn stream_finished(&self, delta: &Delta) -> bool {
        let Delta::Completed(response) = delta else {
            return false;
        };
        response.stop_reason == swarmy_llm::StopReason::EndTurn
            && !self.streamed.is_empty()
            && response
                .parts
                .iter()
                .filter_map(|part| match part {
                    Part::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<String>()
                == self.streamed
    }

    fn delta(&mut self, delta: &Delta) -> Result<()> {
        if self.json {
            println!(
                "{}",
                serde_json::json!({"event": "model_delta", "delta": delta})
            );
            std::io::stdout().flush()?;
        } else if let Delta::Text { text, .. } = delta {
            self.text(text)?;
            self.streamed.push_str(text);
        }
        Ok(())
    }

    pub(crate) fn event(&mut self, event: &Event) -> Result<()> {
        if self.json {
            println!(
                "{}",
                serde_json::json!({"event": "session_event", "value": event})
            );
        } else {
            match event {
                Event::InferenceFailed {
                    error,
                    retryable: false,
                    ..
                } => {
                    self.finish_line();
                    if self.report_errors {
                        eprintln!("Error: {error}");
                    }
                }
                Event::InferenceFailed {
                    error,
                    retryable: true,
                    retry_at,
                    ..
                } => {
                    self.finish_line();
                    self.streamed.clear();
                    eprintln!(
                        "Waiting for inference until {}: {error}",
                        retry_at.map_or_else(|| "soon".into(), |at| at.to_string())
                    );
                }
                Event::ToolCallRequested { call, .. } => {
                    self.finish_line();
                    println!(
                        "Tool call {} {} {}",
                        serde_json::to_string(&call.call_id)?,
                        serde_json::to_string(&call.tool)?,
                        call.arguments
                    );
                }
                Event::ToolCallCompleted {
                    call_id, result, ..
                } => {
                    self.finish_line();
                    println!(
                        "Tool result {} {}",
                        serde_json::to_string(call_id)?,
                        serde_json::to_string(result)?
                    );
                }
                Event::MessageAppended { message, .. }
                | Event::InferenceCompleted { message, .. }
                    if message.role == MessageRole::Assistant
                        && self.messages.insert(message.id) =>
                {
                    let text: String = message
                        .parts
                        .iter()
                        .filter_map(|part| match part {
                            Part::Text { text } => Some(text.as_str()),
                            _ => None,
                        })
                        .collect();
                    // Recover missing text from the durable message without
                    // repeating the prefix already printed from live deltas.
                    let remaining = text
                        .strip_prefix(&self.streamed)
                        .unwrap_or(&text)
                        .to_owned();
                    self.text(&remaining)?;
                    self.streamed.clear();
                }
                _ => {}
            }
        }
        std::io::stdout().flush()?;
        Ok(())
    }
}

/// Tool-call assistant messages do not finish a text turn.
pub(crate) fn final_text(event: &Event) -> bool {
    matches!(event, Event::InferenceCompleted { message, .. } | Event::MessageAppended { message, .. }
        if message.role == MessageRole::Assistant
        && message.parts.iter().any(|part| matches!(part, Part::Text { text } if !text.is_empty()))
        && !message.parts.iter().any(|part| matches!(part, Part::ToolCall { .. })))
}

async fn show_selection(
    store: &swarmy_store::Store,
    session: &swarmy_core::SessionRecord,
    json: bool,
) -> Result<()> {
    let selection = crate::selection::resolved_session(store, session).await?;
    let scratch = store.scratch(session.agent_id).await?;
    let marker = |overridden: bool| if overridden { "" } else { " (inherited)" };
    crate::vol::output(
        &serde_json::json!({ "event": "session_selection", "session_id": session.session_id,
            "state": session.state, "interrupt_requested": session.interrupt_requested,
            "inference": session.inference, "resolved": selection, "scratch": scratch }),
        &format!(
            "Session {}: {:?}, interrupt_requested={} provider={}{} model={}{} effort={}{} scratch_node={} scratch_bytes={}",
            session.session_id,
            session.state,
            session.interrupt_requested,
            selection.provider,
            marker(session.inference.provider.is_some()),
            selection.model,
            marker(session.inference.model.is_some()),
            selection.effort,
            marker(session.inference.effort.is_some()),
            scratch
                .as_ref()
                .map_or_else(|| "-".into(), |record| record.node_id.to_string()),
            scratch.as_ref().map_or(0, |record| record.bytes)
        ),
        json,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{RunOutcome, TurnOutcome};
    use swarmy_core::{
        Event, Message, MessageId, MessageRole, Part, RequestId, SessionId, SessionState,
        ToolCallId, ToolResult,
    };
    use ulid::Ulid;

    #[test]
    fn inference_error_fails_the_turn() {
        let mut turn = TurnOutcome::default();
        turn.record(&Event::InferenceFailed {
            seq: 1,
            request_id: RequestId::for_step(SessionId::from_ulid(Ulid::generate()), 1),
            error: "provider unavailable".into(),
            retryable: false,
            retry_at: None,
        });
        turn.record(&Event::StateChanged {
            seq: 2,
            from: SessionState::Runnable,
            to: SessionState::Idle,
        });
        assert_eq!(
            turn.finish(),
            RunOutcome::Failed("provider unavailable".into())
        );
    }

    #[test]
    fn idle_after_assistant_reply_completes_the_turn() {
        let mut turn = TurnOutcome::default();
        turn.record(&Event::MessageAppended {
            seq: 1,
            message: Message {
                id: MessageId::from_ulid(Ulid::generate()),
                role: MessageRole::Assistant,
                parts: vec![Part::Text {
                    text: "ready".into(),
                }],
            },
        });
        turn.record(&Event::StateChanged {
            seq: 2,
            from: SessionState::Runnable,
            to: SessionState::Idle,
        });
        assert_eq!(turn.finish(), RunOutcome::Completed);
    }

    #[test]
    fn tool_error_without_followup_reply_fails_the_turn() {
        let session = SessionId::from_ulid(Ulid::generate());
        let mut turn = TurnOutcome::default();
        turn.record(&Event::ToolCallCompleted {
            seq: 1,
            request_id: RequestId::for_step(session, 1),
            call_id: ToolCallId("clock".into()),
            result: ToolResult::Error {
                error: "timeout".into(),
            },
        });
        assert_eq!(
            turn.finish(),
            RunOutcome::Failed("tool failed: timeout".into())
        );
    }
}
