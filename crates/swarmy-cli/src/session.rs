use std::{collections::HashSet, io::Write};

use anyhow::{Context, Result, bail};
use swarmy_core::{Event, MessageId, MessageRole, Part, SessionId, SessionState};
use swarmy_llm::Delta;
use swarmy_store::MAX_SCAN_LIMIT;

use crate::conversation::{Conversation, Notification, Opened, TranscriptEvent, store};

pub use crate::session_command::Command;

pub async fn inspect(command: Command, json: bool) -> Result<()> {
    let store = store().await?;
    match command {
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
            let id = SessionId::from_ulid(session_id);
            let session = store
                .fetch_session(id)
                .await?
                .context("session not found")?;
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
                    value["main"] = main.into();
                    value["agent_name"] = serde_json::to_value(&name)?;
                    let kind = match session.kind {
                        swarmy_core::SessionKind::Ephemeral => "ephemeral",
                        swarmy_core::SessionKind::Named { .. } => "named",
                    };
                    crate::vol::output(
                        &value,
                        &format!(
                            "{} {:?} kind={kind} agent={} head={} computer_deleted={} archived={} main={main}",
                            session.session_id,
                            session.state,
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

pub async fn run(
    prompt: String,
    image: Option<String>,
    agent: Option<String>,
    new: bool,
    json: bool,
) -> Result<()> {
    let mut conversation =
        Conversation::open(None, image.as_deref(), agent.as_deref(), new).await?;
    announce(&conversation, json)?;
    conversation.send(prompt).await?;
    until_idle(&mut conversation, json).await
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
) -> Result<()> {
    use tokio::io::AsyncBufReadExt;
    let mut conversation = Conversation::open(id, image.as_deref(), agent.as_deref(), new).await?;
    announce(&conversation, true)?;
    until_idle(&mut conversation, true).await?;
    let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    while let Some(prompt) = lines.next_line().await? {
        conversation.send(prompt).await?;
        until_idle(&mut conversation, true).await?;
    }
    Ok(())
}

async fn until_idle(conversation: &mut Conversation, json: bool) -> Result<()> {
    let mut output = Output::new(json);
    let mut idle_event = false;
    loop {
        match conversation.next().await? {
            Notification::Log(event) => {
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
    Ok(())
}

pub(crate) struct Output {
    json: bool,
    streamed: String,
    messages: HashSet<MessageId>,
    mid_line: bool,
}

impl Output {
    pub(crate) fn new(json: bool) -> Self {
        Self {
            json,
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
