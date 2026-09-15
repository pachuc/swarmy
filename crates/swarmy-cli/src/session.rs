use std::{collections::HashSet, io::Write};

use anyhow::{Context, Result, bail};
use swarmy_core::{Event, MessageId, MessageRole, Part, SessionId, SessionState};
use swarmy_llm::Delta;
use swarmy_store::MAX_SCAN_LIMIT;

use crate::conversation::{Conversation, Notification, TranscriptEvent, store};

pub use crate::session_command::Command;

pub async fn inspect(command: Command, json: bool) -> Result<()> {
    let store = store().await?;
    match command {
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
                    if json {
                        println!("{}", serde_json::to_string(&session)?);
                    } else {
                        println!(
                            "{} {:?} head={}",
                            session.session_id, session.state, session.head_seq
                        );
                    }
                    after = Some(session.session_id);
                }
            }
        }
    }
    Ok(())
}

pub async fn run(prompt: String, json: bool) -> Result<()> {
    let mut conversation = Conversation::open(None).await?;
    let id = conversation.id;
    let mut output = Output::new(json);
    if json {
        println!(
            "{}",
            serde_json::json!({"event": "session_created", "session_id": id})
        );
    } else {
        eprintln!("Session {id}");
    }
    std::io::stdout().flush()?;
    conversation.send(prompt).await?;
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
            }
            Notification::Delta(delta) => output.delta(&delta)?,
            Notification::Transcript(TranscriptEvent::SessionIdle) => {
                if json && !idle_event {
                    println!(
                        "{}",
                        serde_json::json!({"event": "session_idle", "session_id": id})
                    );
                }
                break;
            }
            Notification::Transcript(_) => {}
        }
    }
    output.finish_line();
    Ok(())
}

struct Output {
    json: bool,
    streamed: String,
    messages: HashSet<MessageId>,
    mid_line: bool,
}

impl Output {
    fn new(json: bool) -> Self {
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

    fn event(&mut self, event: &Event) -> Result<()> {
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
