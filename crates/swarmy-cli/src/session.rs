use std::{collections::HashSet, io::Write, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use jiff::Timestamp;
use swarmy_bus::{Bus, Config, LiveFeed, SubjectToken};
use swarmy_core::{
    AgentId, Event, Message, MessageId, MessageRole, Part, SessionId, SessionRecord, SessionState,
    WakeReply,
};
use swarmy_llm::Delta;
use swarmy_store::{MAX_SCAN_LIMIT, Store, blob::ObjectBlobStore};
use ulid::Ulid;

const WAKE_TIMEOUT: Duration = Duration::from_secs(3);
const POLL_INTERVAL: Duration = Duration::from_millis(500);

pub use crate::session_command::Command;

async fn store() -> Result<Store> {
    let settings = swarmy_config::Settings::load()?.settings;
    let cluster = settings.fdb_cluster_file;
    let directory: Vec<_> = settings
        .store_directory
        .split('/')
        .map(str::to_owned)
        .collect();
    Ok(Store::open(
        Some(&cluster),
        Some(&directory),
        Arc::new(ObjectBlobStore::from_env()?),
    )
    .await?)
}

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

async fn bus() -> Result<Bus> {
    let settings = swarmy_config::Settings::load()?.settings;
    let url = settings.nats_url;
    let config = Config {
        prefix: if settings.bus_prefix.is_empty() {
            None
        } else {
            Some(SubjectToken::new(settings.bus_prefix)?)
        },
        ack_wait: Duration::from_millis(settings.bus_ack_wait_ms),
        max_deliver: settings.bus_max_deliver,
    };
    let bus = tokio::time::timeout(WAKE_TIMEOUT, Bus::connect(&url, config))
        .await
        .context("cannot reach scheduler: NATS connection timed out")?
        .context("cannot reach scheduler over NATS")?;
    Ok(bus)
}

pub async fn run(prompt: String, json: bool) -> Result<()> {
    let store = store().await?;
    let bus = bus().await?;
    let id = SessionId::from_ulid(Ulid::generate());
    store
        .create_session(
            &SessionRecord {
                session_id: id,
                agent_id: AgentId::from_ulid(Ulid::generate()),
                state: SessionState::Idle,
                head_seq: 0,
                snapshot_ref: None,
            },
            Timestamp::now(),
        )
        .await?;
    store
        .append_events(
            id,
            0,
            &[Event::MessageAppended {
                seq: 0,
                message: Message {
                    id: MessageId::from_ulid(Ulid::generate()),
                    role: MessageRole::User,
                    parts: vec![Part::Text { text: prompt }],
                },
            }],
        )
        .await?;
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
    // Both registrations are confirmed before the scheduler can dispatch work.
    let (mut events, mut deltas) = tokio::time::timeout(WAKE_TIMEOUT, async {
        let events = bus
            .subscribe_live::<Event>(LiveFeed::SessionEvents(id))
            .await?;
        let deltas = bus
            .subscribe_live::<Delta>(LiveFeed::ModelDeltas(id))
            .await?;
        Ok::<_, swarmy_bus::Error>((events, deltas))
    })
    .await
    .context("cannot reach scheduler: live subscription timed out")??;
    match bus.request_wake(id, WAKE_TIMEOUT).await? {
        WakeReply::Runnable => {}
        WakeReply::Unchanged(state) => bail!("scheduler did not wake session: {state:?}"),
        WakeReply::NotFound => bail!("scheduler could not find session {id}"),
        WakeReply::Failed(error) => bail!("scheduler failed to wake session: {error}"),
    }
    let mut poll = tokio::time::interval(POLL_INTERVAL);
    let mut previous_head = None;
    loop {
        tokio::select! {
            // Drain buffered text before processing an Idle notification from a
            // different publisher, which can arrive at the same time.
            biased;
            delta = deltas.next() => {
                output.delta(&delta.context("model output feed closed")??)?;
                continue;
            }
            event = events.next() => {
                event.context("session event feed closed")??;
            }
            _ = poll.tick() => {}
        }
        if output.catch_up(&store, id).await? {
            break;
        }
        let session = store
            .fetch_session(id)
            .await?
            .context("session disappeared")?;
        // The wake was acknowledged. Two equal heads avoid treating the initial
        // Idle record or an actively growing log as a finished turn.
        if session.state == SessionState::Idle
            && previous_head == Some(session.head_seq)
            && output.after == session.head_seq
        {
            if json {
                println!(
                    "{}",
                    serde_json::json!({"event": "session_idle", "session_id": id})
                );
            }
            break;
        }
        previous_head = Some(session.head_seq);
    }
    output.finish_line();
    Ok(())
}

struct Output {
    json: bool,
    after: u64,
    streamed: String,
    messages: HashSet<MessageId>,
    mid_line: bool,
}

impl Output {
    fn new(json: bool) -> Self {
        Self {
            json,
            after: 0,
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

    async fn catch_up(&mut self, store: &Store, id: SessionId) -> Result<bool> {
        loop {
            let events = store.read_events(id, self.after, MAX_SCAN_LIMIT).await?;
            if events.is_empty() {
                return Ok(false);
            }
            for event in events {
                self.event(&event)?;
                self.after = event.seq();
                if matches!(
                    event,
                    Event::StateChanged {
                        to: SessionState::Idle,
                        ..
                    }
                ) {
                    return Ok(true);
                }
            }
        }
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
