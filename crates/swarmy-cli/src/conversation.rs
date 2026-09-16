//! Shared durable conversation transport for the line client and terminal UI.
use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use jiff::Timestamp;
use swarmy_bus::{Bus, Config, LiveFeed, LiveMessages, SubjectToken};
use swarmy_core::{
    AgentId, Event, Message, MessageId, MessageRole, Part, RequestId, SessionId, SessionRecord,
    SessionState, ToolCallId, ToolCallRecord, ToolResult, WakeReply,
};
use swarmy_llm::Delta;
use swarmy_store::{MAX_SCAN_LIMIT, Store, blob::ObjectBlobStore};
use ulid::Ulid;

const WAKE_TIMEOUT: Duration = Duration::from_secs(3);
const POLL_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Clone, Debug)]
pub enum TranscriptEvent {
    UserMessage(Message),
    SystemMessage(Message),
    AssistantTextDelta {
        index: usize,
        text: String,
    },
    AssistantPart {
        index: usize,
        text: String,
    },
    AssistantMessageFinal(Message),
    ToolRequested {
        request: RequestId,
        call: ToolCallRecord,
    },
    ToolCompleted {
        request: RequestId,
        call: ToolCallId,
        result: ToolResult,
    },
    State(SessionState),
    SessionIdle,
    Error(String),
}

pub enum Notification {
    // Keep the existing run --json protocol alongside the typed UI events.
    Log(Event),
    Delta(Delta),
    Transcript(TranscriptEvent),
}

pub struct Conversation {
    pub id: SessionId,
    store: Store,
    bus: Bus,
    events: LiveMessages<Event>,
    deltas: LiveMessages<Delta>,
    events_open: bool,
    deltas_open: bool,
    poll: tokio::time::Interval,
    after: u64,
    previous_head: Option<u64>,
    state: Option<SessionState>,
    pending: VecDeque<Notification>,
    replay: Replay,
}

impl Conversation {
    pub async fn open(id: Option<SessionId>) -> Result<Self> {
        let bus = bus().await?;
        let store = store().await?;
        let id = if let Some(id) = id {
            store
                .fetch_session(id)
                .await?
                .context("session not found")?;
            id
        } else {
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
            id
        };
        // Subscribe before reading history or waking, so neither path has a gap.
        let (events, deltas) = tokio::time::timeout(WAKE_TIMEOUT, async {
            let events = bus.subscribe_live(LiveFeed::SessionEvents(id)).await?;
            let deltas = bus.subscribe_live(LiveFeed::ModelDeltas(id)).await?;
            Ok::<_, swarmy_bus::Error>((events, deltas))
        })
        .await
        .context("cannot reach scheduler: live subscription timed out")??;
        let mut conversation = Self {
            id,
            store,
            bus,
            events,
            deltas,
            events_open: true,
            deltas_open: true,
            poll: tokio::time::interval(POLL_INTERVAL),
            after: 0,
            previous_head: None,
            state: None,
            pending: VecDeque::new(),
            replay: Replay::default(),
        };
        conversation.catch_up().await?;
        // A client can exit after appending the message but before the wake ack.
        if conversation.state == Some(SessionState::Idle) && conversation.replay.pending_user {
            conversation.wake().await?;
        }
        Ok(conversation)
    }

    pub async fn send(&mut self, text: String) -> Result<()> {
        ensure!(!text.trim().is_empty(), "message is empty");
        let session = self
            .store
            .fetch_session(self.id)
            .await?
            .context("session disappeared")?;
        ensure!(
            session.state == SessionState::Idle,
            "session is {:?}; wait until idle",
            session.state
        );
        self.store
            .append_events(
                self.id,
                session.head_seq,
                &[Event::MessageAppended {
                    seq: 0,
                    message: Message {
                        id: MessageId::from_ulid(Ulid::generate()),
                        role: MessageRole::User,
                        parts: vec![Part::Text { text }],
                    },
                }],
            )
            .await?;
        self.wake().await
    }

    async fn wake(&mut self) -> Result<()> {
        match self.bus.request_wake(self.id, WAKE_TIMEOUT).await? {
            WakeReply::Runnable => {}
            WakeReply::Unchanged(state) => bail!("scheduler did not wake session: {state:?}"),
            WakeReply::NotFound => bail!("scheduler could not find session {}", self.id),
            WakeReply::Failed(error) => bail!("scheduler failed to wake session: {error}"),
        }
        self.previous_head = None;
        self.state = None;
        // An initial Idle observation must not unlock a newly submitted turn.
        self.pending.retain(|event| {
            !matches!(
                event,
                Notification::Transcript(TranscriptEvent::SessionIdle | TranscriptEvent::State(_))
            )
        });
        self.emit(TranscriptEvent::State(SessionState::Runnable));
        Ok(())
    }

    /// Cancellation is safe: consumed feed data and log pages enter `pending`
    /// before another await, even when terminal input wins the outer select.
    pub async fn next(&mut self) -> Result<Notification> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Ok(event);
            }
            tokio::select! {
                biased;
                delta = self.deltas.next(), if self.deltas_open => {
                    match delta {
                        Some(Ok(delta)) => self.delta(delta),
                        Some(Err(error)) => self.emit(TranscriptEvent::Error(error.to_string())),
                        None => self.deltas_open = false,
                    }
                }
                event = self.events.next(), if self.events_open => {
                    if event.is_none() { self.events_open = false; }
                    // Live events are nudges. Only ordered durable events enter history.
                    self.catch_up().await?;
                }
                _ = self.poll.tick() => self.catch_up().await?,
            }
        }
    }

    fn emit(&mut self, event: TranscriptEvent) {
        self.pending.push_back(Notification::Transcript(event));
    }

    fn delta(&mut self, delta: Delta) {
        match &delta {
            Delta::Text { output_index, text } => self.emit(TranscriptEvent::AssistantTextDelta {
                index: *output_index,
                text: text.clone(),
            }),
            Delta::PartDone {
                output_index,
                part: Part::Text { text },
            } => self.emit(TranscriptEvent::AssistantPart {
                index: *output_index,
                text: text.clone(),
            }),
            _ => {}
        }
        self.pending.push_back(Notification::Delta(delta));
    }

    async fn catch_up(&mut self) -> Result<()> {
        let session = self
            .store
            .fetch_session(self.id)
            .await?
            .context("session disappeared")?;
        while self.after < session.head_seq {
            let events = self
                .store
                .read_events(self.id, self.after, MAX_SCAN_LIMIT)
                .await?;
            ensure!(
                !events.is_empty(),
                "session log ended before its recorded head"
            );
            for event in events
                .into_iter()
                .take_while(|event| event.seq() <= session.head_seq)
            {
                if let Some(event) = self.replay.record(&event) {
                    self.emit(event);
                }
                self.after = event.seq();
                self.pending.push_back(Notification::Log(event));
            }
        }
        // A stable head also handles stores that change state without a log event.
        // Historical Idle events must never finish a later, still-running turn.
        if self.state != Some(session.state) {
            self.emit(TranscriptEvent::State(session.state));
            self.state = Some(session.state);
        }
        if session.state == SessionState::Idle && self.previous_head == Some(session.head_seq) {
            self.emit(TranscriptEvent::SessionIdle);
        }
        self.previous_head = Some(session.head_seq);
        Ok(())
    }
}

#[derive(Default)]
pub struct Replay {
    messages: HashSet<MessageId>,
    pending_user: bool,
}

impl Replay {
    pub fn record(&mut self, event: &Event) -> Option<TranscriptEvent> {
        match event {
            Event::MessageAppended { message, .. } | Event::InferenceCompleted { message, .. }
                if self.messages.insert(message.id) =>
            {
                if matches!(message.role, MessageRole::User | MessageRole::Assistant) {
                    self.pending_user = message.role == MessageRole::User;
                }
                match message.role {
                    MessageRole::User => Some(TranscriptEvent::UserMessage(message.clone())),
                    MessageRole::System => Some(TranscriptEvent::SystemMessage(message.clone())),
                    MessageRole::Assistant => {
                        Some(TranscriptEvent::AssistantMessageFinal(message.clone()))
                    }
                    MessageRole::Tool => None,
                }
            }
            Event::ToolCallRequested {
                request_id, call, ..
            } => Some(TranscriptEvent::ToolRequested {
                request: *request_id,
                call: call.clone(),
            }),
            Event::ToolCallCompleted {
                request_id,
                call_id,
                result,
                ..
            } => Some(TranscriptEvent::ToolCompleted {
                request: *request_id,
                call: call_id.clone(),
                result: result.clone(),
            }),
            Event::InferenceFailed { error, .. } => Some(TranscriptEvent::Error(error.clone())),
            _ => None,
        }
    }
}

pub fn message_text(message: &Message) -> String {
    message
        .parts
        .iter()
        .filter_map(|part| match part {
            Part::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

pub async fn recent_sessions() -> Result<Vec<(SessionId, String)>> {
    let store = store().await?;
    let mut records = VecDeque::new();
    let mut after = None;
    loop {
        let page = store.list_sessions(after, MAX_SCAN_LIMIT).await?;
        if page.is_empty() {
            break;
        }
        after = page.last().map(|record| record.session_id);
        records.extend(page);
        while records.len() > 50 {
            records.pop_front();
        }
    }
    let mut sessions = Vec::new();
    for session in records.into_iter().rev().take(50) {
        let mut after = 0;
        let mut first = String::new();
        'history: while after < session.head_seq {
            let events = store
                .read_events(session.session_id, after, MAX_SCAN_LIMIT)
                .await?;
            if events.is_empty() {
                break;
            }
            for event in events {
                after = event.seq();
                if let Event::MessageAppended { message, .. } = event
                    && message.role == MessageRole::User
                {
                    first = message_text(&message);
                    break 'history;
                }
            }
        }
        sessions.push((session.session_id, first));
    }
    Ok(sessions)
}
pub async fn store() -> Result<Store> {
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
