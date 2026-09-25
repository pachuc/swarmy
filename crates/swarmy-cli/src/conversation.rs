//! Shared durable conversation transport for the line client and terminal UI.
use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use jiff::Timestamp;
use swarmy_bus::{Bus, Config, LiveFeed, LiveMessages, SubjectToken};
use swarmy_core::{
    Event, Message, MessageId, MessageRole, Part, RequestId, SessionId, SessionState, ToolCallId,
    ToolCallRecord, ToolResult,
};
use swarmy_llm::Delta;
use swarmy_store::{MAX_SCAN_LIMIT, Store, blob::ObjectBlobStore};
use ulid::Ulid;

const WAKE_TIMEOUT: Duration = Duration::from_secs(3);
const POLL_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
pub enum TranscriptEvent {
    UserMessage(Message),
    SystemMessage(Message),
    SessionChanged {
        previous: SessionId,
        current: SessionId,
    },
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
    Waiting(String),
}

pub enum Notification {
    // Keep the existing run --json protocol alongside the typed UI events.
    Log(Event),
    Delta(Delta),
    Transcript(TranscriptEvent),
}

struct ClientTurn {
    id: MessageId,
    text_rendered: bool,
    input_enabled: bool,
}

impl ClientTurn {
    fn new(id: MessageId) -> Self {
        Self {
            id,
            text_rendered: false,
            input_enabled: false,
        }
    }
}

pub enum Opened {
    Created,
    Resumed,
}

pub struct Conversation {
    pub id: SessionId,
    pub agent_name: Option<String>,
    pub opened: Opened,
    pub selection: swarmy_core::ResolvedSelection,
    turn: Option<ClientTurn>,
    store: Store,
    bus: Bus,
    events: LiveMessages<Event>,
    deltas: LiveMessages<Delta>,
    events_open: bool,
    deltas_open: bool,
    poll: tokio::time::Interval,
    after: u64,
    submitted_head: u64,
    needs_catch_up: bool,
    resend_interval: Duration,
    state: Option<SessionState>,
    pending: VecDeque<Notification>,
    replay: Replay,
}

impl Conversation {
    pub async fn open(
        id: Option<SessionId>,
        image: Option<&str>,
        agent: Option<&str>,
        new: bool,
        selection: swarmy_core::InferenceSelection,
    ) -> Result<Self> {
        ensure!(
            agent.is_none() || (image.is_none() && id.is_none()),
            "--agent cannot be combined with --image or a session id"
        );
        ensure!(!new || agent.is_some(), "--new requires --agent");
        ensure!(
            (id.is_none() && agent.is_none())
                || selection == swarmy_core::InferenceSelection::default(),
            "inference flags apply only to a new ephemeral session"
        );
        let settings = swarmy_config::Settings::load()?.settings;
        let image = if id.is_none() && agent.is_none() {
            let defaults = crate::selection::defaults(&settings)?;
            crate::selection::validate(&settings, &selection, &defaults)?;
            Some(settings.session_image(image)?)
        } else {
            None
        };
        let bus = bus().await?;
        let store = store().await?;
        let (id, created) = if let Some(id) = id {
            (id, false)
        } else {
            let agent = if let Some(name) = agent {
                Some(crate::agent::resolve(&store, name).await?.agent_id)
            } else {
                None
            };
            if let Some(agent) = agent.filter(|_| !new) {
                store.open_main_session(agent, Timestamp::now()).await?
            } else {
                let session = store
                    .create_session_with_inference(
                        SessionId::from_ulid(Ulid::generate()),
                        agent,
                        image,
                        Timestamp::now(),
                        &selection,
                    )
                    .await?;
                (session.session_id, true)
            }
        };
        let session = store
            .fetch_session(id)
            .await?
            .context("session not found")?;
        let id = session.session_id;
        let agent_name = if matches!(session.kind, swarmy_core::SessionKind::Named { .. }) {
            Some(store.get_agent(session.agent_id).await?.map_or_else(
                || format!("deleted agent {}", session.agent_id),
                |agent| agent.name,
            ))
        } else {
            None
        };
        // Subscribe before reading history or waking, so neither path has a gap.
        let (events, deltas) = tokio::time::timeout(WAKE_TIMEOUT, async {
            let events = bus.subscribe_live(LiveFeed::SessionEvents(id)).await?;
            let deltas = bus.subscribe_live(LiveFeed::ModelDeltas(id)).await?;
            Ok::<_, swarmy_bus::Error>((events, deltas))
        })
        .await
        .context("cannot reach scheduler: live subscription timed out")??;
        let selection = crate::selection::resolved_session(&store, &session).await?;
        let mut conversation = Self {
            selection,
            id,
            agent_name,
            opened: if created {
                Opened::Created
            } else {
                Opened::Resumed
            },
            turn: None,
            store,
            bus,
            events,
            deltas,
            events_open: true,
            deltas_open: true,
            poll: tokio::time::interval_at(
                tokio::time::Instant::now() + POLL_INTERVAL,
                POLL_INTERVAL,
            ),
            after: 0,
            submitted_head: 0,
            needs_catch_up: false,
            resend_interval: Duration::from_millis(settings.scheduler_resend_interval_ms),
            state: None,
            pending: VecDeque::new(),
            replay: Replay::default(),
        };
        conversation.catch_up().await?;
        // Recover messages appended by clients predating the atomic append/wake.
        if conversation.state == Some(SessionState::Idle) && conversation.replay.pending_user {
            conversation.wake().await?;
        }
        Ok(conversation)
    }

    pub async fn send(&mut self, text: String) -> Result<()> {
        if self.agent_name.is_some() {
            self.catch_up().await?;
        }
        ensure!(!text.trim().is_empty(), "message is empty");
        let turn = MessageId::from_ulid(Ulid::generate());
        self.turn = Some(ClientTurn::new(turn));
        self.observe(swarmy_core::TurnStage::Submitted).await;
        ensure!(
            self.state == Some(SessionState::Idle),
            "session is not idle; wait until idle"
        );
        let message = Message {
            id: turn,
            role: MessageRole::User,
            parts: vec![Part::Text { text }],
        };
        // The idle observation supplies the expected head. The append checks
        // both that head and the current idle state in the same transaction.
        let head = self
            .store
            .append_user_message(self.id, self.after, &message)
            .await?;
        self.submitted_head = head;
        self.reset_readiness();
        if head == self.after + 1 {
            self.record(Event::MessageAppended { seq: head, message });
        } else {
            self.catch_up().await?;
        }
        self.observe(swarmy_core::TurnStage::Appended).await;
        // The durable runnable index already guarantees recovery if NATS is down.
        if let Err(error) = self
            .bus
            .nudge(self.id, head, Some(turn), self.resend_interval, false)
            .await
        {
            tracing::warn!(%error, "message nudge failed; scheduler will recover");
        }
        Ok(())
    }

    pub fn turn_id(&self) -> Option<MessageId> {
        self.turn.as_ref().map(|turn| turn.id)
    }

    pub async fn timeline(&self) -> Result<LiveMessages<swarmy_core::TurnEvent>> {
        Ok(self
            .bus
            .subscribe_live(LiveFeed::TurnTimeline(self.id))
            .await?)
    }

    /// Called by the renderer only after output was written or input activated.
    pub async fn observe(&mut self, stage: swarmy_core::TurnStage) {
        let Some(turn) = &mut self.turn else {
            return;
        };
        let recorded = match stage {
            swarmy_core::TurnStage::FinalTextRendered => Some(&mut turn.text_rendered),
            swarmy_core::TurnStage::InputEnabled => Some(&mut turn.input_enabled),
            _ => None,
        };
        if let Some(recorded) = recorded {
            if *recorded {
                return;
            }
            *recorded = true;
        }
        let event = Bus::turn_event(self.id, turn.id, stage, None);
        self.bus.record_turn(&event).await;
        self.store.observe_turn_stage(event);
    }

    async fn wake(&mut self) -> Result<()> {
        self.store.wake_session(self.id, Timestamp::now()).await?;
        let session = self
            .store
            .fetch_session(self.id)
            .await?
            .context("session disappeared")?;
        self.reset_readiness();
        if let Err(error) = self
            .bus
            .nudge(
                self.id,
                session.head_seq,
                self.store.turn_id(self.id).await?,
                self.resend_interval,
                false,
            )
            .await
        {
            tracing::warn!(%error, "wake nudge failed; scheduler will recover");
        }
        Ok(())
    }

    fn reset_readiness(&mut self) {
        self.state = Some(SessionState::Runnable);
        // An initial Idle observation must not unlock a newly submitted turn.
        self.pending.retain(|event| {
            !matches!(
                event,
                Notification::Transcript(TranscriptEvent::SessionIdle | TranscriptEvent::State(_))
            )
        });
        self.emit(TranscriptEvent::State(SessionState::Runnable));
    }

    /// Cancellation is safe: consumed feed data and log pages enter `pending`
    /// before another await, even when terminal input wins the outer select.
    pub async fn next(&mut self) -> Result<Notification> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                if let Notification::Transcript(TranscriptEvent::UserMessage(message)) = &event
                    && self.turn_id() != Some(message.id)
                {
                    self.turn = Some(ClientTurn::new(message.id));
                }
                return Ok(event);
            }
            if self.needs_catch_up {
                self.catch_up().await?;
                self.needs_catch_up = false;
                continue;
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
                    match event {
                        Some(Ok(Event::StateChanged { to: SessionState::Completed, .. })) => {
                            self.needs_catch_up = true;
                        }
                        Some(Ok(event)) if event.seq() == self.after + 1 => {
                            let idle = matches!(event, Event::StateChanged { to: SessionState::Idle, .. })
                                && event.seq() > self.submitted_head;
                            self.record(event);
                            if idle { self.idle(); }
                        }
                        Some(Ok(event)) if event.seq() > self.after => self.needs_catch_up = true,
                        Some(Ok(_)) => {}, // A worker can replay an already observed tail.
                        Some(Err(error)) => self.emit(TranscriptEvent::Error(error.to_string())),
                        None => self.events_open = false,
                    }
                }
                _ = self.poll.tick() => self.needs_catch_up = true,
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
        loop {
            let session = self
                .store
                .fetch_session(self.id)
                .await?
                .context("session disappeared")?;
            self.selection = crate::selection::resolved_session(&self.store, &session).await?;
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
                    self.record(event);
                }
            }
            if session.state == SessionState::Completed
                && let Some(current) = self.store.next_session(self.id).await?
            {
                let events = self
                    .bus
                    .subscribe_live(LiveFeed::SessionEvents(current))
                    .await?;
                let deltas = self
                    .bus
                    .subscribe_live(LiveFeed::ModelDeltas(current))
                    .await?;
                let previous = self.id;
                self.id = current;
                self.events = events;
                self.deltas = deltas;
                self.events_open = true;
                self.deltas_open = true;
                self.after = 0;
                self.submitted_head = 0;
                self.state = None;
                self.replay = Replay::default();
                self.turn = None;
                self.pending.retain(|event| {
                    !matches!(
                        event,
                        Notification::Transcript(TranscriptEvent::State(SessionState::Completed))
                    )
                });
                self.emit(TranscriptEvent::SessionChanged { previous, current });
                continue;
            }
            if session.state == SessionState::Idle
                && session.head_seq >= self.submitted_head
                && !self.replay.pending_user
            {
                self.idle();
            } else if session.state == SessionState::Sleeping
                && self.state != Some(SessionState::Sleeping)
            {
                if let Some(wait) = self.store.inference_wait(self.id).await? {
                    self.emit(TranscriptEvent::Waiting(format!(
                        "{} (retry at {})",
                        wait.reasons.join("; "),
                        wait.wake_at
                    )));
                }
                self.emit(TranscriptEvent::State(SessionState::Sleeping));
                self.state = Some(SessionState::Sleeping);
            } else if self.state != Some(session.state) {
                self.emit(TranscriptEvent::State(session.state));
                self.state = Some(session.state);
            }
            return Ok(());
        }
    }

    fn record(&mut self, event: Event) {
        if let Some(transcript) = self.replay.record(&event) {
            self.emit(transcript);
        }
        self.after = event.seq();
        self.pending.push_back(Notification::Log(event));
    }

    fn idle(&mut self) {
        if self.state != Some(SessionState::Idle) {
            self.state = Some(SessionState::Idle);
            self.emit(TranscriptEvent::State(SessionState::Idle));
            self.emit(TranscriptEvent::SessionIdle);
        }
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
            Event::InferenceFailed {
                error,
                retryable: true,
                retry_at,
                ..
            } => Some(TranscriptEvent::Waiting(format!(
                "{error} (retry at {})",
                retry_at.map_or_else(|| "soon".into(), |at| at.to_string())
            ))),
            Event::InferenceFailed { error, .. } => Some(TranscriptEvent::Error(error.clone())),
            Event::StateChanged {
                to: SessionState::Idle,
                ..
            } => {
                self.pending_user = false;
                None
            }
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

pub(crate) async fn bus() -> Result<Bus> {
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
