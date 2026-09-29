//! Human-facing conversation commands use only the public API.
//!
//! This crate never prints: [`Conversation::until_idle`] hands each
//! [`TurnOutput`] to the caller's emitter as it arrives, so streaming text
//! still renders incrementally while the CLI owns every `println!`.
use std::time::{Duration, Instant};

use swarmy_api_types as api;
use swarmy_client::{Client, EventStream, StreamItem};

/// Failures from opening a conversation and driving one turn to idle.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The `--image` value was not `NAME:TAG`.
    #[error("image must be NAME:TAG")]
    InvalidImage,
    /// The requested image is not registered; lists what is.
    #[error("image {requested} not found; registered images: {known}")]
    ImageNotFound { requested: String, known: String },
    /// A CLI flag combination the conversation rejects.
    #[error("{0}")]
    InvalidArgs(&'static str),
    /// An appended message was empty.
    #[error("message is empty")]
    EmptyMessage,
    /// The session was busy and the caller did not ask to queue.
    #[error("session is not idle")]
    SessionNotIdle,
    /// The worker did not pick up the session before the pickup deadline.
    #[error(
        "worker did not pick up session within {} seconds",
        PICKUP_DEADLINE.as_secs()
    )]
    PickupTimeout,
    /// The operator interrupted the turn.
    #[error("interrupted")]
    Interrupted,
    /// The session archived without a successor to follow.
    #[error("session completed")]
    SessionCompleted,
    /// The turn ended with a tool, inference, or missing-reply failure.
    #[error("{0}")]
    TurnFailed(String),
    /// The API did not answer before the client timeout.
    #[error("API at {endpoint}: request timed out")]
    ApiTimeout { endpoint: String },
    /// A control-plane request failed. The message names the endpoint the
    /// binary resolved, and the source chain keeps the typed client error
    /// for `downcast_ref` checks.
    #[error("API at {endpoint}: {source}")]
    Client {
        endpoint: String,
        #[source]
        source: swarmy_client::Error,
    },
    /// Terminal setup or input failed (interactive chat only).
    #[error("terminal error: {0}")]
    Terminal(String),
    /// A turn payload did not serialize.
    #[error("{0}")]
    Json(#[from] serde_json::Error),
    /// A terminal or pipe write failed.
    #[error("{0}")]
    Io(#[from] std::io::Error),
}

/// One renderable turn event, in arrival order. The conversation produces
/// these; the CLI prints them.
#[derive(Debug, Clone)]
pub enum TurnOutput {
    /// Raw streamed text. Text mode prints it verbatim; JSON mode wraps it
    /// in a `model_delta` line.
    TokenText(String),
    /// The raw store record, printed only in JSON mode.
    Record(api::RecordBody),
    /// A delivered queued message, printed only in text mode.
    QueueDelivered,
    /// A requested tool call, printed only in text mode.
    ToolCall {
        call_id: String,
        tool: String,
        arguments: String,
    },
    /// A completed tool call result, printed only in text mode.
    ToolResult {
        call_id: String,
        result: serde_json::Value,
    },
    /// An assistant reply with the already-streamed prefix removed.
    AssistantMessage(String),
    /// The idle marker that ends a turn, printed only in JSON mode.
    SessionIdle { session_id: String },
    /// A summarization successor switch, printed in both modes.
    Summarized {
        previous_session_id: String,
        session_id: String,
    },
}

/// How long `until_idle` waits for the first streamed item before reporting
/// the session as unpicked. Covers scheduler, worker, and gateway startup.
const PICKUP_DEADLINE: Duration = Duration::from_secs(30);

/// One API call with the standard client timeout. The duration lives in
/// `swarmy-client` next to the CLI's own call; this wrapper attaches the
/// endpoint the binary resolved so every client error names it.
pub(crate) async fn call<T>(
    endpoint: &str,
    future: impl std::future::Future<Output = Result<T, swarmy_client::Error>>,
) -> Result<T, Error> {
    swarmy_client::timed_call(future)
        .await
        .map_err(|error| match error {
            swarmy_client::Error::Timeout => Error::ApiTimeout {
                endpoint: endpoint.to_owned(),
            },
            error => Error::Client {
                endpoint: endpoint.to_owned(),
                source: error,
            },
        })
}

/// Wrap one client error that did not go through [`call`] (stream opens and
/// direct matches) so it still names the endpoint while keeping its type.
fn client_error(endpoint: &str, error: swarmy_client::Error) -> Error {
    Error::Client {
        endpoint: endpoint.to_owned(),
        source: error,
    }
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Format a terminal failure without pulling anyhow into this crate.
#[cfg(feature = "terminal")]
pub(crate) fn terminal_error(error: impl std::fmt::Display) -> Error {
    Error::Terminal(error.to_string())
}

/// Client-side stream item: server events plus the CLI-synthesized notice
/// that a summarized session continues elsewhere. The server never sends
/// the notice, so it lives here rather than in the API contract.
#[expect(clippy::large_enum_variant, reason = "the stream variant dwarfs the successor notice, but this enum is short-lived (returned by value from `next` and matched immediately): padding the notice costs one stack slot per call, while boxing would add a heap allocation per streamed event")]
pub enum ConversationItem {
    Stream(StreamItem),
    Summarized {
        previous_session_id: String,
        session_id: String,
    },
}

/// Arguments for opening a conversation, shared by `run`, `chat`, and the
/// benchmark so those paths pass one struct instead of six flags.
pub struct OpenArgs {
    /// Existing session id, if resuming.
    pub id: Option<String>,
    /// Base image in NAME:TAG form for a new ephemeral session.
    pub image: Option<String>,
    /// Named agent whose main session (or a new side session) to use.
    pub agent: Option<String>,
    /// Create a side conversation on the named agent.
    pub new: bool,
    /// Inference overrides for a new session.
    pub selection: swarmy_core::InferenceSelection,
    /// Inference failover route for this session only.
    pub route: Option<String>,
}

pub struct Conversation {
    pub id: String,
    pub agent_name: Option<String>,
    pub created: bool,
    pub provider: Option<String>,
    pub session: api::Session,
    pub last_text: String,
    pub tool_count: usize,
    pub tool_result: Option<serde_json::Value>,
    /// The archived session id when `open` followed a summarization chain.
    /// `run --session OLD` and chat use it to print the summary notice.
    pub predecessor: Option<String>,
    observer: Option<tokio::sync::mpsc::UnboundedSender<swarmy_core::TurnStage>>,
    client: Client,
    endpoint: String,
    stream: EventStream,
    pending: std::collections::VecDeque<StreamItem>,
    min_sequence: u64,
    delivered: u64,
    current_turn: Option<String>,
    poll: tokio::time::Interval,
}

fn image_ref(text: &str) -> Result<api::ImageRef> {
    let (name, tag) = text.split_once(':').ok_or(Error::InvalidImage)?;
    if name.is_empty() || tag.is_empty() {
        return Err(Error::InvalidImage);
    }
    Ok(api::ImageRef {
        name: name.into(),
        tag: tag.into(),
    })
}

/// Reject flag combinations no conversation can open, before any API call.
fn check_open_args(
    id: Option<&str>,
    image: Option<&str>,
    agent: Option<&str>,
    new: bool,
) -> Result<()> {
    if new && agent.is_none() {
        return Err(Error::InvalidArgs("--new requires --agent"));
    }
    if id.is_some() && (image.is_some() || agent.is_some() || new) {
        return Err(Error::InvalidArgs(
            "session id cannot be combined with --image, --agent, or --new",
        ));
    }
    if agent.is_some() && image.is_some() {
        return Err(Error::InvalidArgs(
            "--agent cannot be combined with --image",
        ));
    }
    Ok(())
}

async fn create_session(
    client: &Client,
    endpoint: &str,
    image: Option<&str>,
    agent_id: Option<String>,
    new: bool,
    selection: swarmy_core::InferenceSelection,
    route: Option<String>,
) -> Result<api::Session> {
    let image = image.map(image_ref).transpose()?;
    let result = call(
        endpoint,
        client.create_session(&api::CreateSession {
            idempotency_key: ulid::Ulid::generate().to_string(),
            agent_id,
            new,
            image: image.clone(),
            provider: selection.provider,
            model: selection.model,
            effort: selection.effort.map(Into::into),
            route,
        }),
    )
    .await;
    match result {
        Err(Error::Client { source, .. }) if matches!(&source, swarmy_client::Error::Api { body, .. } if body.code == "image_not_found") =>
        {
            let images = call(endpoint, client.images(None, 100)).await?;
            let known = images
                .iter()
                .map(|image| format!("{}:{}", image.name, image.tag))
                .collect::<Vec<_>>()
                .join(", ");
            Err(Error::ImageNotFound {
                requested: image
                    .as_ref()
                    .map_or_else(|| "default".into(), |i| format!("{}:{}", i.name, i.tag)),
                known,
            })
        }
        other => Ok(other?),
    }
}

/// Wait until required services report healthy. Progress reports go to the
/// caller's `on_problem` callback (the CLI prints them to stderr) instead of
/// the tracing log, so `run` and `chat` do not wait silently.
///
/// # Errors
/// Returns an error if the API call or event stream fails.
pub async fn wait_healthy(
    client: &Client,
    endpoint: &str,
    provider: Option<&str>,
    on_problem: &mut impl FnMut(&str),
) -> Result<()> {
    let mut last = String::new();
    loop {
        let health = call(endpoint, client.health()).await?;
        let provider = provider.or(Some(health.default_provider.as_str()));
        let problem = service_problem(&health.services, provider);
        if problem.is_empty() {
            return Ok(());
        }
        let message = format!(
            "{problem}{}; waiting for service health...",
            provider.map_or(String::new(), |p| format!(" ({p})"))
        );
        if message != last {
            on_problem(&message);
            last = message;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Describe one tool event as renderable outputs. The caller prints them;
/// this helper only translates.
fn tool_outputs(record: &swarmy_core::Event) -> Vec<TurnOutput> {
    match record {
        swarmy_core::Event::ToolCallRequested { call, .. } => vec![TurnOutput::ToolCall {
            call_id: call.call_id.0.clone(),
            tool: call.tool.clone(),
            arguments: call.arguments.to_string(),
        }],
        swarmy_core::Event::ToolCallCompleted {
            call_id, result, ..
        } => vec![TurnOutput::ToolResult {
            call_id: call_id.0.clone(),
            result: serde_json::to_value(result).unwrap_or(serde_json::Value::Null),
        }],
        _ => Vec::new(),
    }
}

/// Assign an explicit `--route` to the opened session only; the agent and
/// swarm default keep their assignments.
async fn apply_session_route(
    client: &Client,
    endpoint: &str,
    session: api::Session,
    route: Option<&str>,
) -> Result<api::Session> {
    if route.is_some() && session.route.as_deref() != route {
        return call(
            endpoint,
            client.set_session_route(
                &session.id,
                &api::SetSessionRoute {
                    idempotency_key: ulid::Ulid::generate().to_string(),
                    route: route.map(str::to_owned),
                },
            ),
        )
        .await;
    }
    Ok(session)
}

impl Conversation {
    /// Open or resume a conversation. The endpoint comes from the binary's
    /// connection, which already resolved and health-checked it.
    ///
    /// # Errors
    /// Returns an error if the API call or event stream fails.
    pub async fn open(client: Client, endpoint: String, args: OpenArgs) -> Result<Self> {
        let OpenArgs {
            id,
            image,
            agent,
            new,
            selection,
            route,
        } = args;
        check_open_args(id.as_deref(), image.as_deref(), agent.as_deref(), new)?;
        let provider = selection.provider.clone();
        let agent_record = if let Some(name) = &agent {
            Some(call(&endpoint, client.agent(name)).await?)
        } else {
            None
        };
        let created = id.is_none()
            && (new
                || agent_record
                    .as_ref()
                    .is_none_or(|a| a.main_session_id.is_none()));
        let session = if let Some(id) = id {
            call(&endpoint, client.session(&id)).await?
        } else {
            // Agent sessions inherit the agent and reject overrides at
            // creation; the route override below assigns them afterwards.
            let for_create = route.clone().filter(|_| agent_record.is_none());
            create_session(
                &client,
                &endpoint,
                image.as_deref(),
                agent_record.as_ref().map(|a| a.id.clone()),
                new,
                selection,
                for_create,
            )
            .await?
        };
        let mut session = session;
        let predecessor = session.id.clone();
        while session.state == api::SessionState::Completed {
            let successor = {
                let session_ref = &session;
                call(&endpoint, client.successor(session_ref)).await?
            };
            let Some(next) = successor else {
                break;
            };
            session = next;
        }
        let predecessor = (predecessor != session.id).then_some(predecessor);
        let agent_record = if agent_record.is_none() {
            if let Some(agent_id) = &session.agent_id {
                Some(call(&endpoint, client.agent(agent_id)).await?)
            } else {
                None
            }
        } else {
            agent_record
        };
        let mut stream = client.stream(api::Subscription {
            cursors: vec![api::Cursor {
                log_id: session.log_id.clone(),
                sequence: session.head_sequence,
            }],
            token_deltas: true,
        });
        stream
            .open()
            .await
            .map_err(|error| client_error(&endpoint, error))?;
        let provider = session
            .provider
            .clone()
            .or(provider)
            .or_else(|| agent_record.as_ref().and_then(|a| a.provider.clone()));
        session = apply_session_route(&client, &endpoint, session, route.as_deref()).await?;
        let head = session.head_sequence;
        Ok(Self {
            provider,
            id: session.id.clone(),
            agent_name: agent_record.map(|a| a.name),
            created,
            predecessor,
            min_sequence: head,
            delivered: head,
            current_turn: None,
            poll: tokio::time::interval_at(
                tokio::time::Instant::now() + Duration::from_secs(3),
                Duration::from_secs(3),
            ),
            session,
            last_text: String::new(),
            tool_count: 0,
            tool_result: None,
            observer: None,
            client,
            endpoint,
            stream,
            pending: std::collections::VecDeque::new(),
        })
    }

    /// Append one user message.
    ///
    /// # Errors
    /// Returns an error if the API call or event stream fails.
    pub async fn send(&mut self, text: String) -> Result<String> {
        self.send_with_queue(text, false).await
    }

    /// # Errors
    /// Returns API and transport errors or an invalid message error.
    pub async fn send_with_queue(&mut self, text: String, queue: bool) -> Result<String> {
        if text.trim().is_empty() {
            return Err(Error::EmptyMessage);
        }
        self.last_text.clear();
        self.tool_count = 0;
        self.tool_result = None;
        self.pending.clear();
        if !queue && self.session.state != api::SessionState::Idle {
            return Err(Error::SessionNotIdle);
        }
        let mut body = api::AppendMessage {
            idempotency_key: ulid::Ulid::generate().to_string(),
            expected_head: self.session.head_sequence,
            queue,
            text,
        };
        let first = call(&self.endpoint, self.client.append_message(&self.id, &body)).await;
        let appended = match first {
            Err(Error::Client { source, .. }) if matches!(&source, swarmy_client::Error::Api { status, body } if status.as_u16() == 409 && body.code == "stale_head") =>
            {
                self.session = call(&self.endpoint, self.client.session(&self.id)).await?;
                if !queue && self.session.state != api::SessionState::Idle {
                    return Err(Error::SessionNotIdle);
                }
                body.expected_head = self.session.head_sequence;
                call(&self.endpoint, self.client.append_message(&self.id, &body)).await?
            }
            other => other?,
        };
        self.current_turn = Some(appended.turn_id.clone());
        self.min_sequence = appended.sequence;
        self.delivered = self.delivered.max(appended.sequence.saturating_sub(1));
        self.session.head_sequence = appended.sequence;
        if !queue || self.session.state == api::SessionState::Idle {
            self.session.state = api::SessionState::Runnable;
        }
        self.poll.reset_after(Duration::from_secs(3));
        Ok(appended.turn_id)
    }

    pub fn observe_with(
        &mut self,
        sender: tokio::sync::mpsc::UnboundedSender<swarmy_core::TurnStage>,
    ) {
        self.observer = Some(sender);
    }

    fn pending_after(&self) -> u64 {
        let queued_max = self
            .pending
            .iter()
            .filter_map(|item| match item {
                StreamItem::Event(event) => Some(event.sequence),
                StreamItem::TokenDelta { .. } => None,
            })
            .max()
            .unwrap_or(self.delivered);
        self.delivered.max(queued_max)
    }

    fn observe_idle(&mut self, sequence: u64) {
        self.session.state = api::SessionState::Idle;
        self.session.head_sequence = sequence;
        self.min_sequence = sequence;
        self.delivered = self.delivered.max(sequence);
        self.current_turn = None;
    }

    fn is_duplicate(&self, event: &api::Event, queued: bool) -> bool {
        if event.log_id != self.session.log_id {
            return true;
        }
        if queued {
            // Queued events were selected as fresh (> delivered) at queue
            // time. They are all delivered, including a synthetic idle that
            // reuses the head sequence when the store has no real idle event.
            // Stale entries from a previous turn cannot survive send(), which
            // clears the queue.
            return false;
        }
        if event.sequence < self.min_sequence || event.sequence <= self.delivered {
            return true;
        }
        // The poll path already queued this sequence; the queued copy drives
        // the turn so the stream duplicate is dropped.
        self.pending
            .iter()
            .any(|queued| matches!(queued, StreamItem::Event(e) if e.sequence == event.sequence))
    }

    async fn poll_tick(&mut self) -> Result<bool> {
        // A successor switch replaces self.id; a tick that started before
        // the switch must not clobber the new session with the archived
        // one it read, nor queue the archived feed into the new turn.
        let id = self.id.clone();
        let endpoint = self.endpoint.clone();
        let session = call(&endpoint, self.client.session(&id)).await?;
        if self.id != id {
            return Ok(false);
        }
        if !(session.state == api::SessionState::Idle && session.head_sequence >= self.min_sequence)
        {
            return Ok(false);
        }
        // Replay only what the stream has not delivered yet. The delivered
        // cursor advances on return, so also skip sequences already waiting
        // in pending when two ticks fire before the queue drains.
        let after = self.pending_after();
        let history = call(&endpoint, self.client.events(&id, after, 100)).await?;
        if self.id != id {
            return Ok(false);
        }
        let (fresh, has_idle) = select_fresh(history, after, session.head_sequence, &self.pending);
        for event in fresh {
            self.pending.push_back(StreamItem::Event(event));
        }
        self.session = session;
        self.current_turn = None;
        if !has_idle {
            self.queue_synthetic_idle(after);
        }
        Ok(true)
    }

    fn queue_synthetic_idle(&mut self, after: u64) {
        let head = self.session.head_sequence;
        // The head was already delivered or queued (after covers both), so a
        // synthetic idle would duplicate. Otherwise the store has no real
        // idle event and the turn needs the synthetic to complete, even when
        // a real tool event shares the head sequence.
        if head <= after {
            return;
        }
        self.pending.push_back(StreamItem::Event(api::Event {
            log_id: self.session.log_id.clone(),
            sequence: head,
            payload: api::EventPayload::StoreRecord {
                record: api::RecordBody::Event(swarmy_core::Event::StateChanged {
                    seq: head,
                    from: swarmy_core::SessionState::Runnable,
                    to: swarmy_core::SessionState::Idle,
                }),
            },
        }));
    }

    async fn take_successor(&mut self) -> Result<Option<ConversationItem>> {
        let old = self.id.clone();
        let endpoint = self.endpoint.clone();
        let session = call(&endpoint, self.client.session(&old)).await?;
        let successor = {
            let session_ref = &session;
            call(&endpoint, self.client.successor(session_ref)).await?
        };
        let Some(successor) = successor else {
            return Ok(None);
        };
        let next = successor.id.clone();
        self.stream.subscription_handle().set(api::Subscription {
            cursors: vec![api::Cursor {
                log_id: successor.log_id.clone(),
                sequence: 0,
            }],
            token_deltas: true,
        });
        self.id.clone_from(&next);
        self.session = successor;
        self.min_sequence = 0;
        self.delivered = 0;
        self.current_turn = None;
        Ok(Some(ConversationItem::Summarized {
            previous_session_id: old,
            session_id: next,
        }))
    }

    /// Interrupt the active turn.
    ///
    /// # Errors
    /// Returns an error if the API call or event stream fails.
    pub async fn interrupt(&mut self) -> Result<()> {
        if self.current_turn.is_some() {
            call(
                &self.endpoint,
                self.client.interrupt(
                    &self.id,
                    &api::InterruptSession {
                        idempotency_key: ulid::Ulid::generate().to_string(),
                    },
                ),
            )
            .await?;
            self.current_turn = None;
        }
        Ok(())
    }

    /// Read the next streamed conversation item.
    ///
    /// # Errors
    /// Returns an error if the API call or event stream fails.
    pub async fn next(&mut self) -> Result<ConversationItem> {
        loop {
            let (item, queued) = if let Some(item) = self.pending.pop_front() {
                (ConversationItem::Stream(item), true)
            } else {
                let endpoint = self.endpoint.clone();
                (
                    ConversationItem::Stream(tokio::select! {
                        item = self.stream.next_item() => item.map_err(|error| client_error(&endpoint, error))?,
                        _ = self.poll.tick(), if self.current_turn.is_some() || self.session.state != api::SessionState::Idle => {
                            if self.poll_tick().await? {
                                continue;
                            }
                            continue;
                        }
                    }),
                    false,
                )
            };
            if let ConversationItem::Stream(StreamItem::Event(event)) = &item {
                if self.is_duplicate(event, queued) {
                    continue;
                }
                if is_completed_event(&event.payload)
                    && let Some(summary) = self.take_successor().await?
                {
                    return Ok(summary);
                }
                if is_idle_event(event) {
                    self.observe_idle(event.sequence);
                }
            }
            if let ConversationItem::Stream(StreamItem::TokenDelta { log_id, .. }) = &item {
                // Deltas from the archived feed can arrive after following
                // the successor; they belong to the old session, not the
                // new view, so drop them instead of rendering stray text.
                if *log_id != self.session.log_id {
                    continue;
                }
            }
            if let ConversationItem::Stream(StreamItem::Event(event)) = &item {
                self.delivered = self.delivered.max(event.sequence);
            }
            return Ok(item);
        }
    }

    pub fn queue(&mut self, item: StreamItem) {
        self.pending.push_back(item);
    }

    /// Wait until the selected provider is healthy, reporting progress
    /// through the caller's callback (see [`wait_healthy`]).
    ///
    /// # Errors
    /// Returns an error if the API call or event stream fails.
    pub async fn wait_healthy(
        &self,
        provider: Option<&str>,
        on_problem: &mut impl FnMut(&str),
    ) -> Result<()> {
        wait_healthy(&self.client, &self.endpoint, provider, on_problem).await
    }

    /// Drive the stream until the session is idle, handing each renderable
    /// output to `emit` as it arrives. The caller prints; this method never
    /// writes to the terminal.
    ///
    /// # Errors
    /// Returns API and transport errors, an interruption, or the turn failure.
    pub async fn until_idle(
        &mut self,
        json: bool,
        requires_reply: bool,
        emit: &mut impl FnMut(TurnOutput),
    ) -> Result<()> {
        let mut progress = TurnProgress::default();
        let deadline = Instant::now() + PICKUP_DEADLINE;
        loop {
            let next = async {
                if requires_reply && !progress.started {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    Ok(tokio::time::timeout(remaining, self.next())
                        .await
                        .map_err(|_| Error::PickupTimeout)??)
                } else {
                    self.next().await
                }
            };
            let item = tokio::select! {
                item = next => Some(item?),
                _ = tokio::signal::ctrl_c() => None,
            };
            let Some(item) = item else {
                self.interrupt().await?;
                return Err(Error::Interrupted);
            };
            match item {
                ConversationItem::Stream(StreamItem::TokenDelta {
                    payload: api::EventPayload::TokenDelta { text, .. },
                    ..
                }) => {
                    emit(TurnOutput::TokenText(text.clone()));
                    progress.streamed.push_str(&text);
                }
                ConversationItem::Stream(StreamItem::Event(event)) => {
                    let sequence = event.sequence;
                    if let api::EventPayload::StoreRecord { record } = event.payload
                        && self.record_event(
                            &record,
                            sequence,
                            json,
                            requires_reply,
                            &mut progress,
                            &mut *emit,
                        )?
                    {
                        return Ok(());
                    }
                }
                ConversationItem::Stream(StreamItem::TokenDelta { .. }) => {}
                ConversationItem::Summarized {
                    previous_session_id,
                    session_id,
                } => {
                    emit(TurnOutput::Summarized {
                        previous_session_id,
                        session_id,
                    });
                    // Preserve the existing successor-switch behavior: a summary
                    // notice marks the turn started and disables the pickup deadline.
                    progress.started = true;
                }
            }
        }
    }

    fn record_event(
        &mut self,
        record: &api::RecordBody,
        sequence: u64,
        json: bool,
        requires_reply: bool,
        progress: &mut TurnProgress,
        emit: &mut impl FnMut(TurnOutput),
    ) -> Result<bool> {
        if sequence < self.min_sequence {
            return Ok(false);
        }
        if json {
            emit(TurnOutput::Record(record.clone()));
        }
        let api::RecordBody::Event(event) = record else {
            return Ok(false);
        };
        match event {
            swarmy_core::Event::StateChanged { to, .. } => match to {
                swarmy_core::SessionState::Leased | swarmy_core::SessionState::WaitingInference => {
                    progress.started = true;
                }
                swarmy_core::SessionState::Completed => return Err(Error::SessionCompleted),
                swarmy_core::SessionState::Idle => {
                    return self.finish_idle(sequence, json, requires_reply, progress, emit);
                }
                _ => {}
            },
            swarmy_core::Event::InferenceRequested { .. } => progress.started = true,
            swarmy_core::Event::MessageQueued { .. } if !json => {
                emit(TurnOutput::QueueDelivered);
            }
            swarmy_core::Event::ToolCallCompleted { result, .. } => {
                self.tool_count += 1;
                self.tool_result = Some(serde_json::to_value(result)?);
                progress.error = tool_error(result);
            }
            swarmy_core::Event::InferenceFailed {
                error,
                failure_kind,
                ..
            } => {
                progress.error = Some(inference_error(error, *failure_kind));
            }
            swarmy_core::Event::MessageAppended { message, .. } => {
                self.render_message(message, json, progress, emit);
            }
            swarmy_core::Event::InferenceCompleted { completion, .. } => {
                self.render_message(&completion.message, json, progress, emit);
            }
            _ => {}
        }
        if !json {
            for output in tool_outputs(event) {
                emit(output);
            }
        }
        Ok(false)
    }

    fn finish_idle(
        &mut self,
        sequence: u64,
        json: bool,
        requires_reply: bool,
        progress: &mut TurnProgress,
        emit: &mut impl FnMut(TurnOutput),
    ) -> Result<bool> {
        if !json && !progress.streamed.is_empty() && !progress.streamed.ends_with('\n') {
            emit(TurnOutput::TokenText("\n".into()));
        }
        if json {
            emit(TurnOutput::SessionIdle {
                session_id: self.id.clone(),
            });
        }
        self.min_sequence = sequence;
        self.session.head_sequence = sequence;
        self.session.state = api::SessionState::Idle;
        self.current_turn = None;
        if let Some(sender) = &self.observer {
            let _ = sender.send(swarmy_core::TurnStage::InputEnabled);
        }
        if let Some(outcome) = turn_outcome(progress, requires_reply) {
            progress.error.take();
            return Err(Error::TurnFailed(outcome));
        }
        Ok(true)
    }

    fn render_message(
        &mut self,
        message: &swarmy_core::Message,
        json: bool,
        progress: &mut TurnProgress,
        emit: &mut impl FnMut(TurnOutput),
    ) {
        let Some(text) = assistant_reply_text(message) else {
            return;
        };
        progress.reply = true;
        progress.error = None;
        self.last_text.clone_from(&text);
        if let Some(sender) = &self.observer {
            let _ = sender.send(swarmy_core::TurnStage::FinalTextRendered);
        }
        if json {
            emit(TurnOutput::AssistantMessage(text));
        } else {
            let remaining = text.strip_prefix(&progress.streamed).unwrap_or(&text);
            let mut line = remaining.to_owned();
            if !text.ends_with('\n') {
                line.push('\n');
            }
            emit(TurnOutput::AssistantMessage(line));
            progress.streamed.clear();
        }
    }
}

/// The tool failure a record reports for the current turn, if any. A later
/// assistant text reply clears it (see `assistant_reply_text`).
fn tool_error(result: &swarmy_core::ToolResult) -> Option<String> {
    match result {
        swarmy_core::ToolResult::Error { error } => Some(format!("tool failed: {error}")),
        swarmy_core::ToolResult::Completed { .. } => None,
    }
}

fn inference_error(error: &str, kind: swarmy_core::FailureKind) -> String {
    if kind == swarmy_core::FailureKind::GatewayUnserved {
        format!("{error}; run `swarmy auth set PROVIDER` or start a gateway that serves it")
    } else {
        error.to_owned()
    }
}

fn assistant_reply_text(message: &swarmy_core::Message) -> Option<String> {
    if message.role != swarmy_core::MessageRole::Assistant
        || message
            .parts
            .iter()
            .any(|part| matches!(part, swarmy_core::Part::ToolCall { .. }))
    {
        return None;
    }
    let text = message
        .parts
        .iter()
        .filter_map(|part| match part {
            swarmy_core::Part::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<String>();
    if text.is_empty() { None } else { Some(text) }
}
/// The idle-time failure for a turn, if the turn did not succeed.
fn turn_outcome(progress: &TurnProgress, run: bool) -> Option<String> {
    if let Some(error) = progress.error.as_deref() {
        return Some(error.to_owned());
    }
    if run && !progress.reply {
        return Some("turn ended without a completed assistant reply".into());
    }
    None
}

#[derive(Default)]
struct TurnProgress {
    reply: bool,
    error: Option<String>,
    streamed: String,
    started: bool,
}

fn is_idle_event(event: &api::Event) -> bool {
    matches!(
        &event.payload,
        api::EventPayload::StoreRecord {
            record: api::RecordBody::Event(swarmy_core::Event::StateChanged {
                to: swarmy_core::SessionState::Idle,
                ..
            })
        }
    )
}

fn is_completed_event(payload: &api::EventPayload) -> bool {
    matches!(
        payload,
        api::EventPayload::StoreRecord {
            record: api::RecordBody::Event(swarmy_core::Event::StateChanged {
                to: swarmy_core::SessionState::Completed,
                ..
            })
        }
    )
}

// Select the events a poll tick must queue: only what the stream has not
// delivered yet, up to the observed head, and never what is already queued.
// Returns the fresh events and whether they already contain the idle event,
// so the caller can skip the synthetic idle when the real one is present.
fn select_fresh(
    history: Vec<api::Event>,
    after: u64,
    head: u64,
    pending: &std::collections::VecDeque<StreamItem>,
) -> (Vec<api::Event>, bool) {
    let mut fresh: Vec<api::Event> = history
        .into_iter()
        .filter(|event| event.sequence > after && event.sequence <= head)
        .collect();
    fresh.retain(|event| {
        !pending
            .iter()
            .any(|queued| matches!(queued, StreamItem::Event(e) if e.sequence == event.sequence))
    });
    let has_idle = fresh.iter().any(is_idle_event);
    (fresh, has_idle)
}

fn service_problem(services: &[api::ServiceHealth], provider: Option<&str>) -> &'static str {
    let alive = |role: api::ServiceRole| services.iter().any(|s| s.alive && s.role == role);
    if !alive(api::ServiceRole::Worker) {
        "No worker is alive"
    } else if !alive(api::ServiceRole::Scheduler) {
        "No scheduler is alive"
    } else if !services.iter().any(|s| {
        s.alive
            && s.role == api::ServiceRole::Gateway
            && provider.is_none_or(|p| s.providers.iter().any(|v| v == p))
    }) {
        "No gateway serves this session's provider"
    } else {
        ""
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn service(role: api::ServiceRole, providers: &[&str], alive: bool) -> api::ServiceHealth {
        api::ServiceHealth {
            role,
            instance_id: String::new(),
            version: String::new(),
            alive,
            last_seen: String::new(),
            providers: providers.iter().map(|s| (*s).into()).collect(),
        }
    }
    #[test]
    fn image_name_and_tag_are_required() {
        assert!(image_ref("ubuntu:dev").is_ok());
        assert!(image_ref("ubuntu").is_err());
        assert!(image_ref(":dev").is_err());
    }

    #[test]
    fn wrapped_client_error_keeps_endpoint_and_typed_source() {
        let inner = swarmy_client::Error::Timeout;
        let error = Error::Client {
            endpoint: "http://127.0.0.1:1".into(),
            source: inner,
        };
        assert_eq!(
            error.to_string(),
            "API at http://127.0.0.1:1: request timed out"
        );
        let source = std::error::Error::source(&error).expect("client error has a source");
        assert!(source.downcast_ref::<swarmy_client::Error>().is_some());
    }

    #[test]
    fn health_reports_missing_services_and_provider() {
        let mut services = vec![
            service(api::ServiceRole::Worker, &[], false),
            service(api::ServiceRole::Scheduler, &[], true),
            service(api::ServiceRole::Gateway, &["fake"], true),
        ];
        assert_eq!(
            service_problem(&services, Some("fake")),
            "No worker is alive"
        );
        services[0].alive = true;
        services[1].alive = false;
        assert_eq!(
            service_problem(&services, Some("fake")),
            "No scheduler is alive"
        );
        services[1].alive = true;
        assert_eq!(
            service_problem(&services, Some("openai")),
            "No gateway serves this session's provider"
        );
        assert_eq!(service_problem(&services, Some("fake")), "");
    }

    fn store_event(sequence: u64, idle: bool) -> api::Event {
        api::Event {
            log_id: api::LogId::Session("s".into()),
            sequence,
            payload: api::EventPayload::StoreRecord {
                record: api::RecordBody::Event(swarmy_core::Event::StateChanged {
                    seq: sequence,
                    from: swarmy_core::SessionState::Runnable,
                    to: if idle {
                        swarmy_core::SessionState::Idle
                    } else {
                        swarmy_core::SessionState::Runnable
                    },
                }),
            },
        }
    }

    #[test]
    fn gateway_unserved_failure_gives_cli_advice() {
        assert_eq!(
            inference_error(
                "no gateway serves provider openai",
                swarmy_core::FailureKind::GatewayUnserved
            ),
            "no gateway serves provider openai; run `swarmy auth set PROVIDER` or start a gateway that serves it"
        );
    }

    #[test]
    fn inference_error_fails_the_turn() {
        let progress = TurnProgress {
            error: Some(inference_error(
                "provider unavailable",
                swarmy_core::FailureKind::Provider,
            )),
            ..TurnProgress::default()
        };
        assert_eq!(
            turn_outcome(&progress, true).as_deref(),
            Some("provider unavailable")
        );
    }

    fn assistant(parts: Vec<swarmy_core::Part>) -> swarmy_core::Message {
        swarmy_core::Message {
            id: swarmy_core::MessageId::from_ulid(ulid::Ulid::generate()),
            role: swarmy_core::MessageRole::Assistant,
            parts,
        }
    }

    #[test]
    fn idle_after_assistant_reply_completes_the_turn() {
        let mut progress = TurnProgress {
            error: Some("provider unavailable".into()),
            ..TurnProgress::default()
        };
        let text = assistant_reply_text(&assistant(vec![swarmy_core::Part::Text {
            text: "ready".into(),
        }]));
        assert_eq!(text.as_deref(), Some("ready"));
        progress.reply = true;
        progress.error = None;
        assert_eq!(turn_outcome(&progress, true), None);
    }

    #[test]
    fn tool_call_message_does_not_finish_the_turn() {
        let text = assistant_reply_text(&assistant(vec![swarmy_core::Part::ToolCall {
            call_id: swarmy_core::ToolCallId("1".into()),
            tool: "bash".into(),
            input: serde_json::json!({}),
        }]));
        assert_eq!(text, None);
    }

    #[test]
    fn tool_error_without_followup_reply_fails_the_turn() {
        let progress = TurnProgress {
            error: tool_error(&swarmy_core::ToolResult::Error {
                error: "timeout".into(),
            }),
            ..TurnProgress::default()
        };
        assert_eq!(
            turn_outcome(&progress, true).as_deref(),
            Some("tool failed: timeout")
        );
    }

    #[test]
    fn poll_replays_only_undelivered_events_and_skips_synthetic_idle() {
        use std::collections::VecDeque;
        // A tick that lands between the store commit and SSE delivery must not
        // replay the whole turn: only events after the delivered cursor.
        let history = vec![
            store_event(2, false),
            store_event(3, false),
            store_event(4, false),
            store_event(5, true),
        ];
        let (fresh, has_idle) = select_fresh(history.clone(), 1, 5, &VecDeque::new());
        assert_eq!(fresh.len(), 4);
        assert!(has_idle);
        // The stream already delivered through sequence 4 while the poll tick
        // was in flight; the tick must queue only the idle event.
        let (fresh, has_idle) = select_fresh(history.clone(), 4, 5, &VecDeque::new());
        assert_eq!(
            fresh.iter().map(|e| e.sequence).collect::<Vec<_>>(),
            vec![5]
        );
        assert!(has_idle);
        // Two ticks before the queue drains must not queue the same event
        // twice, and the synthetic idle is skipped when the real one is queued.
        let mut pending = VecDeque::new();
        pending.push_back(StreamItem::Event(store_event(5, true)));
        let (fresh, _) = select_fresh(history, 4, 5, &pending);
        assert!(fresh.is_empty());
        // Without an idle event in history the caller falls back to the
        // synthetic idle; with one present it must not add a second.
        assert!(is_idle_event(&store_event(5, true)));
        assert!(!is_idle_event(&store_event(4, false)));
    }
}
