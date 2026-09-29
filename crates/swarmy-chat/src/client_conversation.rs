//! Human-facing conversation commands use only the public API.
use std::{
    io::{self, Write},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use swarmy_api_types as api;
use swarmy_client::{Client, EventStream, StreamItem};

/// Client-side stream item: server events plus the CLI-synthesized notice
/// that a summarized session continues elsewhere. The server never sends
/// the notice, so it lives here rather than in the API contract.
// The stream variant dwarfs the successor notice, but this enum is
// short-lived (returned by value from `next` and matched immediately), so
// padding the notice costs one stack slot per call while boxing would add
// a heap allocation per streamed event.
#[allow(clippy::large_enum_variant)]
pub enum ConversationItem {
    Stream(StreamItem),
    Summarized {
        previous_session_id: String,
        session_id: String,
    },
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
    let (name, tag) = text.split_once(':').context("image must be NAME:TAG")?;
    ensure!(
        !name.is_empty() && !tag.is_empty(),
        "image must be NAME:TAG"
    );
    Ok(api::ImageRef {
        name: name.into(),
        tag: tag.into(),
    })
}

async fn create_session(
    client: &Client,
    image: Option<&str>,
    agent_id: Option<String>,
    new: bool,
    selection: swarmy_core::InferenceSelection,
    route: Option<String>,
) -> Result<api::Session> {
    let image = image.map(image_ref).transpose()?;
    let result = client
        .create_session(&api::CreateSession {
            idempotency_key: ulid::Ulid::generate().to_string(),
            agent_id,
            new,
            image: image.clone(),
            provider: selection.provider,
            model: selection.model,
            effort: selection
                .effort
                .map(serde_json::to_value)
                .transpose()?
                .map(serde_json::from_value)
                .transpose()?,
            route,
        })
        .await;
    match result {
        Err(swarmy_client::Error::Api { body, .. }) if body.code == "image_not_found" => {
            let images = client.images(None, 100).await?;
            let known = images
                .iter()
                .map(|image| format!("{}:{}", image.name, image.tag))
                .collect::<Vec<_>>()
                .join(", ");
            anyhow::bail!(
                "image {} not found; registered images: {known}",
                image
                    .as_ref()
                    .map_or_else(|| "default".into(), |i| format!("{}:{}", i.name, i.tag))
            );
        }
        other => Ok(other?),
    }
}

/// Wait until required services report healthy.
///
/// # Errors
/// Returns an error if the API call or event stream fails.
pub async fn wait_healthy(client: &Client, endpoint: &str, provider: Option<&str>) -> Result<()> {
    let mut last = String::new();
    loop {
        let health = crate::api_client::call(endpoint, client.health()).await?;
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
            eprintln!("{message}");
            last = message;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn print_tools(record: &swarmy_core::Event) -> Result<()> {
    match record {
        swarmy_core::Event::ToolCallRequested { call, .. } => println!(
            "Tool call {} {} {}",
            call.call_id.0, call.tool, call.arguments
        ),
        swarmy_core::Event::ToolCallCompleted {
            call_id, result, ..
        } => println!(
            "Tool result {} {}",
            call_id.0,
            serde_json::to_string(result)?
        ),
        _ => {}
    }
    Ok(())
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
        return crate::api_client::call(
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

/// A local idle guard failed before the append reached the API.
#[derive(Debug)]
pub struct SessionNotIdle;

impl std::fmt::Display for SessionNotIdle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("session is not idle")
    }
}

impl std::error::Error for SessionNotIdle {}

impl Conversation {
    /// Open or resume a conversation.
    ///
    /// # Errors
    /// Returns an error if the API call or event stream fails.
    pub async fn open(
        client: Client,
        id: Option<String>,
        image: Option<String>,
        agent: Option<String>,
        new: bool,
        selection: swarmy_core::InferenceSelection,
        route: Option<String>,
    ) -> Result<Self> {
        ensure!(!new || agent.is_some(), "--new requires --agent");
        ensure!(
            id.is_none() || (image.is_none() && agent.is_none() && !new),
            "session id cannot be combined with --image, --agent, or --new"
        );
        ensure!(
            agent.is_none() || image.is_none(),
            "--agent cannot be combined with --image"
        );
        let provider = selection.provider.clone();
        let agent_record = if let Some(name) = &agent {
            Some(client.agent(name).await?)
        } else {
            None
        };
        let created = id.is_none()
            && (new
                || agent_record
                    .as_ref()
                    .is_none_or(|a| a.main_session_id.is_none()));
        let session = if let Some(id) = id {
            client.session(&id).await?
        } else {
            // Agent sessions inherit the agent and reject overrides at
            // creation; the route override below assigns them afterwards.
            let for_create = route.clone().filter(|_| agent_record.is_none());
            create_session(
                &client,
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
            let Some(next) = client.successor(&session).await? else {
                break;
            };
            session = next;
        }
        let predecessor = (predecessor != session.id).then_some(predecessor);
        let agent_record = if agent_record.is_none() {
            if let Some(agent_id) = &session.agent_id {
                Some(client.agent(agent_id).await?)
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
        stream.open().await?;
        let provider = session
            .provider
            .clone()
            .or(provider)
            .or_else(|| agent_record.as_ref().and_then(|a| a.provider.clone()));
        let endpoint = crate::api_client::endpoint()?;
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
        ensure!(!text.trim().is_empty(), "message is empty");
        self.last_text.clear();
        self.tool_count = 0;
        self.tool_result = None;
        self.pending.clear();
        if !queue && self.session.state != api::SessionState::Idle {
            return Err(SessionNotIdle.into());
        }
        let mut body = api::AppendMessage {
            idempotency_key: ulid::Ulid::generate().to_string(),
            expected_head: self.session.head_sequence,
            queue,
            text,
        };
        let first = self.client.append_message(&self.id, &body).await;
        let appended = match first {
            Err(swarmy_client::Error::Api {
                status,
                body: error,
            }) if status.as_u16() == 409 && error.code == "stale_head" => {
                self.session =
                    crate::api_client::call(&self.endpoint, self.client.session(&self.id)).await?;
                if !queue && self.session.state != api::SessionState::Idle {
                    return Err(SessionNotIdle.into());
                }
                body.expected_head = self.session.head_sequence;
                crate::api_client::call(&self.endpoint, self.client.append_message(&self.id, &body))
                    .await?
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
        let session = self.client.session(&id).await?;
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
        let history = self.client.events(&id, after, 100).await?;
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
        let session = self.client.session(&old).await?;
        let Some(successor) = self.client.successor(&session).await? else {
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
            crate::api_client::call(
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
                (
                    ConversationItem::Stream(tokio::select! {
                        item = self.stream.next_item() => item?,
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

    /// Wait until the selected provider is healthy.
    ///
    /// # Errors
    /// Returns an error if the API call or event stream fails.
    pub async fn wait_healthy(&self, provider: Option<&str>) -> Result<()> {
        wait_healthy(&self.client, &self.endpoint, provider).await
    }

    /// Render the stream until the session is idle.
    ///
    /// # Errors
    /// Returns an error if the API call or event stream fails.
    pub async fn until_idle(&mut self, json: bool, run: bool, quiet: bool) -> Result<()> {
        let mut progress = TurnProgress::default();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let next = async {
                if run && !progress.started {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    Ok(tokio::time::timeout(remaining, self.next())
                        .await
                        .context("worker did not pick up session within 30 seconds")??)
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
                bail!("interrupted");
            };
            match item {
                ConversationItem::Stream(StreamItem::TokenDelta {
                    payload: api::EventPayload::TokenDelta { text, .. },
                    ..
                }) => {
                    if !quiet {
                        if json {
                            println!(
                                "{}",
                                serde_json::json!({"event":"model_delta","delta":{"Text":{"output_index":0,"text":text}}})
                            );
                        } else {
                            print!("{text}");
                            io::stdout().flush()?;
                        }
                    }
                    progress.streamed.push_str(&text);
                }
                ConversationItem::Stream(StreamItem::Event(event)) => {
                    let sequence = event.sequence;
                    if let api::EventPayload::StoreRecord { record } = event.payload
                        && self.record_event(&record, sequence, json, run, quiet, &mut progress)?
                    {
                        return Ok(());
                    }
                }
                ConversationItem::Stream(StreamItem::TokenDelta { .. }) => {}
                ConversationItem::Summarized {
                    previous_session_id,
                    session_id,
                } => {
                    report_summary(quiet, json, &previous_session_id, &session_id);
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
        run: bool,
        quiet: bool,
        progress: &mut TurnProgress,
    ) -> Result<bool> {
        if sequence < self.min_sequence {
            return Ok(false);
        }
        if !quiet && json {
            println!(
                "{}",
                serde_json::json!({"event":"session_event","value":record})
            );
        }
        let api::RecordBody::Event(event) = record else {
            return Ok(false);
        };
        match event {
            swarmy_core::Event::StateChanged { to, .. } => match to {
                swarmy_core::SessionState::Leased | swarmy_core::SessionState::WaitingInference => {
                    progress.started = true;
                }
                swarmy_core::SessionState::Completed => bail!("session completed"),
                swarmy_core::SessionState::Idle => {
                    return self.finish_idle(sequence, json, quiet, run, progress);
                }
                _ => {}
            },
            swarmy_core::Event::InferenceRequested { .. } => progress.started = true,
            swarmy_core::Event::MessageQueued { .. } if !quiet && !json => {
                println!("[queued message delivered]");
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
            swarmy_core::Event::MessageAppended { message, .. }
            | swarmy_core::Event::InferenceCompleted { message, .. } => {
                self.render_message(message, json, quiet, progress)?;
            }
            _ => {}
        }
        if !quiet && !json {
            print_tools(event)?;
        }
        Ok(false)
    }

    fn finish_idle(
        &mut self,
        sequence: u64,
        json: bool,
        quiet: bool,
        run: bool,
        progress: &mut TurnProgress,
    ) -> Result<bool> {
        if !quiet && !json && !progress.streamed.is_empty() && !progress.streamed.ends_with('\n') {
            println!();
        }
        if !quiet && json {
            println!(
                "{}",
                serde_json::json!({"event":"session_idle","session_id":self.id})
            );
        }
        self.min_sequence = sequence;
        self.session.head_sequence = sequence;
        self.session.state = api::SessionState::Idle;
        self.current_turn = None;
        if let Some(sender) = &self.observer {
            let _ = sender.send(swarmy_core::TurnStage::InputEnabled);
        }
        if let Some(outcome) = turn_outcome(progress, run) {
            progress.error.take();
            bail!("{outcome}");
        }
        Ok(true)
    }

    fn render_message(
        &mut self,
        message: &swarmy_core::Message,
        json: bool,
        quiet: bool,
        progress: &mut TurnProgress,
    ) -> Result<()> {
        let Some(text) = assistant_reply_text(message) else {
            return Ok(());
        };
        progress.reply = true;
        progress.error = None;
        self.last_text.clone_from(&text);
        if let Some(sender) = &self.observer {
            let _ = sender.send(swarmy_core::TurnStage::FinalTextRendered);
        }
        if quiet {
            return Ok(());
        }
        if json {
            println!(
                "{}",
                serde_json::json!({"event":"assistant_message","text":text})
            );
        } else {
            let remaining = text.strip_prefix(&progress.streamed).unwrap_or(&text);
            print!("{remaining}");
            if !text.ends_with('\n') {
                println!();
            }
            io::stdout().flush()?;
            progress.streamed.clear();
        }
        Ok(())
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

pub fn report_summary(quiet: bool, json: bool, previous_session_id: &str, session_id: &str) {
    if quiet {
        return;
    }
    if json {
        println!(
            "{}",
            serde_json::json!({"event":"session_summarized","previous_session_id":previous_session_id,"session_id":session_id})
        );
    } else {
        eprintln!(
            "Conversation summarized. Session {previous_session_id} archived; continuing in {session_id}."
        );
    }
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
    let alive = |role: &str| services.iter().any(|s| s.alive && s.role == role);
    if !alive("worker") {
        "No worker is alive"
    } else if !alive("scheduler") {
        "No scheduler is alive"
    } else if !services.iter().any(|s| {
        s.alive
            && s.role == "gateway"
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
    fn service(role: &str, providers: &[&str], alive: bool) -> api::ServiceHealth {
        api::ServiceHealth {
            role: role.into(),
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
    fn health_reports_missing_services_and_provider() {
        let mut services = vec![
            service("worker", &[], false),
            service("scheduler", &[], true),
            service("gateway", &["fake"], true),
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
