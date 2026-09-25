//! Human-facing conversation commands use only the public API.
use std::{
    io::{self, Write},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use swarmy_api_types as api;
use swarmy_client::{Client, EventStream, StreamItem};

pub struct Conversation {
    pub id: String,
    pub agent_name: Option<String>,
    pub created: bool,
    pub provider: Option<String>,
    pub session: api::Session,
    pub last_text: String,
    pub tool_count: usize,
    pub tool_result: Option<serde_json::Value>,
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

pub async fn wait_healthy(client: &Client, endpoint: &str, provider: Option<&str>) -> Result<()> {
    let mut last = String::new();
    loop {
        let health = crate::api_client::call(endpoint, client.health()).await?;
        let services: Vec<api::ServiceHealth> = serde_json::from_value(
            health
                .get("services")
                .cloned()
                .context("health has no services")?,
        )?;
        let provider = provider.or_else(|| {
            health
                .get("default_provider")
                .and_then(serde_json::Value::as_str)
        });
        let problem = service_problem(&services, provider);
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

fn print_tools(record: &serde_json::Value) {
    if let Some(call) = record
        .get("tool_call_requested")
        .and_then(|v| v.get("call"))
    {
        println!(
            "Tool call {} {} {}",
            call.get("call_id").unwrap_or(&serde_json::Value::Null),
            call.get("tool").unwrap_or(&serde_json::Value::Null),
            call.get("arguments").unwrap_or(&serde_json::Value::Null)
        );
    }
    if let Some(completed) = record.get("tool_call_completed") {
        println!(
            "Tool result {} {}",
            completed.get("call_id").unwrap_or(&serde_json::Value::Null),
            completed.get("result").unwrap_or(&serde_json::Value::Null)
        );
    }
}

impl Conversation {
    pub async fn open(
        client: Client,
        id: Option<String>,
        image: Option<String>,
        agent: Option<String>,
        new: bool,
        selection: swarmy_core::InferenceSelection,
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
            create_session(
                &client,
                image.as_deref(),
                agent_record.as_ref().map(|a| a.id.clone()),
                new,
                selection,
            )
            .await?
        };
        let mut session = session;
        while session.state == api::SessionState::Completed {
            let Some(next) = client.successor(&session).await? else {
                break;
            };
            session = next;
        }
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
        let head = session.head_sequence;
        Ok(Self {
            provider,
            id: session.id.clone(),
            agent_name: agent_record.map(|a| a.name),
            created,
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

    pub async fn send(&mut self, text: String) -> Result<String> {
        ensure!(!text.trim().is_empty(), "message is empty");
        self.last_text.clear();
        self.tool_count = 0;
        self.tool_result = None;
        self.pending.clear();
        ensure!(
            self.session.state == api::SessionState::Idle,
            "session is not idle"
        );
        let mut body = api::AppendMessage {
            idempotency_key: ulid::Ulid::generate().to_string(),
            expected_head: self.session.head_sequence,
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
                ensure!(
                    self.session.state == api::SessionState::Idle,
                    "session is not idle"
                );
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
        self.session.state = api::SessionState::Runnable;
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
        let session = self.client.session(&self.id).await?;
        if !(session.state == api::SessionState::Idle && session.head_sequence >= self.min_sequence)
        {
            return Ok(false);
        }
        // Replay only what the stream has not delivered yet. The delivered
        // cursor advances on return, so also skip sequences already waiting
        // in pending when two ticks fire before the queue drains.
        let after = self.pending_after();
        let history = self.client.events(&self.id, after, 100).await?;
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
                record: serde_json::json!({"state_changed":{"from":"runnable","to":"idle","seq":head}}),
            },
        }));
    }

    async fn take_successor(&mut self) -> Result<Option<StreamItem>> {
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
        Ok(Some(StreamItem::Event(api::Event {
            log_id: self.session.log_id.clone(),
            sequence: 0,
            payload: api::EventPayload::StoreRecord {
                record: serde_json::json!({"session_summarized":{"previous_session_id":old,"session_id":next}}),
            },
        })))
    }

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

    pub async fn next(&mut self) -> Result<StreamItem> {
        loop {
            let (item, queued) = if let Some(item) = self.pending.pop_front() {
                (item, true)
            } else {
                (
                    tokio::select! {
                        item = self.stream.next_item() => item?,
                        _ = self.poll.tick(), if self.current_turn.is_some() || self.session.state != api::SessionState::Idle => {
                            if self.poll_tick().await? {
                                continue;
                            }
                            continue;
                        }
                    },
                    false,
                )
            };
            if let StreamItem::Event(event) = &item {
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
            if let StreamItem::Event(event) = &item {
                self.delivered = self.delivered.max(event.sequence);
            }
            return Ok(item);
        }
    }

    pub fn queue(&mut self, item: StreamItem) {
        self.pending.push_back(item);
    }

    pub async fn wait_healthy(&self, provider: Option<&str>) -> Result<()> {
        wait_healthy(&self.client, &self.endpoint, provider).await
    }

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
                StreamItem::TokenDelta {
                    payload: api::EventPayload::TokenDelta { text, .. },
                    ..
                } => {
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
                StreamItem::Event(event) => {
                    let api::EventPayload::StoreRecord { record } = event.payload else {
                        continue;
                    };
                    if self.record_event(
                        &record,
                        event.sequence,
                        json,
                        run,
                        quiet,
                        &mut progress,
                    )? {
                        return Ok(());
                    }
                }
                StreamItem::TokenDelta { .. } => {}
            }
        }
    }

    fn record_event(
        &mut self,
        record: &serde_json::Value,
        sequence: u64,
        json: bool,
        run: bool,
        quiet: bool,
        progress: &mut TurnProgress,
    ) -> Result<bool> {
        if sequence < self.min_sequence {
            return Ok(false);
        }
        if let Some(summary) = record.get("session_summarized") {
            report_summary(quiet, json, summary);
            progress.started = true;
            return Ok(false);
        }
        if !quiet && json {
            println!(
                "{}",
                serde_json::json!({"event":"session_event","value":record})
            );
        }
        if let Some(state) = record.get("state_changed").and_then(|s| s.get("to")) {
            if state == "leased" || state == "waiting_inference" {
                progress.started = true;
            }
            if state == "completed" {
                bail!("session completed");
            }
            if state == "idle" {
                return self.finish_idle(sequence, json, quiet, run, progress);
            }
        }
        if record.get("inference_requested").is_some() {
            progress.started = true;
        }
        if !quiet && !json {
            print_tools(record);
        }
        self.track_tools(record, progress);
        if let Some(message) = record
            .get("inference_completed")
            .or_else(|| record.get("message_appended"))
            .and_then(|event| event.get("message"))
        {
            self.render_message(message, json, quiet, progress)?;
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

    fn track_tools(&mut self, record: &serde_json::Value, progress: &mut TurnProgress) {
        if let Some(result) = record
            .get("tool_call_completed")
            .and_then(|v| v.get("result"))
        {
            self.tool_count += 1;
            self.tool_result = Some(result.clone());
        }
        if let Some(error) = tool_error(record) {
            progress.error = Some(error);
        }
        // An inference record supersedes a tool error, and one without an
        // error string still clears a previous error.
        if record.get("inference_failed").is_some() {
            progress.error = inference_error(record);
        }
    }

    fn render_message(
        &mut self,
        message: &serde_json::Value,
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
fn tool_error(record: &serde_json::Value) -> Option<String> {
    let failure = record
        .get("tool_call_completed")?
        .get("result")?
        .get("error")?;
    Some(format!(
        "tool failed: {}",
        failure
            .get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown error")
    ))
}

/// The inference failure a record reports, if the record is an inference
/// failure. Missing error strings clear a previous error.
fn inference_error(record: &serde_json::Value) -> Option<String> {
    record
        .get("inference_failed")?
        .get("error")?
        .as_str()
        .map(str::to_owned)
}

/// The text of an assistant message that finishes a text turn: a non-empty
/// text reply with no tool call. Tool-call messages do not finish the turn.
fn assistant_reply_text(message: &serde_json::Value) -> Option<String> {
    if message.get("role").and_then(serde_json::Value::as_str) != Some("assistant") {
        return None;
    }
    let parts = message.get("parts").and_then(serde_json::Value::as_array)?;
    if parts.iter().any(|part| part.get("tool_call").is_some()) {
        return None;
    }
    let text = parts
        .iter()
        .filter_map(|part| {
            part.get("text")
                .and_then(|v| v.get("text"))
                .and_then(serde_json::Value::as_str)
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
    matches!(&event.payload, api::EventPayload::StoreRecord { record }
        if record.get("state_changed").and_then(|v| v.get("to"))
            == Some(&serde_json::json!("idle")))
}

fn is_completed_event(payload: &api::EventPayload) -> bool {
    matches!(payload, api::EventPayload::StoreRecord { record }
        if record.get("state_changed").and_then(|v| v.get("to"))
            == Some(&serde_json::json!("completed")))
}

fn report_summary(quiet: bool, json: bool, summary: &serde_json::Value) {
    if quiet {
        return;
    }
    if json {
        println!(
            "{}",
            serde_json::json!({"event":"session_summarized","previous_session_id":summary["previous_session_id"],"session_id":summary["session_id"]})
        );
    } else {
        eprintln!(
            "Conversation summarized. Session {} archived; continuing in {}.",
            summary["previous_session_id"], summary["session_id"]
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
                record: if idle {
                    serde_json::json!({"state_changed":{"from":"runnable","to":"idle","seq":sequence}})
                } else {
                    serde_json::json!({"message_appended":{"seq":sequence}})
                },
            },
        }
    }

    #[test]
    fn inference_error_fails_the_turn() {
        // Ported from the removed store-backed run path: an inference
        // failure fails the turn with the provider error.
        let progress = TurnProgress {
            error: inference_error(
                &serde_json::json!({"inference_failed": {"error": "provider unavailable"}}),
            ),
            ..TurnProgress::default()
        };
        assert_eq!(
            turn_outcome(&progress, true).as_deref(),
            Some("provider unavailable")
        );
    }

    #[test]
    fn idle_after_assistant_reply_completes_the_turn() {
        // Ported from the removed store-backed run path: a text reply
        // without a tool call completes the turn and clears a prior error.
        let mut progress = TurnProgress {
            error: Some("provider unavailable".into()),
            ..TurnProgress::default()
        };
        let text = assistant_reply_text(
            &serde_json::json!({"role": "assistant", "parts": [{"text": {"text": "ready"}}]}),
        );
        assert_eq!(text.as_deref(), Some("ready"));
        progress.reply = true;
        progress.error = None;
        assert_eq!(turn_outcome(&progress, true), None);
    }

    #[test]
    fn tool_call_message_does_not_finish_the_turn() {
        // Tool-call assistant messages do not finish a text turn.
        let text = assistant_reply_text(
            &serde_json::json!({"role": "assistant", "parts": [{"tool_call": {"id": "1"}}]}),
        );
        assert_eq!(text, None);
    }

    #[test]
    fn tool_error_without_followup_reply_fails_the_turn() {
        // Ported from the removed store-backed run path: a tool error with
        // no follow-up reply fails the turn.
        let progress = TurnProgress {
            error: tool_error(
                &serde_json::json!({"tool_call_completed": {"result": {"error": {"error": "timeout"}}}}),
            ),
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
