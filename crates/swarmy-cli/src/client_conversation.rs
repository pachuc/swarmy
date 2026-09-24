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
    stream: EventStream,
    pending: std::collections::VecDeque<StreamItem>,
    min_sequence: u64,
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

pub async fn wait_healthy(client: &Client, provider: Option<&str>) -> Result<()> {
    let (_, endpoint) = crate::api_client::connect()?;
    let mut last = String::new();
    loop {
        let health = crate::api_client::call(&endpoint, client.health()).await?;
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
            id.is_none() || (image.is_none() && agent.is_none()),
            "session id cannot be combined with --image or --agent"
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
        Ok(Self {
            provider,
            id: session.id.clone(),
            agent_name: agent_record.map(|a| a.name),
            created,
            min_sequence: session.head_sequence,
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
            stream,
            pending: std::collections::VecDeque::new(),
        })
    }

    pub async fn send(&mut self, text: String) -> Result<String> {
        ensure!(!text.trim().is_empty(), "message is empty");
        self.last_text.clear();
        self.tool_count = 0;
        self.tool_result = None;
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
                let (_, endpoint) = crate::api_client::connect()?;
                self.session =
                    crate::api_client::call(&endpoint, self.client.session(&self.id)).await?;
                ensure!(
                    self.session.state == api::SessionState::Idle,
                    "session is not idle"
                );
                body.expected_head = self.session.head_sequence;
                crate::api_client::call(&endpoint, self.client.append_message(&self.id, &body))
                    .await?
            }
            other => other?,
        };
        self.current_turn = Some(appended.turn_id.clone());
        self.min_sequence = appended.sequence;
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

    pub async fn interrupt(&mut self) -> Result<()> {
        if self.current_turn.is_some() {
            let (_, endpoint) = crate::api_client::connect()?;
            crate::api_client::call(
                &endpoint,
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
                            let session = self.client.session(&self.id).await?;
                            if session.state == api::SessionState::Idle && session.head_sequence >= self.min_sequence {
                                let history = self.client.events(&self.id, self.min_sequence.saturating_sub(1), 100).await?;
                                for event in history {
                                    if event.sequence <= session.head_sequence { self.pending.push_back(StreamItem::Event(event)); }
                                }
                            self.session = session;
                            self.current_turn = None;
                            self.pending.push_back(StreamItem::Event(api::Event {
                                    log_id: self.session.log_id.clone(), sequence: self.session.head_sequence,
                                    payload: api::EventPayload::StoreRecord {
                                        record: serde_json::json!({"state_changed":{"from":"runnable","to":"idle","seq":self.session.head_sequence}}),
                                    },
                                }));
                                continue;
                            }
                            continue;
                        }
                    },
                    false,
                )
            };
            if let StreamItem::Event(event) = &item {
                if event.log_id != self.session.log_id
                    || (!queued && event.sequence < self.min_sequence)
                {
                    continue;
                }
                if let api::EventPayload::StoreRecord { record } = &event.payload
                    && record.get("state_changed").and_then(|v| v.get("to"))
                        == Some(&serde_json::json!("completed"))
                {
                    let old = self.id.clone();
                    let session = self.client.session(&old).await?;
                    if let Some(successor) = self.client.successor(&session).await? {
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
                        self.current_turn = None;
                        return Ok(StreamItem::Event(api::Event {
                            log_id: self.session.log_id.clone(),
                            sequence: 0,
                            payload: api::EventPayload::StoreRecord {
                                record: serde_json::json!({"session_summarized":{"previous_session_id":old,"session_id":next}}),
                            },
                        }));
                    }
                }
                if let api::EventPayload::StoreRecord { record } = &event.payload
                    && record.get("state_changed").and_then(|v| v.get("to"))
                        == Some(&serde_json::json!("idle"))
                {
                    self.session.state = api::SessionState::Idle;
                    self.session.head_sequence = event.sequence;
                    self.min_sequence = event.sequence;
                    self.current_turn = None;
                }
            }
            return Ok(item);
        }
    }

    pub fn queue(&mut self, item: StreamItem) {
        self.pending.push_back(item);
    }

    pub async fn wait_healthy(&self, provider: Option<&str>) -> Result<()> {
        wait_healthy(&self.client, provider).await
    }

    pub async fn until_idle(&mut self, json: bool, run: bool) -> Result<()> {
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
                    if json {
                        println!(
                            "{}",
                            serde_json::json!({"event":"model_delta","delta":{"Text":{"output_index":0,"text":text}}})
                        );
                    } else {
                        print!("{text}");
                        io::stdout().flush()?;
                    }
                    progress.streamed.push_str(&text);
                }
                StreamItem::Event(event) => {
                    let api::EventPayload::StoreRecord { record } = event.payload else {
                        continue;
                    };
                    if self.record_event(&record, event.sequence, json, run, &mut progress)? {
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
        progress: &mut TurnProgress,
    ) -> Result<bool> {
        if sequence < self.min_sequence {
            return Ok(false);
        }
        if let Some(summary) = record.get("session_summarized") {
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
            progress.started = true;
            return Ok(false);
        }
        if json {
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
                if !json && !progress.streamed.is_empty() && !progress.streamed.ends_with('\n') {
                    println!();
                }
                if json {
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
                if let Some(error) = progress.error.take() {
                    bail!("{error}");
                }
                ensure!(
                    !run || progress.reply,
                    "turn ended without a completed assistant reply"
                );
                return Ok(true);
            }
        }
        if record.get("inference_requested").is_some() {
            progress.started = true;
        }
        if !json {
            print_tools(record);
        }
        if let Some(result) = record
            .get("tool_call_completed")
            .and_then(|v| v.get("result"))
        {
            self.tool_count += 1;
            self.tool_result = Some(result.clone());
        }
        if let Some(failure) = record
            .get("tool_call_completed")
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("error"))
        {
            progress.error = Some(format!(
                "tool failed: {}",
                failure
                    .get("error")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown error")
            ));
        }
        if let Some(failure) = record.get("inference_failed") {
            progress.error = failure
                .get("error")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
        }
        if let Some(message) = record
            .get("inference_completed")
            .or_else(|| record.get("message_appended"))
            .and_then(|event| event.get("message"))
        {
            self.render_message(message, json, progress)?;
        }
        Ok(false)
    }

    fn render_message(
        &mut self,
        message: &serde_json::Value,
        json: bool,
        progress: &mut TurnProgress,
    ) -> Result<()> {
        if message.get("role").and_then(serde_json::Value::as_str) != Some("assistant") {
            return Ok(());
        }
        let parts = message.get("parts").and_then(serde_json::Value::as_array);
        if parts.is_some_and(|parts| parts.iter().any(|part| part.get("tool_call").is_some())) {
            return Ok(());
        }
        let text = parts
            .into_iter()
            .flatten()
            .filter_map(|part| {
                part.get("text")
                    .and_then(|v| v.get("text"))
                    .and_then(serde_json::Value::as_str)
            })
            .collect::<String>();
        if text.is_empty() {
            return Ok(());
        }
        progress.reply = true;
        progress.error = None;
        self.last_text.clone_from(&text);
        if let Some(sender) = &self.observer {
            let _ = sender.send(swarmy_core::TurnStage::FinalTextRendered);
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

#[derive(Default)]
struct TurnProgress {
    reply: bool,
    error: Option<String>,
    streamed: String,
    started: bool,
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
}
