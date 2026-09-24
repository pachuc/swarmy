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
    client: Client,
    stream: EventStream,
    pending: Option<StreamItem>,
    min_sequence: u64,
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
            let image = image.as_deref().map(image_ref).transpose()?;
            client
                .create_session(&api::CreateSession {
                    idempotency_key: ulid::Ulid::generate().to_string(),
                    agent_id: agent_record.as_ref().map(|a| a.id.clone()),
                    new,
                    image,
                    provider: selection.provider,
                    model: selection.model,
                    effort: selection
                        .effort
                        .map(serde_json::to_value)
                        .transpose()?
                        .map(serde_json::from_value)
                        .transpose()?,
                })
                .await?
        };
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
            session,
            last_text: String::new(),
            tool_count: 0,
            client,
            stream,
            pending: None,
        })
    }

    pub async fn send(&mut self, text: String) -> Result<String> {
        ensure!(!text.trim().is_empty(), "message is empty");
        self.last_text.clear();
        self.tool_count = 0;
        // A fresh head is required for the atomic idle-and-head append check.
        self.session = self.client.session(&self.id).await?;
        ensure!(
            self.session.state == api::SessionState::Idle,
            "session is not idle"
        );
        let appended = self
            .client
            .append_message(
                &self.id,
                &api::AppendMessage {
                    idempotency_key: ulid::Ulid::generate().to_string(),
                    expected_head: self.session.head_sequence,
                    text,
                },
            )
            .await?;
        self.min_sequence = appended.sequence;
        self.session.head_sequence = appended.sequence;
        self.session.state = api::SessionState::Runnable;
        Ok(appended.turn_id)
    }

    pub async fn next(&mut self) -> Result<StreamItem> {
        if let Some(item) = self.pending.take() {
            return Ok(item);
        }
        Ok(self.stream.next_item().await?)
    }

    pub fn queue(&mut self, item: StreamItem) {
        self.pending = Some(item);
    }

    pub async fn wait_healthy(&self, provider: Option<&str>) -> Result<()> {
        let mut last = String::new();
        loop {
            let health = self.client.health().await?;
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

    pub async fn until_idle(&mut self, json: bool, run: bool) -> Result<()> {
        let mut progress = TurnProgress::default();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let item = if run && !progress.started {
                let remaining = deadline.saturating_duration_since(Instant::now());
                tokio::time::timeout(remaining, self.next())
                    .await
                    .context("worker did not pick up session within 30 seconds")??
            } else {
                self.next().await?
            };
            match item {
                StreamItem::TokenDelta {
                    payload: api::EventPayload::TokenDelta { text, .. },
                    ..
                } => {
                    if json {
                        println!(
                            "{}",
                            serde_json::json!({"event":"model_delta","delta":{"text":{"output_index":0,"text":text}}})
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
        if record.get("tool_call_completed").is_some() {
            self.tool_count += 1;
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
