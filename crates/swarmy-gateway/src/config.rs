use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::{Error, Result};
use futures::StreamExt;
use serde::Deserialize;
use swarmy_bus::{Config as BusConfig, SubjectToken};
use swarmy_core::{MessageRole, Part, ToolCallId};
use swarmy_llm::{
    Provider, ProviderStream, Request, Response, StopReason, TokenUsage,
    fake::{FakeFailure, FakeProvider},
};
use tokio::io::AsyncWriteExt;

pub struct Config {
    pub cluster: String,
    pub directory: Vec<String>,
    pub nats: String,
    pub bus: BusConfig,
    pub settings: swarmy_config::Settings,
    pub concurrency: usize,
    pub resend_interval: Duration,
}

impl Config {
    /// Load and validate gateway settings.
    /// # Errors
    /// Returns invalid settings or transport configuration.
    pub fn from_env() -> Result<Self> {
        let settings = swarmy_config::Settings::load()?.settings;
        let concurrency = settings.gateway_concurrency;
        let ack_wait = Duration::from_millis(settings.bus_ack_wait_ms);
        if concurrency == 0 || ack_wait < Duration::from_millis(30) {
            return Err(Error::Configuration(
                "concurrency must be positive and ack wait at least 30 ms",
            ));
        }
        Ok(Self {
            settings: settings.clone(),
            cluster: settings.fdb_cluster_file,
            directory: settings
                .store_directory
                .split('/')
                .map(str::to_owned)
                .collect(),
            nats: settings.nats_url,
            bus: BusConfig {
                prefix: if settings.bus_prefix.is_empty() {
                    None
                } else {
                    Some(SubjectToken::new(settings.bus_prefix)?)
                },
                ack_wait,
                max_deliver: settings.bus_max_deliver,
            },
            resend_interval: Duration::from_millis(settings.scheduler_resend_interval_ms),
            concurrency,
        })
    }
}

#[derive(Deserialize)]
struct Script {
    #[serde(default)]
    latency_ms: u64,
    #[serde(default)]
    responses: BTreeMap<usize, Response>,
    #[serde(default)]
    failures: BTreeMap<usize, FakeFailure>,
    #[serde(default)]
    fail: bool,
    #[serde(default)]
    request_based: Option<RequestScript>,
    #[serde(default)]
    request_by_prompt: BTreeMap<String, RequestScript>,
}

/// Steps are zero based assistant-message counts, independent of process history.
#[derive(Clone, Deserialize)]
struct RequestScript {
    steps: usize,
    tool_steps: Vec<usize>,
    final_answer: String,
    #[serde(default)]
    bash_command: Option<String>,
}

impl RequestScript {
    fn validate(&self) -> Result<()> {
        ensure(
            self.steps > 0,
            "request-based script needs at least one step",
        )?;
        ensure(
            self.tool_steps.iter().all(|step| *step < self.steps - 1),
            "tool steps must precede the final step",
        )?;
        Ok(())
    }

    fn response(&self, request: &Request) -> std::result::Result<Response, swarmy_llm::Error> {
        // Count the assistant messages since the last user message, so every
        // user turn in a multi-turn conversation starts the script again.
        let step = request
            .messages
            .iter()
            .rev()
            .take_while(|message| message.role != MessageRole::User)
            .filter(|message| message.role == MessageRole::Assistant)
            .count();
        if step >= self.steps {
            return Err(swarmy_llm::Error::UnscriptedTurn(step));
        }
        let tool = self.tool_steps.contains(&step);
        // Scripted answers carry non-zero usage so derived throughput
        // (output tokens over streaming duration) is populated in durable
        // turn metrics; zero usage would leave it null.
        let output_tokens = u64::try_from(self.final_answer.len())
            .unwrap_or(u64::MAX)
            .max(1);
        Ok(Response {
            parts: vec![if tool {
                Part::ToolCall {
                    call_id: ToolCallId(format!("clock-{step}")),
                    tool: if self.bash_command.is_some() {
                        "bash"
                    } else {
                        "get_time"
                    }
                    .into(),
                    input: self.bash_command.as_ref().map_or_else(
                        || serde_json::json!({}),
                        |command| serde_json::json!({"command": command, "timeout_ms": 120_000}),
                    ),
                }
            } else {
                Part::Text {
                    text: self.final_answer.clone(),
                }
            }],
            stop_reason: if tool {
                StopReason::ToolCalls
            } else {
                StopReason::EndTurn
            },
            usage: TokenUsage {
                input_tokens: 10,
                output_tokens,
                total_tokens: 10 + output_tokens,
                ..TokenUsage::default()
            },
            quota_remaining: std::collections::BTreeMap::new(),
            quota_resets: std::collections::BTreeMap::new(),
        })
    }
}

pub struct FileFake {
    provider: Arc<FakeProvider>,
    log: PathBuf,
    fail: bool,
    latency: Duration,
    request_based: Option<RequestScript>,
    request_by_prompt: BTreeMap<String, RequestScript>,
    failures: BTreeMap<usize, FakeFailure>,
    calls: AtomicUsize,
}

impl FileFake {
    /// Load a deterministic fake script.
    /// # Errors
    /// Returns an unreadable or invalid script.
    pub fn from_settings(settings: &swarmy_config::Settings) -> Result<Self> {
        let script: Script = serde_json::from_slice(&std::fs::read(&settings.fake.script)?)?;
        if let Some(mode) = &script.request_based {
            mode.validate()?;
            ensure(
                script.responses.is_empty(),
                "choose responses or request_based, not both",
            )?;
        }
        for mode in script.request_by_prompt.values() {
            mode.validate()?;
        }
        ensure(
            script.request_by_prompt.is_empty()
                || (script.responses.is_empty() && script.request_based.is_none()),
            "choose request_by_prompt, responses, or request_based",
        )?;
        let mut provider = FakeProvider::default();
        provider.responses = script.responses;
        provider.failures = script.failures.clone();
        // Delay each emitted delta in the wrapper so tests can kill a partial stream.
        Ok(Self {
            provider: Arc::new(provider),
            log: settings.fake.call_log.clone().into(),
            fail: script.fail,
            request_based: script.request_based,
            request_by_prompt: script.request_by_prompt,
            failures: script.failures,
            calls: AtomicUsize::new(0),
            latency: Duration::from_millis(script.latency_ms),
        })
    }
}

impl Provider for FileFake {
    fn request(&self, request: Request) -> ProviderStream {
        let provider = self.provider.clone();
        let log = self.log.clone();
        let fail = self.fail;
        let latency = self.latency;
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let prompt = request
            .messages
            .iter()
            .rev()
            .find(|message| message.role == MessageRole::User)
            .map(|message| {
                message
                    .parts
                    .iter()
                    .filter_map(|part| match part {
                        Part::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<String>()
            });
        let request_based = prompt
            .as_ref()
            .and_then(|prompt| self.request_by_prompt.get(prompt))
            .cloned()
            .or_else(|| self.request_based.clone());
        let failure = request_based
            .as_ref()
            .and_then(|_| self.failures.get(&call))
            .cloned();
        Box::pin(async_stream::try_stream! {
            // One JSON entry per line so provider-switch tests can assert on
            // the exact history each provider received; line counters elsewhere
            // keep working unchanged.
            let logged = serde_json::json!({"messages": request.messages});
            let mut line =
                serde_json::to_string(&logged).unwrap_or_else(|_| "{\"messages\":[]}".into());
            line.push('\n');
            let mut file = tokio::fs::OpenOptions::new().create(true).append(true).open(log).await?;
            file.write_all(line.as_bytes()).await?;
            file.sync_data().await?;
            if let Some(failure) = failure {
                Err(failure.error())?;
            }
            if fail {
                if !latency.is_zero() {
                    tokio::time::sleep(latency).await;
                }
                Err(swarmy_llm::Error::Protocol("scripted provider failure".into()))?;
            }
            let mut stream = if let Some(script) = request_based {
                let mut per_request = FakeProvider::default();
                per_request.responses.insert(0, script.response(&request)?);
                per_request.request(request)
            } else {
                provider.request(request)
            };
            while let Some(delta) = stream.next().await {
                if !latency.is_zero() {
                    tokio::time::sleep(latency).await;
                }
                yield delta?;
            }
        })
    }
}

fn ensure(condition: bool, reason: &'static str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(Error::Configuration(reason))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use swarmy_core::{Message, MessageId};
    use swarmy_llm::{Delta, GenerationSettings};

    #[tokio::test]
    async fn request_script_survives_interleaving_retries_and_restart() {
        let files = tempfile::tempdir().unwrap();
        let script: Script = serde_json::from_value(serde_json::json!({
            "request_based": {"steps": 3, "tool_steps": [0, 1], "final_answer": "done"}
        }))
        .unwrap();
        let make_provider = || FileFake {
            provider: Arc::new(FakeProvider::default()),
            log: files.path().join("calls"),
            fail: false,
            latency: Duration::ZERO,
            request_based: script.request_based.clone(),
            request_by_prompt: BTreeMap::new(),
            failures: BTreeMap::new(),
            calls: AtomicUsize::new(0),
        };
        let mut provider = make_provider();
        for step in [0, 1, 0, 2, 1, 2] {
            let mut request = Request {
                system_prompt: String::new(),
                messages: Vec::new(),
                tools: Vec::new(),
                settings: GenerationSettings::default(),
            };
            for role in std::iter::once(MessageRole::User)
                .chain((0..step).flat_map(|_| [MessageRole::Assistant, MessageRole::Tool]))
            {
                request.messages.push(Message {
                    id: MessageId::from_ulid(ulid::Ulid::generate()),
                    role,
                    parts: Vec::new(),
                });
            }
            let deltas: Vec<_> = provider.request(request).collect().await;
            let Delta::Completed(response) = deltas.last().unwrap().as_ref().unwrap() else {
                panic!("missing completion");
            };
            if step == 2 {
                assert_eq!(
                    response.parts,
                    vec![Part::Text {
                        text: "done".into()
                    }]
                );
                assert_eq!(response.stop_reason, StopReason::EndTurn);
                provider = make_provider();
            } else {
                assert!(
                    matches!(&response.parts[0], Part::ToolCall { call_id, tool, .. }
                    if call_id.0 == format!("clock-{step}") && tool == "get_time")
                );
            }
        }
        assert_eq!(
            std::fs::read_to_string(files.path().join("calls"))
                .unwrap()
                .lines()
                .count(),
            6
        );
    }

    #[test]
    fn request_based_scripts_restart_on_each_user_turn() {
        let script: RequestScript = serde_json::from_value(serde_json::json!({
            "steps": 1, "tool_steps": [], "final_answer": "hi"
        }))
        .unwrap();
        let text = |role, text: &str| swarmy_core::Message {
            id: swarmy_core::MessageId::from_ulid(ulid::Ulid::generate()),
            role,
            parts: vec![swarmy_core::Part::Text { text: text.into() }],
        };
        let mut request = Request {
            system_prompt: String::new(),
            messages: vec![text(MessageRole::User, "one")],
            tools: Vec::new(),
            settings: swarmy_llm::GenerationSettings::default(),
        };
        assert!(script.response(&request).is_ok());
        request.messages.push(text(MessageRole::Assistant, "hi"));
        assert!(
            script.response(&request).is_err(),
            "no second step within one turn"
        );
        request.messages.push(text(MessageRole::User, "two"));
        assert!(
            script.response(&request).is_ok(),
            "a new user turn restarts the script"
        );
    }
}
