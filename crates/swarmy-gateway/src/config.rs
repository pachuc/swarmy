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
}
