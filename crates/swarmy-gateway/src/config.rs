use std::{collections::BTreeMap, env, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use serde::Deserialize;
use swarmy_bus::{Config as BusConfig, SubjectToken};
use swarmy_core::{MessageRole, Part, ToolCallId};
use swarmy_llm::{
    Provider, ProviderStream, Request, Response, StopReason, TokenUsage, auth::FileCredentialStore,
    chatgpt::ChatGptProvider, fake::FakeProvider,
};
use tokio::io::AsyncWriteExt;

pub struct Config {
    pub cluster: String,
    pub directory: Vec<String>,
    pub nats: String,
    pub bus: BusConfig,
    pub class: SubjectToken,
    pub concurrency: usize,
    pub provider: Arc<dyn Provider>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let class = env::var("SWARMY_PROVIDER").context("SWARMY_PROVIDER is required")?;
        let provider: Arc<dyn Provider> = match class.as_str() {
            "fake" => Arc::new(FileFake::from_env()?),
            "chatgpt" => Arc::new(ChatGptProvider::new(Arc::new(FileCredentialStore::new(
                env::var("SWARMY_CHATGPT_AUTH").context("SWARMY_CHATGPT_AUTH is required")?,
            )))?),
            _ => bail!("unsupported SWARMY_PROVIDER: {class}"),
        };
        let concurrency = setting("SWARMY_GATEWAY_CONCURRENCY", 4_usize)?;
        let ack_wait = Duration::from_millis(setting("SWARMY_BUS_ACK_WAIT_MS", 30_000_u64)?);
        if concurrency == 0 || ack_wait < Duration::from_millis(30) {
            bail!("concurrency must be positive and ack wait at least 30 ms");
        }
        Ok(Self {
            cluster: env::var("SWARMY_FDB_CLUSTER_FILE")?,
            directory: env::var("SWARMY_STORE_DIRECTORY")
                .unwrap_or_else(|_| "swarmy".into())
                .split('/')
                .map(str::to_owned)
                .collect(),
            nats: env::var("SWARMY_NATS_URL")?,
            bus: BusConfig {
                prefix: env::var("SWARMY_BUS_PREFIX")
                    .ok()
                    .map(SubjectToken::new)
                    .transpose()?,
                ack_wait,
                max_deliver: setting("SWARMY_BUS_MAX_DELIVER", 5_i64)?,
            },
            class: SubjectToken::new(class)?,
            concurrency,
            provider,
        })
    }
}

fn setting<T: std::str::FromStr>(name: &str, default: T) -> Result<T> {
    match env::var(name) {
        Ok(value) => value.parse().map_err(|_| anyhow::anyhow!("invalid {name}")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

#[derive(Deserialize)]
struct Script {
    #[serde(default)]
    latency_ms: u64,
    #[serde(default)]
    responses: BTreeMap<usize, Response>,
    #[serde(default)]
    fail: bool,
    #[serde(default)]
    request_based: Option<RequestScript>,
}

/// Steps are zero based assistant-message counts, independent of process history.
#[derive(Clone, Deserialize)]
struct RequestScript {
    steps: usize,
    tool_steps: Vec<usize>,
    final_answer: String,
}

impl RequestScript {
    fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            self.steps > 0,
            "request-based script needs at least one step"
        );
        anyhow::ensure!(
            self.tool_steps.iter().all(|step| *step < self.steps - 1),
            "tool steps must precede the final step"
        );
        Ok(())
    }

    fn response(&self, request: &Request) -> Result<Response, swarmy_llm::Error> {
        let step = request
            .messages
            .iter()
            .filter(|message| message.role == MessageRole::Assistant)
            .count();
        if step >= self.steps {
            return Err(swarmy_llm::Error::UnscriptedTurn(step));
        }
        let tool = self.tool_steps.contains(&step);
        Ok(Response {
            parts: vec![if tool {
                Part::ToolCall {
                    call_id: ToolCallId(format!("clock-{step}")),
                    tool: "get_time".into(),
                    input: serde_json::json!({}),
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
            usage: TokenUsage::default(),
        })
    }
}

struct FileFake {
    provider: Arc<FakeProvider>,
    log: PathBuf,
    fail: bool,
    latency: Duration,
    request_based: Option<RequestScript>,
}

impl FileFake {
    fn from_env() -> Result<Self> {
        let script: Script =
            serde_json::from_slice(&std::fs::read(env::var("SWARMY_FAKE_SCRIPT")?)?)?;
        if let Some(mode) = &script.request_based {
            mode.validate()?;
            anyhow::ensure!(
                script.responses.is_empty(),
                "choose responses or request_based, not both"
            );
        }
        let mut provider = FakeProvider::default();
        provider.responses = script.responses;
        // Delay each emitted delta in the wrapper so tests can kill a partial stream.
        Ok(Self {
            provider: Arc::new(provider),
            log: env::var("SWARMY_FAKE_CALL_LOG")?.into(),
            fail: script.fail,
            request_based: script.request_based,
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
        let request_based = self.request_based.clone();
        Box::pin(async_stream::try_stream! {
            let mut file = tokio::fs::OpenOptions::new().create(true).append(true).open(log).await?;
            file.write_all(b"call\n").await?;
            file.sync_data().await?;
            if fail {
                tokio::time::sleep(latency).await;
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
                tokio::time::sleep(latency).await;
                yield delta?;
            }
        })
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
}
