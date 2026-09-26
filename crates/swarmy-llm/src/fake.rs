//! Deterministic provider for control-plane tests without external quota.
use crate::{Delta, Error, Provider, ProviderStream, Request, Response, StopReason, TokenUsage};
use futures::StreamExt as _;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use swarmy_core::{MessageRole, Part, ToolCallId};
use tokio::io::AsyncWriteExt;

#[derive(Clone, Debug, serde::Deserialize)]
pub struct FakeFailure {
    pub status: u16,
    pub message: String,
    #[serde(default)]
    pub retry_after_seconds: Option<u64>,
}

impl FakeFailure {
    #[must_use]
    pub fn error(&self) -> Error {
        Error::ProviderResponse {
            status: reqwest::StatusCode::from_u16(self.status)
                .unwrap_or(reqwest::StatusCode::INTERNAL_SERVER_ERROR),
            message: self.message.clone(),
            retry_after: self.retry_after_seconds.map(Duration::from_secs),
        }
    }
}

#[derive(Default)]
pub struct FakeProvider {
    pub latency: Duration,
    /// Turns are zero based and assigned when `request` is called.
    pub responses: BTreeMap<usize, Response>,
    pub failures: BTreeMap<usize, FakeFailure>,
    /// Tool calls take precedence over a response for the same turn.
    pub tool_calls: Option<BTreeMap<usize, Vec<Part>>>,
    calls: AtomicUsize,
    requests: std::sync::Mutex<Vec<Request>>,
}

impl FakeProvider {
    /// Returns requests captured before the provider scripted its responses.
    ///
    /// # Panics
    /// Panics if a prior test poisoned the request lock.
    #[must_use]
    pub fn requests(&self) -> Vec<Request> {
        self.requests
            .lock()
            .expect("fake request lock poisoned")
            .clone()
    }

    #[must_use]
    pub fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl Provider for FakeProvider {
    fn request(&self, request: Request) -> ProviderStream {
        self.requests
            .lock()
            .expect("fake request lock poisoned")
            .push(request);
        let turn = self.calls.fetch_add(1, Ordering::SeqCst);
        let response = self
            .tool_calls
            .as_ref()
            .and_then(|script| script.get(&turn))
            .map(|parts| Response {
                parts: parts.clone(),
                stop_reason: StopReason::ToolCalls,
                usage: TokenUsage::default(),
                quota_remaining: BTreeMap::new(),
                quota_resets: BTreeMap::new(),
            })
            .or_else(|| self.responses.get(&turn).cloned());
        let failure = self.failures.get(&turn).cloned();
        let latency = self.latency;
        Box::pin(async_stream::try_stream! {
            tokio::time::sleep(latency).await;
            if let Some(failure) = failure { Err(failure.error())?; }
            let response = response.ok_or(Error::UnscriptedTurn(turn))?;
            for (output_index, part) in response.parts.iter().enumerate() {
                yield Delta::PartDone { output_index, part: part.clone() };
            }
            yield Delta::Completed(response);
        })
    }
}

#[derive(serde::Deserialize)]
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
#[derive(Clone, serde::Deserialize)]
struct RequestScript {
    steps: usize,
    tool_steps: Vec<usize>,
    final_answer: String,
    #[serde(default)]
    bash_command: Option<String>,
}

impl RequestScript {
    fn validate(&self) -> Result<(), Error> {
        if self.steps == 0 {
            return Err(Error::Protocol(
                "request-based script needs at least one step".into(),
            ));
        }
        if !self.tool_steps.iter().all(|step| *step < self.steps - 1) {
            return Err(Error::Protocol(
                "tool steps must precede the final step".into(),
            ));
        }
        Ok(())
    }

    fn response(&self, request: &Request) -> Result<Response, Error> {
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
            return Err(Error::UnscriptedTurn(step));
        }
        let tool = self.tool_steps.contains(&step);
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
            usage: TokenUsage::default(),
            quota_remaining: BTreeMap::new(),
            quota_resets: BTreeMap::new(),
        })
    }
}

/// A scripted provider read from a file, shared by the gateway and the
/// `models probe` diagnostic. Responses and failures are selected by call
/// count; request-based scripts restart on every user turn.
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
    /// Load a deterministic fake script from explicit paths.
    /// # Errors
    /// Returns an unreadable or invalid script.
    pub fn from_files(script: &Path, call_log: &Path) -> Result<Self, Error> {
        let script: Script = serde_json::from_slice(&std::fs::read(script)?)?;
        if let Some(mode) = &script.request_based {
            mode.validate()?;
            if !script.responses.is_empty() {
                return Err(Error::Protocol(
                    "choose responses or request_based, not both".into(),
                ));
            }
        }
        for mode in script.request_by_prompt.values() {
            mode.validate()?;
        }
        if !script.request_by_prompt.is_empty()
            && (!script.responses.is_empty() || script.request_based.is_some())
        {
            return Err(Error::Protocol(
                "choose request_by_prompt, responses, or request_based".into(),
            ));
        }
        let provider = FakeProvider {
            responses: script.responses,
            failures: script.failures.clone(),
            ..FakeProvider::default()
        };
        // Delay each emitted delta in the wrapper so tests can kill a partial stream.
        Ok(Self {
            provider: Arc::new(provider),
            log: call_log.to_owned(),
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
                Err(Error::Protocol("scripted provider failure".into()))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn scripted_failure_precedes_a_successful_second_call() {
        let mut fake = FakeProvider::default();
        fake.failures.insert(
            0,
            FakeFailure {
                status: 429,
                message: "quota reached".into(),
                retry_after_seconds: Some(2),
            },
        );
        fake.responses.insert(
            1,
            Response {
                parts: vec![Part::Text {
                    text: "done".into(),
                }],
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::default(),
                quota_remaining: BTreeMap::new(),
                quota_resets: BTreeMap::new(),
            },
        );
        let request = Request {
            system_prompt: String::new(),
            messages: Vec::new(),
            tools: Vec::new(),
            settings: crate::GenerationSettings::default(),
        };
        let first = fake.request(request.clone()).next().await.unwrap();
        assert!(
            matches!(first, Err(Error::ProviderResponse { status, retry_after: Some(delay), .. })
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS && delay == Duration::from_secs(2))
        );
        assert!(fake.request(request).next().await.unwrap().is_ok());
        assert_eq!(fake.call_count(), 2);
    }
    #[tokio::test]
    async fn captures_image_inputs() {
        let fake = FakeProvider::default();
        let mut request = Request {
            system_prompt: String::new(),
            messages: Vec::new(),
            tools: Vec::new(),
            settings: crate::GenerationSettings::default(),
        };
        request.messages.push(swarmy_core::Message {
            id: swarmy_core::MessageId::from_ulid(ulid::Ulid::nil()),
            role: swarmy_core::MessageRole::User,
            parts: vec![Part::Image {
                media_type: "image/png".into(),
                bytes: vec![1, 2, 3],
                object_key: None,
                detail: None,
            }],
        });
        let _ = fake.request(request.clone()).next().await;
        assert_eq!(fake.requests(), vec![request]);
    }
}

#[cfg(test)]
mod file_tests {
    use super::*;
    use futures::StreamExt;
    use swarmy_core::{Message, MessageId};

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
                settings: crate::GenerationSettings::default(),
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
        let text = |role, text: &str| Message {
            id: MessageId::from_ulid(ulid::Ulid::generate()),
            role,
            parts: vec![swarmy_core::Part::Text { text: text.into() }],
        };
        let mut request = Request {
            system_prompt: String::new(),
            messages: vec![text(MessageRole::User, "one")],
            tools: Vec::new(),
            settings: crate::GenerationSettings::default(),
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
