//! Anthropic Messages on Anthropic direct, Vertex, and `OpenRouter`.

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use futures::StreamExt;
use serde_json::{Value, json};
use swarmy_core::{MessageRole, Part, ToolCallId, ToolResult};

use crate::{
    BearerSource, ClientAuth, Delta, Error, Provider, ProviderStream, ReasoningEffort, Request,
    Response, StopReason, TokenUsage,
    catalog::{ModelInfo, ProviderInfo},
    retry::{RetryPolicy, retryable, with_retry},
};

const BETAS: &str = "interleaved-thinking-2025-05-14,fine-grained-tool-streaming-2025-05-14";

#[derive(Clone)]
pub enum Endpoint {
    Direct {
        api_key: String,
    },
    Vertex {
        project: String,
        location: String,
        source: Arc<dyn BearerSource>,
    },
    OpenRouter {
        api_key: String,
    },
}

impl Endpoint {
    fn provider(&self) -> &'static str {
        match self {
            Self::Direct { .. } => "anthropic",
            Self::Vertex { .. } => "google-vertex-anthropic",
            Self::OpenRouter { .. } => "openrouter",
        }
    }

    fn default_base(&self) -> String {
        match self {
            Self::Direct { .. } => "https://api.anthropic.com".into(),
            Self::OpenRouter { .. } => "https://openrouter.ai/api/v1".into(),
            Self::Vertex { location, .. } if location == "global" => {
                "https://aiplatform.googleapis.com".into()
            }
            Self::Vertex { location, .. } => {
                format!("https://{location}-aiplatform.googleapis.com")
            }
        }
    }

    fn url(&self, base: &str, model: &str) -> Result<reqwest::Url, Error> {
        let mut url = reqwest::Url::parse(base)
            .map_err(|error| Error::Protocol(format!("invalid Anthropic base URL: {error}")))?;
        let versioned = url.path().trim_end_matches('/').ends_with("/v1");
        let mut path = url
            .path_segments_mut()
            .map_err(|()| Error::Protocol("Anthropic base URL cannot hold path segments".into()))?;
        path.pop_if_empty();
        if !versioned {
            path.push("v1");
        }
        match self {
            Self::Vertex {
                project, location, ..
            } => {
                path.extend([
                    "projects",
                    project,
                    "locations",
                    location,
                    "publishers",
                    "anthropic",
                    "models",
                ]);
                path.push(&format!("{model}:streamRawPredict"));
            }
            Self::Direct { .. } | Self::OpenRouter { .. } => {
                path.push("messages");
            }
        }
        drop(path);
        Ok(url)
    }
}

#[derive(Clone)]
pub struct AnthropicProvider {
    client: reqwest::Client,
    model: ModelInfo,
    endpoint: Endpoint,
    url: reqwest::Url,
}

impl AnthropicProvider {
    /// # Errors
    /// Returns an error if the HTTP client or endpoint cannot be configured.
    pub fn new(model: ModelInfo, endpoint: Endpoint) -> Result<Self, Error> {
        let base = endpoint.default_base();
        Self::with_base(model, endpoint, &base)
    }

    /// Override the base URL for a private endpoint or fixture server.
    /// # Errors
    /// Returns an error if the HTTP client or endpoint cannot be configured.
    pub fn with_base(model: ModelInfo, endpoint: Endpoint, base: &str) -> Result<Self, Error> {
        let url = endpoint.url(base, &model.id)?;
        Ok(Self {
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(30))
                .read_timeout(Duration::from_secs(120))
                .user_agent(concat!("swarmy/", env!("CARGO_PKG_VERSION")))
                .build()?,
            model,
            endpoint,
            url,
        })
    }

    async fn send(&self, body: &Value) -> Result<reqwest::Response, Error> {
        let builder = self
            .client
            .post(self.url.clone())
            .header("accept", "text/event-stream")
            .header("anthropic-beta", BETAS)
            .json(body);
        let builder = match &self.endpoint {
            Endpoint::Direct { api_key } => builder
                .header("x-api-key", api_key)
                .header("anthropic-version", "2023-06-01"),
            Endpoint::OpenRouter { api_key } => builder
                .bearer_auth(api_key)
                .header("anthropic-version", "2023-06-01"),
            Endpoint::Vertex { source, .. } => builder.bearer_auth(source.token().await?),
        };
        let response = builder.send().await?;
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|header| header.to_str().ok())
            .and_then(|value| {
                value
                    .parse::<u64>()
                    .ok()
                    .map(Duration::from_secs)
                    .or_else(|| {
                        httpdate::parse_http_date(value)
                            .ok()
                            .map(|date| date.duration_since(SystemTime::now()).unwrap_or_default())
                    })
            });
        let body = response.text().await?;
        if context_overflow(&body) {
            return Err(Error::ContextOverflow(body));
        }
        if retryable(status) {
            return Err(Error::ProviderResponse {
                status,
                message: body,
                retry_after,
            });
        }
        Err(provider_error(status, &body))
    }
}

/// Keep the provider's own explanation; a bare status hides schema mistakes.
fn provider_error(status: reqwest::StatusCode, body: &str) -> Error {
    let message = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|value| value["error"]["message"].as_str().map(str::to_owned))
        .unwrap_or_else(|| body.trim().chars().take(600).collect());
    if message.is_empty() {
        Error::Status(status)
    } else {
        Error::Protocol(format!("provider error ({status}): {message}"))
    }
}

impl Provider for AnthropicProvider {
    fn request(&self, request: Request) -> ProviderStream {
        let provider = self.clone();
        Box::pin(async_stream::try_stream! {
            let body = request_json(&request, &provider.model, &provider.endpoint)?;
            let response = with_retry(&RetryPolicy::default(), || provider.send(&body)).await?;
            let mut bytes = response.bytes_stream();
            let mut parser = SseParser::new(&provider.model.id, provider.endpoint.provider());
            while let Some(chunk) = bytes.next().await {
                for delta in parser.push(&chunk?)? { yield delta; }
                if parser.completed { break; }
            }
            parser.finish()?;
        })
    }
}

pub(crate) fn client_for(
    provider: &ProviderInfo,
    model: &ModelInfo,
    auth: ClientAuth,
) -> Result<Arc<dyn Provider>, Error> {
    let endpoint = match (provider.id.as_str(), auth) {
        (
            "google-vertex-anthropic",
            ClientAuth::Vertex {
                project,
                location,
                source,
            },
        ) => Endpoint::Vertex {
            project,
            location,
            source,
        },
        ("google-vertex-anthropic", _) => {
            return Err(Error::Credentials(
                "Vertex requires a bearer source, project, and location",
            ));
        }
        (
            "openrouter",
            ClientAuth::ApiKey(api_key)
            | ClientAuth::Bearer(api_key)
            | ClientAuth::BearerWithExtra { token: api_key, .. },
        ) => Endpoint::OpenRouter { api_key },
        ("openrouter", _) => return Err(Error::Credentials("OpenRouter requires an API key")),
        (_, ClientAuth::ApiKey(api_key)) => Endpoint::Direct { api_key },
        _ => return Err(Error::Credentials("Anthropic requires an API key")),
    };
    let base = model.base_url.as_deref().unwrap_or(&provider.base_url);
    let client = if base.is_empty() {
        AnthropicProvider::new(model.clone(), endpoint)?
    } else {
        AnthropicProvider::with_base(model.clone(), endpoint, base)?
    };
    Ok(Arc::new(client))
}

/// Lower core history and generation settings to the Messages request body.
/// # Errors
/// Rejects mismatched models and invalid generation settings or history.
pub fn request_json(
    request: &Request,
    model: &ModelInfo,
    endpoint: &Endpoint,
) -> Result<Value, Error> {
    if request.settings.model != model.id {
        return Err(Error::Protocol(
            "request model differs from configured Anthropic model".into(),
        ));
    }
    let mut system = Vec::new();
    if !request.system_prompt.is_empty() {
        system.push(json!({"type": "text", "text": request.system_prompt}));
    }
    cache_last(&mut system);
    let mut tools: Vec<_> = request
        .tools
        .iter()
        .map(|tool| {
            json!({
                "name": tool.name, "description": tool.description, "input_schema": tool.parameters,
            })
        })
        .collect();
    cache_last(&mut tools);
    let mut body = json!({
        "stream": true, "system": system, "messages": messages(request, endpoint)?,
    });
    if !tools.is_empty() {
        body["tools"] = json!(tools);
    }
    match endpoint {
        Endpoint::Vertex { .. } => body["anthropic_version"] = json!("vertex-2023-10-16"),
        _ => body["model"] = json!(model.id),
    }
    generation(&mut body, request, model)?;
    Ok(body)
}

fn generation(body: &mut Value, request: &Request, model: &ModelInfo) -> Result<(), Error> {
    let limit = model.limit.output.unwrap_or(u64::MAX);
    let mut maximum = request
        .settings
        .max_output_tokens
        .or(model.limit.output)
        .ok_or_else(|| Error::Protocol("Anthropic requires an output token limit".into()))?
        .min(limit);
    let effort = model
        .clamp_effort(
            request
                .settings
                .reasoning_effort
                .unwrap_or(ReasoningEffort::None),
        )
        .0;
    if effort == ReasoningEffort::None {
        body["thinking"] = json!({"type": "disabled"});
        if model.compat.supports_temperature() != Some(false)
            && let Some(temperature) = request.settings.temperature
        {
            if !temperature.is_finite() {
                return Err(Error::Protocol("temperature must be finite".into()));
            }
            body["temperature"] = json!(temperature);
        }
    } else if model.compat.force_adaptive_thinking() == Some(true) {
        body["thinking"] = json!({"type": "adaptive", "display": "summarized"});
        let effort = if effort == ReasoningEffort::Minimal {
            ReasoningEffort::Low
        } else {
            effort
        };
        body["output_config"] = json!({"effort": effort});
    } else {
        let budget = match effort {
            ReasoningEffort::Minimal => 1024,
            ReasoningEffort::Low => 2048,
            ReasoningEffort::Medium => 8192,
            _ => 16384,
        };
        maximum = maximum.saturating_add(budget).min(limit);
        if maximum <= budget {
            return Err(Error::Protocol(
                "output limit must exceed the thinking budget".into(),
            ));
        }
        body["thinking"] = json!({"type": "enabled", "budget_tokens": budget});
    }
    if maximum == 0 {
        return Err(Error::Protocol("max_tokens must be positive".into()));
    }
    body["max_tokens"] = json!(maximum);
    Ok(())
}

fn cache_last(blocks: &mut [Value]) {
    if let Some(block) = blocks.last_mut() {
        block["cache_control"] = json!({"type": "ephemeral"});
    }
}

fn tool_id(id: &str) -> String {
    if !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        id.to_owned()
    } else {
        format!("toolu_{}", &blake3::hash(id.as_bytes()).to_hex()[..32])
    }
}

fn content(part: &Part, request: &Request, endpoint: &Endpoint) -> Option<Value> {
    Some(match part {
        Part::Text { text } => {
            if text.is_empty() {
                return None;
            }
            json!({"type": "text", "text": text})
        }
        Part::Reasoning { text, metadata } => {
            let replay = metadata
                .get("anthropic")
                .filter(|value| {
                    value["model"] == request.settings.model
                        && value["provider"] == endpoint.provider()
                })
                .and_then(|value| value["signature"].as_str())
                .filter(|signature| !signature.is_empty());
            if let Some(signature) = replay {
                json!({"type": "thinking", "thinking": text, "signature": signature})
            } else {
                if text.is_empty() {
                    return None;
                }
                json!({"type": "text", "text": text})
            }
        }
        Part::ToolCall {
            call_id,
            tool,
            input,
        } => json!({"type": "tool_use", "id": tool_id(&call_id.0), "name": tool, "input": input}),
        Part::ToolResult { call_id, result } => {
            let (output, is_error) = match result {
                ToolResult::Completed { output, .. } => (output, false),
                ToolResult::Error { error } => (error, true),
            };
            json!({"type": "tool_result", "tool_use_id": tool_id(&call_id.0), "content": output, "is_error": is_error})
        }
    })
}

fn messages(request: &Request, endpoint: &Endpoint) -> Result<Vec<Value>, Error> {
    let mut messages: Vec<Value> = Vec::new();
    for message in &request.messages {
        let role = if message.role == MessageRole::Assistant {
            "assistant"
        } else {
            "user"
        };
        let blocks: Vec<_> = message
            .parts
            .iter()
            .filter_map(|part| content(part, request, endpoint))
            .collect();
        if blocks.is_empty() {
            continue;
        }
        if blocks.iter().any(|block| {
            (block["type"] == "tool_use" || block["type"] == "thinking") && role != "assistant"
                || block["type"] == "tool_result" && role != "user"
        }) {
            return Err(Error::Protocol(
                "Anthropic content block has an invalid message role".into(),
            ));
        }
        if let Some(last) = messages.last_mut().filter(|last| last["role"] == role) {
            last["content"]
                .as_array_mut()
                .expect("constructed content array")
                .extend(blocks);
        } else {
            messages.push(json!({"role": role, "content": blocks}));
        }
    }
    repair_tool_results(&mut messages);
    if let Some(last) = messages
        .iter_mut()
        .rev()
        .find(|message| message["role"] == "user")
    {
        cache_last(
            last["content"]
                .as_array_mut()
                .expect("constructed content array"),
        );
    }
    Ok(messages)
}

fn repair_tool_results(messages: &mut Vec<Value>) {
    let mut index = 0;
    while index < messages.len() {
        let calls: Vec<_> = messages[index]["content"]
            .as_array()
            .expect("constructed content array")
            .iter()
            .filter(|block| block["type"] == "tool_use")
            .map(|block| block["id"].clone())
            .collect();
        if !calls.is_empty() {
            if messages
                .get(index + 1)
                .is_none_or(|message| message["role"] != "user")
            {
                messages.insert(index + 1, json!({"role": "user", "content": []}));
            }
            let blocks = messages[index + 1]["content"]
                .as_array_mut()
                .expect("constructed content array");
            for id in calls {
                if !blocks
                    .iter()
                    .any(|block| block["type"] == "tool_result" && block["tool_use_id"] == id)
                {
                    blocks.push(json!({"type": "tool_result", "tool_use_id": id, "content": "No result provided", "is_error": true}));
                }
            }
            // Tool results must precede ordinary user text, including rebuild notices.
            blocks.sort_by_key(|block| block["type"] != "tool_result");
        }
        index += 1;
    }
}

fn context_overflow(body: &str) -> bool {
    let lower = body.to_ascii_lowercase();
    lower.contains("prompt is too long") || lower.contains("request_too_large")
}

fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str, Error> {
    value[field]
        .as_str()
        .ok_or_else(|| Error::Protocol(format!("missing Anthropic string field {field}")))
}

fn index(value: &Value) -> Result<usize, Error> {
    value["index"]
        .as_u64()
        .and_then(|index| usize::try_from(index).ok())
        .ok_or_else(|| Error::Protocol("missing Anthropic block index".into()))
}

struct Block {
    part: Part,
    arguments: String,
}

/// Incremental SSE framing and Anthropic block assembly. No completion is
/// emitted until `message_stop` confirms that every block has finished.
pub struct SseParser {
    model: String,
    provider: String,
    line: Vec<u8>,
    data: Vec<u8>,
    previous_cr: bool,
    started: bool,
    completed: bool,
    blocks: BTreeMap<usize, Block>,
    parts: BTreeMap<usize, Part>,
    stop_reason: Option<StopReason>,
    usage: Value,
}

impl SseParser {
    #[must_use]
    pub fn new(model: &str, provider: &str) -> Self {
        Self {
            model: model.into(),
            provider: provider.into(),
            line: Vec::new(),
            data: Vec::new(),
            previous_cr: false,
            started: false,
            completed: false,
            blocks: BTreeMap::new(),
            parts: BTreeMap::new(),
            stop_reason: None,
            usage: json!({}),
        }
    }

    /// # Errors
    /// Rejects malformed events, provider errors, and events larger than 8 MiB.
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<Delta>, Error> {
        let mut deltas = Vec::new();
        for &byte in bytes {
            if self.completed {
                break;
            }
            if byte == b'\n' && self.previous_cr {
                self.previous_cr = false;
                continue;
            }
            self.previous_cr = byte == b'\r';
            if matches!(byte, b'\r' | b'\n') {
                self.end_line(&mut deltas)?;
            } else {
                self.line.push(byte);
                if self.line.len() + self.data.len() > 8 * 1024 * 1024 {
                    return Err(Error::Protocol("SSE event exceeds 8 MiB".into()));
                }
            }
        }
        Ok(deltas)
    }

    fn end_line(&mut self, deltas: &mut Vec<Delta>) -> Result<(), Error> {
        let line = std::mem::take(&mut self.line);
        if line.is_empty() {
            if !self.data.is_empty() {
                let data = std::mem::take(&mut self.data);
                self.event(&serde_json::from_slice::<Value>(&data)?, deltas)?;
            }
        } else if let Some(data) = line.strip_prefix(b"data:") {
            self.data
                .extend_from_slice(data.strip_prefix(b" ").unwrap_or(data));
            self.data.push(b'\n');
        }
        Ok(())
    }

    /// # Errors
    /// EOF without `message_stop` is a failed stream, even after a stop reason.
    pub fn finish(&self) -> Result<(), Error> {
        if self.completed {
            Ok(())
        } else {
            Err(Error::Protocol(
                "Anthropic stream closed before message_stop".into(),
            ))
        }
    }

    fn event(&mut self, event: &Value, deltas: &mut Vec<Delta>) -> Result<(), Error> {
        let kind = string(event, "type")?;
        if kind == "error" {
            let body = event["error"].to_string();
            return Err(if context_overflow(&body) {
                Error::ContextOverflow(body)
            } else {
                Error::Protocol(body)
            });
        }
        if kind == "ping" {
            return Ok(());
        }
        if kind != "message_start" && !self.started {
            return Err(Error::Protocol(
                "Anthropic event before message_start".into(),
            ));
        }
        match kind {
            "message_start" => {
                if self.started {
                    return Err(Error::Protocol("duplicate message_start".into()));
                }
                self.started = true;
                self.merge_usage(&event["message"]["usage"]);
            }
            "content_block_start" => self.start_block(event, deltas)?,
            "content_block_delta" => self.delta(event, deltas)?,
            "content_block_stop" => self.stop_block(event, deltas)?,
            "message_delta" => {
                self.merge_usage(&event["usage"]);
                if let Some(reason) = event["delta"]["stop_reason"].as_str() {
                    self.stop_reason = Some(match reason {
                        "end_turn" | "stop_sequence" => StopReason::EndTurn,
                        "tool_use" => StopReason::ToolCalls,
                        "max_tokens" => StopReason::MaxOutputTokens,
                        "refusal" => StopReason::ContentFilter,
                        other => StopReason::Incomplete(other.into()),
                    });
                }
            }
            "message_stop" => {
                if !self.blocks.is_empty() {
                    return Err(Error::Protocol(
                        "message_stop with unfinished blocks".into(),
                    ));
                }
                let stop_reason = self
                    .stop_reason
                    .take()
                    .ok_or_else(|| Error::Protocol("message_stop without stop reason".into()))?;
                deltas.push(Delta::Completed(Response {
                    parts: std::mem::take(&mut self.parts).into_values().collect(),
                    stop_reason,
                    usage: self.token_usage(),
                }));
                self.completed = true;
            }
            _ => (),
        }
        Ok(())
    }

    fn start_block(&mut self, event: &Value, deltas: &mut Vec<Delta>) -> Result<(), Error> {
        let output_index = index(event)?;
        if self.blocks.contains_key(&output_index) || self.parts.contains_key(&output_index) {
            return Err(Error::Protocol("duplicate Anthropic block index".into()));
        }
        let block = &event["content_block"];
        let part = match string(block, "type")? {
            "text" => {
                let text = string(block, "text")?.to_owned();
                if !text.is_empty() {
                    deltas.push(Delta::Text {
                        output_index,
                        text: text.clone(),
                    });
                }
                Part::Text { text }
            }
            "thinking" => {
                let text = string(block, "thinking")?.to_owned();
                if !text.is_empty() {
                    deltas.push(Delta::Reasoning {
                        output_index,
                        text: text.clone(),
                    });
                }
                Part::Reasoning {
                    text,
                    metadata: BTreeMap::from([(
                        "anthropic".into(),
                        json!({
                            "signature": block["signature"].as_str().unwrap_or_default(), "model": self.model, "provider": self.provider,
                        }),
                    )]),
                }
            }
            "tool_use" => Part::ToolCall {
                call_id: ToolCallId(string(block, "id")?.into()),
                tool: string(block, "name")?.into(),
                input: block["input"].clone(),
            },
            other => {
                return Err(Error::Protocol(format!(
                    "unsupported Anthropic block {other}"
                )));
            }
        };
        self.blocks.insert(
            output_index,
            Block {
                part,
                arguments: String::new(),
            },
        );
        Ok(())
    }

    fn delta(&mut self, event: &Value, deltas: &mut Vec<Delta>) -> Result<(), Error> {
        let output_index = index(event)?;
        let block = self
            .blocks
            .get_mut(&output_index)
            .ok_or_else(|| Error::Protocol("delta without Anthropic block".into()))?;
        let delta = &event["delta"];
        match (string(delta, "type")?, &mut block.part) {
            ("text_delta", Part::Text { text }) => {
                let value = string(delta, "text")?;
                text.push_str(value);
                deltas.push(Delta::Text {
                    output_index,
                    text: value.into(),
                });
            }
            ("thinking_delta", Part::Reasoning { text, .. }) => {
                let value = string(delta, "thinking")?;
                text.push_str(value);
                deltas.push(Delta::Reasoning {
                    output_index,
                    text: value.into(),
                });
            }
            ("signature_delta", Part::Reasoning { metadata, .. }) => {
                let value =
                    &mut metadata.get_mut("anthropic").expect("constructed metadata")["signature"];
                let mut signature = value.as_str().unwrap_or_default().to_owned();
                signature.push_str(string(delta, "signature")?);
                *value = json!(signature);
            }
            ("input_json_delta", Part::ToolCall { .. }) => {
                let arguments = string(delta, "partial_json")?;
                block.arguments.push_str(arguments);
                deltas.push(Delta::ToolArguments {
                    output_index,
                    arguments: arguments.into(),
                });
            }
            ("text_delta" | "thinking_delta" | "signature_delta" | "input_json_delta", _) => {
                return Err(Error::Protocol(
                    "Anthropic delta does not match its block".into(),
                ));
            }
            _ => (),
        }
        Ok(())
    }

    fn stop_block(&mut self, event: &Value, deltas: &mut Vec<Delta>) -> Result<(), Error> {
        let output_index = index(event)?;
        let mut block = self
            .blocks
            .remove(&output_index)
            .ok_or_else(|| Error::Protocol("stop without Anthropic block".into()))?;
        if let Part::ToolCall { input, .. } = &mut block.part {
            if !block.arguments.is_empty() {
                *input = serde_json::from_str(&block.arguments)?;
            }
            if !input.is_object() {
                return Err(Error::Protocol(
                    "Anthropic tool input must be an object".into(),
                ));
            }
        }
        deltas.push(Delta::PartDone {
            output_index,
            part: block.part.clone(),
        });
        self.parts.insert(output_index, block.part);
        Ok(())
    }

    fn merge_usage(&mut self, value: &Value) {
        if let Some(fields) = value.as_object() {
            for (key, value) in fields {
                if value.is_u64() {
                    self.usage[key] = value.clone();
                }
            }
        }
    }

    fn token_usage(&self) -> TokenUsage {
        let read = self.usage["cache_read_input_tokens"]
            .as_u64()
            .unwrap_or_default();
        let write = self.usage["cache_creation_input_tokens"]
            .as_u64()
            .unwrap_or_default();
        // Shared usage includes cached input, as the Responses protocol does.
        let input = self.usage["input_tokens"]
            .as_u64()
            .unwrap_or_default()
            .saturating_add(read)
            .saturating_add(write);
        let output = self.usage["output_tokens"].as_u64().unwrap_or_default();
        // Anthropic includes thinking in output_tokens without a separate count.
        TokenUsage {
            input_tokens: input,
            cached_input_tokens: read,
            cache_write_input_tokens: write,
            output_tokens: output,
            total_tokens: input.saturating_add(output),
            reasoning_output_tokens: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_errors_keep_the_message() {
        let error = provider_error(
            reqwest::StatusCode::BAD_REQUEST,
            r#"{"type":"error","error":{"type":"invalid_request_error","message":"tools.14: bad schema"}}"#,
        );
        assert!(error.to_string().contains("tools.14: bad schema"));
        assert!(matches!(
            provider_error(reqwest::StatusCode::FORBIDDEN, ""),
            Error::Status(reqwest::StatusCode::FORBIDDEN)
        ));
    }
}
