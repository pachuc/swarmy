//! `OpenRouter`'s Chat Completions transport and reasoning replay format.

use crate::sse::{Frame, SseParser};
use base64::Engine as _;
use std::{collections::BTreeMap, time::Duration};

use futures::StreamExt;
use serde_json::{Value, json};
use swarmy_core::{Message, MessageRole, Part, ToolCallId, ToolResult};

use crate::{
    ClientAuth, Delta, Error, Provider, ProviderStream, ReasoningEffort, Request, Response,
    StopReason, TokenUsage,
    catalog::{ModelInfo, ProviderInfo, ReasoningOptions},
    retry::{RetryPolicy, retryable, with_retry},
};

#[derive(Clone)]
pub struct CompletionsProvider {
    client: reqwest::Client,
    base: String,
    provider: String,
    model: ModelInfo,
    token: String,
    pub retry_policy: RetryPolicy,
}

impl CompletionsProvider {
    /// # Errors
    /// Rejects missing bearer credentials or invalid HTTP client configuration.
    pub fn new(
        provider: &ProviderInfo,
        model: &ModelInfo,
        auth: ClientAuth,
    ) -> Result<Self, Error> {
        let token = match auth {
            ClientAuth::ApiKey(token)
            | ClientAuth::Bearer(token)
            | ClientAuth::BearerWithExtra { token, .. }
                if !token.is_empty() =>
            {
                token
            }
            _ => {
                return Err(Error::Credentials(
                    "Chat Completions requires an API key or bearer token",
                ));
            }
        };
        Ok(Self {
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(30))
                .read_timeout(Duration::from_secs(120))
                .user_agent(concat!("swarmy/", env!("CARGO_PKG_VERSION")))
                .build()?,
            base: model
                .base_url
                .as_deref()
                .unwrap_or(&provider.base_url)
                .trim_end_matches('/')
                .into(),
            provider: provider.id.clone(),
            model: model.clone(),
            token,
            retry_policy: RetryPolicy::default(),
        })
    }

    async fn send(&self, body: &Value) -> Result<reqwest::Response, Error> {
        let response = self
            .client
            .post(format!("{}/chat/completions", self.base))
            .bearer_auth(&self.token)
            .header("HTTP-Referer", "https://github.com/pachuc/swarmy")
            .header("X-Title", "swarmy")
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .json(body)
            .send()
            .await?;
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let retry_after = crate::retry::retry_after_header(response.headers());
        let body = response.text().await?;
        let error = serde_json::from_str::<Value>(&body).map_or_else(
            |_| crate::error::message_error(body.clone()),
            |value| error_from_json(&value),
        );
        if matches!(error, Error::ContextOverflow(_)) {
            return Err(error);
        }
        if retryable(status) {
            return Err(Error::ProviderResponse {
                status,
                message: body,
                retry_after,
            });
        }
        Err(error)
    }
}

impl Provider for CompletionsProvider {
    fn request(&self, request: Request) -> ProviderStream {
        let provider = self.clone();
        Box::pin(async_stream::try_stream! {
            let body = request_json(&request, &provider.provider, &provider.model)?;
            let response = with_retry(&provider.retry_policy, || provider.send(&body)).await?;
            if response.headers().get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.starts_with("application/json"))
            {
                Err(error_from_json(&response.json::<Value>().await?))?;
            } else {
                let quota = crate::quota::openai_remaining(response.headers());
                let resets = crate::quota::openai_resets(response.headers());
                let mut bytes = response.bytes_stream();
                let mut parser = CompletionsStream::new(&provider.provider, &provider.model.id);
                parser.set_quota(quota);
                parser.set_quota_resets(resets);
                while let Some(chunk) = bytes.next().await {
                    for delta in parser.push(&chunk?)? { yield delta; }
                    if parser.completed { break; }
                }
                parser.finish()?;
            }
        })
    }
}

/// Build a request using the selected model's compatibility flags.
///
/// # Errors
/// Rejects mismatched models or invalid temperatures.
pub fn request_json(request: &Request, provider: &str, model: &ModelInfo) -> Result<Value, Error> {
    if !request.settings.model.is_empty() && request.settings.model != model.id {
        return Err(Error::Protocol(
            "request model differs from the selected catalog model".into(),
        ));
    }
    let system_role = if model.compat.supports_developer_role() == Some(true) {
        "developer"
    } else {
        "system"
    };
    let mut messages = Vec::new();
    if !request.system_prompt.is_empty() {
        messages.push(json!({"role": system_role, "content": [{"type": "text", "text": request.system_prompt}]}));
    }
    for message in &request.messages {
        messages.extend(convert_message(message, provider, model, system_role)?);
    }
    crate::protocol::repair_tool_results(&mut messages, crate::protocol::ToolWire::Completions);
    if model.compat.cache_control_format() == Some("anthropic") {
        cache_messages(&mut messages);
    }
    let tools: Vec<_> = request
        .tools
        .iter()
        .map(|tool| {
            json!({
                "type": "function", "function": {"name": tool.name, "description": tool.description,
                "parameters": tool.parameters, "strict": false}
            })
        })
        .collect();
    let mut body = json!({
        "model": model.id, "messages": messages, "tools": tools, "stream": true,
        "stream_options": {"include_usage": true}, "usage": {"include": true}
    });
    if let Some(maximum) = request.settings.max_output_tokens {
        body[model.compat.max_tokens_field().unwrap_or("max_tokens")] = json!(maximum);
    }
    if let Some(temperature) = request.settings.temperature {
        if !temperature.is_finite() {
            return Err(Error::Protocol("temperature must be finite".into()));
        }
        body["temperature"] = json!(temperature);
    }
    if let Some(effort) = request.settings.reasoning_effort
        && let Some(reasoning) =
            reasoning_options(model, effort, request.settings.max_output_tokens)
    {
        body["reasoning"] = reasoning;
    }
    if let Some(routing) = model.compat.0.get("openrouter_provider") {
        body["provider"] = routing.clone();
    }
    Ok(body)
}

fn reasoning_options(
    model: &ModelInfo,
    requested: ReasoningEffort,
    output: Option<u64>,
) -> Option<Value> {
    let options = model.reasoning.as_ref()?;
    let (effort, _) = model.clamp_effort(requested);
    if effort == ReasoningEffort::None {
        return model
            .supported_efforts()
            .contains(&effort)
            .then(|| json!({"enabled": false}));
    }
    Some(match options {
        ReasoningOptions::Effort(_) => json!({"effort": effort}),
        ReasoningOptions::Toggle => json!({"enabled": true}),
        ReasoningOptions::BudgetTokens { min, max } => {
            let budget: u64 = match effort {
                ReasoningEffort::None => 0,
                ReasoningEffort::Minimal => 1024,
                ReasoningEffort::Low => 2048,
                ReasoningEffort::Medium => 8192,
                ReasoningEffort::High | ReasoningEffort::Xhigh | ReasoningEffort::Max => 16384,
            };
            let ceiling = max.unwrap_or(u64::MAX).min(
                output
                    .or(model.limit.output)
                    .unwrap_or(u64::MAX)
                    .saturating_sub(1),
            );
            json!({"max_tokens": budget.max(min.unwrap_or(0)).min(ceiling)})
        }
    })
}

fn convert_message(
    message: &Message,
    provider: &str,
    model: &ModelInfo,
    system_role: &str,
) -> Result<Vec<Value>, Error> {
    let role = match message.role {
        MessageRole::System => system_role,
        MessageRole::User => "user",
        MessageRole::Assistant => "assistant",
        MessageRole::Tool => "tool",
    };
    let mut content = Vec::new();
    let mut calls = Vec::new();
    let mut details = Vec::new();
    let mut results = Vec::new();
    for part in &message.parts {
        match part {
            Part::Image { media_type, bytes, detail, .. } => content.push(json!({"type": "image_url", "image_url": {"url": format!("data:{media_type};base64,{}", base64::engine::general_purpose::STANDARD.encode(bytes)), "detail": detail.as_deref().unwrap_or("auto")}})),
            Part::Text { text } => content.push(json!({"type": "text", "text": text})),
            Part::Reasoning { text, metadata } => {
                let replay =
                    metadata
                        .get("openrouter")
                        .and_then(Value::as_array)
                        .filter(|details| {
                            !details.is_empty()
                                && role == "assistant"
                                && metadata.get("model") == Some(&json!(model.id))
                                && metadata.get("provider") == Some(&json!(provider))
                        });
                if let Some(replay) = replay {
                    details.extend(replay.iter().cloned());
                } else if !text.is_empty() {
                    content.push(json!({"type": "text", "text": text}));
                }
            }
            Part::ToolCall {
                call_id,
                tool,
                input,
            } => {
                if message.role != MessageRole::Assistant {
                    return Err(Error::Protocol(
                        "tool calls require an assistant role".into(),
                    ));
                }
                calls.push(json!({"id": call_id.0, "type": "function", "function": {
                    "name": tool, "arguments": serde_json::to_string(input)?
                }}));
            }
            Part::ToolResult { call_id, result } => {
                let output = match result {
                    ToolResult::Completed { output, .. } => output.clone(),
                    ToolResult::Error { error } => json!({"error": error}).to_string(),
                };
                results.push(json!({"role": "tool", "tool_call_id": call_id.0, "content": output}));
            }
        }
    }
    if role == "tool" && !content.is_empty() {
        return Err(Error::Protocol(
            "tool text requires a tool result call id".into(),
        ));
    }
    let mut messages = Vec::new();
    if !content.is_empty() || !calls.is_empty() || !details.is_empty() {
        let content = if role == "assistant" {
            let text: String = content
                .iter()
                .filter_map(|part| part["text"].as_str())
                .collect();
            if text.is_empty() {
                Value::Null
            } else {
                json!(text)
            }
        } else {
            json!(content)
        };
        let mut wire = json!({"role": role, "content": content});
        if !calls.is_empty() {
            wire["tool_calls"] = json!(calls);
        }
        if !details.is_empty() {
            wire["reasoning_details"] = json!(details);
        }
        messages.push(wire);
    }
    messages.extend(results);
    Ok(messages)
}

fn cache_messages(messages: &mut [Value]) {
    let mut system = false;
    let mut users = 0;
    for message in messages.iter_mut().rev() {
        let cache = match message["role"].as_str() {
            Some("system" | "developer") if !system => {
                system = true;
                true
            }
            Some("user") if users < 2 => {
                users += 1;
                true
            }
            _ => false,
        };
        if cache
            && let Some(part) = message["content"]
                .as_array_mut()
                .and_then(|parts| parts.last_mut())
        {
            part["cache_control"] = json!({"type": "ephemeral"});
        }
    }
}

/// Classify the shared context-overflow phrases before deciding to retry.
fn error_from_json(value: &Value) -> Error {
    let error = value.get("error").unwrap_or(value);
    let message = error["message"]
        .as_str()
        .or_else(|| error.as_str())
        .unwrap_or("provider returned an error");
    let code = error["code"].as_str().unwrap_or_default();
    crate::error::message_error(if code.is_empty() {
        message.into()
    } else {
        format!("{code}: {message}")
    })
}

#[derive(Default)]
struct ToolCall {
    id: String,
    name: String,
    arguments: String,
}

enum Output {
    Text(String),
    Reasoning { text: String, details: Vec<Value> },
    Tool(ToolCall),
}

/// Incremental SSE parser. Output indices are allocated on first appearance,
/// independently of the provider's tool-call indices. Reasoning metadata keeps
/// the replay array under `openrouter` and its origin under `provider` and `model`.
pub struct CompletionsStream {
    provider: String,
    model: String,
    framing: SseParser,
    completed: bool,
    output: Vec<Output>,
    text: Option<usize>,
    reasoning: Option<usize>,
    tools: BTreeMap<u64, usize>,
    stop_reason: Option<StopReason>,
    usage: TokenUsage,
    quota_remaining: BTreeMap<String, u64>,
    quota_resets: BTreeMap<String, u64>,
}

impl CompletionsStream {
    #[must_use]
    pub fn new(provider: &str, model: &str) -> Self {
        Self {
            provider: provider.into(),
            model: model.into(),
            framing: SseParser::default(),
            completed: false,
            output: Vec::new(),
            text: None,
            reasoning: None,
            tools: BTreeMap::new(),
            stop_reason: None,
            usage: TokenUsage::default(),
            quota_remaining: BTreeMap::new(),
            quota_resets: BTreeMap::new(),
        }
    }

    /// Capture `OpenAI` remaining-quota headers before streaming starts.
    pub fn set_quota(&mut self, quota: BTreeMap<String, u64>) {
        self.quota_remaining = quota;
    }

    /// Capture reset windows before streaming starts.
    pub fn set_quota_resets(&mut self, resets: BTreeMap<String, u64>) {
        self.quota_resets = resets;
    }

    /// # Errors
    /// Rejects malformed events, provider errors, and events over 8 MiB.
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<Delta>, Error> {
        let mut deltas = Vec::new();
        for frame in self.framing.push(bytes)? {
            if self.completed {
                break;
            }
            self.frame(frame, &mut deltas)?;
        }
        Ok(deltas)
    }

    fn frame(&mut self, frame: Frame, deltas: &mut Vec<Delta>) -> Result<(), Error> {
        match frame {
            Frame::Data(data) if data == b"[DONE]\n" => self.complete(deltas)?,
            Frame::Data(data) => {
                self.event(&serde_json::from_slice::<Value>(&data)?, deltas)?;
            }
            Frame::Raw(data) => {
                if let Ok(value) = serde_json::from_slice::<Value>(&data) {
                    return Err(error_from_json(&value));
                }
            }
        }
        Ok(())
    }

    /// # Errors
    /// Rejects streams closed before `[DONE]`, including JSON error bodies.
    pub fn finish(&self) -> Result<(), Error> {
        if self.completed {
            return Ok(());
        }
        if let Ok(value) = serde_json::from_slice::<Value>(self.framing.pending_line()) {
            return Err(error_from_json(&value));
        }
        Err(Error::Protocol("stream closed before completion".into()))
    }

    fn event(&mut self, event: &Value, deltas: &mut Vec<Delta>) -> Result<(), Error> {
        if event.get("error").is_some_and(|error| !error.is_null()) {
            return Err(error_from_json(event));
        }
        if let Some(usage) = event.get("usage").filter(|usage| usage.is_object()) {
            self.usage = TokenUsage {
                input_tokens: usage["prompt_tokens"].as_u64().unwrap_or(0),
                cached_input_tokens: usage["prompt_tokens_details"]["cached_tokens"]
                    .as_u64()
                    .unwrap_or(0),
                output_tokens: usage["completion_tokens"].as_u64().unwrap_or(0),
                reasoning_output_tokens: usage["completion_tokens_details"]["reasoning_tokens"]
                    .as_u64()
                    .unwrap_or(0),
                total_tokens: usage["total_tokens"].as_u64().unwrap_or(0),
                cache_write_input_tokens: 0,
            };
        }
        let Some(choice) = event["choices"]
            .as_array()
            .and_then(|choices| choices.first())
        else {
            return Ok(());
        };
        if choice.get("error").is_some_and(|error| !error.is_null()) {
            return Err(error_from_json(choice));
        }
        if let Some(reason) = choice["finish_reason"].as_str() {
            self.stop_reason = Some(match reason {
                "stop" => StopReason::EndTurn,
                "length" => StopReason::MaxOutputTokens,
                "tool_calls" => StopReason::ToolCalls,
                "content_filter" => StopReason::ContentFilter,
                other => return Err(Error::Protocol(format!("unknown finish_reason: {other}"))),
            });
        }
        let delta = &choice["delta"];
        if let Some(text) = delta["reasoning"].as_str() {
            let index = self.reasoning_index();
            if let Output::Reasoning {
                text: accumulated, ..
            } = &mut self.output[index]
            {
                accumulated.push_str(text);
            }
            deltas.push(Delta::Reasoning {
                output_index: index,
                text: text.into(),
            });
        }
        if let Some(details) = delta["reasoning_details"].as_array() {
            let index = self.reasoning_index();
            if let Output::Reasoning {
                details: accumulated,
                ..
            } = &mut self.output[index]
            {
                for detail in details {
                    append_detail(accumulated, detail);
                }
            }
        }
        if let Some(details) = choice["message"]["reasoning_details"].as_array() {
            let index = self.reasoning_index();
            if let Output::Reasoning {
                details: accumulated,
                ..
            } = &mut self.output[index]
            {
                accumulated.clone_from(details);
            }
        }
        if let Some(text) = delta["content"].as_str() {
            let index = *self.text.get_or_insert_with(|| {
                self.output.push(Output::Text(String::new()));
                self.output.len() - 1
            });
            if let Output::Text(accumulated) = &mut self.output[index] {
                accumulated.push_str(text);
            }
            deltas.push(Delta::Text {
                output_index: index,
                text: text.into(),
            });
        }
        if let Some(calls) = delta["tool_calls"].as_array() {
            for call in calls {
                self.tool_delta(call, deltas)?;
            }
        }
        Ok(())
    }

    fn reasoning_index(&mut self) -> usize {
        *self.reasoning.get_or_insert_with(|| {
            self.output.push(Output::Reasoning {
                text: String::new(),
                details: Vec::new(),
            });
            self.output.len() - 1
        })
    }

    fn tool_delta(&mut self, call: &Value, deltas: &mut Vec<Delta>) -> Result<(), Error> {
        let wire_index = call["index"]
            .as_u64()
            .ok_or_else(|| Error::Protocol("tool delta has no index".into()))?;
        let index = *self.tools.entry(wire_index).or_insert_with(|| {
            self.output.push(Output::Tool(ToolCall::default()));
            self.output.len() - 1
        });
        if let Output::Tool(tool) = &mut self.output[index] {
            if let Some(id) = call["id"].as_str() {
                tool.id = id.into();
            }
            if tool.name.is_empty()
                && let Some(name) = call["function"]["name"].as_str()
            {
                tool.name = name.into();
            }
            if let Some(arguments) = call["function"]["arguments"].as_str() {
                tool.arguments.push_str(arguments);
                deltas.push(Delta::ToolArguments {
                    output_index: index,
                    arguments: arguments.into(),
                });
            }
        }
        Ok(())
    }

    fn complete(&mut self, deltas: &mut Vec<Delta>) -> Result<(), Error> {
        let stop_reason = self
            .stop_reason
            .clone()
            .ok_or_else(|| Error::Protocol("stream ended without finish_reason".into()))?;
        let mut parts = Vec::new();
        for output in &self.output {
            parts.push(match output {
                Output::Text(text) => Part::Text { text: text.clone() },
                Output::Reasoning { text, details } => Part::Reasoning {
                    text: text.clone(),
                    metadata: BTreeMap::from([
                        ("openrouter".into(), json!(details)),
                        ("provider".into(), json!(self.provider)),
                        ("model".into(), json!(self.model)),
                    ]),
                },
                Output::Tool(tool) => {
                    if tool.id.is_empty() || tool.name.is_empty() {
                        return Err(Error::Protocol("incomplete tool call".into()));
                    }
                    Part::ToolCall {
                        call_id: ToolCallId(tool.id.clone()),
                        tool: tool.name.clone(),
                        input: serde_json::from_str(&tool.arguments).map_err(|error| {
                            Error::Protocol(format!("invalid tool arguments: {error}"))
                        })?,
                    }
                }
            });
        }
        for (output_index, part) in parts.iter().enumerate() {
            deltas.push(Delta::PartDone {
                output_index,
                part: part.clone(),
            });
        }
        deltas.push(Delta::Completed(Response {
            parts,
            stop_reason,
            usage: self.usage.clone(),
            quota_remaining: std::mem::take(&mut self.quota_remaining),
            quota_resets: std::mem::take(&mut self.quota_resets),
        }));
        self.completed = true;
        Ok(())
    }
}

fn append_detail(details: &mut Vec<Value>, detail: &Value) {
    let field = match detail["type"].as_str() {
        Some("reasoning.text") => "text",
        Some("reasoning.summary") => "summary",
        _ => {
            details.push(detail.clone());
            return;
        }
    };
    if let Some(previous) = details.last_mut().filter(|previous| {
        previous["type"] == detail["type"]
            && ["id", "index", "format", "signature"].iter().all(|key| {
                previous[key].is_null() || detail[key].is_null() || previous[key] == detail[key]
            })
    }) && let (Some(old), Some(new)) = (previous[field].as_str(), detail[field].as_str())
    {
        previous[field] = json!(format!("{old}{new}"));
        // Signatures and origin fields may arrive after the initial text delta.
        if let Some(fields) = detail.as_object() {
            for (key, value) in fields {
                if previous[key].is_null() {
                    previous[key] = value.clone();
                }
            }
        }
        return;
    }
    details.push(detail.clone());
}
