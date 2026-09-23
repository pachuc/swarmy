//! Gemini generateContent on the Gemini API and Google Vertex.

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use futures::StreamExt;
use serde_json::{Value, json};
use swarmy_core::{MessageRole, Part, ToolCallId, ToolResult};

use crate::{
    ClientAuth, Delta, Error, Provider, ProviderStream, ReasoningEffort, Request, Response,
    StopReason, TokenUsage,
    catalog::{Api, ModelInfo, ProviderInfo},
    retry::{RetryPolicy, retryable, with_retry},
};

#[derive(Clone)]
pub struct GeminiProvider {
    client: reqwest::Client,
    provider: String,
    model: ModelInfo,
    auth: ClientAuth,
    url: reqwest::Url,
}

/// Construct either Gemini endpoint using catalog overrides for private endpoints.
/// # Errors
/// Rejects missing credentials and invalid endpoint configuration.
pub fn client_for(
    provider: &ProviderInfo,
    model: &ModelInfo,
    auth: ClientAuth,
) -> Result<Arc<dyn Provider>, Error> {
    Ok(Arc::new(GeminiProvider::new(provider, model, auth)?))
}

impl GeminiProvider {
    /// # Errors
    /// Rejects missing credentials and invalid endpoint configuration.
    pub fn new(
        provider: &ProviderInfo,
        model: &ModelInfo,
        auth: ClientAuth,
    ) -> Result<Self, Error> {
        let vertex = model.api.unwrap_or(provider.api) == Api::GoogleVertex;
        match (&auth, vertex) {
            (ClientAuth::ApiKey(key), false) if !key.is_empty() => (),
            (
                ClientAuth::Vertex {
                    project, location, ..
                },
                true,
            ) if !project.is_empty() && !location.is_empty() => (),
            _ => {
                return Err(Error::Credentials(
                    "Gemini requires an API key or Vertex credentials",
                ));
            }
        }
        let base = model.base_url.as_deref().unwrap_or(&provider.base_url);
        let default;
        let base = if base.is_empty() {
            default = match &auth {
                ClientAuth::Vertex { location, .. } if location == "global" => {
                    "https://aiplatform.googleapis.com".into()
                }
                ClientAuth::Vertex { location, .. } => {
                    format!("https://{location}-aiplatform.googleapis.com")
                }
                _ => "https://generativelanguage.googleapis.com/v1beta".into(),
            };
            &default
        } else {
            base
        };
        let url = endpoint_url(base, &model.id, &auth)?;
        Ok(Self {
            client: reqwest::Client::builder()
                .user_agent(concat!("swarmy/", env!("CARGO_PKG_VERSION")))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            provider: provider.id.clone(),
            model: model.clone(),
            auth,
            url,
        })
    }

    /// The resolved URL, including the Vertex resource path when applicable.
    #[must_use]
    pub fn url(&self) -> &reqwest::Url {
        &self.url
    }

    async fn send(&self, body: &Value) -> Result<reqwest::Response, Error> {
        let builder = self
            .client
            .post(self.url.clone())
            .header("accept", "text/event-stream")
            .json(body);
        let builder = match &self.auth {
            ClientAuth::ApiKey(key) => builder.header("x-goog-api-key", key),
            ClientAuth::Vertex { source, .. } => builder.bearer_auth(source.token().await?),
            _ => return Err(Error::Credentials("missing Gemini credentials")),
        };
        let response = builder.send().await?;
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| {
                v.parse::<u64>().ok().map(Duration::from_secs).or_else(|| {
                    httpdate::parse_http_date(v)
                        .ok()
                        .map(|date| date.duration_since(SystemTime::now()).unwrap_or_default())
                })
            });
        let body = response.text().await?;
        if overflow(&body) {
            return Err(Error::ContextOverflow(
                "Gemini input token count exceeds the maximum".into(),
            ));
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

/// Keep the provider's own explanation; a bare status hides configuration mistakes.
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

fn endpoint_url(base: &str, model: &str, auth: &ClientAuth) -> Result<reqwest::Url, Error> {
    let mut url =
        reqwest::Url::parse(base).map_err(|_| Error::Protocol("invalid Gemini base URL".into()))?;
    let versioned = url.path().trim_end_matches('/').ends_with("/v1");
    let mut path = url
        .path_segments_mut()
        .map_err(|()| Error::Protocol("invalid Gemini URL path".into()))?;
    path.pop_if_empty();
    if let ClientAuth::Vertex {
        project, location, ..
    } = auth
    {
        if !versioned {
            path.push("v1");
        }
        path.extend([
            "projects",
            project,
            "locations",
            location,
            "publishers",
            "google",
        ]);
    }
    path.push("models")
        .push(&format!("{model}:streamGenerateContent"));
    drop(path);
    url.query_pairs_mut().append_pair("alt", "sse");
    Ok(url)
}

impl Provider for GeminiProvider {
    fn request(&self, request: Request) -> ProviderStream {
        let this = self.clone();
        Box::pin(async_stream::try_stream! {
            if request.settings.model != this.model.id {
                Err(Error::Protocol("request model differs from Gemini client model".into()))?;
            }
            let body = request_json(&request, &this.provider, &this.model)?;
            let response = with_retry(&RetryPolicy::default(), || this.send(&body)).await?;
            let mut bytes = response.bytes_stream();
            let mut sse = Sse::default();
            let mut state = StreamState::default();
            while let Some(chunk) = bytes.next().await {
                for event in sse.push(&chunk?)? {
                    for delta in state.event(&event)? { yield delta; }
                }
            }
            for event in sse.finish()? {
                for delta in state.event(&event)? { yield delta; }
            }
            let response = state.finish(&this.provider, &this.model.id)?;
            for (output_index, part) in response.parts.iter().enumerate() {
                yield Delta::PartDone { output_index, part: part.clone() };
            }
            yield Delta::Completed(response);
        })
    }
}

fn thinking_config(model: &str, effort: ReasoningEffort) -> Option<Value> {
    use ReasoningEffort::{High, Low, Max, Medium, Minimal, None, Xhigh};
    if model.starts_with("gemini-3") {
        let level = match effort {
            None | Minimal => "MINIMAL",
            Low => "LOW",
            Medium => "MEDIUM",
            High | Xhigh | Max => "HIGH",
        };
        Some(json!({"includeThoughts": true, "thinkingLevel": level}))
    } else if model.starts_with("gemini-2.5") {
        let budget = match effort {
            None => 0,
            Minimal if model.contains("flash-lite") => 512,
            Minimal => 128,
            Low => 2048,
            Medium => 8192,
            High | Xhigh | Max if model.contains("pro") => 32768,
            High | Xhigh | Max => 24576,
        };
        // A zero budget disables thinking, and the API rejects asking for thoughts then.
        if budget == 0 {
            Some(json!({"thinkingBudget": 0}))
        } else {
            Some(json!({"includeThoughts": true, "thinkingBudget": budget}))
        }
    } else {
        Option::None
    }
}

/// Translate durable message parts, preserving signatures only for their origin.
/// # Errors
/// Rejects tool results without a call and invalid generation settings.
pub fn request_json(request: &Request, provider: &str, model: &ModelInfo) -> Result<Value, Error> {
    let mut contents = Vec::new();
    let mut pending = BTreeMap::new();
    let mut system = vec![json!({"text": request.system_prompt})];
    for message in &request.messages {
        if message.role != MessageRole::Tool && !pending.is_empty() {
            flush_orphans(&mut contents, &mut pending);
        }
        let mut parts = Vec::new();
        // Text and tool calls have no metadata field in the durable core format.
        // Metadata-only reasoning parts address their sibling by its core index.
        let signatures: BTreeMap<usize, &Value> = message
            .parts
            .iter()
            .filter_map(|part| {
                let Part::Reasoning { metadata, .. } = part else {
                    return None;
                };
                let meta = metadata.get("google")?;
                if meta["provider"] != provider || meta["model"] != model.id {
                    return None;
                }
                Some((
                    usize::try_from(meta["target_index"].as_u64()?).ok()?,
                    &meta["thoughtSignature"],
                ))
            })
            .collect();
        for (index, part) in message.parts.iter().enumerate() {
            let mut wire = match part {
                Part::Text { text } => json!({"text": text}),
                Part::Reasoning { text, metadata } => {
                    let meta = metadata.get("google");
                    if meta.is_some_and(|m| m.get("target_index").is_some()) {
                        continue;
                    }
                    if text.is_empty() && meta.is_none() {
                        continue;
                    }
                    if let Some(meta) =
                        meta.filter(|m| m["provider"] == provider && m["model"] == model.id)
                    {
                        let mut wire = json!({"text": text, "thought": true});
                        if let Some(signature) = meta.get("thoughtSignature") {
                            wire["thoughtSignature"] = signature.clone();
                        }
                        wire
                    } else {
                        json!({"text": text})
                    }
                }
                Part::ToolCall {
                    call_id,
                    tool,
                    input,
                } => {
                    pending.insert(call_id.0.clone(), tool.clone());
                    json!({"functionCall": {"id": call_id.0, "name": tool, "args": input}})
                }
                Part::ToolResult { call_id, result } => {
                    let name = pending.remove(&call_id.0).ok_or_else(|| {
                        Error::Protocol("Gemini tool result has no pending call".into())
                    })?;
                    let response = match result {
                        ToolResult::Completed { output, .. } => json!({"output": output}),
                        ToolResult::Error { error } => json!({"error": error}),
                    };
                    json!({"functionResponse": {"id": call_id.0, "name": name, "response": response}})
                }
            };
            if let Some(signature) = signatures.get(&index) {
                wire["thoughtSignature"] = (*signature).clone();
            }
            parts.push(wire);
        }
        if message.role == MessageRole::System {
            system.extend(parts);
        } else {
            append_content(
                &mut contents,
                if message.role == MessageRole::Assistant {
                    "model"
                } else {
                    "user"
                },
                parts,
            );
        }
    }
    flush_orphans(&mut contents, &mut pending);
    let config = generation_config(request, model)?;
    let mut body = json!({"systemInstruction": {"parts": system}, "contents": contents, "generationConfig": config});
    if !request.tools.is_empty() {
        let declarations: Vec<_> = request.tools.iter().map(|tool| json!({"name": tool.name, "description": tool.description, "parametersJsonSchema": tool.parameters})).collect();
        body["tools"] = json!([{"functionDeclarations": declarations}]);
        body["toolConfig"] = json!({"functionCallingConfig": {"mode": "AUTO"}});
    }
    Ok(body)
}

fn generation_config(request: &Request, model: &ModelInfo) -> Result<Value, Error> {
    let mut config = json!({});
    if let Some(maximum) = request.settings.max_output_tokens.or(model.limit.output) {
        config["maxOutputTokens"] = json!(maximum);
    }
    if let Some(temperature) = request.settings.temperature {
        if !temperature.is_finite() {
            return Err(Error::Protocol("temperature must be finite".into()));
        }
        config["temperature"] = json!(temperature);
    }
    if let Some(effort) = request.settings.reasoning_effort
        && let Some(thinking) = thinking_config(
            &model.id,
            if model.id.starts_with("gemini-3") {
                model.clamp_effort(effort).0
            } else {
                effort
            },
        )
    {
        config["thinkingConfig"] = thinking;
    }
    Ok(config)
}

fn append_content(contents: &mut Vec<Value>, role: &str, parts: Vec<Value>) {
    if parts.is_empty() {
        return;
    }
    if let Some(last) = contents.last_mut().filter(|v| v["role"] == role) {
        if let Some(existing) = last["parts"].as_array_mut() {
            existing.extend(parts);
        }
    } else {
        contents.push(json!({"role": role, "parts": parts}));
    }
}

fn flush_orphans(contents: &mut Vec<Value>, pending: &mut BTreeMap<String, String>) {
    let parts = std::mem::take(pending).into_iter().map(|(id, name)| json!({"functionResponse": {"id": id, "name": name, "response": {"error": "No result provided"}}})).collect();
    append_content(contents, "user", parts);
}

fn overflow(text: &str) -> bool {
    let text = text.to_ascii_lowercase();
    text.contains("input token count") && text.contains("exceeds the maximum")
}

#[derive(Default)]
struct Sse {
    line: Vec<u8>,
    data: Vec<u8>,
    previous_cr: bool,
}
impl Sse {
    fn push(&mut self, bytes: &[u8]) -> Result<Vec<Value>, Error> {
        let mut events = Vec::new();
        for &byte in bytes {
            if byte == b'\n' && self.previous_cr {
                self.previous_cr = false;
                continue;
            }
            self.previous_cr = byte == b'\r';
            if matches!(byte, b'\r' | b'\n') {
                self.end_line(&mut events)?;
            } else {
                self.line.push(byte);
            }
            if self.line.len() + self.data.len() > 8 * 1024 * 1024 {
                return Err(Error::Protocol("Gemini SSE event exceeds 8 MiB".into()));
            }
        }
        Ok(events)
    }
    fn end_line(&mut self, events: &mut Vec<Value>) -> Result<(), Error> {
        let line = std::mem::take(&mut self.line);
        if line.is_empty() {
            if !self.data.is_empty() {
                let data = std::mem::take(&mut self.data);
                if data != b"[DONE]\n" {
                    events.push(serde_json::from_slice(&data)?);
                }
            }
        } else if let Some(data) = line.strip_prefix(b"data:") {
            self.data
                .extend_from_slice(data.strip_prefix(b" ").unwrap_or(data));
            self.data.push(b'\n');
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<Vec<Value>, Error> {
        let mut events = Vec::new();
        self.end_line(&mut events)?;
        self.end_line(&mut events)?;
        Ok(events)
    }
}

#[derive(Default)]
struct StreamState {
    parts: Vec<Value>,
    reason: Option<String>,
    usage: TokenUsage,
}
impl StreamState {
    fn event(&mut self, event: &Value) -> Result<Vec<Delta>, Error> {
        if let Some(error) = event.get("error") {
            return Err(if overflow(&error.to_string()) {
                Error::ContextOverflow("Gemini input token count exceeds the maximum".into())
            } else {
                Error::Protocol("Gemini stream returned a provider error".into())
            });
        }
        if let Some(usage) = event.get("usageMetadata") {
            let count = |key: &str| usage[key].as_u64().unwrap_or_default();
            let prompt = count("promptTokenCount");
            let cached = count("cachedContentTokenCount");
            let reasoning = count("thoughtsTokenCount");
            let output = count("candidatesTokenCount").saturating_add(reasoning);
            self.usage = TokenUsage {
                input_tokens: prompt.saturating_sub(cached),
                cached_input_tokens: cached,
                cache_write_input_tokens: 0,
                output_tokens: output,
                reasoning_output_tokens: reasoning,
                total_tokens: usage["totalTokenCount"]
                    .as_u64()
                    .unwrap_or_else(|| prompt.saturating_add(output)),
            };
        }
        let candidate = &event["candidates"][0];
        if let Some(reason) = candidate["finishReason"].as_str() {
            self.reason = Some(reason.into());
        }
        if event["promptFeedback"]["blockReason"].is_string() {
            self.reason = Some("SAFETY".into());
        }
        let mut deltas = Vec::new();
        if let Some(parts) = candidate["content"]["parts"].as_array() {
            for part in parts {
                self.part(part, &mut deltas)?;
            }
        }
        Ok(deltas)
    }

    fn part(&mut self, part: &Value, deltas: &mut Vec<Delta>) -> Result<(), Error> {
        if let Some(text) = part["text"].as_str() {
            let thought = part["thought"] == true;
            let merge = self
                .parts
                .last()
                .is_some_and(|p| p["text"].is_string() && (p["thought"] == true) == thought);
            let index = if merge {
                self.parts.len() - 1
            } else {
                self.parts.push(json!({"text": "", "thought": thought}));
                self.parts.len() - 1
            };
            let existing = self.parts[index]["text"].as_str().unwrap_or_default();
            self.parts[index]["text"] = json!(format!("{existing}{text}"));
            if let Some(sig) = part.get("thoughtSignature") {
                self.parts[index]["thoughtSignature"] = sig.clone();
            }
            deltas.push(if thought {
                Delta::Reasoning {
                    output_index: index,
                    text: text.into(),
                }
            } else {
                Delta::Text {
                    output_index: index,
                    text: text.into(),
                }
            });
        } else if let Some(call) = part.get("functionCall") {
            if !call["name"].is_string() {
                return Err(Error::Protocol("Gemini function call has no name".into()));
            }
            let mut part = part.clone();
            if !call["id"].is_string() {
                part["functionCall"]["id"] = json!(format!("gemini-{}", ulid::Ulid::generate()));
            }
            let index = self.parts.len();
            deltas.push(Delta::ToolArguments {
                output_index: index,
                arguments: call.get("args").unwrap_or(&json!({})).to_string(),
            });
            self.parts.push(part);
        } else if let Some(signature) = part.get("thoughtSignature") {
            if let Some(last) = self.parts.last_mut() {
                last["thoughtSignature"] = signature.clone();
            } else {
                return Err(Error::Protocol("Gemini signature has no content".into()));
            }
        }
        Ok(())
    }

    fn finish(self, provider: &str, model: &str) -> Result<Response, Error> {
        let reason = self
            .reason
            .ok_or_else(|| Error::Protocol("Gemini stream closed before finishReason".into()))?;
        let calls = self.parts.iter().any(|p| p.get("functionCall").is_some());
        let stop_reason = match reason.as_str() {
            "STOP" if calls => StopReason::ToolCalls,
            "STOP" => StopReason::EndTurn,
            "MAX_TOKENS" => StopReason::MaxOutputTokens,
            "SAFETY" | "RECITATION" => StopReason::ContentFilter,
            _ => StopReason::Incomplete(reason),
        };
        let mut parts = Vec::new();
        let mut signatures = Vec::new();
        for (index, wire) in self.parts.into_iter().enumerate() {
            let mut meta = json!({"provider": provider, "model": model});
            if let Some(signature) = wire.get("thoughtSignature") {
                meta["thoughtSignature"] = signature.clone();
            }
            let thought = wire["thought"] == true;
            let part = if let Some(call) = wire.get("functionCall") {
                Part::ToolCall {
                    call_id: ToolCallId(call["id"].as_str().unwrap_or_default().into()),
                    tool: call["name"].as_str().unwrap_or_default().into(),
                    input: call.get("args").cloned().unwrap_or_else(|| json!({})),
                }
            } else if thought {
                Part::Reasoning {
                    text: wire["text"].as_str().unwrap_or_default().into(),
                    metadata: BTreeMap::from([("google".into(), meta.clone())]),
                }
            } else {
                Part::Text {
                    text: wire["text"].as_str().unwrap_or_default().into(),
                }
            };
            parts.push(part);
            if !thought && meta.get("thoughtSignature").is_some() {
                meta["target_index"] = json!(index);
                signatures.push(Part::Reasoning {
                    text: String::new(),
                    metadata: BTreeMap::from([("google".into(), meta)]),
                });
            }
        }
        parts.extend(signatures);
        Ok(Response {
            parts,
            stop_reason,
            usage: self.usage,
        })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn zero_thinking_budget_never_requests_thoughts() {
        let off = thinking_config("gemini-2.5-flash", ReasoningEffort::None).unwrap();
        assert_eq!(off, json!({"thinkingBudget": 0}));
        let on = thinking_config("gemini-2.5-flash", ReasoningEffort::Medium).unwrap();
        assert_eq!(on["includeThoughts"], json!(true));
        assert_eq!(on["thinkingBudget"], json!(8192));
    }

    #[test]
    fn provider_errors_keep_the_message() {
        let error = provider_error(
            reqwest::StatusCode::BAD_REQUEST,
            r#"{"error": {"code": 400, "message": "thinking is disabled"}}"#,
        );
        assert!(error.to_string().contains("thinking is disabled"));
        assert!(matches!(
            provider_error(reqwest::StatusCode::FORBIDDEN, ""),
            Error::Status(reqwest::StatusCode::FORBIDDEN)
        ));
    }

    use super::*;

    #[test]
    fn sse_survives_every_byte_boundary_and_trailing_usage() {
        let wire = concat!(
            ": keepalive\r\nevent: message\r\n",
            "data: {\r\ndata: \"candidates\":[{\"content\":{\"parts\":[{\"text\":\"世界\"}]},\"finishReason\":\"STOP\"}]}\r\n\r\n",
            "data: {\"usageMetadata\":{\"promptTokenCount\":3,\"candidatesTokenCount\":2}}"
        );
        for split in 0..=wire.len() {
            let mut sse = Sse::default();
            let mut events = sse.push(&wire.as_bytes()[..split]).unwrap();
            events.extend(sse.push(&wire.as_bytes()[split..]).unwrap());
            events.extend(sse.finish().unwrap());
            let mut state = StreamState::default();
            for event in events {
                state.event(&event).unwrap();
            }
            let response = state.finish("google", "model").unwrap();
            assert_eq!(
                response.parts,
                vec![Part::Text {
                    text: "世界".into()
                }]
            );
            assert_eq!(response.usage.total_tokens, 5);
        }
    }

    #[test]
    fn malformed_and_blocked_streams() {
        assert!(Sse::default().push(b"data: nope\n\n").is_err());
        assert!(
            Sse::default()
                .push(&vec![b'x'; 8 * 1024 * 1024 + 1])
                .is_err()
        );
        let mut state = StreamState::default();
        state
            .event(&json!({"promptFeedback":{"blockReason":"SAFETY"}}))
            .unwrap();
        assert_eq!(
            state.finish("google", "model").unwrap().stop_reason,
            StopReason::ContentFilter
        );
        let mut state = StreamState::default();
        assert!(matches!(
            state.event(&json!({"error":{"message":"Input token count exceeds the maximum"}})),
            Err(Error::ContextOverflow(_))
        ));
        assert!(
            state
                .event(&json!({"candidates":[{"content":{"parts":[{"functionCall":{}}]}}]}))
                .is_err()
        );
    }
}
