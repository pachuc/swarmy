//! Responses request normalization and incremental stream parsing.
use crate::{Delta, Error, Request, Response, StopReason, TokenUsage};
use crate::{
    ReasoningEffort,
    api::responses::ResponsesEndpoint,
    catalog::{Compat, ModelInfo, ReasoningOptions},
};
use base64::Engine as _;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use swarmy_core::{MessageRole, Part, SessionId, ToolCallId, ToolResult};

/// Legacy Codex codec retained for existing fixture consumers. Transports use
/// `request_json_for`, which enforces catalog compatibility and replay provenance.
/// # Errors
/// Rejects uncorrelated tool text and non-finite temperatures.
pub fn request_json(request: &Request) -> Result<Value, Error> {
    build_request(request, None)
}

struct RequestContext<'a> {
    provider: &'a str,
    model: Option<&'a ModelInfo>,
    compat: &'a Compat,
    codex: bool,
}

/// Build a catalog-aware request, with session affinity when available.
/// # Errors
/// Rejects uncorrelated tool text and non-finite temperatures.
pub fn request_json_for(
    request: &Request,
    endpoint: &ResponsesEndpoint,
    provider: &str,
    model: Option<&ModelInfo>,
    session: Option<SessionId>,
) -> Result<Value, Error> {
    let context = RequestContext {
        provider,
        model,
        compat: &endpoint.compat,
        codex: endpoint.codex,
    };
    let mut value = build_request(request, Some(&context))?;
    if let Some(session) = session {
        value["prompt_cache_key"] = json!(session.to_string());
    }
    if context.compat.supports_long_cache_retention() == Some(true) && !context.codex {
        value["prompt_cache_retention"] = json!("24h");
    }
    Ok(value)
}

fn build_input(
    request: &Request,
    context: Option<&RequestContext<'_>>,
) -> Result<Vec<Value>, Error> {
    let mut durable = request.messages.clone();
    // The legacy codec without catalog context preserves existing fixtures.
    // Transports repair old sessions so provider switches never fail.
    if context.is_some()
        && crate::transcript::synthesize_missing_tool_calls(&mut durable, &request.tools)
    {
        tracing::warn!("repaired orphan tool result; synthesized assistant tool call");
    }
    let call_tools = call_tool_names(&durable, &request.tools);
    let fallback = request
        .tools
        .first()
        .map_or("unknown_tool", |tool| tool.name.as_str())
        .to_owned();
    let mut input = Vec::new();
    let developer = context.is_none_or(|ctx| ctx.compat.supports_developer_role() == Some(true));
    if context.is_some_and(|ctx| !ctx.codex) && !request.system_prompt.is_empty() {
        input.push(text_item(
            if developer { "developer" } else { "system" },
            "input_text",
            &request.system_prompt,
        ));
    }
    for message in &durable {
        for part in &message.parts {
            if let Part::Reasoning { text, metadata } = part {
                let saved = metadata
                    .get("openai_responses")
                    .or_else(|| metadata.get("chatgpt"));
                if let Some(saved) = saved {
                    let item = saved.get("item").unwrap_or(saved);
                    let same = context.is_none_or(|ctx| {
                        saved["provider"] == ctx.provider
                            && saved["model"] == request.settings.model
                    });
                    if same && item["type"] == "reasoning" {
                        input.push(item.clone());
                        continue;
                    }
                }
                if !text.is_empty() {
                    input.push(text_item("assistant", "output_text", text));
                }
                continue;
            }
            input.push(match part {
                Part::Image { media_type, bytes, detail, .. } => json!({"type": "message", "role": "user", "content": [{"type": "input_image", "image_url": format!("data:{media_type};base64,{}", base64::engine::general_purpose::STANDARD.encode(bytes)), "detail": detail.as_deref().unwrap_or("auto") }]}),
                Part::Text { text } => {
                    let (role, kind) = match message.role {
                        // Harness notices use the backend's developer role. A system
                        // role in input is rejected after a computer rebuild.
                        MessageRole::System => (if developer { "developer" } else { "system" }, "input_text"),
                        MessageRole::User => ("user", "input_text"),
                        MessageRole::Assistant => ("assistant", "output_text"),
                        MessageRole::Tool => return Err(Error::Protocol("tool text requires a tool result call id".into())),
                    };
                    json!({"type": "message", "role": role, "content": [{"type": kind, "text": text}]})
                }
                Part::ToolCall { call_id, tool, input } => json!({"type": "function_call", "call_id": call_id.0, "name": tool, "arguments": serde_json::to_string(input)?}),
                Part::ToolResult { call_id, result } => {
                    let output = match result {
                        ToolResult::Completed { output, .. } => output.clone(),
                        ToolResult::Error { error } => serde_json::to_string(&json!({"error": error}))?,
                    };
                    json!({"type": "function_call_output", "call_id": call_id.0, "output": output})
                }
                Part::Reasoning { .. } => continue,
            });
        }
    }
    if context.is_some() {
        normalize_calls(&mut input, &call_tools, &fallback);
    }
    Ok(input)
}

fn build_request(request: &Request, context: Option<&RequestContext<'_>>) -> Result<Value, Error> {
    let input = build_input(request, context)?;
    let tools: Vec<_> = request
        .tools
        .iter()
        .map(|tool| {
            json!({
                "type": "function", "name": tool.name, "description": tool.description,
                "parameters": tool.parameters, "strict": context.is_some_and(|ctx| ctx.compat.supports_strict_mode() == Some(true)) && strict_schema(&tool.parameters),
            })
        })
        .collect();
    let mut value = json!({
        "model": request.settings.model, "instructions": request.system_prompt, "input": input,
        "tools": tools, "tool_choice": "auto", "parallel_tool_calls": true,
        "stream": true, "store": false, "include": ["reasoning.encrypted_content"],
    });
    if context.is_some_and(|ctx| !ctx.codex) {
        value
            .as_object_mut()
            .expect("request is an object")
            .remove("instructions");
    }
    let effort = match context {
        None => request.settings.reasoning_effort,
        Some(ctx) => ctx.model.filter(|model| model.reasoning.is_some()).and_then(|model| {
            let (effort, _) = model.clamp_effort(request.settings.reasoning_effort.unwrap_or(ReasoningEffort::Medium));
            let explicit_none = matches!(&model.reasoning, Some(ReasoningOptions::Effort(efforts)) if efforts.contains(&ReasoningEffort::None));
            (effort != ReasoningEffort::None || explicit_none).then_some(effort)
        }),
    };
    if let Some(effort) = effort {
        value["reasoning"] = json!({"effort": effort, "summary": "auto"});
    }
    if context.is_some_and(|ctx| {
        request.settings.model.starts_with("gpt-5")
            || ctx.model.is_some_and(|model| {
                // Azure deployment ids can differ from the catalog's model name.
                model.name.starts_with("GPT-5")
                    || model
                        .family
                        .as_deref()
                        .is_some_and(|family| family.starts_with("gpt-5"))
            })
    }) {
        value["text"] = json!({"verbosity": "low"});
    }
    if let Some(maximum) = request.settings.max_output_tokens {
        value["max_output_tokens"] = json!(maximum);
    }
    if let Some(temperature) = request.settings.temperature {
        if !temperature.is_finite() {
            return Err(Error::Protocol("temperature must be finite".into()));
        }
        value["temperature"] = json!(temperature);
    }
    Ok(value)
}

// Strict mode requires every object property to be required. Keep optional
// parameters and schemas outside this supported subset unchanged and non-strict.
fn strict_schema(schema: &Value) -> bool {
    let Some(object) = schema.as_object() else {
        return false;
    };
    if object.keys().any(|key| {
        !matches!(
            key.as_str(),
            "type"
                | "properties"
                | "required"
                | "additionalProperties"
                | "items"
                | "enum"
                | "description"
                | "title"
                | "anyOf"
        )
    }) {
        return false;
    }
    match schema["type"].as_str() {
        Some("object") => {
            let Some(properties) = schema["properties"].as_object() else {
                return false;
            };
            schema["additionalProperties"] == false
                && properties.iter().all(|(name, property)| {
                    schema["required"]
                        .as_array()
                        .is_some_and(|required| required.iter().any(|value| value == name))
                        && strict_schema(property)
                })
        }
        Some("array") => strict_schema(&schema["items"]),
        Some("string" | "number" | "integer" | "boolean" | "null") => true,
        _ => schema["anyOf"].as_array().is_some_and(|alternatives| {
            !alternatives.is_empty() && alternatives.iter().all(strict_schema)
        }),
    }
}

fn text_item(role: &str, kind: &str, text: &str) -> Value {
    json!({"type": "message", "role": role, "content": [{"type": kind, "text": text}]})
}

fn call_tool_names(
    messages: &[swarmy_core::Message],
    _tools: &[crate::ToolDefinition],
) -> std::collections::BTreeMap<String, String> {
    let mut names = std::collections::BTreeMap::new();
    for message in messages {
        for part in &message.parts {
            match part {
                swarmy_core::Part::ToolCall { call_id, tool, .. } => {
                    names.insert(call_id.0.clone(), tool.clone());
                }
                swarmy_core::Part::ToolResult { call_id, result } => {
                    if let swarmy_core::ToolResult::Completed { title, .. } = result
                        && !title.is_empty()
                    {
                        names
                            .entry(call_id.0.clone())
                            .or_insert_with(|| title.clone());
                    }
                }
                _ => {}
            }
        }
    }
    names
}

fn normalize_calls(
    input: &mut Vec<Value>,
    call_tools: &std::collections::BTreeMap<String, String>,
    fallback: &str,
) {
    // Old sessions can hold a result whose call was never stored neutrally.
    // Synthesize the missing call from the result so provider switches succeed.
    let mut calls: std::collections::BTreeSet<String> = input
        .iter()
        .filter(|item| item["type"] == "function_call")
        .filter_map(|item| item["call_id"].as_str().map(str::to_owned))
        .collect();
    let mut repaired = false;
    let mut with_calls = Vec::with_capacity(input.len() * 2);
    for item in input.drain(..) {
        if item["type"] == "function_call_output"
            && let Some(id) = item["call_id"].as_str()
            && !calls.contains(id)
        {
            let tool = call_tools
                .get(id)
                .cloned()
                .unwrap_or_else(|| fallback.to_owned());
            with_calls.push(
                json!({"type": "function_call", "call_id": id, "name": tool, "arguments": "{}"}),
            );
            calls.insert(id.to_owned());
            repaired = true;
        }
        if item["type"] == "function_call"
            && let Some(id) = item["call_id"].as_str()
        {
            calls.insert(id.to_owned());
        }
        with_calls.push(item);
    }
    *input = with_calls;
    // Hash unsupported ids so replacements remain stable across calls and results
    // without colliding with another id after punctuation is removed.
    for item in input.iter_mut() {
        if let Some(id) = item["call_id"].as_str()
            && (id.len() > 64
                || id.is_empty()
                || !id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-')))
        {
            item["call_id"] = json!(blake3::hash(id.as_bytes()).to_hex().to_string());
        }
    }
    let results: std::collections::BTreeSet<_> = input
        .iter()
        .filter(|item| item["type"] == "function_call_output")
        .filter_map(|item| item["call_id"].as_str().map(str::to_owned))
        .collect();
    let mut normalized = Vec::with_capacity(input.len());
    for item in std::mem::take(input) {
        let missing = item["type"] == "function_call"
            && item["call_id"]
                .as_str()
                .is_some_and(|id| !results.contains(id));
        let result = missing.then(|| json!({"type": "function_call_output", "call_id": item["call_id"], "output": "Error: No result provided"}));
        normalized.push(item);
        if let Some(result) = result {
            normalized.push(result);
        }
    }
    *input = normalized;
    if repaired {
        tracing::warn!("repaired orphan tool result; synthesized assistant tool call");
    }
}

/// Incremental SSE parser, including CRLF, multiline data, comments, and UTF-8
/// split across arbitrary network chunks. Unknown event types are ignored.
#[derive(Default, Clone, Debug)]
struct PendingTool {
    call_id: String,
    name: String,
    arguments: String,
}

#[derive(Default)]
pub struct SseParser {
    line: Vec<u8>,
    data: Vec<u8>,
    previous_cr: bool,
    output: BTreeMap<usize, Vec<Part>>,
    text_content: BTreeMap<usize, BTreeMap<usize, String>>,
    pending_tools: BTreeMap<usize, PendingTool>,
    context: Option<(String, String)>,
    completed: bool,
}

impl SseParser {
    /// Record provenance for safe reasoning replay. The default parser retains
    /// the legacy metadata shape for callers of the original standalone codec.
    #[must_use]
    pub fn with_context(provider: &str, model: &str) -> Self {
        Self {
            context: Some((provider.into(), model.into())),
            ..Self::default()
        }
    }

    fn item_parts(&self, item: &Value) -> Result<Vec<Part>, Error> {
        let mut parts = item_parts(item)?;
        if let Some((provider, model)) = &self.context {
            for part in &mut parts {
                if let Part::Reasoning { metadata, .. } = part {
                    metadata.clear();
                    metadata.insert(
                        "openai_responses".into(),
                        json!({"provider": provider, "model": model, "item": item}),
                    );
                }
            }
        }
        Ok(parts)
    }
    #[must_use]
    pub const fn is_completed(&self) -> bool {
        self.completed
    }

    /// # Errors
    /// Rejects malformed events, provider errors, and events over 8 MiB.
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
            if matches!(byte, b'\n' | b'\r') {
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
                if data == b"[DONE]\n" {
                    return Err(Error::Protocol(
                        "stream ended without response.completed".into(),
                    ));
                }
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
    /// An EOF before a terminal response is an error, never a partial success.
    pub fn finish(&self) -> Result<(), Error> {
        if self.completed {
            Ok(())
        } else {
            Err(Error::Protocol("stream closed before completion".into()))
        }
    }

    fn save_text(&mut self, output_index: usize) -> Part {
        let part = Part::Text {
            text: self.text_content[&output_index]
                .values()
                .cloned()
                .collect::<String>(),
        };
        self.output.insert(output_index, vec![part.clone()]);
        part
    }

    fn pending_part(pending: &PendingTool) -> Result<Part, Error> {
        if pending.call_id.is_empty() || pending.name.is_empty() {
            return Err(Error::Protocol("incomplete tool call".into()));
        }
        Ok(Part::ToolCall {
            call_id: ToolCallId(pending.call_id.clone()),
            tool: pending.name.clone(),
            input: serde_json::from_str(&pending.arguments)
                .map_err(|error| Error::Protocol(format!("invalid tool arguments: {error}")))?,
        })
    }

    fn streamed_parts(&mut self) -> Vec<Part> {
        // Build unfinished text only at completion; copying it on every delta
        // makes long responses unnecessarily expensive.
        let unfinished: Vec<_> = self
            .text_content
            .keys()
            .filter(|index| !self.output.contains_key(index))
            .copied()
            .collect();
        for index in unfinished {
            self.save_text(index);
        }
        // Every tool call is stored as a neutral part, even when the terminal
        // output is empty (Codex) or an output_item.done was never observed.
        // Encrypted reasoning never replaces the separate function call item.
        let pending: Vec<(usize, PendingTool)> = self
            .pending_tools
            .iter()
            .filter(|(index, _)| !self.output.contains_key(*index))
            .map(|(index, pending)| (*index, pending.clone()))
            .collect();
        for (index, pending) in pending {
            if let Ok(part) = Self::pending_part(&pending) {
                self.output.insert(index, vec![part]);
            }
        }
        self.pending_tools.clear();
        std::mem::take(&mut self.output)
            .into_values()
            .flatten()
            .collect()
    }

    fn event(&mut self, event: &Value, deltas: &mut Vec<Delta>) -> Result<(), Error> {
        match string(event, "type")? {
            "response.output_text.delta" | "response.refusal.delta" => {
                let output_index = index(event)?;
                let text = string(event, "delta")?.to_owned();
                let content_index = content_index(event);
                self.text_content
                    .entry(output_index)
                    .or_default()
                    .entry(content_index)
                    .or_default()
                    .push_str(&text);
                deltas.push(Delta::Text { output_index, text });
            }
            "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => deltas
                .push(Delta::Reasoning {
                    output_index: index(event)?,
                    text: string(event, "delta")?.to_owned(),
                }),
            "response.output_item.added" => {
                let output_index = index(event)?;
                let item = &event["item"];
                if item["type"] == "function_call" {
                    let pending = self.pending_tools.entry(output_index).or_default();
                    if let Some(call_id) = item["call_id"].as_str() {
                        pending.call_id = call_id.into();
                    }
                    if let Some(name) = item["name"].as_str() {
                        pending.name = name.into();
                    }
                    if let Some(arguments) = item["arguments"].as_str() {
                        pending.arguments = arguments.into();
                    }
                }
            }
            "response.function_call_arguments.delta" => {
                let output_index = index(event)?;
                let delta = string(event, "delta")?.to_owned();
                self.pending_tools
                    .entry(output_index)
                    .or_default()
                    .arguments
                    .push_str(&delta);
                deltas.push(Delta::ToolArguments {
                    output_index,
                    arguments: delta,
                });
            }
            "response.output_text.done" => {
                let output_index = index(event)?;
                let content_index = content_index(event);
                self.text_content
                    .entry(output_index)
                    .or_default()
                    .insert(content_index, string(event, "text")?.to_owned());
                let part = self.save_text(output_index);
                deltas.push(Delta::PartDone { output_index, part });
            }
            "response.output_item.done" => {
                let output_index = index(event)?;
                let parts = self.item_parts(&event["item"])?;
                for part in &parts {
                    deltas.push(Delta::PartDone {
                        output_index,
                        part: part.clone(),
                    });
                }
                // An authoritative item replaces any delta-buffered fragments.
                self.pending_tools.remove(&output_index);
                self.output.insert(output_index, parts);
            }
            "response.completed" | "response.incomplete" => {
                self.complete_event(event, deltas)?;
            }
            "response.failed" => return Err(provider_error(&event["response"]["error"])),
            "error" => return Err(provider_error(event.get("error").unwrap_or(event))),
            _ => (),
        }
        Ok(())
    }

    fn complete_event(&mut self, event: &Value, deltas: &mut Vec<Delta>) -> Result<(), Error> {
        let response = &event["response"];
        if !response.is_object() {
            return Err(Error::Protocol("missing completed response".into()));
        }
        if response["status"] == "failed" {
            return Err(provider_error(&response["error"]));
        }
        let parts = self.terminal_parts(response)?;
        let stop_reason = terminal_stop(event, response, &parts);
        deltas.push(Delta::Completed(Response {
            parts,
            stop_reason,
            usage: usage(&response["usage"]),
        }));
        self.completed = true;
        Ok(())
    }

    fn terminal_parts(&mut self, response: &Value) -> Result<Vec<Part>, Error> {
        // The Codex backend sends an empty output array in the terminal
        // event; the items already collected from output_item.done are
        // authoritative in that case.
        match response["output"].as_array() {
            Some(output) if !output.is_empty() => {
                let mut parts: Vec<Part> = output
                    .iter()
                    .map(|item| self.item_parts(item))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .flatten()
                    .collect();
                let seen: std::collections::BTreeSet<_> = parts
                    .iter()
                    .filter_map(|part| match part {
                        Part::ToolCall { call_id, .. } => Some(call_id.0.clone()),
                        _ => None,
                    })
                    .collect();
                for pending in std::mem::take(&mut self.pending_tools).into_values() {
                    if !pending.call_id.is_empty()
                        && !seen.contains(&pending.call_id)
                        && let Ok(part) = Self::pending_part(&pending)
                    {
                        parts.push(part);
                    }
                }
                Ok(parts)
            }
            _ => Ok(self.streamed_parts()),
        }
    }
}

fn terminal_stop(event: &Value, response: &Value, parts: &[Part]) -> StopReason {
    if event["type"] == "response.incomplete" || response["status"] == "incomplete" {
        match response["incomplete_details"]["reason"]
            .as_str()
            .unwrap_or("unknown")
        {
            "max_output_tokens" => StopReason::MaxOutputTokens,
            "content_filter" => StopReason::ContentFilter,
            reason => StopReason::Incomplete(reason.to_owned()),
        }
    } else if parts
        .iter()
        .any(|part| matches!(part, Part::ToolCall { .. }))
    {
        StopReason::ToolCalls
    } else {
        StopReason::EndTurn
    }
}

fn content_index(event: &Value) -> usize {
    event["content_index"]
        .as_u64()
        .and_then(|i| usize::try_from(i).ok())
        .unwrap_or(0)
}

fn index(event: &Value) -> Result<usize, Error> {
    event["output_index"]
        .as_u64()
        .and_then(|i| usize::try_from(i).ok())
        .ok_or_else(|| Error::Protocol("missing output index".into()))
}

fn string<'a>(value: &'a Value, key: &str) -> Result<&'a str, Error> {
    value[key]
        .as_str()
        .ok_or_else(|| Error::Protocol(format!("missing string field {key}")))
}

fn provider_error(value: &Value) -> Error {
    let message = format!(
        "provider error ({}): {}",
        value["code"]
            .as_str()
            .or_else(|| value["type"].as_str())
            .unwrap_or("unknown"),
        value["message"].as_str().unwrap_or("request failed")
    );
    if is_context_overflow(&message) {
        Error::ContextOverflow(message)
    } else {
        Error::Protocol(message)
    }
}

/// Vendor error codes and phrases used for context-window failures.
pub(crate) fn is_context_overflow(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    if ["rate limit", "rate_limit", "too many requests", "throttl"]
        .iter()
        .any(|phrase| message.contains(phrase))
    {
        return false;
    }
    [
        "context_length_exceeded",
        "context length exceeded",
        "maximum context length",
        "exceeds the context window",
        "maximum prompt length",
        "prompt is too long",
        "prompt too long",
        "input is too long",
        "request_too_large",
        "too many tokens",
        "token limit exceeded",
        "exceeded model token limit",
        "reduce the length of the messages",
        "exceeds the available context size",
        "greater than the context length",
        "maximum allowed input length",
    ]
    .iter()
    .any(|phrase| message.contains(phrase))
}

fn item_parts(item: &Value) -> Result<Vec<Part>, Error> {
    Ok(match string(item, "type")? {
        "message" => item["content"]
            .as_array()
            .ok_or_else(|| Error::Protocol("missing message content".into()))?
            .iter()
            .map(|content| {
                let key = if content["type"] == "refusal" {
                    "refusal"
                } else {
                    "text"
                };
                Ok(Part::Text {
                    text: string(content, key)?.to_owned(),
                })
            })
            .collect::<Result<_, Error>>()?,
        "function_call" => vec![Part::ToolCall {
            call_id: ToolCallId(string(item, "call_id")?.to_owned()),
            tool: string(item, "name")?.to_owned(),
            input: serde_json::from_str(string(item, "arguments")?)?,
        }],
        "reasoning" => vec![Part::Reasoning {
            text: item["summary"]
                .as_array()
                .map(|parts| {
                    parts
                        .iter()
                        .filter_map(|part| part["text"].as_str())
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .unwrap_or_default(),
            metadata: BTreeMap::from([("chatgpt".into(), item.clone())]),
        }],
        kind => return Err(Error::Protocol(format!("unsupported output item {kind}"))),
    })
}

fn usage(value: &Value) -> TokenUsage {
    TokenUsage {
        input_tokens: value["input_tokens"].as_u64().unwrap_or(0),
        cached_input_tokens: value["input_tokens_details"]["cached_tokens"]
            .as_u64()
            .unwrap_or(0),
        output_tokens: value["output_tokens"].as_u64().unwrap_or(0),
        reasoning_output_tokens: value["output_tokens_details"]["reasoning_tokens"]
            .as_u64()
            .unwrap_or(0),
        total_tokens: value["total_tokens"].as_u64().unwrap_or(0),
        cache_write_input_tokens: 0,
    }
}
