//! The Responses wire adapter. No platform API keys or gateway dependencies.
use crate::{Delta, Error, Request, Response, StopReason, TokenUsage};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use swarmy_core::{MessageRole, Part, ToolCallId, ToolResult};

/// Convert core parts into ordered Responses input items.
/// # Errors
/// Rejects text with a tool role, which requires a correlated tool result,
/// and reasoning without the provider metadata needed for replay.
pub fn request_json(request: &Request) -> Result<Value, Error> {
    let mut input = Vec::new();
    for message in &request.messages {
        for part in &message.parts {
            input.push(match part {
                Part::Text { text } => {
                    let (role, kind) = match message.role {
                        MessageRole::System => ("system", "input_text"),
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
                Part::Reasoning { metadata, .. } => metadata.get("chatgpt").filter(|v| v["type"] == "reasoning").cloned()
                    .ok_or_else(|| Error::Protocol("reasoning replay requires ChatGPT metadata".into()))?,
            });
        }
    }
    let tools: Vec<_> = request
        .tools
        .iter()
        .map(|tool| {
            json!({
                "type": "function", "name": tool.name, "description": tool.description,
                "parameters": tool.parameters, "strict": false,
            })
        })
        .collect();
    let mut value = json!({
        "model": request.settings.model, "instructions": request.system_prompt, "input": input,
        "tools": tools, "tool_choice": "auto", "parallel_tool_calls": true,
        "stream": true, "store": false, "include": ["reasoning.encrypted_content"],
    });
    if let Some(effort) = request.settings.reasoning_effort {
        value["reasoning"] = json!({"effort": effort, "summary": "auto"});
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

/// Incremental SSE parser, including CRLF, multiline data, comments, and UTF-8
/// split across arbitrary network chunks. Unknown event types are ignored.
#[derive(Default)]
pub struct SseParser {
    line: Vec<u8>,
    data: Vec<u8>,
    previous_cr: bool,
    output: BTreeMap<usize, Vec<Part>>,
    completed: bool,
}

impl SseParser {
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

    fn event(&mut self, event: &Value, deltas: &mut Vec<Delta>) -> Result<(), Error> {
        match string(event, "type")? {
            "response.output_text.delta" | "response.refusal.delta" => deltas.push(Delta::Text {
                output_index: index(event)?,
                text: string(event, "delta")?.to_owned(),
            }),
            "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => deltas
                .push(Delta::Reasoning {
                    output_index: index(event)?,
                    text: string(event, "delta")?.to_owned(),
                }),
            "response.function_call_arguments.delta" => deltas.push(Delta::ToolArguments {
                output_index: index(event)?,
                arguments: string(event, "delta")?.to_owned(),
            }),
            "response.output_item.done" => {
                let output_index = index(event)?;
                let parts = item_parts(&event["item"])?;
                for part in &parts {
                    deltas.push(Delta::PartDone {
                        output_index,
                        part: part.clone(),
                    });
                }
                self.output.insert(output_index, parts);
            }
            "response.completed" | "response.incomplete" => {
                let response = &event["response"];
                if !response.is_object() {
                    return Err(Error::Protocol("missing completed response".into()));
                }
                if response["status"] == "failed" {
                    return Err(provider_error(&response["error"]));
                }
                let parts: Vec<Part> = if let Some(output) = response["output"].as_array() {
                    output
                        .iter()
                        .map(item_parts)
                        .collect::<Result<Vec<_>, _>>()?
                        .into_iter()
                        .flatten()
                        .collect()
                } else {
                    std::mem::take(&mut self.output)
                        .into_values()
                        .flatten()
                        .collect()
                };
                let stop_reason = if event["type"] == "response.incomplete"
                    || response["status"] == "incomplete"
                {
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
                };
                deltas.push(Delta::Completed(Response {
                    parts,
                    stop_reason,
                    usage: usage(&response["usage"]),
                }));
                self.completed = true;
            }
            "response.failed" => return Err(provider_error(&event["response"]["error"])),
            "error" => return Err(provider_error(event.get("error").unwrap_or(event))),
            _ => (),
        }
        Ok(())
    }
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
    Error::Protocol(format!(
        "provider error ({}): {}",
        value["code"].as_str().unwrap_or("unknown"),
        value["message"].as_str().unwrap_or("request failed")
    ))
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
    }
}
