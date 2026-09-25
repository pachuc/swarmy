//! Portable transcript repair for provider switches.
//!
//! The durable log is provider neutral: every tool call an assistant makes is
//! stored as a `Part::ToolCall` with its provider call id. Old sessions written
//! under the Responses API can contain a tool result whose call is not stored
//! that way, so converters synthesize the missing call instead of failing.

use std::collections::BTreeSet;

use serde_json::json;
use swarmy_core::{Message, MessageId, MessageRole, Part, ToolResult};

use crate::ToolDefinition;

/// Insert a neutral `ToolCall` for every `ToolResult` without a preceding call.
///
/// The tool name comes from the completed result's title when present, which
/// workers set to the tool name, and falls back to the first declared tool.
/// Returns true when at least one call was synthesized so callers log once.
#[must_use]
pub fn synthesize_missing_tool_calls(
    messages: &mut Vec<Message>,
    tools: &[ToolDefinition],
) -> bool {
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for message in messages.iter() {
        for part in &message.parts {
            if let Part::ToolCall { call_id, .. } = part {
                seen.insert(call_id.0.clone());
            }
        }
    }
    let mut repaired = false;
    let mut index = 0;
    while index < messages.len() {
        let mut missing: Vec<(String, String)> = Vec::new();
        let mut seen_here = seen.clone();
        for part in &messages[index].parts {
            match part {
                Part::ToolCall { call_id, .. } => {
                    seen_here.insert(call_id.0.clone());
                }
                Part::ToolResult { call_id, result } if !seen_here.contains(&call_id.0) => {
                    missing.push((call_id.0.clone(), tool_name(result, tools)));
                    seen_here.insert(call_id.0.clone());
                }
                _ => {}
            }
        }
        for (call_id, tool) in missing {
            insert_call(messages, index, call_id, tool);
            repaired = true;
        }
        // Refresh the global set after any insertion before this index.
        seen.clear();
        for message in messages.iter().take(index + 1) {
            for part in &message.parts {
                if let Part::ToolCall { call_id, .. } = part {
                    seen.insert(call_id.0.clone());
                }
            }
        }
        index += 1;
    }
    repaired
}

fn tool_name(result: &ToolResult, tools: &[ToolDefinition]) -> String {
    if let ToolResult::Completed { title, .. } = result
        && !title.is_empty()
    {
        return title.clone();
    }
    tools
        .first()
        .map_or_else(|| "unknown_tool".to_owned(), |tool| tool.name.clone())
}

fn insert_call(messages: &mut Vec<Message>, result_index: usize, call_id: String, tool: String) {
    let call = Part::ToolCall {
        call_id: swarmy_core::ToolCallId(call_id),
        tool,
        input: json!({}),
    };
    if let Some(assistant) = messages[..result_index]
        .iter_mut()
        .rev()
        .find(|message| message.role == MessageRole::Assistant)
    {
        assistant.parts.push(call);
        return;
    }
    messages.insert(
        result_index,
        Message {
            id: MessageId::from_ulid(ulid::Ulid::generate()),
            role: MessageRole::Assistant,
            parts: vec![call],
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use swarmy_core::ToolCallId;

    fn assistant(parts: Vec<Part>) -> Message {
        Message {
            id: MessageId::from_ulid(ulid::Ulid::nil()),
            role: MessageRole::Assistant,
            parts,
        }
    }

    fn tool_message(call_id: &str, title: &str) -> Message {
        Message {
            id: MessageId::from_ulid(ulid::Ulid::nil()),
            role: MessageRole::Tool,
            parts: vec![Part::ToolResult {
                call_id: ToolCallId(call_id.into()),
                result: ToolResult::Completed {
                    output: "ok".into(),
                    title: title.into(),
                    metadata: std::collections::BTreeMap::new(),
                },
            }],
        }
    }

    #[test]
    fn orphan_result_gains_a_neutral_call() {
        let mut messages = vec![
            assistant(vec![Part::Text {
                text: "Checking.".into(),
            }]),
            tool_message("call_1", "get_time"),
        ];
        let tools = vec![ToolDefinition {
            name: "get_time".into(),
            description: String::new(),
            parameters: json!({}),
        }];
        assert!(synthesize_missing_tool_calls(&mut messages, &tools));
        assert!(matches!(
            &messages[0].parts[1],
            Part::ToolCall { tool, .. } if tool == "get_time"
        ));
        assert!(!synthesize_missing_tool_calls(&mut messages, &tools));
    }
}
