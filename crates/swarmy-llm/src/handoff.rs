//! Neutral replay for a named session whose route changes its model.
//! This replaces the reasoning-as-text replay in `reasoning` on a model handoff.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use swarmy_core::{Message, MessageRole, Part, ToolResult};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reasoning {
    Keep,
    Drop,
}

pub const TOOL_RESULT_KEEP_CHARS: usize = 1_500;
pub const TOOL_ARGUMENTS_KEEP_CHARS: usize = 500;
pub const TRIMMED_HISTORY_MARKER: &str =
    "[Older history was trimmed to fit the new model's context window.]";

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelHandoff {
    pub from_provider: String,
    pub from_model: String,
    pub to_provider: String,
    pub to_model: String,
    pub reason: String,
}

fn shorten(text: &str, keep: usize, label: &str, separator: &str) -> String {
    let count = text.chars().count();
    if count <= keep * 2 {
        return text.to_owned();
    }
    let start: String = text.chars().take(keep).collect();
    let end: String = text.chars().skip(count - keep).collect();
    format!(
        "{start}{separator}[... {} characters cut from {label} ...]{separator}{end}",
        count - keep * 2
    )
}

fn render_user(message: &Message, role: MessageRole, blocks: &mut Vec<String>) {
    let label = match role {
        MessageRole::User => "User",
        _ => "System",
    };
    let pieces: Vec<&str> = message
        .parts
        .iter()
        .filter_map(|part| match part {
            Part::Text { text } | Part::Notice { text, .. } => Some(text.as_str()),
            Part::Image { .. } if role == MessageRole::User => Some("[image omitted]"),
            _ => None,
        })
        .collect();
    if !pieces.is_empty() {
        blocks.push(format!("[{label}]: {}", pieces.join("\n")));
    }
}

fn render_assistant<'a>(
    message: &'a Message,
    blocks: &mut Vec<String>,
    calls: &mut BTreeMap<&'a str, &'a str>,
    reasoning: Reasoning,
) {
    if reasoning == Reasoning::Keep {
        let thinking: Vec<&str> = message
            .parts
            .iter()
            .filter_map(|part| match part {
                Part::Reasoning { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        if !thinking.is_empty() {
            blocks.push(format!("[Assistant thinking]: {}", thinking.join("\n")));
        }
    }
    let texts: Vec<&str> = message
        .parts
        .iter()
        .filter_map(|part| match part {
            Part::Text { text } if !text.is_empty() => Some(text.as_str()),
            _ => None,
        })
        .collect();
    if !texts.is_empty() {
        blocks.push(format!("[Assistant]: {}", texts.join("\n")));
    }
    for part in &message.parts {
        if let Part::ToolCall {
            call_id,
            tool,
            input,
        } = part
        {
            calls.insert(&call_id.0, tool);
            let args = serde_json::to_string(input).expect("serializing a JSON value cannot fail");
            blocks.push(format!(
                "[Tool call] {tool} {}",
                shorten(&args, TOOL_ARGUMENTS_KEEP_CHARS, "these arguments", " ")
            ));
        }
    }
}

fn render_tool(message: &Message, blocks: &mut Vec<String>, calls: &BTreeMap<&str, &str>) {
    for part in &message.parts {
        if let Part::ToolResult { call_id, result } = part {
            let tool = calls
                .get(call_id.0.as_str())
                .copied()
                .unwrap_or("unknown_tool");
            match result {
                ToolResult::Completed {
                    output, metadata, ..
                } => {
                    let mut text = if output.is_empty() {
                        "(empty)".to_owned()
                    } else {
                        shorten(output, TOOL_RESULT_KEEP_CHARS, "this tool result", "\n")
                    };
                    if metadata.contains_key("image_object_key") {
                        text.push_str("\n[image omitted]");
                    }
                    let delimiter = if output.is_empty() { " " } else { "\n" };
                    blocks.push(format!("[Tool result] {tool}:{delimiter}{text}"));
                }
                ToolResult::Error { error } => blocks.push(format!(
                    "[Tool error] {tool}:\n{}",
                    shorten(error, TOOL_RESULT_KEEP_CHARS, "this tool result", "\n")
                )),
            }
        }
    }
}

/// Pi packages/coding-agent/src/core/compaction/utils.ts@8eb2bcc:109-149: serialize old context as data, never as live chat turns.
#[must_use]
pub fn render_transcript(messages: &[Message], reasoning: Reasoning) -> String {
    let mut blocks = Vec::new();
    let mut calls = BTreeMap::new();
    for message in messages {
        match message.role {
            MessageRole::User => render_user(message, MessageRole::User, &mut blocks),
            MessageRole::System => render_user(message, MessageRole::System, &mut blocks),
            MessageRole::Assistant => render_assistant(message, &mut blocks, &mut calls, reasoning),
            MessageRole::Tool => render_tool(message, &mut blocks, &calls),
        }
    }
    blocks.join("\n\n")
}

#[must_use]
pub fn keep_newest(rendered: &str, max_chars: usize) -> String {
    let count = rendered.chars().count();
    if count <= max_chars {
        return rendered.to_owned();
    }
    let available = max_chars.saturating_sub(TRIMMED_HISTORY_MARKER.chars().count() + 2);
    let suffix: String = rendered
        .chars()
        .skip(count.saturating_sub(available))
        .collect();
    let suffix = suffix
        .find("\n\n")
        .and_then(|pos| suffix.get(pos + 2..))
        .filter(|rest| !rest.is_empty())
        .unwrap_or(&suffix)
        .trim_start_matches('\n');
    format!("{TRIMMED_HISTORY_MARKER}\n\n{suffix}")
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    fn message(role: MessageRole, parts: Vec<Part>) -> Message {
        Message {
            id: swarmy_core::MessageId::from_ulid(ulid::Ulid::generate()),
            role,
            parts,
        }
    }
    #[test]
    fn renders_a_known_history_exactly() {
        let history = vec![
            message(
                MessageRole::User,
                vec![Part::Text {
                    text: "Fix the bug".into(),
                }],
            ),
            message(
                MessageRole::Assistant,
                vec![
                    Part::Reasoning {
                        text: "**Testing the timeouts**".into(),
                        metadata: BTreeMap::from([(
                            "openai_responses".into(),
                            serde_json::json!({"provider":"fake-a","model":"fake-model","item":{"type":"reasoning"}}),
                        )]),
                    },
                    Part::Text {
                        text: "Looking.".into(),
                    },
                    Part::ToolCall {
                        call_id: swarmy_core::ToolCallId("c1".into()),
                        tool: "read".into(),
                        input: serde_json::json!({"path":"src/lib.rs"}),
                    },
                ],
            ),
            message(
                MessageRole::Tool,
                vec![Part::ToolResult {
                    call_id: swarmy_core::ToolCallId("c1".into()),
                    result: ToolResult::Completed {
                        output: "fn main() {}".into(),
                        title: String::new(),
                        metadata: BTreeMap::new(),
                    },
                }],
            ),
            message(
                MessageRole::Assistant,
                vec![Part::Text {
                    text: "Done.".into(),
                }],
            ),
        ];
        let rendered = render_transcript(&history, Reasoning::Drop);
        assert_eq!(
            rendered,
            "[User]: Fix the bug\n\n[Assistant]: Looking.\n\n[Tool call] read {\"path\":\"src/lib.rs\"}\n\n[Tool result] read:\nfn main() {}\n\n[Assistant]: Done."
        );
        assert_eq!(render_transcript(&history, Reasoning::Drop), rendered);
        assert!(!rendered.contains("Testing the timeouts"));
    }
    #[test]
    fn trims_long_tool_results_keeping_start_and_end() {
        let output = format!(
            "{}{}{}",
            "a".repeat(2000),
            "b".repeat(1000),
            "c".repeat(2000)
        );
        let rendered = render_transcript(
            &[message(
                MessageRole::Tool,
                vec![Part::ToolResult {
                    call_id: swarmy_core::ToolCallId("x".into()),
                    result: ToolResult::Completed {
                        output,
                        title: String::new(),
                        metadata: BTreeMap::new(),
                    },
                }],
            )],
            Reasoning::Drop,
        );
        assert!(rendered.contains(&format!(
            "{}\n[... 2000 characters cut from this tool result ...]\n{}",
            "a".repeat(1500),
            "c".repeat(1500)
        )));
        assert!(!rendered.contains('b'));
    }
    #[test]
    fn trims_long_tool_arguments_on_one_line() {
        let rendered = render_transcript(
            &[message(
                MessageRole::Assistant,
                vec![Part::ToolCall {
                    call_id: swarmy_core::ToolCallId("x".into()),
                    tool: "read".into(),
                    input: serde_json::json!({"data":"x".repeat(2000)}),
                }],
            )],
            Reasoning::Drop,
        );
        assert_eq!(rendered.lines().count(), 1);
        assert!(rendered.contains("characters cut from these arguments"));
    }
    #[test]
    fn errors_and_unknown_calls_are_named() {
        let history = [
            message(
                MessageRole::Assistant,
                vec![Part::ToolCall {
                    call_id: swarmy_core::ToolCallId("x".into()),
                    tool: "read".into(),
                    input: serde_json::json!({}),
                }],
            ),
            message(
                MessageRole::Tool,
                vec![
                    Part::ToolResult {
                        call_id: swarmy_core::ToolCallId("x".into()),
                        result: ToolResult::Error {
                            error: "bad".into(),
                        },
                    },
                    Part::ToolResult {
                        call_id: swarmy_core::ToolCallId("y".into()),
                        result: ToolResult::Completed {
                            output: "ok".into(),
                            title: String::new(),
                            metadata: BTreeMap::new(),
                        },
                    },
                ],
            ),
        ];
        let rendered = render_transcript(&history, Reasoning::Drop);
        assert!(rendered.contains("[Tool error] read:\nbad"));
        assert!(rendered.contains("[Tool result] unknown_tool:\nok"));
    }
    #[test]
    fn tool_result_image_is_marked() {
        let history = [message(
            MessageRole::Tool,
            vec![Part::ToolResult {
                call_id: swarmy_core::ToolCallId("screenshot".into()),
                result: ToolResult::Completed {
                    output: "caption".into(),
                    title: String::new(),
                    metadata: BTreeMap::from([(
                        "image_object_key".into(),
                        serde_json::json!("image"),
                    )]),
                },
            }],
        )];
        assert_eq!(
            render_transcript(&history, Reasoning::Drop),
            "[Tool result] unknown_tool:\ncaption\n[image omitted]"
        );
    }
    #[test]
    fn keep_newest_trims_at_a_block_boundary_and_says_so() {
        let text = ["a", "b", "c", "d", "e"]
            .map(|c| c.repeat(100))
            .join("\n\n");
        assert_eq!(keep_newest(&text, 600), text);
        let result = keep_newest(&text, 220);
        assert!(result.starts_with(TRIMMED_HISTORY_MARKER));
        assert!(result.ends_with(&"e".repeat(100)));
        assert!(!result.split_once("\n\n").unwrap().1.contains('d'));
        assert!(result.chars().count() <= 220);
        assert!(!result.contains("\n\n\n"));
    }
    #[test]
    fn multibyte_output_is_cut_on_char_boundaries() {
        let history = [message(
            MessageRole::Tool,
            vec![Part::ToolResult {
                call_id: swarmy_core::ToolCallId("x".into()),
                result: ToolResult::Completed {
                    output: "é".repeat(4000),
                    title: String::new(),
                    metadata: BTreeMap::new(),
                },
            }],
        )];
        let result = render_transcript(&history, Reasoning::Drop);
        assert_eq!(result.matches('é').count(), 3000);
    }
}
