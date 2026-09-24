use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

use crate::MessageId;

/// A provider's opaque tool call id, preserved exactly for result correlation.
/// Unlike swarmy's own identities, provider call ids are not necessarily ULIDs.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ToolCallId(pub String);

/// Ordered parts form the content of one message in the session log.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub id: MessageId,
    pub role: MessageRole,
    pub parts: Vec<Part>,
}

/// Variant order is part of storage version 1; append new roles at the end.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    System,
    User,
    Assistant,
    Tool,
}

/// The text, reasoning, input, output, and metadata fields follow
/// [OpenCode's message parts](https://github.com/anomalyco/opencode/blob/dev/packages/opencode/src/session/message-v2.ts).
/// Calls and results are separate immutable parts here because
/// they arrive in separate steps, rather than updates to one mutable tool part.
/// Provider reasoning metadata is retained so signatures survive prompt replay.
///
/// Serde uses external tags; append variants to preserve postcard discriminants.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Part {
    Text {
        text: String,
    },
    ToolCall {
        call_id: ToolCallId,
        tool: String,
        #[serde(with = "crate::encoding::json")]
        input: Value,
    },
    ToolResult {
        call_id: ToolCallId,
        result: ToolResult,
    },
    Reasoning {
        text: String,
        #[serde(with = "crate::encoding::json")]
        metadata: BTreeMap<String, Value>,
    },
    /// Image bytes are kept outside the log when `object_key` is set.
    Image {
        media_type: String,
        bytes: Vec<u8>,
        object_key: Option<String>,
        detail: Option<String>,
    },
}

/// Tool success and failure remain distinct when replayed into a model prompt.
/// Variant order is part of storage version 1; append new variants at the end.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolResult {
    Completed {
        output: String,
        title: String,
        #[serde(with = "crate::encoding::json")]
        metadata: BTreeMap<String, Value>,
    },
    Error {
        error: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCallRecord {
    pub call_id: ToolCallId,
    pub tool: String,
    #[serde(with = "crate::encoding::json")]
    pub arguments: Value,
    pub result: Option<ToolResult>,
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::encoding::tests::assert_round_trip;
    use serde_json::json;
    use ulid::Ulid;

    pub(crate) fn tool_result() -> ToolResult {
        ToolResult::Completed {
            output: "hello\nworld 🌍".into(),
            title: "Read file".into(),
            metadata: BTreeMap::from([("lines".into(), json!(2))]),
        }
    }

    pub(crate) fn tool_call() -> ToolCallRecord {
        ToolCallRecord {
            call_id: ToolCallId("call_provider_123".into()),
            tool: "read".into(),
            arguments: json!({"path": "src/main.rs", "options": [null, true, -7, 1.5, {"limit": 42}]}),
            result: None,
        }
    }

    pub(crate) fn parts() -> Vec<Part> {
        vec![
            Part::Text {
                text: "Hello\n世界".into(),
            },
            Part::ToolCall {
                call_id: ToolCallId("call_provider_123".into()),
                tool: "read".into(),
                input: tool_call().arguments,
            },
            Part::ToolResult {
                call_id: ToolCallId("call_provider_123".into()),
                result: tool_result(),
            },
            Part::ToolResult {
                call_id: ToolCallId("call_failed".into()),
                result: ToolResult::Error {
                    error: "File not found".into(),
                },
            },
            Part::Reasoning {
                text: "I should inspect the file.".into(),
                metadata: BTreeMap::from([(
                    "provider".into(),
                    json!({"signature": "opaque", "nested": [null, false]}),
                )]),
            },
        ]
    }

    pub(crate) fn message() -> Message {
        Message {
            id: MessageId::from_ulid(Ulid::from_parts(1, 5)),
            role: MessageRole::Assistant,
            parts: parts(),
        }
    }

    #[test]
    fn every_part_and_message_role_round_trips() {
        for part in parts() {
            assert_round_trip(&part);
        }
        for role in [
            MessageRole::System,
            MessageRole::User,
            MessageRole::Assistant,
            MessageRole::Tool,
        ] {
            assert_round_trip(&Message { role, ..message() });
            assert_round_trip(&Message {
                role,
                parts: Vec::new(),
                ..message()
            });
        }
        assert_round_trip(&Part::Image {
            media_type: "image/png".into(),
            bytes: vec![0, 1, 2],
            object_key: None,
            detail: Some("low".into()),
        });
        assert_round_trip(&Part::Reasoning {
            text: String::new(),
            metadata: BTreeMap::new(),
        });
    }

    #[test]
    fn tool_records_preserve_optional_results_and_arbitrary_arguments() {
        let mut call = tool_call();
        for result in [
            None,
            Some(tool_result()),
            Some(ToolResult::Error {
                error: "Permission denied".into(),
            }),
        ] {
            call.result = result;
            assert_round_trip(&call);
        }
        for arguments in [
            Value::Null,
            json!(true),
            json!("hello"),
            json!([]),
            json!({}),
            json!(i64::MIN),
            json!(u64::MAX),
            json!(1.25),
            json!(f64::MIN_POSITIVE),
            json!(f64::MAX),
            json!(1.234_567_890_123_456_7),
        ] {
            call.arguments = arguments;
            assert_round_trip(&call);
        }
    }

    #[test]
    fn json_arguments_remain_structured_and_parts_have_tags() {
        let call = tool_call();
        let json = serde_json::to_value(&call).unwrap();
        assert_eq!(json["arguments"], call.arguments);
        assert_eq!(
            serde_json::to_value(Part::Text {
                text: "hello".into()
            })
            .unwrap(),
            json!({"text": {"text": "hello"}})
        );
    }
}
