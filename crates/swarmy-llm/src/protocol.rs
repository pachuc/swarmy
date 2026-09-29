//! Normalize tool-call adjacency for each wire format.
use serde_json::{Value, json};

/// Tool result representation used by the receiving protocol.
#[derive(Clone, Copy)]
pub(crate) enum ToolWire {
    Anthropic,
    Completions,
    Responses,
}

/// Repair missing and displaced tool results before submitting a request.
pub(crate) fn repair_tool_results(messages: &mut Vec<Value>, wire: ToolWire) {
    match wire {
        ToolWire::Anthropic => repair_anthropic(messages),
        ToolWire::Completions => *messages = repair_completions(std::mem::take(messages)),
        ToolWire::Responses => *messages = repair_responses(std::mem::take(messages)),
    }
}

fn repair_anthropic(messages: &mut Vec<Value>) {
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

/// Reorder wire messages so every tool result immediately follows the
/// assistant message holding its call.
///
/// A system notice or a late user prompt can sit between a call and its
/// result in the durable log; those messages are emitted after the results
/// with their relative order preserved. A result whose call id appears
/// nowhere in the history gets a neutral placeholder call so switching
/// providers never fails.
fn repair_completions(messages: Vec<Value>) -> Vec<Value> {
    repair_paired(messages, ToolWire::Completions)
}

/// Keep valid wire IDs and hash unsupported ones without collisions.
pub(crate) fn sanitize_tool_id(id: &str, prefix: &str, max_len: usize) -> String {
    if !id.is_empty()
        && id.len() <= max_len
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return id.to_owned();
    }
    let hash = blake3::hash(id.as_bytes()).to_hex();
    format!(
        "{prefix}{}",
        &hash[..max_len.saturating_sub(prefix.len()).min(64)]
    )
}

fn repair_responses(messages: Vec<Value>) -> Vec<Value> {
    repair_paired(messages, ToolWire::Responses)
}

fn call_ids(message: &Value, wire: ToolWire) -> Vec<String> {
    match wire {
        ToolWire::Completions => message["tool_calls"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|call| call["id"].as_str().map(str::to_owned))
            .collect(),
        ToolWire::Responses => (message["type"] == "function_call")
            .then(|| message["call_id"].as_str().map(str::to_owned))
            .flatten()
            .into_iter()
            .collect(),
        ToolWire::Anthropic => Vec::new(),
    }
}

fn result_id(message: &Value, wire: ToolWire) -> Option<&str> {
    match wire {
        ToolWire::Completions if message["role"] == "tool" => message["tool_call_id"].as_str(),
        ToolWire::Responses if message["type"] == "function_call_output" => {
            message["call_id"].as_str()
        }
        _ => None,
    }
}

fn missing_result(id: &str, wire: ToolWire) -> Value {
    match wire {
        ToolWire::Completions => json!({"role": "tool", "tool_call_id": id,
            "content": json!({"error": "No result provided"}).to_string()}),
        ToolWire::Responses => json!({"type": "function_call_output", "call_id": id,
            "output": "Error: No result provided"}),
        ToolWire::Anthropic => unreachable!("Anthropic has its own block adjacency"),
    }
}

fn placeholder_call(id: &str, wire: ToolWire) -> Value {
    match wire {
        ToolWire::Completions => json!({"role": "assistant", "content": Value::Null,
            "tool_calls": [{"id": id, "type": "function",
                "function": {"name": "unknown_tool", "arguments": "{}"}}]}),
        ToolWire::Responses => json!({"type": "function_call", "call_id": id,
            "name": "unknown_tool", "arguments": "{}"}),
        ToolWire::Anthropic => unreachable!("Anthropic has its own block adjacency"),
    }
}

/// The same adjacency pass handles both item-based and message-based wire shapes.
fn repair_paired(mut messages: Vec<Value>, wire: ToolWire) -> Vec<Value> {
    let known: std::collections::BTreeSet<_> = messages
        .iter()
        .flat_map(|message| call_ids(message, wire))
        .collect();
    let mut taken = vec![false; messages.len()];
    let mut repaired = std::collections::BTreeSet::new();
    let mut output = Vec::with_capacity(messages.len() * 2);
    for index in 0..messages.len() {
        if taken[index] {
            continue;
        }
        let message = std::mem::take(&mut messages[index]);
        if message.is_null() {
            continue;
        }
        if let Some(id) = result_id(&message, wire).map(str::to_owned) {
            if known.contains(&id) {
                continue;
            }
            taken[index] = true;
            output.push(placeholder_call(&id, wire));
            repaired.insert(id);
            output.push(message);
            continue;
        }
        let calls = call_ids(&message, wire);
        output.push(message);
        for id in calls {
            if let Some(candidate) = messages.iter().enumerate().find_map(|(candidate, other)| {
                (!taken[candidate] && result_id(other, wire) == Some(&id)).then_some(candidate)
            }) {
                taken[candidate] = true;
                output.push(std::mem::take(&mut messages[candidate]));
            } else {
                output.push(missing_result(&id, wire));
            }
        }
    }
    if !repaired.is_empty() {
        tracing::warn!(
            call_ids = repaired.iter().cloned().collect::<Vec<_>>().join(", "),
            "repaired tool result without a stored tool call; synthesized unknown_tool call"
        );
    }
    output
}

#[cfg(test)]
mod sanitizer_tests {
    use super::sanitize_tool_id;

    #[test]
    fn valid_ids_survive_and_invalid_ids_are_stable_per_wire() {
        let valid = "x".repeat(64);
        assert_eq!(sanitize_tool_id(&valid, "toolu_", 64), valid);
        let invalid = "a.b";
        let anthropic = sanitize_tool_id(invalid, "toolu_", 64);
        assert!(anthropic.starts_with("toolu_"));
        assert_eq!(anthropic.len(), 64);
        assert!(
            anthropic
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        );
        assert_eq!(sanitize_tool_id(invalid, "", 64).len(), 64);
        assert_eq!(anthropic, sanitize_tool_id(invalid, "toolu_", 64));
        assert_ne!(anthropic, sanitize_tool_id("a/b", "toolu_", 64));
    }
}
