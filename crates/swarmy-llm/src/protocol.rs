//! Normalize tool-call adjacency for each wire format.
use serde_json::{Value, json};

/// Tool result representation used by the receiving protocol.
#[derive(Clone, Copy)]
pub enum ToolWire {
    Anthropic,
    Completions,
}

/// Repair missing and displaced tool results before submitting a request.
pub fn repair_tool_results(messages: &mut Vec<Value>, wire: ToolWire) {
    match wire {
        ToolWire::Anthropic => repair_anthropic(messages),
        ToolWire::Completions => *messages = repair_completions(std::mem::take(messages)),
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
    let mut call_sites: std::collections::BTreeMap<String, usize> =
        std::collections::BTreeMap::new();
    for (index, message) in messages.iter().enumerate() {
        if let Some(calls) = message.get("tool_calls").and_then(Value::as_array) {
            for call in calls {
                if let Some(id) = call.get("id").and_then(Value::as_str) {
                    call_sites.entry(id.to_owned()).or_insert(index);
                }
            }
        }
    }
    let mut taken = vec![false; messages.len()];
    let mut messages = messages;
    let mut repaired: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut output = Vec::with_capacity(messages.len() * 2);
    for index in 0..messages.len() {
        if taken[index] {
            continue;
        }
        let message = std::mem::take(&mut messages[index]);
        if message.is_null() {
            continue;
        }
        if message.get("role").and_then(Value::as_str) == Some("tool") {
            let id = message
                .get("tool_call_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            if call_sites.contains_key(&id) {
                // The result is pulled forward to its assistant message below,
                // or was already emitted there; never emit it twice.
                continue;
            }
            taken[index] = true;
            output.push(json!({"role": "assistant", "content": Value::Null, "tool_calls": [{"id": id.clone(), "type": "function", "function": {"name": "unknown_tool", "arguments": "{}"}}]}));
            repaired.insert(id);
            output.push(message);
            continue;
        }
        let calls: Vec<String> = message
            .get("tool_calls")
            .and_then(Value::as_array)
            .map(|calls| {
                calls
                    .iter()
                    .filter_map(|call| call.get("id").and_then(Value::as_str).map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();
        output.push(message);
        for id in calls {
            let mut found = None;
            for (candidate, message) in messages.iter().enumerate() {
                if taken[candidate] {
                    continue;
                }
                if message.get("role").and_then(Value::as_str) == Some("tool")
                    && message.get("tool_call_id").and_then(Value::as_str) == Some(&id)
                {
                    found = Some(candidate);
                    break;
                }
            }
            if let Some(candidate) = found {
                taken[candidate] = true;
                output.push(std::mem::take(&mut messages[candidate]));
            } else {
                output.push(json!({
                    "role": "tool", "tool_call_id": id,
                    "content": json!({"error": "No result provided"}).to_string()
                }));
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
