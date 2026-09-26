//! Reasoning replay across providers.
//!
//! Reasoning blocks carry the provider and model that produced them, and the
//! wire builders replay them only for that same pair. This module applies the
//! same rule to outgoing request messages: when a turn fails over to another
//! provider or model override, reasoning from the previous step is downgraded
//! to plain text (or dropped when empty) instead of travelling as reasoning.
//! Downgrading matches exactly what each wire builder would have sent, so the
//! provider sees identical input; the stored request simply no longer claims
//! a replayable signature it cannot honor.
use std::collections::BTreeMap;

use serde_json::Value;
use swarmy_core::{Message, MessageRole, Part};

/// Downgrade reasoning blocks whose provenance does not match the current
/// provider and model. Parts without recognized provenance are left alone;
/// the wire builders downgrade those themselves.
pub fn downgrade_mismatched_reasoning(messages: &mut [Message], provider: &str, model: &str) {
    for message in messages {
        let mut parts = Vec::with_capacity(message.parts.len());
        for part in message.parts.drain(..) {
            parts.extend(downgrade_part(message.role, part, provider, model));
        }
        message.parts = parts;
    }
}

fn downgrade_part(role: MessageRole, part: Part, provider: &str, model: &str) -> Option<Part> {
    let Part::Reasoning { text, metadata } = part else {
        return Some(part);
    };
    if replayable(role, &metadata, provider, model) {
        return Some(Part::Reasoning { text, metadata });
    }
    if has_provenance(&metadata) {
        // The wire builders send this text without its signature; an empty
        // block sends nothing at all.
        if text.is_empty() {
            return None;
        }
        return Some(Part::Text { text });
    }
    Some(Part::Reasoning { text, metadata })
}

/// Whether any wire builder would replay this block for the current pair.
fn replayable(
    role: MessageRole,
    metadata: &BTreeMap<String, Value>,
    provider: &str,
    model: &str,
) -> bool {
    responses_replayable(metadata, provider, model)
        || anthropic_replayable(metadata, provider, model)
        || bedrock_replayable(metadata, model)
        || gemini_replayable(metadata, provider, model)
        || completions_replayable(role, metadata, provider, model)
}

/// Whether the metadata names a provider that records replay provenance.
/// Parts without any of these keys keep their shape here; each wire builder
/// decides their fate when it serializes the request.
fn has_provenance(metadata: &BTreeMap<String, Value>) -> bool {
    metadata.contains_key("openai_responses")
        || metadata.contains_key("chatgpt")
        || metadata.contains_key("anthropic")
        || metadata.contains_key("bedrock")
        || metadata.contains_key("google")
        || metadata.contains_key("openrouter")
}

fn responses_replayable(metadata: &BTreeMap<String, Value>, provider: &str, model: &str) -> bool {
    let saved = metadata
        .get("openai_responses")
        .or_else(|| metadata.get("chatgpt"));
    saved.is_some_and(|saved| {
        let item = saved.get("item").unwrap_or(saved);
        saved["provider"] == provider && saved["model"] == model && item["type"] == "reasoning"
    })
}

fn anthropic_replayable(metadata: &BTreeMap<String, Value>, provider: &str, model: &str) -> bool {
    metadata.get("anthropic").is_some_and(|value| {
        value["model"] == model
            && value["provider"] == provider
            && value["signature"]
                .as_str()
                .is_some_and(|signature| !signature.is_empty())
    })
}

fn bedrock_replayable(metadata: &BTreeMap<String, Value>, model: &str) -> bool {
    metadata.get("bedrock").is_some_and(|data| {
        data["model"].as_str() == Some(model)
            && (data["redacted_content"].as_str().is_some()
                || data["signature"]
                    .as_str()
                    .is_some_and(|signature| !signature.is_empty()))
    })
}

fn gemini_replayable(metadata: &BTreeMap<String, Value>, provider: &str, model: &str) -> bool {
    metadata.get("google").is_some_and(|meta| {
        meta["provider"] == provider && meta["model"] == model && meta.get("target_index").is_none()
    }) || metadata
        .get("google")
        .is_some_and(|meta| meta.get("target_index").is_some())
}

fn completions_replayable(
    role: MessageRole,
    metadata: &BTreeMap<String, Value>,
    provider: &str,
    model: &str,
) -> bool {
    metadata
        .get("openrouter")
        .and_then(Value::as_array)
        .is_some_and(|details| {
            !details.is_empty()
                && role == MessageRole::Assistant
                && metadata.get("model") == Some(&Value::String(model.to_owned()))
                && metadata.get("provider") == Some(&Value::String(provider.to_owned()))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reasoning(text: &str, metadata: BTreeMap<String, Value>) -> Part {
        Part::Reasoning {
            text: text.into(),
            metadata,
        }
    }

    fn messages(parts: Vec<Part>) -> Vec<Message> {
        vec![Message {
            id: swarmy_core::MessageId::from_ulid(ulid::Ulid::nil()),
            role: MessageRole::Assistant,
            parts,
        }]
    }

    #[test]
    fn keeps_matching_replay_and_downgrades_the_rest() {
        let matching = reasoning(
            "Think first.",
            BTreeMap::from([(
                "openai_responses".into(),
                serde_json::json!({"provider": "openai", "model": "gpt-5.5", "item": {"type": "reasoning"}}),
            )]),
        );
        let foreign = reasoning(
            "Think first.",
            BTreeMap::from([(
                "anthropic".into(),
                serde_json::json!({"provider": "anthropic", "model": "claude", "signature": "sig"}),
            )]),
        );
        let empty_foreign = reasoning(
            "",
            BTreeMap::from([(
                "bedrock".into(),
                serde_json::json!({"model": "other", "signature": "sig"}),
            )]),
        );
        let bare = reasoning("", BTreeMap::new());
        let mut log = messages(vec![matching, foreign, empty_foreign, bare]);
        downgrade_mismatched_reasoning(&mut log, "openai", "gpt-5.5");
        assert!(matches!(&log[0].parts[0], Part::Reasoning { .. }));
        assert!(matches!(&log[0].parts[1], Part::Text { text } if text == "Think first."));
        assert_eq!(log[0].parts.len(), 3);
        assert!(matches!(&log[0].parts[2], Part::Reasoning { .. }));
    }
}
