//! Fake-provider script builder replacing hand-built response literals.

use std::{collections::BTreeMap, path::Path};

use swarmy_core::{Part, ToolCallId};
use swarmy_llm::{Response, StopReason, TokenUsage};

/// Builds the `script.json` a fake-provider service reads.
///
/// The e2e suites each hand-built `Response { .. }` literals with empty quota
/// maps and a 0..100 turn fan-out. One builder keeps the shape: a shared
/// answer text for unscripted turns, tool calls pinned to their turn, and
/// rate-limit failures that recover after `failures` turns.
#[derive(Default)]
pub struct Script {
    answer: Option<String>,
    latency_ms: u64,
    tools: BTreeMap<usize, Vec<Part>>,
    texts: BTreeMap<usize, String>,
    failures: usize,
    retry_after_seconds: u64,
}

impl Script {
    /// Start a script answering every unscripted turn with `text`.
    #[must_use]
    pub fn new(text: &str) -> Self {
        Self {
            answer: Some(text.to_owned()),
            ..Default::default()
        }
    }

    /// Answer one turn with literal text instead of the shared answer.
    #[must_use]
    pub fn text(mut self, turn: usize, text: &str) -> Self {
        self.texts.insert(turn, text.to_owned());
        self
    }

    /// Make `turn` issue a tool call instead of answering text.
    #[must_use]
    pub fn tool_call(mut self, turn: usize, call_id: &str, tool: &str) -> Self {
        self.tools.entry(turn).or_default().push(Part::ToolCall {
            call_id: ToolCallId(call_id.into()),
            tool: tool.into(),
            input: serde_json::json!({}),
        });
        self
    }

    /// Fail the first `failures` turns with a 429 before recovering.
    #[must_use]
    pub fn rate_limited(mut self, failures: usize, retry_after_seconds: u64) -> Self {
        self.failures = failures;
        self.retry_after_seconds = retry_after_seconds;
        self
    }

    /// Set the fake provider's per-turn latency.
    #[must_use]
    pub fn latency_ms(mut self, latency_ms: u64) -> Self {
        self.latency_ms = latency_ms;
        self
    }

    fn response(parts: Vec<Part>, stop: StopReason) -> Response {
        Response {
            parts,
            stop_reason: stop,
            usage: TokenUsage::default(),
            quota_remaining: BTreeMap::new(),
            quota_resets: BTreeMap::new(),
        }
    }

    fn answer(text: &str) -> Response {
        Self::response(vec![Part::Text { text: text.into() }], StopReason::EndTurn)
    }

    /// Render the script document the fake provider loads.
    #[must_use]
    pub fn json(&self) -> serde_json::Value {
        let answer = self.answer.as_deref().unwrap_or("done.");
        let mut responses = BTreeMap::new();
        for turn in self.failures..100 {
            if let Some(parts) = self.tools.get(&turn) {
                responses.insert(turn, Self::response(parts.clone(), StopReason::ToolCalls));
            } else if let Some(text) = self.texts.get(&turn) {
                responses.insert(turn, Self::answer(text));
            } else {
                responses.insert(turn, Self::answer(answer));
            }
        }
        // Tool calls below the failure window still need their scripted turn;
        // the provider reads failures first, so they surface after recovery.
        for (turn, parts) in &self.tools {
            if *turn < self.failures {
                responses.insert(
                    *turn + self.failures,
                    Self::response(parts.clone(), StopReason::ToolCalls),
                );
            }
        }
        let failures: BTreeMap<_, _> = (0..self.failures)
            .map(|turn| {
                (
                    turn,
                    serde_json::json!({
                        "status": 429,
                        "message": "quota reached",
                        "retry_after_seconds": self.retry_after_seconds,
                    }),
                )
            })
            .collect();
        serde_json::json!({
            "latency_ms": self.latency_ms,
            "responses": responses,
            "failures": failures,
        })
    }

    /// Write the script document to `path` (usually `<files>/script.json`).
    ///
    /// # Panics
    /// Panics when the file cannot be written; fixture setup has no recovery.
    pub fn write_to(&self, path: &Path) {
        std::fs::write(path, serde_json::to_vec(&self.json()).unwrap()).unwrap();
    }
}
