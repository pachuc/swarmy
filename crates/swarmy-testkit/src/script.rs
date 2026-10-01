//! Fake-provider script builder for tests.

use std::{collections::BTreeMap, path::Path};

use swarmy_core::{Part, ToolCallId};
use swarmy_llm::{Response, StopReason, TokenUsage};

/// Builds the `script.json` a fake-provider service reads.
///
/// One builder covers every scripted shape: a shared answer text for
/// unscripted turns, tool calls pinned to their turn, per-turn usage for cost
/// assertions, full part overrides for reasoning and mixed text-plus-tool
/// turns, and failures (rate limits or auth errors) that recover after their
/// turns.
#[derive(Default)]
pub struct Script {
    answer: Option<String>,
    latency_ms: u64,
    output_tokens: u64,
    tools: BTreeMap<usize, Vec<Part>>,
    usages: BTreeMap<usize, TokenUsage>,
    overrides: BTreeMap<usize, (Vec<Part>, StopReason)>,
    failures: BTreeMap<usize, serde_json::Value>,
    fail: bool,
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

    /// Answer `turn` with exact parts and stop reason instead of the shared
    /// answer: reasoning turns, text-plus-tool turns, and per-turn texts.
    #[must_use]
    pub fn parts(mut self, turn: usize, parts: Vec<Part>, stop: StopReason) -> Self {
        self.overrides.insert(turn, (parts, stop));
        self
    }

    /// Record token usage for `turn`, for tests asserting on durable costs.
    #[must_use]
    pub fn usage(mut self, turn: usize, input_tokens: u64, output_tokens: u64) -> Self {
        self.usages.insert(
            turn,
            TokenUsage {
                input_tokens,
                output_tokens,
                total_tokens: input_tokens + output_tokens,
                ..TokenUsage::default()
            },
        );
        self
    }

    /// Report `output_tokens` on every generated answer, for tests asserting
    /// on durable costs without per-turn usage maps.
    #[must_use]
    pub fn output_tokens(mut self, output_tokens: u64) -> Self {
        self.output_tokens = output_tokens;
        self
    }

    /// Fail one turn with an HTTP error before recovering.
    #[must_use]
    pub fn failure(
        mut self,
        turn: usize,
        status: u16,
        message: &str,
        retry_after_seconds: Option<u64>,
    ) -> Self {
        self.failures.insert(
            turn,
            serde_json::json!({
                "status": status,
                "message": message,
                "retry_after_seconds": retry_after_seconds,
            }),
        );
        self
    }

    /// Fail the first `failures` turns with a 429 before recovering.
    #[must_use]
    pub fn rate_limited(mut self, failures: usize, retry_after_seconds: u64) -> Self {
        for turn in 0..failures {
            self = self.failure(turn, 429, "quota reached", Some(retry_after_seconds));
        }
        self
    }

    /// Fail every turn with a scripted provider error.
    #[must_use]
    pub fn fail(mut self) -> Self {
        self.fail = true;
        self
    }

    /// Set the fake provider's per-turn latency.
    #[must_use]
    pub fn latency_ms(mut self, latency_ms: u64) -> Self {
        self.latency_ms = latency_ms;
        self
    }

    fn response(&self, turn: usize, parts: Vec<Part>, stop: StopReason) -> Response {
        let usage = self.usages.get(&turn).cloned().unwrap_or(TokenUsage {
            output_tokens: self.output_tokens,
            total_tokens: self.output_tokens,
            ..TokenUsage::default()
        });
        Response {
            parts,
            stop_reason: stop,
            usage,
            quota_remaining: BTreeMap::new(),
            quota_resets: BTreeMap::new(),
        }
    }

    fn answer(&self, turn: usize, text: &str) -> Response {
        self.response(
            turn,
            vec![Part::Text { text: text.into() }],
            StopReason::EndTurn,
        )
    }

    /// The answer response for comparison with stored inference results.
    #[must_use]
    pub fn sample(&self) -> Response {
        self.answer(0, self.answer.as_deref().unwrap_or("done."))
    }

    /// Render the script document the fake provider loads.
    #[must_use]
    pub fn json(&self) -> serde_json::Value {
        let answer = self.answer.as_deref().unwrap_or("done.");
        let mut responses = BTreeMap::new();
        for turn in 0..100 {
            if let Some((parts, stop)) = self.overrides.get(&turn) {
                responses.insert(turn, self.response(turn, parts.clone(), stop.clone()));
            } else if let Some(parts) = self.tools.get(&turn) {
                responses.insert(
                    turn,
                    self.response(turn, parts.clone(), StopReason::ToolCalls),
                );
            } else {
                responses.insert(turn, self.answer(turn, answer));
            }
        }
        serde_json::json!({
            "latency_ms": self.latency_ms,
            "responses": responses,
            "failures": self.failures,
            "fail": self.fail,
        })
    }

    /// Write the script document to `path` (usually `<files>/script.json`).
    ///
    /// # Panics
    /// Panics when the file cannot be written; fixture setup has no recovery.
    pub fn write_to(&self, path: &Path) {
        std::fs::write(
            path,
            serde_json::to_vec(&self.json()).expect("script must serialize"),
        )
        .expect("script file must write");
    }
}
