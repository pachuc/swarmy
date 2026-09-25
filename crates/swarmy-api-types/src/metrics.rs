//! Bounded, incrementally assembled measurements for one durable turn.
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct StageTiming {
    pub stage: String,
    pub request_id: Option<String>,
    pub clock_id: String,
    pub monotonic_ns: u64,
    pub unix_ns: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct InferenceMetric {
    pub request_id: String,
    pub provider: String,
    pub model: String,
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_tokens: u64,
    pub cost_micros: u64,
    pub time_to_first_token_ms: Option<f64>,
    pub streaming_duration_ms: Option<f64>,
    pub output_tokens_per_second: Option<f64>,
    pub retries: u32,
    pub rate_limit_waits: u32,
    pub gateway_waits: u32,
    pub provider_failures: u32,
    pub error: Option<String>,
}

impl InferenceMetric {
    /// A zero interval cannot yield a meaningful throughput.
    #[must_use]
    // Throughput is an approximate rate; sub-token precision is not meaningful.
    #[allow(clippy::cast_precision_loss)]
    pub fn tokens_per_second(tokens: u64, duration_ms: f64) -> Option<f64> {
        (tokens > 0 && duration_ms > 0.0 && duration_ms.is_finite())
            .then_some(tokens as f64 * 1_000.0 / duration_ms)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ToolMetric {
    pub request_id: String,
    pub name: String,
    pub dispatched_ns: Option<i64>,
    pub started_ns: Option<i64>,
    pub completed_ns: Option<i64>,
    pub exit_status: Option<i32>,
    pub output_bytes: Option<u64>,
    pub queue_ms: Option<f64>,
    pub process_wall_ms: Option<f64>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ComputerMetric {
    pub placement_ms: Option<f64>,
    pub cold: Option<bool>,
    pub chunks_fetched: u64,
    pub bytes_fetched: u64,
    pub fetch_p50_ms: Option<f64>,
    pub fetch_p95_ms: Option<f64>,
    /// Volume counters re-sampled after the first tool call of the turn
    /// completes. The boot sample above is taken before the first command
    /// runs, so lazy chunk hydration during that command is invisible in it.
    #[serde(default)]
    pub first_tool_chunks_fetched: Option<u64>,
    /// Total bytes re-sampled after the first tool call completes.
    #[serde(default)]
    pub first_tool_bytes_fetched: Option<u64>,
    /// Fetch p50 re-sampled after the first tool call completes.
    #[serde(default)]
    pub first_tool_fetch_p50_ms: Option<f64>,
    /// Fetch p95 re-sampled after the first tool call completes.
    #[serde(default)]
    pub first_tool_fetch_p95_ms: Option<f64>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct TurnMetrics {
    pub session_id: String,
    pub turn_id: String,
    pub stages: Vec<StageTiming>,
    pub inference: Vec<InferenceMetric>,
    pub tools: Vec<ToolMetric>,
    pub computer: Option<ComputerMetric>,
    pub append_to_first_token_ms: Option<f64>,
    pub inference_duration_ms: Option<f64>,
    pub append_to_idle_ms: Option<f64>,
    pub error: Option<String>,
    /// Stages dropped when the 64-entry cap is hit.
    #[serde(default)]
    pub dropped_stages: u64,
    /// Model requests dropped when the 16-entry cap is hit.
    #[serde(default)]
    pub dropped_inference: u64,
    /// Tool calls dropped when the 64-entry cap is hit.
    #[serde(default)]
    pub dropped_tools: u64,
}

impl TurnMetrics {
    /// Compare monotonic times only within a boot; otherwise use wall time.
    #[must_use]
    // Millisecond display precision is lower than the nanosecond source precision.
    #[allow(clippy::cast_precision_loss)]
    pub fn duration_ms(first: &StageTiming, last: &StageTiming) -> Option<f64> {
        if first.clock_id == last.clock_id {
            last.monotonic_ns
                .checked_sub(first.monotonic_ns)
                .map(|ns| ns as f64 / 1_000_000.0)
        } else {
            last.unix_ns
                .checked_sub(first.unix_ns)
                .filter(|ns| *ns >= 0)
                .map(|ns| ns as f64 / 1_000_000.0)
        }
    }

    // Queue durations are approximate millisecond measurements.
    #[allow(clippy::cast_precision_loss)]
    pub fn derive(&mut self) {
        let first = |stage: &str| self.stages.iter().find(|row| row.stage == stage);
        self.append_to_first_token_ms = first("appended")
            .zip(first("first_token"))
            .and_then(|(a, b)| Self::duration_ms(a, b));
        self.inference_duration_ms = first("inference_started")
            .zip(first("inference_finished"))
            .and_then(|(a, b)| Self::duration_ms(a, b));
        self.append_to_idle_ms = first("appended")
            .zip(first("idle"))
            .and_then(|(a, b)| Self::duration_ms(a, b));
        for tool in &mut self.tools {
            tool.queue_ms = tool
                .dispatched_ns
                .zip(tool.started_ns)
                .and_then(|(a, b)| b.checked_sub(a).filter(|ns| *ns >= 0))
                .map(|ns| ns as f64 / 1_000_000.0);
        }
        for request in &mut self.inference {
            let stages = &self.stages;
            let stage = |name: &str| {
                stages.iter().find(|row| {
                    row.stage == name && row.request_id.as_deref() == Some(&request.request_id)
                })
            };
            request.time_to_first_token_ms = stage("inference_started")
                .zip(stage("first_token"))
                .and_then(|(a, b)| Self::duration_ms(a, b));
            request.streaming_duration_ms = stage("first_token")
                .zip(stage("inference_finished"))
                .and_then(|(a, b)| Self::duration_ms(a, b));
            request.output_tokens_per_second = request
                .streaming_duration_ms
                .and_then(|ms| InferenceMetric::tokens_per_second(request.output_tokens, ms));
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct LatencyPercentiles {
    pub p50_ms: f64,
    pub p95_ms: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct AgentMetrics {
    pub agent_id: String,
    pub main_session_id: Option<String>,
    pub turns: u64,
    pub latencies: BTreeMap<String, LatencyPercentiles>,
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_tokens: u64,
    pub mean_output_tokens_per_second: Option<f64>,
    pub retries: u64,
    pub errors: u64,
    pub cost_micros: u64,
}
