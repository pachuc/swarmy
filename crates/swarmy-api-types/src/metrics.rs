//! API response shapes for per-turn measurements. These carry the values the
//! store derives; the only duration and throughput derivation lives on the
//! store's mirror shapes in `swarmy-store/src/metrics.rs`.
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
    /// Whole-request duration the store derives from the earliest
    /// `InferenceStarted` stage to the latest `InferenceFinished` stage for
    /// this request id. The window starts when the gateway observes the
    /// attempt (before provider and entry resolution) and ends when the
    /// terminal stage lands, so it includes breaker waits and any backoff
    /// between retries that share the request id. Throughput divides by this
    /// interval, not by the streaming interval, so single-chunk responses do
    /// not report absurd rates.
    #[serde(default)]
    pub request_duration_ms: Option<f64>,
    /// Whether the response arrived as more than one content chunk (`Some(true)`
    /// streamed, `Some(false)` delivered whole in one chunk). `None` means the
    /// request produced no content: a failed attempt, or a row written before
    /// the flag existed. Reports skip `None` when counting single-chunk
    /// responses.
    #[serde(default)]
    pub streamed: Option<bool>,
    pub output_tokens_per_second: Option<f64>,
    pub retries: u32,
    pub rate_limit_waits: u32,
    pub gateway_waits: u32,
    pub provider_failures: u32,
    pub error: Option<String>,
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
    /// Anchor and per-request stages reconstructed from the summary and detail
    /// rows: turn anchors (`submitted`, `appended`, `first_token`,
    /// `inference_started`, `inference_finished`, `idle`) plus one
    /// `inference_started` / `first_token` / `inference_finished` triple per
    /// inference request that survives paging. The durable store keeps no
    /// bounded stage list and emits no `first_tool` stage; tool timing lives
    /// on the `tools` rows.
    pub stages: Vec<StageTiming>,
    /// Inference requests in chronological order (earliest start first).
    /// A paged read truncates to `inference_limit` and reports the remainder
    /// in `dropped_inference`.
    pub inference: Vec<InferenceMetric>,
    /// Tool calls in chronological order (earliest dispatch first). A paged
    /// read truncates to `tools_limit` and reports the remainder in
    /// `dropped_tools`.
    pub tools: Vec<ToolMetric>,
    pub computer: Option<ComputerMetric>,
    pub append_to_first_token_ms: Option<f64>,
    pub inference_duration_ms: Option<f64>,
    pub append_to_idle_ms: Option<f64>,
    pub error: Option<String>,
    /// Always zero for turns written in the unbounded layout; kept so paged
    /// reads have a place for the remainder (unused for stages).
    #[serde(default)]
    pub dropped_stages: u64,
    /// Remainder of the `inference` array omitted by `inference_limit`, plus
    /// rows omitted from a paged response.
    #[serde(default)]
    pub dropped_inference: u64,
    /// Remainder of the `tools` array omitted by `tools_limit`, plus rows
    /// rows omitted from a paged response.
    #[serde(default)]
    pub dropped_tools: u64,
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
    /// Turn-level latencies (`append_to_first_token`, `inference`,
    /// `append_to_idle`, `placement`) over the rolled-up turns. Per-tool
    /// percentiles are intentionally absent: the rollup scans summary rows
    /// only, and tool timing lives on per-tool detail rows that the rollup
    /// never fans out to.
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
