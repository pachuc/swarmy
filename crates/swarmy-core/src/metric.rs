use serde::{Deserialize, Serialize};

/// One tool call's timing and result size.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ToolMetric {
    pub request_id: String,
    pub name: String,
    /// Worker clock: the worker dispatched the call.
    pub dispatched_ns: Option<i64>,
    /// Node clock: the node started handling the NATS delivery.
    pub delivered_ns: Option<i64>,
    /// Node clock: the node's tool claim committed.
    pub claimed_ns: Option<i64>,
    /// Node clock: the sandbox command started.
    pub started_ns: Option<i64>,
    /// Node clock: the node stamped `ToolCompleted` right after its completion commit.
    pub completed_ns: Option<i64>,
    /// Worker clock: the worker claimed the step that folds this result.
    pub woken_ns: Option<i64>,
    pub exit_status: Option<i32>,
    pub output_bytes: Option<u64>,
    pub queue_ms: Option<f64>,
    pub process_wall_ms: Option<f64>,
}

/// Volume and placement counters sampled for the turn.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ComputerMetric {
    pub placement_ms: Option<f64>,
    pub cold: Option<bool>,
    pub chunks_fetched: u64,
    pub bytes_fetched: u64,
    pub fetch_p50_ms: Option<f64>,
    pub fetch_p95_ms: Option<f64>,
    /// Volume counters re-sampled after the first tool call of the turn
    /// completes.
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
    /// Milliseconds from placement start through volume binding and spec read.
    #[serde(default)]
    pub open_ms: Option<f64>,
    /// Milliseconds spent creating the sandbox, including volume attach.
    #[serde(default)]
    pub boot_ms: Option<f64>,
}
