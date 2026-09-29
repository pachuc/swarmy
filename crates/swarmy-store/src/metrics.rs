//! Compact, independently committed observations; no conversation event is added.
//!
//! Turns of any length are recorded completely. The turn summary lives under
//! `("turn_metrics", session, turn)` and every inference request and tool
//! call lives in its own row under `("turn_inference", session, turn,
//! request_id)` and `("turn_tool", session, turn, call_id)`. One
//! `FoundationDB` value never grows with the turn, so a fleet task with
//! hundreds of tool calls needs hundreds of small rows instead of one capped
//! record. The summary keeps the few anchor timestamps the rollup needs
//! (submitted, appended, first token, first tool, idle, plus the global
//! inference span) with running token and wait totals, so `agent metrics`
//! scans summaries only.
use std::collections::{BTreeMap, BTreeSet};

use foundationdb::{RangeOption, Transaction};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use swarmy_core::{AgentId, MessageId, SessionId, TurnEvent, TurnStage};

use crate::{MAX_SCAN_LIMIT, Result, Store, StoreError, decode, scan, write};

// Persistence-layer metric shapes. These mirror `swarmy-api-types::metrics`
// field for field so stored rows decode the same way, but they live here so
// the store never builds API response shapes (and never compiles the `OpenAPI`
// tooling). `crates/swarmy-api/src/views.rs` converts between these and the
// API types.

/// One timed anchor or per-request stage, as recorded by the turn summary.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageTiming {
    pub stage: String,
    pub request_id: Option<String>,
    pub clock_id: String,
    pub monotonic_ns: u64,
    pub unix_ns: i64,
}

/// One inference request's token, cost, and timing totals.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
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
    /// Whole-request duration derived from the earliest `InferenceStarted`
    /// stage to the latest `InferenceFinished` stage for this request id.
    #[serde(default)]
    pub request_duration_ms: Option<f64>,
    /// Whether the response arrived as more than one content chunk.
    #[serde(default)]
    pub streamed: Option<bool>,
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
    pub(crate) fn tokens_per_second(tokens: u64, duration_ms: f64) -> Option<f64> {
        (tokens > 0 && duration_ms > 0.0 && duration_ms.is_finite())
            .then_some(tokens as f64 * 1_000.0 / duration_ms)
    }
}

/// One tool call's timing and result size.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
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
}

/// One turn's assembled metrics, as read back from the summary and rows.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TurnMetrics {
    pub session_id: String,
    pub turn_id: String,
    /// Anchor and per-request stages reconstructed from the summary and
    /// detail rows.
    pub stages: Vec<StageTiming>,
    /// Inference requests in chronological order (earliest start first).
    pub inference: Vec<InferenceMetric>,
    /// Tool calls in chronological order (earliest dispatch first).
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
    /// Remainder of the `inference` array omitted by `inference_limit`.
    #[serde(default)]
    pub dropped_inference: u64,
    /// Remainder of the `tools` array omitted by `tools_limit`.
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
            request.request_duration_ms = stage("inference_started")
                .zip(stage("inference_finished"))
                .and_then(|(a, b)| Self::duration_ms(a, b));
            request.output_tokens_per_second = request
                .request_duration_ms
                .or(request.streaming_duration_ms)
                .and_then(|ms| InferenceMetric::tokens_per_second(request.output_tokens, ms));
        }
    }
}

/// Turn-level latency percentiles over the rolled-up turns.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LatencyPercentiles {
    pub p50_ms: f64,
    pub p95_ms: f64,
}

/// Rollup over an agent's recent main-session turns.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentMetrics {
    pub agent_id: String,
    pub main_session_id: Option<String>,
    pub turns: u64,
    /// Turn-level latencies over the rolled-up turns.
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

#[derive(Clone)]
pub enum MetricPatch {
    Stage(TurnEvent),
    Inference(InferenceMetric),
    Tool(ToolMetric),
    Computer(ComputerMetric),
    Error(String),
    Wait { request_id: String, kind: WaitKind },
}

#[derive(Clone, Copy)]
pub enum WaitKind {
    Retry,
    RateLimit,
    MissingGateway,
    ProviderFailure,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct StoredTurnSummaryCurrent {
    session_id: String,
    turn_id: String,
    submitted_ns: Option<i64>,
    appended_ns: Option<i64>,
    first_token_ns: Option<i64>,
    first_tool_ns: Option<i64>,
    idle_ns: Option<i64>,
    inference_started_ns: Option<i64>,
    inference_finished_ns: Option<i64>,
    computer: Option<ComputerMetric>,
    append_to_first_token_ms: Option<f64>,
    inference_duration_ms: Option<f64>,
    append_to_idle_ms: Option<f64>,
    error: Option<String>,
    input_tokens: u64,
    cached_input_tokens: u64,
    output_tokens: u64,
    reasoning_tokens: u64,
    cost_micros: u64,
    retries: u64,
    rate_limit_waits: u64,
    gateway_waits: u64,
    provider_failures: u64,
    inference_errors: u64,
    throughput_sum: f64,
    throughput_count: u64,
    /// Paging counters. New turns store zero until a response is paginated.
    dropped_stages: u64,
    dropped_inference: u64,
    dropped_tools: u64,
}

/// Frozen inference fields for one current request row. This mirrors
/// [`InferenceMetric`] at the current layout revision: postcard is positional, so
/// the API type cannot be embedded directly (the next field added to the API
/// type would make every stored row undecodable). Convert to and from the API
/// type with the helpers below. Changing this positional layout requires a
/// new fixed-byte fixture and a one-way break.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct StoredInferenceMetricCurrent {
    request_id: String,
    provider: String,
    model: String,
    input_tokens: u64,
    cached_input_tokens: u64,
    output_tokens: u64,
    reasoning_tokens: u64,
    cost_micros: u64,
    time_to_first_token_ms: Option<f64>,
    streaming_duration_ms: Option<f64>,
    request_duration_ms: Option<f64>,
    streamed: Option<bool>,
    output_tokens_per_second: Option<f64>,
    retries: u32,
    rate_limit_waits: u32,
    gateway_waits: u32,
    provider_failures: u32,
    error: Option<String>,
}

impl StoredInferenceMetricCurrent {
    fn into_public(self) -> InferenceMetric {
        InferenceMetric {
            request_id: self.request_id,
            provider: self.provider,
            model: self.model,
            input_tokens: self.input_tokens,
            cached_input_tokens: self.cached_input_tokens,
            output_tokens: self.output_tokens,
            reasoning_tokens: self.reasoning_tokens,
            cost_micros: self.cost_micros,
            time_to_first_token_ms: self.time_to_first_token_ms,
            streaming_duration_ms: self.streaming_duration_ms,
            request_duration_ms: self.request_duration_ms,
            streamed: self.streamed,
            output_tokens_per_second: self.output_tokens_per_second,
            retries: self.retries,
            rate_limit_waits: self.rate_limit_waits,
            gateway_waits: self.gateway_waits,
            provider_failures: self.provider_failures,
            error: self.error,
        }
    }
}

/// Frozen tool fields for one current tool row. Mirrors [`ToolMetric`] at the
/// current layout revision for the same positional-encoding reason as
/// [`StoredInferenceMetricCurrent`]; future stored fields go last through
/// a new fixed-byte fixture and a one-way break.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct StoredToolMetricCurrent {
    request_id: String,
    name: String,
    dispatched_ns: Option<i64>,
    started_ns: Option<i64>,
    completed_ns: Option<i64>,
    exit_status: Option<i32>,
    output_bytes: Option<u64>,
    queue_ms: Option<f64>,
    process_wall_ms: Option<f64>,
}

impl StoredToolMetricCurrent {
    fn into_public(self) -> ToolMetric {
        ToolMetric {
            request_id: self.request_id,
            name: self.name,
            dispatched_ns: self.dispatched_ns,
            started_ns: self.started_ns,
            completed_ns: self.completed_ns,
            exit_status: self.exit_status,
            output_bytes: self.output_bytes,
            queue_ms: self.queue_ms,
            process_wall_ms: self.process_wall_ms,
        }
    }
}

/// One inference request row. Token counts and the streamed flag arrive with
/// the terminal patch; the stage path fills the wall-clock anchors that
/// throughput derives from.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct StoredTurnInferenceCurrent {
    metric: StoredInferenceMetricCurrent,
    started_ns: Option<i64>,
    first_token_ns: Option<i64>,
    finished_ns: Option<i64>,
}

/// Stored summary and detail rows. Each keyspace accepts only its own variant.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum StoredTurnMetrics {
    Summary(Box<StoredTurnSummaryCurrent>),
    Inference(StoredTurnInferenceCurrent),
    Tool(StoredToolMetricCurrent),
}

fn decode_summary(bytes: &[u8]) -> Result<StoredTurnSummaryCurrent> {
    match decode::<StoredTurnMetrics>(bytes)? {
        StoredTurnMetrics::Summary(summary) => Ok(*summary),
        StoredTurnMetrics::Inference(_) | StoredTurnMetrics::Tool(_) => {
            Err(StoreError::Storage(crate::StorageError::Corrupt))
        }
    }
}

fn decode_inference(bytes: &[u8]) -> Result<StoredTurnInferenceCurrent> {
    match decode::<StoredTurnMetrics>(bytes)? {
        StoredTurnMetrics::Inference(row) => Ok(row),
        StoredTurnMetrics::Summary(_) | StoredTurnMetrics::Tool(_) => {
            Err(StoreError::Storage(crate::StorageError::Corrupt))
        }
    }
}

fn decode_tool(bytes: &[u8]) -> Result<StoredToolMetricCurrent> {
    match decode::<StoredTurnMetrics>(bytes)? {
        StoredTurnMetrics::Tool(row) => Ok(row),
        StoredTurnMetrics::Summary(_) | StoredTurnMetrics::Inference(_) => {
            Err(StoreError::Storage(crate::StorageError::Corrupt))
        }
    }
}

/// One worker dispatch folds the tool name into the dispatch stage it already
/// writes, so a dispatch costs one transaction instead of two.
#[must_use]
pub fn dispatch_patches(event: TurnEvent, tool_name: &str) -> Vec<MetricPatch> {
    let request_id = event
        .request_id
        .map(|id| id.to_string())
        .unwrap_or_default();
    vec![
        MetricPatch::Tool(ToolMetric {
            request_id,
            name: tool_name.to_owned(),
            ..ToolMetric::default()
        }),
        MetricPatch::Stage(event),
    ]
}

/// One node completion folds the tool result and the completion stage into
/// a single transaction. The first-tool computer re-sample follows in its
/// own transaction from a spawned task so the completion never waits on the
/// volume stat round trip; the helper keeps the optional computer slot for
/// tests that build the batch directly.
#[must_use]
pub fn completion_patches(
    tool: ToolMetric,
    computer: Option<ComputerMetric>,
    completed: TurnEvent,
) -> Vec<MetricPatch> {
    let mut patches = vec![MetricPatch::Tool(tool)];
    if let Some(sample) = computer {
        patches.push(MetricPatch::Computer(sample));
    }
    patches.push(MetricPatch::Stage(completed));
    patches
}

fn clipped(value: &str, max: usize) -> String {
    value.chars().take(max).collect()
}

// Wall nanoseconds exceed f64 integer precision; millisecond display values do not need it.
#[allow(clippy::cast_precision_loss)]
fn wall_ms(first_ns: Option<i64>, last_ns: Option<i64>) -> Option<f64> {
    let (first, last) = (first_ns?, last_ns?);
    last.checked_sub(first)
        .filter(|ns| *ns >= 0)
        // Nanosecond wall times fit in an f64 millisecond display value.
        .map(|ns| ns as f64 / 1_000_000.0)
}

fn unix_ns(event: &TurnEvent) -> i64 {
    i64::try_from(event.unix_ns).unwrap_or(i64::MAX)
}

fn derive_summary(summary: &mut StoredTurnSummaryCurrent) {
    summary.append_to_first_token_ms = wall_ms(summary.appended_ns, summary.first_token_ns);
    summary.inference_duration_ms =
        wall_ms(summary.inference_started_ns, summary.inference_finished_ns);
    // The idle anchor lives on the summary itself, so a turn of any length
    // keeps its wall time even though no bounded stage list survives.
    summary.append_to_idle_ms = wall_ms(summary.appended_ns, summary.idle_ns);
}

/// Throughput covers the whole request. A provider that delivers the entire
/// response in one chunk would otherwise report tens of thousands of tokens
/// per second over a millisecond streaming interval.
fn derive_inference(row: &mut StoredTurnInferenceCurrent) {
    let metric = &mut row.metric;
    metric.time_to_first_token_ms = wall_ms(row.started_ns, row.first_token_ns);
    metric.streaming_duration_ms = wall_ms(row.first_token_ns, row.finished_ns);
    metric.request_duration_ms = wall_ms(row.started_ns, row.finished_ns);
    metric.output_tokens_per_second = metric
        .request_duration_ms
        .or(metric.streaming_duration_ms)
        .and_then(|ms| InferenceMetric::tokens_per_second(metric.output_tokens, ms));
}

fn apply_computer(summary: &mut StoredTurnSummaryCurrent, value: &ComputerMetric) {
    let current = summary.computer.get_or_insert_with(ComputerMetric::default);
    if value.placement_ms.is_some() {
        current.placement_ms = value.placement_ms;
    }
    if value.cold.is_some() {
        current.cold = value.cold;
    }
    if value.chunks_fetched > 0 {
        current.chunks_fetched = value.chunks_fetched;
    }
    if value.bytes_fetched > 0 {
        current.bytes_fetched = value.bytes_fetched;
    }
    if value.fetch_p50_ms.is_some() {
        current.fetch_p50_ms = value.fetch_p50_ms;
    }
    if value.fetch_p95_ms.is_some() {
        current.fetch_p95_ms = value.fetch_p95_ms;
    }
    // The boot sample arrives before the first tool runs; the re-sampled
    // first-tool counters arrive after. The first re-sample wins so later
    // tools do not overwrite what the first command observed.
    if current.first_tool_chunks_fetched.is_none() {
        current.first_tool_chunks_fetched = value.first_tool_chunks_fetched;
    }
    if current.first_tool_bytes_fetched.is_none() {
        current.first_tool_bytes_fetched = value.first_tool_bytes_fetched;
    }
    if current.first_tool_fetch_p50_ms.is_none() {
        current.first_tool_fetch_p50_ms = value.first_tool_fetch_p50_ms;
    }
    if current.first_tool_fetch_p95_ms.is_none() {
        current.first_tool_fetch_p95_ms = value.first_tool_fetch_p95_ms;
    }
}

/// Per-turn mutable state held inside one transaction: the summary plus only
/// the inference and tool rows this batch touches.
struct TurnWrite {
    summary: StoredTurnSummaryCurrent,
    inference: BTreeMap<String, StoredTurnInferenceCurrent>,
    tools: BTreeMap<String, StoredToolMetricCurrent>,
}

fn adjust_throughput(summary: &mut StoredTurnSummaryCurrent, old: Option<f64>, new: Option<f64>) {
    match (old, new) {
        (Some(previous), Some(current)) => {
            summary.throughput_sum += current - previous;
        }
        (Some(previous), None) => {
            summary.throughput_sum -= previous;
            summary.throughput_count = summary.throughput_count.saturating_sub(1);
        }
        (None, Some(current)) => {
            summary.throughput_sum += current;
            summary.throughput_count += 1;
        }
        (None, None) => {}
    }
}

fn inference_entry<'a>(
    inference: &'a mut BTreeMap<String, StoredTurnInferenceCurrent>,
    request_id: &str,
) -> &'a mut StoredTurnInferenceCurrent {
    inference
        .entry(request_id.to_owned())
        .or_insert_with(|| StoredTurnInferenceCurrent {
            metric: StoredInferenceMetricCurrent {
                request_id: request_id.to_owned(),
                ..StoredInferenceMetricCurrent::default()
            },
            ..StoredTurnInferenceCurrent::default()
        })
}

fn tool_entry<'a>(
    tools: &'a mut BTreeMap<String, StoredToolMetricCurrent>,
    request_id: &str,
) -> &'a mut StoredToolMetricCurrent {
    tools
        .entry(request_id.to_owned())
        .or_insert_with(|| StoredToolMetricCurrent {
            request_id: request_id.to_owned(),
            ..StoredToolMetricCurrent::default()
        })
}

// One merge updates row, summary totals, and throughput together; splitting
// would separate the delta bookkeeping from the row it deltas against.
#[allow(clippy::too_many_lines)]
fn apply_inference_update(
    summary: &mut StoredTurnSummaryCurrent,
    inference: &mut BTreeMap<String, StoredTurnInferenceCurrent>,
    update: &InferenceMetric,
) {
    let row = inference_entry(inference, &update.request_id);
    let old_throughput = row.metric.output_tokens_per_second;
    let old_tokens = (
        row.metric.input_tokens,
        row.metric.cached_input_tokens,
        row.metric.output_tokens,
        row.metric.reasoning_tokens,
        row.metric.cost_micros,
    );
    let old_counters = (
        row.metric.retries,
        row.metric.rate_limit_waits,
        row.metric.gateway_waits,
        row.metric.provider_failures,
    );
    let old_error = row.metric.error.is_some();
    // A failed request carries its error on the row and the turn so the
    // rollup counts it even when the terminal Error patch lands first; a
    // success clears a previous turn error for the same turn.
    if update.error.is_some() {
        summary.error.clone_from(&update.error);
    } else if summary.error.is_some() && old_error {
        summary.error = None;
    }
    if !update.provider.is_empty() {
        row.metric.provider = clipped(&update.provider, 80);
    }
    if !update.model.is_empty() {
        row.metric.model = clipped(&update.model, 120);
    }
    row.metric.input_tokens = update.input_tokens;
    row.metric.cached_input_tokens = update.cached_input_tokens;
    row.metric.output_tokens = update.output_tokens;
    row.metric.reasoning_tokens = update.reasoning_tokens;
    row.metric.cost_micros = update.cost_micros;
    row.metric.streamed = update.streamed;
    if update.error.is_some() {
        row.metric.error.clone_from(&update.error);
    } else {
        row.metric.error = None;
    }
    row.metric.retries = row.metric.retries.max(update.retries).max(old_counters.0);
    row.metric.rate_limit_waits = row
        .metric
        .rate_limit_waits
        .max(update.rate_limit_waits)
        .max(old_counters.1);
    row.metric.gateway_waits = row
        .metric
        .gateway_waits
        .max(update.gateway_waits)
        .max(old_counters.2);
    row.metric.provider_failures = row
        .metric
        .provider_failures
        .max(update.provider_failures)
        .max(old_counters.3);
    derive_inference(row);
    let new_tokens = (
        row.metric.input_tokens,
        row.metric.cached_input_tokens,
        row.metric.output_tokens,
        row.metric.reasoning_tokens,
        row.metric.cost_micros,
    );
    let new_counters = (
        row.metric.retries,
        row.metric.rate_limit_waits,
        row.metric.gateway_waits,
        row.metric.provider_failures,
    );
    let new_throughput = row.metric.output_tokens_per_second;
    let new_error = row.metric.error.is_some();
    summary.input_tokens = summary
        .input_tokens
        .saturating_add(new_tokens.0.saturating_sub(old_tokens.0));
    summary.cached_input_tokens = summary
        .cached_input_tokens
        .saturating_add(new_tokens.1.saturating_sub(old_tokens.1));
    summary.output_tokens = summary
        .output_tokens
        .saturating_add(new_tokens.2.saturating_sub(old_tokens.2));
    summary.reasoning_tokens = summary
        .reasoning_tokens
        .saturating_add(new_tokens.3.saturating_sub(old_tokens.3));
    summary.cost_micros = summary
        .cost_micros
        .saturating_add(new_tokens.4.saturating_sub(old_tokens.4));
    summary.retries = summary
        .retries
        .saturating_add(u64::from(new_counters.0).saturating_sub(u64::from(old_counters.0)));
    summary.rate_limit_waits = summary
        .rate_limit_waits
        .saturating_add(u64::from(new_counters.1).saturating_sub(u64::from(old_counters.1)));
    summary.gateway_waits = summary
        .gateway_waits
        .saturating_add(u64::from(new_counters.2).saturating_sub(u64::from(old_counters.2)));
    summary.provider_failures = summary
        .provider_failures
        .saturating_add(u64::from(new_counters.3).saturating_sub(u64::from(old_counters.3)));
    match (old_error, new_error) {
        (false, true) => summary.inference_errors += 1,
        (true, false) => summary.inference_errors = summary.inference_errors.saturating_sub(1),
        _ => {}
    }
    adjust_throughput(summary, old_throughput, new_throughput);
}

fn apply_tool_update(tools: &mut BTreeMap<String, StoredToolMetricCurrent>, update: &ToolMetric) {
    let row = tool_entry(tools, &update.request_id);
    if !update.name.is_empty() {
        row.name = clipped(&update.name, 80);
    }
    if let Some(value) = update.dispatched_ns {
        row.dispatched_ns = Some(value);
    }
    if let Some(value) = update.started_ns {
        row.started_ns = Some(value);
    }
    if let Some(value) = update.completed_ns {
        row.completed_ns = Some(value);
    }
    if let Some(value) = update.exit_status {
        row.exit_status = Some(value);
    }
    if let Some(value) = update.output_bytes {
        row.output_bytes = Some(value);
    }
    if let Some(value) = update.queue_ms {
        row.queue_ms = Some(value);
    }
    if let Some(value) = update.process_wall_ms {
        row.process_wall_ms = Some(value);
    }
}

fn apply_stage(
    summary: &mut StoredTurnSummaryCurrent,
    inference: &mut BTreeMap<String, StoredTurnInferenceCurrent>,
    tools: &mut BTreeMap<String, StoredToolMetricCurrent>,
    event: &TurnEvent,
) {
    let wall = unix_ns(event);
    match event.stage {
        TurnStage::Submitted => {
            if summary.submitted_ns.is_none() {
                summary.submitted_ns = Some(wall);
            }
        }
        TurnStage::Appended => {
            if summary.appended_ns.is_none() {
                summary.appended_ns = Some(wall);
            }
        }
        TurnStage::FirstToken => {
            if let Some(request_id) = event.request_id {
                let key = request_id.to_string();
                let old_throughput = inference
                    .get(&key)
                    .and_then(|row| row.metric.output_tokens_per_second);
                let row = inference_entry(inference, &key);
                row.first_token_ns = Some(row.first_token_ns.unwrap_or(i64::MAX).min(wall));
                derive_inference(row);
                let new_throughput = row.metric.output_tokens_per_second;
                adjust_throughput(summary, old_throughput, new_throughput);
            }
            summary.first_token_ns = Some(summary.first_token_ns.unwrap_or(i64::MAX).min(wall));
            if summary.inference_started_ns.is_none() {
                summary.inference_started_ns = Some(wall);
            }
        }
        TurnStage::InferenceStarted => {
            if let Some(request_id) = event.request_id {
                let key = request_id.to_string();
                let old_throughput = inference
                    .get(&key)
                    .and_then(|row| row.metric.output_tokens_per_second);
                let row = inference_entry(inference, &key);
                row.started_ns = Some(row.started_ns.unwrap_or(i64::MAX).min(wall));
                derive_inference(row);
                let new_throughput = row.metric.output_tokens_per_second;
                adjust_throughput(summary, old_throughput, new_throughput);
            }
            summary.inference_started_ns =
                Some(summary.inference_started_ns.unwrap_or(i64::MAX).min(wall));
        }
        TurnStage::InferenceFinished => {
            if let Some(request_id) = event.request_id {
                let key = request_id.to_string();
                let old_throughput = inference
                    .get(&key)
                    .and_then(|row| row.metric.output_tokens_per_second);
                let row = inference_entry(inference, &key);
                row.finished_ns = Some(row.finished_ns.unwrap_or(i64::MIN).max(wall));
                derive_inference(row);
                let new_throughput = row.metric.output_tokens_per_second;
                adjust_throughput(summary, old_throughput, new_throughput);
            }
            summary.inference_finished_ns =
                Some(summary.inference_finished_ns.unwrap_or(i64::MIN).max(wall));
        }
        TurnStage::ToolDispatched => {
            if let Some(request_id) = event.request_id {
                let key = request_id.to_string();
                let row = tool_entry(tools, &key);
                if row.dispatched_ns.is_none() {
                    row.dispatched_ns = Some(wall);
                }
            }
            summary.first_tool_ns = Some(summary.first_tool_ns.unwrap_or(i64::MAX).min(wall));
        }
        TurnStage::ToolCompleted => {
            if let Some(request_id) = event.request_id {
                let key = request_id.to_string();
                let row = tool_entry(tools, &key);
                row.completed_ns = Some(row.completed_ns.unwrap_or(i64::MIN).max(wall));
            }
        }
        TurnStage::Idle => {
            // The idle anchor is the turn's wall time. It arrives once from
            // the worker (and the gateway snapshot path), and the summary
            // keeps it as a dedicated field so long turns can never drop it.
            if summary.idle_ns.is_none() {
                summary.idle_ns = Some(wall);
            }
        }
        TurnStage::Nudged
        | TurnStage::Claimed
        | TurnStage::FinalTextRendered
        | TurnStage::InputEnabled => {}
    }
    derive_summary(summary);
}

fn apply_wait(
    summary: &mut StoredTurnSummaryCurrent,
    inference: &mut BTreeMap<String, StoredTurnInferenceCurrent>,
    request_id: &str,
    kind: WaitKind,
) {
    let old_throughput = inference
        .get(request_id)
        .and_then(|row| row.metric.output_tokens_per_second);
    let row = inference_entry(inference, request_id);
    match kind {
        WaitKind::Retry => {
            row.metric.retries = row.metric.retries.saturating_add(1);
            summary.retries += 1;
        }
        WaitKind::RateLimit => {
            row.metric.rate_limit_waits = row.metric.rate_limit_waits.saturating_add(1);
            summary.rate_limit_waits += 1;
        }
        WaitKind::MissingGateway => {
            row.metric.gateway_waits = row.metric.gateway_waits.saturating_add(1);
            summary.gateway_waits += 1;
        }
        WaitKind::ProviderFailure => {
            row.metric.provider_failures = row.metric.provider_failures.saturating_add(1);
            summary.provider_failures += 1;
        }
    }
    derive_inference(row);
    let new_throughput = row.metric.output_tokens_per_second;
    adjust_throughput(summary, old_throughput, new_throughput);
}

fn apply_one(state: &mut TurnWrite, patch: &MetricPatch) {
    let TurnWrite {
        summary,
        inference,
        tools,
    } = state;
    match patch {
        MetricPatch::Stage(event) => apply_stage(summary, inference, tools, event),
        MetricPatch::Inference(update) => apply_inference_update(summary, inference, update),
        MetricPatch::Tool(update) => apply_tool_update(tools, update),
        MetricPatch::Computer(value) => apply_computer(summary, value),
        MetricPatch::Error(value) => {
            summary.error = Some(clipped(value, 512));
        }
        MetricPatch::Wait { request_id, kind } => apply_wait(summary, inference, request_id, *kind),
    }
}

async fn scan_reverse(
    trx: &Transaction,
    range: (Vec<u8>, Vec<u8>),
    limit: usize,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    if !(1..=MAX_SCAN_LIMIT).contains(&limit) {
        return Err(crate::StoreError::Domain(crate::DomainError::InvalidLimit));
    }
    let options = RangeOption {
        limit: Some(limit),
        reverse: true,
        ..range.into()
    };
    Ok(trx
        .get_ranges_keyvalues(options, false)
        .map_ok(|kv| (kv.key().to_vec(), kv.value().to_vec()))
        .try_collect()
        .await?)
}

fn anchor_stage(name: &str, unix_ns: Option<i64>) -> Option<StageTiming> {
    let unix_ns = unix_ns?;
    Some(StageTiming {
        stage: name.to_owned(),
        request_id: None,
        clock_id: "summary".to_owned(),
        monotonic_ns: u64::try_from(unix_ns).unwrap_or(0),
        unix_ns,
    })
}

fn request_stage(name: &str, request_id: &str, unix_ns: Option<i64>) -> Option<StageTiming> {
    let unix_ns = unix_ns?;
    Some(StageTiming {
        stage: name.to_owned(),
        request_id: Some(request_id.to_owned()),
        clock_id: "summary".to_owned(),
        monotonic_ns: u64::try_from(unix_ns).unwrap_or(0),
        unix_ns,
    })
}

/// Turn anchors in the unbounded summary.
fn anchor_stages(summary: &StoredTurnSummaryCurrent) -> Vec<StageTiming> {
    let mut stages = Vec::new();
    for (name, stamp) in [
        ("submitted", summary.submitted_ns),
        ("appended", summary.appended_ns),
        ("first_token", summary.first_token_ns),
        ("inference_started", summary.inference_started_ns),
        ("inference_finished", summary.inference_finished_ns),
        ("idle", summary.idle_ns),
    ] {
        if let Some(row) = anchor_stage(name, stamp) {
            stages.push(row);
        }
    }
    stages
}

/// Chronological order for per-request rows. Request ids are blake3 hashes,
/// so id order is arbitrary; the per-row start timestamp restores the order
/// the turn executed in, with the id only breaking ties.
fn sort_inference_rows(rows: &mut [StoredTurnInferenceCurrent]) {
    rows.sort_by(|a, b| {
        (a.started_ns.unwrap_or(i64::MAX), &a.metric.request_id)
            .cmp(&(b.started_ns.unwrap_or(i64::MAX), &b.metric.request_id))
    });
}

/// Chronological order for per-tool rows by dispatch, then start, then id.
fn sort_tool_rows(rows: &mut [StoredToolMetricCurrent]) {
    rows.sort_by(|a, b| {
        (
            a.dispatched_ns.unwrap_or(i64::MAX),
            a.started_ns.unwrap_or(i64::MAX),
            &a.request_id,
        )
            .cmp(&(
                b.dispatched_ns.unwrap_or(i64::MAX),
                b.started_ns.unwrap_or(i64::MAX),
                &b.request_id,
            ))
    });
}

/// Assemble one public turn record from a summary plus its detail rows.
/// Paging truncates the chronologically sorted arrays and reports the
/// remainder in `dropped_*`; a complete read reports zero.
fn assemble_turn(
    summary: &StoredTurnSummaryCurrent,
    mut inference_rows: Vec<StoredTurnInferenceCurrent>,
    mut tool_rows: Vec<StoredToolMetricCurrent>,
    inference_limit: Option<usize>,
    tools_limit: Option<usize>,
) -> TurnMetrics {
    let mut stages = anchor_stages(summary);
    sort_inference_rows(&mut inference_rows);
    sort_tool_rows(&mut tool_rows);
    let total_inference = inference_rows.len();
    let total_tools = tool_rows.len();
    let inference_page: Vec<StoredTurnInferenceCurrent> = match inference_limit {
        Some(limit) => inference_rows.into_iter().take(limit).collect(),
        None => inference_rows,
    };
    let tools_page: Vec<StoredToolMetricCurrent> = match tools_limit {
        Some(limit) => tool_rows.into_iter().take(limit).collect(),
        None => tool_rows,
    };
    for row in &inference_page {
        if let Some(stage) =
            request_stage("inference_started", &row.metric.request_id, row.started_ns)
        {
            stages.push(stage);
        }
        if let Some(stage) =
            request_stage("first_token", &row.metric.request_id, row.first_token_ns)
        {
            stages.push(stage);
        }
        if let Some(stage) = request_stage(
            "inference_finished",
            &row.metric.request_id,
            row.finished_ns,
        ) {
            stages.push(stage);
        }
    }
    let mut turn = TurnMetrics {
        session_id: summary.session_id.clone(),
        turn_id: summary.turn_id.clone(),
        stages,
        inference: inference_page
            .into_iter()
            .map(|row| row.metric.into_public())
            .collect(),
        tools: tools_page
            .into_iter()
            .map(StoredToolMetricCurrent::into_public)
            .collect(),
        computer: summary.computer.clone(),
        append_to_first_token_ms: None,
        inference_duration_ms: None,
        append_to_idle_ms: None,
        error: summary.error.clone(),
        dropped_stages: summary.dropped_stages,
        dropped_inference: summary.dropped_inference,
        dropped_tools: summary.dropped_tools,
    };
    turn.dropped_inference = turn.dropped_inference.saturating_add(
        u64::try_from(total_inference.saturating_sub(turn.inference.len())).unwrap_or(u64::MAX),
    );
    turn.dropped_tools = turn.dropped_tools.saturating_add(
        u64::try_from(total_tools.saturating_sub(turn.tools.len())).unwrap_or(u64::MAX),
    );
    turn.derive();
    if turn.append_to_first_token_ms.is_none() {
        turn.append_to_first_token_ms = summary.append_to_first_token_ms;
    }
    if turn.inference_duration_ms.is_none() {
        turn.inference_duration_ms = summary.inference_duration_ms;
    }
    if turn.append_to_idle_ms.is_none() {
        turn.append_to_idle_ms = summary.append_to_idle_ms;
    }
    turn
}

impl Store {
    fn turn_summary_key(&self, session: SessionId, turn: MessageId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).turn_metrics(session, turn)
    }

    fn turn_inference_key(&self, session: SessionId, turn: MessageId, request_id: &str) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).turn_inference(session, turn, request_id)
    }

    fn turn_tool_key(&self, session: SessionId, turn: MessageId, call_id: &str) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).turn_tool(session, turn, call_id)
    }

    /// Merge several independent observations in one read-modify-write
    /// transaction. A dispatch folds its tool name into its stage, and a node
    /// completion folds its tool result and completion stage, so each costs
    /// one transaction; the first-tool computer re-sample follows in its own
    /// transaction from a spawned task so the completion never waits on the
    /// volume stat round trip. A `CommitUnknown`
    /// outcome is retried once, except for batches that contain a wait patch:
    /// wait patches increment retry and wait counters, so replaying them
    /// after a commit that actually landed would double-count. Stage,
    /// inference, tool, computer, and error patches are idempotent (anchors
    /// keep the first timestamp, inference keeps the maximum counters, tool
    /// and computer merges keep the first sample), so replaying those batches
    /// is safe.
    /// # Errors
    /// Returns database or encoding failures without changing the conversation.
    // One transaction reads the summary and touched rows, then writes them back; splitting would separate the atomic
    // read-modify-write the fence relies on.
    #[allow(clippy::too_many_lines)]
    pub async fn record_turn_metrics(
        &self,
        session: SessionId,
        turn: MessageId,
        patches: Vec<MetricPatch>,
    ) -> Result<()> {
        let summary_key = self.turn_summary_key(session, turn);
        // Collect the detail rows this batch touches so the transaction reads
        // only what it writes; a turn with hundreds of calls still commits
        // one small transaction per boundary.
        let mut inference_ids = BTreeSet::new();
        let mut tool_ids = BTreeSet::new();
        for patch in &patches {
            match patch {
                MetricPatch::Stage(event) => {
                    if let Some(id) = event.request_id {
                        let key = id.to_string();
                        match event.stage {
                            TurnStage::InferenceStarted
                            | TurnStage::InferenceFinished
                            | TurnStage::FirstToken => {
                                inference_ids.insert(key);
                            }
                            TurnStage::ToolDispatched | TurnStage::ToolCompleted => {
                                tool_ids.insert(key);
                            }
                            _ => {}
                        }
                    }
                }
                MetricPatch::Inference(update) => {
                    inference_ids.insert(update.request_id.clone());
                }
                MetricPatch::Tool(update) => {
                    tool_ids.insert(update.request_id.clone());
                }
                MetricPatch::Wait { request_id, .. } => {
                    inference_ids.insert(request_id.clone());
                }
                MetricPatch::Computer(_) | MetricPatch::Error(_) => {}
            }
        }
        let has_wait = patches
            .iter()
            .any(|patch| matches!(patch, MetricPatch::Wait { .. }));
        let mut attempts = 0;
        loop {
            let attempted = self
                .transaction(|trx| {
                    let summary_key = &summary_key;
                    let patches = &patches;
                    let inference_ids = &inference_ids;
                    let tool_ids = &tool_ids;
                    async move {
                        let mut state = TurnWrite {
                            summary: match trx.get(summary_key, false).await? {
                                None => StoredTurnSummaryCurrent {
                                    session_id: session.to_string(),
                                    turn_id: turn.to_string(),
                                    ..StoredTurnSummaryCurrent::default()
                                },
                                Some(value) => decode_summary(&value)?,
                            },
                            inference: BTreeMap::new(),
                            tools: BTreeMap::new(),
                        };
                        for id in inference_ids {
                            if !state.inference.contains_key(id) {
                                let key = self.turn_inference_key(session, turn, id);
                                if let Some(bytes) = trx.get(&key, false).await? {
                                    state
                                        .inference
                                        .insert(id.clone(), decode_inference(&bytes)?);
                                }
                            }
                        }
                        for id in tool_ids {
                            if !state.tools.contains_key(id) {
                                let key = self.turn_tool_key(session, turn, id);
                                if let Some(bytes) = trx.get(&key, false).await? {
                                    state.tools.insert(id.clone(), decode_tool(&bytes)?);
                                }
                            }
                        }
                        for patch in patches {
                            apply_one(&mut state, patch);
                        }
                        write(
                            &trx,
                            summary_key,
                            &StoredTurnMetrics::Summary(Box::new(state.summary.clone())),
                        )?;
                        for id in inference_ids {
                            if let Some(row) = state.inference.get(id) {
                                write(
                                    &trx,
                                    &self.turn_inference_key(session, turn, id),
                                    &StoredTurnMetrics::Inference(row.clone()),
                                )?;
                            }
                        }
                        for id in tool_ids {
                            if let Some(row) = state.tools.get(id) {
                                write(
                                    &trx,
                                    &self.turn_tool_key(session, turn, id),
                                    &StoredTurnMetrics::Tool(row.clone()),
                                )?;
                            }
                        }
                        Ok(())
                    }
                })
                .await;
            match attempted {
                // Wait batches are not replayed: the increment is not
                // idempotent, so a landed commit would double-count.
                Err(StoreError::Storage(crate::StorageError::CommitUnknown))
                    if attempts == 0 && !has_wait =>
                {
                    attempts += 1;
                }
                other => return other,
            }
        }
    }

    /// Spawn observability after the stage, without waiting on a turn's hot path.
    pub fn observe_turn_stage(&self, event: TurnEvent) {
        self.observe_turn_metric(event.session_id, event.turn_id, MetricPatch::Stage(event));
    }

    /// Spawn observability after the stage, without waiting on a turn's hot path.
    pub fn observe_turn_metric(&self, session: SessionId, turn: MessageId, patch: MetricPatch) {
        self.observe_turn_metrics(session, turn, vec![patch]);
    }

    /// Spawn one transaction for several patches that share a turn.
    pub fn observe_turn_metrics(
        &self,
        session: SessionId,
        turn: MessageId,
        patches: Vec<MetricPatch>,
    ) {
        let store = self.clone();
        tokio::spawn(async move {
            if let Err(error) = store.record_turn_metrics(session, turn, patches).await {
                tracing::warn!(%error, %session, %turn, "turn metric write failed");
            }
        });
    }

    async fn turn_rows(
        &self,
        session: SessionId,
        turn: MessageId,
    ) -> Result<(
        Vec<StoredTurnInferenceCurrent>,
        Vec<StoredToolMetricCurrent>,
    )> {
        let (inference_begin, inference_end) = crate::keys::Keys::new(&self.root)
            .turn_inference_space(session, turn)
            .range();
        let (tool_begin, tool_end) = crate::keys::Keys::new(&self.root)
            .turn_tool_space(session, turn)
            .range();
        let mut inference = Vec::new();
        let mut after: Option<Vec<u8>> = None;
        loop {
            let page = self
                .transaction(|trx| {
                    let (mut begin, end) = (inference_begin.clone(), inference_end.clone());
                    if let Some(cursor) = after.clone() {
                        begin = cursor;
                    }
                    async move { scan(&trx, (begin, end), MAX_SCAN_LIMIT).await }
                })
                .await?;
            let page_len = page.len();
            if page_len == 0 {
                break;
            }
            let mut next = page.last().map(|(key, _)| {
                let mut key = key.clone();
                key.push(0);
                key
            });
            for (_, bytes) in page {
                inference.push(decode_inference(&bytes)?);
            }
            after = next.take();
            if page_len < MAX_SCAN_LIMIT {
                break;
            }
        }
        let mut tools = Vec::new();
        let mut after: Option<Vec<u8>> = None;
        loop {
            let page = self
                .transaction(|trx| {
                    let (mut begin, end) = (tool_begin.clone(), tool_end.clone());
                    if let Some(cursor) = after.clone() {
                        begin = cursor;
                    }
                    async move { scan(&trx, (begin, end), MAX_SCAN_LIMIT).await }
                })
                .await?;
            let page_len = page.len();
            if page_len == 0 {
                break;
            }
            let mut next = page.last().map(|(key, _)| {
                let mut key = key.clone();
                key.push(0);
                key
            });
            for (_, bytes) in page {
                tools.push(decode_tool(&bytes)?);
            }
            after = next.take();
            if page_len < MAX_SCAN_LIMIT {
                break;
            }
        }
        Ok((inference, tools))
    }

    /// Read one turn's detail rows and attach their per-request timestamps as
    /// pseudo-stages so the shared derive computes whole-request throughput.
    async fn assemble_from_summary(
        &self,
        summary: StoredTurnSummaryCurrent,
        inference_limit: Option<usize>,
        tools_limit: Option<usize>,
    ) -> Result<TurnMetrics> {
        let session: SessionId = summary
            .session_id
            .parse()
            .map(SessionId::from_ulid)
            .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?;
        let turn: MessageId = summary
            .turn_id
            .parse()
            .map(MessageId::from_ulid)
            .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?;
        let (inference_rows, tool_rows) = self.turn_rows(session, turn).await?;
        Ok(assemble_turn(
            &summary,
            inference_rows,
            tool_rows,
            inference_limit,
            tools_limit,
        ))
    }

    /// Read a bounded page of per-turn records, ordered by turn id.
    /// Rows that fail to decode are skipped with a warning so one bad row
    /// never fails the whole page; storage and API types evolve independently.
    /// # Errors
    /// Returns database failures. Callers must still validate `limit` through
    /// the shared scan bound.
    pub async fn list_turn_metrics(
        &self,
        session: SessionId,
        after: Option<MessageId>,
        limit: usize,
    ) -> Result<Vec<TurnMetrics>> {
        self.list_turn_metrics_paged(session, after, limit, None, None)
            .await
    }

    /// Read a page of turns with paging on the per-turn arrays. Very long
    /// turns truncate their chronologically sorted `inference` and `tools`
    /// arrays to the given limits and report the remainder.
    /// dropped rows in the `dropped_*` counters; an absent limit returns the
    /// complete arrays.
    /// # Errors
    /// Returns database failures.
    pub async fn list_turn_metrics_paged(
        &self,
        session: SessionId,
        after: Option<MessageId>,
        limit: usize,
        inference_limit: Option<usize>,
        tools_limit: Option<usize>,
    ) -> Result<Vec<TurnMetrics>> {
        let raw = self
            .transaction(|trx| async move {
                let (mut begin, end) = crate::keys::Keys::new(&self.root)
                    .turn_metrics_space(session)
                    .range();
                if let Some(turn) = after {
                    begin = crate::keys::Keys::new(&self.root).turn_metrics(session, turn);
                    begin.push(0);
                }
                scan(&trx, (begin, end), limit).await
            })
            .await?;
        let mut summaries = Vec::new();
        for (_, bytes) in raw {
            match decode_summary(&bytes) {
                Ok(summary) => summaries.push(summary),
                Err(error) => tracing::warn!(%error, "skipping undecodable turn metric"),
            }
        }
        let mut turns = Vec::new();
        for summary in summaries {
            turns.push(
                self.assemble_from_summary(summary, inference_limit, tools_limit)
                    .await?,
            );
        }
        turns.sort_by(|a, b| a.turn_id.cmp(&b.turn_id));
        Ok(turns)
    }

    /// Most recent turns first, without paging the whole session. The rollup
    /// only needs the tail, so pages walk the key range in reverse. Only
    /// summary rows are read; token, wait, and throughput totals live on the
    /// summary so no per-request fan-out is needed.
    async fn recent_turn_summaries(
        &self,
        session: SessionId,
        since: Option<MessageId>,
        limit: usize,
    ) -> Result<Vec<StoredTurnSummaryCurrent>> {
        let mut summaries = Vec::new();
        let mut end = crate::keys::Keys::new(&self.root)
            .turn_metrics_space(session)
            .range()
            .1;
        let begin = match since {
            Some(turn) => {
                let mut key = crate::keys::Keys::new(&self.root).turn_metrics(session, turn);
                key.push(0);
                key
            }
            None => {
                crate::keys::Keys::new(&self.root)
                    .turn_metrics_space(session)
                    .range()
                    .0
            }
        };
        while summaries.len() < limit {
            let take = (limit - summaries.len()).min(MAX_SCAN_LIMIT);
            let raw = self
                .transaction(|trx| {
                    let (begin, end) = (begin.clone(), end.clone());
                    async move { scan_reverse(&trx, (begin, end), take).await }
                })
                .await?;
            if raw.is_empty() {
                break;
            }
            // Reverse pages arrive in descending key order; the last key of
            // the page is the exclusive end of the next (older) page.
            end = raw
                .last()
                .map_or_else(|| begin.clone(), |(key, _)| key.clone());
            for (_, bytes) in raw {
                match decode_summary(&bytes) {
                    Ok(summary) => summaries.push(summary),
                    Err(error) => tracing::warn!(%error, "skipping undecodable turn metric"),
                }
                if summaries.len() >= limit {
                    break;
                }
            }
        }
        // Collected newest-first; restore ascending order for the rollup.
        summaries.reverse();
        Ok(summaries)
    }

    /// Roll up at most `limit` of the most recent turns of the named agent's
    /// current main session, optionally after `since`. The 200-turn cap keeps
    /// the rollup bounded no matter how long the session runs. Only summary
    /// rows are scanned; per-request and per-tool rows are never read here.
    /// # Errors
    /// Returns agent lookup or database failures.
    // Aggregate rates and durations have only approximate floating-point precision.
    #[allow(clippy::cast_precision_loss)]
    pub async fn agent_turn_metrics(
        &self,
        agent: AgentId,
        limit: usize,
        since: Option<MessageId>,
    ) -> Result<AgentMetrics> {
        let record = self
            .get_agent(agent)
            .await?
            .ok_or(crate::StoreError::Domain(crate::DomainError::AgentMissing))?;
        let mut output = AgentMetrics {
            agent_id: agent.to_string(),
            main_session_id: record.main_session.map(|id| id.to_string()),
            ..AgentMetrics::default()
        };
        let Some(session) = record.main_session else {
            return Ok(output);
        };
        // A zero limit means the default tail; larger requests are capped.
        let limit = if limit == 0 { 200 } else { limit.min(200) };
        let mut latency: BTreeMap<&str, Vec<f64>> = BTreeMap::new();
        let page = self.recent_turn_summaries(session, since, limit).await?;
        let (mut throughput_sum, mut throughput_count) = (0.0, 0_u64);
        for turn in &page {
            output.turns += 1;
            for (name, value) in [
                ("append_to_first_token", turn.append_to_first_token_ms),
                ("inference", turn.inference_duration_ms),
                ("append_to_idle", turn.append_to_idle_ms),
                (
                    "placement",
                    turn.computer.as_ref().and_then(|c| c.placement_ms),
                ),
            ] {
                if let Some(value) = value {
                    latency.entry(name).or_default().push(value);
                }
            }
            output.errors += u64::from(turn.error.is_some());
            output.errors += turn.inference_errors;
            output.input_tokens += turn.input_tokens;
            output.cached_input_tokens += turn.cached_input_tokens;
            output.output_tokens += turn.output_tokens;
            output.reasoning_tokens += turn.reasoning_tokens;
            output.cost_micros += turn.cost_micros;
            output.retries += turn.retries;
            throughput_sum += turn.throughput_sum;
            throughput_count += turn.throughput_count;
        }
        // Mean throughput is the mean across requests, not turns; the summary
        // carries the sum and count so no per-request read is needed.
        if throughput_count > 0 {
            output.mean_output_tokens_per_second = Some(throughput_sum / throughput_count as f64);
        }
        output.latencies = latency
            .into_iter()
            .map(|(name, mut samples)| {
                samples.sort_by(f64::total_cmp);
                let percentile =
                    |p: usize| samples[(samples.len() * p / 100).min(samples.len() - 1)];
                (
                    name.to_owned(),
                    LatencyPercentiles {
                        p50_ms: percentile(50),
                        p95_ms: percentile(95),
                    },
                )
            })
            .collect();
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn hex_to_bytes(hex: &str) -> Vec<u8> {
        let hex: String = hex.chars().filter(|c| !c.is_whitespace()).collect();
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }

    /// Checked-in current summary bytes. Generated with
    /// `swarmy_core::encode(&StoredTurnMetrics::Summary(fixture_summary()))`;
    /// decoding them pins the unbounded layout.
    const CURRENT_SUMMARY_HEX: &str = "010001730174000180897a00000180b6dc050000000000000000000000000000000000000000000000000000000000";
    /// Checked-in current inference-row bytes for a single-chunk request.
    const CURRENT_INFERENCE_HEX: &str = "010101720466616b6508736372697074656400000400000000000100000000000000018092f401000180ade204";
    /// Checked-in current tool-row bytes.
    const CURRENT_TOOL_HEX: &str = "01020163046261736800000000000000";

    fn fixture_summary() -> StoredTurnSummaryCurrent {
        StoredTurnSummaryCurrent {
            session_id: "s".into(),
            turn_id: "t".into(),
            appended_ns: Some(1_000_000),
            idle_ns: Some(6_000_000),
            ..StoredTurnSummaryCurrent::default()
        }
    }

    fn fixture_inference() -> StoredTurnInferenceCurrent {
        StoredTurnInferenceCurrent {
            metric: StoredInferenceMetricCurrent {
                request_id: "r".into(),
                provider: "fake".into(),
                model: "scripted".into(),
                output_tokens: 4,
                streamed: Some(false),
                ..StoredInferenceMetricCurrent::default()
            },
            started_ns: Some(2_000_000),
            finished_ns: Some(5_000_000),
            ..StoredTurnInferenceCurrent::default()
        }
    }

    fn fixture_tool() -> StoredToolMetricCurrent {
        StoredToolMetricCurrent {
            request_id: "c".into(),
            name: "bash".into(),
            ..StoredToolMetricCurrent::default()
        }
    }

    #[test]
    fn detail_row_at_summary_key_is_corrupt() {
        let bytes = swarmy_core::encode(&StoredTurnMetrics::Tool(fixture_tool())).unwrap();
        assert!(matches!(
            decode_summary(&bytes),
            Err(StoreError::Storage(crate::StorageError::Corrupt))
        ));
    }

    #[test]
    fn versioned_envelope_decodes_checked_in_v2_bytes() {
        let summary = decode::<StoredTurnMetrics>(&hex_to_bytes(CURRENT_SUMMARY_HEX)).unwrap();
        let StoredTurnMetrics::Summary(decoded) = summary else {
            panic!("expected currentSummary");
        };
        assert_eq!(*decoded, fixture_summary());
        let inference = decode::<StoredTurnMetrics>(&hex_to_bytes(CURRENT_INFERENCE_HEX)).unwrap();
        let StoredTurnMetrics::Inference(row) = inference else {
            panic!("expected currentInference");
        };
        assert_eq!(row, fixture_inference());
        assert_eq!(row.metric.streamed, Some(false));
        let tool = decode::<StoredTurnMetrics>(&hex_to_bytes(CURRENT_TOOL_HEX)).unwrap();
        let StoredTurnMetrics::Tool(row) = tool else {
            panic!("expected currentTool");
        };
        assert_eq!(row, fixture_tool());
    }

    #[test]
    fn detail_rows_sort_chronologically_not_by_hash() {
        let row = |id: &str, started: Option<i64>| StoredTurnInferenceCurrent {
            metric: StoredInferenceMetricCurrent {
                request_id: id.into(),
                ..StoredInferenceMetricCurrent::default()
            },
            started_ns: started,
            ..StoredTurnInferenceCurrent::default()
        };
        // Ids chosen so hash order disagrees with time order; the later
        // request must still sort first when its start is earlier.
        let mut rows = vec![row("zzz", Some(3_000_000)), row("aaa", Some(2_000_000))];
        sort_inference_rows(&mut rows);
        assert_eq!(rows[0].metric.request_id, "aaa");
        assert_eq!(rows[1].metric.request_id, "zzz");
        // Rows without timestamps sort last with ties broken by id.
        let mut rows = vec![row("b", None), row("a", None)];
        sort_inference_rows(&mut rows);
        assert_eq!(rows[0].metric.request_id, "a");
        let tool = |id: &str, dispatched: Option<i64>| StoredToolMetricCurrent {
            request_id: id.into(),
            dispatched_ns: dispatched,
            ..StoredToolMetricCurrent::default()
        };
        let mut tools = vec![tool("zzz", Some(5)), tool("aaa", Some(1))];
        sort_tool_rows(&mut tools);
        assert_eq!(tools[0].request_id, "aaa");
    }

    #[test]
    fn v2_summary_and_rows_round_trip() {
        let summary = StoredTurnSummaryCurrent {
            session_id: "s".into(),
            turn_id: "t".into(),
            appended_ns: Some(1_000_000),
            idle_ns: Some(6_000_000),
            ..StoredTurnSummaryCurrent::default()
        };
        for value in [
            StoredTurnMetrics::Summary(Box::new(summary)),
            StoredTurnMetrics::Inference(StoredTurnInferenceCurrent {
                metric: StoredInferenceMetricCurrent {
                    request_id: "r".into(),
                    ..StoredInferenceMetricCurrent::default()
                },
                started_ns: Some(2_000_000),
                finished_ns: Some(5_000_000),
                ..StoredTurnInferenceCurrent::default()
            }),
            StoredTurnMetrics::Tool(StoredToolMetricCurrent {
                request_id: "c".into(),
                name: "bash".into(),
                ..StoredToolMetricCurrent::default()
            }),
        ] {
            let bytes = swarmy_core::encode(&value).unwrap();
            let decoded: StoredTurnMetrics = swarmy_core::decode(&bytes).unwrap();
            assert_eq!(decoded, value);
        }
    }

    #[test]
    fn unbounded_turn_keeps_every_row_and_derives_idle() {
        let session = SessionId::from_ulid(ulid::Ulid::nil());
        let turn = MessageId::from_ulid(ulid::Ulid::nil());
        let mut state = TurnWrite {
            summary: StoredTurnSummaryCurrent {
                session_id: session.to_string(),
                turn_id: turn.to_string(),
                ..StoredTurnSummaryCurrent::default()
            },
            inference: BTreeMap::new(),
            tools: BTreeMap::new(),
        };
        let event =
            |stage: TurnStage, request: Option<swarmy_core::RequestId>, ns: i128| TurnEvent {
                session_id: session,
                turn_id: turn,
                stage,
                request_id: request,
                clock_id: "boot".into(),
                monotonic_ns: u64::try_from(ns).unwrap_or(0),
                unix_ns: ns,
            };
        apply_one(
            &mut state,
            &MetricPatch::Stage(event(TurnStage::Appended, None, 1_000_000)),
        );
        for index in 0..200_u64 {
            let request = swarmy_core::RequestId::for_step(session, index + 1);
            apply_one(
                &mut state,
                &MetricPatch::Stage(event(
                    TurnStage::ToolDispatched,
                    Some(request),
                    i128::from(2_000_000 + index),
                )),
            );
            apply_one(
                &mut state,
                &MetricPatch::Tool(ToolMetric {
                    request_id: request.to_string(),
                    name: "bash".into(),
                    ..ToolMetric::default()
                }),
            );
        }
        for index in 0..100_u64 {
            let request = swarmy_core::RequestId::for_step(session, 1000 + index);
            apply_one(
                &mut state,
                &MetricPatch::Stage(event(
                    TurnStage::InferenceStarted,
                    Some(request),
                    i128::from(3_000_000 + index),
                )),
            );
            apply_one(
                &mut state,
                &MetricPatch::Inference(InferenceMetric {
                    request_id: request.to_string(),
                    provider: "fake".into(),
                    model: "scripted".into(),
                    output_tokens: 4,
                    ..InferenceMetric::default()
                }),
            );
            apply_one(
                &mut state,
                &MetricPatch::Stage(event(
                    TurnStage::InferenceFinished,
                    Some(request),
                    i128::from(4_000_000 + index),
                )),
            );
        }
        apply_one(
            &mut state,
            &MetricPatch::Stage(event(TurnStage::Idle, None, 6_000_000)),
        );
        assert_eq!(state.tools.len(), 200);
        assert_eq!(state.inference.len(), 100);
        assert_eq!(state.summary.append_to_idle_ms, Some(5.0));
    }

    #[test]
    fn throughput_covers_the_whole_request() {
        let mut turn = TurnMetrics::default();
        let row = |stage: &str, ns, request: Option<&str>| StageTiming {
            stage: stage.into(),
            request_id: request.map(str::to_owned),
            clock_id: "boot".into(),
            monotonic_ns: ns,
            unix_ns: i64::try_from(ns).unwrap(),
        };
        turn.stages.push(row("appended", 1_000_000_000, None));
        turn.stages
            .push(row("inference_started", 2_000_000_000, Some("r")));
        turn.stages
            .push(row("first_token", 3_000_000_000, Some("r")));
        // A single-chunk provider finishes a millisecond after the first
        // token; the old streaming-only throughput would divide by that
        // millisecond and report an absurd rate.
        turn.stages
            .push(row("inference_finished", 3_001_000_000, Some("r")));
        turn.stages.push(row("idle", 6_000_000_000, None));
        turn.inference.push(InferenceMetric {
            request_id: "r".into(),
            output_tokens: 363,
            ..InferenceMetric::default()
        });
        turn.derive();
        let request = &turn.inference[0];
        assert_eq!(request.request_duration_ms, Some(1001.0));
        // 363 tokens over the whole 1001 ms request, not over the 1 ms
        // streaming tail.
        let expected = 363.0 * 1000.0 / 1001.0;
        assert!((request.output_tokens_per_second.unwrap() - expected).abs() < 1.0);
        assert_eq!(turn.append_to_idle_ms, Some(5000.0));
        assert_eq!(InferenceMetric::tokens_per_second(0, 1.0), None);
        assert_eq!(InferenceMetric::tokens_per_second(1, 0.0), None);
    }

    #[test]
    fn batched_patches_equal_sequential_writes_and_bound_transactions() {
        // One dispatch folds name plus stage; one completion folds tool plus
        // stage. A turn with three tool calls costs six transactions (one
        // dispatch and one completion per call) instead of roughly nine
        // read-modify-write transactions per call; the first-tool computer
        // re-sample adds one spawned transaction per turn.
        let session = SessionId::from_ulid(ulid::Ulid::nil());
        let turn = MessageId::from_ulid(ulid::Ulid::nil());
        let dispatched = |request: swarmy_core::RequestId| TurnEvent {
            session_id: session,
            turn_id: turn,
            stage: TurnStage::ToolDispatched,
            request_id: Some(request),
            clock_id: "boot".into(),
            monotonic_ns: 1,
            unix_ns: 1,
        };
        let completed = |request: swarmy_core::RequestId| TurnEvent {
            session_id: session,
            turn_id: turn,
            stage: TurnStage::ToolCompleted,
            request_id: Some(request),
            clock_id: "boot".into(),
            monotonic_ns: 2,
            unix_ns: 2,
        };
        let apply_batch = |state: &mut TurnWrite, patches: Vec<MetricPatch>| {
            for patch in &patches {
                apply_one(state, patch);
            }
        };
        let mut batched = TurnWrite {
            summary: StoredTurnSummaryCurrent::default(),
            inference: BTreeMap::new(),
            tools: BTreeMap::new(),
        };
        let mut sequential = TurnWrite {
            summary: StoredTurnSummaryCurrent::default(),
            inference: BTreeMap::new(),
            tools: BTreeMap::new(),
        };
        let mut writes = 0;
        for (index, name) in ["a", "b", "c"].iter().enumerate() {
            let request =
                swarmy_core::RequestId::for_step(session, u64::try_from(index).unwrap() + 1);
            let dispatch = dispatch_patches(dispatched(request), name);
            assert_eq!(dispatch.len(), 2);
            let completion = completion_patches(
                ToolMetric {
                    request_id: request.to_string(),
                    exit_status: Some(0),
                    output_bytes: Some(1),
                    ..ToolMetric::default()
                },
                None,
                completed(request),
            );
            assert_eq!(completion.len(), 2);
            apply_batch(&mut batched, dispatch.clone());
            apply_batch(&mut batched, completion.clone());
            writes += 2;
            for patch in dispatch.into_iter().chain(completion) {
                apply_one(&mut sequential, &patch);
            }
        }
        assert_eq!(batched.tools, sequential.tools);
        assert_eq!(writes, 6);
        assert_eq!(batched.tools.len(), 3);
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::blob::MemoryBlobStore;
    use std::sync::{Arc, OnceLock};
    use swarmy_core::{RequestId, TurnStage};

    static NETWORK: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();

    #[tokio::test]
    // One turn with staged writes, inference, and idle checks needs its setup inline.
    #[allow(clippy::too_many_lines)]
    async fn incremental_records_merge_under_one_turn_key() {
        let Ok(cluster) = std::env::var("SWARMY_FDB_CLUSTER_FILE") else {
            return;
        };
        NETWORK.get_or_init(crate::boot);
        let path = vec![
            "turn-metrics-test".into(),
            ulid::Ulid::generate().to_string(),
        ];
        let store = Store::open(
            Some(&cluster),
            Some(&path),
            Arc::new(MemoryBlobStore::default()),
        )
        .await
        .unwrap();
        let session = SessionId::from_ulid(ulid::Ulid::generate());
        let turn = MessageId::from_ulid(ulid::Ulid::generate());
        let request = RequestId::for_step(session, 1);
        for (stage, ns) in [
            (TurnStage::Appended, 1_000_000),
            (TurnStage::InferenceStarted, 2_000_000),
            (TurnStage::FirstToken, 3_000_000),
            (TurnStage::InferenceFinished, 5_000_000),
            (TurnStage::Idle, 6_000_000),
        ] {
            store
                .record_turn_metrics(
                    session,
                    turn,
                    vec![MetricPatch::Stage(TurnEvent {
                        session_id: session,
                        turn_id: turn,
                        stage,
                        request_id: Some(request),
                        clock_id: "boot".into(),
                        monotonic_ns: ns,
                        unix_ns: i128::from(ns),
                    })],
                )
                .await
                .unwrap();
        }
        // The appended and idle anchors carry no request id in production;
        // record them that way so the wall-time derivation matches.
        store
            .record_turn_metrics(
                session,
                turn,
                vec![MetricPatch::Stage(TurnEvent {
                    session_id: session,
                    turn_id: turn,
                    stage: TurnStage::Appended,
                    request_id: None,
                    clock_id: "boot".into(),
                    monotonic_ns: 1_000_000,
                    unix_ns: 1_000_000,
                })],
            )
            .await
            .unwrap();
        store
            .record_turn_metrics(
                session,
                turn,
                vec![MetricPatch::Stage(TurnEvent {
                    session_id: session,
                    turn_id: turn,
                    stage: TurnStage::Idle,
                    request_id: None,
                    clock_id: "boot".into(),
                    monotonic_ns: 6_000_000,
                    unix_ns: 6_000_000,
                })],
            )
            .await
            .unwrap();
        store
            .record_turn_metrics(
                session,
                turn,
                vec![MetricPatch::Inference(InferenceMetric {
                    request_id: request.to_string(),
                    provider: "fake".into(),
                    model: "scripted".into(),
                    output_tokens: 10,
                    ..InferenceMetric::default()
                })],
            )
            .await
            .unwrap();
        let records = store.list_turn_metrics(session, None, 10).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].append_to_first_token_ms, Some(2.0));
        assert_eq!(records[0].inference_duration_ms, Some(3.0));
        assert_eq!(records[0].append_to_idle_ms, Some(5.0));
        assert_eq!(records[0].dropped_stages, 0);
        assert_eq!(records[0].dropped_inference, 0);
        assert_eq!(records[0].dropped_tools, 0);
        // Throughput now covers the whole request (2 ms to 5 ms), not the
        // streaming tail (3 ms to 5 ms).
        assert_eq!(records[0].inference[0].request_duration_ms, Some(3.0));
        assert_eq!(
            records[0].inference[0].output_tokens_per_second,
            Some(10_000.0 / 3.0)
        );
        assert!(
            store
                .list_turn_metrics(session, Some(turn), 10)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn batched_tool_patches_merge_in_one_transaction() {
        let Ok(cluster) = std::env::var("SWARMY_FDB_CLUSTER_FILE") else {
            return;
        };
        NETWORK.get_or_init(crate::boot);
        let path = vec![
            "turn-metrics-batch-test".into(),
            ulid::Ulid::generate().to_string(),
        ];
        let store = Store::open(
            Some(&cluster),
            Some(&path),
            Arc::new(MemoryBlobStore::default()),
        )
        .await
        .unwrap();
        let session = SessionId::from_ulid(ulid::Ulid::generate());
        let turn = MessageId::from_ulid(ulid::Ulid::generate());
        let request = RequestId::for_step(session, 7);
        let dispatched = TurnEvent {
            session_id: session,
            turn_id: turn,
            stage: TurnStage::ToolDispatched,
            request_id: Some(request),
            clock_id: "boot".into(),
            monotonic_ns: 1,
            unix_ns: 1,
        };
        let completed = TurnEvent {
            session_id: session,
            turn_id: turn,
            stage: TurnStage::ToolCompleted,
            request_id: Some(request),
            clock_id: "boot".into(),
            monotonic_ns: 2,
            unix_ns: 2,
        };
        store
            .record_turn_metrics(session, turn, dispatch_patches(dispatched, "bash"))
            .await
            .unwrap();
        store
            .record_turn_metrics(
                session,
                turn,
                completion_patches(
                    ToolMetric {
                        request_id: request.to_string(),
                        exit_status: Some(0),
                        output_bytes: Some(9),
                        process_wall_ms: Some(0.5),
                        ..ToolMetric::default()
                    },
                    None,
                    completed,
                ),
            )
            .await
            .unwrap();
        let records = store.list_turn_metrics(session, None, 10).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].tools.len(), 1);
        assert_eq!(records[0].tools[0].name, "bash");
        assert_eq!(records[0].tools[0].exit_status, Some(0));
    }

    #[tokio::test]
    // The required 200-tool, 100-request turn builds its rows inline.
    #[allow(clippy::too_many_lines)]
    async fn long_turn_with_two_hundred_tools_and_one_hundred_requests_reads_back_complete() {
        let Ok(cluster) = std::env::var("SWARMY_FDB_CLUSTER_FILE") else {
            return;
        };
        NETWORK.get_or_init(crate::boot);
        let path = vec![
            "turn-metrics-long-test".into(),
            ulid::Ulid::generate().to_string(),
        ];
        let store = Store::open(
            Some(&cluster),
            Some(&path),
            Arc::new(MemoryBlobStore::default()),
        )
        .await
        .unwrap();
        let session = SessionId::from_ulid(ulid::Ulid::generate());
        let turn = MessageId::from_ulid(ulid::Ulid::generate());
        let appended_ns: i128 = 1_000_000;
        let idle_ns: i128 = 2_000_000_000;
        store
            .record_turn_metrics(
                session,
                turn,
                vec![MetricPatch::Stage(TurnEvent {
                    session_id: session,
                    turn_id: turn,
                    stage: TurnStage::Appended,
                    request_id: None,
                    clock_id: "boot".into(),
                    monotonic_ns: 1_000_000,
                    unix_ns: appended_ns,
                })],
            )
            .await
            .unwrap();
        for index in 0..200_u64 {
            let request = RequestId::for_step(session, index + 1);
            store
                .record_turn_metrics(
                    session,
                    turn,
                    dispatch_patches(
                        TurnEvent {
                            session_id: session,
                            turn_id: turn,
                            stage: TurnStage::ToolDispatched,
                            request_id: Some(request),
                            clock_id: "boot".into(),
                            monotonic_ns: 1_000_001 + index,
                            unix_ns: appended_ns + i128::from(index + 1),
                        },
                        "bash",
                    ),
                )
                .await
                .unwrap();
            store
                .record_turn_metrics(
                    session,
                    turn,
                    completion_patches(
                        ToolMetric {
                            request_id: request.to_string(),
                            exit_status: Some(0),
                            output_bytes: Some(9),
                            ..ToolMetric::default()
                        },
                        None,
                        TurnEvent {
                            session_id: session,
                            turn_id: turn,
                            stage: TurnStage::ToolCompleted,
                            request_id: Some(request),
                            clock_id: "boot".into(),
                            monotonic_ns: 1_000_002 + index,
                            unix_ns: appended_ns + i128::from(index + 2),
                        },
                    ),
                )
                .await
                .unwrap();
        }
        for index in 0..100_u64 {
            let request = RequestId::for_step(session, 10_000 + index);
            store
                .record_turn_metrics(
                    session,
                    turn,
                    vec![MetricPatch::Stage(TurnEvent {
                        session_id: session,
                        turn_id: turn,
                        stage: TurnStage::InferenceStarted,
                        request_id: Some(request),
                        clock_id: "boot".into(),
                        monotonic_ns: 2_000_000 + index,
                        unix_ns: appended_ns + i128::from(500 + index),
                    })],
                )
                .await
                .unwrap();
            store
                .record_turn_metrics(
                    session,
                    turn,
                    vec![MetricPatch::Inference(InferenceMetric {
                        request_id: request.to_string(),
                        provider: "fake".into(),
                        model: "scripted".into(),
                        output_tokens: 4,
                        ..InferenceMetric::default()
                    })],
                )
                .await
                .unwrap();
            store
                .record_turn_metrics(
                    session,
                    turn,
                    vec![MetricPatch::Stage(TurnEvent {
                        session_id: session,
                        turn_id: turn,
                        stage: TurnStage::InferenceFinished,
                        request_id: Some(request),
                        clock_id: "boot".into(),
                        monotonic_ns: 3_000_000 + index,
                        unix_ns: appended_ns + i128::from(600 + index),
                    })],
                )
                .await
                .unwrap();
        }
        let idle_event = TurnEvent {
            session_id: session,
            turn_id: turn,
            stage: TurnStage::Idle,
            request_id: None,
            clock_id: "boot".into(),
            monotonic_ns: 4_000_000,
            unix_ns: idle_ns,
        };
        store
            .record_turn_metrics(session, turn, vec![MetricPatch::Stage(idle_event)])
            .await
            .unwrap();
        let records = store.list_turn_metrics(session, None, 10).await.unwrap();
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.tools.len(), 200);
        assert_eq!(record.inference.len(), 100);
        assert_eq!(record.dropped_stages, 0);
        assert_eq!(record.dropped_inference, 0);
        assert_eq!(record.dropped_tools, 0);
        // Nanosecond wall times exceed f64 integer precision; the millisecond check does not need it.
        #[allow(clippy::cast_precision_loss)]
        let expected_idle_ms = (idle_ns - appended_ns) as f64 / 1_000_000.0;
        assert_eq!(record.append_to_idle_ms, Some(expected_idle_ms));
        // Paging on the arrays truncates deterministically and reports the
        // remainder in the dropped counters.
        let paged = store
            .list_turn_metrics_paged(session, None, 10, Some(10), Some(20))
            .await
            .unwrap();
        assert_eq!(paged[0].inference.len(), 10);
        assert_eq!(paged[0].tools.len(), 20);
        assert_eq!(paged[0].dropped_inference, 90);
        assert_eq!(paged[0].dropped_tools, 180);
    }
}
