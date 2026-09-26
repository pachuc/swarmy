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
use swarmy_api_types::{
    AgentMetrics, ComputerMetric, InferenceMetric, LatencyPercentiles, StageTiming, ToolMetric,
    TurnMetrics,
};
use swarmy_core::{AgentId, MessageId, SessionId, TurnEvent, TurnStage};

use crate::{MAX_SCAN_LIMIT, Result, Store, StoreError, decode, scan, write};

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

/// First durable layout for a turn record. Field order is frozen: postcard is
/// positional, so `#[serde(default)]` cannot rescue a shorter row. New layouts
/// become `V2` and later variants of [`StoredTurnMetrics`]; old rows keep
/// decoding through the enum. See the compatibility test below, which decodes
/// checked-in `V1` bytes the way the session header test pins its layout.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct StoredTurnMetricsV1 {
    session_id: String,
    turn_id: String,
    stages: Vec<StageTiming>,
    inference: Vec<StoredInferenceMetricV1>,
    tools: Vec<ToolMetric>,
    computer: Option<ComputerMetric>,
    append_to_first_token_ms: Option<f64>,
    inference_duration_ms: Option<f64>,
    append_to_idle_ms: Option<f64>,
    error: Option<String>,
    dropped_stages: u64,
    dropped_inference: u64,
    dropped_tools: u64,
}

/// Frozen inference row for `V1`. The live [`InferenceMetric`] gained
/// `request_duration_ms` and `streamed` after `V1` was pinned, so `V1` keeps
/// its own copy with the original field order; postcard cannot skip the new
/// trailing fields when reading old bytes.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct StoredInferenceMetricV1 {
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
    output_tokens_per_second: Option<f64>,
    retries: u32,
    rate_limit_waits: u32,
    gateway_waits: u32,
    provider_failures: u32,
    error: Option<String>,
}

impl StoredInferenceMetricV1 {
    fn into_api(self) -> InferenceMetric {
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
            // Rows written before the whole-request throughput existed carry
            // no request interval; the reader recomputes it from stages.
            request_duration_ms: None,
            // Rows written before the flag existed carry no chunk
            // information; `None` keeps them out of the single-chunk count
            // so old turns are never mislabeled.
            streamed: None,
            output_tokens_per_second: self.output_tokens_per_second,
            retries: self.retries,
            rate_limit_waits: self.rate_limit_waits,
            gateway_waits: self.gateway_waits,
            provider_failures: self.provider_failures,
            error: self.error,
        }
    }

    fn from_api(value: &InferenceMetric) -> Self {
        Self {
            request_id: value.request_id.clone(),
            provider: value.provider.clone(),
            model: value.model.clone(),
            input_tokens: value.input_tokens,
            cached_input_tokens: value.cached_input_tokens,
            output_tokens: value.output_tokens,
            reasoning_tokens: value.reasoning_tokens,
            cost_micros: value.cost_micros,
            time_to_first_token_ms: value.time_to_first_token_ms,
            streaming_duration_ms: value.streaming_duration_ms,
            output_tokens_per_second: value.output_tokens_per_second,
            retries: value.retries,
            rate_limit_waits: value.rate_limit_waits,
            gateway_waits: value.gateway_waits,
            provider_failures: value.provider_failures,
            error: value.error.clone(),
        }
    }
}

/// Unbounded turn summary. Anchor timestamps are wall-clock nanoseconds so
/// durations stay comparable across hosts; the idle anchor is a dedicated
/// field so it can never be dropped the way the old 64-entry stage list
/// dropped it. Token, cost, wait, and throughput totals are maintained
/// incrementally so the agent rollup never fans out to per-request rows.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct StoredTurnSummaryV2 {
    session_id: String,
    turn_id: String,
    #[serde(default)]
    submitted_ns: Option<i64>,
    #[serde(default)]
    appended_ns: Option<i64>,
    #[serde(default)]
    first_token_ns: Option<i64>,
    #[serde(default)]
    first_tool_ns: Option<i64>,
    #[serde(default)]
    idle_ns: Option<i64>,
    #[serde(default)]
    inference_started_ns: Option<i64>,
    #[serde(default)]
    inference_finished_ns: Option<i64>,
    #[serde(default)]
    computer: Option<ComputerMetric>,
    #[serde(default)]
    append_to_first_token_ms: Option<f64>,
    #[serde(default)]
    inference_duration_ms: Option<f64>,
    #[serde(default)]
    append_to_idle_ms: Option<f64>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    input_tokens: u64,
    #[serde(default)]
    cached_input_tokens: u64,
    #[serde(default)]
    output_tokens: u64,
    #[serde(default)]
    reasoning_tokens: u64,
    #[serde(default)]
    cost_micros: u64,
    #[serde(default)]
    retries: u64,
    #[serde(default)]
    rate_limit_waits: u64,
    #[serde(default)]
    gateway_waits: u64,
    #[serde(default)]
    provider_failures: u64,
    #[serde(default)]
    inference_errors: u64,
    #[serde(default)]
    throughput_sum: f64,
    #[serde(default)]
    throughput_count: u64,
    /// Rows the legacy capped layout dropped before migration. Native `V2`
    /// turns always store zero here; migrated turns keep their original
    /// counters so old baseline sessions do not read as complete.
    #[serde(default)]
    dropped_stages: u64,
    #[serde(default)]
    dropped_inference: u64,
    #[serde(default)]
    dropped_tools: u64,
}

/// Frozen inference fields for one `V2` request row. This mirrors
/// [`InferenceMetric`] at the `V2` layout revision: postcard is positional, so
/// the API type cannot be embedded directly (the next field added to the API
/// type would make every stored row undecodable). Convert to and from the API
/// type with the helpers below, and add any future stored fields as trailing
/// fields through `swarmy_core::trailing` (see `swarmy_core::usage`), never by
/// inserting mid-struct.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct StoredInferenceMetricV2 {
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
    #[serde(default)]
    request_duration_ms: Option<f64>,
    #[serde(default)]
    streamed: Option<bool>,
    output_tokens_per_second: Option<f64>,
    retries: u32,
    rate_limit_waits: u32,
    gateway_waits: u32,
    provider_failures: u32,
    error: Option<String>,
}

impl StoredInferenceMetricV2 {
    fn into_api(self) -> InferenceMetric {
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

    fn from_api(value: &InferenceMetric) -> Self {
        Self {
            request_id: value.request_id.clone(),
            provider: value.provider.clone(),
            model: value.model.clone(),
            input_tokens: value.input_tokens,
            cached_input_tokens: value.cached_input_tokens,
            output_tokens: value.output_tokens,
            reasoning_tokens: value.reasoning_tokens,
            cost_micros: value.cost_micros,
            time_to_first_token_ms: value.time_to_first_token_ms,
            streaming_duration_ms: value.streaming_duration_ms,
            request_duration_ms: value.request_duration_ms,
            streamed: value.streamed,
            output_tokens_per_second: value.output_tokens_per_second,
            retries: value.retries,
            rate_limit_waits: value.rate_limit_waits,
            gateway_waits: value.gateway_waits,
            provider_failures: value.provider_failures,
            error: value.error.clone(),
        }
    }
}

/// Frozen tool fields for one `V2` tool row. Mirrors [`ToolMetric`] at the
/// `V2` layout revision for the same positional-encoding reason as
/// [`StoredInferenceMetricV2`]; future stored fields go last through
/// `swarmy_core::trailing`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct StoredToolMetricV2 {
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

impl StoredToolMetricV2 {
    fn into_api(self) -> ToolMetric {
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

    fn from_api(value: &ToolMetric) -> Self {
        Self {
            request_id: value.request_id.clone(),
            name: value.name.clone(),
            dispatched_ns: value.dispatched_ns,
            started_ns: value.started_ns,
            completed_ns: value.completed_ns,
            exit_status: value.exit_status,
            output_bytes: value.output_bytes,
            queue_ms: value.queue_ms,
            process_wall_ms: value.process_wall_ms,
        }
    }
}

/// One inference request row. Token counts and the streamed flag arrive with
/// the terminal patch; the stage path fills the wall-clock anchors that
/// throughput derives from.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct StoredTurnInferenceV2 {
    #[serde(default)]
    metric: StoredInferenceMetricV2,
    #[serde(default)]
    started_ns: Option<i64>,
    #[serde(default)]
    first_token_ns: Option<i64>,
    #[serde(default)]
    finished_ns: Option<i64>,
}

/// Versioned storage envelope. New variants are appended so old tags retain
/// their meaning; new readers read old variants while old readers reject
/// unknown ones. `V1` is the complete capped record. `V2Summary`,
/// `V2Inference`, and `V2Tool` are the unbounded rows keyed by their own
/// prefixes; the public `TurnMetrics` API type is assembled from them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum StoredTurnMetrics {
    V1(StoredTurnMetricsV1),
    V2Summary(StoredTurnSummaryV2),
    V2Inference(StoredTurnInferenceV2),
    V2Tool(StoredToolMetricV2),
}

impl StoredTurnMetrics {
    fn into_api(self) -> TurnMetrics {
        match self {
            Self::V1(inner) => TurnMetrics {
                session_id: inner.session_id,
                turn_id: inner.turn_id,
                stages: inner.stages,
                inference: inner
                    .inference
                    .into_iter()
                    .map(StoredInferenceMetricV1::into_api)
                    .collect(),
                tools: inner.tools,
                computer: inner.computer,
                append_to_first_token_ms: inner.append_to_first_token_ms,
                inference_duration_ms: inner.inference_duration_ms,
                append_to_idle_ms: inner.append_to_idle_ms,
                error: inner.error,
                dropped_stages: inner.dropped_stages,
                dropped_inference: inner.dropped_inference,
                dropped_tools: inner.dropped_tools,
            },
            // Detail rows are never decoded as whole turns; reaching here
            // means the caller read the wrong keyspace.
            Self::V2Summary(_) | Self::V2Inference(_) | Self::V2Tool(_) => {
                unreachable!("detail rows are never decoded as whole turns")
            }
        }
    }

    fn from_api(value: TurnMetrics) -> Self {
        Self::V1(StoredTurnMetricsV1 {
            session_id: value.session_id,
            turn_id: value.turn_id,
            stages: value.stages,
            inference: value
                .inference
                .iter()
                .map(StoredInferenceMetricV1::from_api)
                .collect(),
            tools: value.tools,
            computer: value.computer,
            append_to_first_token_ms: value.append_to_first_token_ms,
            inference_duration_ms: value.inference_duration_ms,
            append_to_idle_ms: value.append_to_idle_ms,
            error: value.error,
            dropped_stages: value.dropped_stages,
            dropped_inference: value.dropped_inference,
            dropped_tools: value.dropped_tools,
        })
    }
}

/// Decode a summary key. Returns the summary when the row is `V2`, the
/// migrated legacy record when the row is still `V1`, or an error the caller
/// skips with a warning.
enum DecodedSummary {
    V2(StoredTurnSummaryV2),
    Legacy(TurnMetrics),
}

fn decode_summary(bytes: &[u8]) -> Result<DecodedSummary> {
    if let Ok(record) = decode::<StoredTurnMetrics>(bytes) {
        match record {
            StoredTurnMetrics::V2Summary(summary) => return Ok(DecodedSummary::V2(summary)),
            StoredTurnMetrics::V1(inner) => {
                return Ok(DecodedSummary::Legacy(
                    StoredTurnMetrics::V1(inner).into_api(),
                ));
            }
            StoredTurnMetrics::V2Inference(_) | StoredTurnMetrics::V2Tool(_) => {
                return Err(StoreError::Corrupt);
            }
        }
    }
    // Bare `V1` rows predate the envelope.
    if let Ok(legacy) = decode::<StoredTurnMetricsV1>(bytes) {
        return Ok(DecodedSummary::Legacy(
            StoredTurnMetrics::V1(legacy).into_api(),
        ));
    }
    match decode::<TurnMetrics>(bytes) {
        Ok(record) => Ok(DecodedSummary::Legacy(
            StoredTurnMetrics::from_api(record).into_api(),
        )),
        Err(first) => Err(StoreError::from(first)),
    }
}

fn decode_inference(bytes: &[u8]) -> Result<StoredTurnInferenceV2> {
    if let Ok(StoredTurnMetrics::V2Inference(row)) = decode::<StoredTurnMetrics>(bytes) {
        return Ok(row);
    }
    if let Ok(row) = decode::<StoredTurnInferenceV2>(bytes) {
        return Ok(row);
    }
    Err(StoreError::Corrupt)
}

fn decode_tool(bytes: &[u8]) -> Result<StoredToolMetricV2> {
    if let Ok(StoredTurnMetrics::V2Tool(row)) = decode::<StoredTurnMetrics>(bytes) {
        return Ok(row);
    }
    if let Ok(row) = decode::<ToolMetric>(bytes) {
        return Ok(StoredToolMetricV2::from_api(&row));
    }
    Err(StoreError::Corrupt)
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

fn derive_summary(summary: &mut StoredTurnSummaryV2) {
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
fn derive_inference(row: &mut StoredTurnInferenceV2) {
    let metric = &mut row.metric;
    metric.time_to_first_token_ms = wall_ms(row.started_ns, row.first_token_ns);
    metric.streaming_duration_ms = wall_ms(row.first_token_ns, row.finished_ns);
    metric.request_duration_ms = wall_ms(row.started_ns, row.finished_ns);
    metric.output_tokens_per_second = metric
        .request_duration_ms
        .or(metric.streaming_duration_ms)
        .and_then(|ms| InferenceMetric::tokens_per_second(metric.output_tokens, ms));
}

fn apply_computer(summary: &mut StoredTurnSummaryV2, value: &ComputerMetric) {
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
    summary: StoredTurnSummaryV2,
    inference: BTreeMap<String, StoredTurnInferenceV2>,
    tools: BTreeMap<String, StoredToolMetricV2>,
    /// Rows migrated from a legacy `V1` record that must be written even
    /// when this batch does not touch them.
    migrated_inference: BTreeMap<String, StoredTurnInferenceV2>,
    migrated_tools: BTreeMap<String, StoredToolMetricV2>,
}

fn adjust_throughput(summary: &mut StoredTurnSummaryV2, old: Option<f64>, new: Option<f64>) {
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
    inference: &'a mut BTreeMap<String, StoredTurnInferenceV2>,
    request_id: &str,
) -> &'a mut StoredTurnInferenceV2 {
    inference
        .entry(request_id.to_owned())
        .or_insert_with(|| StoredTurnInferenceV2 {
            metric: StoredInferenceMetricV2 {
                request_id: request_id.to_owned(),
                ..StoredInferenceMetricV2::default()
            },
            ..StoredTurnInferenceV2::default()
        })
}

fn tool_entry<'a>(
    tools: &'a mut BTreeMap<String, StoredToolMetricV2>,
    request_id: &str,
) -> &'a mut StoredToolMetricV2 {
    tools
        .entry(request_id.to_owned())
        .or_insert_with(|| StoredToolMetricV2 {
            request_id: request_id.to_owned(),
            ..StoredToolMetricV2::default()
        })
}

// One merge updates row, summary totals, and throughput together; splitting
// would separate the delta bookkeeping from the row it deltas against.
#[allow(clippy::too_many_lines)]
fn apply_inference_update(
    summary: &mut StoredTurnSummaryV2,
    inference: &mut BTreeMap<String, StoredTurnInferenceV2>,
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

fn apply_tool_update(tools: &mut BTreeMap<String, StoredToolMetricV2>, update: &ToolMetric) {
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
    summary: &mut StoredTurnSummaryV2,
    inference: &mut BTreeMap<String, StoredTurnInferenceV2>,
    tools: &mut BTreeMap<String, StoredToolMetricV2>,
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
    summary: &mut StoredTurnSummaryV2,
    inference: &mut BTreeMap<String, StoredTurnInferenceV2>,
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
        migrated_inference: _,
        migrated_tools: _,
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

/// Convert a legacy capped record into an unbounded summary plus rows.
fn migrate_legacy(
    record: TurnMetrics,
) -> (
    StoredTurnSummaryV2,
    Vec<StoredTurnInferenceV2>,
    Vec<StoredToolMetricV2>,
) {
    let mut summary = StoredTurnSummaryV2 {
        session_id: record.session_id.clone(),
        turn_id: record.turn_id.clone(),
        computer: record.computer.clone(),
        error: record.error.clone(),
        ..StoredTurnSummaryV2::default()
    };
    let stage_time = |name: &str, request: Option<&str>| {
        record
            .stages
            .iter()
            .filter(|row| row.stage == name && row.request_id.as_deref() == request)
            .map(|row| row.unix_ns)
            .next()
    };
    let stage_min = |name: &str| {
        record
            .stages
            .iter()
            .filter(|row| row.stage == name)
            .map(|row| row.unix_ns)
            .min()
    };
    let stage_max = |name: &str| {
        record
            .stages
            .iter()
            .filter(|row| row.stage == name)
            .map(|row| row.unix_ns)
            .max()
    };
    summary.submitted_ns = stage_min("submitted");
    summary.appended_ns = stage_min("appended");
    summary.first_token_ns = stage_min("first_token");
    summary.idle_ns = stage_min("idle");
    summary.inference_started_ns = stage_min("inference_started");
    summary.inference_finished_ns = stage_max("inference_finished");
    summary.first_tool_ns = stage_min("tool_dispatched");
    // The legacy caps dropped rows the unbounded layout keeps; carry the
    // counters so migrated baseline sessions still report what was lost.
    summary.dropped_stages = record.dropped_stages;
    summary.dropped_inference = record.dropped_inference;
    summary.dropped_tools = record.dropped_tools;
    let mut inference_rows = Vec::new();
    for metric in record.inference {
        let id = metric.request_id.clone();
        let mut row = StoredTurnInferenceV2 {
            metric: StoredInferenceMetricV2::from_api(&metric),
            started_ns: stage_time("inference_started", Some(&id)),
            first_token_ns: stage_time("first_token", Some(&id)),
            finished_ns: stage_time("inference_finished", Some(&id)),
        };
        derive_inference(&mut row);
        summary.input_tokens += row.metric.input_tokens;
        summary.cached_input_tokens += row.metric.cached_input_tokens;
        summary.output_tokens += row.metric.output_tokens;
        summary.reasoning_tokens += row.metric.reasoning_tokens;
        summary.cost_micros += row.metric.cost_micros;
        summary.retries += u64::from(row.metric.retries);
        summary.rate_limit_waits += u64::from(row.metric.rate_limit_waits);
        summary.gateway_waits += u64::from(row.metric.gateway_waits);
        summary.provider_failures += u64::from(row.metric.provider_failures);
        if row.metric.error.is_some() {
            summary.inference_errors += 1;
        }
        if let Some(rate) = row.metric.output_tokens_per_second {
            summary.throughput_sum += rate;
            summary.throughput_count += 1;
        }
        inference_rows.push(row);
    }
    derive_summary(&mut summary);
    let tool_rows = record
        .tools
        .into_iter()
        .map(|tool| StoredToolMetricV2::from_api(&tool))
        .collect();
    (summary, inference_rows, tool_rows)
}

/// Bounded reverse scan for newest-first pagination. Mirrors the forward
/// [`scan`](crate::scan) bound so rollups stay under transaction limits.
async fn scan_reverse(
    trx: &Transaction,
    range: (Vec<u8>, Vec<u8>),
    limit: usize,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    if !(1..=MAX_SCAN_LIMIT).contains(&limit) {
        return Err(crate::StoreError::InvalidLimit);
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

/// Turn anchors shared by the `V2` and migrated-legacy assembly paths.
fn anchor_stages(summary: &StoredTurnSummaryV2) -> Vec<StageTiming> {
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
fn sort_inference_rows(rows: &mut [StoredTurnInferenceV2]) {
    rows.sort_by(|a, b| {
        (a.started_ns.unwrap_or(i64::MAX), &a.metric.request_id)
            .cmp(&(b.started_ns.unwrap_or(i64::MAX), &b.metric.request_id))
    });
}

/// Chronological order for per-tool rows by dispatch, then start, then id.
fn sort_tool_rows(rows: &mut [StoredToolMetricV2]) {
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
/// remainder plus any legacy dropped counters in `dropped_*`; a complete
/// read reports only the legacy remainder (zero for native `V2` turns).
fn assemble_turn(
    summary: &StoredTurnSummaryV2,
    mut inference_rows: Vec<StoredTurnInferenceV2>,
    mut tool_rows: Vec<StoredToolMetricV2>,
    inference_limit: Option<usize>,
    tools_limit: Option<usize>,
) -> TurnMetrics {
    let mut stages = anchor_stages(summary);
    sort_inference_rows(&mut inference_rows);
    sort_tool_rows(&mut tool_rows);
    let total_inference = inference_rows.len();
    let total_tools = tool_rows.len();
    let inference_page: Vec<StoredTurnInferenceV2> = match inference_limit {
        Some(limit) => inference_rows.into_iter().take(limit).collect(),
        None => inference_rows,
    };
    let tools_page: Vec<StoredToolMetricV2> = match tools_limit {
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
            .map(|row| row.metric.into_api())
            .collect(),
        tools: tools_page
            .into_iter()
            .map(StoredToolMetricV2::into_api)
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
        self.root.pack(&(
            "turn_metrics",
            session.as_ulid().to_bytes().as_slice(),
            turn.as_ulid().to_bytes().as_slice(),
        ))
    }

    fn turn_inference_key(&self, session: SessionId, turn: MessageId, request_id: &str) -> Vec<u8> {
        self.root.pack(&(
            "turn_inference",
            session.as_ulid().to_bytes().as_slice(),
            turn.as_ulid().to_bytes().as_slice(),
            request_id.as_bytes(),
        ))
    }

    fn turn_tool_key(&self, session: SessionId, turn: MessageId, call_id: &str) -> Vec<u8> {
        self.root.pack(&(
            "turn_tool",
            session.as_ulid().to_bytes().as_slice(),
            turn.as_ulid().to_bytes().as_slice(),
            call_id.as_bytes(),
        ))
    }

    /// Merge one independent observation after its boundary has completed.
    /// # Errors
    /// Returns database or encoding failures without changing the conversation.
    pub async fn record_turn_metric(
        &self,
        session: SessionId,
        turn: MessageId,
        patch: MetricPatch,
    ) -> Result<()> {
        self.record_turn_metrics(session, turn, vec![patch]).await
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
    // One transaction reads the summary, the touched rows, and any migrated
    // legacy rows, then writes them back; splitting would separate the atomic
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
                        let mut state = match trx.get(summary_key, false).await? {
                            None => TurnWrite {
                                summary: StoredTurnSummaryV2 {
                                    session_id: session.to_string(),
                                    turn_id: turn.to_string(),
                                    ..StoredTurnSummaryV2::default()
                                },
                                inference: BTreeMap::new(),
                                tools: BTreeMap::new(),
                                migrated_inference: BTreeMap::new(),
                                migrated_tools: BTreeMap::new(),
                            },
                            Some(value) => match decode_summary(&value)? {
                                DecodedSummary::V2(summary) => TurnWrite {
                                    summary,
                                    inference: BTreeMap::new(),
                                    tools: BTreeMap::new(),
                                    migrated_inference: BTreeMap::new(),
                                    migrated_tools: BTreeMap::new(),
                                },
                                DecodedSummary::Legacy(record) => {
                                    let (summary, rows, tools) = migrate_legacy(record);
                                    let mut state = TurnWrite {
                                        summary,
                                        inference: BTreeMap::new(),
                                        tools: BTreeMap::new(),
                                        migrated_inference: BTreeMap::new(),
                                        migrated_tools: BTreeMap::new(),
                                    };
                                    for row in rows {
                                        state
                                            .migrated_inference
                                            .insert(row.metric.request_id.clone(), row);
                                    }
                                    for tool in tools {
                                        state.migrated_tools.insert(tool.request_id.clone(), tool);
                                    }
                                    state
                                }
                            },
                        };
                        // Seed the write map with migrated rows and the rows
                        // this batch touches.
                        for (id, row) in &state.migrated_inference {
                            state
                                .inference
                                .entry(id.clone())
                                .or_insert_with(|| row.clone());
                        }
                        for (id, row) in &state.migrated_tools {
                            state.tools.entry(id.clone()).or_insert_with(|| row.clone());
                        }
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
                            &StoredTurnMetrics::V2Summary(state.summary.clone()),
                        )?;
                        // Migrated rows land even when untouched so the legacy
                        // summary is never the only copy of their data.
                        for (id, row) in &state.migrated_inference {
                            if !inference_ids.contains(id) {
                                let key = self.turn_inference_key(session, turn, id);
                                if trx.get(&key, false).await?.is_none() {
                                    write(
                                        &trx,
                                        &key,
                                        &StoredTurnMetrics::V2Inference(row.clone()),
                                    )?;
                                }
                            }
                        }
                        for (id, row) in &state.migrated_tools {
                            if !tool_ids.contains(id) {
                                let key = self.turn_tool_key(session, turn, id);
                                if trx.get(&key, false).await?.is_none() {
                                    write(&trx, &key, &StoredTurnMetrics::V2Tool(row.clone()))?;
                                }
                            }
                        }
                        for id in inference_ids {
                            if let Some(row) = state.inference.get(id) {
                                write(
                                    &trx,
                                    &self.turn_inference_key(session, turn, id),
                                    &StoredTurnMetrics::V2Inference(row.clone()),
                                )?;
                            }
                        }
                        for id in tool_ids {
                            if let Some(row) = state.tools.get(id) {
                                write(
                                    &trx,
                                    &self.turn_tool_key(session, turn, id),
                                    &StoredTurnMetrics::V2Tool(row.clone()),
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
                Err(StoreError::CommitUnknown) if attempts == 0 && !has_wait => attempts += 1,
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
    ) -> Result<(Vec<StoredTurnInferenceV2>, Vec<StoredToolMetricV2>)> {
        let (inference_begin, inference_end) = self
            .root
            .subspace(&(
                "turn_inference",
                session.as_ulid().to_bytes().as_slice(),
                turn.as_ulid().to_bytes().as_slice(),
            ))
            .range();
        let (tool_begin, tool_end) = self
            .root
            .subspace(&(
                "turn_tool",
                session.as_ulid().to_bytes().as_slice(),
                turn.as_ulid().to_bytes().as_slice(),
            ))
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
                match decode_inference(&bytes) {
                    Ok(row) => inference.push(row),
                    Err(error) => tracing::warn!(%error, "skipping undecodable turn inference"),
                }
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
                match decode_tool(&bytes) {
                    Ok(row) => tools.push(row),
                    Err(error) => tracing::warn!(%error, "skipping undecodable turn tool"),
                }
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
        summary: StoredTurnSummaryV2,
        inference_limit: Option<usize>,
        tools_limit: Option<usize>,
    ) -> Result<TurnMetrics> {
        let session: SessionId = summary
            .session_id
            .parse()
            .map(SessionId::from_ulid)
            .map_err(|_| StoreError::Corrupt)?;
        let turn: MessageId = summary
            .turn_id
            .parse()
            .map(MessageId::from_ulid)
            .map_err(|_| StoreError::Corrupt)?;
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
    /// arrays to the given limits and report the remainder plus any legacy
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
                let (mut begin, end) = self
                    .root
                    .subspace(&("turn_metrics", session.as_ulid().to_bytes().as_slice()))
                    .range();
                if let Some(turn) = after {
                    begin = self.root.pack(&(
                        "turn_metrics",
                        session.as_ulid().to_bytes().as_slice(),
                        turn.as_ulid().to_bytes().as_slice(),
                    ));
                    begin.push(0);
                }
                scan(&trx, (begin, end), limit).await
            })
            .await?;
        let mut summaries = Vec::new();
        let mut legacy_turns: BTreeMap<String, TurnMetrics> = BTreeMap::new();
        for (_, bytes) in raw {
            match decode_summary(&bytes) {
                Ok(DecodedSummary::V2(summary)) => summaries.push(summary),
                Ok(DecodedSummary::Legacy(record)) => {
                    // Turns written before the unbounded layout have no detail
                    // rows; serve this page from the migrated record itself.
                    // The next write to the turn persists the migrated rows.
                    let (summary, rows, tools) = migrate_legacy(record);
                    let turn = assemble_turn(&summary, rows, tools, inference_limit, tools_limit);
                    legacy_turns.insert(turn.turn_id.clone(), turn);
                }
                Err(error) => {
                    tracing::warn!(%error, "skipping undecodable turn metric");
                }
            }
        }
        let mut turns = Vec::new();
        for summary in summaries {
            // A legacy turn served above shares the summary keyspace; prefer
            // the already-assembled record when both exist (the migration has
            // not been written yet, so the summary key still holds V1 bytes
            // only for turns in `legacy_turns`).
            turns.push(
                self.assemble_from_summary(summary, inference_limit, tools_limit)
                    .await?,
            );
        }
        // Legacy turns sort with the V2 turns by turn id.
        turns.extend(legacy_turns.into_values());
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
    ) -> Result<Vec<StoredTurnSummaryV2>> {
        let mut summaries = Vec::new();
        let mut end = self
            .root
            .subspace(&("turn_metrics", session.as_ulid().to_bytes().as_slice()))
            .range()
            .1;
        let begin = match since {
            Some(turn) => {
                let mut key = self.root.pack(&(
                    "turn_metrics",
                    session.as_ulid().to_bytes().as_slice(),
                    turn.as_ulid().to_bytes().as_slice(),
                ));
                key.push(0);
                key
            }
            None => {
                self.root
                    .subspace(&("turn_metrics", session.as_ulid().to_bytes().as_slice()))
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
                    Ok(DecodedSummary::V2(summary)) => summaries.push(summary),
                    Ok(DecodedSummary::Legacy(record)) => {
                        let (summary, _, _) = migrate_legacy(record);
                        summaries.push(summary);
                    }
                    Err(error) => {
                        tracing::warn!(%error, "skipping undecodable turn metric");
                    }
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
            .ok_or(crate::StoreError::AgentMissing)?;
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
    /// Checked-in `V1` envelope bytes for a fixed record. Generated once with
    /// `swarmy_core::encode(&StoredTurnMetrics::V1(fixture_v1()))`; decoding
    /// them pins the `FoundationDB` layout the way the session header test pins
    /// its tuple encoding. New layouts add enum variants; this row must keep
    /// decoding.
    const V1_ENVELOPE_HEX: &str = "0100017301740108617070656e6465640004626f6f74c0843d80897a0101720466616b6508736372697074656400000400000000000000000000000000000000010203";
    /// Checked-in bare-struct bytes from before the envelope existed. The
    /// current reader keeps these rows visible after the layout change.
    const V1_BARE_HEX: &str = "01017301740108617070656e6465640004626f6f74c0843d80897a0101720466616b6508736372697074656400000400000000000000000000000000000000010203";

    fn fixture_v1() -> StoredTurnMetricsV1 {
        StoredTurnMetricsV1 {
            session_id: "s".into(),
            turn_id: "t".into(),
            stages: vec![StageTiming {
                stage: "appended".into(),
                request_id: None,
                clock_id: "boot".into(),
                monotonic_ns: 1_000_000,
                unix_ns: 1_000_000,
            }],
            inference: vec![StoredInferenceMetricV1 {
                request_id: "r".into(),
                provider: "fake".into(),
                model: "scripted".into(),
                output_tokens: 4,
                ..StoredInferenceMetricV1::default()
            }],
            dropped_stages: 1,
            dropped_inference: 2,
            dropped_tools: 3,
            ..StoredTurnMetricsV1::default()
        }
    }

    fn hex_to_bytes(hex: &str) -> Vec<u8> {
        let hex: String = hex.chars().filter(|c| !c.is_whitespace()).collect();
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }

    /// Checked-in `V2` summary bytes. Generated with
    /// `swarmy_core::encode(&StoredTurnMetrics::V2Summary(fixture_summary()))`;
    /// decoding them pins the unbounded layout the way the `V1` fixtures pin
    /// the capped record.
    const V2_SUMMARY_HEX: &str = "010101730174000180897a00000180b6dc050000000000000000000000000000000000000000000000000000000000";
    /// Checked-in `V2` inference-row bytes for a single-chunk request.
    const V2_INFERENCE_HEX: &str = "010201720466616b6508736372697074656400000400000000000100000000000000018092f401000180ade204";
    /// Checked-in `V2` tool-row bytes.
    const V2_TOOL_HEX: &str = "01030163046261736800000000000000";

    fn fixture_summary() -> StoredTurnSummaryV2 {
        StoredTurnSummaryV2 {
            session_id: "s".into(),
            turn_id: "t".into(),
            appended_ns: Some(1_000_000),
            idle_ns: Some(6_000_000),
            ..StoredTurnSummaryV2::default()
        }
    }

    fn fixture_inference() -> StoredTurnInferenceV2 {
        StoredTurnInferenceV2 {
            metric: StoredInferenceMetricV2 {
                request_id: "r".into(),
                provider: "fake".into(),
                model: "scripted".into(),
                output_tokens: 4,
                streamed: Some(false),
                ..StoredInferenceMetricV2::default()
            },
            started_ns: Some(2_000_000),
            finished_ns: Some(5_000_000),
            ..StoredTurnInferenceV2::default()
        }
    }

    fn fixture_tool() -> StoredToolMetricV2 {
        StoredToolMetricV2 {
            request_id: "c".into(),
            name: "bash".into(),
            ..StoredToolMetricV2::default()
        }
    }

    #[test]
    fn versioned_envelope_decodes_checked_in_v1_bytes() {
        if V1_ENVELOPE_HEX.starts_with("PLACEHOLDER") || V1_BARE_HEX.starts_with("PLACEHOLDER") {
            return;
        }
        for hex in [V1_ENVELOPE_HEX, V1_BARE_HEX] {
            // The fixture pins the production summary-key path: envelope and
            // bare bytes both decode through `decode_summary` as legacy turns.
            let DecodedSummary::Legacy(record) = decode_summary(&hex_to_bytes(hex)).unwrap() else {
                panic!("expected legacy V1 turn");
            };
            assert_eq!(record.session_id, "s");
            assert_eq!(record.turn_id, "t");
            assert_eq!(record.dropped_stages, 1);
            assert_eq!(record.dropped_inference, 2);
            assert_eq!(record.dropped_tools, 3);
            assert_eq!(record.inference[0].provider, "fake");
        }
        // The envelope adds a discriminant, so its bytes differ from the bare
        // struct they supersede.
        assert_ne!(V1_ENVELOPE_HEX, V1_BARE_HEX);
    }

    #[test]
    fn stored_layout_round_trips_and_converts_to_the_api_type() {
        let stored = StoredTurnMetrics::from_api(StoredTurnMetrics::V1(fixture_v1()).into_api());
        let bytes = swarmy_core::encode(&stored).unwrap();
        let decoded: StoredTurnMetrics = swarmy_core::decode(&bytes).unwrap();
        assert_eq!(decoded, stored);
        assert_eq!(swarmy_core::encode(&decoded).unwrap(), bytes);
        let api = decoded.into_api();
        assert_eq!(api.session_id, "s");
        assert_eq!(api.dropped_stages, 1);
        assert_eq!(api.dropped_inference, 2);
        assert_eq!(api.dropped_tools, 3);
    }

    #[test]
    fn versioned_envelope_decodes_checked_in_v2_bytes() {
        let summary = decode::<StoredTurnMetrics>(&hex_to_bytes(V2_SUMMARY_HEX)).unwrap();
        let StoredTurnMetrics::V2Summary(decoded) = summary else {
            panic!("expected V2Summary");
        };
        assert_eq!(decoded, fixture_summary());
        let inference = decode::<StoredTurnMetrics>(&hex_to_bytes(V2_INFERENCE_HEX)).unwrap();
        let StoredTurnMetrics::V2Inference(row) = inference else {
            panic!("expected V2Inference");
        };
        assert_eq!(row, fixture_inference());
        assert_eq!(row.metric.streamed, Some(false));
        let tool = decode::<StoredTurnMetrics>(&hex_to_bytes(V2_TOOL_HEX)).unwrap();
        let StoredTurnMetrics::V2Tool(row) = tool else {
            panic!("expected V2Tool");
        };
        assert_eq!(row, fixture_tool());
        // Bare frozen rows decode the same way as their enveloped form.
        let bare: StoredTurnInferenceV2 =
            swarmy_core::decode(&swarmy_core::encode(&fixture_inference()).unwrap()).unwrap();
        assert_eq!(bare, fixture_inference());
        let bare_tool: StoredToolMetricV2 =
            swarmy_core::decode(&swarmy_core::encode(&fixture_tool()).unwrap()).unwrap();
        assert_eq!(bare_tool, fixture_tool());
    }

    #[test]
    fn detail_rows_sort_chronologically_not_by_hash() {
        let row = |id: &str, started: Option<i64>| StoredTurnInferenceV2 {
            metric: StoredInferenceMetricV2 {
                request_id: id.into(),
                ..StoredInferenceMetricV2::default()
            },
            started_ns: started,
            ..StoredTurnInferenceV2::default()
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
        let tool = |id: &str, dispatched: Option<i64>| StoredToolMetricV2 {
            request_id: id.into(),
            dispatched_ns: dispatched,
            ..StoredToolMetricV2::default()
        };
        let mut tools = vec![tool("zzz", Some(5)), tool("aaa", Some(1))];
        sort_tool_rows(&mut tools);
        assert_eq!(tools[0].request_id, "aaa");
    }

    #[test]
    fn legacy_dropped_counters_survive_migration() {
        let mut legacy = fixture_v1();
        legacy.dropped_stages = 7;
        legacy.dropped_inference = 27;
        legacy.dropped_tools = 42;
        let api = StoredTurnMetrics::V1(legacy).into_api();
        let (summary, rows, tools) = migrate_legacy(api);
        assert_eq!(summary.dropped_stages, 7);
        assert_eq!(summary.dropped_inference, 27);
        assert_eq!(summary.dropped_tools, 42);
        // The assembled turn reports the migrated remainder even on a
        // complete read, and adds paging truncation on top.
        let turn = assemble_turn(&summary, rows.clone(), tools.clone(), None, None);
        assert_eq!(turn.dropped_stages, 7);
        assert_eq!(turn.dropped_inference, 27);
        assert_eq!(turn.dropped_tools, 42);
        let paged = assemble_turn(&summary, rows, tools, Some(0), Some(0));
        assert_eq!(paged.dropped_inference, 27 + 1);
        assert_eq!(paged.dropped_tools, 42);
        assert_eq!(paged.dropped_stages, 7);
    }

    #[test]
    fn v2_summary_and_rows_round_trip() {
        let summary = StoredTurnSummaryV2 {
            session_id: "s".into(),
            turn_id: "t".into(),
            appended_ns: Some(1_000_000),
            idle_ns: Some(6_000_000),
            ..StoredTurnSummaryV2::default()
        };
        for value in [
            StoredTurnMetrics::V2Summary(summary),
            StoredTurnMetrics::V2Inference(StoredTurnInferenceV2 {
                metric: StoredInferenceMetricV2 {
                    request_id: "r".into(),
                    ..StoredInferenceMetricV2::default()
                },
                started_ns: Some(2_000_000),
                finished_ns: Some(5_000_000),
                ..StoredTurnInferenceV2::default()
            }),
            StoredTurnMetrics::V2Tool(StoredToolMetricV2 {
                request_id: "c".into(),
                name: "bash".into(),
                ..StoredToolMetricV2::default()
            }),
        ] {
            let bytes = swarmy_core::encode(&value).unwrap();
            let decoded: StoredTurnMetrics = swarmy_core::decode(&bytes).unwrap();
            assert_eq!(decoded, value);
        }
        // `V1` bytes still decode through the production legacy path.
        if V1_ENVELOPE_HEX.starts_with("PLACEHOLDER") {
            return;
        }
        assert!(matches!(
            decode_summary(&hex_to_bytes(V1_ENVELOPE_HEX)),
            Ok(DecodedSummary::Legacy(_))
        ));
    }

    #[test]
    fn unbounded_turn_keeps_every_row_and_derives_idle() {
        let session = SessionId::from_ulid(ulid::Ulid::nil());
        let turn = MessageId::from_ulid(ulid::Ulid::nil());
        let mut state = TurnWrite {
            summary: StoredTurnSummaryV2 {
                session_id: session.to_string(),
                turn_id: turn.to_string(),
                ..StoredTurnSummaryV2::default()
            },
            inference: BTreeMap::new(),
            tools: BTreeMap::new(),
            migrated_inference: BTreeMap::new(),
            migrated_tools: BTreeMap::new(),
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
            summary: StoredTurnSummaryV2::default(),
            inference: BTreeMap::new(),
            tools: BTreeMap::new(),
            migrated_inference: BTreeMap::new(),
            migrated_tools: BTreeMap::new(),
        };
        let mut sequential = TurnWrite {
            summary: StoredTurnSummaryV2::default(),
            inference: BTreeMap::new(),
            tools: BTreeMap::new(),
            migrated_inference: BTreeMap::new(),
            migrated_tools: BTreeMap::new(),
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
                .record_turn_metric(
                    session,
                    turn,
                    MetricPatch::Stage(TurnEvent {
                        session_id: session,
                        turn_id: turn,
                        stage,
                        request_id: Some(request),
                        clock_id: "boot".into(),
                        monotonic_ns: ns,
                        unix_ns: i128::from(ns),
                    }),
                )
                .await
                .unwrap();
        }
        // The appended and idle anchors carry no request id in production;
        // record them that way so the wall-time derivation matches.
        store
            .record_turn_metric(
                session,
                turn,
                MetricPatch::Stage(TurnEvent {
                    session_id: session,
                    turn_id: turn,
                    stage: TurnStage::Appended,
                    request_id: None,
                    clock_id: "boot".into(),
                    monotonic_ns: 1_000_000,
                    unix_ns: 1_000_000,
                }),
            )
            .await
            .unwrap();
        store
            .record_turn_metric(
                session,
                turn,
                MetricPatch::Stage(TurnEvent {
                    session_id: session,
                    turn_id: turn,
                    stage: TurnStage::Idle,
                    request_id: None,
                    clock_id: "boot".into(),
                    monotonic_ns: 6_000_000,
                    unix_ns: 6_000_000,
                }),
            )
            .await
            .unwrap();
        store
            .record_turn_metric(
                session,
                turn,
                MetricPatch::Inference(InferenceMetric {
                    request_id: request.to_string(),
                    provider: "fake".into(),
                    model: "scripted".into(),
                    output_tokens: 10,
                    ..InferenceMetric::default()
                }),
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
            .record_turn_metric(
                session,
                turn,
                MetricPatch::Stage(TurnEvent {
                    session_id: session,
                    turn_id: turn,
                    stage: TurnStage::Appended,
                    request_id: None,
                    clock_id: "boot".into(),
                    monotonic_ns: 1_000_000,
                    unix_ns: appended_ns,
                }),
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
                .record_turn_metric(
                    session,
                    turn,
                    MetricPatch::Stage(TurnEvent {
                        session_id: session,
                        turn_id: turn,
                        stage: TurnStage::InferenceStarted,
                        request_id: Some(request),
                        clock_id: "boot".into(),
                        monotonic_ns: 2_000_000 + index,
                        unix_ns: appended_ns + i128::from(500 + index),
                    }),
                )
                .await
                .unwrap();
            store
                .record_turn_metric(
                    session,
                    turn,
                    MetricPatch::Inference(InferenceMetric {
                        request_id: request.to_string(),
                        provider: "fake".into(),
                        model: "scripted".into(),
                        output_tokens: 4,
                        ..InferenceMetric::default()
                    }),
                )
                .await
                .unwrap();
            store
                .record_turn_metric(
                    session,
                    turn,
                    MetricPatch::Stage(TurnEvent {
                        session_id: session,
                        turn_id: turn,
                        stage: TurnStage::InferenceFinished,
                        request_id: Some(request),
                        clock_id: "boot".into(),
                        monotonic_ns: 3_000_000 + index,
                        unix_ns: appended_ns + i128::from(600 + index),
                    }),
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
            .record_turn_metric(session, turn, MetricPatch::Stage(idle_event))
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

    #[tokio::test]
    async fn legacy_v1_rows_keep_dropped_counters_through_rewrite() {
        let Ok(cluster) = std::env::var("SWARMY_FDB_CLUSTER_FILE") else {
            return;
        };
        NETWORK.get_or_init(crate::boot);
        let path = vec![
            "turn-metrics-legacy-migration-test".into(),
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
        // A baseline session written by the capped layout: V1 envelope bytes
        // with non-zero dropped counters at the summary key.
        let legacy = StoredTurnMetricsV1 {
            session_id: session.to_string(),
            turn_id: turn.to_string(),
            stages: vec![StageTiming {
                stage: "appended".into(),
                request_id: None,
                clock_id: "boot".into(),
                monotonic_ns: 1_000_000,
                unix_ns: 1_000_000,
            }],
            inference: vec![StoredInferenceMetricV1 {
                request_id: "legacy-r".into(),
                provider: "fake".into(),
                model: "scripted".into(),
                output_tokens: 4,
                ..StoredInferenceMetricV1::default()
            }],
            dropped_stages: 7,
            dropped_inference: 27,
            dropped_tools: 42,
            ..StoredTurnMetricsV1::default()
        };
        let summary_key = store.turn_summary_key(session, turn);
        store
            .transaction(|trx| {
                let summary_key = &summary_key;
                let legacy = &legacy;
                async move {
                    write(&trx, summary_key, &StoredTurnMetrics::V1(legacy.clone()))?;
                    Ok(())
                }
            })
            .await
            .unwrap();
        // The first read migrates in memory without rewriting the summary key.
        let records = store.list_turn_metrics(session, None, 10).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].dropped_stages, 7);
        assert_eq!(records[0].dropped_inference, 27);
        assert_eq!(records[0].dropped_tools, 42);
        assert_eq!(records[0].inference.len(), 1);
        assert_eq!(records[0].inference[0].request_id, "legacy-r");
        // One more inference for the same turn takes the legacy branch of
        // `record_turn_metrics`, which rewrites the summary as V2 plus detail
        // rows. The migrated counters must survive the rewrite.
        store
            .record_turn_metric(
                session,
                turn,
                MetricPatch::Inference(InferenceMetric {
                    request_id: "new-r".into(),
                    provider: "fake".into(),
                    model: "scripted".into(),
                    output_tokens: 1,
                    ..InferenceMetric::default()
                }),
            )
            .await
            .unwrap();
        let records = store.list_turn_metrics(session, None, 10).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].dropped_stages, 7);
        assert_eq!(records[0].dropped_inference, 27);
        assert_eq!(records[0].dropped_tools, 42);
        assert_eq!(records[0].inference.len(), 2);
        assert!(
            records[0]
                .inference
                .iter()
                .any(|row| row.request_id == "legacy-r")
        );
    }
}
