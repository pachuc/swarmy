//! Turn-metric model: public types and pure update logic.
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use swarmy_core::{TurnEvent, TurnStage};

use crate::metrics_codec::{
    StoredInferenceMetricCurrent, StoredToolMetricCurrent, StoredTurnInferenceCurrent,
    StoredTurnSummaryCurrent,
};

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
    #[expect(
        clippy::cast_precision_loss,
        reason = "throughput is an approximate rate; sub-token precision is not meaningful"
    )]
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
    #[expect(
        clippy::cast_precision_loss,
        reason = "millisecond display precision is lower than the nanosecond source precision"
    )]
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

    #[expect(
        clippy::cast_precision_loss,
        reason = "queue durations are approximate millisecond measurements"
    )]
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

#[derive(Clone, Debug)]
pub enum MetricPatch {
    Stage(TurnEvent),
    Inference(InferenceMetric),
    Tool(ToolMetric),
    Computer(ComputerMetric),
    Error(String),
    Wait { request_id: String, kind: WaitKind },
}

#[derive(Clone, Copy, Debug)]
pub enum WaitKind {
    Retry,
    RateLimit,
    MissingGateway,
    ProviderFailure,
}

impl From<StoredInferenceMetricCurrent> for InferenceMetric {
    fn from(value: StoredInferenceMetricCurrent) -> Self {
        Self {
            request_id: value.request_id,
            provider: value.provider,
            model: value.model,
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
            error: value.error,
        }
    }
}

impl From<StoredToolMetricCurrent> for ToolMetric {
    fn from(value: StoredToolMetricCurrent) -> Self {
        Self {
            request_id: value.request_id,
            name: value.name,
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

#[expect(
    clippy::cast_precision_loss,
    reason = "wall nanoseconds exceed f64 integer precision; millisecond display values do not need it"
)]
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
pub(crate) struct TurnWrite {
    pub(crate) summary: StoredTurnSummaryCurrent,
    pub(crate) inference: BTreeMap<String, StoredTurnInferenceCurrent>,
    pub(crate) tools: BTreeMap<String, StoredToolMetricCurrent>,
}

/// The detail rows one patch batch touches, so the transaction reads only
/// what it writes. A turn with hundreds of calls still commits one small
/// transaction per batch.
#[derive(Clone, Debug, Default)]
pub(crate) struct TouchedRows {
    pub(crate) inference: BTreeSet<String>,
    pub(crate) tools: BTreeSet<String>,
    pub(crate) has_wait: bool,
}

pub(crate) fn touched_rows(patches: &[MetricPatch]) -> TouchedRows {
    let mut touched = TouchedRows::default();
    for patch in patches {
        match patch {
            MetricPatch::Stage(event) => {
                if let Some(id) = event.request_id {
                    let key = id.to_string();
                    match event.stage {
                        TurnStage::InferenceStarted
                        | TurnStage::InferenceFinished
                        | TurnStage::FirstToken => {
                            touched.inference.insert(key);
                        }
                        TurnStage::ToolDispatched | TurnStage::ToolCompleted => {
                            touched.tools.insert(key);
                        }
                        _ => {}
                    }
                }
            }
            MetricPatch::Inference(update) => {
                touched.inference.insert(update.request_id.clone());
            }
            MetricPatch::Tool(update) => {
                touched.tools.insert(update.request_id.clone());
            }
            MetricPatch::Wait { request_id, .. } => {
                touched.inference.insert(request_id.clone());
            }
            MetricPatch::Computer(_) | MetricPatch::Error(_) => {}
        }
    }
    touched.has_wait = patches
        .iter()
        .any(|patch| matches!(patch, MetricPatch::Wait { .. }));
    touched
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

#[expect(
    clippy::too_many_lines,
    reason = "one merge updates row, summary totals, and throughput together; splitting would separate the delta bookkeeping from the row it deltas against"
)]
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

pub(crate) fn apply_one(state: &mut TurnWrite, patch: &MetricPatch) {
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
pub(crate) fn sort_inference_rows(rows: &mut [StoredTurnInferenceCurrent]) {
    rows.sort_by(|a, b| {
        (a.started_ns.unwrap_or(i64::MAX), &a.metric.request_id)
            .cmp(&(b.started_ns.unwrap_or(i64::MAX), &b.metric.request_id))
    });
}

/// Chronological order for per-tool rows by dispatch, then start, then id.
pub(crate) fn sort_tool_rows(rows: &mut [StoredToolMetricCurrent]) {
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
pub(crate) fn assemble_turn(
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
            .map(|row| InferenceMetric::from(row.metric))
            .collect(),
        tools: tools_page.into_iter().map(ToolMetric::from).collect(),
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
