//! Compact, independently committed observations; no conversation event is added.
use std::collections::BTreeMap;

use foundationdb::{RangeOption, Transaction};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use swarmy_api_types::{
    AgentMetrics, ComputerMetric, InferenceMetric, LatencyPercentiles, StageTiming, ToolMetric,
    TurnMetrics,
};
use swarmy_core::{AgentId, MessageId, SessionId, TurnEvent};

use crate::{MAX_SCAN_LIMIT, Result, Store, decode, scan, write};

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

/// Private storage layout for a turn record. The public `TurnMetrics` type
/// is the API contract and may gain display fields; this struct pins the
/// `FoundationDB` encoding so API changes never make existing rows undecodable.
/// New fields go at the end with `#[serde(default)]`. See the compatibility
/// test below, which plays the same role as the session header one.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct StoredTurnMetrics {
    session_id: String,
    turn_id: String,
    stages: Vec<StageTiming>,
    inference: Vec<InferenceMetric>,
    tools: Vec<ToolMetric>,
    computer: Option<ComputerMetric>,
    append_to_first_token_ms: Option<f64>,
    inference_duration_ms: Option<f64>,
    append_to_idle_ms: Option<f64>,
    error: Option<String>,
    #[serde(default)]
    dropped_stages: u64,
    #[serde(default)]
    dropped_inference: u64,
    #[serde(default)]
    dropped_tools: u64,
}

impl StoredTurnMetrics {
    fn into_api(self) -> TurnMetrics {
        TurnMetrics {
            session_id: self.session_id,
            turn_id: self.turn_id,
            stages: self.stages,
            inference: self.inference,
            tools: self.tools,
            computer: self.computer,
            append_to_first_token_ms: self.append_to_first_token_ms,
            inference_duration_ms: self.inference_duration_ms,
            append_to_idle_ms: self.append_to_idle_ms,
            error: self.error,
            dropped_stages: self.dropped_stages,
            dropped_inference: self.dropped_inference,
            dropped_tools: self.dropped_tools,
        }
    }
}

impl From<TurnMetrics> for StoredTurnMetrics {
    fn from(value: TurnMetrics) -> Self {
        Self {
            session_id: value.session_id,
            turn_id: value.turn_id,
            stages: value.stages,
            inference: value.inference,
            tools: value.tools,
            computer: value.computer,
            append_to_first_token_ms: value.append_to_first_token_ms,
            inference_duration_ms: value.inference_duration_ms,
            append_to_idle_ms: value.append_to_idle_ms,
            error: value.error,
            dropped_stages: value.dropped_stages,
            dropped_inference: value.dropped_inference,
            dropped_tools: value.dropped_tools,
        }
    }
}

fn clipped(value: &str, max: usize) -> String {
    value.chars().take(max).collect()
}

fn apply(record: &mut TurnMetrics, patch: &MetricPatch) {
    match patch {
        MetricPatch::Stage(event) => apply_stage(record, event),
        MetricPatch::Inference(update) => apply_inference(record, update),
        MetricPatch::Tool(update) => apply_tool(record, update),
        MetricPatch::Computer(value) => apply_computer(record, value),
        MetricPatch::Error(value) => record.error = Some(clipped(value, 512)),
        MetricPatch::Wait { request_id, kind } => apply_wait(record, request_id, *kind),
    }
    record.derive();
}

fn apply_stage(record: &mut TurnMetrics, event: &TurnEvent) {
    let stage = serde_json::to_value(event.stage)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_default();
    let row = StageTiming {
        stage: stage.clone(),
        request_id: event.request_id.map(|id| id.to_string()),
        clock_id: clipped(&event.clock_id, 80),
        monotonic_ns: event.monotonic_ns,
        unix_ns: i64::try_from(event.unix_ns).unwrap_or(i64::MAX),
    };
    if record.stages.contains(&row) {
        // Duplicate delivery; the row is already recorded.
    } else if record.stages.len() < 64 {
        record.stages.push(row.clone());
        record.stages.sort_by_key(|item| item.unix_ns);
    } else {
        record.dropped_stages = record.dropped_stages.saturating_add(1);
    }
    if let Some(id) = &row.request_id
        && (stage == "tool_dispatched" || stage == "tool_completed")
        && let Some(tool) = tool_entry(record, id)
    {
        if stage == "tool_dispatched" {
            tool.dispatched_ns = Some(row.unix_ns);
        } else {
            tool.completed_ns = Some(row.unix_ns);
        }
    }
}

fn apply_inference(record: &mut TurnMetrics, update: &InferenceMetric) {
    // A failed request carries its error on the row and the turn so the
    // rollup counts it even when the terminal Error patch lands first; a
    // success clears a previous turn error for the same turn.
    if update.error.is_some() {
        record.error.clone_from(&update.error);
    } else {
        record.error = None;
    }
    let existing = record
        .inference
        .iter_mut()
        .find(|row| row.request_id == update.request_id);
    if let Some(row) = existing {
        let (retries, rate_limit_waits, gateway_waits, provider_failures) = (
            row.retries,
            row.rate_limit_waits,
            row.gateway_waits,
            row.provider_failures,
        );
        *row = update.clone();
        row.retries = row.retries.max(retries);
        row.rate_limit_waits = row.rate_limit_waits.max(rate_limit_waits);
        row.gateway_waits = row.gateway_waits.max(gateway_waits);
        row.provider_failures = row.provider_failures.max(provider_failures);
    } else if record.inference.len() < 16 {
        record.inference.push(update.clone());
    } else {
        record.dropped_inference = record.dropped_inference.saturating_add(1);
    }
}

fn apply_tool(record: &mut TurnMetrics, update: &ToolMetric) {
    let Some(row) = tool_entry(record, &update.request_id) else {
        return;
    };
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

fn apply_computer(record: &mut TurnMetrics, value: &ComputerMetric) {
    let current = record.computer.get_or_insert_with(ComputerMetric::default);
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

fn apply_wait(record: &mut TurnMetrics, request_id: &str, kind: WaitKind) {
    let index = match record
        .inference
        .iter()
        .position(|row| row.request_id == request_id)
    {
        Some(index) => index,
        None if record.inference.len() < 16 => {
            record.inference.push(InferenceMetric {
                request_id: request_id.to_owned(),
                ..InferenceMetric::default()
            });
            record.inference.len() - 1
        }
        None => {
            record.dropped_inference = record.dropped_inference.saturating_add(1);
            return;
        }
    };
    let row = &mut record.inference[index];
    match kind {
        WaitKind::Retry => row.retries = row.retries.saturating_add(1),
        WaitKind::RateLimit => row.rate_limit_waits = row.rate_limit_waits.saturating_add(1),
        WaitKind::MissingGateway => row.gateway_waits = row.gateway_waits.saturating_add(1),
        WaitKind::ProviderFailure => {
            row.provider_failures = row.provider_failures.saturating_add(1);
        }
    }
}

fn tool_entry<'a>(record: &'a mut TurnMetrics, request_id: &str) -> Option<&'a mut ToolMetric> {
    if let Some(index) = record
        .tools
        .iter()
        .position(|row| row.request_id == request_id)
    {
        return Some(&mut record.tools[index]);
    }
    if record.tools.len() >= 64 {
        record.dropped_tools = record.dropped_tools.saturating_add(1);
        return None;
    }
    record.tools.push(ToolMetric {
        request_id: request_id.to_owned(),
        ..ToolMetric::default()
    });
    record.tools.last_mut()
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

impl Store {
    /// Merge one independent observation after its boundary has completed.
    /// # Errors
    /// Returns database or encoding failures without changing the conversation.
    pub async fn record_turn_metric(
        &self,
        session: SessionId,
        turn: MessageId,
        patch: MetricPatch,
    ) -> Result<()> {
        let key = self.root.pack(&(
            "turn_metrics",
            session.as_ulid().to_bytes().as_slice(),
            turn.as_ulid().to_bytes().as_slice(),
        ));
        self.transaction(|trx| {
            let key = &key;
            let patch = &patch;
            async move {
                let mut record = match trx.get(key, false).await? {
                    None => TurnMetrics {
                        session_id: session.to_string(),
                        turn_id: turn.to_string(),
                        ..TurnMetrics::default()
                    },
                    Some(value) => decode::<StoredTurnMetrics>(&value)
                        .or_else(|_| {
                            // Rows written before the storage type was split
                            // from the API type share the same field prefix.
                            decode::<TurnMetrics>(&value).map(StoredTurnMetrics::from)
                        })
                        .map(StoredTurnMetrics::into_api)
                        .map_err(crate::StoreError::from)?,
                };
                apply(&mut record, patch);
                write(&trx, key, &StoredTurnMetrics::from(record))?;
                Ok(())
            }
        })
        .await
    }

    /// Spawn observability after the stage, without waiting on a turn's hot path.
    pub fn observe_turn_stage(&self, event: TurnEvent) {
        self.observe_turn_metric(event.session_id, event.turn_id, MetricPatch::Stage(event));
    }

    /// Spawn observability after the stage, without waiting on a turn's hot path.
    pub fn observe_turn_metric(&self, session: SessionId, turn: MessageId, patch: MetricPatch) {
        let store = self.clone();
        tokio::spawn(async move {
            if let Err(error) = store.record_turn_metric(session, turn, patch).await {
                tracing::warn!(%error, %session, %turn, "turn metric write failed");
            }
        });
    }

    /// Resolve a tool's durable turn asynchronously after its fenced completion.
    pub fn observe_request_tool(
        &self,
        session: SessionId,
        request: swarmy_core::RequestId,
        metric: ToolMetric,
    ) {
        let store = self.clone();
        tokio::spawn(async move {
            match store.request_turn_id(request).await {
                Ok(Some(turn)) => {
                    if let Err(error) = store
                        .record_turn_metric(session, turn, MetricPatch::Tool(metric))
                        .await
                    {
                        tracing::warn!(%error, %request, "tool metric write failed");
                    }
                }
                Ok(None) => {}
                Err(error) => tracing::warn!(%error, %request, "tool turn lookup failed"),
            }
        });
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
        Ok(raw
            .into_iter()
            .filter_map(|(_, bytes)| {
                match decode::<StoredTurnMetrics>(&bytes)
                    .or_else(|_| decode::<TurnMetrics>(&bytes).map(StoredTurnMetrics::from))
                {
                    Ok(record) => Some(record.into_api()),
                    Err(error) => {
                        tracing::warn!(%error, "skipping undecodable turn metric");
                        None
                    }
                }
            })
            .collect())
    }

    /// Most recent turns first, without paging the whole session. The rollup
    /// only needs the tail, so pages walk the key range in reverse.
    async fn recent_turn_metrics(
        &self,
        session: SessionId,
        since: Option<MessageId>,
        limit: usize,
    ) -> Result<Vec<TurnMetrics>> {
        let mut turns = Vec::new();
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
        while turns.len() < limit {
            let take = (limit - turns.len()).min(MAX_SCAN_LIMIT);
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
                match decode::<StoredTurnMetrics>(&bytes)
                    .or_else(|_| decode::<TurnMetrics>(&bytes).map(StoredTurnMetrics::from))
                {
                    Ok(record) => turns.push(record.into_api()),
                    Err(error) => {
                        tracing::warn!(%error, "skipping undecodable turn metric");
                    }
                }
                if turns.len() >= limit {
                    break;
                }
            }
        }
        // Collected newest-first; restore ascending order for the rollup.
        turns.reverse();
        Ok(turns)
    }

    /// Roll up at most `limit` of the most recent turns of the named agent's
    /// current main session, optionally after `since`. The 200-turn cap keeps
    /// the rollup bounded no matter how long the session runs.
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
        let mut rates = Vec::new();
        {
            let page = self.recent_turn_metrics(session, since, limit).await?;
            for turn in page {
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
                for tool in &turn.tools {
                    if let Some(ms) = tool
                        .dispatched_ns
                        .zip(tool.completed_ns)
                        .and_then(|(start, end)| end.checked_sub(start).filter(|ns| *ns >= 0))
                        .map(|ns| ns as f64 / 1_000_000.0)
                    {
                        latency.entry("tool_round_trip").or_default().push(ms);
                    }
                    if let Some(ms) = tool.queue_ms {
                        latency.entry("tool_queue").or_default().push(ms);
                    }
                    if let Some(ms) = tool.process_wall_ms {
                        latency.entry("tool_process").or_default().push(ms);
                    }
                }
                for request in turn.inference {
                    output.input_tokens += request.input_tokens;
                    output.cached_input_tokens += request.cached_input_tokens;
                    output.output_tokens += request.output_tokens;
                    output.reasoning_tokens += request.reasoning_tokens;
                    output.cost_micros += request.cost_micros;
                    output.retries += u64::from(request.retries);
                    output.errors += u64::from(request.error.is_some());
                    if let Some(rate) = request.output_tokens_per_second {
                        rates.push(rate);
                    }
                }
            }
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
        if !rates.is_empty() {
            output.mean_output_tokens_per_second =
                Some(rates.iter().sum::<f64>() / rates.len() as f64);
        }
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stored_layout_round_trips_and_converts_to_the_api_type() {
        // Pins the FoundationDB encoding the way the session header test
        // pins its layout: storage bytes must decode after API-only changes.
        let stored = StoredTurnMetrics {
            session_id: "s".into(),
            turn_id: "t".into(),
            stages: vec![StageTiming {
                stage: "appended".into(),
                request_id: None,
                clock_id: "boot".into(),
                monotonic_ns: 1_000_000,
                unix_ns: 1_000_000,
            }],
            inference: vec![InferenceMetric {
                request_id: "r".into(),
                provider: "fake".into(),
                model: "scripted".into(),
                output_tokens: 4,
                ..InferenceMetric::default()
            }],
            dropped_stages: 1,
            dropped_inference: 2,
            dropped_tools: 3,
            ..StoredTurnMetrics::default()
        };
        let bytes = swarmy_core::encode(&stored).unwrap();
        let decoded: StoredTurnMetrics = swarmy_core::decode(&bytes).unwrap();
        assert_eq!(decoded, stored);
        assert_eq!(swarmy_core::encode(&decoded).unwrap(), bytes);
        let api = decoded.into_api();
        assert_eq!(api.session_id, "s");
        assert_eq!(api.dropped_stages, 1);
        assert_eq!(api.dropped_inference, 2);
        assert_eq!(api.dropped_tools, 3);
        // The API type converts back without losing counters.
        let round_trip = StoredTurnMetrics::from(api);
        assert_eq!(round_trip, stored);
    }
    #[test]
    fn caps_count_dropped_rows_instead_of_growing() {
        let mut turn = TurnMetrics::default();
        let event = |stage: &str, index: u64| TurnEvent {
            session_id: SessionId::from_ulid(ulid::Ulid::nil()),
            turn_id: MessageId::from_ulid(ulid::Ulid::nil()),
            stage: serde_json::from_value(serde_json::json!(stage)).unwrap(),
            request_id: None,
            clock_id: "boot".into(),
            monotonic_ns: index,
            unix_ns: i128::from(index),
        };
        for index in 0..70_u64 {
            // Cycle valid stage names; rows stay distinct through the clock.
            let name = ["submitted", "appended", "nudged"][usize::try_from(index % 3).unwrap()];
            apply(&mut turn, &MetricPatch::Stage(event(name, index)));
        }
        assert_eq!(turn.stages.len(), 64);
        assert_eq!(turn.dropped_stages, 6);
        for index in 0..20_u32 {
            apply(
                &mut turn,
                &MetricPatch::Inference(InferenceMetric {
                    request_id: format!("r-{index}"),
                    ..InferenceMetric::default()
                }),
            );
        }
        assert_eq!(turn.inference.len(), 16);
        assert_eq!(turn.dropped_inference, 4);
    }
    #[test]
    fn incremental_stages_and_zero_throughput() {
        let mut turn = TurnMetrics::default();
        let row = |stage: &str, ns| StageTiming {
            stage: stage.into(),
            request_id: Some("r".into()),
            clock_id: "boot".into(),
            monotonic_ns: ns,
            unix_ns: i64::try_from(ns).unwrap(),
        };
        turn.stages.push(row("appended", 1_000_000));
        turn.stages.push(row("inference_started", 2_000_000));
        turn.stages.push(row("first_token", 3_000_000));
        turn.stages.push(row("inference_finished", 5_000_000));
        turn.stages.push(row("idle", 6_000_000));
        turn.inference.push(InferenceMetric {
            request_id: "r".into(),
            output_tokens: 10,
            ..InferenceMetric::default()
        });
        turn.derive();
        assert_eq!(turn.append_to_first_token_ms, Some(2.0));
        assert_eq!(turn.inference_duration_ms, Some(3.0));
        assert_eq!(turn.append_to_idle_ms, Some(5.0));
        assert_eq!(turn.inference[0].output_tokens_per_second, Some(5_000.0));
        assert_eq!(InferenceMetric::tokens_per_second(0, 1.0), None);
        assert_eq!(InferenceMetric::tokens_per_second(1, 0.0), None);
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
        assert_eq!(
            records[0].inference[0].output_tokens_per_second,
            Some(5_000.0)
        );
        assert!(
            store
                .list_turn_metrics(session, Some(turn), 10)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
