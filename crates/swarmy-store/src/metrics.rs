//! Turn-metric transactions: the bounded queue and `FoundationDB` writes.
use std::{
    collections::BTreeMap,
    sync::{Arc, atomic::AtomicU64},
};

use foundationdb::{Database, RangeOption, Transaction, tuple::Subspace};
use futures::TryStreamExt;
use swarmy_core::{AgentId, MessageId, SessionId, TurnEvent, ignore_best_effort};

use crate::{
    MAX_SCAN_LIMIT, Result, StoreError,
    metrics_codec::{decode_inference, decode_summary, decode_tool},
    metrics_model::{TouchedRows, TurnWrite, apply_one, assemble_turn, touched_rows},
    scan, write,
};

pub(crate) use crate::metrics_codec::{
    StoredToolMetricCurrent, StoredTurnInferenceCurrent, StoredTurnMetrics,
    StoredTurnSummaryCurrent,
};
pub use crate::metrics_model::{
    AgentMetrics, ComputerMetric, InferenceMetric, LatencyPercentiles, MetricPatch, StageTiming,
    ToolMetric, TurnMetrics, WaitKind, completion_patches, dispatch_patches,
};

/// One queued observability write. Patches that share a turn are batched by
/// the drain task so concurrent stages do not contend on the summary key.
#[derive(Debug)]
pub(crate) struct MetricJob {
    pub(crate) session: SessionId,
    pub(crate) turn: MessageId,
    pub(crate) patches: Vec<MetricPatch>,
}

pub(crate) enum MetricMsg {
    Job(MetricJob),
    Flush(tokio::sync::oneshot::Sender<()>),
}

/// Bound for the per-store metric queue. The hot path never blocks on
/// observability; a full queue drops with a warning and `flush` waits.
pub(crate) const METRICS_CHANNEL_BOUND: usize = 1024;

/// Write handle for turn metrics without the observability queue sender.
/// The drain task owns only this handle, never a `Store`, so dropping every
/// `Store` closes the channel and lets the drain exit instead of leaking.
#[derive(Clone)]
pub(crate) struct MetricsWriter {
    db: Arc<Database>,
    root: Subspace,
    transactions: Arc<AtomicU64>,
}

impl MetricsWriter {
    pub(crate) fn new(db: Arc<Database>, root: Subspace, transactions: Arc<AtomicU64>) -> Self {
        Self {
            db,
            root,
            transactions,
        }
    }

    fn keys(&self) -> crate::keys::Keys<'_> {
        crate::keys::Keys::new(&self.root)
    }

    async fn transaction<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: Fn(foundationdb::RetryableTransaction) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        crate::run_transaction(&self.db, &self.transactions, operation).await
    }
}

pub(crate) fn spawn_metrics_drain(
    writer: MetricsWriter,
    mut rx: tokio::sync::mpsc::Receiver<MetricMsg>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        use std::collections::HashMap;
        loop {
            let first = rx.recv().await;
            let Some(first) = first else {
                break;
            };
            let mut batches: HashMap<(SessionId, MessageId), Vec<MetricPatch>> = HashMap::new();
            let mut flushes = Vec::new();
            match first {
                MetricMsg::Job(job) => {
                    batches
                        .entry((job.session, job.turn))
                        .or_default()
                        .extend(job.patches);
                }
                MetricMsg::Flush(ack) => flushes.push(ack),
            }
            while let Ok(msg) = rx.try_recv() {
                match msg {
                    MetricMsg::Job(job) => {
                        batches
                            .entry((job.session, job.turn))
                            .or_default()
                            .extend(job.patches);
                    }
                    MetricMsg::Flush(ack) => flushes.push(ack),
                }
            }
            for ((session, turn), patches) in batches {
                if let Err(error) = writer.record_turn_metrics(session, turn, patches).await {
                    tracing::warn!(%error, %session, %turn, "turn metric write failed");
                }
            }
            for ack in flushes {
                ignore_best_effort(ack.send(()), "acknowledge metric flush");
            }
        }
    })
}

pub(crate) async fn scan_reverse(
    trx: &Transaction,
    range: (Vec<u8>, Vec<u8>),
    limit: usize,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    use crate::check_limit;
    check_limit(limit)?;
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

impl MetricsWriter {
    /// Merge several independent observations in one read-modify-write
    /// transaction. A dispatch folds its tool name into its stage, and a node
    /// completion folds its tool result and completion stage, so each costs
    /// one transaction; the first-tool computer re-sample arrives as its own
    /// queued job so the completion never waits on the volume stat round
    /// trip. A `CommitUnknown`
    /// outcome is retried once, except for batches that contain a wait patch:
    /// wait patches increment retry and wait counters, so replaying them
    /// after a commit that actually landed would double-count. Stage,
    /// inference, tool, computer, and error patches are idempotent (anchors
    /// keep the first timestamp, inference keeps the maximum counters, tool
    /// and computer merges keep the first sample), so replaying those batches
    /// is safe.
    /// # Errors
    /// Returns database or encoding failures without changing the conversation.
    pub(crate) async fn record_turn_metrics(
        &self,
        session: SessionId,
        turn: MessageId,
        patches: Vec<MetricPatch>,
    ) -> Result<()> {
        let touched = touched_rows(&patches);
        let summary_key = self.keys().turn_metrics(session, turn);
        let mut attempts = 0;
        loop {
            let attempted = self
                .commit_turn_batch(session, turn, &summary_key, &patches, &touched)
                .await;
            match attempted {
                // Wait batches are not replayed: the increment is not
                // idempotent, so a landed commit would double-count.
                Err(StoreError::Storage(crate::StorageError::CommitUnknown))
                    if attempts == 0 && !touched.has_wait =>
                {
                    attempts += 1;
                }
                other => return other,
            }
        }
    }

    async fn commit_turn_batch(
        &self,
        session: SessionId,
        turn: MessageId,
        summary_key: &[u8],
        patches: &[MetricPatch],
        touched: &TouchedRows,
    ) -> Result<()> {
        // Share the batch across binding-level retries without re-cloning
        // the patches on every attempt.
        let patches: Arc<[MetricPatch]> = patches.into();
        let touched = touched.clone();
        let summary_key = summary_key.to_vec();
        self.transaction(|trx| {
            let patches = patches.clone();
            let summary_key = summary_key.clone();
            let touched = touched.clone();
            async move {
                let mut state = self
                    .load_turn_state(&trx, session, turn, &summary_key, &touched)
                    .await?;
                for patch in &*patches {
                    apply_one(&mut state, patch);
                }
                self.write_turn_state(&trx, session, turn, &summary_key, &state, &touched)
            }
        })
        .await
    }

    async fn load_turn_state(
        &self,
        trx: &Transaction,
        session: SessionId,
        turn: MessageId,
        summary_key: &[u8],
        touched: &TouchedRows,
    ) -> Result<TurnWrite> {
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
        for id in &touched.inference {
            if !state.inference.contains_key(id) {
                let key = self.keys().turn_inference(session, turn, id);
                if let Some(bytes) = trx.get(&key, false).await? {
                    state
                        .inference
                        .insert(id.clone(), decode_inference(&bytes)?);
                }
            }
        }
        for id in &touched.tools {
            if !state.tools.contains_key(id) {
                let key = self.keys().turn_tool(session, turn, id);
                if let Some(bytes) = trx.get(&key, false).await? {
                    state.tools.insert(id.clone(), decode_tool(&bytes)?);
                }
            }
        }
        Ok(state)
    }

    fn write_turn_state(
        &self,
        trx: &Transaction,
        session: SessionId,
        turn: MessageId,
        summary_key: &[u8],
        state: &TurnWrite,
        touched: &TouchedRows,
    ) -> Result<()> {
        write(
            trx,
            summary_key,
            &StoredTurnMetrics::Summary(Box::new(state.summary.clone())),
        )?;
        for id in &touched.inference {
            if let Some(row) = state.inference.get(id) {
                write(
                    trx,
                    &self.keys().turn_inference(session, turn, id),
                    &StoredTurnMetrics::Inference(row.clone()),
                )?;
            }
        }
        for id in &touched.tools {
            if let Some(row) = state.tools.get(id) {
                write(
                    trx,
                    &self.keys().turn_tool(session, turn, id),
                    &StoredTurnMetrics::Tool(row.clone()),
                )?;
            }
        }
        Ok(())
    }
}

impl crate::Store {
    /// Merge several independent observations in one read-modify-write
    /// transaction. Tests and one-off writes call this directly; the hot
    /// path queues through `observe_turn_metrics` instead.
    /// # Errors
    /// Returns database or encoding failures without changing the conversation.
    pub async fn record_turn_metrics(
        &self,
        session: SessionId,
        turn: MessageId,
        patches: Vec<MetricPatch>,
    ) -> Result<()> {
        self.metrics_writer()
            .record_turn_metrics(session, turn, patches)
            .await
    }

    /// Queue observability without waiting on a turn's hot path. The drain
    /// task batches by turn; `flush_turn_metrics` waits for the queue.
    pub fn observe_turn_stage(&self, event: TurnEvent) {
        self.observe_turn_metric(event.session_id, event.turn_id, MetricPatch::Stage(event));
    }

    /// Queue observability without waiting on a turn's hot path.
    pub fn observe_turn_metric(&self, session: SessionId, turn: MessageId, patch: MetricPatch) {
        self.observe_turn_metrics(session, turn, vec![patch]);
    }

    /// Queue one transaction's patches that share a turn. The write is
    /// bounded and non-blocking: a full queue drops the job with a warning
    /// and `flush_turn_metrics` cannot recover it; the flush only waits for
    /// jobs that were queued.
    pub fn observe_turn_metrics(
        &self,
        session: SessionId,
        turn: MessageId,
        patches: Vec<MetricPatch>,
    ) {
        self.ensure_metrics_drain();
        let job = MetricJob {
            session,
            turn,
            patches,
        };
        if let Err(error) = self.metrics_tx.try_send(MetricMsg::Job(job)) {
            match error {
                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                    tracing::warn!(%session, %turn, "turn metric queue full; dropping");
                }
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    tracing::warn!(%session, %turn, "turn metric drain closed; dropping");
                }
            }
        }
    }

    /// Wait for queued observability to commit. Hosts call this on shutdown
    /// so queued metric writes are not lost; tests that need a queued
    /// write to be visible await this before reading. Jobs dropped while
    /// the queue was full are not recovered by this call.
    /// # Errors
    /// Returns a storage error when the flush handshake itself fails; the
    /// drain task only logs per-turn write failures.
    pub async fn flush_turn_metrics(&self) -> Result<()> {
        self.ensure_metrics_drain();
        let (ack, rx) = tokio::sync::oneshot::channel();
        // The drain exiting means its receiver is gone; with no queued
        // writes left to wait for, report success.
        if self.metrics_tx.send(MetricMsg::Flush(ack)).await.is_err() {
            return Ok(());
        }
        rx.await
            .map_err(|_| StoreError::Storage(crate::StorageError::CommitUnknown))?;
        Ok(())
    }

    async fn turn_rows(
        &self,
        session: SessionId,
        turn: MessageId,
    ) -> Result<(
        Vec<StoredTurnInferenceCurrent>,
        Vec<StoredToolMetricCurrent>,
    )> {
        let (inference_begin, inference_end) =
            self.keys().turn_inference_space(session, turn).range();
        let (tool_begin, tool_end) = self.keys().turn_tool_space(session, turn).range();
        let mut inference = Vec::new();
        for (_, bytes) in self.scan_all_pages(inference_begin, inference_end).await? {
            inference.push(decode_inference(&bytes)?);
        }
        let mut tools = Vec::new();
        for (_, bytes) in self.scan_all_pages(tool_begin, tool_end).await? {
            tools.push(decode_tool(&bytes)?);
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
        let session: SessionId = summary.session_id.parse().map(SessionId::from_ulid)?;
        let turn: MessageId = summary.turn_id.parse().map(MessageId::from_ulid)?;
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
                let (mut begin, end) = self.keys().turn_metrics_space(session).range();
                if let Some(turn) = after {
                    begin = crate::next_cursor(&self.keys().turn_metrics(session, turn));
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
        let mut end = self.keys().turn_metrics_space(session).range().1;
        let begin = match since {
            Some(turn) => {
                let mut key = self.keys().turn_metrics(session, turn);
                key = crate::next_cursor(&key);
                key
            }
            None => self.keys().turn_metrics_space(session).range().0,
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
    #[expect(
        clippy::cast_precision_loss,
        reason = "aggregate rates and durations have only approximate floating-point precision"
    )]
    pub async fn agent_turn_metrics(
        &self,
        agent: AgentId,
        limit: usize,
        since: Option<MessageId>,
    ) -> Result<AgentMetrics> {
        let record = self
            .get_agent(agent)
            .await?
            .ok_or(StoreError::Domain(crate::DomainError::AgentMissing))?;
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
