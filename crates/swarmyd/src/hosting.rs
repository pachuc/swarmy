use crate::{
    heads::Heads,
    keeper::{Answered, Keeper, Occupancy, PlacementHold},
    tools::Claimed,
};
use anyhow::{Context, Result, bail};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use swarmy_core::{
    AgentId, BlockDevice, ManifestHeader, ManifestId, MessageId, PlacementRecord, SandboxSpec,
    SessionId, ToolJob, VolumeId, ignore_best_effort,
};
use swarmy_node_protocol::{
    ClientError, DEFAULT_DEADLINE, HostingBootSetRequest, HostingCloseRequest, HostingOpenRequest,
    HostingReadyRequest, MetricBatch, NodeClient, NodeMetric, NodeRefusal, ToolRouteRequest,
    WriterToken,
};
use swarmy_sandbox::RuncRuntime;
use swarmy_volume::{WriterGrant, WriterLease};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::time::Instant;

/// Stop recording and store the nonempty boot set under its content address.
pub(crate) async fn store_boot_set(
    runtime: &RuncRuntime,
    volume: VolumeId,
) -> Result<Option<(String, swarmy_core::BootSet)>> {
    let set = runtime.boot_set(volume).await?;
    if set.chunks.is_empty() {
        return Ok(None);
    }
    let key = swarmy_volume::put_boot_set(runtime.objects(), &set).await?;
    Ok(Some((key, set)))
}

#[derive(Clone, Copy)]
struct BootTimes {
    placement: f64,
    open: f64,
    boot: f64,
}

struct Call {
    job: ToolJob,
    delivered_ns: Option<i64>,
    reply: oneshot::Sender<Result<()>>,
    activity: ActivityGuard,
}
struct Entry {
    calls: mpsc::Sender<Call>,
    task: tokio::task::JoinHandle<()>,
    activity: Arc<std::sync::Mutex<Occupancy>>,
}

#[derive(Default)]

// The guard follows the call through admission, boot and execution. Cancellation
// and dropped queues must release occupancy even when no reply can be sent.
struct ActivityGuard {
    state: Arc<std::sync::Mutex<Occupancy>>,
    active: bool,
}

impl ActivityGuard {
    fn queued(state: Arc<std::sync::Mutex<Occupancy>>) -> Self {
        state.lock().expect("activity lock poisoned").queued += 1;
        Self {
            state,
            active: false,
        }
    }

    fn start(&mut self, session: SessionId) {
        let mut state = self.state.lock().expect("activity lock poisoned");
        state.queued -= 1;
        state.holder = Some(session);
        self.active = true;
    }
}

impl Drop for ActivityGuard {
    fn drop(&mut self) {
        let mut state = self.state.lock().expect("activity lock poisoned");
        if self.active {
            state.holder = None;
        } else {
            state.queued -= 1;
        }
    }
}

struct Opening {
    placement: PlacementRecord,
    hold: PlacementHold,
    volume: VolumeId,
    writer: Option<WriterLease>,
    head: ManifestId,
    boot_set: Option<String>,
    header: ManifestHeader,
    spec: SandboxSpec,
}

/// Each actor serializes calls for one agent while the keeper renews its leases.
pub(crate) struct Hosting {
    heads: Heads,
    runtime: Arc<RuncRuntime>,
    client: NodeClient,
    keeper: Keeper,
    routes: Mutex<BTreeMap<SessionId, AgentId>>,
    idle: Duration,
    entries: Mutex<BTreeMap<AgentId, Entry>>,
    previous: Mutex<BTreeMap<AgentId, u64>>,
    /// The turn whose first-tool computer re-sample already landed, per
    /// agent. The boot sample is taken before the first command runs; only
    /// the first completion per turn carries the re-sample, so later tools
    /// skip the volume stat read and the extra transaction. One entry per
    /// agent bounds the map for the life of the node process; entries leave
    /// with the hosting entry when the agent is evicted or shuts down.
    computer_sampled: Mutex<BTreeMap<AgentId, MessageId>>,
    shutdown: watch::Sender<bool>,
}

impl Hosting {
    pub(crate) fn new(
        heads: Heads,
        client: NodeClient,
        keeper: Keeper,
        runtime: Arc<RuncRuntime>,
        settings: &swarmy_config::Settings,
        previous: &[PlacementRecord],
    ) -> Arc<Self> {
        Arc::new(Self {
            heads,
            client,
            keeper,
            runtime,
            idle: settings.sandbox.idle_secs,
            entries: Mutex::new(BTreeMap::new()),
            routes: Mutex::new(BTreeMap::new()),
            previous: Mutex::new(previous.iter().map(|p| (p.agent_id, p.epoch)).collect()),
            computer_sampled: Mutex::new(BTreeMap::new()),
            shutdown: watch::channel(false).0,
        })
    }

    async fn route(&self, job: &ToolJob) -> Result<Option<AgentId>> {
        if let Some(agent) = self.routes.lock().await.get(&job.session_id).copied() {
            return Ok(Some(agent));
        }
        let agent = match self
            .client
            .route_tool(
                &ToolRouteRequest { job: job.clone() },
                Instant::now() + DEFAULT_DEADLINE,
            )
            .await
        {
            Ok(answer) => answer.agent,
            Err(ClientError::Refused(NodeRefusal::AlreadyDone)) => return Ok(None),
            Err(ClientError::Refused(refusal)) => return Err(refusal_error(refusal)),
            Err(error) => return Err(error.into()),
        };
        self.routes.lock().await.insert(job.session_id, agent);
        Ok(Some(agent))
    }

    async fn open(
        &self,
        agent: AgentId,
        job: &ToolJob,
        occupancy: Arc<std::sync::Mutex<Occupancy>>,
    ) -> Result<Option<Opening>> {
        let previous_epoch = self.previous.lock().await.get(&agent).copied();
        let sent = Instant::now();
        let answer = match self
            .client
            .open_hosting(
                &HostingOpenRequest {
                    agent,
                    job: job.clone(),
                    previous_epoch,
                },
                sent + DEFAULT_DEADLINE,
            )
            .await
        {
            Ok(answer) => answer,
            Err(ClientError::Refused(NodeRefusal::AlreadyDone)) => return Ok(None),
            Err(ClientError::Refused(refusal)) => return Err(refusal_error(refusal)),
            Err(error) => return Err(error.into()),
        };
        let answered = Answered {
            sent,
            server_now: answer.server_now,
        };
        let hold = self
            .keeper
            .hold_placement(answer.placement.clone(), answered, occupancy);
        let writer = self
            .keeper
            .hold_writer(answer.volume, answer.writer, answered);
        self.previous
            .lock()
            .await
            .insert(agent, answer.placement.epoch);
        self.heads.set(answer.volume, answer.head);
        Ok(Some(Opening {
            placement: answer.placement,
            hold,
            volume: answer.volume,
            writer: Some(writer),
            head: answer.head,
            boot_set: answer.boot_set,
            header: answer.header,
            spec: answer.spec,
        }))
    }

    /// Serve a job through the hosting actor.
    pub(crate) async fn call(
        self: &Arc<Self>,
        job: ToolJob,
        delivered_ns: Option<i64>,
    ) -> Result<()> {
        let Some(agent) = self.route(&job).await? else {
            return Ok(());
        };
        let (reply, response) = oneshot::channel();
        let (calls, activity) = {
            let mut entries = self.entries.lock().await;
            if *self.shutdown.borrow() {
                bail!("node is shutting down");
            }
            entries.retain(|_, entry| !entry.task.is_finished());
            // Drop samples for agents with no hosting entry so the map
            // holds at most one turn per live agent.
            let live: Vec<AgentId> = entries.keys().copied().collect();
            self.computer_sampled
                .lock()
                .await
                .retain(|agent, _| live.contains(agent));
            self.routes
                .lock()
                .await
                .retain(|_, a| entries.contains_key(a) || *a == agent);
            let entry = entries.entry(agent).or_insert_with(|| {
                let (calls, receive) = mpsc::channel(16);
                let hosting = self.clone();
                let task = tokio::spawn(async move {
                    if let Err(error) = hosting.host(agent, receive).await {
                        tracing::warn!(%agent, error = %swarmy_core::error_chain(&*error), "agent hosting stopped");
                    }
                });
                Entry {
                    calls,
                    task,
                    activity: Arc::default(),
                }
            });
            (
                entry.calls.clone(),
                ActivityGuard::queued(entry.activity.clone()),
            )
        };
        calls
            .send(Call {
                job,
                delivered_ns,
                reply,
                activity,
            })
            .await
            .context("agent stopped serving; placement lease lost")?;
        response
            .await
            .context("agent stopped serving; placement lease lost")?
    }

    #[expect(
        clippy::cast_precision_loss,
        reason = "fetch histogram reads are approximate, so floating-point display precision is sufficient"
    )]
    fn report_ready(
        &self,
        placement: PlacementRecord,
        volume: VolumeId,
        session: SessionId,
        turn: Option<MessageId>,
        times: BootTimes,
    ) {
        let client = self.client.clone();
        let runtime = self.runtime.clone();
        tokio::spawn(async move {
            let metrics = if let Some(turn) = turn {
                if let Ok(stats) = runtime.volume_stats(volume).await {
                    vec![MetricBatch {
                        session,
                        turn,
                        patches: vec![NodeMetric::Computer(swarmy_core::ComputerMetric {
                            placement_ms: Some(times.placement),
                            open_ms: Some(times.open),
                            boot_ms: Some(times.boot),
                            cold: Some(stats.fetched_chunks > 0),
                            chunks_fetched: stats.fetched_chunks,
                            bytes_fetched: stats.fetched_bytes,
                            fetch_p50_ms: stats.fetch_p50_us.map(|us| us as f64 / 1_000.0),
                            fetch_p95_ms: stats.fetch_p95_us.map(|us| us as f64 / 1_000.0),
                            ..Default::default()
                        })],
                    }]
                } else {
                    vec![]
                }
            } else {
                vec![]
            };
            if let Err(error) = client
                .hosting_ready(
                    &HostingReadyRequest {
                        placement,
                        address: RuncRuntime::NETWORK_ADDRESS,
                        metrics,
                    },
                    Instant::now() + DEFAULT_DEADLINE,
                )
                .await
            {
                tracing::warn!(error = %swarmy_core::error_chain(&error), "hosting ready report failed");
            }
        });
    }

    async fn boot_computer(
        &self,
        opening: &mut Opening,
        placement_started: Instant,
        open_ms: f64,
    ) -> Result<BootTimes> {
        let grant = WriterGrant {
            volume: opening.volume,
            head: opening.head,
            header: opening.header.clone(),
            lease: opening.writer.take().context("writer grant already used")?,
            renewal: None,
        };
        let booting = Instant::now();
        self.runtime
            .create(
                opening.spec.clone(),
                BlockDevice {
                    volume_id: opening.volume,
                },
                grant,
                opening.boot_set.clone(),
            )
            .await?;
        let boot_ms = booting.elapsed().as_secs_f64() * 1_000.0;
        Ok(BootTimes {
            placement: placement_started.elapsed().as_secs_f64() * 1_000.0,
            open: open_ms,
            boot: boot_ms,
        })
    }

    /// Claim the first call while its sandbox boots, then report readiness.
    async fn first_call(
        &self,
        opening: &mut Opening,
        first: Call,
        placement_started: Instant,
        open_ms: f64,
    ) -> Result<()> {
        let placement = opening.placement.clone();
        let deadline = opening
            .hold
            .deadline()
            .context("placement lease lost before the first claim")?;
        let (booted, claimed) = tokio::join!(
            self.boot_computer(opening, placement_started, open_ms),
            crate::tools::claim(
                &self.client,
                &self.keeper,
                &placement,
                first.job.clone(),
                deadline
            ),
        );
        let times = booted?;
        let turn = claimed
            .as_ref()
            .ok()
            .and_then(|c| c.as_ref())
            .and_then(|c| c.turn);
        self.report_ready(
            placement.clone(),
            opening.volume,
            first.job.session_id,
            turn,
            times,
        );
        let session = first.job.session_id;
        self.finish(&placement, first, claimed).await?;
        self.record_boot_set(opening, session);
        Ok(())
    }

    /// Record after the first committed call without delaying subsequent calls.
    fn record_boot_set(&self, opening: &Opening, session: SessionId) {
        let runtime = self.runtime.clone();
        let client = self.client.clone();
        let placement = opening.placement.clone();
        let volume = opening.volume;
        let head = opening.head;
        tokio::spawn(async move {
            let record = async {
                let Some((key, set)) = store_boot_set(&runtime, volume).await? else {
                    return Ok::<_, anyhow::Error>(());
                };
                client
                    .report_boot_set(
                        &HostingBootSetRequest {
                            placement: placement.clone(),
                            session,
                            head,
                            key: key.clone(),
                        },
                        Instant::now() + DEFAULT_DEADLINE,
                    )
                    .await?;
                tracing::info!(agent = %placement.agent_id, chunks = set.chunks.len(), leaves = set.leaves.len(), %key, "boot set recorded");
                Ok(())
            };
            match tokio::time::timeout(Duration::from_secs(30), record).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(error = %swarmy_core::error_chain(&*error), "boot set recording failed");
                }
                Err(error) => {
                    tracing::warn!(error = %swarmy_core::error_chain(&error), "boot set recording failed");
                }
            }
        });
    }

    /// A requirement release stops hosting between calls even if processes run:
    /// card devices belong to the sandbox created for the old requirement.
    async fn host(&self, agent: AgentId, mut calls: mpsc::Receiver<Call>) -> Result<()> {
        let Some(mut first) = calls.recv().await else {
            return Ok(());
        };
        first.activity.start(first.job.session_id);
        let placement_started = Instant::now();
        let mut opening = match self
            .open(agent, &first.job, first.activity.state.clone())
            .await
        {
            Ok(Some(opening)) => opening,
            Ok(None) => {
                ignore_best_effort(first.reply.send(Ok(())), "reply to completed call");
                return Ok(());
            }
            Err(error) => {
                ignore_best_effort(first.reply.send(Err(error)), "reply to waiter");
                return Ok(());
            }
        };
        let open_ms = placement_started.elapsed().as_secs_f64() * 1_000.0;
        let mut shutdown = self.shutdown.subscribe();
        let mut released = opening.hold.clone();
        let mut lost = opening.hold.clone();
        let lost = lost.lost();
        tokio::pin!(lost);
        let serving = async {
            anyhow::ensure!(!*shutdown.borrow(), "node is shutting down");
            self.first_call(&mut opening, first, placement_started, open_ms)
                .await?;

            loop {
                if *shutdown.borrow() || opening.hold.release_requested() {
                    break;
                }
                tokio::select! {
                    call = calls.recv() => {
                        let Some(mut call) = call else { break; };
                        call.activity.start(call.job.session_id);
                        self.execute(&opening.placement, &opening.hold, call).await?;
                    }
                    () = tokio::time::sleep(self.idle) => {
                        if !crate::tools::has_processes(&self.runtime, &opening.placement).await? { break; }
                    },
                    _ = shutdown.changed() => break,
                    () = released.released() => break,
                }
            }
            Ok::<_, anyhow::Error>(())
        };
        let serving = tokio::select! {
            result = serving => result,
            reason = &mut lost => {
                self.runtime.discard_if_present(agent).await?;
                return Err(anyhow::anyhow!("placement lease lost: {reason}"));
            }
        };
        // Keep renewing throughout the final checkpoint, which can take longer
        // than a lease on a heavily written disk.
        let cause = if opening.hold.release_requested() {
            "requirement change"
        } else {
            "eviction"
        };
        let cleanup = self.stop_and_release(agent, &opening, cause);
        tokio::select! {
            result = cleanup => {
                if let Err(error) = result {
                    self.runtime.discard_if_present(agent).await?;
                    return Err(error);
                }
            },
            reason = &mut lost => {
                self.runtime.discard_if_present(agent).await?;
                return Err(anyhow::anyhow!("placement lease lost: {reason}"));
            }
        }
        serving
    }

    /// Stop the agent's local sandbox and release its placement. A sandbox that
    /// exists is destroyed, which publishes its final checkpoint; if that fails
    /// the placement is kept so its expiry reports a possible computer loss.
    /// A missing sandbox means the boot never finished and no guest command
    /// ran, so once any partial local state is discarded nothing can still
    /// write the disk and the placement is released too.
    async fn stop_and_release(
        &self,
        agent: AgentId,
        opening: &Opening,
        cause: &'static str,
    ) -> Result<()> {
        let writer = self.keeper.writer_lease(opening.volume);
        let hosted = self
            .runtime
            .list()
            .await
            .iter()
            .any(|sandbox| sandbox.agent_id == agent);
        if hosted {
            self.runtime
                .destroy(swarmy_core::Sandbox { agent_id: agent })
                .await?;
        } else {
            self.runtime.discard_if_present(agent).await?;
        }
        let deadline = opening
            .hold
            .deadline()
            .context("placement lease lost before close")?;
        self.client
            .close_hosting(
                &HostingCloseRequest {
                    placement: opening
                        .hold
                        .current()
                        .context("placement lease lost before close")?,
                    writer: writer.map(|lease| WriterToken {
                        volume: opening.volume,
                        lease,
                    }),
                },
                deadline,
            )
            .await?;
        self.heads.forget(opening.volume);
        self.previous.lock().await.remove(&agent);
        self.computer_sampled.lock().await.remove(&agent);
        tracing::info!(%agent, epoch = opening.placement.epoch, reason = cause, booted = hosted, "placement released");
        Ok(())
    }

    async fn execute(
        &self,
        placement: &PlacementRecord,
        hold: &PlacementHold,
        call: Call,
    ) -> Result<()> {
        let claimed = match hold.deadline() {
            Some(deadline) => {
                crate::tools::claim(
                    &self.client,
                    &self.keeper,
                    placement,
                    call.job.clone(),
                    deadline,
                )
                .await
            }
            None => Err(anyhow::anyhow!("placement lease lost")),
        };
        self.finish(placement, call, claimed).await
    }

    async fn finish(
        &self,
        placement: &PlacementRecord,
        call: Call,
        claimed: Result<Option<Claimed>>,
    ) -> Result<()> {
        let claimed = match claimed {
            Ok(Some(claimed)) => claimed,
            Ok(None) => {
                ignore_best_effort(call.reply.send(Ok(())), "reply to completed call");
                return Ok(());
            }
            Err(error) => {
                ignore_best_effort(call.reply.send(Err(error)), "reply to waiter");
                bail!("tool execution interrupted; stopping agent");
            }
        };
        let mut shutdown = self.shutdown.subscribe();
        anyhow::ensure!(!*shutdown.borrow(), "node is shutting down");
        // Only the first completion per turn carries the computer re-sample;
        // later tools skip the volume stat read entirely. One entry per
        // agent keeps the map bounded for the life of the node process.
        let needs_sample = match claimed.turn {
            Some(turn) => {
                self.computer_sampled.lock().await.get(&placement.agent_id) != Some(&turn)
            }
            None => false,
        };
        let turn = claimed.turn;
        let metrics = crate::tools::CallMetrics {
            turn,
            needs_computer_sample: needs_sample,
            delivered_ns: call.delivered_ns,
            claimed_ns: claimed.stamp_ns,
        };
        let result = tokio::select! {
            result = crate::tools::run(
                &self.client, &self.keeper, self.runtime.clone(), &self.heads, claimed, metrics,
            ) => result,
            _ = shutdown.changed() => Err(anyhow::anyhow!("node is shutting down")),
        };
        if needs_sample
            && let Some(turn) = turn
            && result.is_ok()
        {
            self.computer_sampled
                .lock()
                .await
                .insert(placement.agent_id, turn);
        }
        let failed = result.is_err();
        ignore_best_effort(call.reply.send(result), "reply to waiter");
        if failed {
            bail!("tool execution interrupted; stopping agent");
        }
        Ok(())
    }

    pub(crate) async fn shutdown(&self) {
        self.shutdown.send_replace(true);
        let entries = std::mem::take(&mut *self.entries.lock().await);
        for (_, entry) in entries {
            ignore_best_effort(entry.task.await, "await background task");
        }
        self.computer_sampled.lock().await.clear();
        self.routes.lock().await.clear();
    }
}

pub(crate) fn refusal_error(refusal: NodeRefusal) -> anyhow::Error {
    match refusal {
        NodeRefusal::PlacedElsewhere { node, epoch } => {
            anyhow::anyhow!("agent is placed on another node {node} at epoch {epoch}")
        }
        NodeRefusal::EpochStopped { epoch } => anyhow::anyhow!(
            "placement epoch {epoch} stopped; waiting for lease expiry before rebuilding"
        ),
        other => anyhow::anyhow!("node API refused: {other}"),
    }
}

#[cfg(test)]
#[path = "hosting_tests.rs"]
pub(crate) mod tests;
