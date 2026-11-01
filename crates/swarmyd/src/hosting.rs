use crate::keeper::{Answered, Keeper, Occupancy, PlacementHold};
use anyhow::{Context, Result, bail};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use swarmy_core::{
    AgentId, BlockDevice, ManifestHeader, ManifestId, MessageId, NodeId, PlacementRecord,
    SandboxSpec, SessionId, ToolJob, VolumeId, ignore_best_effort,
};
use swarmy_node_protocol::{
    ClientError, DEFAULT_DEADLINE, HostingBootSetRequest, HostingCloseRequest, HostingOpenRequest,
    HostingReadyRequest, MetricBatch, NodeClient, NodeMetric, NodeRefusal, ToolRouteRequest,
    WriterToken,
};
use swarmy_sandbox::RuncRuntime;
use swarmy_store::Store;
use swarmy_volume::{WriterGrant, WriterLease};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::time::Instant;

#[derive(Clone, Copy)]
struct BootTimes {
    placement: f64,
    open: f64,
    boot: f64,
}

struct Call {
    job: ToolJob,
    turn: Option<MessageId>,
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
    store: Store,
    runtime: Arc<RuncRuntime>,
    client: NodeClient,
    keeper: Keeper,
    node: NodeId,
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
        store: Store,
        client: NodeClient,
        keeper: Keeper,
        runtime: Arc<RuncRuntime>,
        node: NodeId,
        settings: &swarmy_config::Settings,
        previous: &[PlacementRecord],
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            client,
            keeper,
            runtime,
            node,
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

    /// Serve one tool call with its durable turn already resolved by the
    /// caller, so the execution path needs no `request_turn_id` lookup.
    pub(crate) async fn call(
        self: &Arc<Self>,
        job: ToolJob,
        turn: Option<MessageId>,
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
                turn,
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
        first: &Call,
        placement_started: Instant,
        open_ms: f64,
    ) -> Result<()> {
        let grant = WriterGrant {
            volume: opening.volume,
            head: opening.head,
            header: opening.header.clone(),
            lease: opening.writer.take().context("writer grant already used")?,
            renewal: None,
        };
        let booting = Instant::now();
        self.runtime
            .create_with(
                opening.spec.clone(),
                BlockDevice {
                    volume_id: opening.volume,
                },
                grant,
                opening.boot_set.clone(),
            )
            .await?;
        let boot_ms = booting.elapsed().as_secs_f64() * 1_000.0;
        self.report_ready(
            opening.placement.clone(),
            opening.volume,
            first.job.session_id,
            first.turn,
            BootTimes {
                placement: placement_started.elapsed().as_secs_f64() * 1_000.0,
                open: open_ms,
                boot: boot_ms,
            },
        );
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
                let set = runtime.boot_set(volume).await?;
                if set.chunks.is_empty() {
                    return Ok::<_, anyhow::Error>(());
                }
                let key = swarmy_volume::put_boot_set(runtime.objects(), &set).await?;
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
                    tracing::warn!(error = %swarmy_core::error_chain(&*error), "boot set recording failed")
                }
                Err(error) => {
                    tracing::warn!(error = %swarmy_core::error_chain(&error), "boot set recording failed")
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
            self.boot_computer(&mut opening, &first, placement_started, open_ms)
                .await?;
            let session = first.job.session_id;
            self.execute(&opening.placement, first).await?;
            self.record_boot_set(&opening, session);
            loop {
                if *shutdown.borrow() || opening.hold.release_requested() {
                    break;
                }
                tokio::select! {
                    call = calls.recv() => {
                        let Some(mut call) = call else { break; };
                        call.activity.start(call.job.session_id);
                        self.execute(&opening.placement, call).await?;
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
        self.previous.lock().await.remove(&agent);
        self.computer_sampled.lock().await.remove(&agent);
        tracing::info!(%agent, epoch = opening.placement.epoch, reason = cause, booted = hosted, "placement released");
        Ok(())
    }

    async fn execute(&self, placement: &PlacementRecord, call: Call) -> Result<()> {
        let mut shutdown = self.shutdown.subscribe();
        anyhow::ensure!(!*shutdown.borrow(), "node is shutting down");
        // Only the first completion per turn carries the computer re-sample;
        // later tools skip the volume stat read entirely. One entry per
        // agent keeps the map bounded for the life of the node process.
        let needs_sample = match call.turn {
            Some(turn) => {
                self.computer_sampled.lock().await.get(&placement.agent_id) != Some(&turn)
            }
            None => false,
        };
        let turn = call.turn;
        let metrics = crate::tools::CallMetrics {
            turn,
            needs_computer_sample: needs_sample,
            delivered_ns: call.delivered_ns,
            claimed_ns: None,
        };
        let result = tokio::select! {
            result = crate::tools::execute(
                &self.store,
                &self.keeper,
                self.runtime.clone(),
                self.node,
                placement,
                call.job,
                metrics,
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

fn refusal_error(refusal: NodeRefusal) -> anyhow::Error {
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
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use crate::keeper::{Answered, Occupancy};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::{num::NonZeroUsize, path::Path};
    use swarmy_bus::{Bus, Config, SubjectToken};
    use swarmy_core::{NodeCapacity, NodeRecord, NodeRole, PlacementChangeReason, VolumeId};
    use swarmy_node_api::test_support::drop_first_response;
    use swarmy_node_api::{NodeApiPolicy, NodeApiState, router, test_support::TestApi};
    use swarmy_node_protocol::{NodeAdvert, RegisterRequest, SYNC_PATH};

    async fn node_api(
        store: &Store,
        stack: &swarmy_testkit::Stack,
        record: &NodeRecord,
        fault: bool,
    ) -> (NodeClient, Keeper, TestApi, Arc<AtomicUsize>) {
        let token = store.issue_node_credential(record.node_id).await.unwrap();
        let bus = Bus::connect(
            &stack.nats_url,
            Config {
                prefix: Some(SubjectToken::new(&stack.prefix).unwrap()),
                ..Config::default()
            },
        )
        .await
        .unwrap();
        let router = router(NodeApiState::new(
            store.clone(),
            bus,
            NodeApiPolicy {
                placement_lease: Duration::from_secs(60),
                snapshot_retention: NonZeroUsize::new(10).unwrap(),
                resend_interval: Duration::from_secs(5),
            },
        ));
        let (router, count) = if fault {
            drop_first_response(router, SYNC_PATH)
        } else {
            (router, Arc::new(AtomicUsize::new(0)))
        };
        let api = TestApi::start(router).await;
        let client = NodeClient::new(api.url(), &token).unwrap();
        let advert = NodeAdvert {
            roles: record.roles.clone(),
            capacity: record.capacity.clone(),
            gpu: None,
        };
        let registered = client
            .register(&RegisterRequest {
                node: advert.clone(),
            })
            .await
            .unwrap();
        let keeper = Keeper::new(
            Arc::new(client.clone()),
            advert,
            registered.leases,
            Duration::from_secs(5),
        );
        (client, keeper, api, count)
    }

    async fn placed(
        store: &Store,
        stack: &swarmy_testkit::Stack,
        root: &Path,
    ) -> (Arc<Hosting>, AgentId, PlacementRecord, Opening, TestApi) {
        let node_id = NodeId::from_ulid(ulid::Ulid::generate());
        let record = NodeRecord {
            node_id,
            roles: vec![NodeRole::Sandbox],
            capacity: NodeCapacity {
                cpu_millis: 1000,
                memory_bytes: 4 * 1024 * 1024 * 1024,
                disk_bytes: 1024,
                sandboxes: 1,
            },
            last_heartbeat: store.now(),
            cached_images: vec![],
            gpu: None,
        };
        store.put_node(&record).await.unwrap();
        let (client, keeper, api, _) = node_api(store, stack, &record, false).await;
        let agent = AgentId::from_ulid(ulid::Ulid::generate());
        let placement = store
            .place(
                agent,
                node_id,
                store.now().checked_add(Duration::from_secs(60)).unwrap(),
            )
            .await
            .unwrap();
        store.claim_placement(&placement).await.unwrap();
        let runtime = RuncRuntime::open(
            root.join("runtime"),
            swarmy_volume::server::ServerConfig {
                directory: root.join("volumes"),
                node: node_id,
                metadata: Arc::new(store.clone()),
                objects: Arc::new(object_store::memory::InMemory::new()),
            },
            swarmy_store::StoreSandboxMetadata::new(store.clone(), node_id),
        )
        .await
        .unwrap();
        let settings = swarmy_config::Settings::default();
        let hosting = Hosting::new(
            store.clone(),
            client,
            keeper.clone(),
            Arc::new(runtime),
            node_id,
            &settings,
            std::slice::from_ref(&placement),
        );
        let answered = Answered {
            sent: Instant::now(),
            server_now: store.now(),
        };
        let hold = keeper.hold_placement(
            placement.clone(),
            answered,
            Arc::new(std::sync::Mutex::new(Occupancy::default())),
        );
        let volume = VolumeId::from_ulid(agent.as_ulid());
        let head = ManifestId::from_ulid(ulid::Ulid::generate());
        let header = ManifestHeader {
            size: u64::from(swarmy_core::CHUNK_SIZE),
            chunk_size: swarmy_core::CHUNK_SIZE,
            root_hash: swarmy_core::ContentHash::ZERO,
        };
        store.put_manifest(head, &header).await.unwrap();
        store.create_volume(volume, head).await.unwrap();
        let writer_lease = store
            .open_writer_lease(
                volume,
                swarmy_core::LeaseOwnerId::from(node_id),
                Duration::from_secs(60),
            )
            .await
            .unwrap()
            .lease;
        let writer = keeper.hold_writer(volume, writer_lease, answered);
        let opening = Opening {
            placement: placement.clone(),
            hold,
            volume,
            writer: Some(writer),
            head,
            header,
            spec: SandboxSpec {
                agent_id: agent,
                scratch: vec![],
                requirements: swarmy_core::SandboxRequirements::default(),
            },
        };
        (hosting, agent, placement, opening, api)
    }

    #[tokio::test]
    async fn cleanup_without_a_sandbox_releases_the_placement() {
        let Some(stack) = swarmy_testkit::Stack::load("hosting-release") else {
            return;
        };
        let (store, mut guard) = stack
            .open_store(Arc::new(swarmy_store::blob::MemoryBlobStore::default()))
            .await;
        let root = tempfile::tempdir().unwrap();
        let (hosting, agent, placement, opening, _api) = placed(&store, &stack, root.path()).await;
        hosting
            .stop_and_release(agent, &opening, "eviction")
            .await
            .unwrap();
        assert!(store.get_by_agent(agent).await.unwrap().is_none());
        assert!(!hosting.previous.lock().await.contains_key(&agent));
        assert!(
            store
                .get_volume(opening.volume)
                .await
                .unwrap()
                .unwrap()
                .writer_lease
                .is_none()
        );
        let next = store
            .place(
                agent,
                placement.node_id,
                jiff::Timestamp::now()
                    .checked_add(Duration::from_secs(60))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(next.epoch, placement.epoch + 1);
        assert_eq!(next.last_change_reason, PlacementChangeReason::Eviction);
        guard.cleanup().await;
    }

    #[tokio::test]
    async fn cleanup_keeps_fencing_when_the_placement_is_stale() {
        let Some(stack) = swarmy_testkit::Stack::load("hosting-release") else {
            return;
        };
        let (store, mut guard) = stack
            .open_store(Arc::new(swarmy_store::blob::MemoryBlobStore::default()))
            .await;
        let root = tempfile::tempdir().unwrap();
        let (hosting, agent, placement, opening, _api) = placed(&store, &stack, root.path()).await;
        store.release(&placement).await.unwrap();
        let replacement = store
            .place(
                agent,
                placement.node_id,
                jiff::Timestamp::now()
                    .checked_add(Duration::from_secs(60))
                    .unwrap(),
            )
            .await
            .unwrap();
        let error = hosting
            .stop_and_release(agent, &opening, "eviction")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("fenced"), "{error}");
        assert_eq!(store.get_by_agent(agent).await.unwrap(), Some(replacement));
        assert_eq!(
            hosting.previous.lock().await.get(&agent),
            Some(&placement.epoch)
        );
        guard.cleanup().await;
    }
    #[tokio::test]
    async fn keeper_renews_on_the_server_clock() {
        let Some(stack) = swarmy_testkit::Stack::load("hosting-server-clock") else {
            return;
        };
        let (store, mut guard) = stack
            .open_store(Arc::new(swarmy_store::blob::MemoryBlobStore::default()))
            .await;
        let clock = Arc::new(std::sync::Mutex::new(
            jiff::Timestamp::now()
                .checked_sub(Duration::from_secs(3600))
                .unwrap(),
        ));
        let manual = clock.clone();
        let store = store.with_clock(move || *manual.lock().unwrap());
        let root = tempfile::tempdir().unwrap();
        let (hosting, agent, placement, opening, _api) = placed(&store, &stack, root.path()).await;
        *clock.lock().unwrap() = placement
            .expires_at
            .checked_sub(Duration::from_secs(50))
            .unwrap();
        hosting.keeper.sync_once().await.unwrap();
        let stored = store.get_by_agent(agent).await.unwrap().unwrap();
        assert_eq!(
            stored.expires_at,
            clock
                .lock()
                .unwrap()
                .checked_add(Duration::from_secs(60))
                .unwrap()
        );
        let remaining = opening
            .hold
            .deadline()
            .unwrap()
            .duration_since(Instant::now());
        assert!(
            remaining > Duration::from_secs(53) && remaining <= Duration::from_secs(54),
            "{remaining:?}"
        );
        guard.cleanup().await;
    }
    #[tokio::test]
    async fn keeper_rides_out_a_node_api_outage() {
        let Some(stack) = swarmy_testkit::Stack::load("hosting-api-outage") else {
            return;
        };
        let (store, mut guard) = stack
            .open_store(Arc::new(swarmy_store::blob::MemoryBlobStore::default()))
            .await;
        let root = tempfile::tempdir().unwrap();
        let (hosting, agent, old, opening, mut api) = placed(&store, &stack, root.path()).await;
        api.pause();
        let mut sync = Box::pin(hosting.keeper.sync_once());
        assert!(
            tokio::time::timeout(Duration::from_secs(2), &mut sync)
                .await
                .is_err()
        );
        api.resume().await;
        tokio::time::timeout(Duration::from_secs(10), sync)
            .await
            .unwrap()
            .unwrap();
        let current = store.get_by_agent(agent).await.unwrap().unwrap();
        assert!(current.expires_at > old.expires_at);
        assert!(opening.hold.deadline().is_some());
        guard.cleanup().await;
    }
    #[tokio::test]
    async fn hosting_remembers_registered_epochs() {
        let Some(stack) = swarmy_testkit::Stack::load("hosting-previous") else {
            return;
        };
        let (store, mut guard) = stack
            .open_store(Arc::new(swarmy_store::blob::MemoryBlobStore::default()))
            .await;
        let root = tempfile::tempdir().unwrap();
        let (hosting, agent, placement, _opening, _api) = placed(&store, &stack, root.path()).await;
        assert_eq!(
            hosting.previous.lock().await.get(&agent),
            Some(&placement.epoch)
        );
        guard.cleanup().await;
    }
    #[tokio::test]
    async fn sync_retry_after_a_dropped_answer() {
        let Some(stack) = swarmy_testkit::Stack::load("hosting-sync-retry") else {
            return;
        };
        let (store, mut guard) = stack
            .open_store(Arc::new(swarmy_store::blob::MemoryBlobStore::default()))
            .await;
        let node_id = NodeId::from_ulid(ulid::Ulid::generate());
        let record = NodeRecord {
            node_id,
            roles: vec![NodeRole::Sandbox],
            capacity: NodeCapacity {
                cpu_millis: 1000,
                memory_bytes: 4 * 1024 * 1024 * 1024,
                disk_bytes: 1000,
                sandboxes: 1,
            },
            last_heartbeat: store.now(),
            cached_images: vec![],
            gpu: None,
        };
        store.put_node(&record).await.unwrap();
        let (_client, keeper, _api, count) = node_api(&store, &stack, &record, true).await;
        let agent = AgentId::from_ulid(ulid::Ulid::generate());
        let placement = store
            .place(
                agent,
                node_id,
                store.now().checked_add(Duration::from_secs(60)).unwrap(),
            )
            .await
            .unwrap();
        store.claim_placement(&placement).await.unwrap();
        let hold = keeper.hold_placement(
            placement,
            Answered {
                sent: Instant::now(),
                server_now: store.now(),
            },
            Arc::default(),
        );
        keeper.sync_once().await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert_eq!(hold.current(), store.get_by_agent(agent).await.unwrap());
        guard.cleanup().await;
    }
}
