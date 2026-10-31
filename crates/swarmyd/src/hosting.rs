use anyhow::{Context, Result, bail};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use swarmy_core::{
    AgentCallStatus, AgentId, BlockDevice, MessageId, NodeId, PlacementRecord, SandboxSpec,
    SessionId, ToolJob, ignore_best_effort,
};
use swarmy_sandbox::RuncRuntime;
use swarmy_store::Store;
use tokio::sync::{Mutex, mpsc, oneshot, watch};

struct Call {
    job: ToolJob,
    turn: Option<MessageId>,
    reply: oneshot::Sender<Result<()>>,
    activity: ActivityGuard,
}
struct Entry {
    calls: mpsc::Sender<Call>,
    task: tokio::task::JoinHandle<()>,
    activity: Arc<std::sync::Mutex<Activity>>,
}

#[derive(Default)]
struct Activity {
    placement: Option<PlacementRecord>,
    holder: Option<SessionId>,
    queued: u64,
}

// The guard follows the call through admission, boot and execution. Cancellation
// and dropped queues must release occupancy even when no reply can be sent.
struct ActivityGuard {
    state: Arc<std::sync::Mutex<Activity>>,
    active: bool,
}

impl ActivityGuard {
    fn queued(state: Arc<std::sync::Mutex<Activity>>) -> Self {
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

/// Each actor serializes calls for one agent while its renewal runs independently.
pub(crate) struct Hosting {
    store: Store,
    runtime: Arc<RuncRuntime>,
    node: NodeId,
    lease: Duration,
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
    async fn sandbox_spec(&self, agent: AgentId, session: SessionId) -> Result<SandboxSpec> {
        Ok(SandboxSpec {
            agent_id: agent,
            scratch: self.scratch_for_session(session).await?,
            requirements: self.requirements_for_session(agent, session).await?,
        })
    }

    async fn requirements_for_session(
        &self,
        agent: AgentId,
        session: SessionId,
    ) -> Result<swarmy_core::SandboxRequirements> {
        if let Some(record) = self.store.get_agent(agent).await? {
            return Ok(record.requirements);
        }
        if let Some(image) = self.store.pinned_image(session).await? {
            return Ok(swarmy_core::SandboxRequirements {
                memory_mib: self.store.image_memory(&image).await?.unwrap_or(768),
                gpu: swarmy_core::GpuRequirement::default(),
            });
        }
        Ok(swarmy_core::SandboxRequirements::default())
    }

    async fn scratch_for_session(&self, session: SessionId) -> Result<Vec<String>> {
        if let Some(image) = self.store.pinned_image(session).await? {
            Ok(self.store.image_scratch(&image).await?)
        } else {
            Ok(Vec::new())
        }
    }
    pub(crate) async fn new(
        store: Store,
        runtime: Arc<RuncRuntime>,
        node: NodeId,
        settings: &swarmy_config::Settings,
    ) -> Result<Arc<Self>> {
        let mut previous = BTreeMap::new();
        let mut cursor = None;
        loop {
            let page = store
                .list_by_node(node, cursor, swarmy_store::MAX_SCAN_LIMIT)
                .await?;
            if page.is_empty() {
                break;
            }
            cursor = page.last().map(|p| p.agent_id);
            previous.extend(page.into_iter().map(|p| (p.agent_id, p.epoch)));
        }
        Ok(Arc::new(Self {
            store,
            runtime,
            node,
            lease: settings.scheduler.placement_lease_secs,
            idle: settings.sandbox.idle_secs,
            entries: Mutex::new(BTreeMap::new()),
            previous: Mutex::new(previous),
            computer_sampled: Mutex::new(BTreeMap::new()),
            shutdown: watch::channel(false).0,
        }))
    }

    /// Serve one tool call with its durable turn already resolved by the
    /// caller, so the execution path needs no `request_turn_id` lookup.
    pub(crate) async fn call(
        self: &Arc<Self>,
        job: ToolJob,
        turn: Option<MessageId>,
    ) -> Result<()> {
        let agent = match self.store.tool_agent(&job, self.node).await {
            Ok(Some(agent)) => agent,
            Ok(None) => return Ok(()),
            // The store's fence check is the guard; read placements afterwards
            // only to explain a refusal for a job fenced to another node.
            Err(swarmy_store::StoreError::Fence(swarmy_store::FenceError::PlacementMismatch)) => {
                if let Some(dispatched) = self.store.tool_placement(job.request_id).await? {
                    let current = self.store.get_by_agent(dispatched.agent_id).await?;
                    if let Some(refusal) =
                        crate::tools::placement_refusal(current.as_ref(), &dispatched, self.node)
                    {
                        return Err(refusal);
                    }
                }
                return Err(swarmy_store::StoreError::Fence(
                    swarmy_store::FenceError::PlacementMismatch,
                )
                .into());
            }
            Err(error) => return Err(error.into()),
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
                reply,
                activity,
            })
            .await
            .context("agent stopped serving; placement lease lost")?;
        response
            .await
            .context("agent stopped serving; placement lease lost")?
    }

    async fn placement(&self, agent: AgentId, job: &ToolJob) -> Result<PlacementRecord> {
        let dispatched = self.store.tool_placement(job.request_id).await?;
        let expiry = jiff::Timestamp::now().checked_add(self.lease)?;
        let placement = match self.store.get_by_agent(agent).await? {
            None => {
                anyhow::ensure!(dispatched.is_none(), "dispatch placement was released");
                self.store.place(agent, self.node, expiry).await?
            }
            Some(old) if old.expires_at <= jiff::Timestamp::now() => {
                anyhow::ensure!(
                    dispatched.is_none(),
                    "dispatch placement expired; worker must recover the call"
                );
                self.store.take_over(&old, self.node, expiry).await?
            }
            Some(old) if old.node_id != self.node => bail!(
                "agent is placed on another node {} at epoch {}",
                old.node_id,
                old.epoch
            ),
            Some(old) => {
                anyhow::ensure!(
                    dispatched
                        .as_ref()
                        .is_none_or(|expected| expected.epoch == old.epoch),
                    "dispatch placement epoch changed"
                );
                if self.previous.lock().await.get(&agent) == Some(&old.epoch) {
                    bail!(
                        "placement epoch {} stopped; waiting for lease expiry before rebuilding",
                        old.epoch
                    );
                }
                self.store
                    .renew(
                        &old,
                        expiry.max(old.expires_at.checked_add(Duration::from_millis(1))?),
                    )
                    .await?
            }
        };
        self.store.claim_placement(&placement).await?;
        self.previous.lock().await.insert(agent, placement.epoch);
        Ok(placement)
    }

    #[expect(
        clippy::cast_precision_loss,
        reason = "fetch histogram reads are approximate, so floating-point display precision is sufficient"
    )]
    fn observe_computer_boot(
        &self,
        session: SessionId,
        turn: Option<MessageId>,
        volume: swarmy_core::VolumeId,
        elapsed: f64,
    ) {
        let Some(turn) = turn else { return };
        let store = self.store.clone();
        let runtime = self.runtime.clone();
        tokio::spawn(async move {
            if let Ok(stats) = runtime.volume_stats(volume).await {
                store.observe_turn_metric(
                    session,
                    turn,
                    swarmy_store::MetricPatch::Computer(swarmy_store::ComputerMetric {
                        placement_ms: Some(elapsed),
                        cold: Some(stats.fetched_chunks > 0),
                        chunks_fetched: stats.fetched_chunks,
                        bytes_fetched: stats.fetched_bytes,
                        fetch_p50_ms: stats.fetch_p50_us.map(|us| us as f64 / 1_000.0),
                        fetch_p95_ms: stats.fetch_p95_us.map(|us| us as f64 / 1_000.0),
                        ..Default::default()
                    }),
                );
            }
        });
    }

    async fn boot_computer(
        &self,
        agent: AgentId,
        placement: &PlacementRecord,
        first: &Call,
        placement_started: Instant,
    ) -> Result<()> {
        let volume = self
            .store
            .agent_volume(first.job.session_id, placement)
            .await?;
        self.runtime
            .create(
                self.sandbox_spec(agent, first.job.session_id).await?,
                BlockDevice { volume_id: volume },
            )
            .await?;
        self.store
            .set_placement_address(placement, RuncRuntime::NETWORK_ADDRESS)
            .await?;
        self.observe_computer_boot(
            first.job.session_id,
            first.turn,
            volume,
            placement_started.elapsed().as_secs_f64() * 1_000.0,
        );
        Ok(())
    }

    /// A requirement release stops hosting between calls even if processes run:
    /// card devices belong to the sandbox created for the old requirement.
    async fn host(&self, agent: AgentId, mut calls: mpsc::Receiver<Call>) -> Result<()> {
        let Some(mut first) = calls.recv().await else {
            return Ok(());
        };
        first.activity.start(first.job.session_id);
        let placement_started = Instant::now();
        let placement = match self.placement(agent, &first.job).await {
            Ok(placement) => placement,
            Err(error) => {
                ignore_best_effort(first.reply.send(Err(error)), "reply to waiter");
                return Ok(());
            }
        };
        first
            .activity
            .state
            .lock()
            .expect("activity lock poisoned")
            .placement = Some(placement.clone());
        let mut shutdown = self.shutdown.subscribe();
        let (release, mut released) = watch::channel(false);
        let serving = async {
            anyhow::ensure!(!*shutdown.borrow(), "node is shutting down");
            self.boot_computer(agent, &placement, &first, placement_started)
                .await?;
            self.execute(&placement, first).await?;
            loop {
                if *shutdown.borrow() || *released.borrow() {
                    break;
                }
                tokio::select! {
                    call = calls.recv() => {
                        let Some(mut call) = call else { break; };
                        call.activity.start(call.job.session_id);
                        self.execute(&placement, call).await?;
                    }
                    () = tokio::time::sleep(self.idle) => {
                        if !crate::tools::has_processes(&self.runtime, &placement).await? { break; }
                    },
                    _ = shutdown.changed() => break,
                    _ = released.changed() => break,
                }
            }
            Ok::<_, anyhow::Error>(())
        };
        let renew = self.renew(placement.clone(), &release);
        tokio::pin!(renew);
        let serving = tokio::select! {
            result = serving => result,
            error = &mut renew => {
                self.runtime.discard_if_present(agent).await?;
                return Err(error);
            }
        };
        // Keep renewing throughout the final checkpoint, which can take longer
        // than a lease on a heavily written disk.
        let cause = if *released.borrow() {
            "requirement change"
        } else {
            "eviction"
        };
        let cleanup = self.stop_and_release(agent, &placement, cause);
        tokio::select! {
            result = cleanup => {
                if let Err(error) = result {
                    self.runtime.discard_if_present(agent).await?;
                    return Err(error);
                }
            },
            error = &mut renew => {
                self.runtime.discard_if_present(agent).await?;
                return Err(error);
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
        placement: &PlacementRecord,
        cause: &'static str,
    ) -> Result<()> {
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
        self.store.release(placement).await?;
        self.previous.lock().await.remove(&agent);
        self.computer_sampled.lock().await.remove(&agent);
        tracing::info!(%agent, epoch = placement.epoch, reason = cause, booted = hosted, "placement released");
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
        let result = tokio::select! {
            result = crate::tools::execute(
                &self.store,
                self.runtime.clone(),
                self.node,
                placement,
                call.job,
                turn,
                needs_sample,
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

    async fn renew(
        &self,
        mut placement: PlacementRecord,
        release: &watch::Sender<bool>,
    ) -> anyhow::Error {
        loop {
            let remaining: Duration = placement
                .expires_at
                .duration_since(jiff::Timestamp::now())
                .try_into()
                .unwrap_or(Duration::ZERO);
            // Reserve time to stop local processes even when the store hangs or
            // a renewal acknowledgement arrives near the old lease deadline.
            let budget = remaining.saturating_sub(remaining / 10);
            let renewal = async {
                tokio::time::sleep(self.lease.min(remaining) / 3).await;
                let current = self
                    .store
                    .get_by_agent(placement.agent_id)
                    .await?
                    .context("placement missing during renewal")?;
                // A worker or a changed grant can leave more time than the
                // node requests. Preserve it and the store's strict increase.
                let expiry = jiff::Timestamp::now()
                    .checked_add(self.lease)?
                    .max(current.expires_at.checked_add(Duration::from_millis(1))?);
                // Keep the original epoch token even if the read saw a takeover.
                let renewed = self.store.renew(&placement, expiry).await?;
                let requested = self.store.placement_release_requested(&renewed).await?;
                Ok::<_, anyhow::Error>((renewed, requested))
            };
            match tokio::time::timeout(budget, renewal).await {
                Ok(Ok((current, requested))) => {
                    placement = current;
                    if requested {
                        release.send_replace(true);
                    }
                }
                Ok(Err(error)) => {
                    return anyhow::anyhow!("placement lease renewal failed: {error}");
                }
                Err(_) => return anyhow::anyhow!("placement lease expired during renewal"),
            }
        }
    }

    /// Heartbeat observations expire independently of placement authority.
    pub(crate) async fn report_status(&self, lifetime: Duration) -> Result<()> {
        let observed_at = jiff::Timestamp::now();
        let expires_at = observed_at.checked_add(lifetime)?;
        let observations: Vec<_> = self
            .entries
            .lock()
            .await
            .values()
            .filter(|entry| !entry.task.is_finished())
            .filter_map(|entry| {
                let activity = entry.activity.lock().expect("activity lock poisoned");
                activity
                    .placement
                    .as_ref()
                    .map(|placement| AgentCallStatus {
                        agent_id: placement.agent_id,
                        node_id: self.node,
                        epoch: placement.epoch,
                        holder_session_id: activity.holder,
                        queued_calls: activity.queued,
                        observed_at,
                        expires_at,
                    })
            })
            .collect();
        for status in observations {
            match self.store.put_agent_call_status(&status).await {
                Ok(())
                | Err(
                    swarmy_store::StoreError::Fence(swarmy_store::FenceError::PlacementMismatch)
                    | swarmy_store::StoreError::Domain(swarmy_store::DomainError::ComputerDeleted),
                ) => {}
                Err(error) => return Err(error.into()),
            }
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
    }
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]

    use super::Hosting;
    use std::{collections::BTreeMap, path::Path, sync::Arc, time::Duration};
    use swarmy_core::{
        AgentId, NodeCapacity, NodeId, NodeRecord, NodeRole, PlacementChangeReason, PlacementRecord,
    };
    use swarmy_store::Store;
    use tokio::sync::{Mutex, watch};

    /// A node with one sandbox slot, a claimed placement for a new agent on it,
    /// and a hosting whose runtime has no sandboxes at all.
    async fn placed(store: &Store, root: &Path) -> (Arc<Hosting>, AgentId, PlacementRecord) {
        let node_id = NodeId::from_ulid(ulid::Ulid::generate());
        store
            .put_node(&NodeRecord {
                node_id,
                roles: vec![NodeRole::Sandbox],
                capacity: NodeCapacity {
                    cpu_millis: 1000,
                    memory_bytes: 4 * 1024 * 1024 * 1024,
                    disk_bytes: 1024,
                    sandboxes: 1,
                },
                last_heartbeat: jiff::Timestamp::now(),
                cached_images: vec![],
                gpu: None,
            })
            .await
            .unwrap();
        let agent = AgentId::from_ulid(ulid::Ulid::generate());
        let placement = store
            .place(
                agent,
                node_id,
                jiff::Timestamp::now()
                    .checked_add(Duration::from_secs(60))
                    .unwrap(),
            )
            .await
            .unwrap();
        store.claim_placement(&placement).await.unwrap();
        let runtime = swarmy_sandbox::RuncRuntime::open(
            root.join("runtime"),
            swarmy_volume::server::ServerConfig {
                directory: root.join("volumes"),
                node: node_id,
                store: store.clone(),
                objects: Arc::new(object_store::memory::InMemory::new()),
            },
        )
        .await
        .unwrap();
        let hosting = Arc::new(Hosting {
            store: store.clone(),
            runtime: Arc::new(runtime),
            node: node_id,
            lease: Duration::from_secs(60),
            idle: Duration::from_secs(60),
            entries: Mutex::new(BTreeMap::new()),
            previous: Mutex::new(BTreeMap::from([(agent, placement.epoch)])),
            computer_sampled: Mutex::new(BTreeMap::new()),
            shutdown: watch::channel(false).0,
        });
        (hosting, agent, placement)
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
        let (hosting, agent, placement) = placed(&store, root.path()).await;
        hosting
            .stop_and_release(agent, &placement, "eviction")
            .await
            .unwrap();
        assert!(
            store.get_by_agent(agent).await.unwrap().is_none(),
            "cleanup must release a placement whose sandbox never booted"
        );
        assert!(!hosting.previous.lock().await.contains_key(&agent));
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
        let (hosting, agent, placement) = placed(&store, root.path()).await;
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
            .stop_and_release(agent, &placement, "eviction")
            .await
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<swarmy_store::StoreError>(),
            Some(swarmy_store::StoreError::Fence(
                swarmy_store::FenceError::PlacementMismatch
            ))
        ));
        assert_eq!(store.get_by_agent(agent).await.unwrap(), Some(replacement));
        assert_eq!(
            hosting.previous.lock().await.get(&agent),
            Some(&placement.epoch)
        );
        guard.cleanup().await;
    }
}
