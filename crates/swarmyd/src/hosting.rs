use anyhow::{Context, Result, bail};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use swarmy_core::{
    AgentCallStatus, AgentId, BlockDevice, NodeId, PlacementRecord, SandboxSpec, SessionId, ToolJob,
};
use swarmy_sandbox::{RuncRuntime, SandboxRuntime};
use swarmy_store::Store;
use tokio::sync::{Mutex, mpsc, oneshot, watch};

struct Call {
    job: ToolJob,
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
pub struct Hosting {
    store: Store,
    runtime: Arc<RuncRuntime>,
    node: NodeId,
    lease: Duration,
    idle: Duration,
    entries: Mutex<BTreeMap<AgentId, Entry>>,
    previous: Mutex<BTreeMap<AgentId, u64>>,
    shutdown: watch::Sender<bool>,
}

impl Hosting {
    async fn scratch_for_session(&self, session: SessionId) -> Result<Vec<String>> {
        if let Some(image) = self.store.pinned_image(session).await? {
            Ok(self.store.image_scratch(&image).await?)
        } else {
            Ok(Vec::new())
        }
    }
    pub async fn new(
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
            lease: Duration::from_secs(settings.placement_lease_seconds.get()),
            idle: Duration::from_secs(settings.sandbox_idle_seconds.get()),
            entries: Mutex::new(BTreeMap::new()),
            previous: Mutex::new(previous),
            shutdown: watch::channel(false).0,
        }))
    }

    pub async fn call(self: &Arc<Self>, job: ToolJob) -> Result<()> {
        let Some(agent) = self.store.tool_agent(&job, self.node).await? else {
            return Ok(());
        };
        let (reply, response) = oneshot::channel();
        let (calls, activity) = {
            let mut entries = self.entries.lock().await;
            if *self.shutdown.borrow() {
                bail!("node is shutting down");
            }
            entries.retain(|_, entry| !entry.task.is_finished());
            let entry = entries.entry(agent).or_insert_with(|| {
                let (calls, receive) = mpsc::channel(16);
                let hosting = self.clone();
                let task = tokio::spawn(async move {
                    if let Err(error) = hosting.host(agent, receive).await {
                        tracing::warn!(%agent, %error, "agent hosting stopped");
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

    async fn host(&self, agent: AgentId, mut calls: mpsc::Receiver<Call>) -> Result<()> {
        let Some(mut first) = calls.recv().await else {
            return Ok(());
        };
        first.activity.start(first.job.session_id);
        let placement = match self.placement(agent, &first.job).await {
            Ok(placement) => placement,
            Err(error) => {
                let _ = first.reply.send(Err(error));
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
        let serving = async {
            anyhow::ensure!(!*shutdown.borrow(), "node is shutting down");
            let volume = self
                .store
                .agent_volume(first.job.session_id, &placement)
                .await?;
            let scratch = self.scratch_for_session(first.job.session_id).await?;
            let requirements = if let Some(agent_record) = self.store.get_agent(agent).await? {
                agent_record.requirements
            } else if let Some(image) = self.store.pinned_image(first.job.session_id).await? {
                swarmy_core::SandboxRequirements {
                    memory_mib: self.store.image_memory(&image).await?.unwrap_or(768),
                    gpu: Default::default(),
                }
            } else {
                Default::default()
            };
            self.runtime
                .create(
                    SandboxSpec {
                        agent_id: agent,
                        scratch,

                        requirements,
                    },
                    BlockDevice { volume_id: volume },
                )
                .await?;
            self.store
                .set_placement_address(&placement, swarmy_sandbox::RuncRuntime::NETWORK_ADDRESS)
                .await?;
            self.execute(&placement, first).await?;
            loop {
                if *shutdown.borrow() {
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
                }
            }
            Ok::<_, anyhow::Error>(())
        };
        let renew = self.renew(placement.clone());
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
        let cleanup = async {
            if self
                .runtime
                .list()
                .await
                .iter()
                .any(|sandbox| sandbox.agent_id == agent)
            {
                self.runtime
                    .destroy(swarmy_core::Sandbox { agent_id: agent })
                    .await?;
                self.store.release(&placement).await?;
                self.previous.lock().await.remove(&agent);
                tracing::info!(%agent, epoch = placement.epoch, reason = "eviction", "placement released");
            } else {
                self.runtime.discard_if_present(agent).await?;
            }
            Ok::<_, anyhow::Error>(())
        };
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

    async fn execute(&self, placement: &PlacementRecord, call: Call) -> Result<()> {
        let mut shutdown = self.shutdown.subscribe();
        anyhow::ensure!(!*shutdown.borrow(), "node is shutting down");
        let result = tokio::select! {
            result = crate::tools::execute(&self.store, &self.runtime, placement, call.job) => result,
            _ = shutdown.changed() => Err(anyhow::anyhow!("node is shutting down")),
        };
        let failed = result.is_err();
        let _ = call.reply.send(result);
        if failed {
            bail!("tool execution interrupted; stopping agent");
        }
        Ok(())
    }

    async fn renew(&self, mut placement: PlacementRecord) -> anyhow::Error {
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
                Ok::<_, anyhow::Error>(self.store.renew(&placement, expiry).await?)
            };
            match tokio::time::timeout(budget, renewal).await {
                Ok(Ok(current)) => placement = current,
                Ok(Err(error)) => {
                    return anyhow::anyhow!("placement lease renewal failed: {error}");
                }
                Err(_) => return anyhow::anyhow!("placement lease expired during renewal"),
            }
        }
    }

    /// Heartbeat observations expire independently of placement authority.
    pub async fn report_status(&self, lifetime: Duration) -> Result<()> {
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
                    swarmy_store::StoreError::LeaseMismatch
                    | swarmy_store::StoreError::ComputerDeleted,
                ) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    pub async fn shutdown(&self) {
        self.shutdown.send_replace(true);
        let entries = std::mem::take(&mut *self.entries.lock().await);
        for (_, entry) in entries {
            let _ = entry.task.await;
        }
    }
}
