use anyhow::{Context, Result, bail};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use swarmy_core::{AgentId, BlockDevice, NodeId, PlacementRecord, SandboxSpec, ToolJob};
use swarmy_sandbox::{RuncRuntime, SandboxRuntime};
use swarmy_store::Store;
use tokio::sync::{Mutex, mpsc, oneshot, watch};

struct Call {
    job: ToolJob,
    reply: oneshot::Sender<Result<()>>,
}
struct Entry {
    calls: mpsc::Sender<Call>,
    task: tokio::task::JoinHandle<()>,
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
        if self.store.tool_completed(job.request_id).await? {
            return Ok(());
        }
        let agent = self
            .store
            .fetch_session(job.session_id)
            .await?
            .context("session missing")?
            .agent_id;
        let (reply, response) = oneshot::channel();
        let calls = {
            let mut entries = self.entries.lock().await;
            if *self.shutdown.borrow() {
                bail!("node is shutting down");
            }
            entries.retain(|_, entry| !entry.task.is_finished());
            entries
                .entry(agent)
                .or_insert_with(|| {
                    let (calls, receive) = mpsc::channel(16);
                    let hosting = self.clone();
                    let task = tokio::spawn(async move {
                        if let Err(error) = hosting.host(agent, receive).await {
                            tracing::warn!(%agent, %error, "agent hosting stopped");
                        }
                    });
                    Entry { calls, task }
                })
                .calls
                .clone()
        };
        calls
            .send(Call { job, reply })
            .await
            .context("agent stopped serving; placement lease lost")?;
        response
            .await
            .context("agent stopped serving; placement lease lost")?
    }

    async fn placement(&self, agent: AgentId) -> Result<PlacementRecord> {
        let expiry = jiff::Timestamp::now().checked_add(self.lease)?;
        let placement = match self.store.get_by_agent(agent).await? {
            None => self.store.place(agent, self.node, expiry).await?,
            Some(old) if old.expires_at <= jiff::Timestamp::now() => {
                self.store.take_over(&old, self.node, expiry).await?
            }
            Some(old) if old.node_id != self.node => bail!(
                "agent is placed on another node {} at epoch {}",
                old.node_id,
                old.epoch
            ),
            Some(old) => {
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
        self.previous.lock().await.insert(agent, placement.epoch);
        Ok(placement)
    }

    async fn host(&self, agent: AgentId, mut calls: mpsc::Receiver<Call>) -> Result<()> {
        let Some(first) = calls.recv().await else {
            return Ok(());
        };
        let placement = match self.placement(agent).await {
            Ok(placement) => placement,
            Err(error) => {
                let _ = first.reply.send(Err(error));
                return Ok(());
            }
        };
        let mut shutdown = self.shutdown.subscribe();
        let serving = async {
            anyhow::ensure!(!*shutdown.borrow(), "node is shutting down");
            let volume = self
                .store
                .agent_volume(first.job.session_id, &placement)
                .await?;
            self.runtime
                .create(
                    SandboxSpec { agent_id: agent },
                    BlockDevice { volume_id: volume },
                )
                .await?;
            self.execute(&placement, first).await?;
            loop {
                if *shutdown.borrow() {
                    break;
                }
                tokio::select! {
                    call = calls.recv() => {
                        let Some(call) = call else { break; };
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
            let budget = remaining.saturating_sub(self.lease / 10);
            let renewal = async {
                tokio::time::sleep(self.lease / 3).await;
                let expiry = jiff::Timestamp::now().checked_add(self.lease)?;
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

    pub async fn shutdown(&self) {
        self.shutdown.send_replace(true);
        let entries = std::mem::take(&mut *self.entries.lock().await);
        for (_, entry) in entries {
            let _ = entry.task.await;
        }
    }
}
