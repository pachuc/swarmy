//! Selection policy is separate from the store's transactional capacity admission.
use std::{collections::HashMap, time::Duration};
use tokio::sync::Mutex;

use anyhow::{Result, bail};
use jiff::Timestamp;
use swarmy_core::{AgentId, NodeRecord, NodeRole, PlacementRecord, VolumeId};
use swarmy_store::{MAX_SCAN_LIMIT, Store, StoreError};

/// A cached route is a hint; dispatch and execution still check the stored epoch.
#[derive(Default)]
pub struct Cache(Mutex<HashMap<AgentId, PlacementRecord>>);

impl Cache {
    /// Resolve against an explicit clock. Tests advance the shared store
    /// clock past lease expiry instead of sleeping out the real lease; the
    /// worker passes its own clock at every call site.
    pub async fn resolve_at(
        &self,
        store: &Store,
        agent: AgentId,
        lease: Duration,
        now: Timestamp,
    ) -> Result<PlacementRecord> {
        {
            let entries = self.0.lock().await;
            if let Some(placement) = entries.get(&agent)
                && placement.expires_at > now
            {
                return Ok(placement.clone());
            }
        }
        let placement = resolve_at(store, agent, lease, now).await?;
        let mut entries = self.0.lock().await;
        // Bound memory even when many short-lived agents pass through a worker.
        if entries.len() >= 4096 {
            entries.retain(|_, entry| entry.expires_at > now);
            if entries.len() >= 4096 {
                entries.clear();
            }
        }
        entries.insert(agent, placement.clone());
        Ok(placement)
    }

    pub async fn invalidate(&self, agent: AgentId) {
        self.0.lock().await.remove(&agent);
    }
}

/// Resolve against an explicit clock; see [`Cache::resolve_at`] for why the
/// clock is a parameter instead of a wall-clock read.
pub async fn resolve_at(
    store: &Store,
    agent: AgentId,
    lease: Duration,
    now: Timestamp,
) -> Result<PlacementRecord> {
    // The last rejection explains a placement that never succeeds.
    let mut rejection = None;
    // Contention can change the winner while capacity is being reserved. Re-read
    // instead of treating another session's successful placement as an error.
    for _ in 0..8 {
        let old = store.get_by_agent(agent).await?;
        if let Some(current) = &old
            && current.expires_at > now
        {
            return Ok(current.clone());
        }
        // The disk writer can outlive the placement. Granting a new epoch before
        // it expires makes the next call fail while booting its replacement.
        if let Some(volume) = store
            .get_volume(VolumeId::from_ulid(agent.as_ulid()))
            .await?
            && volume
                .writer_lease
                .is_some_and(|writer| writer.expires_at > now)
        {
            bail!("waiting for the previous computer's volume writer lease to expire");
        }
        let mut nodes = Vec::new();
        let mut cursor = None;
        let since = now.checked_sub(Duration::from_secs(30))?;
        loop {
            let (page, next) = store.scan_live_nodes(cursor, since, MAX_SCAN_LIMIT).await?;
            nodes.extend(page);
            if next.is_none() {
                break;
            }
            cursor = next;
        }
        let scratch_node = store.scratch(agent).await?.map(|record| record.node_id);
        order_candidates(&mut nodes, scratch_node, old.as_ref());
        for node in nodes {
            let expiry = now.checked_add(lease)?;
            let result = if let Some(old) = &old {
                store.take_over(old, node.node_id, expiry).await
            } else {
                store.place(agent, node.node_id, expiry).await
            };
            match result {
                Ok(placement) => return Ok(placement),
                Err(
                    error @ StoreError::Domain(
                        swarmy_store::DomainError::NodeAtCapacity { .. }
                        | swarmy_store::DomainError::NodeMissing,
                    ),
                ) => {
                    rejection = Some(error.to_string());
                }
                Err(
                    error @ (StoreError::Domain(swarmy_store::DomainError::PlacementExists)
                    | StoreError::Fence(swarmy_store::FenceError::PlacementMismatch)),
                ) => {
                    rejection = Some(error.to_string());
                    break;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
    match rejection {
        Some(rejection) => {
            bail!("no live sandbox node has available computer capacity: {rejection}")
        }
        None => bail!("no live sandbox node has available computer capacity"),
    }
}

fn order_candidates(
    nodes: &mut Vec<NodeRecord>,
    scratch_node: Option<swarmy_core::NodeId>,
    old: Option<&PlacementRecord>,
) {
    nodes.retain(|node| node.roles.contains(&NodeRole::Sandbox) && node.capacity.sandboxes > 0);
    // Capacity admission remains transactional; affinity is only an ordering hint.
    nodes.sort_by_key(|node| {
        (
            scratch_node != Some(node.node_id),
            old.is_some_and(|old| old.node_id == node.node_id),
            node.node_id,
        )
    });
}
