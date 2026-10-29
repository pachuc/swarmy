//! Selection policy is separate from the store's transactional capacity admission.
use std::{collections::HashMap, time::Duration};
use tokio::sync::Mutex;

use anyhow::{Result, bail};
use jiff::Timestamp;
use swarmy_core::{
    AgentId, GpuRequirement, NodeId, NodeRecord, NodeRole, PlacementRecord, VolumeId,
};
use swarmy_store::{MAX_SCAN_LIMIT, Store, StoreError};

/// No live node could take the computer. The capacity wait matches on this type
/// to park the session instead of failing the step, so it must never be wrapped in context.
#[derive(Debug)]
pub(crate) struct NoEligibleNode {
    pub(crate) requirement: GpuRequirement,
    /// The last store refusal, when some node was eligible but refused.
    pub(crate) rejection: Option<StoreError>,
}

impl std::fmt::Display for NoEligibleNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.requirement {
            GpuRequirement::None => {
                write!(f, "no live sandbox node has available computer capacity")
            }
            GpuRequirement::Shared => write!(
                f,
                "no live sandbox node with a GPU can host this shared GPU computer"
            ),
            GpuRequirement::Dedicated => write!(
                f,
                "no live sandbox node with a GPU can host this dedicated GPU computer"
            ),
        }
    }
}

impl std::error::Error for NoEligibleNode {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.rejection
            .as_ref()
            .map(|error| error as &(dyn std::error::Error + 'static))
    }
}

/// A cached route is a hint; dispatch and execution still check the stored epoch.
#[derive(Default)]
pub(crate) struct Cache(Mutex<HashMap<AgentId, PlacementRecord>>);

impl Cache {
    /// Resolve against an explicit clock. Tests advance the shared store
    /// clock past lease expiry instead of sleeping out the real lease; the
    /// worker passes its own clock at every call site.
    pub(crate) async fn resolve_at(
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

    pub(crate) async fn invalidate(&self, agent: AgentId) {
        self.0.lock().await.remove(&agent);
    }
}

/// Resolve against an explicit clock; see [`Cache::resolve_at`] for why the
/// clock is a parameter instead of a wall-clock read.
pub(crate) async fn resolve_at(
    store: &Store,
    agent: AgentId,
    lease: Duration,
    now: Timestamp,
) -> Result<PlacementRecord> {
    let requirement = store.computer_requirements(agent).await?.gpu;
    // The last rejection explains a placement that never succeeds.
    let mut rejection = None;
    // Contention can change the winner while capacity is being reserved. Re-read
    // instead of treating another session's successful placement as an error.
    for _ in 0..8 {
        let old = store.get_by_agent(agent).await?;
        if let Some(current) = &old
            && current.expires_at > now
        {
            if requirement.needs_card()
                && !store
                    .get_node(current.node_id)
                    .await?
                    .is_some_and(|node| swarmy_core::node_satisfies(&node, requirement))
            {
                // A placement that raced the requirement change has no release
                // request; ask for one so the node lets it go.
                store.request_placement_release(agent).await?;
                return Err(NoEligibleNode {
                    requirement,
                    rejection: None,
                }
                .into());
            }
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
        order_candidates(&mut nodes, requirement, scratch_node, old.as_ref());
        if nodes.is_empty() {
            break;
        }
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
                        | swarmy_store::DomainError::NodeMissing
                        | swarmy_store::DomainError::GpuUnavailable { .. },
                    ),
                ) => {
                    rejection = Some(error);
                }
                Err(
                    error @ (StoreError::Domain(swarmy_store::DomainError::PlacementExists)
                    | StoreError::Fence(swarmy_store::FenceError::PlacementMismatch)),
                ) => {
                    rejection = Some(error);
                    break;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
    Err(NoEligibleNode {
        requirement,
        rejection,
    }
    .into())
}

fn order_candidates(
    nodes: &mut Vec<NodeRecord>,
    requirement: GpuRequirement,
    scratch_node: Option<NodeId>,
    old: Option<&PlacementRecord>,
) {
    nodes.retain(|node| {
        node.roles.contains(&NodeRole::Sandbox)
            && node.capacity.sandboxes > 0
            && swarmy_core::node_satisfies(node, requirement)
    });
    // Admission remains transactional. A CPU node beats scratch on a card node
    // for CPU work; ineligible nodes are filtered before scratch can win.
    nodes.sort_by_key(|node| {
        (
            old.is_some_and(|old| old.node_id == node.node_id),
            !requirement.needs_card() && node.gpu_count() > 0,
            scratch_node != Some(node.node_id),
            node.node_id,
        )
    });
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use swarmy_core::{GpuCards, NodeCapacity};

    fn node(number: u128, cards: bool) -> NodeRecord {
        NodeRecord {
            node_id: NodeId::from_ulid(ulid::Ulid::from(number)),
            roles: vec![NodeRole::Sandbox],
            capacity: NodeCapacity {
                cpu_millis: 1000,
                memory_bytes: 1 << 30,
                disk_bytes: 1024,
                sandboxes: 2,
            },
            last_heartbeat: Timestamp::UNIX_EPOCH,
            cached_images: vec![],
            gpu: cards.then(|| GpuCards {
                model: "L4".into(),
                memory_mib: 23034,
                count: 1,
            }),
        }
    }
    fn ids(nodes: &[NodeRecord]) -> Vec<NodeId> {
        nodes.iter().map(|node| node.node_id).collect()
    }
    #[test]
    fn cpu_work_tries_card_nodes_last() {
        let mut nodes = vec![node(1, true), node(2, false)];
        order_candidates(&mut nodes, GpuRequirement::None, None, None);
        assert_eq!(
            ids(&nodes),
            vec![node(2, false).node_id, node(1, true).node_id]
        );
    }
    #[test]
    fn cpu_work_prefers_a_cpu_node_over_scratch_on_a_card_node() {
        let mut nodes = vec![node(1, true), node(2, false)];
        order_candidates(
            &mut nodes,
            GpuRequirement::None,
            Some(node(1, true).node_id),
            None,
        );
        assert_eq!(
            ids(&nodes),
            vec![node(2, false).node_id, node(1, true).node_id]
        );
    }
    #[test]
    fn gpu_work_sees_only_card_nodes() {
        let mut nodes = vec![node(1, false), node(2, true)];
        order_candidates(&mut nodes, GpuRequirement::Shared, None, None);
        assert_eq!(ids(&nodes), vec![node(2, true).node_id]);
        let mut nodes = vec![node(1, false)];
        order_candidates(&mut nodes, GpuRequirement::Dedicated, None, None);
        assert!(nodes.is_empty());
    }
    #[test]
    fn gpu_requirement_ignores_scratch_on_an_ineligible_node() {
        let mut nodes = vec![node(1, false), node(2, true), node(3, true)];
        order_candidates(
            &mut nodes,
            GpuRequirement::Dedicated,
            Some(node(1, false).node_id),
            None,
        );
        assert_eq!(
            ids(&nodes),
            vec![node(2, true).node_id, node(3, true).node_id]
        );
        order_candidates(
            &mut nodes,
            GpuRequirement::Dedicated,
            Some(node(3, true).node_id),
            None,
        );
        assert_eq!(
            ids(&nodes),
            vec![node(3, true).node_id, node(2, true).node_id]
        );
    }
    #[test]
    fn expired_old_node_still_goes_last() {
        let old = PlacementRecord {
            agent_id: AgentId::from_ulid(ulid::Ulid::from(4_u128)),
            node_id: node(2, true).node_id,
            epoch: 1,
            expires_at: Timestamp::UNIX_EPOCH,
            last_change_reason: swarmy_core::PlacementChangeReason::Initial,
            last_changed_at: Timestamp::UNIX_EPOCH,
            gpu: GpuRequirement::Shared,
        };
        let mut nodes = vec![node(2, true), node(3, true)];
        order_candidates(
            &mut nodes,
            GpuRequirement::Shared,
            Some(old.node_id),
            Some(&old),
        );
        assert_eq!(
            ids(&nodes),
            vec![node(3, true).node_id, node(2, true).node_id]
        );
    }
}
