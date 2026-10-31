use crate::failure::Answer;
use swarmy_core::{NodeId, PlacementRecord, ToolJob};
use swarmy_node_protocol::{FenceKind, NodeRefusal};
use swarmy_store::Store;
/// Prefer the current foreign placement, then the dispatch placement.
pub(crate) fn placed_elsewhere(
    current: Option<&PlacementRecord>,
    dispatched: Option<&PlacementRecord>,
    node: NodeId,
) -> Option<NodeRefusal> {
    current
        .into_iter()
        .chain(dispatched)
        .find(|p| p.node_id != node)
        .map(|p| NodeRefusal::PlacedElsewhere {
            node: p.node_id,
            epoch: p.epoch,
        })
}
/// Explain a dispatch fence using the current and dispatched placements.
pub(crate) async fn dispatch_fence(store: &Store, node: NodeId, job: &ToolJob) -> Answer {
    let dispatched = match store.tool_placement(job.request_id).await {
        Ok(value) => value,
        Err(error) => return error.into(),
    };
    let current = if let Some(ref dispatched) = dispatched {
        match store.get_by_agent(dispatched.agent_id).await {
            Ok(value) => value,
            Err(error) => return error.into(),
        }
    } else {
        None
    };
    placed_elsewhere(current.as_ref(), dispatched.as_ref(), node)
        .unwrap_or(NodeRefusal::Fenced(FenceKind::Placement))
        .into()
}
#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use ulid::Ulid;
    #[test]
    fn prefers_current_foreign_placement() {
        let node = NodeId::from_ulid(Ulid::generate());
        let other = NodeId::from_ulid(Ulid::generate());
        let third = NodeId::from_ulid(Ulid::generate());
        let placement = |node_id| PlacementRecord {
            agent_id: swarmy_core::AgentId::from_ulid(Ulid::generate()),
            node_id,
            epoch: 7,
            expires_at: jiff::Timestamp::UNIX_EPOCH,
            last_change_reason: swarmy_core::PlacementChangeReason::Initial,
            last_changed_at: jiff::Timestamp::UNIX_EPOCH,
            gpu: swarmy_core::GpuRequirement::default(),
        };
        let current = placement(other);
        let dispatched = placement(third);
        assert_eq!(
            placed_elsewhere(Some(&current), Some(&dispatched), node),
            Some(NodeRefusal::PlacedElsewhere {
                node: other,
                epoch: 7
            })
        );
        let local = placement(node);
        assert_eq!(
            placed_elsewhere(Some(&local), Some(&dispatched), node),
            Some(NodeRefusal::PlacedElsewhere {
                node: third,
                epoch: 7
            })
        );
        assert_eq!(placed_elsewhere(Some(&local), Some(&local), node), None);
        assert_eq!(placed_elsewhere(None, None, node), None);
    }
}
