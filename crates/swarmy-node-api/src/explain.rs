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
