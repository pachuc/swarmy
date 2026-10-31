use swarmy_core::{Lease, LeaseOwnerId, NodeId, PlacedToolClaim, PlacementRecord};
use swarmy_node_protocol::NodeRefusal;
/// A fencing token echoed by the node.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Echoed<'a> {
    Placement(&'a PlacementRecord),
    Claim(&'a PlacedToolClaim),
    WriterLease(&'a Lease),
}
/// Check that an echoed grant belongs to the authenticated caller before using the store.
/// # Errors
/// A foreign grant is `NotYours`.
pub(crate) fn authorize_token(node: NodeId, token: Echoed<'_>) -> Result<(), NodeRefusal> {
    let owned = match token {
        Echoed::Placement(p) => p.node_id == node,
        Echoed::Claim(c) => c.placement.node_id == node,
        Echoed::WriterLease(l) => l.owner == LeaseOwnerId::from(node),
    };
    if owned {
        Ok(())
    } else {
        Err(NodeRefusal::NotYours)
    }
}
