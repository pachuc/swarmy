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
#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use swarmy_core::{AgentId, GpuRequirement, PlacementChangeReason};
    use ulid::Ulid;
    #[test]
    fn checks_placement_claim_and_lease_owners() {
        let node = NodeId::from_ulid(Ulid::generate());
        let other = NodeId::from_ulid(Ulid::generate());
        let placement = PlacementRecord {
            agent_id: AgentId::from_ulid(Ulid::generate()),
            node_id: node,
            epoch: 1,
            expires_at: jiff::Timestamp::now(),
            last_change_reason: PlacementChangeReason::Initial,
            last_changed_at: jiff::Timestamp::now(),
            gpu: GpuRequirement::default(),
        };
        let lease = Lease {
            owner: LeaseOwnerId::from(node),
            seq: 1,
            expires_at: jiff::Timestamp::now(),
        };
        assert!(authorize_token(node, Echoed::Placement(&placement)).is_ok());
        assert!(authorize_token(other, Echoed::Placement(&placement)).is_err());
        assert!(authorize_token(node, Echoed::WriterLease(&lease)).is_ok());
        assert_eq!(
            authorize_token(other, Echoed::WriterLease(&lease)),
            Err(NodeRefusal::NotYours)
        );
        let claim = PlacedToolClaim {
            job: swarmy_core::ToolJob {
                session_id: swarmy_core::SessionId::from_ulid(Ulid::generate()),
                step: 1,
                request_id: swarmy_core::RequestId::for_step(
                    swarmy_core::SessionId::from_ulid(Ulid::generate()),
                    1,
                ),
                call_id: swarmy_core::ToolCallId("c".into()),
                arguments: swarmy_core::SandboxArguments::Bash(swarmy_core::BashArguments {
                    command: "true".into(),
                    timeout_ms: 1000,
                    yield_seconds: 1,
                    output_budget_bytes: 1000,
                }),
            },
            owner: LeaseOwnerId::from(node),
            placement,
            expires_at: jiff::Timestamp::now(),
        };
        assert!(authorize_token(node, Echoed::Claim(&claim)).is_ok());
        assert_eq!(
            authorize_token(other, Echoed::Claim(&claim)),
            Err(NodeRefusal::NotYours)
        );
    }
}
