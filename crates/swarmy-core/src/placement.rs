use crate::{AgentId, NodeId};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlacementChangeReason {
    Initial,
    Failure,
    Eviction,
    /// The previous placement expired before a node claimed it for hosting.
    Unstarted,
}

/// Authority to host an agent's computer. Renewals preserve the epoch and its last-change metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementRecord {
    pub agent_id: AgentId,
    pub node_id: NodeId,
    pub epoch: u64,
    pub expires_at: Timestamp,
    pub last_change_reason: PlacementChangeReason,
    pub last_changed_at: Timestamp,
}

/// A node's recent observation of calls sharing one computer, not execution authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCallStatus {
    pub agent_id: AgentId,
    pub node_id: NodeId,
    pub epoch: u64,
    pub holder_session_id: Option<crate::SessionId>,
    /// Calls waiting for the holder, including callers waiting for channel capacity.
    pub queued_calls: u64,
    pub observed_at: Timestamp,
    pub expires_at: Timestamp,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{decode, encode};

    #[test]
    fn placement_record_has_fixed_bytes() {
        // Placement records are stored with postcard. A silent encoding change
        // would invalidate every stored placement, so pin the bytes.
        let record = PlacementRecord {
            agent_id: AgentId::from_ulid(ulid::Ulid::from(0_u128)),
            node_id: NodeId::from_ulid(ulid::Ulid::from(1_u128)),
            epoch: 1,
            expires_at: Timestamp::UNIX_EPOCH,
            last_change_reason: PlacementChangeReason::Initial,
            last_changed_at: Timestamp::UNIX_EPOCH,
        };
        let bytes = encode(&record).unwrap();
        assert_eq!(
            bytes,
            [
                1, 26, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
                48, 48, 48, 48, 48, 48, 48, 26, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
                48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 1, 20, 49, 57, 55, 48, 45, 48,
                49, 45, 48, 49, 84, 48, 48, 58, 48, 48, 58, 48, 48, 90, 0, 20, 49, 57, 55, 48, 45,
                48, 49, 45, 48, 49, 84, 48, 48, 58, 48, 48, 58, 48, 48, 90
            ]
        );
        assert_eq!(decode::<PlacementRecord>(&bytes).unwrap(), record);
    }

    #[test]
    fn agent_call_status_has_fixed_bytes() {
        // Call routing observations share the same postcard store. Pin them so
        // an encoding change cannot silently orphan in-flight routing state.
        let status = AgentCallStatus {
            agent_id: AgentId::from_ulid(ulid::Ulid::from(0_u128)),
            node_id: NodeId::from_ulid(ulid::Ulid::from(1_u128)),
            epoch: 2,
            holder_session_id: None,
            queued_calls: 3,
            observed_at: Timestamp::UNIX_EPOCH,
            expires_at: Timestamp::UNIX_EPOCH,
        };
        let bytes = encode(&status).unwrap();
        assert_eq!(
            bytes,
            [
                1, 26, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
                48, 48, 48, 48, 48, 48, 48, 26, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
                48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 2, 0, 3, 20, 49, 57, 55, 48,
                45, 48, 49, 45, 48, 49, 84, 48, 48, 58, 48, 48, 58, 48, 48, 90, 20, 49, 57, 55, 48,
                45, 48, 49, 45, 48, 49, 84, 48, 48, 58, 48, 48, 58, 48, 48, 90
            ]
        );
        assert_eq!(decode::<AgentCallStatus>(&bytes).unwrap(), status);
    }
}
