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
