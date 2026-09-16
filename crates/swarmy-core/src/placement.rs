use crate::{AgentId, NodeId};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlacementChangeReason {
    Initial,
    Failure,
    Eviction,
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
