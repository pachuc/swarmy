use serde::{Deserialize, Serialize};

/// Read-only memory request addressed to the current placement holder.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemoryRequest {
    pub agent_id: crate::AgentId,
    pub epoch: u64,
    pub directory: String,
    pub max_bytes: usize,
}
