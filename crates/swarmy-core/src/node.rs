use crate::{ManifestId, NodeId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    Sandbox,
    Volume,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCapacity {
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    /// Maximum resident computers. Placement occupancy is maintained by the store.
    pub sandboxes: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRecord {
    pub node_id: NodeId,
    pub roles: Vec<NodeRole>,
    pub capacity: NodeCapacity,
    pub last_heartbeat: jiff::Timestamp,
    pub cached_images: Vec<ManifestId>,
}
