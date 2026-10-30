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

/// The NVIDIA cards a node offers to GPU sandboxes, as one homogeneous set.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuCards {
    pub model: String,
    pub memory_mib: u64,
    pub count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRecord {
    pub node_id: NodeId,
    pub roles: Vec<NodeRole>,
    pub capacity: NodeCapacity,
    pub last_heartbeat: jiff::Timestamp,
    pub cached_images: Vec<ManifestId>,
    /// Cards advertised for GPU sandboxes; `None` when the node has none.
    pub gpu: Option<GpuCards>,
}

impl NodeRecord {
    /// Cards this node can give GPU sandboxes; zero without cards.
    #[must_use]
    pub fn gpu_count(&self) -> u32 {
        self.gpu.as_ref().map_or(0, |gpu| gpu.count)
    }
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use crate::{decode, encode};

    #[test]
    fn node_record_has_fixed_bytes() {
        // Node records are stored with postcard; pin the bytes so a field change is deliberate.
        let record = NodeRecord {
            node_id: NodeId::from_ulid(ulid::Ulid::from(0_u128)),
            roles: vec![NodeRole::Sandbox],
            capacity: NodeCapacity {
                cpu_millis: 1000,
                memory_bytes: 4096,
                disk_bytes: 8192,
                sandboxes: 2,
            },
            last_heartbeat: jiff::Timestamp::UNIX_EPOCH,
            cached_images: vec![],
            gpu: Some(GpuCards {
                model: "NVIDIA L4".into(),
                memory_mib: 23034,
                count: 1,
            }),
        };
        let bytes = encode(&record).unwrap();
        assert_eq!(
            bytes,
            [
                1, 26, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
                48, 48, 48, 48, 48, 48, 48, 1, 0, 232, 7, 128, 32, 128, 64, 2, 20, 49, 57, 55, 48,
                45, 48, 49, 45, 48, 49, 84, 48, 48, 58, 48, 48, 58, 48, 48, 90, 0, 1, 9, 78, 86,
                73, 68, 73, 65, 32, 76, 52, 250, 179, 1, 1
            ]
        );
        assert_eq!(decode::<NodeRecord>(&bytes).unwrap(), record);
    }
}
