use crate::{NodeAdvert, NodeRefusal};
use serde::{Deserialize, Serialize};
use swarmy_core::{
    AgentId, ComputerMetric, Lease, MessageId, PlacedToolClaim, PlacementRecord, SessionId,
    ToolMetric, VolumeId,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SyncRequest {
    /// Heartbeat for the authenticated node.
    pub node: NodeAdvert,
    pub placements: Vec<HostedPlacement>,
    pub claims: Vec<PlacedToolClaim>,
    pub writers: Vec<WriterToken>,
    pub scratch: Vec<ScratchEntry>,
    /// Scratch directories removed since the previous sync.
    pub scratch_evicted: Vec<AgentId>,
    pub metrics: Vec<MetricBatch>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostedPlacement {
    pub placement: PlacementRecord,
    pub holder_session_id: Option<SessionId>,
    pub queued_calls: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterToken {
    pub volume: VolumeId,
    pub lease: Lease,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScratchEntry {
    pub agent: AgentId,
    pub bytes: u64,
    pub hosted: bool,
    pub idle: bool,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricBatch {
    pub session: SessionId,
    pub turn: MessageId,
    pub patches: Vec<NodeMetric>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeMetric {
    Tool(ToolMetric),
    Computer(ComputerMetric),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Renewal<T> {
    Renewed(T),
    Lost(NodeRefusal),
    Pending(String),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementRenewal {
    pub placement: PlacementRecord,
    pub release_requested: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScratchVerdict {
    Keep,
    Evict(EvictReason),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvictReason {
    Deleted,
    Moved,
    Idle,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncResponse {
    pub server_now: jiff::Timestamp,
    pub heartbeat_recorded: bool,
    pub placements: Vec<Renewal<PlacementRenewal>>,
    pub claims: Vec<Renewal<jiff::Timestamp>>,
    pub writers: Vec<Renewal<Lease>>,
    pub scratch: Vec<ScratchVerdict>,
}
#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use swarmy_core::{
        BashArguments, GpuRequirement, LeaseOwnerId, NodeCapacity, NodeId, NodeRole,
        PlacementChangeReason, RequestId, SandboxArguments, ToolCallId, ToolJob,
    };
    use ulid::Ulid;
    #[test]
    fn sync_round_trip() {
        let id = || Ulid::generate();
        let node = NodeId::from_ulid(id());
        let agent = AgentId::from_ulid(id());
        let session = SessionId::from_ulid(id());
        let now = jiff::Timestamp::now();
        let placement = PlacementRecord {
            agent_id: agent,
            node_id: node,
            epoch: 1,
            expires_at: now,
            last_change_reason: PlacementChangeReason::Initial,
            last_changed_at: now,
            gpu: GpuRequirement::default(),
        };
        let lease = Lease {
            owner: LeaseOwnerId::from(node),
            seq: 1,
            expires_at: now,
        };
        let claim = PlacedToolClaim {
            job: ToolJob {
                session_id: session,
                step: 1,
                request_id: RequestId::for_step(session, 1),
                call_id: ToolCallId("c".into()),
                arguments: SandboxArguments::Bash(BashArguments {
                    command: "true".into(),
                    timeout_ms: 1000,
                    yield_seconds: 1,
                    output_budget_bytes: 1024,
                }),
            },
            owner: LeaseOwnerId::from(node),
            placement: placement.clone(),
            expires_at: now,
        };
        let request = SyncRequest {
            node: NodeAdvert {
                roles: vec![NodeRole::Sandbox],
                capacity: NodeCapacity {
                    cpu_millis: 1000,
                    memory_bytes: 1000,
                    disk_bytes: 1000,
                    sandboxes: 1,
                },
                gpu: None,
            },
            placements: vec![HostedPlacement {
                placement: placement.clone(),
                holder_session_id: Some(session),
                queued_calls: 2,
            }],
            claims: vec![claim],
            writers: vec![WriterToken {
                volume: VolumeId::from_ulid(id()),
                lease: lease.clone(),
            }],
            scratch: vec![ScratchEntry {
                agent,
                bytes: 5,
                hosted: true,
                idle: false,
            }],
            scratch_evicted: vec![agent],
            metrics: vec![MetricBatch {
                session,
                turn: MessageId::from_ulid(id()),
                patches: vec![
                    NodeMetric::Tool(ToolMetric::default()),
                    NodeMetric::Computer(ComputerMetric::default()),
                ],
            }],
        };
        let encoded = serde_json::to_vec(&request).unwrap();
        assert_eq!(
            serde_json::from_slice::<SyncRequest>(&encoded).unwrap(),
            request
        );
        let response = SyncResponse {
            server_now: now,
            heartbeat_recorded: true,
            placements: vec![
                Renewal::Renewed(PlacementRenewal {
                    placement,
                    release_requested: false,
                }),
                Renewal::Lost(NodeRefusal::NotYours),
                Renewal::Pending("later".into()),
            ],
            claims: vec![Renewal::Renewed(now)],
            writers: vec![Renewal::Renewed(lease)],
            scratch: vec![ScratchVerdict::Evict(EvictReason::Idle)],
        };
        let encoded = serde_json::to_vec(&response).unwrap();
        assert_eq!(
            serde_json::from_slice::<SyncResponse>(&encoded).unwrap(),
            response
        );
    }
}
