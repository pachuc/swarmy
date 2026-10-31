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
