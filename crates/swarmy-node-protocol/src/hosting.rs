use crate::{MetricBatch, WriterToken};
use serde::{Deserialize, Serialize};
/// Open a hosting placement and its volume.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostingOpenRequest {
    pub agent: swarmy_core::AgentId,
    pub job: swarmy_core::ToolJob,
    pub previous_epoch: Option<u64>,
}
/// Resources required to boot a sandbox.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostingOpenResponse {
    pub server_now: jiff::Timestamp,
    pub placement: swarmy_core::PlacementRecord,
    pub volume: swarmy_core::VolumeId,
    pub writer: swarmy_core::Lease,
    pub head: swarmy_core::ManifestId,
    pub header: swarmy_core::ManifestHeader,
    pub spec: swarmy_core::SandboxSpec,
    pub boot_set: Option<String>,
}
/// Announce a booted sandbox and send its boot samples.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HostingReadyRequest {
    pub placement: swarmy_core::PlacementRecord,
    pub address: std::net::Ipv4Addr,
    pub metrics: Vec<MetricBatch>,
}
/// Release the writer before the placement.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostingCloseRequest {
    pub placement: swarmy_core::PlacementRecord,
    pub writer: Option<WriterToken>,
}
