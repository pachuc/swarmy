use serde::{Deserialize, Serialize};
use std::time::Duration;
/// Advertised resources; identity comes only from the bearer token.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeAdvert {
    pub roles: Vec<swarmy_core::NodeRole>,
    pub capacity: swarmy_core::NodeCapacity,
    pub gpu: Option<swarmy_core::GpuCards>,
}
/// Registration payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterRequest {
    pub node: NodeAdvert,
}
/// Lease policy selected by the server.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseLengths {
    pub placement: Duration,
    pub tool_claim: Duration,
    pub writer: Duration,
    pub sync_interval: Duration,
}
/// Registration result including expired placements from before restart.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterResponse {
    pub server_now: jiff::Timestamp,
    pub previous: Vec<swarmy_core::PlacementRecord>,
    pub leases: LeaseLengths,
}
/// Placement and most recent call occupancy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementOccupancy {
    pub placement: swarmy_core::PlacementRecord,
    pub occupied: bool,
}
/// Current placements belonging to the caller.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementsResponse {
    pub server_now: jiff::Timestamp,
    pub placements: Vec<PlacementOccupancy>,
}
/// Acknowledgement carrying the server clock.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ack {
    pub server_now: jiff::Timestamp,
}
