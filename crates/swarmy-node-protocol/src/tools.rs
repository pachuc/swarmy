use serde::{Deserialize, Serialize};
/// Resolve a job's agent before selecting its hosting actor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRouteRequest {
    pub job: swarmy_core::ToolJob,
}
/// Agent for the job's immutable session.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRouteResponse {
    pub server_now: jiff::Timestamp,
    pub agent: swarmy_core::AgentId,
}

/// Claim a dispatched tool call.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolClaimRequest {
    pub job: swarmy_core::ToolJob,
    pub placement: swarmy_core::PlacementRecord,
    /// A fresh id per call; a retry sends the same one.
    pub owner: swarmy_core::LeaseOwnerId,
}
/// The stored claim, including its server-clock expiry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolClaimResponse {
    pub server_now: jiff::Timestamp,
    pub claim: swarmy_core::PlacedToolClaim,
    pub turn: Option<swarmy_core::MessageId>,
}
/// Complete one claimed tool call.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolCompleteRequest {
    pub claim: swarmy_core::PlacedToolClaim,
    pub result: swarmy_core::ToolResult,
    pub tool: swarmy_core::ToolMetric,
}
/// Whether this call should stop its background process.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCompleteResponse {
    pub server_now: jiff::Timestamp,
    pub interrupt_requested: bool,
}
