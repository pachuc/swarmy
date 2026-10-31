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
