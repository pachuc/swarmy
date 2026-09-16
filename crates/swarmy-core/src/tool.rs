use crate::{LeaseOwnerId, ManifestId, NodeId, RequestId, SessionId, ToolCallId, VolumeId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BashArguments {
    pub command: String,
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
}
const fn default_timeout() -> u64 {
    120_000
}
impl BashArguments {
    #[must_use]
    pub fn valid(&self) -> bool {
        !self.command.is_empty() && (1..=3_600_000).contains(&self.timeout_ms)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolJob {
    pub session_id: SessionId,
    pub request_id: RequestId,
    pub call_id: ToolCallId,
    pub step: u64,
    pub arguments: BashArguments,
}

/// A fresh owner and private volume per attempt prevent stale disk writes from
/// becoming visible after a tool lease expires.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolClaim {
    pub job: ToolJob,
    pub owner: LeaseOwnerId,
    pub node_id: NodeId,
    pub expires_at: jiff::Timestamp,
    pub attempt_volume: VolumeId,
}

/// A call on an agent's persistent computer, fenced by its placement epoch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacedToolClaim {
    pub job: ToolJob,
    pub owner: LeaseOwnerId,
    pub placement: crate::PlacementRecord,
    pub expires_at: jiff::Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxRecord {
    pub session_id: SessionId,
    pub node_id: NodeId,
    pub volume_id: VolumeId,
    pub manifest_id: ManifestId,
    pub active_call: Option<RequestId>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct PlaceRequest {
    pub session_id: SessionId,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PlaceReply {
    Placed(SandboxRecord),
    Failed(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BashResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub timed_out: bool,
    pub manifest_id: ManifestId,
}
impl BashResult {
    #[must_use]
    pub fn tool_result(&self) -> crate::ToolResult {
        let metadata = std::collections::BTreeMap::from([
            ("stdout".into(), serde_json::json!(self.stdout)),
            ("stderr".into(), serde_json::json!(self.stderr)),
            ("exit_code".into(), serde_json::json!(self.exit_code)),
            ("timed_out".into(), serde_json::json!(self.timed_out)),
            ("manifest_id".into(), serde_json::json!(self.manifest_id)),
        ]);
        crate::ToolResult::Completed {
            title: "bash".into(),
            // Providers send output text to the model, so status and stderr must
            // be included here as well as in the structured event metadata.
            output: serde_json::json!(metadata).to_string(),
            metadata,
        }
    }
}
