use crate::{LeaseOwnerId, ManifestId, NodeId, RequestId, SessionId, ToolCallId, VolumeId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BashArguments {
    pub command: String,
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
    #[serde(default = "default_yield")]
    pub yield_seconds: u64,
    #[serde(default = "default_output_budget")]
    pub output_budget_bytes: usize,
}
const fn default_timeout() -> u64 {
    120_000
}
const fn default_yield() -> u64 {
    10
}
const fn default_output_budget() -> usize {
    32 * 1024
}
impl BashArguments {
    #[must_use]
    pub fn valid(&self) -> bool {
        !self.command.is_empty()
            && (1..=3_600_000).contains(&self.timeout_ms)
            && self.yield_seconds <= 3600
            && (1024..=32 * 1024).contains(&self.output_budget_bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessStartArguments {
    pub command: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessArguments {
    pub process_id: crate::ProcessId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteStdinArguments {
    pub process_id: crate::ProcessId,
    pub text: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebFetchArguments {
    pub url: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmptyArguments {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxArguments {
    Bash(BashArguments),
    ProcessStart(ProcessStartArguments),
    ProcessList(EmptyArguments),
    ProcessLog(ProcessArguments),
    ProcessStop(ProcessArguments),
    Checkpoint(EmptyArguments),
    WriteStdin(WriteStdinArguments),
    WebFetch(WebFetchArguments),
}

#[derive(Debug, thiserror::Error)]
pub enum SandboxArgumentError {
    #[error("{0}")]
    Decode(#[from] serde_json::Error),
    #[error(
        "invalid sandbox arguments: command/URL must be nonempty, timeout_ms must be 1..=3600000, yield_seconds 0..=3600, and output_budget_bytes 1024..=32768"
    )]
    Invalid,
}

impl SandboxArguments {
    /// Decode and validate the model's tool call before dispatch.
    /// # Errors
    /// Rejects unknown tools, malformed arguments, and invalid timeouts.
    pub fn parse(name: &str, arguments: serde_json::Value) -> Result<Self, SandboxArgumentError> {
        let value: Self = serde_json::from_value(serde_json::Value::Object(
            serde_json::Map::from_iter([(name.to_owned(), arguments)]),
        ))?;
        if !value.valid() {
            return Err(SandboxArgumentError::Invalid);
        }
        Ok(value)
    }

    #[must_use]
    pub fn valid(&self) -> bool {
        match self {
            Self::Bash(arguments) => arguments.valid(),
            Self::ProcessStart(arguments) => !arguments.command.is_empty(),
            Self::WebFetch(arguments) => !arguments.url.is_empty(),
            _ => true,
        }
    }

    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Bash(_) => "bash",
            Self::ProcessStart(_) => "process_start",
            Self::ProcessList(_) => "process_list",
            Self::ProcessLog(_) => "process_log",
            Self::ProcessStop(_) => "process_stop",
            Self::Checkpoint(_) => "checkpoint",
            Self::WriteStdin(_) => "write_stdin",
            Self::WebFetch(_) => "web_fetch",
        }
    }

    #[must_use]
    pub fn parameters(&self) -> serde_json::Value {
        match self {
            Self::Bash(value) => serde_json::json!(value),
            Self::WriteStdin(value) => serde_json::json!(value),
            Self::WebFetch(value) => serde_json::json!(value),
            Self::ProcessStart(value) => serde_json::json!(value),
            Self::ProcessLog(value) | Self::ProcessStop(value) => serde_json::json!(value),
            Self::ProcessList(value) | Self::Checkpoint(value) => serde_json::json!(value),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolJob {
    pub session_id: SessionId,
    pub request_id: RequestId,
    pub call_id: ToolCallId,
    pub step: u64,
    pub arguments: SandboxArguments,
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
