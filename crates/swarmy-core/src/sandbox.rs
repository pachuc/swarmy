use crate::{AgentId, VolumeId};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GpuRequirement {
    #[default]
    None,
    Shared,
    Dedicated,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxRequirements {
    pub memory_mib: u64,
    pub gpu: GpuRequirement,
}

impl Default for SandboxRequirements {
    fn default() -> Self {
        Self {
            memory_mib: 768,
            gpu: GpuRequirement::None,
        }
    }
}

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SandboxSpec {
    pub agent_id: AgentId,
    #[serde(default)]
    pub scratch: Vec<String>,
    #[serde(default)]
    pub requirements: SandboxRequirements,
}

/// A volume to attach through the node's block device service.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct BlockDevice {
    pub volume_id: VolumeId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Sandbox {
    pub agent_id: AgentId,
}

/// Runc keeps only the disk across a pause.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PauseHandle {
    pub spec: SandboxSpec,
    pub disk: BlockDevice,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCaps {
    pub memory_pause: bool,
    pub kvm: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecRequest {
    pub args: Vec<String>,
    pub timeout_ms: u64,
    #[serde(default)]
    pub stdin: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecOutput {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecResult {
    pub exit_code: i32,
    pub timed_out: bool,
}
