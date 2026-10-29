use crate::{
    AgentId, GpuRequirement, ImageRecord, ReasoningEffort, SandboxRequirements, SessionId,
};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

/// Why an agent asked for its current GPU requirement with `request_gpu`.
/// The mode is `requirements.gpu`; this records only the cause.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuRequest {
    pub reason: String,
    pub requested_at: Timestamp,
    pub session_id: SessionId,
}

/// Public identity and immutable image selection for its computer.
/// The private `github_token` field lives in a `FoundationDB` side row and is
/// accessed separately so serialization and diagnostics cannot disclose it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRecord {
    pub agent_id: AgentId,
    pub name: String,
    pub image: ImageRecord,
    pub description: String,
    pub created_at: Timestamp,
    /// Current continuous conversation; side sessions never change this pointer.
    #[serde(default)]
    pub main_session: Option<SessionId>,
    #[serde(default)]
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub reasoning_effort: Option<ReasoningEffort>,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub requirements: SandboxRequirements,
    /// Failover chain for this agent's turns. `None` inherits the swarm default.
    #[serde(default)]
    pub route: Option<String>,
    /// Whether the agent may change its own GPU requirement with request_gpu and release_gpu.
    #[serde(default)]
    pub allow_gpu_request: bool,
    /// Set by request_gpu; cleared by release_gpu and by an operator GPU change.
    #[serde(default)]
    pub gpu_request: Option<GpuRequest>,
}

/// Optional inference overrides. Omitted fields inherit the stack defaults on create
/// and retain their current values on update.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AgentSettings {
    pub system_prompt: Option<String>,
    pub model: Option<String>,
    pub reasoning_effort: Option<ReasoningEffort>,
    pub provider: Option<String>,
    pub memory_mib: Option<u64>,
    pub gpu: Option<GpuRequirement>,
    pub route: Option<String>,
    pub allow_gpu_request: Option<bool>,
}

impl AgentRecord {
    #[must_use]
    pub fn inference(&self) -> crate::InferenceSelection {
        crate::InferenceSelection {
            provider: self.provider.clone(),
            model: self.model.clone(),
            effort: self.reasoning_effort,
        }
    }
}

impl AgentSettings {
    /// Apply only supplied settings, after clearing explicitly reset fields.
    pub fn apply_to(&self, agent: &mut AgentRecord, resets: &[crate::InferenceField]) {
        for field in resets {
            match field {
                crate::InferenceField::Provider => agent.provider = None,
                crate::InferenceField::Model => agent.model = None,
                crate::InferenceField::Effort => agent.reasoning_effort = None,
                crate::InferenceField::Route => agent.route = None,
            }
        }
        if let Some(provider) = &self.provider {
            agent.provider = Some(provider.clone());
        }
        if let Some(route) = &self.route {
            agent.route = Some(route.clone());
        }
        if let Some(prompt) = &self.system_prompt {
            agent.system_prompt = Some(prompt.clone());
        }
        if let Some(model) = &self.model {
            agent.model = Some(model.clone());
        }
        if let Some(memory) = self.memory_mib {
            agent.requirements.memory_mib = memory;
        }
        if let Some(gpu) = self.gpu {
            if gpu != agent.requirements.gpu {
                agent.gpu_request = None;
            }
            agent.requirements.gpu = gpu;
        }
        if let Some(allow) = self.allow_gpu_request {
            agent.allow_gpu_request = allow;
        }
        if let Some(effort) = self.reasoning_effort {
            agent.reasoning_effort = Some(effort);
        }
    }
}
