use crate::{AgentId, ImageRecord, ReasoningEffort, SessionId};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

/// Named identity and immutable image selection for its computer.
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
}

/// Optional inference overrides. Omitted fields inherit the stack defaults on create
/// and retain their current values on update.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AgentSettings {
    pub system_prompt: Option<String>,
    pub model: Option<String>,
    pub reasoning_effort: Option<ReasoningEffort>,
}
