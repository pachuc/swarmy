use crate::{AgentId, ImageRecord};
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
}
