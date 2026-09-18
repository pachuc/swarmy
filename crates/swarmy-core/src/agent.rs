use crate::{AgentId, ImageRecord};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

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
}
