//! Durable sandbox lookups without a direct store dependency.
use swarmy_core::AgentId;
use swarmy_volume::MetadataError;

#[async_trait::async_trait]
pub trait SandboxMetadata: Send + Sync {
    /// The agent's GitHub token, if set.
    /// # Errors
    /// Returns metadata failures.
    async fn github_token(&self, agent: AgentId) -> Result<Option<String>, MetadataError>;
}
