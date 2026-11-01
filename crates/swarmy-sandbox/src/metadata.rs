//! Durable sandbox lookups without a direct store dependency.
use swarmy_core::{AgentId, VolumeId};
use swarmy_volume::{MetadataError, WriterGrant};

#[async_trait::async_trait]
pub trait SandboxMetadata: Send + Sync {
    /// Open a writer for the control socket and resume; hosting passes its own grant to `create_with`.
    /// # Errors
    /// Returns a fence or metadata failure.
    async fn open_writer(&self, volume: VolumeId) -> Result<WriterGrant, MetadataError>;
    /// The agent's GitHub token, if set.
    /// # Errors
    /// Returns metadata failures.
    async fn github_token(&self, agent: AgentId) -> Result<Option<String>, MetadataError>;
}
