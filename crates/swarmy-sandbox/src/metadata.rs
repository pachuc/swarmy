//! Durable sandbox lookups without a direct store dependency.
use swarmy_core::{AgentId, NodeId, VolumeId};
use swarmy_volume::{MetadataError, WriterGrant};

#[async_trait::async_trait]
pub trait SandboxMetadata: Send + Sync {
    /// Acquire this node's writer lease and read the volume head.
    /// # Errors
    /// Returns a fence or metadata failure.
    async fn open_writer(&self, volume: VolumeId) -> Result<WriterGrant, MetadataError>;
    /// Whether the agent's computer was deleted.
    /// # Errors
    /// Returns metadata failures.
    async fn computer_deleted(&self, agent: AgentId) -> Result<bool, MetadataError>;
    /// The node holding the agent's placement, live or expired.
    /// # Errors
    /// Returns metadata failures.
    async fn placement_node(&self, agent: AgentId) -> Result<Option<NodeId>, MetadataError>;
    /// Record this node's scratch usage.
    /// # Errors
    /// Returns a placement fence or metadata failure.
    async fn report_scratch(&self, agent: AgentId, bytes: u64) -> Result<(), MetadataError>;
    /// Forget this node's scratch record.
    /// # Errors
    /// Returns metadata failures.
    async fn clear_scratch(&self, agent: AgentId) -> Result<(), MetadataError>;
    /// The agent's GitHub token, if set.
    /// # Errors
    /// Returns metadata failures.
    async fn github_token(&self, agent: AgentId) -> Result<Option<String>, MetadataError>;
}
