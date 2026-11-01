//! Store-backed sandbox lookups for runtimes on the primary's process tree.
use std::sync::Arc;
use swarmy_core::{AgentId, LeaseOwnerId, NodeId, VolumeId};
use swarmy_volume::{MetadataError, WriterGrant};

use crate::Store;

#[derive(Clone)]
pub struct StoreSandboxMetadata {
    store: Store,
    node: NodeId,
}

impl StoreSandboxMetadata {
    /// Lookups for sandboxes on `node`, matching the runtime's server config.
    #[must_use]
    pub fn new(store: Store, node: NodeId) -> Arc<Self> {
        Arc::new(Self { store, node })
    }
}

#[async_trait::async_trait]
impl swarmy_sandbox::SandboxMetadata for StoreSandboxMetadata {
    async fn open_writer(&self, volume: VolumeId) -> Result<WriterGrant, MetadataError> {
        Ok(self
            .store
            .open_volume_writer(volume, LeaseOwnerId::from_ulid(self.node.as_ulid()))
            .await?)
    }
    async fn github_token(&self, agent: AgentId) -> Result<Option<String>, MetadataError> {
        Ok(self.store.agent_github_token(agent).await?)
    }
}
