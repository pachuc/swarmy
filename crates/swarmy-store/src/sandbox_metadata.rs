//! Store-backed sandbox lookups for runtimes on the primary's process tree.
use std::sync::Arc;
use swarmy_core::{AgentId, NodeId};
use swarmy_volume::MetadataError;

use crate::Store;

#[derive(Clone)]
pub struct StoreSandboxMetadata {
    store: Store,
}

impl StoreSandboxMetadata {
    /// Lookups for sandboxes on `node`, matching the runtime's server config.
    #[must_use]
    pub fn new(store: Store, _node: NodeId) -> Arc<Self> {
        Arc::new(Self { store })
    }
}

#[async_trait::async_trait]
impl swarmy_sandbox::SandboxMetadata for StoreSandboxMetadata {
    async fn github_token(&self, agent: AgentId) -> Result<Option<String>, MetadataError> {
        Ok(self.store.agent_github_token(agent).await?)
    }
}
