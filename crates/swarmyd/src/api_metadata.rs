//! Volume metadata served by the primary node API.
use crate::heads::Heads;
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use swarmy_core::{
    AgentId, ContentHash, FenceKind, Lease, LeaseOwnerId, ManifestHeader, ManifestId, NodeId,
    VolumeId,
};
use swarmy_node_protocol::{
    ClientError, DEFAULT_DEADLINE, GithubTokenRequest, NodeClient, NodeRefusal,
    VolumePublishRequest, WriterToken,
};
use swarmy_store::Store;
use swarmy_volume::{MetadataError, VolumeMetadata, WriterGrant};

/// Node API credentials and store-backed control-socket writer opening until NA10.
pub(crate) struct NodeSandboxMetadata {
    client: NodeClient,
    store: Store,
    node: NodeId,
}
impl NodeSandboxMetadata {
    pub(crate) fn new(client: NodeClient, store: Store, node: NodeId) -> Arc<Self> {
        Arc::new(Self {
            client,
            store,
            node,
        })
    }
}
#[async_trait::async_trait]
impl swarmy_sandbox::SandboxMetadata for NodeSandboxMetadata {
    async fn open_writer(&self, volume: VolumeId) -> Result<WriterGrant, MetadataError> {
        self.store
            .open_volume_writer(volume, LeaseOwnerId::from(self.node))
            .await
            .map_err(MetadataError::from)
    }
    async fn github_token(&self, agent: AgentId) -> Result<Option<String>, MetadataError> {
        match self
            .client
            .github_token(
                &GithubTokenRequest { agent },
                tokio::time::Instant::now() + Duration::from_secs(10),
            )
            .await
        {
            Ok(answer) => Ok(answer.token),
            Err(ClientError::Refused(NodeRefusal::NotYours)) => Ok(None),
            Err(error) => Err(metadata_error(error)),
        }
    }
}

pub(crate) struct NodeVolumeMetadata {
    client: NodeClient,
    store: Store,
    heads: Heads,
}
impl NodeVolumeMetadata {
    pub(crate) fn new(client: NodeClient, store: Store, heads: Heads) -> Self {
        Self {
            client,
            store,
            heads,
        }
    }
}
#[async_trait::async_trait]
impl VolumeMetadata for NodeVolumeMetadata {
    async fn publish(
        &self,
        volume: VolumeId,
        lease: &Lease,
        previous: ManifestId,
        next: ManifestId,
        header: &ManifestHeader,
        _retention: NonZeroUsize,
    ) -> Result<(), MetadataError> {
        self.client
            .publish_volume(
                &VolumePublishRequest {
                    writer: WriterToken {
                        volume,
                        lease: lease.clone(),
                    },
                    previous,
                    next,
                    header: header.clone(),
                },
                tokio::time::Instant::now() + DEFAULT_DEADLINE,
            )
            .await
            .map_err(metadata_error)?;
        self.heads.set(volume, next);
        Ok(())
    }
    async fn protect_reused_chunks(
        &self,
        hashes: &[ContentHash],
    ) -> Result<Vec<ContentHash>, MetadataError> {
        self.client
            .protect_chunks(hashes, tokio::time::Instant::now() + DEFAULT_DEADLINE)
            .await
            .map_err(metadata_error)
    }
    async fn release_writer_lease(
        &self,
        volume: VolumeId,
        lease: &Lease,
    ) -> Result<(), MetadataError> {
        self.store
            .release_writer_lease(volume, lease)
            .await
            .map_err(MetadataError::from)
    }
}
pub(crate) fn metadata_error(error: ClientError) -> MetadataError {
    match error {
        ClientError::Refused(refusal) => fenced_or_failed(refusal),
        other => MetadataError::Failed(Box::new(other)),
    }
}
pub(crate) fn fenced_or_failed(refusal: NodeRefusal) -> MetadataError {
    use swarmy_node_protocol::FenceKind as ProtocolFence;
    let kind = match &refusal {
        NodeRefusal::Fenced(ProtocolFence::Placement) => Some(FenceKind::Placement),
        NodeRefusal::Fenced(ProtocolFence::PlacedToolClaim) => Some(FenceKind::PlacedToolClaim),
        NodeRefusal::Fenced(ProtocolFence::VolumeLease) => Some(FenceKind::VolumeLease),
        NodeRefusal::Fenced(ProtocolFence::VolumeHead) => Some(FenceKind::VolumeHead),
        _ => None,
    };
    kind.map_or_else(
        || MetadataError::Failed(Box::new(refusal)),
        MetadataError::Fenced,
    )
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use swarmy_node_protocol::FenceKind as ProtocolFence;
    #[test]
    fn refusals_map_to_volume_errors() {
        let cases = [
            (ProtocolFence::Placement, FenceKind::Placement),
            (ProtocolFence::PlacedToolClaim, FenceKind::PlacedToolClaim),
            (ProtocolFence::VolumeLease, FenceKind::VolumeLease),
            (ProtocolFence::VolumeHead, FenceKind::VolumeHead),
        ];
        for (protocol, core) in cases {
            assert!(
                matches!(fenced_or_failed(NodeRefusal::Fenced(protocol)), MetadataError::Fenced(kind) if kind == core)
            );
        }
        assert!(matches!(
            fenced_or_failed(NodeRefusal::NotYours),
            MetadataError::Failed(_)
        ));
        assert!(matches!(
            fenced_or_failed(NodeRefusal::Fenced(ProtocolFence::StaleSequence {
                expected: 1,
                actual: 2
            })),
            MetadataError::Failed(_)
        ));
        assert!(matches!(
            metadata_error(ClientError::Deadline {
                path: "test",
                attempts: 1,
                last: "timeout".into()
            }),
            MetadataError::Failed(_)
        ));
    }
}
