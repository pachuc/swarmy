//! Volume metadata served by the primary node API.
use std::num::NonZeroUsize;
use swarmy_core::{ContentHash, FenceKind, Lease, ManifestHeader, ManifestId, VolumeId};
use swarmy_node_protocol::{
    ClientError, DEFAULT_DEADLINE, NodeClient, NodeRefusal, VolumePublishRequest, WriterToken,
};
use swarmy_store::Store;
use swarmy_volume::{MetadataError, VolumeMetadata};

pub(crate) struct NodeVolumeMetadata {
    client: NodeClient,
    store: Store,
}
impl NodeVolumeMetadata {
    pub(crate) fn new(client: NodeClient, store: Store) -> Self {
        Self { client, store }
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
