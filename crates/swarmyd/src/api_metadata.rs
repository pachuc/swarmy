//! Volume metadata served by the primary node API.
use crate::heads::Heads;
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use swarmy_core::{AgentId, ContentHash, FenceKind, Lease, ManifestHeader, ManifestId, VolumeId};
use swarmy_node_protocol::{
    ClientError, DEFAULT_DEADLINE, GithubTokenRequest, NodeClient, NodeRefusal,
    VolumePublishRequest, VolumeReleaseRequest, WriterToken,
};
use swarmy_volume::{MetadataError, VolumeMetadata};

/// Node API credentials for sandbox metadata.
pub(crate) struct NodeSandboxMetadata {
    client: NodeClient,
}
impl NodeSandboxMetadata {
    pub(crate) fn new(client: NodeClient) -> Arc<Self> {
        Arc::new(Self { client })
    }
}
#[async_trait::async_trait]
impl swarmy_sandbox::SandboxMetadata for NodeSandboxMetadata {
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
    heads: Heads,
}
impl NodeVolumeMetadata {
    pub(crate) fn new(client: NodeClient, heads: Heads) -> Self {
        Self { client, heads }
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
        self.client
            .release_volume(
                &VolumeReleaseRequest {
                    writer: WriterToken {
                        volume,
                        lease: lease.clone(),
                    },
                },
                tokio::time::Instant::now() + DEFAULT_DEADLINE,
            )
            .await
            .map_err(metadata_error)?;
        self.heads.forget(volume);
        Ok(())
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
    #[tokio::test]
    async fn publish_records_the_head_only_when_accepted() {
        use axum::{Json, Router, response::IntoResponse, routing::post};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use swarmy_core::{ContentHash, LeaseOwnerId, NodeId, NodeToken};
        use swarmy_node_api::test_support::TestApi;
        use swarmy_node_protocol::{Ack, Failure, VOLUME_PUBLISH_PATH};
        use swarmy_store::blob::MemoryBlobStore;
        let Some(stack) = swarmy_testkit::Stack::load("published-head") else {
            return;
        };
        let (store, mut guard) = stack.open_store(Arc::new(MemoryBlobStore::default())).await;
        let count = Arc::new(AtomicUsize::new(0));
        let counter = count.clone();
        let router = Router::new().route(
            VOLUME_PUBLISH_PATH,
            post(move || {
                let counter = counter.clone();
                async move {
                    if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                        Json(Ack {
                            server_now: jiff::Timestamp::now(),
                        })
                        .into_response()
                    } else {
                        (
                            axum::http::StatusCode::CONFLICT,
                            Json(Failure::Refused {
                                refusal: NodeRefusal::Fenced(ProtocolFence::VolumeLease),
                            }),
                        )
                            .into_response()
                    }
                }
            }),
        );
        let api = TestApi::start(router).await;
        let node = NodeId::from_ulid(ulid::Ulid::generate());
        let client = NodeClient::new(api.url(), &NodeToken::new(node, [1; 32])).unwrap();
        let heads = Heads::default();
        let metadata = NodeVolumeMetadata::new(client, heads.clone());
        let volume = VolumeId::from_ulid(ulid::Ulid::generate());
        let previous = ManifestId::from_ulid(ulid::Ulid::generate());
        let a = ManifestId::from_ulid(ulid::Ulid::generate());
        let b = ManifestId::from_ulid(ulid::Ulid::generate());
        let lease = Lease {
            owner: LeaseOwnerId::from(node),
            expires_at: jiff::Timestamp::now(),
            seq: 1,
        };
        let header = ManifestHeader {
            size: u64::from(swarmy_core::CHUNK_SIZE),
            chunk_size: swarmy_core::CHUNK_SIZE,
            root_hash: ContentHash([1; 32]),
        };
        let retention = NonZeroUsize::new(1).unwrap();
        metadata
            .publish(volume, &lease, previous, a, &header, retention)
            .await
            .unwrap();
        assert_eq!(heads.get(volume), Some(a));
        assert!(
            metadata
                .publish(volume, &lease, a, b, &header, retention)
                .await
                .is_err()
        );
        assert_eq!(heads.get(volume), Some(a));
        assert_eq!(count.load(Ordering::SeqCst), 2);
        guard.cleanup().await;
    }
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
