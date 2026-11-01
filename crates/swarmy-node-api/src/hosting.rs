use crate::{
    NodeApiState,
    auth::AuthenticatedNode,
    authorize::{Echoed, authorize_token},
    explain,
    failure::{Answer, idempotent},
    sync,
};
use axum::{Extension, Json, extract::State};
use swarmy_core::{LeaseOwnerId, ManifestId, SessionId};
use swarmy_node_protocol::{
    Ack, FenceKind, HostingBootSetRequest, HostingCloseRequest, HostingOpenRequest,
    HostingOpenResponse, HostingReadyRequest, NodeRefusal, WRITER_LEASE,
};
use swarmy_store::{FenceError, OpenHosting, StoreError};
/// Open only a job dispatched to the caller; each store step is repeatable.
pub(crate) async fn open(
    State(state): State<NodeApiState>,
    Extension(AuthenticatedNode(node)): Extension<AuthenticatedNode>,
    Json(request): Json<HostingOpenRequest>,
) -> Result<Json<HostingOpenResponse>, Answer> {
    let now = state.store.now();
    let (agent, dispatched) = tokio::join!(
        state.store.tool_agent(&request.job, node),
        state.store.tool_placement(request.job.request_id)
    );
    let agent = match agent {
        Ok(Some(agent)) => agent,
        Ok(None) => return Err(NodeRefusal::AlreadyDone.into()),
        Err(StoreError::Fence(FenceError::PlacementMismatch)) => {
            return Err(explain::dispatch_fence(&state.store, node, &request.job).await);
        }
        Err(error) => return Err(error.into()),
    };
    if agent != request.agent {
        return Err(NodeRefusal::Fenced(FenceKind::ToolJob).into());
    }
    if dispatched?.is_none() {
        return Err(NodeRefusal::Fenced(FenceKind::DispatchPlacementStale {
            detail: "the job has no dispatch placement".into(),
        })
        .into());
    }
    let placement = match idempotent(|| {
        state.store.open_hosting(
            node,
            request.agent,
            &request.job,
            request.previous_epoch,
            state.placement_lease,
        )
    })
    .await?
    {
        OpenHosting::Opened(placement) => placement,
        OpenHosting::EpochStopped { epoch } => {
            return Err(NodeRefusal::EpochStopped { epoch }.into());
        }
        OpenHosting::PlacedElsewhere { node, epoch } => {
            return Err(NodeRefusal::PlacedElsewhere { node, epoch }.into());
        }
    };
    let volume =
        idempotent(|| state.store.agent_volume(request.job.session_id, &placement)).await?;
    let ((opening, boot_set), spec) = tokio::try_join!(
        async {
            let opening = idempotent(|| {
                state
                    .store
                    .open_writer_lease(volume, LeaseOwnerId::from(node), WRITER_LEASE)
            })
            .await
            .map_err(Answer::from)?;
            let boot_set = boot_set_for(&state.store, opening.head, request.job.session_id)
                .await
                .map_err(Answer::from)?;
            Ok::<_, Answer>((opening, boot_set))
        },
        async {
            state
                .store
                .sandbox_spec(request.agent, request.job.session_id)
                .await
                .map_err(Answer::from)
        },
    )?;
    Ok(Json(HostingOpenResponse {
        server_now: now,
        placement,
        volume,
        writer: opening.lease,
        head: opening.head,
        header: opening.header,
        spec,
        boot_set,
    }))
}
/// Record an address and enqueue boot metrics.
pub(crate) async fn ready(
    State(state): State<NodeApiState>,
    Extension(AuthenticatedNode(node)): Extension<AuthenticatedNode>,
    Json(request): Json<HostingReadyRequest>,
) -> Result<Json<Ack>, Answer> {
    let now = state.store.now();
    authorize_token(node, Echoed::Placement(&request.placement))?;
    idempotent(|| {
        state
            .store
            .set_placement_address(&request.placement, request.address)
    })
    .await?;
    sync::queue_metrics(&state, request.metrics);
    Ok(Json(Ack { server_now: now }))
}
/// Release the writer before releasing the placement.
pub(crate) async fn close(
    State(state): State<NodeApiState>,
    Extension(AuthenticatedNode(node)): Extension<AuthenticatedNode>,
    Json(request): Json<HostingCloseRequest>,
) -> Result<Json<Ack>, Answer> {
    let now = state.store.now();
    authorize_token(node, Echoed::Placement(&request.placement))?;
    if let Some(ref writer) = request.writer {
        authorize_token(node, Echoed::WriterLease(&writer.lease))?;
    }
    if let Some(ref writer) = request.writer {
        idempotent(|| {
            state
                .store
                .release_writer_lease(writer.volume, &writer.lease)
        })
        .await?;
    }
    idempotent(|| state.store.release(&request.placement)).await?;
    Ok(Json(Ack { server_now: now }))
}

/// Look up the head first, then the pinned image as a fallback.
async fn boot_set_for(
    store: &swarmy_store::Store,
    head: ManifestId,
    session: SessionId,
) -> swarmy_store::Result<Option<String>> {
    if let Some(key) = store.boot_set(head).await? {
        return Ok(Some(key));
    }
    let Some(image) = store.pinned_image(session).await? else {
        return Ok(None);
    };
    store.boot_set(image.manifest_id).await
}

/// Record a boot's set against the opened head and the session image.
pub(crate) async fn boot_set(
    State(state): State<NodeApiState>,
    Extension(AuthenticatedNode(node)): Extension<AuthenticatedNode>,
    Json(request): Json<HostingBootSetRequest>,
) -> Result<Json<Ack>, Answer> {
    let now = state.store.now();
    authorize_token(node, Echoed::Placement(&request.placement))?;
    super::images::validate_boot_set_key(&request.key)?;
    let head_recorded =
        idempotent(|| state.store.record_boot_set(request.head, &request.key)).await?;
    let image_recorded = if let Some(image) = state.store.pinned_image(request.session).await? {
        idempotent(|| state.store.record_boot_set(image.manifest_id, &request.key)).await?
    } else {
        false
    };
    tracing::info!(head_recorded, image_recorded, "boot set reported");
    Ok(Json(Ack { server_now: now }))
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use std::sync::Arc;
    use swarmy_store::blob::MemoryBlobStore;
    use ulid::Ulid;

    #[tokio::test]
    async fn boot_set_for_prefers_head_then_image() {
        let Some(stack) = swarmy_testkit::Stack::load("na9-boot-lookup") else {
            return;
        };
        let (store, _guard) = stack.open_store(Arc::new(MemoryBlobStore::default())).await;
        let session = SessionId::from_ulid(Ulid::generate());
        let head = ManifestId::from_ulid(Ulid::generate());
        assert_eq!(boot_set_for(&store, head, session).await.unwrap(), None);
        // The pinned-image lookup is also covered by the hosting integration test;
        // here an unknown session has no fallback, even with another manifest recorded.
        let other = ManifestId::from_ulid(Ulid::generate());
        let key = swarmy_core::boot_set_key(b"head");
        store.record_boot_set(other, &key).await.unwrap();
        assert_eq!(boot_set_for(&store, head, session).await.unwrap(), None);
        store.record_boot_set(head, &key).await.unwrap();
        assert_eq!(
            boot_set_for(&store, head, session).await.unwrap(),
            Some(key)
        );
    }
}
