use crate::{
    NodeApiState,
    auth::AuthenticatedNode,
    authorize::{Echoed, authorize_token},
    explain,
    failure::{Answer, idempotent},
    sync,
};
use axum::{Extension, Json, extract::State};
use swarmy_core::LeaseOwnerId;
use swarmy_node_protocol::{
    Ack, FenceKind, HostingCloseRequest, HostingOpenRequest, HostingOpenResponse,
    HostingReadyRequest, NodeRefusal, WRITER_LEASE,
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
    let (opening, spec) = tokio::try_join!(
        async {
            idempotent(|| {
                state
                    .store
                    .open_writer_lease(volume, LeaseOwnerId::from(node), WRITER_LEASE)
            })
            .await
            .map_err(Answer::from)
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
        boot_set: None,
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
