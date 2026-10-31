use crate::{
    NodeApiState,
    auth::AuthenticatedNode,
    authorize::{Echoed, authorize_token},
    failure::{Answer, idempotent},
};
use axum::{Extension, Json, extract::State};
use swarmy_node_protocol::{
    Ack, Failure, MAX_PROTECT_HASHES, VolumeProtectRequest, VolumeProtectResponse,
    VolumePublishRequest,
};
/// Publish a head using server-side retention.
pub(crate) async fn publish(
    State(state): State<NodeApiState>,
    Extension(AuthenticatedNode(node)): Extension<AuthenticatedNode>,
    Json(request): Json<VolumePublishRequest>,
) -> Result<Json<Ack>, Answer> {
    let now = state.store.now();
    authorize_token(node, Echoed::WriterLease(&request.writer.lease))?;
    idempotent(|| {
        state.store.advance_volume_retained(
            request.writer.volume,
            &request.writer.lease,
            request.previous,
            request.next,
            &request.header,
            state.snapshot_retention,
        )
    })
    .await?;
    Ok(Json(Ack { server_now: now }))
}
/// Reject requests too large for a bounded protection transaction.
fn check_protect_len(len: usize) -> Result<(), Answer> {
    if len > MAX_PROTECT_HASHES {
        return Err(Answer(Failure::Rejected {
            message: format!("at most {MAX_PROTECT_HASHES} hashes per request"),
        }));
    }
    Ok(())
}
/// Protect reused chunks; no placement is needed for this conservative operation.
pub(crate) async fn protect(
    State(state): State<NodeApiState>,
    Extension(AuthenticatedNode(_node)): Extension<AuthenticatedNode>,
    Json(request): Json<VolumeProtectRequest>,
) -> Result<Json<VolumeProtectResponse>, Answer> {
    let now = state.store.now();
    check_protect_len(request.hashes.len())?;
    let deleting = idempotent(|| state.store.protect_reused_chunks(&request.hashes)).await?;
    Ok(Json(VolumeProtectResponse {
        server_now: now,
        deleting,
    }))
}
