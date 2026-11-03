use crate::{
    NodeApiState,
    auth::AuthenticatedNode,
    failure::{Answer, idempotent},
};
use axum::{Extension, Json, extract::State};
use swarmy_core::{AgentId, ImageProbe, ImageRecord, LeaseOwnerId, is_boot_set_key};
use swarmy_node_protocol::{
    Ack, Failure, ImageProbeFinishRequest, ImageProbeOpenResponse, NodeRefusal, WRITER_LEASE,
};

pub(crate) fn validate_boot_set_key(key: &str) -> Result<(), Answer> {
    if !is_boot_set_key(key) {
        return Err(Answer(Failure::Rejected {
            message: "invalid boot set key".into(),
        }));
    }
    Ok(())
}

pub(crate) async fn probe_open(
    State(state): State<NodeApiState>,
    Extension(AuthenticatedNode(node)): Extension<AuthenticatedNode>,
    Json(probe): Json<ImageProbe>,
) -> Result<Json<ImageProbeOpenResponse>, Answer> {
    let now = state.store.now();
    if state.store.get_image(&probe.name, &probe.tag).await? != Some(probe.manifest) {
        return Err(NodeRefusal::AlreadyDone.into());
    }
    let Some(volume) = idempotent(|| state.store.open_image_probe(probe.manifest)).await? else {
        return Err(NodeRefusal::AlreadyDone.into());
    };
    let opening = idempotent(|| {
        state
            .store
            .open_writer_lease(volume, LeaseOwnerId::from(node), WRITER_LEASE)
    })
    .await?;
    let spec = state
        .store
        .image_sandbox_spec(
            AgentId::from_ulid(probe.manifest.as_ulid()),
            &ImageRecord {
                name: probe.name,
                tag: probe.tag,
                manifest_id: probe.manifest,
            },
        )
        .await?;
    Ok(Json(ImageProbeOpenResponse {
        server_now: now,
        volume,
        writer: opening.lease,
        head: opening.head,
        header: opening.header,
        spec,
    }))
}

pub(crate) async fn probe_finish(
    State(state): State<NodeApiState>,
    Extension(AuthenticatedNode(_node)): Extension<AuthenticatedNode>,
    Json(request): Json<ImageProbeFinishRequest>,
) -> Result<Json<Ack>, Answer> {
    let now = state.store.now();
    if let Some(key) = &request.key {
        validate_boot_set_key(key)?;
    }
    let recorded = idempotent(|| {
        state
            .store
            .finish_image_probe(request.manifest, request.key.as_deref())
    })
    .await?;
    tracing::info!(%request.manifest, recorded, "image probe finished");
    Ok(Json(Ack { server_now: now }))
}
