use crate::{
    NodeApiState,
    auth::AuthenticatedNode,
    failure::{Answer, idempotent},
};
use axum::{Extension, Json, extract::State};
use jiff::Timestamp;
use swarmy_core::{NodeId, NodeRecord, PlacementRecord};
use swarmy_node_protocol::{NodeAdvert, RegisterRequest, RegisterResponse};
use swarmy_store::{MAX_SCAN_LIMIT, Store};
pub(crate) fn node_record(node: NodeId, advert: &NodeAdvert, now: Timestamp) -> NodeRecord {
    NodeRecord {
        node_id: node,
        roles: advert.roles.clone(),
        capacity: advert.capacity.clone(),
        last_heartbeat: now,
        cached_images: Vec::new(),
        gpu: advert.gpu.clone(),
    }
}
pub(crate) async fn all_placements(
    store: &Store,
    node: NodeId,
) -> swarmy_store::Result<Vec<PlacementRecord>> {
    let mut result = Vec::new();
    let mut cursor = None;
    loop {
        let page = store.list_by_node(node, cursor, MAX_SCAN_LIMIT).await?;
        if page.is_empty() {
            break;
        }
        cursor = page.last().map(|p| p.agent_id);
        result.extend(page);
    }
    Ok(result)
}
pub(crate) async fn handle(
    State(state): State<NodeApiState>,
    Extension(AuthenticatedNode(node)): Extension<AuthenticatedNode>,
    Json(request): Json<RegisterRequest>,
) -> Result<Json<RegisterResponse>, Answer> {
    let now = state.store.now();
    let record = node_record(node, &request.node, now);
    idempotent(|| state.store.put_node(&record)).await?;
    let previous = all_placements(&state.store, node).await?;
    Ok(Json(RegisterResponse {
        server_now: now,
        previous,
        leases: state.lease_lengths(),
    }))
}
