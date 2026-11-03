use crate::{NodeApiState, auth::AuthenticatedNode, failure::Answer, register::all_placements};
use axum::{Extension, Json, extract::State};
use futures::future::try_join_all;
use swarmy_node_protocol::{PlacementOccupancy, PlacementsResponse};
pub(crate) async fn handle(
    State(state): State<NodeApiState>,
    Extension(AuthenticatedNode(node)): Extension<AuthenticatedNode>,
) -> Result<Json<PlacementsResponse>, Answer> {
    let now = state.store.now();
    let placements = all_placements(&state.store, node).await?;
    let placements = try_join_all(placements.into_iter().map(|placement| {
        let store = &state.store;
        async move {
            let status = store.agent_call_status(placement.agent_id).await?;
            let occupied =
                status.is_some_and(|s| s.holder_session_id.is_some() || s.queued_calls > 0);
            Ok::<_, swarmy_store::StoreError>(PlacementOccupancy {
                placement,
                occupied,
            })
        }
    }))
    .await?;
    Ok(Json(PlacementsResponse {
        server_now: now,
        placements,
    }))
}
