use crate::{NodeApiState, auth::AuthenticatedNode, explain, failure::Answer};
use axum::{Extension, Json, extract::State};
use swarmy_node_protocol::{NodeRefusal, ToolRouteRequest, ToolRouteResponse};
use swarmy_store::{FenceError, StoreError};
/// Route a tool job to its agent after checking dispatch ownership.
pub(crate) async fn route(
    State(state): State<NodeApiState>,
    Extension(AuthenticatedNode(node)): Extension<AuthenticatedNode>,
    Json(request): Json<ToolRouteRequest>,
) -> Result<Json<ToolRouteResponse>, Answer> {
    let now = state.store.now();
    let agent = match state.store.tool_agent(&request.job, node).await {
        Ok(Some(agent)) => agent,
        Ok(None) => return Err(NodeRefusal::AlreadyDone.into()),
        Err(StoreError::Fence(FenceError::PlacementMismatch)) => {
            return Err(explain::dispatch_fence(&state.store, node, &request.job).await);
        }
        Err(error) => return Err(error.into()),
    };
    Ok(Json(ToolRouteResponse {
        server_now: now,
        agent,
    }))
}
