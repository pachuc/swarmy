use crate::{NodeApiState, auth::AuthenticatedNode, failure::Answer};
use axum::{Extension, Json, extract::State};
use swarmy_node_protocol::{GithubTokenRequest, GithubTokenResponse, NodeRefusal};
/// Return a token only while the requesting node has a live placement.
pub(crate) async fn github(
    State(state): State<NodeApiState>,
    Extension(AuthenticatedNode(node)): Extension<AuthenticatedNode>,
    Json(request): Json<GithubTokenRequest>,
) -> Result<Json<GithubTokenResponse>, Answer> {
    let now = state.store.now();
    match state.store.get_by_agent(request.agent).await? {
        Some(placement) if placement.node_id == node && placement.expires_at > now => {}
        _ => return Err(NodeRefusal::NotYours.into()),
    }
    let token = state.store.agent_github_token(request.agent).await?;
    Ok(Json(GithubTokenResponse {
        server_now: now,
        token,
    }))
}
