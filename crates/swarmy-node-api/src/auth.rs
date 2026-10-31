use crate::{NodeApiState, failure::Answer};
use axum::{
    Extension,
    extract::{Request, State},
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};
use swarmy_core::{NodeId, NodeToken};
use swarmy_node_protocol::{BUILD_HEADER, Failure};
use swarmy_store::Store;
/// Identity derived only from verified bearer text.
#[derive(Clone, Copy, Debug)]
pub(crate) struct AuthenticatedNode(pub(crate) NodeId);
pub(crate) struct CredentialCache {
    ttl: Duration,
    verified: Mutex<HashMap<NodeId, (blake3::Hash, Instant)>>,
}
impl CredentialCache {
    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            verified: Mutex::new(HashMap::new()),
        }
    }
    pub(crate) async fn verify(
        &self,
        store: &Store,
        token: &NodeToken,
    ) -> swarmy_store::Result<bool> {
        let hash = blake3::Hash::from_bytes(token.secret_hash());
        {
            let entries = self
                .verified
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some((known, at)) = entries.get(&token.node()) {
                if *known == hash && at.elapsed() < self.ttl {
                    return Ok(true);
                }
            }
        }
        let valid = store.verify_node_credential(token).await?;
        let mut entries = self
            .verified
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if valid {
            entries.insert(token.node(), (hash, Instant::now()));
        } else {
            entries.remove(&token.node());
        }
        Ok(valid)
    }
}
pub(crate) async fn authenticate(
    State(state): State<NodeApiState>,
    mut request: Request,
    next: Next,
) -> Response {
    let Some(text) = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
    else {
        return Answer(Failure::Unauthorized).into_response();
    };
    let Ok(token) = NodeToken::parse(text) else {
        return Answer(Failure::Unauthorized).into_response();
    };
    match state.credentials.verify(&state.store, &token).await {
        Ok(true) => {}
        Ok(false) => return Answer(Failure::Unauthorized).into_response(),
        Err(error) => return Answer::from(error).into_response(),
    }
    let node = token.node();
    let path = request.uri().path().to_owned();
    let start = Instant::now();
    request.extensions_mut().insert(AuthenticatedNode(node));
    let response = next.run(request).await;
    let status = response.status().as_u16();
    let handler_ms = start.elapsed().as_millis();
    tracing::debug!(node = %node, path, status, handler_ms, "node api request");
    response
}
pub(crate) async fn check_build(
    State(state): State<NodeApiState>,
    request: Request,
    next: Next,
) -> Response {
    let node = request
        .headers()
        .get(BUILD_HEADER)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    if node != state.build {
        return Answer(Failure::BuildMismatch {
            server: state.build.into(),
            node: node.into(),
        })
        .into_response();
    }
    next.run(request).await
}
