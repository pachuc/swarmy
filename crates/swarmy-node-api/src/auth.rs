use crate::{NodeApiState, failure::Answer};
use axum::{
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
            if let Some((known, at)) = entries.get(&token.node())
                && *known == hash
                && at.elapsed() < self.ttl
            {
                return Ok(true);
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
#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use ulid::Ulid;
    #[test]
    fn cache_matches_hash_and_lifetime() {
        let node = NodeId::from_ulid(Ulid::generate());
        let token = NodeToken::new(node, [7; 32]);
        let hash = blake3::Hash::from_bytes(token.secret_hash());
        let cache = CredentialCache::new(Duration::from_secs(60));
        cache
            .verified
            .lock()
            .unwrap()
            .insert(node, (hash, Instant::now()));
        assert!(
            cache
                .verified
                .lock()
                .unwrap()
                .get(&node)
                .is_some_and(|(known, at)| *known == hash && at.elapsed() < cache.ttl)
        );
        let other = NodeToken::new(node, [8; 32]);
        assert_ne!(hash, blake3::Hash::from_bytes(other.secret_hash()));
        let zero = CredentialCache::new(Duration::ZERO);
        zero.verified
            .lock()
            .unwrap()
            .insert(node, (hash, Instant::now()));
        assert!(
            zero.verified
                .lock()
                .unwrap()
                .get(&node)
                .is_some_and(|(_, at)| at.elapsed() >= zero.ttl)
        );
    }
}
