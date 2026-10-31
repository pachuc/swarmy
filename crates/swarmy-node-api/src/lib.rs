//! Authenticated node access to durable state on the primary.
#![deny(unreachable_pub)]
use axum::{
    Router, middleware,
    routing::{get, post},
};
use std::{sync::Arc, time::Duration};
use swarmy_node_protocol::{
    LeaseLengths, PLACEMENTS_PATH, REGISTER_PATH, SYNC_INTERVAL, TOOL_CLAIM_LEASE, WRITER_LEASE,
};
use swarmy_store::Store;
mod auth;
mod failure;
mod placements;
mod register;
pub mod role;
/// Maximum lifetime of a cached positive credential verification.
pub const CREDENTIAL_CACHE_TTL: Duration = Duration::from_secs(10);
/// Shared server resources.
#[derive(Clone)]
pub struct NodeApiState {
    store: Store,
    placement_lease: Duration,
    build: &'static str,
    credentials: Arc<auth::CredentialCache>,
}
impl NodeApiState {
    /// Use the scheduler placement lease as the server's policy.
    #[must_use]
    pub fn new(store: Store, placement_lease: Duration) -> Self {
        Self {
            store,
            placement_lease,
            build: swarmy_version::IDENTITY,
            credentials: Arc::new(auth::CredentialCache::new(CREDENTIAL_CACHE_TTL)),
        }
    }
    /// Override credential cache lifetime; zero disables caching.
    #[must_use]
    pub fn with_credential_ttl(mut self, ttl: Duration) -> Self {
        self.credentials = Arc::new(auth::CredentialCache::new(ttl));
        self
    }
    fn lease_lengths(&self) -> LeaseLengths {
        LeaseLengths {
            placement: self.placement_lease,
            tool_claim: TOOL_CLAIM_LEASE,
            writer: WRITER_LEASE,
            sync_interval: SYNC_INTERVAL,
        }
    }
}
/// Assemble the independent node API router.
pub fn router(state: NodeApiState) -> Router {
    Router::new()
        .route(REGISTER_PATH, post(register::handle))
        .route(PLACEMENTS_PATH, get(placements::handle))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth::check_build,
        ))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth::authenticate,
        ))
        .with_state(state)
}
