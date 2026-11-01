//! Authenticated node access to durable state on the primary.
#![deny(unreachable_pub)]
use axum::{
    Router, middleware,
    routing::{get, post},
};
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use swarmy_bus::Bus;
use swarmy_node_protocol::{
    GITHUB_TOKEN_PATH, HOSTING_BOOT_SET_PATH, HOSTING_CLOSE_PATH, HOSTING_OPEN_PATH,
    HOSTING_READY_PATH, LeaseLengths, PLACEMENTS_PATH, REGISTER_PATH, SYNC_INTERVAL, SYNC_PATH,
    TOOL_CLAIM_LEASE, TOOL_CLAIM_PATH, TOOL_COMPLETE_PATH, TOOL_ROUTE_PATH, VOLUME_PROTECT_PATH,
    VOLUME_PUBLISH_PATH, WRITER_LEASE,
};
use swarmy_store::Store;
mod auth;
mod authorize;
mod credentials;
mod explain;
mod failure;
mod hosting;
mod placements;
mod register;
pub mod role;
mod sync;
mod tools;
mod volumes;
/// Maximum lifetime of a cached positive credential verification.
pub const CREDENTIAL_CACHE_TTL: Duration = Duration::from_secs(10);
/// Server policy derived from settings.
#[derive(Clone, Copy, Debug)]
pub struct NodeApiPolicy {
    pub placement_lease: Duration,
    pub snapshot_retention: NonZeroUsize,
    pub resend_interval: Duration,
}
impl NodeApiPolicy {
    /// Derive lease and retention policy from service settings.
    #[must_use]
    pub fn from_settings(settings: &swarmy_config::Settings) -> Self {
        Self {
            placement_lease: settings.scheduler.placement_lease_secs,
            snapshot_retention: settings.volume_snapshots.retention,
            resend_interval: settings.scheduler.resend_interval_ms,
        }
    }
}
/// Shared server resources.
#[derive(Clone)]
pub struct NodeApiState {
    store: Store,
    bus: Bus,
    resend_interval: Duration,
    placement_lease: Duration,
    snapshot_retention: NonZeroUsize,
    build: &'static str,
    credentials: Arc<auth::CredentialCache>,
}
impl NodeApiState {
    /// Use the scheduler placement lease as the server's policy.
    #[must_use]
    pub fn new(store: Store, bus: Bus, policy: NodeApiPolicy) -> Self {
        Self {
            store,
            bus,
            resend_interval: policy.resend_interval,
            placement_lease: policy.placement_lease,
            snapshot_retention: policy.snapshot_retention,
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
        .route(TOOL_ROUTE_PATH, post(tools::route))
        .route(TOOL_CLAIM_PATH, post(tools::claim))
        .route(TOOL_COMPLETE_PATH, post(tools::complete))
        .route(HOSTING_OPEN_PATH, post(hosting::open))
        .route(HOSTING_READY_PATH, post(hosting::ready))
        .route(HOSTING_BOOT_SET_PATH, post(hosting::boot_set))
        .route(HOSTING_CLOSE_PATH, post(hosting::close))
        .route(VOLUME_PUBLISH_PATH, post(volumes::publish))
        .route(VOLUME_PROTECT_PATH, post(volumes::protect))
        .route(GITHUB_TOKEN_PATH, post(credentials::github))
        .route(REGISTER_PATH, post(register::handle))
        .route(SYNC_PATH, post(sync::handle))
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

#[cfg(feature = "test-support")]
pub mod test_support;
