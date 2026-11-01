//! Wire contract between sandbox nodes and the node API on the primary.
#![deny(unreachable_pub)]
use std::time::Duration;
mod client;
mod credentials;
mod hosting;
mod refusal;
#[cfg(test)]
mod roundtrip;
mod sync;
mod tools;
mod types;
mod volumes;
pub use client::{ClientError, NodeClient};
pub use credentials::{GithubTokenRequest, GithubTokenResponse};
pub use hosting::{
    HostingBootSetRequest, HostingCloseRequest, HostingOpenRequest, HostingOpenResponse,
    HostingReadyRequest,
};
pub use refusal::{Failure, FenceKind, NodeRefusal};
pub use sync::{
    EvictReason, HostedPlacement, MetricBatch, NodeMetric, PlacementRenewal, Renewal, ScratchEntry,
    ScratchVerdict, SyncRequest, SyncResponse, WriterToken,
};
pub use tools::{
    ToolClaimRequest, ToolClaimResponse, ToolCompleteRequest, ToolCompleteResponse,
    ToolRouteRequest, ToolRouteResponse,
};
pub use types::{
    Ack, LeaseLengths, NodeAdvert, PlacementOccupancy, PlacementsResponse, RegisterRequest,
    RegisterResponse,
};
pub use volumes::{
    VolumeProtectRequest, VolumeProtectResponse, VolumePublishRequest, VolumeReleaseRequest,
};
/// Sync all periodic node state.
pub const SYNC_PATH: &str = "/node/v1/sync";
/// Register a node.
pub const REGISTER_PATH: &str = "/node/v1/register";
/// Read a node's placements.
pub const PLACEMENTS_PATH: &str = "/node/v1/placements";
/// Exact binary identity header.
pub const BUILD_HEADER: &str = "x-swarmy-build";
/// Sync cadence.
pub const SYNC_INTERVAL: Duration = Duration::from_secs(5);
/// Tool claim lease.
pub const TOOL_CLAIM_LEASE: Duration = Duration::from_secs(30);
/// Writer lease.
pub const WRITER_LEASE: Duration = Duration::from_secs(60);
/// Non-lease request deadline.
pub const DEFAULT_DEADLINE: Duration = Duration::from_secs(30);

/// Claim one tool call under this node's placement.
pub const TOOL_CLAIM_PATH: &str = "/node/v1/tool/claim";
/// Commit one tool call's result.
pub const TOOL_COMPLETE_PATH: &str = "/node/v1/tool/complete";
/// Resolve a tool job's agent.
pub const TOOL_ROUTE_PATH: &str = "/node/v1/tool/route";
/// Open agent hosting.
pub const HOSTING_OPEN_PATH: &str = "/node/v1/hosting/open";
/// Record a booted sandbox.
pub const HOSTING_BOOT_SET_PATH: &str = "/node/v1/hosting/boot-set";
/// Record a booted sandbox.
pub const HOSTING_READY_PATH: &str = "/node/v1/hosting/ready";
/// Release hosting.
pub const HOSTING_CLOSE_PATH: &str = "/node/v1/hosting/close";
/// Publish a volume head.
pub const VOLUME_PUBLISH_PATH: &str = "/node/v1/volume/publish";
/// `POST`: release a writer lease this node owns.
pub const VOLUME_RELEASE_PATH: &str = "/node/v1/volume/release";
/// Protect reused chunks.
pub const VOLUME_PROTECT_PATH: &str = "/node/v1/volume/protect";
/// Read a hosted agent's GitHub token.
pub const GITHUB_TOKEN_PATH: &str = "/node/v1/credentials/github";
/// Maximum hashes in one protection request.
pub const MAX_PROTECT_HASHES: usize = 1024;
