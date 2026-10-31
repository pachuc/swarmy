//! Wire contract between sandbox nodes and the node API on the primary.
#![deny(unreachable_pub)]
use std::time::Duration;
mod client;
mod refusal;
mod types;
pub use client::{ClientError, NodeClient};
pub use refusal::{Failure, FenceKind, NodeRefusal};
pub use types::{
    LeaseLengths, NodeAdvert, PlacementOccupancy, PlacementsResponse, RegisterRequest,
    RegisterResponse,
};
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
