//! Domain types and rules shared by every Swarmy service. No I/O, no clock, no network.
//!
//! An agent is data: an append-only event log, a snapshot, and a durable volume. This
//! crate defines the identifiers, events, and state machines those services agree on.
//! See `docs/DESIGN.md` for the design this crate implements.

mod encoding;
mod event;
mod id;
mod message;
mod scheduler;
mod session;
mod store;
mod volume;

pub use encoding::json;
pub use encoding::{EncodingError, STORAGE_VERSION, decode, encode};
pub use event::Event;
pub use id::{
    AgentId, ImageTag, LeaseOwnerId, ManifestId, MessageId, NodeId, RequestId, SessionId, VolumeId,
};
pub use message::{Message, MessageRole, Part, ToolCallId, ToolCallRecord, ToolResult};
pub use scheduler::{Nudge, WakeReply, WakeRequest};
pub use session::{Lease, SessionRecord, SessionState, SnapshotRef, can_transition};

pub use store::{IdempotencyRecord, IdempotencyState, InflightRecord, RunnableEntry};

pub use volume::{CHUNK_SIZE, ContentHash, ImageRecord, ManifestHeader, VolumeRecord};

mod node;
pub use node::{NodeCapacity, NodeRecord, NodeRole};
mod sandbox;
pub use sandbox::{
    BlockDevice, ExecOutput, ExecRequest, ExecResult, PauseHandle, RuntimeCaps, Sandbox,
    SandboxSpec,
};

mod tool;
pub use tool::{
    BashArguments, BashResult, PlaceReply, PlaceRequest, PlacedToolClaim, SandboxRecord, ToolClaim,
    ToolJob,
};

mod placement;
pub use placement::{PlacementChangeReason, PlacementRecord};
