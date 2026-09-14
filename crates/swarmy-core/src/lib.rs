//! Domain types and rules shared by every Swarmy service. No I/O, no clock, no network.
//!
//! An agent is data: an append-only event log, a snapshot, and a durable volume. This
//! crate defines the identifiers, events, and state machines those services agree on.
//! See `docs/DESIGN.md` for the design this crate implements.

mod encoding;
mod event;
mod id;
mod message;
mod session;
mod store;

pub use encoding::{EncodingError, STORAGE_VERSION, decode, encode};
pub use event::Event;
pub use id::{AgentId, LeaseOwnerId, MessageId, RequestId, SessionId};
pub use message::{Message, MessageRole, Part, ToolCallId, ToolCallRecord, ToolResult};
pub use session::{Lease, SessionRecord, SessionState, SnapshotRef, can_transition};

pub use store::{IdempotencyRecord, IdempotencyState, InflightRecord, RunnableEntry};
