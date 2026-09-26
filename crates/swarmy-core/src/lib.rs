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
mod selection;
mod session;
pub use selection::{InferenceField, InferenceSelection, ResolvedSelection};
mod store;
mod volume;

pub use encoding::{EncodingError, STORAGE_VERSION, decode, encode};
pub use encoding::{json, trailing};
pub use event::Event;
pub use id::{
    AgentId, ImageTag, LeaseOwnerId, ManifestId, MessageId, NodeId, ProcessId, RequestId,
    SessionId, TimerId, VolumeId,
};
pub use message::{Message, MessageRole, Part, ToolCallId, ToolCallRecord, ToolResult};
pub use scheduler::{Nudge, RUNNABLE_PARTITIONS, WakeReply, WakeRequest, runnable_partition};
pub use session::{
    ConversationSummary, Lease, SessionKind, SessionRecord, SessionState, SnapshotRef,
    can_transition,
};

pub use store::{IdempotencyRecord, IdempotencyState, InflightRecord, RunnableEntry};

pub use volume::{CHUNK_SIZE, ContentHash, GcRun, ImageRecord, ManifestHeader, VolumeRecord};

mod node;
pub use node::{NodeCapacity, NodeRecord, NodeRole};
mod sandbox;
pub use sandbox::{
    BlockDevice, ExecOutput, ExecRequest, ExecResult, GpuRequirement, PauseHandle, RuntimeCaps,
    Sandbox, SandboxRequirements, SandboxSpec,
};

mod tool;
pub use tool::{
    BashArguments, BashResult, EmptyArguments, MAX_TOOL_OUTPUT_BYTES, PlaceReply, PlaceRequest,
    PlacedToolClaim, ProcessArguments, ProcessListArguments, ProcessStartArguments,
    SandboxArgumentError, SandboxArguments, SandboxRecord, ToolClaim, ToolJob, WebFetchArguments,
    WriteStdinArguments, cap_tool_output, tool_spill_path,
};

mod placement;
pub use placement::{AgentCallStatus, PlacementChangeReason, PlacementRecord};

mod rebuild;
pub use rebuild::computer_rebuilt_message;

mod turn;
pub use turn::{TurnEvent, TurnStage};

mod agent;
pub use agent::{AgentRecord, AgentSettings};
pub mod route;
pub use route::{ANY_ENTRY, ExpandedRouteStep, MAX_ROUTE_STEPS, RouteRecord, RouteStep};

mod memory;
pub use memory::MemoryRequest;

mod files;
pub use files::{EditArguments, LsArguments, ReadArguments, SearchArguments, WriteArguments};
mod plan;
pub use plan::{PlanStatus, PlanStep, UpdatePlanArguments};

mod reasoning;
pub use reasoning::{InvalidReasoningEffort, ReasoningEffort};

mod timer;
pub use timer::{
    CancelTimerArguments, MAX_AGENT_TIMERS, MAX_TIMER_NOTE_BYTES, SetTimerArguments, TimerRecord,
    TimerStatus,
};

pub mod credential;
pub use credential::{CredentialKind, CredentialRecord, CredentialScope, CredentialStatus};

pub mod quota;

mod usage;
pub use usage::{TokenUsage, UsageTotals};

/// An ephemeral token update on the live bus, without a durable cursor.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LiveTokenDelta {
    pub turn_id: String,
    pub position: u64,
    pub text: String,
}
