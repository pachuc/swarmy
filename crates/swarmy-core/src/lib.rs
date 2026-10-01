//! Domain types and rules shared by every Swarmy service. No I/O, no clock, no network.
//!
//! An agent is data: an append-only event log, a snapshot, and a durable volume. This
//! crate defines the identifiers, events, and state machines those services agree on.
//! See `docs/ARCHITECTURE.md` for the design this crate implements.

mod encoding;
mod error;
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
pub use error::error_chain;
pub use event::{Event, FailureKind, InferenceCompletion, interrupted_event};
pub use id::{
    AgentId, ImageTag, LeaseOwnerId, ManifestId, MessageId, NodeId, ProcessId, RequestId,
    SessionId, TimerId, VolumeId,
};
pub use message::{Message, MessageRole, NoticeKind, Part, ToolCallId, ToolCallRecord, ToolResult};
pub use scheduler::{Nudge, RUNNABLE_PARTITIONS, WakeReply, WakeRequest, runnable_partition};
pub use session::{
    Lease, SessionKind, SessionRecord, SessionSettings, SessionState, SnapshotRef, can_transition,
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
    BashArguments, BashResult, EmptyArguments, MAX_TOOL_OUTPUT_BYTES, PlacedToolClaim,
    ProcessArguments, ProcessListArguments, ProcessStartArguments, SandboxArgumentError,
    SandboxArguments, SandboxRecord, ToolJob, WebFetchArguments, WriteStdinArguments,
    cap_tool_output, tool_spill_path,
};

mod placement;
pub use placement::{AgentCallStatus, PlacementChangeReason, PlacementRecord};

mod rebuild;
pub use rebuild::computer_rebuilt_message;

mod turn;
pub use turn::{TurnEvent, TurnStage};

mod agent;
pub use agent::{AgentRecord, AgentSettings};
pub mod best_effort;
pub use best_effort::ignore_best_effort;
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
pub use credential::{
    BEDROCK_CONSOLE_KEY_EXPIRED, CredentialBookkeeping, CredentialEntryKind, CredentialKind,
    CredentialRecord, CredentialScope, CredentialStatus,
};

pub mod quota;

pub mod time;

mod usage;
pub use usage::{TokenUsage, UsageTotals};

/// An ephemeral token update on the live bus, without a durable cursor.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LiveTokenDelta {
    pub turn_id: String,
    pub position: u64,
    pub text: String,
}

/// Bounded retry delay shared by durable inference and reconnect loops.
#[must_use]
pub fn backoff(base: std::time::Duration, attempt: u32, max_doublings: u32) -> std::time::Duration {
    base.saturating_mul(1_u32 << attempt.saturating_sub(1).min(max_doublings.min(31)))
}

#[cfg(test)]
mod backoff_tests {
    #![deny(clippy::disallowed_methods)]
    use super::backoff;
    use std::time::Duration;

    #[test]
    fn starts_at_one_hundred_millis_and_caps_at_three_point_two_seconds() {
        assert_eq!(
            backoff(Duration::from_millis(100), 0, 5),
            Duration::from_millis(100)
        );
        assert_eq!(
            backoff(Duration::from_millis(100), 1, 5),
            Duration::from_millis(100)
        );
        assert_eq!(
            backoff(Duration::from_millis(100), 3, 5),
            Duration::from_millis(400)
        );
        assert_eq!(
            backoff(Duration::from_millis(100), u32::MAX, 5),
            Duration::from_millis(3_200)
        );
    }
}
