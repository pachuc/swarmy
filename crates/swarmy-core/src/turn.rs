//! Turn observations are separate from the durable conversation log.
use crate::{MessageId, RequestId, SessionId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnStage {
    Submitted,
    Appended,
    Nudged,
    Claimed,
    InferenceStarted,
    InferenceFinished,
    ToolDispatched,
    ToolCompleted,
    Idle,
    FinalTextRendered,
    InputEnabled,
    FirstToken,
}

/// Monotonic time is comparable only within the same host boot. Wall time is
/// retained for correlation across hosts, never silently treated as monotonic.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnEvent {
    pub session_id: SessionId,
    pub turn_id: MessageId,
    pub stage: TurnStage,
    pub request_id: Option<RequestId>,
    pub clock_id: String,
    pub monotonic_ns: u64,
    pub unix_ns: i128,
}
