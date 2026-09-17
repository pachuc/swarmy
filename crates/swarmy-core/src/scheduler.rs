//! Scheduler messages contain identities, never authoritative work state.
use serde::{Deserialize, Serialize};

use crate::{SessionId, SessionState};

/// Workers must claim a store lease before acting on this hint.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Nudge {
    pub session_id: SessionId,
}

/// Ask the scheduler to make an idle session runnable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WakeRequest {
    pub session_id: SessionId,
}

/// A repeated wake is safe; active and completed sessions keep their state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WakeReply {
    /// The session is runnable. Workers may already have claimed it by receipt.
    Runnable,
    /// The session was not idle or runnable and was left alone.
    Unchanged(SessionState),
    NotFound,
    /// The caller may retry; a store commit might already have succeeded.
    Failed(String),
}

pub const RUNNABLE_PARTITIONS: u16 = 256;

/// Stable BLAKE3 partition of the session's 16 ULID bytes.
#[must_use]
pub fn runnable_partition(id: SessionId) -> u16 {
    u16::from(blake3::hash(&id.as_ulid().to_bytes()).as_bytes()[0])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::tests::assert_round_trip;

    #[test]
    fn scheduler_messages_round_trip() {
        let session_id = SessionId::from_ulid(ulid::Ulid::from_parts(1, 2));
        assert_round_trip(&Nudge { session_id });
        assert_round_trip(&WakeRequest { session_id });
        for reply in [
            WakeReply::Runnable,
            WakeReply::Unchanged(SessionState::WaitingInference),
            WakeReply::NotFound,
            WakeReply::Failed("store unavailable".into()),
        ] {
            assert_round_trip(&reply);
        }
    }
}
