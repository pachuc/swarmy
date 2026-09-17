use serde::{Deserialize, Serialize};

use crate::{
    Message, RequestId, SessionState, SnapshotRef, ToolCallId, ToolCallRecord, ToolResult,
};

/// One immutable entry in a session log. Sequence numbers start at one and are
/// assigned by the store when appending; they are distinct from request step ids.
///
/// This is an externally tagged Serde enum. Version 1 stores its tag as a postcard
/// discriminant, so new variants must be appended and existing variants must not
/// be reordered. New readers retain the ability to decode existing events.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Event {
    MessageAppended {
        seq: u64,
        message: Message,
    },
    InferenceRequested {
        seq: u64,
        request_id: RequestId,
        /// The step used to derive the request id, even if earlier events were appended.
        step: u64,
    },
    InferenceCompleted {
        seq: u64,
        request_id: RequestId,
        message: Message,
    },
    ToolCallRequested {
        seq: u64,
        request_id: RequestId,
        call: ToolCallRecord,
    },
    ToolCallCompleted {
        seq: u64,
        request_id: RequestId,
        call_id: ToolCallId,
        result: ToolResult,
    },
    StateChanged {
        seq: u64,
        from: SessionState,
        to: SessionState,
    },
    SnapshotWritten {
        seq: u64,
        snapshot: SnapshotRef,
    },
    /// Provider retries were exhausted; the next step can handle the failure.
    InferenceFailed {
        seq: u64,
        request_id: RequestId,
        error: String,
    },
}

impl Event {
    /// Set the sequence assigned by a successful store append.
    pub fn set_seq(&mut self, value: u64) {
        match self {
            Self::MessageAppended { seq, .. }
            | Self::InferenceRequested { seq, .. }
            | Self::InferenceCompleted { seq, .. }
            | Self::ToolCallRequested { seq, .. }
            | Self::ToolCallCompleted { seq, .. }
            | Self::StateChanged { seq, .. }
            | Self::SnapshotWritten { seq, .. }
            | Self::InferenceFailed { seq, .. } => *seq = value,
        }
    }

    #[must_use]
    pub const fn seq(&self) -> u64 {
        match self {
            Self::MessageAppended { seq, .. }
            | Self::InferenceRequested { seq, .. }
            | Self::InferenceCompleted { seq, .. }
            | Self::ToolCallRequested { seq, .. }
            | Self::ToolCallCompleted { seq, .. }
            | Self::StateChanged { seq, .. }
            | Self::SnapshotWritten { seq, .. }
            | Self::InferenceFailed { seq, .. } => *seq,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        SessionId, decode, encode,
        encoding::tests::assert_round_trip,
        message::tests::{message, tool_call, tool_result},
    };
    use ulid::Ulid;

    fn events() -> [Event; 8] {
        let request_id = RequestId::for_step(SessionId::from_ulid(Ulid::from_parts(1, 2)), 1);
        [
            Event::MessageAppended {
                seq: 1,
                message: message(),
            },
            Event::InferenceRequested {
                seq: 2,
                request_id,
                step: 1,
            },
            Event::InferenceCompleted {
                seq: 3,
                request_id,
                message: message(),
            },
            Event::ToolCallRequested {
                seq: 4,
                request_id,
                call: tool_call(),
            },
            Event::ToolCallCompleted {
                seq: 5,
                request_id,
                call_id: ToolCallId("call_provider_123".into()),
                result: tool_result(),
            },
            Event::StateChanged {
                seq: 6,
                from: SessionState::Leased,
                to: SessionState::Idle,
            },
            Event::SnapshotWritten {
                seq: 7,
                snapshot: SnapshotRef {
                    object_key: "snapshots/session/6".into(),
                    seq: 6,
                },
            },
            Event::InferenceFailed {
                seq: 8,
                request_id,
                error: "provider failed".into(),
            },
        ]
    }

    #[test]
    fn every_event_round_trips_with_its_sequence_and_tag() {
        let tags = [
            "message_appended",
            "inference_requested",
            "inference_completed",
            "tool_call_requested",
            "tool_call_completed",
            "state_changed",
            "snapshot_written",
            "inference_failed",
        ];
        for (index, (event, tag)) in events().into_iter().zip(tags).enumerate() {
            assert_round_trip(&event);
            assert_eq!(event.seq(), u64::try_from(index).unwrap() + 1);
            let json = serde_json::to_value(&event).unwrap();
            assert_eq!(json.as_object().unwrap().len(), 1);
            assert_eq!(json[tag]["seq"], event.seq());
            // Freeze existing discriminants so adding a variant cannot silently
            // change the interpretation of old stored events.
            assert_eq!(usize::from(encode(&event).unwrap()[1]), index);
        }
    }

    #[test]
    fn version_one_event_fixture_remains_readable() {
        // Version 1, StateChanged tag 5, seq 42, Leased tag 2, Idle tag 0.
        let bytes = [1, 5, 42, 2, 0];
        let event = Event::StateChanged {
            seq: 42,
            from: SessionState::Leased,
            to: SessionState::Idle,
        };
        assert_eq!(decode::<Event>(&bytes).unwrap(), event);
        assert_eq!(encode(&event).unwrap(), bytes);
    }
}
