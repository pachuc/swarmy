mod wire;

use crate::{
    Message, RequestId, SessionState, SnapshotRef, ToolCallId, ToolCallRecord, ToolResult,
};

/// Machine-readable cause of a failed inference. Older events default to Unknown.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureKind {
    #[default]
    Unknown,
    OperatorInterrupted,
    GatewayUnserved,
    Provider,
    Publication,
    WaitExceeded,
    ContextOverflow,
}

/// The completion payload shared by the public event and the binary row.
/// One struct keeps the field list in one place; both variants below embed
/// it so each field is written once. Field order matches the stored layout.
/// Defaults keep old JSON readable: early completions predate attribution.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InferenceCompletion {
    pub message: Message,
    #[serde(default)]
    pub provider: String,
    #[serde(default)]
    pub model: String,
    #[serde(default)]
    pub effort_used: Option<crate::ReasoningEffort>,
    #[serde(default)]
    pub usage: crate::TokenUsage,
    #[serde(default)]
    pub cost_micros: u64,
    #[serde(default)]
    pub effort_requested: Option<crate::ReasoningEffort>,
    #[serde(default)]
    pub effort_clamped: bool,
    /// Stored entry label behind this completion, for metering attribution.
    #[serde(default)]
    pub entry: Option<String>,
    /// Route that selected the entry, when a named route resolved it.
    #[serde(default)]
    pub route: Option<String>,
    /// Index into the resolved route, so metering names the exact step.
    #[serde(default)]
    pub route_step: Option<u32>,
}

/// One immutable entry in a session log. Sequence numbers start at one and are
/// assigned by the store when appending; they are distinct from request step ids.
///
/// This is an externally tagged Serde enum. Version 1 stores its tag as a postcard
/// discriminant, so new variants must be appended and existing variants must not
/// be reordered. New readers retain the ability to decode existing events.
#[derive(Clone, Debug, PartialEq, Eq)]
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
        completion: InferenceCompletion,
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
    /// The gateway records an exhausted provider attempt for the next step.
    InferenceFailed {
        seq: u64,
        request_id: RequestId,
        error: String,
        retryable: bool,
        retry_at: Option<jiff::Timestamp>,
        failure_kind: FailureKind,
    },
    /// Recorded at delivery with the original enqueue time; the following
    /// `MessageAppended` is the user input seen by inference.
    MessageQueued {
        seq: u64,
        message: Message,
        queued_at: jiff::Timestamp,
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
            | Self::InferenceFailed { seq, .. }
            | Self::MessageQueued { seq, .. } => *seq = value,
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
            | Self::InferenceFailed { seq, .. }
            | Self::MessageQueued { seq, .. } => *seq,
        }
    }
}

/// An operator interruption is a terminal inference failure, whether appended
/// by the worker or while the store settles an interrupted session.
#[must_use]
pub fn interrupted_event(seq: u64, request_id: crate::RequestId) -> Event {
    Event::InferenceFailed {
        seq,
        request_id,
        error: "interrupted by operator".into(),
        retryable: false,
        retry_at: None,
        failure_kind: FailureKind::OperatorInterrupted,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        SessionId, encode,
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
                completion: InferenceCompletion {
                    message: message(),
                    provider: String::new(),
                    model: String::new(),
                    effort_used: None,
                    usage: crate::TokenUsage::default(),
                    cost_micros: 0,
                    effort_requested: None,
                    effort_clamped: false,
                    entry: None,
                    route: None,
                    route_step: None,
                },
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
                retryable: false,
                retry_at: None,
                failure_kind: FailureKind::Unknown,
            },
        ]
    }

    pub(crate) fn every_event_round_trips_with_its_sequence_and_tag() {
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
            // Freeze current discriminants so adding a variant changes the fixture.
            assert_eq!(
                usize::from(encode(&event).unwrap()[1]),
                match index.cmp(&2) {
                    std::cmp::Ordering::Less => index,
                    std::cmp::Ordering::Equal => 7,
                    std::cmp::Ordering::Greater => index - 1,
                }
            );
        }
    }

    #[test]
    fn old_failure_defaults_to_permanent_and_new_failure_keeps_retry_time() {
        let id = RequestId::for_step(SessionId::from_ulid(Ulid::from_parts(1, 2)), 3);
        let old =
            serde_json::json!({"inference_failed": {"seq": 3, "request_id": id, "error": "old"}});
        assert_eq!(
            serde_json::from_value::<Event>(old).unwrap(),
            Event::InferenceFailed {
                seq: 3,
                request_id: id,
                error: "old".into(),
                retryable: false,
                retry_at: None,
                failure_kind: FailureKind::Unknown,
            }
        );
        let retry_at = "2026-09-23T01:00:00Z".parse().unwrap();
        let event = Event::InferenceFailed {
            seq: 4,
            request_id: id,
            error: "limit reached".into(),
            retryable: true,
            retry_at: Some(retry_at),
            failure_kind: FailureKind::Unknown,
        };
        assert_round_trip(&event);
    }
}
