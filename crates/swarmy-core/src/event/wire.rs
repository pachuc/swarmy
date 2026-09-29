//! Encode current postcard events; JSON keeps the public completion name.
use super::{
    Event, FailureKind, InferenceCompletion, Message, RequestId, SessionState, SnapshotRef,
    ToolCallId, ToolCallRecord, ToolResult,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Serialize, Deserialize)]
#[serde(remote = "Event", rename_all = "snake_case")]
enum HumanEvent {
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
        /// Flattened so the JSON shape stays flat: one object with the
        /// sequence, the request id, and every completion field.
        #[serde(flatten)]
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
        #[serde(default)]
        retryable: bool,
        #[serde(default)]
        retry_at: Option<jiff::Timestamp>,
        #[serde(default)]
        failure_kind: FailureKind,
    },
    MessageQueued {
        seq: u64,
        message: Message,
        queued_at: jiff::Timestamp,
    },
}

#[derive(Serialize, Deserialize)]
enum BinaryEvent {
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
        failure_kind: FailureKind,
    },
    InferenceCompleted {
        seq: u64,
        request_id: RequestId,
        /// Nested structs encode inline in postcard, so the stored bytes
        /// keep the sequence, the request id, and every completion field
        /// in order with no extra framing.
        completion: InferenceCompletion,
    },
    RetryableInferenceFailed {
        seq: u64,
        request_id: RequestId,
        error: String,
        retryable: bool,
        retry_at: Option<jiff::Timestamp>,
        failure_kind: FailureKind,
    },
    MessageQueued {
        seq: u64,
        message: Message,
        queued_at: jiff::Timestamp,
    },
}

impl Serialize for Event {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            HumanEvent::serialize(self, serializer)
        } else {
            BinaryEvent::from(self.clone()).serialize(serializer)
        }
    }
}
impl<'de> Deserialize<'de> for Event {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            HumanEvent::deserialize(deserializer)
        } else {
            BinaryEvent::deserialize(deserializer).map(Self::from)
        }
    }
}
/// Decode a failure, retryable or not.
fn failed_completion(
    seq: u64,
    request_id: RequestId,
    error: String,
    retryable: bool,
    retry_at: Option<jiff::Timestamp>,
    failure_kind: FailureKind,
) -> Event {
    Event::InferenceFailed {
        seq,
        request_id,
        error,
        retryable,
        retry_at,
        failure_kind,
    }
}

impl From<Event> for BinaryEvent {
    fn from(event: Event) -> Self {
        match event {
            Event::MessageAppended { seq, message } => Self::MessageAppended { seq, message },
            Event::MessageQueued {
                seq,
                message,
                queued_at,
            } => Self::MessageQueued {
                seq,
                message,
                queued_at,
            },
            Event::InferenceRequested {
                seq,
                request_id,
                step,
            } => Self::InferenceRequested {
                seq,
                request_id,
                step,
            },
            Event::InferenceCompleted {
                seq,
                request_id,
                completion,
            } => Self::InferenceCompleted {
                seq,
                request_id,
                completion,
            },
            Event::ToolCallRequested {
                seq,
                request_id,
                call,
            } => Self::ToolCallRequested {
                seq,
                request_id,
                call,
            },
            Event::ToolCallCompleted {
                seq,
                request_id,
                call_id,
                result,
            } => Self::ToolCallCompleted {
                seq,
                request_id,
                call_id,
                result,
            },
            Event::StateChanged { seq, from, to } => Self::StateChanged { seq, from, to },
            Event::SnapshotWritten { seq, snapshot } => Self::SnapshotWritten { seq, snapshot },
            Event::InferenceFailed {
                seq,
                request_id,
                error,
                retryable: false,
                retry_at: None,
                failure_kind: FailureKind::Unknown,
            } => Self::InferenceFailed {
                seq,
                request_id,
                error,
                failure_kind: FailureKind::Unknown,
            },
            Event::InferenceFailed {
                seq,
                request_id,
                error,
                retryable,
                retry_at,
                failure_kind,
            } => Self::RetryableInferenceFailed {
                seq,
                request_id,
                error,
                retryable,
                retry_at,
                failure_kind,
            },
        }
    }
}
impl From<BinaryEvent> for Event {
    fn from(event: BinaryEvent) -> Self {
        match event {
            BinaryEvent::MessageAppended { seq, message } => Self::MessageAppended { seq, message },
            BinaryEvent::MessageQueued {
                seq,
                message,
                queued_at,
            } => Self::MessageQueued {
                seq,
                message,
                queued_at,
            },
            BinaryEvent::InferenceRequested {
                seq,
                request_id,
                step,
            } => Self::InferenceRequested {
                seq,
                request_id,
                step,
            },
            BinaryEvent::ToolCallRequested {
                seq,
                request_id,
                call,
            } => Self::ToolCallRequested {
                seq,
                request_id,
                call,
            },
            BinaryEvent::ToolCallCompleted {
                seq,
                request_id,
                call_id,
                result,
            } => Self::ToolCallCompleted {
                seq,
                request_id,
                call_id,
                result,
            },
            BinaryEvent::StateChanged { seq, from, to } => Self::StateChanged { seq, from, to },
            BinaryEvent::SnapshotWritten { seq, snapshot } => {
                Self::SnapshotWritten { seq, snapshot }
            }
            BinaryEvent::InferenceFailed {
                seq,
                request_id,
                error,
                failure_kind,
            } => failed_completion(seq, request_id, error, false, None, failure_kind),
            BinaryEvent::RetryableInferenceFailed {
                seq,
                request_id,
                error,
                retryable,
                retry_at,
                failure_kind,
            } => failed_completion(seq, request_id, error, retryable, retry_at, failure_kind),
            BinaryEvent::InferenceCompleted {
                seq,
                request_id,
                completion,
            } => Self::InferenceCompleted {
                seq,
                request_id,
                completion,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routed_completions_keep_the_metered_discriminant() {
        let request_id = RequestId::for_step(
            crate::SessionId::from_ulid(ulid::Ulid::from_parts(3, 4)),
            2,
        );
        let plain = Event::InferenceCompleted {
            seq: 5,
            request_id,
            completion: InferenceCompletion {
                message: crate::message::tests::message(),
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                effort_used: None,
                usage: crate::TokenUsage::default(),
                cost_micros: 7,
                effort_requested: None,
                effort_clamped: false,
                entry: None,
                route: None,
                route_step: None,
            },
        };
        // Attributed and plain completions share the completion discriminant.
        assert_eq!(usize::from(crate::encode(&plain).unwrap()[1]), 7);
        assert_eq!(
            crate::decode::<Event>(&crate::encode(&plain).unwrap()).unwrap(),
            plain
        );
        let Event::InferenceCompleted {
            seq,
            request_id,
            mut completion,
        } = plain.clone()
        else {
            unreachable!("plain completion");
        };
        completion.entry = Some("backup".into());
        completion.route = Some("fallback".into());
        completion.route_step = Some(1);
        let routed = Event::InferenceCompleted {
            seq,
            request_id,
            completion,
        };
        let bytes = crate::encode(&routed).unwrap();
        assert_eq!(usize::from(bytes[1]), 7);
        assert_eq!(crate::decode::<Event>(&bytes).unwrap(), routed);
        let json = serde_json::to_value(&routed).unwrap();
        assert_eq!(json["inference_completed"]["entry"], "backup");
        assert_eq!(json["inference_completed"]["route"], "fallback");
        assert_eq!(json["inference_completed"]["route_step"], 1);
        // Old JSON without the new fields still decodes.
        let mut old = json;
        for field in ["entry", "route", "route_step"] {
            old["inference_completed"]
                .as_object_mut()
                .unwrap()
                .remove(field);
        }
        assert_eq!(serde_json::from_value::<Event>(old).unwrap(), plain);
    }

    #[test]
    fn queued_event_has_frozen_binary_layout() {
        let event = Event::MessageQueued {
            seq: 1,
            message: Message {
                id: crate::MessageId::from_ulid(ulid::Ulid::from_bytes([0; 16])),
                role: crate::MessageRole::User,
                parts: vec![],
            },
            queued_at: jiff::Timestamp::UNIX_EPOCH,
        };
        let bytes = crate::encode(&event).unwrap();
        let mut expected = vec![1, 9, 1, 26];
        expected.extend_from_slice(b"00000000000000000000000000");
        expected.extend_from_slice(&[1, 0, 20]);
        expected.extend_from_slice(b"1970-01-01T00:00:00Z");
        assert_eq!(bytes, expected);
        assert_eq!(crate::decode::<Event>(&bytes).unwrap(), event);
    }
}
