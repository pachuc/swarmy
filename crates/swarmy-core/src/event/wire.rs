//! Preserve existing postcard discriminants; JSON keeps the public completion name.
use super::{
    Event, Message, RequestId, SessionState, SnapshotRef, ToolCallId, ToolCallRecord, ToolResult,
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
        message: Message,
        #[serde(default)]
        provider: String,
        #[serde(default)]
        model: String,
        #[serde(default)]
        effort_used: Option<crate::ReasoningEffort>,
        #[serde(default)]
        usage: crate::TokenUsage,
        #[serde(default)]
        cost_micros: u64,
        #[serde(default)]
        effort_requested: Option<crate::ReasoningEffort>,
        #[serde(default)]
        effort_clamped: bool,
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
    MeteredInferenceCompleted {
        seq: u64,
        request_id: RequestId,
        message: Message,
        #[serde(default)]
        provider: String,
        #[serde(default)]
        model: String,
        #[serde(default)]
        effort_used: Option<crate::ReasoningEffort>,
        #[serde(default)]
        usage: crate::TokenUsage,
        #[serde(default)]
        cost_micros: u64,
        #[serde(default)]
        effort_requested: Option<crate::ReasoningEffort>,
        #[serde(default)]
        effort_clamped: bool,
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
impl From<Event> for BinaryEvent {
    fn from(event: Event) -> Self {
        match event {
            Event::MessageAppended { seq, message } => Self::MessageAppended { seq, message },
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
                message,
                provider,
                model,
                effort_used,
                usage,
                cost_micros,
                effort_requested,
                effort_clamped,
            } => Self::MeteredInferenceCompleted {
                seq,
                request_id,
                message,
                provider,
                model,
                effort_used,
                usage,
                cost_micros,
                effort_requested,
                effort_clamped,
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
            } => Self::InferenceFailed {
                seq,
                request_id,
                error,
            },
        }
    }
}
impl From<BinaryEvent> for Event {
    fn from(event: BinaryEvent) -> Self {
        match event {
            BinaryEvent::MessageAppended { seq, message } => Self::MessageAppended { seq, message },
            BinaryEvent::InferenceRequested {
                seq,
                request_id,
                step,
            } => Self::InferenceRequested {
                seq,
                request_id,
                step,
            },
            BinaryEvent::InferenceCompleted {
                seq,
                request_id,
                message,
            } => Self::InferenceCompleted {
                seq,
                request_id,
                message,
                provider: String::new(),
                model: String::new(),
                effort_used: None,
                usage: crate::TokenUsage::default(),
                cost_micros: 0,
                effort_requested: None,
                effort_clamped: false,
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
            } => Self::InferenceFailed {
                seq,
                request_id,
                error,
            },
            BinaryEvent::MeteredInferenceCompleted {
                seq,
                request_id,
                message,
                provider,
                model,
                effort_used,
                usage,
                cost_micros,
                effort_requested,
                effort_clamped,
            } => Self::InferenceCompleted {
                seq,
                request_id,
                message,
                provider,
                model,
                effort_used,
                usage,
                cost_micros,
                effort_requested,
                effort_clamped,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn old_completions_decode_alone_and_inside_a_sequence() {
        let old = BinaryEvent::InferenceCompleted {
            seq: 7,
            request_id: crate::RequestId::for_step(
                crate::SessionId::from_ulid(ulid::Ulid::from_parts(1, 2)),
                6,
            ),
            message: crate::message::tests::message(),
        };
        let bytes = crate::encode(&old).unwrap();
        let event: Event = crate::decode(&bytes).unwrap();
        assert!(
            matches!(&event, Event::InferenceCompleted { usage, cost_micros: 0, provider, .. } if usage == &crate::TokenUsage::default() && provider.is_empty())
        );
        let bytes = crate::encode(&vec![
            old,
            BinaryEvent::StateChanged {
                seq: 8,
                from: SessionState::WaitingInference,
                to: SessionState::Idle,
            },
        ])
        .unwrap();
        let events: Vec<Event> = crate::decode(&bytes).unwrap();
        assert_eq!(events[0], event);
        assert_eq!(events[1].seq(), 8);
        let json = serde_json::json!({"inference_completed": {"seq": 7, "request_id": match &event { Event::InferenceCompleted {request_id, ..} => request_id, _ => unreachable!() }, "message": crate::message::tests::message()}});
        assert_eq!(serde_json::from_value::<Event>(json).unwrap(), event);
    }
}
