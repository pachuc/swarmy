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
        #[serde(default)]
        entry: Option<String>,
        #[serde(default)]
        route: Option<String>,
        #[serde(default)]
        route_step: Option<u32>,
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
    /// The gateway records an exhausted provider attempt for the next step.
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
    RetryableInferenceFailed {
        seq: u64,
        request_id: RequestId,
        error: String,
        retryable: bool,
        retry_at: Option<jiff::Timestamp>,
    },
    /// A completion that names its auth entry and route step. Appended after
    /// the retryable failure variant so every earlier discriminant is frozen.
    /// Completions without route attribution keep the metered shape, so old
    /// readers still decode the turns they wrote.
    RoutedInferenceCompleted {
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
        #[serde(default)]
        entry: Option<String>,
        #[serde(default)]
        route: Option<String>,
        #[serde(default)]
        route_step: Option<u32>,
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
/// Encode a completion with the metered shape when it carries no route
/// attribution, so old readers decode the turns they wrote; attributed
/// completions use the appended routed shape.
#[allow(clippy::too_many_arguments)]
fn completion_to_binary(
    seq: u64,
    request_id: RequestId,
    message: Message,
    provider: String,
    model: String,
    effort_used: Option<crate::ReasoningEffort>,
    usage: crate::TokenUsage,
    cost_micros: u64,
    effort_requested: Option<crate::ReasoningEffort>,
    effort_clamped: bool,
    entry: Option<String>,
    route: Option<String>,
    route_step: Option<u32>,
) -> BinaryEvent {
    if entry.is_none() && route.is_none() && route_step.is_none() {
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
        }
    } else {
        BinaryEvent::RoutedInferenceCompleted {
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
            entry,
            route,
            route_step,
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
) -> Event {
    Event::InferenceFailed {
        seq,
        request_id,
        error,
        retryable,
        retry_at,
    }
}

/// Decode a completion from before metering existed.
fn unmetered_completion(seq: u64, request_id: RequestId, message: Message) -> Event {
    Event::InferenceCompleted {
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
        entry: None,
        route: None,
        route_step: None,
    }
}

/// Decode a routed completion with its entry and route step.
#[allow(clippy::too_many_arguments)]
fn routed_completion(
    seq: u64,
    request_id: RequestId,
    message: Message,
    provider: String,
    model: String,
    effort_used: Option<crate::ReasoningEffort>,
    usage: crate::TokenUsage,
    cost_micros: u64,
    effort_requested: Option<crate::ReasoningEffort>,
    effort_clamped: bool,
    entry: Option<String>,
    route: Option<String>,
    route_step: Option<u32>,
) -> Event {
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
        entry,
        route,
        route_step,
    }
}

/// Decode a metered completion without route attribution.
#[allow(clippy::too_many_arguments)]
fn metered_completion(
    seq: u64,
    request_id: RequestId,
    message: Message,
    provider: String,
    model: String,
    effort_used: Option<crate::ReasoningEffort>,
    usage: crate::TokenUsage,
    cost_micros: u64,
    effort_requested: Option<crate::ReasoningEffort>,
    effort_clamped: bool,
) -> Event {
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
        entry: None,
        route: None,
        route_step: None,
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
                entry,
                route,
                route_step,
            } => completion_to_binary(
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
                entry,
                route,
                route_step,
            ),
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
            } => Self::InferenceFailed {
                seq,
                request_id,
                error,
            },
            Event::InferenceFailed {
                seq,
                request_id,
                error,
                retryable,
                retry_at,
            } => Self::RetryableInferenceFailed {
                seq,
                request_id,
                error,
                retryable,
                retry_at,
            },
        }
    }
}
impl From<BinaryEvent> for Event {
    // The binary layout contract lives in this one exhaustive table: one arm
    // per frozen discriminant. Splitting arms further would scatter the
    // mapping the discriminant test freezes, so the length lint is allowed here.
    #[allow(clippy::too_many_lines)]
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
            } => unmetered_completion(seq, request_id, message),
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
            } => failed_completion(seq, request_id, error, false, None),
            BinaryEvent::RetryableInferenceFailed {
                seq,
                request_id,
                error,
                retryable,
                retry_at,
            } => failed_completion(seq, request_id, error, retryable, retry_at),
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
            } => metered_completion(
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
            ),
            BinaryEvent::RoutedInferenceCompleted {
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
                entry,
                route,
                route_step,
            } => routed_completion(
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
                entry,
                route,
                route_step,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routed_completions_keep_metered_bytes_when_unattributed() {
        let request_id = crate::RequestId::for_step(
            crate::SessionId::from_ulid(ulid::Ulid::from_parts(3, 4)),
            2,
        );
        let plain = Event::InferenceCompleted {
            seq: 5,
            request_id,
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
        };
        // Unattributed completions keep the metered discriminant so old
        // readers decode the turns they wrote.
        assert_eq!(usize::from(crate::encode(&plain).unwrap()[1]), 8);
        assert_eq!(
            crate::decode::<Event>(&crate::encode(&plain).unwrap()).unwrap(),
            plain
        );
        let Event::InferenceCompleted {
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
            ..
        } = plain.clone()
        else {
            unreachable!("plain completion");
        };
        let routed = Event::InferenceCompleted {
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
            entry: Some("backup".into()),
            route: Some("fallback".into()),
            route_step: Some(1),
        };
        let bytes = crate::encode(&routed).unwrap();
        assert_eq!(usize::from(bytes[1]), 10);
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
