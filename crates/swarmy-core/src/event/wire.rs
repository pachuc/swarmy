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
        /// Auth entry that served the turn, appended as trailing fields so
        /// readers from before routes still recognize the metered
        /// discriminant and decode the prefix they understand.
        #[serde(default, with = "crate::trailing")]
        entry: Option<String>,
        /// Named route that selected the entry, if any.
        #[serde(default, with = "crate::trailing")]
        route: Option<String>,
        /// Index into the resolved route, so metering names the exact step.
        #[serde(default, with = "crate::trailing")]
        route_step: Option<u32>,
    },
    RetryableInferenceFailed {
        seq: u64,
        request_id: RequestId,
        error: String,
        retryable: bool,
        retry_at: Option<jiff::Timestamp>,
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
/// Encode a completion on the metered shape with its route attribution as
/// trailing fields, so the discriminant never changes when attribution is
/// added. Readers from before routes decode the prefix they understand;
/// current readers default missing trailing fields exactly like old rows.
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
        entry,
        route,
        route_step,
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

/// Decode a metered completion, defaulting route attribution that older
/// rows never stored.
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
fn unmetered_attribution(
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
                entry,
                route,
                route_step,
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
    fn routed_completions_keep_the_metered_discriminant() {
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
        // Attributed and plain completions share the metered discriminant;
        // only the trailing bytes differ.
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
        assert_eq!(usize::from(bytes[1]), 8);
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

    /// The pre-routes binary schema for the metered variant: the same fields
    /// in the same order, without the trailing route attribution.
    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    enum PreRoutesBinaryEvent {
        MessageAppended {
            seq: u64,
            message: Message,
        },
        InferenceRequested {
            seq: u64,
            request_id: RequestId,
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
    }

    #[test]
    fn pre_routes_decoder_reads_attributed_completions() {
        let request_id = crate::RequestId::for_step(
            crate::SessionId::from_ulid(ulid::Ulid::from_parts(5, 6)),
            4,
        );
        let routed = Event::InferenceCompleted {
            seq: 9,
            request_id,
            message: crate::message::tests::message(),
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            effort_used: None,
            usage: crate::TokenUsage::default(),
            cost_micros: 11,
            effort_requested: None,
            effort_clamped: false,
            entry: Some("backup".into()),
            route: Some("fallback".into()),
            route_step: Some(1),
        };
        let bytes = crate::encode(&routed).unwrap();
        // The discriminant is unchanged, so the pre-routes schema recognizes
        // the variant and decodes the prefix it understands, leaving only
        // the trailing route fields unread.
        let payload = &bytes[1..];
        let (old, _remainder): (PreRoutesBinaryEvent, _) =
            postcard::take_from_bytes(payload).unwrap();
        assert_eq!(
            old,
            PreRoutesBinaryEvent::MeteredInferenceCompleted {
                seq: 9,
                request_id,
                message: crate::message::tests::message(),
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                effort_used: None,
                usage: crate::TokenUsage::default(),
                cost_micros: 11,
                effort_requested: None,
                effort_clamped: false,
            }
        );
        // Rows written before routes decode with defaulted attribution.
        let old_bytes = postcard::to_extend(&old, vec![crate::STORAGE_VERSION]).unwrap();
        assert_eq!(
            crate::decode::<Event>(&old_bytes).unwrap(),
            Event::InferenceCompleted {
                seq: 9,
                request_id,
                message: crate::message::tests::message(),
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                effort_used: None,
                usage: crate::TokenUsage::default(),
                cost_micros: 11,
                effort_requested: None,
                effort_clamped: false,
                entry: None,
                route: None,
                route_step: None,
            }
        );
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
