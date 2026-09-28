use serde::{Deserialize, Serialize};
use swarmy_core::{Event, Message, MessageRole, Part, RequestId, ToolCallRecord};

/// Harness state stored behind `SessionRecord::snapshot_ref`.
/// Pending work is retained so a snapshot can split a parallel tool batch.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    messages: Vec<Message>,
    pub(crate) phase: Phase,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Phase {
    #[default]
    Wait,
    Ready,
    WaitingInference {
        request_id: RequestId,
    },
    Tools {
        expected: Vec<ToolCallRecord>,
        requested: Vec<PendingCall>,
    },
    EndTurn,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PendingCall {
    pub request_id: RequestId,
    pub call: ToolCallRecord,
}

impl Snapshot {
    #[must_use]
    pub fn messages(&self) -> &[Message] {
        &self.messages
    }

    /// Discard an unusable checkpoint reply while preserving the durable log.
    #[must_use]
    pub fn without_message(mut self, id: swarmy_core::MessageId) -> Self {
        let removed_tool_call = self.messages.iter().any(|message| {
            message.id == id
                && message
                    .parts
                    .iter()
                    .any(|part| matches!(part, Part::ToolCall { .. }))
        });
        self.messages.retain(|message| message.id != id);
        if removed_tool_call {
            self.phase = self
                .messages
                .last()
                .map_or(Phase::Wait, |message| match message.role {
                    MessageRole::Assistant => model_phase(message),
                    MessageRole::User | MessageRole::Tool | MessageRole::System => Phase::Ready,
                });
        }
        self
    }

    /// A failed recovery ends the turn even when a system notice was appended.
    #[must_use]
    pub fn end_turn(mut self) -> Self {
        self.phase = Phase::EndTurn;
        self
    }

    /// Replays an ordered tail without changing the input snapshot or events.
    /// State and snapshot bookkeeping events do not change conversation decisions.
    #[must_use]
    pub fn replay(&self, events: &[Event]) -> Self {
        let mut snapshot = self.clone();
        for event in events {
            snapshot.apply(event);
        }
        snapshot
    }

    fn apply(&mut self, event: &Event) {
        match event {
            Event::MessageAppended { message, .. } => {
                self.messages.push(message.clone());
                // Incoming messages, including timer notes, must not interrupt external work.
                if !matches!(
                    self.phase,
                    Phase::WaitingInference { .. } | Phase::Tools { .. }
                ) || message.role == MessageRole::Tool
                {
                    self.phase = match message.role {
                        // Between turns, a system note delivered by a timer starts inference.
                        MessageRole::User | MessageRole::Tool | MessageRole::System => Phase::Ready,
                        MessageRole::Assistant => Phase::EndTurn,
                    };
                }
            }
            Event::InferenceRequested { request_id, .. } => {
                self.phase = Phase::WaitingInference {
                    request_id: *request_id,
                };
            }
            Event::InferenceCompleted {
                request_id,
                message,
                ..
            } => {
                if matches!(self.phase, Phase::WaitingInference { request_id: pending } if pending != *request_id)
                {
                    return;
                }
                self.messages.push(message.clone());
                self.phase = model_phase(message);
            }
            Event::ToolCallRequested {
                request_id, call, ..
            } => {
                if let Phase::Tools {
                    expected,
                    requested,
                } = &mut self.phase
                    && expected.iter().any(|item| item.call_id == call.call_id)
                    && !requested
                        .iter()
                        .any(|item| item.call.call_id == call.call_id)
                {
                    let mut call = call.clone();
                    // Only completion events are authoritative for execution results.
                    call.result = None;
                    requested.push(PendingCall {
                        request_id: *request_id,
                        call,
                    });
                }
            }
            Event::ToolCallCompleted {
                request_id,
                call_id,
                result,
                ..
            } => {
                if let Phase::Tools { requested, .. } = &mut self.phase
                    && let Some(pending) = requested.iter_mut().find(|pending| {
                        pending.call.call_id == *call_id && pending.request_id == *request_id
                    })
                {
                    pending.call.result = Some(result.clone());
                }
            }
            Event::InferenceFailed {
                request_id,
                retryable,
                ..
            } => {
                // A retryable failure leaves the prompt ready for a later attempt.
                if matches!(self.phase, Phase::WaitingInference { request_id: pending } if pending == *request_id)
                {
                    self.phase = if *retryable {
                        Phase::Ready
                    } else {
                        Phase::EndTurn
                    };
                }
            }
            Event::StateChanged { .. } | Event::SnapshotWritten { .. } => {}
        }
    }
}

fn model_phase(message: &Message) -> Phase {
    let calls: Vec<_> = message
        .parts
        .iter()
        .filter_map(|part| match part {
            Part::ToolCall {
                call_id,
                tool,
                input,
            } => Some(ToolCallRecord {
                call_id: call_id.clone(),
                tool: tool.clone(),
                arguments: input.clone(),
                result: None,
            }),
            _ => None,
        })
        .collect();
    if calls.is_empty() {
        Phase::EndTurn
    } else {
        Phase::Tools {
            expected: calls,
            requested: Vec::new(),
        }
    }
}

#[cfg(test)]
mod checkpoint_tests {
    use super::*;
    use swarmy_core::MessageId;
    use ulid::Ulid;

    #[test]
    fn removing_tool_call_recomputes_phase() {
        let user = Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role: MessageRole::User,
            parts: vec![Part::Text { text: "run".into() }],
        };
        let call = Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role: MessageRole::Assistant,
            parts: vec![Part::ToolCall {
                call_id: swarmy_core::ToolCallId("one".into()),
                tool: "get_time".into(),
                input: serde_json::json!({}),
            }],
        };
        let snapshot = Snapshot {
            messages: vec![user.clone(), call.clone()],
            phase: model_phase(&call),
        };
        let retained = snapshot.without_message(call.id);
        assert_eq!(retained.messages(), &[user]);
        assert!(matches!(retained.phase, Phase::Ready));
    }

    #[test]
    fn rejected_checkpoint_reply_is_not_replayed_from_next_snapshot() {
        let original = Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role: MessageRole::Assistant,
            parts: vec![Part::Text {
                text: "answer".into(),
            }],
        };
        let rejected = Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role: MessageRole::Assistant,
            parts: vec![Part::Text {
                text: "partial checkpoint".into(),
            }],
        };
        let snapshot = Snapshot {
            messages: vec![original.clone(), rejected.clone()],
            phase: Phase::EndTurn,
        };
        let retained = snapshot.without_message(rejected.id);
        assert_eq!(retained.messages(), std::slice::from_ref(&original));
        assert_eq!(retained.replay(&[]).messages(), &[original]);
    }
}
