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
                // New user messages must not interrupt external work already in flight.
                if !matches!(
                    self.phase,
                    Phase::WaitingInference { .. } | Phase::Tools { .. }
                ) || message.role == MessageRole::Tool
                {
                    self.phase = match message.role {
                        MessageRole::User | MessageRole::Tool => Phase::Ready,
                        MessageRole::Assistant => Phase::EndTurn,
                        MessageRole::System => self.phase.clone(),
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
            Event::InferenceFailed { request_id, .. } => {
                // Retries were exhausted by the gateway. End the turn rather than
                // requesting inference again, so a broken provider cannot loop.
                if matches!(self.phase, Phase::WaitingInference { request_id: pending } if pending == *request_id)
                {
                    self.phase = Phase::EndTurn;
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
