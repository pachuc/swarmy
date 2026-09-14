//! Pure conversation replay and step selection. Workers persist actions and run tools.

mod snapshot;
mod tools;

pub use snapshot::Snapshot;
pub use tools::{GetTime, Tool, ToolRegistry, execution_result, result_part};

use swarmy_core::{
    Event, Message, MessageId, MessageRole, SessionRecord, SessionState, ToolCallRecord,
};
use swarmy_llm::{GenerationSettings, Request};

use snapshot::Phase;

#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    BuildInference(Request),
    DispatchTools(Vec<ToolCallRecord>),
    FoldResults(Message),
    Wait,
    EndTurn,
}

/// No interpolation or whitespace normalization is applied to the system template.
/// Callers supply any substitutions explicitly before assembling the prompt.
#[must_use]
pub fn assemble_prompt(
    system_prompt_template: &str,
    messages: &[Message],
    settings: &GenerationSettings,
    tools: &ToolRegistry,
) -> Request {
    Request {
        system_prompt: system_prompt_template.to_owned(),
        messages: messages.to_vec(),
        tools: tools.definitions(),
        settings: settings.clone(),
    }
}

pub struct Harness {
    pub system_prompt_template: String,
    pub settings: GenerationSettings,
    pub tools: ToolRegistry,
}

impl Harness {
    /// Selects work from a decoded snapshot and its ordered event tail.
    ///
    /// `SessionRecord` contains only a snapshot reference; the caller loads the
    /// corresponding `Snapshot` before calling. Use an empty snapshot for a new log.
    /// Events must belong to this session and follow the snapshot in sequence order.
    /// The caller supplies a stable message id for retries of the same fold step.
    ///
    /// Workers must append all dispatched `ToolCallRequested` events atomically.
    /// Persist `FoldResults` as `MessageAppended` before asking for the next step.
    /// Lease ownership and eligibility to execute the action are checked by workers.
    #[must_use]
    pub fn step(
        &self,
        session: &SessionRecord,
        snapshot: &Snapshot,
        events: &[Event],
        result_message_id: MessageId,
    ) -> Action {
        if session.state == SessionState::Completed {
            return Action::EndTurn;
        }
        let replayed = snapshot.replay(events);
        match &replayed.phase {
            Phase::Wait | Phase::WaitingInference { .. } => Action::Wait,
            Phase::Ready => Action::BuildInference(assemble_prompt(
                &self.system_prompt_template,
                replayed.messages(),
                &self.settings,
                &self.tools,
            )),
            Phase::EndTurn => Action::EndTurn,
            Phase::Tools {
                expected,
                requested,
                ..
            } => {
                if requested.is_empty() {
                    return Action::DispatchTools(expected.clone());
                }
                if requested.len() != expected.len()
                    || requested
                        .iter()
                        .any(|pending| pending.call.result.is_none())
                {
                    return Action::Wait;
                }
                Action::FoldResults(Message {
                    id: result_message_id,
                    role: MessageRole::Tool,
                    parts: requested
                        .iter()
                        .filter_map(|pending| {
                            pending
                                .call
                                .result
                                .clone()
                                .map(|result| result_part(pending.call.call_id.clone(), result))
                        })
                        .collect(),
                })
            }
        }
    }
}
