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
        no_cache: false,
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

// Prompts copied verbatim from Pi: packages/coding-agent/src/core/compaction/
// utils.ts:156 and compaction.ts:529-601, 964-978; messages.ts:11-16.
pub const SUMMARIZATION_SYSTEM_PROMPT: &str = "You are a context summarization assistant. Your task is to read a conversation between a user and an AI assistant, then produce a structured summary following the exact format specified.\n\nDo NOT continue the conversation. Do NOT respond to any questions in the conversation. ONLY output the structured summary.";

pub const SUMMARIZATION_PROMPT: &str = "The messages above are a conversation to summarize. Create a structured context checkpoint summary that another LLM will use to continue the work.\n\nUse this EXACT format:\n\n## Goal\n[What is the user trying to accomplish? Can be multiple items if the session covers different tasks.]\n\n## Constraints & Preferences\n- [Any constraints, preferences, or requirements mentioned by user]\n- [Or \"(none)\" if none were mentioned]\n\n## Progress\n### Done\n- [x] [Completed tasks/changes]\n\n### In Progress\n- [ ] [Current work]\n\n### Blocked\n- [Issues preventing progress, if any]\n\n## Key Decisions\n- **[Decision]**: [Brief rationale]\n\n## Next Steps\n1. [Ordered list of what should happen next]\n\n## Critical Context\n- [Any data, examples, or references needed to continue]\n- [Or \"(none)\" if not applicable]\n\nKeep each section concise. Preserve exact file paths, function names, and error messages.";

pub const UPDATE_SUMMARIZATION_PROMPT: &str = "The messages above are NEW conversation messages to incorporate into the existing summary provided in <previous-summary> tags.\n\nUpdate the existing structured summary with new information. RULES:\n- PRESERVE all existing information from the previous summary\n- ADD new progress, decisions, and context from the new messages\n- UPDATE the Progress section: move items from \"In Progress\" to \"Done\" when completed\n- UPDATE \"Next Steps\" based on what was accomplished\n- PRESERVE exact file paths, function names, and error messages\n- If something is no longer relevant, you may remove it\n\nUse this EXACT format:\n\n## Goal\n[Preserve existing goals, add new ones if the task expanded]\n\n## Constraints & Preferences\n- [Preserve existing, add new ones discovered]\n\n## Progress\n### Done\n- [x] [Include previously done items AND newly completed items]\n\n### In Progress\n- [ ] [Current work - update based on progress]\n\n### Blocked\n- [Current blockers - remove if resolved]\n\n## Key Decisions\n- **[Decision]**: [Brief rationale] (preserve all previous, add new)\n\n## Next Steps\n1. [Update based on current state]\n\n## Critical Context\n- [Preserve important context, add new if needed]\n\nKeep each section concise. Preserve exact file paths, function names, and error messages.";

pub const TURN_PREFIX_SUMMARIZATION_PROMPT: &str = "The messages above are earlier context from an ongoing conversation. Later messages are stored separately and do not need to be reconstructed.\n\nCreate a concise checkpoint of the user's request and the progress shown above. This checkpoint will be placed before the later messages so the conversation can continue with the necessary context.\n\n## Original Request\n[What did the user ask for?]\n\n## Progress So Far\n- [Key decisions and work completed in these messages]\n\n## Context Needed to Continue\n- [Information from these messages needed to understand the later work]\n\nOnly summarize information explicitly present above. Do not infer or recreate later messages.";

pub const COMPACTION_SUMMARY_PREFIX: &str = "The conversation history before this point was compacted into the following summary:\n\n<summary>\n";

pub const COMPACTION_SUMMARY_SUFFIX: &str = "\n</summary>";
