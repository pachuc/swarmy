use std::collections::{BTreeMap, HashSet};

use ratatui::{
    layout::{Constraint, Layout, Rect},
    style::{Color, Style},
    text::Line,
    widgets::{Paragraph, Wrap},
};
use swarmy_core::{MessageId, RequestId, SessionId, SessionState, ToolCallId, ToolResult};

use crate::conversation::{TranscriptEvent, message_text};

#[derive(Default)]
pub struct Transcript {
    entries: Vec<Entry>,
    notice_session: Option<SessionId>,
    partial: BTreeMap<usize, String>,
    messages: HashSet<MessageId>,
    pub state: Option<SessionState>,
    pub ready: bool,
}

enum Entry {
    User(String),
    System(String),
    Assistant(String),
    Tool {
        request: RequestId,
        call: ToolCallId,
        name: String,
        arguments: String,
        result: Option<ToolResult>,
    },
    Error(String),
}

impl Transcript {
    pub fn new(notice_session: Option<SessionId>) -> Self {
        Self {
            notice_session,
            ..Self::default()
        }
    }

    pub fn apply(&mut self, event: TranscriptEvent) {
        match event {
            TranscriptEvent::UserMessage(message) => {
                if self.messages.insert(message.id) {
                    self.entries.push(Entry::User(message_text(&message)));
                }
            }
            TranscriptEvent::SystemMessage(message) => {
                if self.messages.insert(message.id) {
                    self.entries.push(Entry::System(message_text(&message)));
                }
            }
            TranscriptEvent::AssistantTextDelta { index, text } => {
                self.partial.entry(index).or_default().push_str(&text);
            }
            TranscriptEvent::AssistantPart { index, text } => { self.partial.insert(index, text); }
            TranscriptEvent::AssistantMessageFinal(message) => {
                if self.messages.insert(message.id) {
                    self.partial.clear();
                    let text = message_text(&message);
                    if !text.is_empty() { self.entries.push(Entry::Assistant(text)); }
                }
            }
            TranscriptEvent::ToolRequested { request, call } => {
                if !self.entries.iter().any(|entry| matches!(entry, Entry::Tool { request: id, call: call_id, .. } if *id == request && *call_id == call.call_id)) {
                    self.entries.push(Entry::Tool {
                        request, call: call.call_id, name: call.tool,
                        arguments: call.arguments.to_string(), result: call.result,
                    });
                }
            }
            TranscriptEvent::ToolCompleted { request, call, result } => {
                if let Some(Entry::Tool { result: stored, .. }) = self.entries.iter_mut().find(|entry| matches!(entry, Entry::Tool { request: id, call: call_id, .. } if *id == request && *call_id == call)) {
                    *stored = Some(result);
                }
            }
            TranscriptEvent::State(state) => {
                self.state = Some(state);
                self.ready = false;
            }
            TranscriptEvent::SessionIdle => {
                self.partial.clear();
                self.state = Some(SessionState::Idle);
                self.ready = true;
            }
            TranscriptEvent::Error(error) => self.entries.push(Entry::Error(error)),
        }
    }

    pub fn lines(&self) -> Vec<Line<'static>> {
        let mut lines = Vec::new();
        for entry in &self.entries {
            match entry {
                Entry::System(text) => {
                    let label = self
                        .notice_session
                        .map_or_else(|| "System".into(), |id| format!("System [session {id}]"));
                    append_lines(&mut lines, &label, text, Color::Yellow);
                }
                Entry::User(text) => append_lines(&mut lines, "You", text, Color::Cyan),
                Entry::Assistant(text) => append_lines(&mut lines, "Agent", text, Color::Reset),
                Entry::Error(text) => append_lines(&mut lines, "Error", text, Color::Reset),
                Entry::Tool {
                    call,
                    name,
                    arguments,
                    result,
                    ..
                } => {
                    let status = if result.is_some() { "done" } else { "running" };
                    append_lines(
                        &mut lines,
                        "Tool",
                        &format!("{name} [{}] {arguments} ({status})", call.0),
                        Color::Yellow,
                    );
                    if let Some(result) = result {
                        let text = match result {
                            ToolResult::Completed { output, .. } => output.clone(),
                            ToolResult::Error { error } => format!("Error: {error}"),
                        };
                        append_lines(&mut lines, "  Result", &text, Color::Yellow);
                    }
                }
            }
        }
        if !self.partial.is_empty() {
            append_lines(
                &mut lines,
                "Agent",
                &self
                    .partial
                    .values()
                    .map(String::as_str)
                    .collect::<String>(),
                Color::Reset,
            );
        }
        lines
    }

    pub fn paragraph(&self) -> Paragraph<'static> {
        Paragraph::new(self.lines()).wrap(Wrap { trim: false })
    }

    pub fn status(&self, id: SessionId, provider: &str, agent: Option<&str>) -> Line<'static> {
        let state = self
            .state
            .map_or_else(|| "Loading".into(), |state| format!("{state:?}"));
        let input = if self.ready {
            "Enter: send"
        } else {
            "input locked"
        };
        let agent = clean(agent.unwrap_or("ephemeral"));
        Line::raw(format!(
            "{agent} | {id} | {state} | {provider} | {input} | Esc: quit"
        ))
    }
}

fn append_lines(lines: &mut Vec<Line<'static>>, label: &str, text: &str, color: Color) {
    let text = clean(text);
    for (index, line) in text.split('\n').enumerate() {
        let prefix = if index == 0 {
            format!("{label}: ")
        } else {
            "  ".into()
        };
        lines.push(Line::styled(
            format!("{prefix}{line}"),
            Style::default().fg(color),
        ));
    }
}

pub fn clean(text: &str) -> String {
    text.chars()
        .map(|c| if c.is_control() && c != '\n' { ' ' } else { c })
        .collect()
}

pub fn panes(area: Rect) -> [Rect; 3] {
    Layout::vertical([
        Constraint::Min(0),
        Constraint::Length(1),
        Constraint::Length(1),
    ])
    .areas(area)
}

/// A fixed offset keeps the same text visible as new replies arrive. None follows.
#[derive(Default)]
pub struct Scroll {
    offset: Option<usize>,
}

impl Scroll {
    pub fn position(&self, lines: usize, height: u16) -> u16 {
        let bottom = lines.saturating_sub(usize::from(height));
        u16::try_from(self.offset.unwrap_or(bottom).min(bottom)).unwrap_or(u16::MAX)
    }

    pub fn up(&mut self, lines: usize, height: u16) {
        self.offset = Some(
            usize::from(self.position(lines, height)).saturating_sub(usize::from(height.max(1))),
        );
    }

    pub fn down(&mut self, lines: usize, height: u16) {
        let next = usize::from(self.position(lines, height)) + usize::from(height.max(1));
        self.offset = (next < lines.saturating_sub(usize::from(height))).then_some(next);
    }

    pub fn end(&mut self) {
        self.offset = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversation::Replay;
    use ratatui::{Terminal, backend::TestBackend};
    use swarmy_core::Event;

    fn recording() -> Vec<Event> {
        include_str!("../../tests/fixtures/conversation.jsonl")
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn replay(transcript: &mut Transcript, replay: &mut Replay, events: &[Event]) {
        for event in events {
            if let Some(event) = replay.record(event) {
                transcript.apply(event);
            }
        }
    }

    fn text(transcript: &Transcript) -> String {
        transcript
            .lines()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn recorded_turns_replace_partial_text_and_update_tools_in_place() {
        let events = recording();
        let mut transcript = Transcript::default();
        let mut history = Replay::default();
        replay(&mut transcript, &mut history, &events[..2]);
        transcript.apply(TranscriptEvent::AssistantTextDelta {
            index: 0,
            text: "I will ".into(),
        });
        assert!(text(&transcript).ends_with("Agent: I will "));
        // A retried stream can repeat a prefix. The durable final replaces it.
        transcript.apply(TranscriptEvent::AssistantTextDelta {
            index: 0,
            text: "I will check.".into(),
        });
        replay(&mut transcript, &mut history, &events[2..5]);
        assert_eq!(
            text(&transcript),
            "You: What time is it?\nAgent: I will check.\nTool: get_time [clock] {} (running)"
        );
        replay(&mut transcript, &mut history, &events[5..]);
        transcript.apply(TranscriptEvent::SessionIdle);
        assert_eq!(
            text(&transcript),
            concat!(
                "You: What time is it?\nAgent: I will check.\n",
                "Tool: get_time [clock] {} (done)\n  Result: 2026-09-15T12:00:00Z\n",
                "Agent: It is noon UTC.\nYou: And now?\n",
                "Tool: get_time [clock] {} (done)\n  Result: Error: clock unavailable\n  try again\n",
                "Agent: The clock is unavailable."
            )
        );
        assert!(transcript.ready);
        let mut resumed = Transcript::default();
        replay(&mut resumed, &mut Replay::default(), &events);
        assert_eq!(text(&transcript), text(&resumed));
        replay(&mut resumed, &mut history, &events);
        assert_eq!(text(&transcript), text(&resumed));
    }

    #[test]
    fn part_replacement_preserves_output_order_and_idle_clears_late_deltas() {
        let mut transcript = Transcript::default();
        transcript.apply(TranscriptEvent::AssistantTextDelta {
            index: 1,
            text: "world".into(),
        });
        transcript.apply(TranscriptEvent::AssistantTextDelta {
            index: 0,
            text: "Hel".into(),
        });
        transcript.apply(TranscriptEvent::AssistantPart {
            index: 0,
            text: "Hello ".into(),
        });
        assert_eq!(text(&transcript), "Agent: Hello world");
        transcript.apply(TranscriptEvent::SessionIdle);
        assert_eq!(text(&transcript), "");
        assert!(transcript.ready);
        transcript.apply(TranscriptEvent::State(SessionState::WaitingInference));
        assert!(!transcript.ready);
        transcript.apply(TranscriptEvent::Error("provider exhausted retries".into()));
        assert_eq!(text(&transcript), "Error: provider exhausted retries");
    }

    #[test]
    fn recorded_layout_wraps_and_keeps_a_scrolled_view_still() {
        let mut transcript = Transcript::default();
        replay(&mut transcript, &mut Replay::default(), &recording());
        let mut terminal = Terminal::new(TestBackend::new(24, 6)).unwrap();
        let mut scroll = Scroll::default();
        let lines = transcript.paragraph().line_count(24);
        terminal
            .draw(|frame| {
                let [body, status, input] = panes(frame.area());
                assert_eq!((body.height, status.height, input.height), (4, 1, 1));
                frame.render_widget(
                    transcript
                        .paragraph()
                        .scroll((scroll.position(lines, body.height), 0)),
                    body,
                );
                frame.render_widget(Paragraph::new("> "), input);
            })
            .unwrap();
        let buffer = terminal.backend().buffer();
        let rows: Vec<String> = (0..6)
            .map(|y| (0..24).map(|x| buffer[(x, y)].symbol()).collect())
            .collect();
        assert_eq!(rows[2].trim_end(), "Agent: The clock is");
        assert_eq!(rows[3].trim_end(), "unavailable.");
        assert_eq!(rows[5].trim_end(), ">");
        scroll.up(lines, 4);
        let held = scroll.position(lines, 4);
        assert_eq!(scroll.position(lines + 10, 4), held);
        scroll.down(lines, 4);
        assert_eq!(usize::from(scroll.position(lines + 10, 4)), lines + 6);
        scroll.end();
        assert_eq!(scroll.position(1, 4), 0);
    }
}

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use crate::conversation::Replay;
    use swarmy_core::{Event, Message, MessageRole, Part};

    #[test]
    fn system_notice_survives_feed_replay_and_renders_once() {
        let message = Message {
            id: MessageId::from_ulid(ulid::Ulid::generate()),
            role: MessageRole::System,
            parts: vec![Part::Text {
                text: "Computer rebuilt from its snapshot.".into(),
            }],
        };
        let event = Event::MessageAppended { seq: 1, message };
        let id = SessionId::from_ulid(ulid::Ulid::generate());
        for agent in [None, Some("tommy")] {
            let mut replay = Replay::default();
            let mut transcript = Transcript::new(agent.map(|_| id));
            transcript.apply(replay.record(&event).unwrap());
            assert!(replay.record(&event).is_none());
            let text = transcript
                .lines()
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("\n");
            let label =
                agent.map_or_else(|| "System:".into(), |_| format!("System [session {id}]:"));
            assert!(text.contains(&label));
            assert_eq!(
                text.matches("Computer rebuilt from its snapshot.").count(),
                1
            );
            let status = transcript.status(id, "fake", agent).to_string();
            assert!(status.starts_with(agent.unwrap_or("ephemeral")));
            assert!(status.contains(&id.to_string()));
        }
    }
}
