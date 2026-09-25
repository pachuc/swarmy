//! Terminal renderer for the API conversation stream.

use crate::input::Input;
use crate::{client_conversation::Conversation, selection_command::SelectionArgs};
use anyhow::{Context, Result, ensure};
use crossterm::{
    event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use futures_util::StreamExt;
use ratatui::{
    DefaultTerminal,
    widgets::{List, ListItem, ListState, Paragraph, Wrap},
};
use std::{
    collections::{HashMap, HashSet},
    io::{self, IsTerminal},
};
use swarmy_api_types as api;
use swarmy_client::{Client, StreamItem};

struct RestoreTerminal;
impl Drop for RestoreTerminal {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen, crossterm::cursor::Show);
    }
}

fn terminal() -> Result<(DefaultTerminal, RestoreTerminal)> {
    enable_raw_mode()?;
    let restore = RestoreTerminal;
    execute!(io::stdout(), EnterAlternateScreen)?;
    let mut terminal = DefaultTerminal::new(ratatui::backend::CrosstermBackend::new(io::stdout()))?;
    terminal.clear()?;
    Ok((terminal, restore))
}

fn quit(key: KeyEvent) -> bool {
    key.code == KeyCode::Esc
        || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
}

async fn recent(client: &Client) -> Result<Vec<(String, String)>> {
    let mut sessions = client.sessions(None, 100).await?;
    sessions.reverse();
    let mut result = Vec::new();
    for session in sessions.into_iter().take(20) {
        let events = client.events(&session.id, 0, 50).await?;
        let text = events
            .into_iter()
            .find_map(|event| {
                let api::EventPayload::StoreRecord { record } = event.payload else {
                    return None;
                };
                record
                    .get("message_appended")?
                    .get("message")?
                    .get("parts")?
                    .as_array()?
                    .iter()
                    .find_map(|p| p.get("text")?.get("text")?.as_str().map(str::to_owned))
            })
            .unwrap_or_default();
        result.push((session.id, text));
    }
    Ok(result)
}

async fn picker(
    client: &Client,
    terminal: &mut DefaultTerminal,
    keys: &mut EventStream,
) -> Result<Option<Option<String>>> {
    let sessions = recent(client).await?;
    let mut selection = ListState::default().with_selected(Some(0));
    loop {
        terminal.draw(|frame| {
            let items = std::iter::once(ListItem::new("New session")).chain(
                sessions
                    .iter()
                    .map(|(id, text)| ListItem::new(format!("{id}  {}", text.replace('\n', " ")))),
            );
            frame.render_stateful_widget(
                List::new(items).highlight_symbol("> "),
                frame.area(),
                &mut selection,
            );
        })?;
        if let Event::Key(key) = keys.next().await.context("terminal input closed")??
            && key.kind != KeyEventKind::Release
        {
            if quit(key) {
                return Ok(None);
            }
            let index = selection.selected().unwrap_or(0);
            match key.code {
                KeyCode::Up => selection.select(Some(index.saturating_sub(1))),
                KeyCode::Down => selection.select(Some((index + 1).min(sessions.len()))),
                KeyCode::Enter => {
                    return Ok(Some(index.checked_sub(1).map(|i| sessions[i].0.clone())));
                }
                _ => {}
            }
        }
    }
}

pub async fn run(
    client: Client,
    id: Option<ulid::Ulid>,
    image: Option<String>,
    agent: Option<String>,
    new: bool,
    selection: SelectionArgs,
) -> Result<()> {
    ensure!(
        io::stdin().is_terminal() && io::stdout().is_terminal(),
        "chat requires an interactive terminal"
    );
    let (mut terminal, _restore) = terminal()?;
    let mut keys = EventStream::new();
    let choice = if id.is_none()
        && agent.is_none()
        && selection.provider.is_none()
        && selection.model.is_none()
        && selection.effort.is_none()
    {
        let Some(choice) = picker(&client, &mut terminal, &mut keys).await? else {
            return Ok(());
        };
        choice
    } else {
        id.map(|value| value.to_string())
    };
    let mut conversation =
        Conversation::open(client.clone(), choice, image, agent, new, selection.into()).await?;
    // Health warnings belong on the ordinary terminal, not behind the alternate screen.
    disable_raw_mode()?;
    execute!(io::stdout(), LeaveAlternateScreen)?;
    conversation
        .wait_healthy(conversation.provider.as_deref())
        .await?;
    enable_raw_mode()?;
    execute!(io::stdout(), EnterAlternateScreen)?;
    terminal.clear()?;
    let mut view = View::new(&conversation);
    // History is read after subscribing, so a concurrent append cannot be lost.
    let mut after = 0;
    while after < conversation.session.head_sequence {
        let events = client.events(&conversation.id, after, 100).await?;
        if events.is_empty() {
            break;
        }
        for event in events {
            after = event.sequence;
            view.event(StreamItem::Event(event));
        }
    }
    view.ready = conversation.session.state == api::SessionState::Idle;
    let mut input = Input::default();
    loop {
        terminal.draw(|frame| {
            let area = frame.area();
            let rows = ratatui::layout::Layout::vertical([
                ratatui::layout::Constraint::Min(0),
                ratatui::layout::Constraint::Length(1),
                ratatui::layout::Constraint::Length(1),
            ])
            .split(area);
            let body = view.body();
            let paragraph = Paragraph::new(body).wrap(Wrap { trim: false });
            let offset = paragraph
                .line_count(rows[0].width)
                .saturating_sub(usize::from(rows[0].height));
            frame.render_widget(
                paragraph.scroll((u16::try_from(offset).unwrap_or(u16::MAX), 0)),
                rows[0],
            );
            frame.render_widget(Paragraph::new(view.status(&conversation)), rows[1]);
            let (line, cursor) = input.view(rows[2].width);
            frame.render_widget(Paragraph::new(line), rows[2]);
            if rows[2].width > 0 {
                frame.set_cursor_position((rows[2].x + cursor, rows[2].y));
            }
        })?;
        tokio::select! {
            event = conversation.next() => {
                view.event(event?);
                flush_queued(&mut view, &mut conversation).await?;
            }
            key = keys.next() => {
                if let Event::Key(key) = key.context("terminal input closed")??
                    && key.kind != KeyEventKind::Release {
                    if quit(key) {
                        if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) { conversation.interrupt().await?; }
                        return Ok(());
                    }
                    if let Some(text) = handle_key(&mut view, &mut input, key) {
                        send_or_queue(&mut view, &mut conversation, text).await?;
                    }
                }
            }
        }
    }
}

/// Handle one key press against the input line. Typing, backspace, and cursor
/// movement always edit the line so keystrokes are never lost while the
/// session is busy. Enter with a non-empty line while idle returns the text
/// for an immediate send; while busy it moves the line into the view queue so
/// the next idle state sends it. Returns the text to send immediately, if any.
fn handle_key(view: &mut View, input: &mut Input, key: KeyEvent) -> Option<String> {
    match key.code {
        KeyCode::Enter if !input.text.trim().is_empty() => {
            if view.ready && view.queued.is_none() {
                Some(input.take())
            } else {
                view.queue_current(input);
                None
            }
        }
        KeyCode::Char(c)
            if !key
                .modifiers
                .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
        {
            input.insert(c);
            None
        }
        KeyCode::Backspace => {
            input.backspace();
            None
        }
        KeyCode::Left => {
            input.left();
            None
        }
        KeyCode::Right => {
            input.right();
            None
        }
        _ => None,
    }
}

/// Send a line now, or keep it queued when the send loses the busy-session
/// race. The readiness render and the store check are separate steps, and the
/// store can flip between them (scheduler or sweep touches, head races);
/// neither case may drop keystrokes. Only the busy race requeues: a permanent
/// failure (rejected token, deleted session, API down) still returns the
/// error so the client exits with the message instead of waiting forever on
/// `input locked (queued)`. A queued line sends exactly once on the next idle
/// state.
async fn send_or_queue(
    view: &mut View,
    conversation: &mut Conversation,
    text: String,
) -> Result<()> {
    match conversation.send(text.clone()).await {
        Ok(turn) => {
            view.sent(&text, turn);
            Ok(())
        }
        Err(error) if is_busy_send_error(&error) => {
            view.queued = Some(text);
            view.ready = false;
            Ok(())
        }
        Err(error) => Err(error),
    }
}

/// Whether a send failure is the transient busy-session race that may be
/// retried on the next idle state. The API reports it as a 409 conflict with
/// code `session_not_idle` (or `stale_head` when the head moved between the
/// ready render and the append); the local idle guard reports it as
/// `session is not idle`. `api_client::call` wraps the client error in a
/// string, so match the wrapped text as well as the typed error.
fn is_busy_send_error(error: &anyhow::Error) -> bool {
    if let Some(client) = error.downcast_ref::<swarmy_client::Error>() {
        return is_busy_client_error(client);
    }
    let text = format!("{error:#}");
    text.contains("session_not_idle")
        || text.contains("stale_head")
        || text.contains("session is not idle")
}

fn is_busy_client_error(error: &swarmy_client::Error) -> bool {
    match error {
        swarmy_client::Error::Api { status, body } => {
            status.as_u16() == 409 && (body.code == "session_not_idle" || body.code == "stale_head")
        }
        swarmy_client::Error::Status { status, body } => {
            status.as_u16() == 409
                && (body.contains("session_not_idle") || body.contains("stale_head"))
        }
        _ => false,
    }
}

/// Send the queued line now that the session is idle again. A send that still
/// loses the idle race keeps the line queued instead of dropping it.
async fn flush_queued(view: &mut View, conversation: &mut Conversation) -> Result<()> {
    let Some(text) = view.take_queued_if_ready() else {
        return Ok(());
    };
    send_or_queue(view, conversation, text).await
}

struct View {
    entries: Vec<String>,
    partial: String,
    users: HashSet<String>,
    assistants: HashSet<String>,
    systems: HashSet<String>,
    tools: HashMap<String, usize>,
    ready: bool,
    queued: Option<String>,
    state: String,
    selection: String,
}
impl View {
    fn new(conversation: &Conversation) -> Self {
        let settings = swarmy_config::Settings::load().ok().map(|v| v.settings);
        let provider = conversation
            .provider
            .as_deref()
            .or_else(|| settings.as_ref().map(|v| v.provider.as_str()))
            .unwrap_or("default");
        let model = conversation
            .session
            .model
            .as_deref()
            .or_else(|| settings.as_ref().map(|v| v.model.as_str()))
            .unwrap_or("default");
        let effort = conversation.session.effort.as_ref().map_or_else(
            || {
                settings
                    .as_ref()
                    .map_or_else(|| "default".into(), |v| v.reasoning_effort.clone())
            },
            |v| format!("{v:?}").to_lowercase(),
        );
        Self {
            entries: Vec::new(),
            partial: String::new(),
            users: HashSet::new(),
            assistants: HashSet::new(),
            systems: HashSet::new(),
            tools: HashMap::new(),
            ready: false,
            queued: None,
            state: format!("{:?}", conversation.session.state),
            selection: format!("{provider}/{model} {effort}"),
        }
    }
    /// Record a turn that was just sent, locally echoing the user line and
    /// locking input until the next idle state record arrives.
    fn sent(&mut self, text: &str, turn: String) {
        self.users.insert(turn);
        self.entries.push(format!("You: {text}"));
        self.ready = false;
        self.state = "Runnable".into();
    }
    /// Move the current line into the queued slot so the next idle state
    /// sends it. A line that is already waiting keeps its place; a second
    /// Enter leaves the new typing intact for the turn after.
    fn queue_current(&mut self, input: &mut Input) {
        if self.queued.is_none() && !input.text.trim().is_empty() {
            self.queued = Some(input.take());
        }
    }
    /// Take the queued line once the session is idle again. Returns `None`
    /// while busy or when nothing is waiting, so a queued message sends
    /// exactly once.
    fn take_queued_if_ready(&mut self) -> Option<String> {
        if self.ready { self.queued.take() } else { None }
    }
    fn body(&self) -> String {
        let mut lines = self.entries.join("\n");
        if !self.partial.is_empty() {
            lines.push_str("\nAgent: ");
            lines.push_str(&self.partial);
        }
        lines
    }
    fn status(&self, conversation: &Conversation) -> String {
        self.status_text(
            conversation.agent_name.as_deref().unwrap_or("ephemeral"),
            &conversation.id,
        )
    }
    fn status_text(&self, agent: &str, id: &str) -> String {
        let base = if self.ready {
            "Enter: send"
        } else {
            "input locked"
        };
        let input = if self.queued.is_some() {
            format!("{base} (queued)")
        } else {
            base.into()
        };
        format!(
            "{agent} | {id} | {} | {} | {input} | Esc: quit",
            self.state, self.selection
        )
    }
    fn event(&mut self, item: StreamItem) {
        match item {
            StreamItem::TokenDelta {
                payload: api::EventPayload::TokenDelta { text, .. },
                ..
            } => self.partial.push_str(&text),
            StreamItem::Event(event) => {
                let api::EventPayload::StoreRecord { record } = event.payload else {
                    return;
                };
                if let Some(summary) = record.get("session_summarized") {
                    self.entries.push(format!(
                        "System: Conversation summarized. Session {} archived; continuing in {}.",
                        summary["previous_session_id"], summary["session_id"]
                    ));
                    return;
                }
                if let Some(state) = record
                    .get("state_changed")
                    .and_then(|s| s.get("to"))
                    .and_then(serde_json::Value::as_str)
                {
                    self.state = format!("{}{}", state[..1].to_uppercase(), &state[1..]);
                    self.ready = state == "idle";
                    if self.ready {
                        self.partial.clear();
                    }
                }
                if let Some(error) = record
                    .get("inference_failed")
                    .and_then(|v| v.get("error"))
                    .and_then(serde_json::Value::as_str)
                {
                    self.entries.push(format!("Error: {error}"));
                }
                if let Some(call) = record
                    .get("tool_call_requested")
                    .and_then(|v| v.get("call"))
                {
                    let id = call
                        .get("call_id")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown")
                        .to_owned();
                    if !self.tools.contains_key(&id) {
                        self.tools.insert(id.clone(), self.entries.len());
                        self.entries.push(format!(
                            "Tool: {} [{id}] {} (running)",
                            call.get("tool")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or("tool"),
                            call.get("arguments").unwrap_or(&serde_json::Value::Null)
                        ));
                    }
                }
                if let Some(completed) = record.get("tool_call_completed") {
                    if let Some(id) = completed.get("call_id").and_then(serde_json::Value::as_str)
                        && let Some(index) = self.tools.get(id).copied()
                    {
                        self.entries[index] = self.entries[index].replace("(running)", "(done)");
                    }
                    if let Some(output) = completed
                        .get("result")
                        .and_then(|v| v.get("completed"))
                        .and_then(|v| v.get("output"))
                        .and_then(serde_json::Value::as_str)
                    {
                        self.entries.push(format!("  Result: {output}"));
                    }
                }
                if let Some(message) = record
                    .get("inference_completed")
                    .or_else(|| record.get("message_appended"))
                    .and_then(|v| v.get("message"))
                    && let api::LogId::Session(session_id) = &event.log_id
                {
                    self.message(message, session_id);
                }
            }
            StreamItem::TokenDelta { .. } => {}
        }
    }
    fn message(&mut self, message: &serde_json::Value, session_id: &str) {
        let Some(id) = message.get("id").and_then(serde_json::Value::as_str) else {
            return;
        };
        let role = message.get("role").and_then(serde_json::Value::as_str);
        let text = message
            .get("parts")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|part| {
                part.get("text")
                    .and_then(|v| v.get("text"))
                    .and_then(serde_json::Value::as_str)
            })
            .collect::<String>();
        match role {
            Some("user") if self.users.insert(id.into()) => {
                self.entries.push(format!("You: {text}"));
            }
            Some("system") if self.systems.insert(id.into()) => {
                self.entries
                    .push(format!("System [session {session_id}]: {text}"));
            }
            Some("assistant") if self.assistants.insert(id.into()) => {
                self.partial.clear();
                if !text.is_empty() {
                    self.entries.push(format!("Agent: {text}"));
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view_busy() -> View {
        View {
            entries: Vec::new(),
            partial: String::new(),
            users: HashSet::new(),
            assistants: HashSet::new(),
            systems: HashSet::new(),
            tools: HashMap::new(),
            ready: false,
            queued: None,
            state: "Runnable".into(),
            selection: "fake/test low".into(),
        }
    }

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::empty())
    }

    fn state_event(to: &str, sequence: u64) -> StreamItem {
        StreamItem::Event(api::Event {
            log_id: api::LogId::Session("test".into()),
            sequence,
            payload: api::EventPayload::StoreRecord {
                record: serde_json::json!({"state_changed": {"from": "other", "to": to}}),
            },
        })
    }

    #[test]
    fn typing_edits_input_while_busy() {
        let mut view = view_busy();
        let mut input = Input::default();
        for c in "hi".chars() {
            assert_eq!(
                handle_key(&mut view, &mut input, key(KeyCode::Char(c))),
                None
            );
        }
        assert_eq!(input.text, "hi");
        assert_eq!(handle_key(&mut view, &mut input, key(KeyCode::Left)), None);
        assert_eq!(
            handle_key(&mut view, &mut input, key(KeyCode::Backspace)),
            None
        );
        assert_eq!(input.text, "i");
        assert_eq!(
            handle_key(&mut view, &mut input, key(KeyCode::Char('a'))),
            None
        );
        assert_eq!(input.text, "ai");
        assert!(!view.ready);
        // The line survives a busy state record and stays visible for render.
        view.event(state_event("leased", 1));
        assert_eq!(input.text, "ai");
        assert!(!view.ready);
        assert!(input.view(20).0.contains("ai"));
    }

    #[test]
    fn enter_while_busy_queues_and_idle_sends_once() {
        let mut view = view_busy();
        let mut input = Input::default();
        for c in "queued hello".chars() {
            handle_key(&mut view, &mut input, key(KeyCode::Char(c)));
        }
        assert_eq!(handle_key(&mut view, &mut input, key(KeyCode::Enter)), None);
        assert_eq!(view.queued.as_deref(), Some("queued hello"));
        assert!(input.text.is_empty());
        assert!(view.status_text("ephemeral", "test").contains("queued"));
        assert!(
            view.status_text("ephemeral", "test")
                .contains("input locked")
        );
        // Still busy: nothing to send yet.
        assert_eq!(view.take_queued_if_ready(), None);
        assert_eq!(view.queued.as_deref(), Some("queued hello"));
        // A transient busy record keeps the queued line waiting.
        view.event(state_event("leased", 2));
        assert_eq!(view.take_queued_if_ready(), None);
        // The next idle record releases the line exactly once.
        view.event(state_event("idle", 3));
        assert!(view.ready);
        assert_eq!(view.take_queued_if_ready().as_deref(), Some("queued hello"));
        assert_eq!(view.take_queued_if_ready(), None);
        assert_eq!(view.queued, None);
    }

    #[test]
    fn enter_while_idle_sends_immediately() {
        let mut view = view_busy();
        view.ready = true;
        let mut input = Input::default();
        for c in "now".chars() {
            handle_key(&mut view, &mut input, key(KeyCode::Char(c)));
        }
        assert_eq!(
            handle_key(&mut view, &mut input, key(KeyCode::Enter)).as_deref(),
            Some("now")
        );
        assert_eq!(view.queued, None);
        assert!(input.text.is_empty());
    }

    fn api_error(status: reqwest::StatusCode, code: &str) -> anyhow::Error {
        swarmy_client::Error::Api {
            status,
            body: swarmy_api_types::ApiError {
                code: code.into(),
                message: code.into(),
                provider_text: None,
            },
        }
        .into()
    }

    #[test]
    fn only_busy_race_requeues_and_permanent_errors_propagate() {
        // Typed busy races requeue: the session flipped after the ready
        // render, or the head moved between the render and the append.
        assert!(is_busy_send_error(&api_error(
            reqwest::StatusCode::CONFLICT,
            "session_not_idle"
        )));
        assert!(is_busy_send_error(&api_error(
            reqwest::StatusCode::CONFLICT,
            "stale_head"
        )));
        // The local idle guard reports the same race without a status code.
        assert!(is_busy_send_error(&anyhow::anyhow!("session is not idle")));
        // The retry path wraps the client error in a string; the code text
        // must still count as busy.
        assert!(is_busy_send_error(&anyhow::anyhow!(
            "API at http://example: session_not_idle"
        )));
        // Permanent failures propagate so the client exits with the message
        // instead of waiting forever on `input locked (queued)`.
        assert!(!is_busy_send_error(&api_error(
            reqwest::StatusCode::NOT_FOUND,
            "session_not_found"
        )));
        assert!(!is_busy_send_error(&api_error(
            reqwest::StatusCode::UNAUTHORIZED,
            "unauthorized"
        )));
        assert!(!is_busy_send_error(&api_error(
            reqwest::StatusCode::CONFLICT,
            "main_session_close"
        )));
        assert!(!is_busy_send_error(&anyhow::anyhow!(
            "API at http://example: request timed out"
        )));
    }
}
