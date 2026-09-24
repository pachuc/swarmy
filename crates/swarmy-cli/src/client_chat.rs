//! Terminal renderer for the API conversation stream.
#[path = "chat/input.rs"]
mod input;

use crate::{client_conversation::Conversation, selection_command::SelectionArgs};
use anyhow::{Context, Result, ensure};
use crossterm::{
    event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use futures_util::StreamExt;
use input::Input;
use ratatui::{
    DefaultTerminal,
    widgets::{List, ListItem, ListState, Paragraph, Wrap},
};
use std::{
    collections::HashSet,
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

async fn picker(client: &Client) -> Result<Option<Option<String>>> {
    let sessions = recent(client).await?;
    let (mut terminal, _restore) = terminal()?;
    let mut keys = EventStream::new();
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
    let choice = if id.is_none()
        && agent.is_none()
        && selection.provider.is_none()
        && selection.model.is_none()
        && selection.effort.is_none()
    {
        let Some(choice) = picker(&client).await? else {
            return Ok(());
        };
        choice
    } else {
        id.map(|value| value.to_string())
    };
    let mut conversation =
        Conversation::open(client.clone(), choice, image, agent, new, selection.into()).await?;
    conversation
        .wait_healthy(conversation.provider.as_deref())
        .await?;
    let (mut terminal, _restore) = terminal()?;
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
    let mut keys = EventStream::new();
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
            if view.ready && rows[2].width > 0 {
                frame.set_cursor_position((rows[2].x + cursor, rows[2].y));
            }
        })?;
        tokio::select! {
            event = conversation.next() => view.event(event?),
            key = keys.next() => {
                if let Event::Key(key) = key.context("terminal input closed")??
                    && key.kind != KeyEventKind::Release {
                    if quit(key) { return Ok(()); }
                    if !view.ready { continue; }
                    match key.code {
                        KeyCode::Enter if !input.text.trim().is_empty() => {
                            let text = input.take();
                            let turn = conversation.send(text.clone()).await?;
                            view.users.insert(turn);
                            view.entries.push(format!("You: {text}"));
                            view.ready = false;
                            view.state = "Runnable".into();
                        }
                        KeyCode::Char(c) if !key.modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) => input.insert(c),
                        KeyCode::Backspace => input.backspace(),
                        KeyCode::Left => input.left(),
                        KeyCode::Right => input.right(),
                        _ => {}
                    }
                }
            }
        }
    }
}

struct View {
    entries: Vec<String>,
    partial: String,
    users: HashSet<String>,
    assistants: HashSet<String>,
    ready: bool,
    state: String,
}
impl View {
    fn new(conversation: &Conversation) -> Self {
        Self {
            entries: Vec::new(),
            partial: String::new(),
            users: HashSet::new(),
            assistants: HashSet::new(),
            ready: false,
            state: format!("{:?}", conversation.session.state),
        }
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
        let input = if self.ready {
            "Enter: send"
        } else {
            "input locked"
        };
        format!(
            "{} | {} | {} | {} | {input} | Esc: quit",
            conversation.agent_name.as_deref().unwrap_or("ephemeral"),
            conversation.id,
            self.state,
            conversation.provider.as_deref().unwrap_or("default")
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
                if let Some(state) = record
                    .get("state_changed")
                    .and_then(|s| s.get("to"))
                    .and_then(serde_json::Value::as_str)
                {
                    self.state = state.into();
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
                    self.entries.push(format!(
                        "Tool: {} {}",
                        call.get("tool")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("tool"),
                        call.get("arguments").unwrap_or(&serde_json::Value::Null)
                    ));
                }
                if let Some(message) = record
                    .get("inference_completed")
                    .or_else(|| record.get("message_appended"))
                    .and_then(|v| v.get("message"))
                {
                    self.message(message);
                }
            }
            StreamItem::TokenDelta { .. } => {}
        }
    }
    fn message(&mut self, message: &serde_json::Value) {
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
