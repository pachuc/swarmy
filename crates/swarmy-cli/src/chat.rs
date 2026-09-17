mod input;
mod transcript;

use std::io::{self, IsTerminal};

use anyhow::{Context, Result, ensure};
use crossterm::{
    event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use futures_util::StreamExt;
use ratatui::{
    DefaultTerminal,
    widgets::{List, ListItem, ListState, Paragraph},
};
use swarmy_core::SessionId;

use crate::conversation::{Conversation, Notification, recent_sessions};
use input::Input;
use transcript::{Scroll, Transcript, clean, panes};

struct RestoreTerminal;

impl Drop for RestoreTerminal {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen, crossterm::cursor::Show);
    }
}

pub async fn run(id: Option<SessionId>) -> Result<()> {
    ensure!(
        io::stdin().is_terminal() && io::stdout().is_terminal(),
        "chat requires an interactive terminal"
    );
    let provider = swarmy_config::Settings::load()?.settings.provider;
    let sessions = if id.is_none() {
        recent_sessions().await?
    } else {
        Vec::new()
    };
    enable_raw_mode()?;
    let _restore = RestoreTerminal;
    execute!(io::stdout(), EnterAlternateScreen)?;
    let mut terminal = DefaultTerminal::new(ratatui::backend::CrosstermBackend::new(io::stdout()))?;
    terminal.clear()?;
    let mut keys = EventStream::new();
    let id = if let Some(id) = id {
        Some(id)
    } else {
        let Some(selection) = picker(&mut terminal, &mut keys, &sessions).await? else {
            return Ok(());
        };
        selection
    };
    terminal.draw(|frame| {
        frame.render_widget(Paragraph::new("Loading conversation..."), frame.area());
    })?;
    let conversation = Conversation::open(id).await?;
    interact(&mut terminal, &mut keys, conversation, &provider).await
}

fn quit(key: KeyEvent) -> bool {
    key.code == KeyCode::Esc
        || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
}

fn picker_items(sessions: &[(SessionId, String)]) -> Vec<ListItem<'static>> {
    std::iter::once(ListItem::new("New session"))
        .chain(
            sessions.iter().map(|(id, text)| {
                ListItem::new(format!("{id}  {}", clean(text).replace('\n', " ")))
            }),
        )
        .collect()
}

async fn picker(
    terminal: &mut DefaultTerminal,
    keys: &mut EventStream,
    sessions: &[(SessionId, String)],
) -> Result<Option<Option<SessionId>>> {
    let mut selection = ListState::default().with_selected(Some(0));
    loop {
        terminal.draw(|frame| {
            let [body, status, _] = panes(frame.area());
            frame.render_stateful_widget(
                List::new(picker_items(sessions)).highlight_symbol("> "),
                body,
                &mut selection,
            );
            frame.render_widget(
                Paragraph::new("Recent sessions | Up/Down: select | Enter: open | Esc: quit"),
                status,
            );
        })?;
        if let Event::Key(key) = keys.next().await.context("terminal input closed")??
            && key.kind != KeyEventKind::Release
        {
            let index = selection.selected().unwrap_or(0);
            if quit(key) {
                return Ok(None);
            }
            match key.code {
                KeyCode::Up => selection.select(Some(index.saturating_sub(1))),
                KeyCode::Down => selection.select(Some((index + 1).min(sessions.len()))),
                KeyCode::Enter => return Ok(Some(index.checked_sub(1).map(|i| sessions[i].0))),
                _ => {}
            }
        }
    }
}

async fn interact(
    terminal: &mut DefaultTerminal,
    keys: &mut EventStream,
    mut conversation: Conversation,
    provider: &str,
) -> Result<()> {
    let mut transcript = Transcript::default();
    let mut input = Input::default();
    let mut scroll = Scroll::default();
    let mut rendered = false;
    let mut enabled = false;
    loop {
        terminal.draw(|frame| {
            let [body, status, prompt] = panes(frame.area());
            let paragraph = transcript.paragraph();
            let offset = scroll.position(paragraph.line_count(body.width), body.height);
            frame.render_widget(paragraph.scroll((offset, 0)), body);
            frame.render_widget(
                Paragraph::new(transcript.status(conversation.id, provider)),
                status,
            );
            let (line, cursor) = input.view(prompt.width);
            frame.render_widget(Paragraph::new(line), prompt);
            if transcript.ready && prompt.width > 0 && prompt.height > 0 {
                frame.set_cursor_position((prompt.x + cursor, prompt.y));
            }
        })?;
        if rendered {
            conversation
                .observe(swarmy_core::TurnStage::FinalTextRendered)
                .await;
            rendered = false;
        }
        if enabled {
            conversation
                .observe(swarmy_core::TurnStage::InputEnabled)
                .await;
            enabled = false;
        }
        tokio::select! {
            event = conversation.next() => {
                match event? {
                Notification::Transcript(event) => {
                    rendered = matches!(&event, crate::conversation::TranscriptEvent::AssistantMessageFinal(message)
                        if crate::session::final_text(&swarmy_core::Event::MessageAppended { seq: 0, message: message.clone() }));
                    enabled = matches!(&event, crate::conversation::TranscriptEvent::SessionIdle) && !transcript.ready;
                    transcript.apply(event);
                }
                Notification::Delta(swarmy_llm::Delta::Completed(response))
                    if response.stop_reason == swarmy_llm::StopReason::EndTurn => {
                    rendered = response.parts.iter().any(|part| matches!(part,
                        swarmy_core::Part::Text { text } if !text.is_empty()));
                }
                _ => {}
                }
            }
            event = keys.next() => {
                if let Event::Key(key) = event.context("terminal input closed")??
                    && key.kind != KeyEventKind::Release {
                    if quit(key) { return Ok(()); }
                    let [body, _, _] = panes(terminal.get_frame().area());
                    let lines = transcript.paragraph().line_count(body.width);
                    match key.code {
                        KeyCode::PageUp => scroll.up(lines, body.height),
                        KeyCode::PageDown => scroll.down(lines, body.height),
                        KeyCode::End => scroll.end(),
                        _ if !transcript.ready => {}
                        KeyCode::Enter if !input.text.trim().is_empty() => {
                            transcript.ready = false;
                            conversation.send(input.take()).await?;
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
