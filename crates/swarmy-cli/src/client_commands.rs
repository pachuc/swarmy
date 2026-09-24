use crate::{client_conversation::Conversation, selection_command::SelectionArgs};
use anyhow::Result;
use std::io::Write;
use swarmy_api_types as api;
use swarmy_client::Client;

pub async fn run(
    client: Client,
    prompt: String,
    image: Option<String>,
    agent: Option<String>,
    new: bool,
    selection: SelectionArgs,
    json: bool,
) -> Result<()> {
    let mut conversation =
        Conversation::open(client, None, image, agent, new, selection.into()).await?;
    announce(&conversation, json);
    conversation.send(prompt).await?;
    let result = conversation.until_idle(json, true).await;
    if json {
        match &result {
            Ok(()) => println!(
                "{}",
                serde_json::json!({"event":"run_outcome","outcome":"completed"})
            ),
            Err(error) => println!(
                "{}",
                serde_json::json!({"event":"run_outcome","outcome":"failed","reason":error.to_string()})
            ),
        }
    }
    result
}

fn announce(conversation: &Conversation, json: bool) {
    if json {
        println!(
            "{}",
            serde_json::json!({"event":if conversation.created {"session_created"} else {"session_opened"}, "session_id":conversation.id,"agent_name":conversation.agent_name})
        );
    } else {
        eprintln!("Session {}", conversation.id);
    }
}

pub async fn chat(
    client: Client,
    id: Option<ulid::Ulid>,
    image: Option<String>,
    agent: Option<String>,
    new: bool,
    selection: SelectionArgs,
    json: bool,
) -> Result<()> {
    use tokio::io::AsyncBufReadExt;
    let mut conversation = Conversation::open(
        client,
        id.map(|id| id.to_string()),
        image,
        agent,
        new,
        selection.into(),
    )
    .await?;
    announce(&conversation, json);
    // Do not accept input while a worker or scheduler is missing. Recheck without
    // consuming stdin, so a user can type a prompt while the stack recovers.
    conversation
        .wait_healthy(conversation.provider.as_deref())
        .await?;
    if conversation.session.state != api::SessionState::Idle {
        conversation.until_idle(json, false).await?;
    }
    let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    loop {
        if !json {
            print!("> ");
            std::io::stdout().flush()?;
        }
        tokio::select! {
            line = lines.next_line() => {
                let Some(prompt) = line? else { break };
                if prompt.trim().is_empty() { continue; }
                conversation.wait_healthy(conversation.provider.as_deref()).await?;
                conversation.send(prompt).await?;
                conversation.until_idle(json, false).await?;
            }
            _ = tokio::signal::ctrl_c() => {
                conversation.interrupt().await?;
                return Ok(());
            }
            item = conversation.next() => {
                if let swarmy_client::StreamItem::Event(event) = item? {
                    conversation.queue(swarmy_client::StreamItem::Event(event));
                    conversation.until_idle(json, false).await?;
                }
            }
        }
    }
    Ok(())
}
