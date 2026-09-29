use crate::selection_command::SelectionArgs;
use anyhow::Result;
use std::io::Write;
use swarmy_api_types as api;
use swarmy_chat::client_conversation::{Conversation, ConversationItem, OutputMode, TurnOutput};
use swarmy_client::Client;

// The arguments mirror the `run` CLI flags plus the client and output mode,
// so eight parameters is inherent to the dispatch shape.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    client: Client,
    prompt: String,
    image: Option<String>,
    agent: Option<String>,
    new: bool,
    session: Option<ulid::Ulid>,
    queue: bool,
    selection: SelectionArgs,
    json: bool,
) -> Result<()> {
    let route = selection.route.clone();
    let mut conversation = Conversation::open(
        client,
        session.map(|id| id.to_string()),
        image,
        agent,
        new,
        selection.into(),
        route,
    )
    .await?;
    announce(&conversation, json);
    report_followed(&conversation, json);
    let busy = conversation.session.state != api::SessionState::Idle;
    conversation.send_with_queue(prompt, queue).await?;
    if queue && busy {
        if json {
            println!(
                "{}",
                serde_json::to_string(&MessageQueued {
                    event: "message_queued",
                    session_id: &conversation.id,
                })?
            );
        } else {
            eprintln!("Message queued for next step boundary.");
        }
        return Ok(());
    }
    let mode = if json {
        OutputMode::JsonRun
    } else {
        OutputMode::TextRun
    };
    let result = conversation
        .until_idle(mode, &mut |output| print_turn_output(output, json))
        .await;
    if json {
        match &result {
            Ok(()) => println!(
                "{}",
                serde_json::to_string(&RunOutcome {
                    event: "run_outcome",
                    outcome: "completed",
                    reason: None,
                })?
            ),
            Err(error) => println!(
                "{}",
                serde_json::to_string(&RunOutcome {
                    event: "run_outcome",
                    outcome: "failed",
                    reason: Some(&error.to_string()),
                })?
            ),
        }
    }
    Ok(result?)
}

fn announce(conversation: &Conversation, json: bool) {
    if json {
        println!(
            "{}",
            serde_json::to_string(&SessionAnnounce {
                event: if conversation.created {
                    "session_created"
                } else {
                    "session_opened"
                },
                session_id: &conversation.id,
                agent_name: &conversation.agent_name,
            })
            .expect("announcement serializes")
        );
    } else {
        eprintln!("Session {}", conversation.id);
    }
}

/// Print the summary notice when `open` followed an archived session.
/// `run --session OLD` lands on the successor, so the operator sees where
/// the transcript continued without re-reading the archived log.
fn report_followed(conversation: &Conversation, json: bool) {
    let Some(previous) = &conversation.predecessor else {
        return;
    };
    print_summary(json, previous, &conversation.id);
}

fn print_summary(json: bool, previous_session_id: &str, session_id: &str) {
    if json {
        println!(
            "{}",
            serde_json::to_string(&SessionSummarized {
                event: "session_summarized",
                previous_session_id,
                session_id,
            })
            .expect("summary serializes")
        );
    } else {
        eprintln!(
            "Conversation summarized. Session {previous_session_id} archived; continuing in {session_id}."
        );
    }
}

/// Print one turn output in the mode `until_idle` ran with. The conversation
/// hands these over as they arrive, so text still streams incrementally.
fn print_turn_output(output: TurnOutput, json: bool) {
    match output {
        TurnOutput::TokenText(text) => {
            if json {
                println!(
                    "{}",
                    serde_json::to_string(&ModelDelta {
                        event: "model_delta",
                        delta: ModelDeltaInner {
                            text: ModelDeltaText {
                                output_index: 0,
                                text: &text,
                            },
                        },
                    })
                    .expect("delta serializes")
                );
            } else {
                print!("{text}");
                std::io::stdout().flush().expect("stdout flushes");
            }
        }
        TurnOutput::Record(record) => {
            println!(
                "{}",
                serde_json::to_string(&record).expect("record serializes")
            );
        }
        TurnOutput::QueueDelivered => {
            println!("[queued message delivered]");
        }
        TurnOutput::ToolCall {
            call_id,
            tool,
            arguments,
        } => {
            println!("Tool call {call_id} {tool} {arguments}");
        }
        TurnOutput::ToolResult { call_id, result } => {
            println!(
                "Tool result {call_id} {}",
                serde_json::to_string(&result).expect("tool result serializes")
            );
        }
        TurnOutput::AssistantMessage(text) => {
            if json {
                println!(
                    "{}",
                    serde_json::to_string(&AssistantMessage {
                        event: "assistant_message",
                        text: &text,
                    })
                    .expect("assistant message serializes")
                );
            } else {
                print!("{text}");
                std::io::stdout().flush().expect("stdout flushes");
            }
        }
        TurnOutput::SessionIdle { session_id } => {
            println!(
                "{}",
                serde_json::to_string(&SessionIdle {
                    event: "session_idle",
                    session_id: &session_id,
                })
                .expect("idle marker serializes")
            );
        }
        TurnOutput::Summarized {
            previous_session_id,
            session_id,
        } => {
            print_summary(json, &previous_session_id, &session_id);
        }
    }
}

/// One `session_created` or `session_opened` line the fleet driver reads.
#[derive(serde::Serialize)]
struct SessionAnnounce<'a> {
    event: &'a str,
    session_id: &'a str,
    agent_name: &'a Option<String>,
}

/// One `message_queued` line for a queued delivery.
#[derive(serde::Serialize)]
struct MessageQueued<'a> {
    event: &'static str,
    session_id: &'a str,
}

/// The terminal `run_outcome` line the fleet driver reads.
#[derive(serde::Serialize)]
struct RunOutcome<'a> {
    event: &'static str,
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'a str>,
}

/// One `session_summarized` line for a successor switch.
#[derive(serde::Serialize)]
struct SessionSummarized<'a> {
    event: &'static str,
    previous_session_id: &'a str,
    session_id: &'a str,
}

/// One `model_delta` line wrapping raw streamed text.
#[derive(serde::Serialize)]
struct ModelDelta<'a> {
    event: &'static str,
    delta: ModelDeltaInner<'a>,
}

/// The `delta` wrapper keeps the `Text` discriminant name the fleet reads.
#[derive(serde::Serialize)]
struct ModelDeltaInner<'a> {
    #[serde(rename = "Text")]
    text: ModelDeltaText<'a>,
}

/// One streamed text delta.
#[derive(serde::Serialize)]
struct ModelDeltaText<'a> {
    output_index: u32,
    text: &'a str,
}

/// The `session_idle` marker that ends a JSON turn.
#[derive(serde::Serialize)]
struct SessionIdle<'a> {
    event: &'static str,
    session_id: &'a str,
}

/// One `assistant_message` line with the turn's reply text.
#[derive(serde::Serialize)]
struct AssistantMessage<'a> {
    event: &'static str,
    text: &'a str,
}

/// The `image_built` line for a registered build. Lives here so the image
/// command shares the CLI's typed event lines.
#[derive(serde::Serialize)]
pub struct ImageBuilt<'a> {
    event: &'static str,
    name: &'a str,
    tag: &'a str,
    manifest_id: &'a str,
    header: &'a swarmy_api_types::ImageHeader,
    size: u64,
    chunks_total: u64,
    chunks_stored: u64,
    chunks_uploaded: u64,
}

impl<'a> ImageBuilt<'a> {
    /// The typed `image_built` line for an upload response.
    #[must_use]
    pub fn from_upload(uploaded: &'a swarmy_api_types::ImageUpload) -> Self {
        Self {
            event: "image_built",
            name: &uploaded.name,
            tag: &uploaded.tag,
            manifest_id: &uploaded.manifest_id,
            header: &uploaded.header,
            size: uploaded.size,
            chunks_total: uploaded.chunks_total,
            chunks_stored: uploaded.chunks_stored,
            chunks_uploaded: uploaded.chunks_uploaded,
        }
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
    let route = selection.route.clone();
    let mut conversation = Conversation::open(
        client,
        id.map(|id| id.to_string()),
        image,
        agent,
        new,
        selection.into(),
        route,
    )
    .await?;
    announce(&conversation, json);
    // Do not accept input while a worker or scheduler is missing. Recheck without
    // consuming stdin, so a user can type a prompt while the stack recovers.
    conversation
        .wait_healthy(conversation.provider.as_deref())
        .await?;
    let mode = if json {
        OutputMode::JsonChat
    } else {
        OutputMode::TextChat
    };
    if conversation.session.state != api::SessionState::Idle {
        conversation
            .until_idle(mode, &mut |output| print_turn_output(output, json))
            .await?;
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
                conversation.until_idle(mode, &mut |output| print_turn_output(output, json)).await?;
            }
            _ = tokio::signal::ctrl_c() => {
                conversation.interrupt().await?;
                return Ok(());
            }
            item = conversation.next() => {
                match item? {
                    ConversationItem::Stream(swarmy_client::StreamItem::Event(event)) => {
                        conversation.queue(swarmy_client::StreamItem::Event(event));
                        conversation.until_idle(mode, &mut |output| print_turn_output(output, json)).await?;
                    }
                    ConversationItem::Stream(swarmy_client::StreamItem::TokenDelta { .. }) => {}
                    ConversationItem::Summarized {
                        previous_session_id,
                        session_id,
                    } => {
                        print_summary(json, &previous_session_id, &session_id);
                    }
                }
            }
        }
    }
    Ok(())
}
