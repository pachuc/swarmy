use crate::selection_command::SelectionArgs;
use anyhow::Result;
use std::io::Write;
use swarmy_api_types as api;
use swarmy_chat::client_conversation::{Conversation, ConversationItem, OpenArgs, TurnOutput};
use swarmy_client::Client;

/// Flags for `swarmy run`, sharing one struct from parsing to execution so
/// the dispatch passes three arguments instead of nine.
#[derive(clap::Args)]
pub(crate) struct RunArgs {
    pub prompt: String,
    #[arg(long)]
    pub image: Option<String>,
    /// Resume the main session on a named agent (name or agent id)
    #[arg(long, conflicts_with = "image")]
    pub agent: Option<String>,
    /// Create a side conversation on the named agent
    #[arg(long, requires = "agent")]
    pub new: bool,
    /// Continue an existing session by id instead of an agent's main one
    #[arg(long, conflicts_with_all = ["agent", "image", "new"])]
    pub session: Option<ulid::Ulid>,
    /// Deliver after the current tool call without interrupting the turn.
    #[arg(long, requires = "session")]
    pub queue: bool,
    #[command(flatten)]
    pub selection: SelectionArgs,
}

/// Flags for `swarmy chat`, sharing one struct from parsing to execution.
#[derive(clap::Args)]
pub(crate) struct ChatArgs {
    #[arg(conflicts_with_all = ["provider", "model", "effort"])]
    pub session_id: Option<ulid::Ulid>,
    /// Base image in NAME:TAG form; otherwise use `default_image`.
    #[arg(long, conflicts_with = "session_id")]
    pub image: Option<String>,
    /// Resume the main session on a named agent (name or agent id)
    #[arg(long, conflicts_with_all = ["image", "session_id"])]
    pub agent: Option<String>,
    /// Create a side conversation on the named agent
    #[arg(long, requires = "agent")]
    pub new: bool,
    #[command(flatten)]
    pub selection: SelectionArgs,
}

pub(crate) async fn run(client: Client, endpoint: String, args: RunArgs, json: bool) -> Result<()> {
    let RunArgs {
        prompt,
        image,
        agent,
        new,
        session,
        queue,
        selection,
    } = args;
    let route = selection.route.clone();
    let mut conversation = Conversation::open(
        client,
        endpoint,
        OpenArgs {
            id: session.map(|id| id.to_string()),
            image,
            agent,
            new,
            selection: selection.into(),
            route,
        },
    )
    .await?;
    announce(&conversation, json);
    report_followed(&conversation, json);
    let busy = conversation.session.state != api::SessionState::Idle;
    conversation.send_with_queue(prompt, queue).await?;
    if queue && busy {
        if json {
            print_event(&Event::MessageQueued {
                session_id: &conversation.id,
            });
        } else {
            eprintln!("Message queued for next step boundary.");
        }
        return Ok(());
    }
    let result = conversation
        .until_idle(json, true, &mut |output| print_turn_output(output, json))
        .await;
    if json {
        match &result {
            Ok(()) => print_event(&Event::RunOutcome {
                outcome: "completed",
                reason: None,
            }),
            Err(error) => print_event(&Event::RunOutcome {
                outcome: "failed",
                reason: Some(&error.to_string()),
            }),
        }
    }
    Ok(result?)
}

fn announce(conversation: &Conversation, json: bool) {
    if json {
        print_event(&if conversation.created {
            Event::SessionCreated {
                session_id: &conversation.id,
                agent_name: &conversation.agent_name,
            }
        } else {
            Event::SessionOpened {
                session_id: &conversation.id,
                agent_name: &conversation.agent_name,
            }
        });
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
        print_event(&Event::SessionSummarized {
            previous_session_id,
            session_id,
        });
    } else {
        eprintln!(
            "Conversation summarized. Session {previous_session_id} archived; continuing in {session_id}."
        );
    }
}

/// Print one turn output. The conversation hands these over as they arrive,
/// so text still streams incrementally; the emitter already knows JSON vs text.
fn print_turn_output(output: TurnOutput, json: bool) {
    match output {
        TurnOutput::TokenText(text) => {
            if json {
                print_event(&Event::model_delta(&text));
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
                print_event(&Event::AssistantMessage { text: &text });
            } else {
                print!("{text}");
                std::io::stdout().flush().expect("stdout flushes");
            }
        }
        TurnOutput::SessionIdle { session_id } => {
            print_event(&Event::SessionIdle {
                session_id: &session_id,
            });
        }
        TurnOutput::Summarized {
            previous_session_id,
            session_id,
        } => {
            print_summary(json, &previous_session_id, &session_id);
        }
    }
}

/// One machine-readable JSON line on stdout. A single tagged enum replaces
/// the earlier per-event structs so every line shares one shape and one
/// print helper; the serialized form is unchanged, including the `Text`
/// discriminant the fleet driver and the `cli_session` suite read. Field
/// insertion order in the serialized JSON may differ from earlier builds;
/// field names and nesting do not, and nothing parses by position.
#[derive(serde::Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub(crate) enum Event<'a> {
    SessionCreated {
        session_id: &'a str,
        agent_name: &'a Option<String>,
    },
    SessionOpened {
        session_id: &'a str,
        agent_name: &'a Option<String>,
    },
    MessageQueued {
        session_id: &'a str,
    },
    RunOutcome {
        outcome: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<&'a str>,
    },
    SessionSummarized {
        previous_session_id: &'a str,
        session_id: &'a str,
    },
    ModelDelta {
        delta: serde_json::Value,
    },
    SessionIdle {
        session_id: &'a str,
    },
    AssistantMessage {
        text: &'a str,
    },
    ImageBuilt {
        name: &'a str,
        tag: &'a str,
        manifest_id: &'a str,
        header: &'a swarmy_api_types::ImageHeader,
        size: u64,
        chunks_total: u64,
        chunks_stored: u64,
        chunks_uploaded: u64,
    },
    ProbeSummary {
        provider: &'a str,
        model: &'a str,
        usage: &'a serde_json::Value,
        cost_micros: u64,
        effort: swarmy_api_types::ReasoningEffort,
        elapsed_seconds: f64,
    },
}

impl<'a> Event<'a> {
    /// The typed `image_built` line for an upload response.
    #[must_use]
    pub(crate) fn image_built(uploaded: &'a swarmy_api_types::ImageUpload) -> Self {
        Self::ImageBuilt {
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

    /// The `model_delta` line keeps the `Text` discriminant name the fleet
    /// reads. The payload is built as JSON so no separate wrapper structs
    /// are needed; the serialized bytes match the old shape.
    #[must_use]
    pub(crate) fn model_delta(text: &str) -> Self {
        Self::ModelDelta {
            delta: serde_json::json!({
                "Text": {
                    "output_index": 0,
                    "text": text,
                },
            }),
        }
    }
}

/// Print one event line. Serialization of these shapes cannot fail, so the
/// helper owns the `expect` instead of repeating it at every call site.
pub(crate) fn print_event(event: &Event) {
    println!(
        "{}",
        serde_json::to_string(event).expect("event serializes")
    );
}

pub(crate) async fn chat(client: Client, endpoint: String, args: ChatArgs, json: bool) -> Result<()> {
    use tokio::io::AsyncBufReadExt;
    let ChatArgs {
        session_id,
        image,
        agent,
        new,
        selection,
    } = args;
    let route = selection.route.clone();
    let mut conversation = Conversation::open(
        client,
        endpoint,
        OpenArgs {
            id: session_id.map(|id| id.to_string()),
            image,
            agent,
            new,
            selection: selection.into(),
            route,
        },
    )
    .await?;
    announce(&conversation, json);
    // Do not accept input while a worker or scheduler is missing. Recheck without
    // consuming stdin, so a user can type a prompt while the stack recovers.
    conversation
        .wait_healthy(conversation.provider.as_deref(), &mut |message| {
            eprintln!("{message}");
        })
        .await?;

    if conversation.session.state != api::SessionState::Idle {
        conversation
            .until_idle(json, false, &mut |output| {
                print_turn_output(output, json);
            })
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
                conversation.wait_healthy(conversation.provider.as_deref(), &mut |message| {
                    eprintln!("{message}");
                }).await?;
                conversation.send(prompt).await?;
                conversation
                    .until_idle(json, false, &mut |output| {
                        print_turn_output(output, json);
                    })
                    .await?;
            }
            _ = tokio::signal::ctrl_c() => {
                conversation.interrupt().await?;
                return Ok(());
            }
            item = conversation.next() => {
                match item? {
                    ConversationItem::Stream(swarmy_client::StreamItem::Event(event)) => {
                        conversation.queue(swarmy_client::StreamItem::Event(event));
                        conversation
                            .until_idle(json, false, &mut |output| {
                                print_turn_output(output, json);
                            })
                            .await?;
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
