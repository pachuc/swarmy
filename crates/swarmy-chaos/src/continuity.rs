//! The persistent-agent acceptance uses real services, disk tools, and a scripted provider.
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use jiff::Timestamp;
use serde_json::{Value, json};
use swarmy_core::{
    AgentId, Event, Message, MessageId, MessageRole, Part, SessionId, SessionState, TimerRecord,
    TimerStatus, ToolResult,
};
use swarmy_llm::InferenceJob;
use tokio::time::{sleep, timeout};

use crate::{Fixture, process::Kind};

const FACT: &str = "Tommy's favorite observatory is Violet Ridge.";
const NOTE: &str = "Two-minute reminder: check the observatory notebook.";
const ANSWER: &str = "My memory file says: Tommy's favorite observatory is Violet Ridge.";

fn response(parts: &Value, stop: &str, tokens: u64) -> Value {
    json!({"parts":parts, "stop_reason":stop, "usage":{"input_tokens":tokens,"cached_input_tokens":0,"output_tokens":0,"reasoning_output_tokens":0,"total_tokens":tokens}})
}

fn tool(name: &str, input: &Value) -> Value {
    response(
        &json!([{"tool_call":{"call_id":name,"tool":name,"input":input}}]),
        "tool_calls",
        0,
    )
}

fn answer(text: &str, tokens: u64) -> Value {
    response(&json!([{"text":{"text":text}}]), "end_turn", tokens)
}

async fn script(f: &mut Fixture, responses: Vec<Value>) -> Result<()> {
    let responses: serde_json::Map<_, _> = responses
        .into_iter()
        .enumerate()
        .map(|(index, value)| (index.to_string(), value))
        .collect();
    std::fs::write(
        f.files.path().join("script.json"),
        serde_json::to_vec(&json!({"responses":responses}))?,
    )?;
    for process in &mut f.processes {
        if process.kind == Kind::Gateway {
            process.restart().await?;
        }
    }
    Ok(())
}

async fn send(f: &Fixture, id: SessionId, text: &str) -> Result<u64> {
    let head = f
        .store
        .fetch_session(id)
        .await?
        .context("session missing")?
        .head_seq;
    let message = Message {
        id: MessageId::from_ulid(ulid::Ulid::generate()),
        role: MessageRole::User,
        parts: vec![Part::Text { text: text.into() }],
    };
    let head = f.store.append_user_message(id, head, &message).await?;
    f.bus
        .nudge(id, head, Some(message.id), Duration::ZERO, true)
        .await?;
    tracing::info!(%id, user = text, "continuity transcript");
    Ok(head)
}

async fn settled(
    f: &mut Fixture,
    agent: AgentId,
    previous: SessionId,
    after: u64,
    summarized: bool,
) -> Result<SessionId> {
    timeout(Duration::from_secs(240), async {
        loop {
            for process in &mut f.processes {
                process.check()?;
            }
            let id = f
                .store
                .get_agent(agent)
                .await?
                .and_then(|a| a.main_session)
                .context("main missing")?;
            let session = f
                .store
                .fetch_session(id)
                .await?
                .context("session missing")?;
            if session.state == SessionState::Idle
                && if summarized {
                    id != previous
                } else {
                    session.head_seq > after
                }
            {
                let mut events = Vec::new();
                let old = f
                    .store
                    .fetch_session(previous)
                    .await?
                    .context("old missing")?;
                crate::read_through(&f.store, previous, &mut events, old.head_seq).await?;
                for event in events.iter().filter(|event| event.seq() > after) {
                    if let Event::ToolCallCompleted { result, .. } = event {
                        let ToolResult::Completed { title, output, .. } = result else {
                            anyhow::bail!("tool failed: {result:?}")
                        };
                        if title == "bash" {
                            let value: Value = serde_json::from_str(output)?;
                            ensure!(value["exit_code"] == 0, "bash failed: {value}");
                        }
                        tracing::info!(tool = title, %output, "continuity transcript");
                    }
                }
                return Ok(id);
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("continuity turn timed out")?
}

pub async fn exercise(f: &mut Fixture) -> Result<()> {
    let agent = f
        .store
        .create_agent(
            "tommy",
            "chaos:test",
            "Remember durable facts",
            Timestamp::now(),
        )
        .await?
        .agent_id;
    let (first, _) = f.store.open_main_session(agent, Timestamp::now()).await?;
    f.sessions.push(first);
    script(f, vec![
        tool("write", &json!({"path":"/home/agent/memory/facts.txt","content":format!("{FACT}\n")})),
        tool("bash", &json!({"command":"apt-get update -qq && apt-get install -y -qq tree && tree --version", "yield_seconds":120,"timeout_ms":180_000})),
        tool("checkpoint", &json!({})),
        answer("Saved the fact in my memory file, installed tree, and checkpointed the disk.", 0),
    ]).await?;
    let head = send(f, first, &format!("Remember this fact in your memory files: {FACT} Install the tree tool and checkpoint your disk.")).await?;
    settled(f, agent, first, head, false).await?;

    // Deliberately omit the fact from the summary. The recalled fact must be in disk memory.
    let summary = json!({"goals":"Remember the user's fact", "state_of_work":"Memory saved and tree installed", "open_questions":"", "facts_to_keep":"Read memory files for the user's fact"}).to_string();
    script(
        f,
        vec![
            tool("set_timer", &json!({"delay_seconds":120,"note":NOTE})),
            answer("Timer set; ready to summarize.", 100),
            answer(&summary, 0),
        ],
    )
    .await?;
    let head = send(
        f,
        first,
        "Set a two-minute timer with the observatory notebook reminder, then summarize.",
    )
    .await?;
    let main = settled(f, agent, first, head, true).await?;
    f.sessions.push(main);
    let timers = f.store.list_timers(agent).await?;
    ensure!(timers.len() == 1, "expected one pending timer");
    let timer: &TimerRecord = &timers[0];
    ensure!(timer.note == NOTE, "wrong timer note");
    ensure!(
        f.store.next_session(first).await? == Some(main),
        "missing summary link"
    );
    let opening = f.store.read_events(main, 0, 64).await?;
    ensure!(
        !serde_json::to_string(&opening)?.contains("Violet Ridge"),
        "summary leaked the fact"
    );
    tracing::info!(%first, %main, timer_id = %timer.timer_id, due_at = %timer.due_at, "continuity summarized; fact absent from new transcript");

    restart_all(f, agent).await?;
    ensure!(
        f.store.list_timers(agent).await? == timers,
        "pending timer changed across restart"
    );
    let head = send(f, main, "What observatory did I ask you to remember? Read your memory file and verify tree is still installed.").await?;
    settled(f, agent, main, head, false).await?;
    verify_recall(f, main).await?;
    tracing::info!(
        assistant = ANSWER,
        "continuity transcript; fact verified in inference memory context"
    );
    verify_timer(f, main, timer).await
}

async fn restart_all(f: &mut Fixture, agent: AgentId) -> Result<()> {
    // All services, including the node, are stopped before any are started again.
    for process in &mut f.processes {
        process.stop().await?;
    }
    ensure!(
        f.store.get_by_agent(agent).await?.is_none(),
        "node did not release the computer"
    );
    let volume = f
        .store
        .get_volume(swarmy_core::VolumeId::from_ulid(agent.as_ulid()))
        .await?
        .context("home volume missing")?;
    ensure!(
        volume.writer_lease.is_none(),
        "node did not detach the home volume"
    );
    tracing::info!(at = %Timestamp::now(), "continuity all swarmy services and node stopped");
    let responses = [
        tool(
            "bash",
            &json!({"command":"tree --version", "yield_seconds":10}),
        ),
        tool("read", &json!({"path":"/home/agent/memory/facts.txt"})),
        answer(ANSWER, 0),
        answer(
            "The timer fired: Two-minute reminder: check the observatory notebook.",
            0,
        ),
    ];
    let responses: serde_json::Map<_, _> = responses
        .into_iter()
        .enumerate()
        .map(|(i, r)| (i.to_string(), r))
        .collect();
    std::fs::write(
        f.files.path().join("script.json"),
        serde_json::to_vec(&json!({"responses":responses}))?,
    )?;
    for process in &mut f.processes {
        process.start_stopped()?;
    }
    f.ready().await?;
    tracing::info!(at = %Timestamp::now(), "continuity all swarmy services and node restarted");
    Ok(())
}

async fn verify_timer(f: &mut Fixture, main: SessionId, timer: &TimerRecord) -> Result<()> {
    let agent = timer.agent_id;
    timeout(Duration::from_secs(180), async {
        loop {
            let receipt = f.store.get_timer(agent, timer.timer_id).await?.context("timer missing")?;
            let session = f.store.fetch_session(main).await?.context("main missing")?;
            if matches!(receipt.status, TimerStatus::Fired { .. }) && session.state == SessionState::Idle {
                let mut events = Vec::new();
                crate::read_through(&f.store, main, &mut events, session.head_seq).await?;
                ensure!(events.iter().filter(|event| matches!(event, Event::MessageAppended { message, .. } if message.role == MessageRole::System && message.parts == [Part::Text { text: NOTE.into() }])).count() == 1, "expected exactly one timer note");
                ensure!(serde_json::to_string(&events)?.contains("The timer fired:"), "worker did not answer timer");
                ensure!(Timestamp::now() >= timer.due_at, "timer fired early");
                tracing::info!(at = %Timestamp::now(), note = NOTE, ?receipt.status, "continuity timer delivered once and processed after restart");
                return Ok::<_, anyhow::Error>(());
            }
            for process in &mut f.processes { process.check()?; }
            sleep(Duration::from_millis(100)).await;
        }
    }).await.context("timer did not fire after restart")??;
    Ok(())
}

async fn verify_recall(f: &Fixture, main: SessionId) -> Result<()> {
    let head = f
        .store
        .fetch_session(main)
        .await?
        .context("main missing")?
        .head_seq;
    let mut events = Vec::new();
    crate::read_through(&f.store, main, &mut events, head).await?;
    let request = events
        .iter()
        .rev()
        .find_map(|event| match event {
            Event::InferenceCompleted {
                request_id,
                message,
                ..
            } if message.parts
                == [Part::Text {
                    text: ANSWER.into(),
                }] =>
            {
                Some(*request_id)
            }
            _ => None,
        })
        .context("memory answer missing")?;
    let job = f
        .store
        .get_inference_input::<InferenceJob>(request)
        .await?
        .context("inference input missing")?;
    ensure!(
        job.request.system_prompt.contains(FACT),
        "memory file absent from system prompt"
    );
    ensure!(events.iter().any(|event| matches!(event, Event::ToolCallCompleted { result: ToolResult::Completed { title, output, .. }, .. } if title == "read" && output.contains(FACT))), "memory read missing");
    Ok(())
}
