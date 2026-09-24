use crate::{bench_command, client_conversation::Conversation, selection_command::SelectionArgs};
use anyhow::{Context, Result, ensure};
use std::{
    io::Write,
    time::{Duration, Instant},
};
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

pub async fn bench(client: Client, command: bench_command::Command, json: bool) -> Result<()> {
    let bench_command::Command::Turn {
        turns,
        image,
        output,
        timeout_secs,
    } = command;
    let settings = swarmy_config::Settings::load()?.settings;
    ensure!(
        settings.provider == "fake",
        "bench turn requires provider=fake"
    );
    let script: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&settings.fake.script).context("read fake script")?)?;
    let expected: serde_json::Value =
        serde_json::from_str(include_str!("../../../scripts/benchmarks/turn-fake.json"))?;
    ensure!(
        script == expected,
        "bench turn requires scripts/benchmarks/turn-fake.json"
    );
    let mut samples = Vec::new();
    for shape in ["no_tool", "bash"] {
        let mut conversation = Conversation::open(
            client.clone(),
            None,
            Some(image.clone()),
            None,
            false,
            swarmy_core::InferenceSelection::default(),
        )
        .await?;
        for index in 0..=turns {
            let start = Instant::now();
            tokio::time::timeout(Duration::from_secs(timeout_secs), async {
                conversation
                    .send(format!("swarmy bench turn {shape}"))
                    .await?;
                conversation.until_idle(false, true).await
            })
            .await
            .with_context(|| format!("{shape} turn {index} timed out"))??;
            ensure!(
                conversation.last_text == "TURN_OK\n",
                "unexpected fake response"
            );
            ensure!(
                conversation.tool_count == usize::from(shape == "bash"),
                "wrong tool count"
            );
            samples.push(serde_json::json!({"shape":shape,"warmup":index == 0,"end_to_end_ms":start.elapsed().as_secs_f64()*1000.0}));
        }
    }
    if let Some(path) = output {
        std::fs::write(path, serde_json::to_vec_pretty(&samples)?)?;
    }
    for shape in ["no_tool", "bash"] {
        let mut values: Vec<f64> = samples
            .iter()
            .filter(|s| s["shape"] == shape && s["warmup"] == false)
            .filter_map(|s| s["end_to_end_ms"].as_f64())
            .collect();
        values.sort_by(f64::total_cmp);
        let percentile = |p: usize| values[(values.len() * p).div_ceil(100) - 1];
        let (p50, p95) = (percentile(50), percentile(95));
        if json {
            println!(
                "{}",
                serde_json::json!({"shape":shape,"stage":"end_to_end","turns":turns,"p50_ms":p50,"p95_ms":p95})
            );
        } else {
            println!(
                "{shape}: {turns} turns, one excluded warmup; API end-to-end (ms)\nend_to_end               p50 {p50:10.3}  p95 {p95:10.3}"
            );
        }
    }
    Ok(())
}
