//! Coding acceptance with a real Git remote and scripted inference, without credentials.
use std::{path::Path, process::Command, time::Duration};

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use swarmy_core::{Event, Message, MessageId, MessageRole, Part, SessionState, ToolResult};
use tokio::time::{Instant, sleep, timeout};

use crate::{Fixture, process::Kind};

const WORK: &str = "/home/agent/work/proof";
const BRANCH: &str = "agent/readme-proof";
const LINE: &str = "2026-09-19: written by a swarmy agent.";
const RULE: &str = "Run sh test.sh after changing answer.txt.";
const DONE: &str = "Fixed the failing test and pushed the README proof.";

fn git(root: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git").current_dir(root).args(args).output()?;
    ensure!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?)
}

fn remote(root: &Path) -> Result<()> {
    git(
        root,
        &["init", "--bare", "--initial-branch=master", "remote.git"],
    )?;
    git(root, &["init", "--initial-branch=master", "seed"])?;
    let seed = root.join("seed");
    std::fs::write(seed.join("README.md"), "# Coding proof\n")?;
    std::fs::write(seed.join("AGENTS.md"), RULE)?;
    std::fs::write(seed.join("answer.txt"), "41\n")?;
    std::fs::write(seed.join("test.sh"), "test \"$(cat answer.txt)\" = 42\n")?;
    git(&seed, &["add", "."])?;
    git(
        &seed,
        &[
            "-c",
            "user.name=Swarmy Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-m",
            "Add failing fixture",
        ],
    )?;
    git(&seed, &["push", "../remote.git", "master"])?;
    Ok(())
}

fn tool(index: usize, name: &str, input: &Value) -> Value {
    json!({"parts":[{"tool_call":{"call_id":format!("coding-{index}"),"tool":name,"input":input}}], "stop_reason":"tool_calls", "usage":{"input_tokens":0,"cached_input_tokens":0,"output_tokens":0,"reasoning_output_tokens":0,"total_tokens":0}})
}

fn script(remote: &str, work: &str) -> Vec<Value> {
    let commands = [
        format!(
            "git clone {remote} {work} && cd {work} && git switch -c {BRANCH} && git config user.name 'Swarmy Test' && git config user.email test@example.invalid"
        ),
        format!("cd {work} && sh test.sh"),
        format!("cd {work} && touch kill-ready && sleep 120"),
        format!("cd {work} && test ! -e kill-ready && git status --short && git log -1 --oneline"),
        format!(
            "cd {work} && sh test.sh && git diff --check && git add README.md answer.txt && git commit -m 'Fix the answer and record the coding proof' && git push -u origin {BRANCH}"
        ),
    ];
    let mut responses = vec![
        tool(
            0,
            "bash",
            &json!({"command":commands[0], "yield_seconds":120}),
        ),
        tool(1, "read", &json!({"path":format!("{work}/AGENTS.md")})),
        tool(
            2,
            "bash",
            &json!({"command":commands[1], "yield_seconds":120}),
        ),
        tool(3, "checkpoint", &json!({})),
        tool(
            4,
            "bash",
            &json!({"command":commands[2], "yield_seconds":120,"timeout_ms":180_000}),
        ),
        tool(
            5,
            "bash",
            &json!({"command":commands[3], "yield_seconds":120}),
        ),
        tool(
            6,
            "edit",
            &json!({"path":format!("{work}/README.md"),"old_string":"# Coding proof\n","new_string":format!("# Coding proof\n{LINE}\n")}),
        ),
        tool(
            7,
            "edit",
            &json!({"path":format!("{work}/answer.txt"),"old_string":"41","new_string":"42"}),
        ),
        tool(
            8,
            "bash",
            &json!({"command":commands[4], "yield_seconds":120}),
        ),
        json!({"parts":[{"text":{"text":DONE}}],"stop_reason":"end_turn","usage":{"input_tokens":0,"cached_input_tokens":0,"output_tokens":0,"reasoning_output_tokens":0,"total_tokens":0}}),
    ];
    let readme = tool(10, "read", &json!({"path":format!("{work}/README.md")}));
    responses[1]["parts"]
        .as_array_mut()
        .unwrap()
        .push(readme["parts"][0].clone());
    responses
}

fn verify_remote(root: &Path) -> Result<()> {
    let remote = root.join("remote.git");
    ensure!(
        git(&remote, &["show", &format!("{BRANCH}:README.md")])?
            == format!("# Coding proof\n{LINE}\n"),
        "pushed README differs"
    );
    ensure!(
        git(&remote, &["show", &format!("{BRANCH}:answer.txt")])? == "42\n",
        "fix was not pushed"
    );
    ensure!(
        git(
            &remote,
            &["rev-list", "--count", &format!("master..{BRANCH}")]
        )?
        .trim()
            == "1",
        "expected one proof commit"
    );
    ensure!(
        git(&remote, &["show", "master:README.md"])? == "# Coding proof\n",
        "master changed"
    );
    Ok(())
}

async fn serve_remote(root: &Path) -> Result<(tokio::process::Child, u16)> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    // The node uses the host network; this local daemon is never internet-facing.
    let mut daemon = tokio::process::Command::new("git")
        .args([
            "daemon",
            "--reuseaddr",
            "--export-all",
            "--enable=receive-pack",
            "--listen=127.0.0.1",
        ])
        .arg(format!("--port={port}"))
        .arg(format!("--base-path={}", root.display()))
        .arg(root)
        .kill_on_drop(true)
        .spawn()?;
    timeout(Duration::from_secs(5), async {
        while tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_err()
        {
            ensure!(daemon.try_wait()?.is_none(), "git daemon exited");
            sleep(Duration::from_millis(25)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok((daemon, port))
}

pub async fn exercise(f: &mut Fixture) -> Result<()> {
    remote(f.files.path())?;
    let (mut daemon, port) = serve_remote(f.files.path()).await?;
    let responses: serde_json::Map<_, _> =
        script(&format!("git://127.0.0.1:{port}/remote.git"), WORK)
            .into_iter()
            .enumerate()
            .map(|(i, value)| (i.to_string(), value))
            .collect();
    std::fs::write(
        f.files.path().join("script.json"),
        serde_json::to_vec(&json!({"responses":responses}))?,
    )?;
    f.processes
        .iter_mut()
        .find(|p| p.kind == Kind::Gateway)
        .context("gateway missing")?
        .restart()
        .await?;
    let agent = f
        .store
        .create_agent(
            "coding-proof",
            "chaos:test",
            "Coding proof",
            jiff::Timestamp::now(),
        )
        .await?;
    let (session, _) = f
        .store
        .open_main_session(agent.agent_id, jiff::Timestamp::now())
        .await?;
    f.sessions.push(session);
    let message = Message { id: MessageId::from_ulid(ulid::Ulid::generate()), role: MessageRole::User,
        parts: vec![Part::Text { text: "Clone the local repository, fix its failing test, add the dated README line, commit and push the proof branch.".into() }] };
    let head = f.store.append_user_message(session, 0, &message).await?;
    let started = Instant::now();
    f.bus
        .nudge(session, head, Some(message.id), Duration::ZERO, true)
        .await?;
    kill_at_marker(f, agent.agent_id).await?;
    let events = timeout(Duration::from_secs(240), async {
        loop {
            for process in &mut f.processes {
                process.check()?;
            }
            let record = f
                .store
                .fetch_session(session)
                .await?
                .context("session missing")?;
            if record.state == SessionState::Idle {
                let mut events = Vec::new();
                crate::read_through(&f.store, session, &mut events, record.head_seq).await?;
                return Ok::<_, anyhow::Error>(events);
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("coding recovery timed out")??;
    verify_events(f, &events).await?;
    verify_remote(f.files.path())?;
    daemon.kill().await?;
    tracing::info!(
        seconds = started.elapsed().as_secs_f64(),
        "coding proof passed: failing test repaired, README committed and pushed after one node kill"
    );
    Ok(())
}

async fn kill_at_marker(f: &mut Fixture, agent: swarmy_core::AgentId) -> Result<()> {
    let marker = f.files.path().join(format!(
        ".swarmy/node/bundles/{agent}/rootfs{WORK}/kill-ready"
    ));
    timeout(Duration::from_secs(180), async {
        while !marker.exists() {
            for process in &mut f.processes {
                process.check()?;
            }
            sleep(Duration::from_millis(25)).await;
        }
        f.processes
            .iter_mut()
            .find(|p| p.kind == Kind::Node)
            .context("node missing")?
            .restart()
            .await?;
        tracing::info!(%agent, "coding proof killed swarmyd once during bash after checkpoint");
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("coding kill marker timed out")?
}

async fn verify_events(f: &Fixture, events: &[Event]) -> Result<()> {
    let mut instructions = false;
    let mut exits = Vec::new();
    let mut interrupted = 0;
    let mut notices = 0;
    let mut finished = false;
    for event in events {
        match event {
            Event::InferenceRequested { request_id, .. } => {
                let job = f
                    .store
                    .get_inference_input::<swarmy_llm::InferenceJob>(*request_id)
                    .await?
                    .context("inference missing")?;
                instructions |= job.request.system_prompt.contains(RULE);
            }
            Event::InferenceCompleted { message, .. } => {
                finished |= message.parts == [Part::Text { text: DONE.into() }];
            }
            Event::MessageAppended { message, .. } if message.role == MessageRole::System => {
                notices += 1;
            }
            Event::ToolCallCompleted {
                call_id, result, ..
            } => {
                if call_id.0 == "coding-4" {
                    ensure!(
                        matches!(result, ToolResult::Error { .. }),
                        "interrupted call did not fail"
                    );
                    interrupted += 1;
                } else {
                    let ToolResult::Completed { title, output, .. } = result else {
                        anyhow::bail!("unexpected tool failure: {result:?}");
                    };
                    if title == "bash" {
                        exits.push(serde_json::from_str::<Value>(output)?["exit_code"].clone());
                    }
                }
            }
            _ => {}
        }
    }
    ensure!(
        instructions,
        "repository AGENTS.md missing from inference prompt"
    );
    ensure!(
        finished && notices == 1 && interrupted == 1,
        "missing finish or duplicate recovery: notices={notices}, interrupted={interrupted}"
    );
    ensure!(
        exits == [json!(0), json!(1), json!(0), json!(0)],
        "expected test failure followed by successful recovery and test: {exits:?}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use swarmy_llm::Provider;

    #[tokio::test]
    async fn reduced_fake_coding_push() {
        let files = tempfile::tempdir().unwrap();
        remote(files.path()).unwrap();
        let work = files.path().join("work");
        let script = script(
            files.path().join("remote.git").to_str().unwrap(),
            work.to_str().unwrap(),
        );
        let mut provider = swarmy_llm::fake::FakeProvider::default();
        provider.responses = script
            .into_iter()
            .enumerate()
            .map(|(i, v)| (i, serde_json::from_value(v).unwrap()))
            .collect();
        // CI exercises the same provider script and real git commands. Privileged
        // checkpoint rollback, tool dispatch, and node death are covered by --coding.
        for index in 0..10 {
            let request = swarmy_llm::Request {
                system_prompt: String::new(),
                messages: vec![],
                tools: vec![],
                settings: swarmy_llm::GenerationSettings::default(),
            };
            let mut stream = provider.request(request);
            while let Some(delta) = stream.next().await {
                let swarmy_llm::Delta::PartDone {
                    part: Part::ToolCall { tool, input, .. },
                    ..
                } = delta.unwrap()
                else {
                    continue;
                };
                match tool.as_str() {
                    "bash" if index == 4 => {}
                    "bash" => {
                        let output = Command::new("bash")
                            .args(["-c", input["command"].as_str().unwrap()])
                            .output()
                            .unwrap();
                        assert_eq!(
                            output.status.code(),
                            Some(i32::from(index == 2)),
                            "{}",
                            String::from_utf8_lossy(&output.stderr)
                        );
                    }
                    "read" => assert_eq!(
                        std::fs::read_to_string(input["path"].as_str().unwrap()).unwrap(),
                        if input["path"].as_str().unwrap().ends_with("AGENTS.md") {
                            RULE
                        } else {
                            "# Coding proof\n"
                        }
                    ),
                    "edit" => {
                        let path = input["path"].as_str().unwrap();
                        let before = std::fs::read_to_string(path).unwrap();
                        let old = input["old_string"].as_str().unwrap();
                        assert_eq!(before.matches(old).count(), 1);
                        std::fs::write(
                            path,
                            before.replacen(old, input["new_string"].as_str().unwrap(), 1),
                        )
                        .unwrap();
                    }
                    "checkpoint" => {}
                    _ => panic!("unexpected tool {tool}"),
                }
            }
        }
        verify_remote(files.path()).unwrap();
    }
}
