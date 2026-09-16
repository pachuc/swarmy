//! Deterministic scenarios through the gateway, worker, and real node services.
use crate::{
    Fixture, check,
    process::{Kind, Process},
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use swarmy_core::{
    Event, Message, MessageId, MessageRole, NodeId, Part, PlacementChangeReason, SessionId,
    SessionState, ToolResult, VolumeId,
};
use tokio::time::{Instant, sleep, timeout};

struct Nodes {
    roots: Vec<PathBuf>,
    processes: Vec<Process>,
    driver: Option<PathBuf>,
}

impl Drop for Nodes {
    fn drop(&mut self) {
        if let Some(driver) = &self.driver {
            let _ = std::process::Command::new(driver).arg("stop").status();
        }
        for process in &mut self.processes {
            process.kill_now();
        }
        for root in &self.roots {
            crate::disk::cleanup(&root.join(".swarmy/node"));
        }
    }
}

pub async fn exercise(fixture: &mut Fixture, binaries: &Path, driver: Option<&Path>) -> Result<()> {
    let mut nodes = Nodes {
        roots: Vec::new(),
        processes: Vec::new(),
        driver: driver.map(Path::to_owned),
    };
    for index in 0..2 {
        let root = fixture.files.path().join(format!("host-{index}"));
        std::fs::create_dir_all(root.join(".swarmy"))?;
        std::fs::write(root.join(".swarmy/config.toml"), "")?;
        let mut environment = fixture.environment.clone();
        environment.extend([
            ("SWARMY_PLACEMENT_LEASE_SECONDS".into(), "3".into()),
            ("SWARMY_SANDBOX_IDLE_SECONDS".into(), "3".into()),
            (
                "SWARMY_NODE_ID".into(),
                NodeId::from_ulid(ulid::Ulid::from_parts(1, index + 1))
                    .to_string()
                    .into(),
            ),
        ]);
        nodes.roots.push(root.clone());
        if index == 0
            && let Some(driver) = driver
        {
            ensure!(
                tokio::process::Command::new(driver)
                    .arg("start")
                    .envs(environment.iter().cloned())
                    .status()
                    .await?
                    .success(),
                "remote node startup failed"
            );
            continue;
        }
        nodes.processes.push(Process::start(
            Kind::Node,
            usize::try_from(index)?,
            binaries,
            &root,
            &environment,
        )?);
    }
    let result = scenarios(fixture, &mut nodes).await;
    for process in &mut nodes.processes {
        process.stop().await?;
    }
    result
}

async fn scenarios(f: &mut Fixture, nodes: &mut Nodes) -> Result<()> {
    let first = f.sessions[0];
    let second = f.sessions[1];
    let agent = f
        .store
        .fetch_session(first)
        .await?
        .context("session missing")?
        .agent_id;
    let volume = VolumeId::from_ulid(agent.as_ulid());
    timeout(Duration::from_secs(30), async {
        loop {
            if f.store
                .get_node(NodeId::from_ulid(ulid::Ulid::from_parts(1, 1)))
                .await?
                .is_some()
                && f.store
                    .get_node(NodeId::from_ulid(ulid::Ulid::from_parts(1, 2)))
                    .await?
                    .is_some()
            {
                return Ok::<_, anyhow::Error>(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;
    let (placement, snapshot) = shared(f, agent, volume, first, second).await?;
    // Restart removes orphan containers before another node takes over. The
    // durable placement and writer leases still have to expire normally.
    let index = usize::try_from(placement.node_id.as_ulid().random() - 1)?;
    if let Some(driver) = &nodes.driver {
        ensure!(index == 0, "expected initial placement on remote node");
        bash(
            f,
            second,
            "dd if=/dev/urandom of=/root/snapshot-dirty bs=1M count=256 status=none && sync",
        )
        .await?;
        ensure!(
            tokio::process::Command::new(driver)
                .arg("kill")
                .arg(agent.to_string())
                .status()
                .await?
                .success(),
            "remote node kill failed"
        );
        ensure!(
            f.store
                .get_volume(volume)
                .await?
                .context("volume missing")?
                .head_manifest
                == snapshot,
            "interrupted snapshot advanced the head"
        );
    } else {
        nodes.processes[index].restart().await?;
    }
    let start = Instant::now();
    wait_for_dead_writer(f, agent, volume).await?;
    bash(f, second, "test $(cat /root/persistent) = durable && test $(cat /root/shared) = shared && test ! -e /root/uncommitted && ! curl --max-time 1 -fsS http://127.0.0.1:18765/ >/dev/null").await?;
    let current = f
        .store
        .get_by_agent(agent)
        .await?
        .context("rebuilt placement missing")?;
    ensure!(
        current.node_id != placement.node_id && current.epoch > placement.epoch,
        "computer did not move to the other node"
    );
    ensure!(
        current.last_change_reason == PlacementChangeReason::Failure,
        "wrong rebuild reason"
    );
    let listed = invoke(f, second, "process_list", json!({})).await?;
    ensure!(
        listed
            .as_array()
            .context("process list missing")?
            .iter()
            .all(|p| p["status"] != "running"),
        "process survived rebuild"
    );
    notice(f, snapshot, PlacementChangeReason::Failure).await?;
    tracing::info!(seconds = start.elapsed().as_secs_f64(), %snapshot, "persistent node kill with background server passed; time includes lease expiry");
    eviction(f, agent, volume, first, second).await?;
    rehydration(f, nodes, agent, second).await
}

async fn wait_for_dead_writer(
    f: &Fixture,
    agent: swarmy_core::AgentId,
    volume: VolumeId,
) -> Result<()> {
    timeout(Duration::from_secs(90), async {
        loop {
            let now = jiff::Timestamp::now();
            let disk = f
                .store
                .get_volume(volume)
                .await?
                .context("volume missing")?;
            let placed = f
                .store
                .get_by_agent(agent)
                .await?
                .context("placement missing")?;
            if placed.expires_at <= now
                && disk
                    .writer_lease
                    .is_none_or(|lease| lease.expires_at <= now)
            {
                return Ok::<_, anyhow::Error>(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("dead node leases did not expire")?
}

async fn eviction(
    f: &mut Fixture,
    agent: swarmy_core::AgentId,
    volume: VolumeId,
    first: SessionId,
    second: SessionId,
) -> Result<()> {
    bash(f, first, "echo eviction > /root/eviction").await?;
    timeout(Duration::from_secs(30), async {
        while f.store.get_by_agent(agent).await?.is_some() {
            sleep(Duration::from_millis(50)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("idle eviction timed out")??;
    let final_head = f
        .store
        .get_volume(volume)
        .await?
        .context("volume missing")?;
    ensure!(
        final_head.writer_lease.is_none(),
        "eviction retained writer lease"
    );
    bash(f, second, "test $(cat /root/eviction) = eviction").await?;
    ensure!(
        f.store
            .get_by_agent(agent)
            .await?
            .context("evicted computer did not rebuild")?
            .last_change_reason
            == PlacementChangeReason::Eviction,
        "wrong eviction reason"
    );
    notice(f, final_head.head_manifest, PlacementChangeReason::Eviction).await?;
    tracing::info!("persistent idle eviction and durable notice passed");
    Ok(())
}

async fn shared(
    f: &mut Fixture,
    agent: swarmy_core::AgentId,
    volume: VolumeId,
    first: SessionId,
    second: SessionId,
) -> Result<(swarmy_core::PlacementRecord, swarmy_core::ManifestId)> {
    let server = invoke(
        f,
        first,
        "process_start",
        json!({"command":"exec python3 -u -m http.server 18765 --bind 127.0.0.1"}),
    )
    .await?;
    bash(f, first, "echo durable > /root/persistent; curl --retry 5 --retry-connrefused --retry-delay 1 -fsS http://127.0.0.1:18765/ >/dev/null").await?;
    bash(
        f,
        second,
        "test $(cat /root/persistent) = durable && echo shared > /root/shared",
    )
    .await?;
    bash(f, first, "test $(cat /root/shared) = shared").await?;
    let listed = invoke(f, second, "process_list", json!({})).await?;
    ensure!(
        listed
            .as_array()
            .context("process list missing")?
            .iter()
            .any(|p| p["process_id"] == server["process_id"] && p["status"] == "running"),
        "second session did not see the server"
    );
    let placement = f
        .store
        .get_by_agent(agent)
        .await?
        .context("placement missing")?;
    sleep(Duration::from_secs(4)).await;
    ensure!(
        f.store
            .get_by_agent(agent)
            .await?
            .context("managed server was evicted")?
            .epoch
            == placement.epoch,
        "managed server did not prevent eviction"
    );
    tracing::info!(%agent, "persistent shared sessions and managed process idle protection passed");
    let checkpoint = invoke(f, first, "checkpoint", json!({})).await?;
    let snapshot = f
        .store
        .get_volume(volume)
        .await?
        .context("volume missing")?
        .head_manifest;
    ensure!(
        checkpoint["manifest_id"] == json!(snapshot),
        "checkpoint acknowledgement differs from head"
    );
    bash(f, second, "echo lost > /root/uncommitted").await?;
    Ok((placement, snapshot))
}

async fn rehydration(
    f: &mut Fixture,
    nodes: &Nodes,
    agent: swarmy_core::AgentId,
    session: SessionId,
) -> Result<()> {
    for cache in ["cold", "warm"] {
        for sample in 0..2 {
            timeout(Duration::from_secs(30), async {
                while f.store.get_by_agent(agent).await?.is_some() {
                    sleep(Duration::from_millis(50)).await;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
            if cache == "cold" {
                for root in &nodes.roots {
                    let path = root.join(".swarmy/volumes/cache");
                    if path.exists() {
                        std::fs::remove_dir_all(path)?;
                    }
                }
            }
            ensure!(
                tokio::process::Command::new("sync")
                    .status()
                    .await?
                    .success(),
                "sync failed"
            );
            std::fs::write("/proc/sys/vm/drop_caches", "3\n")?;
            let start = Instant::now();
            bash(
                f,
                session,
                "test $(cat /root/persistent) = durable && test $(cat /root/eviction) = eviction",
            )
            .await?;
            tracing::info!(
                cache,
                sample,
                seconds = start.elapsed().as_secs_f64(),
                "persistent routed rehydration sample; includes gateway restart and inference"
            );
        }
    }
    Ok(())
}

async fn notice(
    f: &Fixture,
    snapshot: swarmy_core::ManifestId,
    reason: PlacementChangeReason,
) -> Result<()> {
    let agent = f
        .store
        .fetch_session(f.sessions[0])
        .await?
        .context("session missing")?
        .agent_id;
    let placement = f
        .store
        .get_by_agent(agent)
        .await?
        .context("placement missing")?;
    let timestamp =
        jiff::Timestamp::from_millisecond(i64::try_from(snapshot.as_ulid().timestamp_ms())?)?;
    let expected =
        swarmy_core::computer_rebuilt_message(reason, timestamp, placement.last_changed_at)
            .context("notice missing")?;
    let mut count = 0;
    for id in &f.sessions {
        let session = f
            .store
            .fetch_session(*id)
            .await?
            .context("session missing")?;
        let mut events = Vec::new();
        crate::read_through(&f.store, *id, &mut events, session.head_seq).await?;
        count += events.iter().filter(|e| matches!(e, Event::MessageAppended {message, ..} if message.role == MessageRole::System && message.parts == [Part::Text {text: expected.clone()}])).count();
    }
    ensure!(
        count == 1,
        "expected one durable notice with exact snapshot time, got {count}: {expected}"
    );
    Ok(())
}

async fn bash(f: &mut Fixture, session: SessionId, command: &str) -> Result<Value> {
    let result = invoke(f, session, "bash", json!({"command":command})).await?;
    ensure!(result["exit_code"] == 0, "bash assertion failed: {result}");
    Ok(result)
}

async fn invoke(f: &mut Fixture, session: SessionId, tool: &str, input: Value) -> Result<Value> {
    let usage = json!({"input_tokens":0,"cached_input_tokens":0,"output_tokens":0,"reasoning_output_tokens":0,"total_tokens":0});
    std::fs::write(
        f.files.path().join("script.json"),
        serde_json::to_vec(&json!({"responses": {
            "0": {"parts":[{"tool_call":{"call_id":"persistent", "tool":tool, "input":input}}], "stop_reason":"tool_calls", "usage":usage},
            "1": {"parts":[{"text":{"text":check::ANSWER}}], "stop_reason":"end_turn", "usage":usage}
        }}))?,
    )?;
    f.processes
        .iter_mut()
        .find(|p| p.kind == Kind::Gateway)
        .context("gateway missing")?
        .restart()
        .await?;
    let before = f
        .store
        .fetch_session(session)
        .await?
        .context("session missing")?
        .head_seq;
    f.store
        .append_events(
            session,
            before,
            &[Event::MessageAppended {
                seq: 0,
                message: Message {
                    id: MessageId::from_ulid(ulid::Ulid::generate()),
                    role: MessageRole::User,
                    parts: vec![Part::Text {
                        text: format!("Run {tool}"),
                    }],
                },
            }],
        )
        .await?;
    f.bus.request_wake(session, Duration::from_secs(3)).await?;
    timeout(Duration::from_secs(180), async {
        loop {
            let record = f
                .store
                .fetch_session(session)
                .await?
                .context("session missing")?;
            if record.state == SessionState::Idle && record.head_seq > before + 1 {
                let mut events = Vec::new();
                crate::read_through(&f.store, session, &mut events, record.head_seq).await?;
                check::log(session, &events)?;
                let results: Vec<_> = events
                    .iter()
                    .filter(|e| e.seq() > before)
                    .filter_map(|e| match e {
                        Event::ToolCallCompleted { result, .. } => Some(result),
                        _ => None,
                    })
                    .collect();
                ensure!(
                    results.len() == 1,
                    "expected one tool completion: {events:?}"
                );
                let ToolResult::Completed { output, .. } = results[0] else {
                    anyhow::bail!("tool failed: {:?}", results[0]);
                };
                return Ok(serde_json::from_str(output)?);
            }
            for process in &mut f.processes {
                process.check()?;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .context("persistent tool timed out")?
}
