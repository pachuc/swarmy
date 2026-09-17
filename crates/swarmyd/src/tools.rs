use anyhow::{Context, Result};
use std::{sync::Arc, time::Duration};
use swarmy_bus::{Bus, WorkQueue};
use swarmy_core::{
    BashResult, LeaseOwnerId, NodeId, PlacedToolClaim, PlacementRecord, Sandbox, SandboxArguments,
    ToolJob, ToolResult, VolumeId,
};
use swarmy_sandbox::{ExecOutput, ExecRequest, RuncRuntime, SandboxRuntime};
use swarmy_store::{Store, StoreError};
use tokio::sync::mpsc;

const LEASE: Duration = Duration::from_secs(30);

pub fn spawn(
    bus: Bus,
    store: &Store,
    node: NodeId,
    hosting: &Arc<crate::hosting::Hosting>,
    settings: &swarmy_config::Settings,
) -> tokio::task::JoinHandle<Result<()>> {
    tokio::spawn(serve(
        bus,
        store.clone(),
        node,
        hosting.clone(),
        Duration::from_millis(settings.bus_ack_wait_ms),
        Duration::from_millis(settings.scheduler_resend_interval_ms),
    ))
}

async fn serve(
    bus: Bus,
    store: Store,
    node: NodeId,
    hosting: Arc<crate::hosting::Hosting>,
    ack_wait: Duration,
    resend_interval: Duration,
) -> Result<()> {
    let queue = WorkQueue::NodeTools(node);
    bus.setup(std::slice::from_ref(&queue)).await?;
    let mut messages = bus.consume::<ToolJob>(&queue).await?;
    let mut calls = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            delivery = messages.next() => {
                let message = delivery.context("node tool subscription closed")??;
                let hosting = hosting.clone();
                let store = store.clone();
                let bus = bus.clone();
                calls.spawn(async move {
                    let turn = store.request_turn_id(message.value.request_id).await?;
                    let result = tokio::select! {
                        result = hosting.call(message.value.clone()) => result,
                        result = async {
                            loop {
                                tokio::time::sleep(ack_wait / 3).await;
                                if let Err(error) = message.extend_deadline().await { break Err(anyhow::Error::from(error)); }
                            }
                        } => result,
                    };
                    match result {
                        Ok(()) => {
                            if let Some(turn) = turn {
                                bus.record_turn(&Bus::turn_event(message.value.session_id, turn,
                                    swarmy_core::TurnStage::ToolCompleted,
                                    Some(message.value.request_id))).await;
                            }
                            if let Some(session) = store.fetch_session(message.value.session_id).await?
                                && session.state == swarmy_core::SessionState::Runnable
                                && let Err(error) = bus.nudge(session.session_id, session.head_seq, turn, resend_interval, false).await
                            {
                                tracing::warn!(%error, "tool completion nudge failed; scheduler will recover");
                            }
                            message.acknowledge().await?;
                        }
                        Err(error) => {
                            tracing::warn!(%error, request_id = %message.value.request_id, "sandbox tool refused or interrupted");
                            message.negative_acknowledge(Some(Duration::from_secs(2))).await?;
                        }
                    }
                    Ok::<_, anyhow::Error>(())
                });
            }
            Some(result) = calls.join_next(), if !calls.is_empty() => { result??; }
        }
    }
}

pub async fn execute(
    store: &Store,
    runtime: &RuncRuntime,
    placement: &PlacementRecord,
    job: ToolJob,
) -> Result<()> {
    if store.tool_completed(job.request_id).await? {
        return Ok(());
    }
    // claim_placed_tool checks the live placement and dispatch epoch in the
    // same transaction as the claim; a separate validation could race anyway.
    let claim = PlacedToolClaim {
        job,
        owner: LeaseOwnerId::from_ulid(ulid::Ulid::generate()),
        placement: placement.clone(),
        expires_at: jiff::Timestamp::now().checked_add(LEASE)?,
    };
    anyhow::ensure!(
        store.claim_placed_tool(&claim).await?,
        "tool call already claimed"
    );
    let _activity = swarmy_volume::priority::ToolActivity::begin();
    tokio::select! {
        result = run(store, runtime, &claim) => result,
        result = heartbeat(store, &claim) => result,
    }
}

async fn run(store: &Store, runtime: &RuncRuntime, claim: &PlacedToolClaim) -> Result<()> {
    tracing::info!(request_id = %claim.job.request_id, epoch = claim.placement.epoch, "executing sandbox command");
    let sandbox = swarmy_core::Sandbox {
        agent_id: claim.placement.agent_id,
    };
    let result = match &claim.job.arguments {
        SandboxArguments::Checkpoint(_) => {
            let manifest_id = runtime.checkpoint(&sandbox).await?;
            completed(
                "checkpoint",
                &serde_json::json!({"manifest_id": manifest_id}),
            )
        }
        arguments => {
            let request = request(arguments, claim.placement.epoch);
            let (exit, stdout, stderr) = exec(runtime, &sandbox, request).await?;
            if exit.timed_out {
                ToolResult::Error {
                    error: format!(
                        "{} timed out; its process group was stopped. stdout: {stdout} stderr: {stderr}",
                        arguments.name()
                    ),
                }
            } else if matches!(arguments, SandboxArguments::Bash(_)) {
                let manifest_id = store
                    .get_volume(VolumeId::from_ulid(sandbox.agent_id.as_ulid()))
                    .await?
                    .context("agent volume missing")?
                    .head_manifest;
                BashResult {
                    stdout,
                    stderr,
                    exit_code: exit.exit_code,
                    timed_out: false,
                    manifest_id,
                }
                .tool_result()
            } else if exit.exit_code != 0 {
                ToolResult::Error { error: stderr }
            } else {
                completed(arguments.name(), &serde_json::from_str(&stdout)?)
            }
        }
    };
    loop {
        let head = store
            .fetch_session(claim.job.session_id)
            .await?
            .context("session missing")?
            .head_seq;
        match store.complete_placed_tool(claim, head, &result).await {
            Ok(()) => break,
            Err(StoreError::StaleSequence { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
    tracing::info!(request_id = %claim.job.request_id, "committed sandbox tool output");
    Ok(())
}

fn completed(name: &str, value: &serde_json::Value) -> ToolResult {
    ToolResult::Completed {
        title: name.into(),
        output: value.to_string(),
        metadata: std::collections::BTreeMap::new(),
    }
}

fn request(arguments: &SandboxArguments, epoch: u64) -> ExecRequest {
    if let SandboxArguments::Bash(arguments) = arguments {
        return ExecRequest {
            args: vec!["/bin/bash".into(), "-c".into(), arguments.command.clone()],
            timeout_ms: arguments.timeout_ms,
        };
    }
    let (id, command) = match arguments {
        SandboxArguments::ProcessStart(arguments) => (
            swarmy_core::ProcessId::from_ulid(ulid::Ulid::generate()).to_string(),
            arguments.command.clone(),
        ),
        SandboxArguments::ProcessLog(arguments) | SandboxArguments::ProcessStop(arguments) => {
            (arguments.process_id.to_string(), String::new())
        }
        _ => (String::new(), String::new()),
    };
    ExecRequest {
        args: vec![
            "/usr/bin/python3".into(),
            "-c".into(),
            include_str!("processes.py").into(),
            arguments.name().into(),
            epoch.to_string(),
            id,
            command,
        ],
        timeout_ms: 10_000,
    }
}

async fn exec(
    runtime: &RuncRuntime,
    sandbox: &Sandbox,
    request: ExecRequest,
) -> Result<(swarmy_core::ExecResult, String, String)> {
    let (send, mut receive) = mpsc::channel(16);
    let collect = async {
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        while let Some(output) = receive.recv().await {
            match output {
                ExecOutput::Stdout(bytes) => stdout.extend(bytes),
                ExecOutput::Stderr(bytes) => stderr.extend(bytes),
            }
        }
        (
            String::from_utf8_lossy(&stdout).into_owned(),
            String::from_utf8_lossy(&stderr).into_owned(),
        )
    };
    let (exit, (stdout, stderr)) = tokio::join!(runtime.exec(sandbox, request, send), collect);
    Ok((exit?, stdout, stderr))
}

pub async fn has_processes(runtime: &RuncRuntime, placement: &PlacementRecord) -> Result<bool> {
    let arguments = SandboxArguments::ProcessList(swarmy_core::EmptyArguments {});
    let (exit, stdout, stderr) = exec(
        runtime,
        &Sandbox {
            agent_id: placement.agent_id,
        },
        request(&arguments, placement.epoch),
    )
    .await?;
    anyhow::ensure!(
        exit.exit_code == 0 && !exit.timed_out,
        "process listing failed: {stderr}"
    );
    let records: Vec<serde_json::Value> = serde_json::from_str(&stdout)?;
    Ok(records.iter().any(|record| record["status"] == "running"))
}

async fn heartbeat(store: &Store, claim: &PlacedToolClaim) -> Result<()> {
    let mut expiry = claim.expires_at;
    loop {
        tokio::time::sleep(LEASE / 3).await;
        let next = jiff::Timestamp::now().checked_add(LEASE)?;
        let remaining = expiry
            .duration_since(jiff::Timestamp::now())
            .try_into()
            .unwrap_or(Duration::ZERO);
        tokio::time::timeout(remaining, store.renew_placed_tool(claim, next)).await??;
        expiry = next;
    }
}

#[cfg(test)]
mod tests {

    struct Processes(tempfile::TempDir);

    impl Processes {
        fn call(&self, action: &str, id: &str, command: &str, epoch: u64) -> serde_json::Value {
            let script = include_str!("processes.py").replace(
                "Path('/var/lib/swarmy/processes')",
                &format!("Path({})", serde_json::json!(self.0.path())),
            );
            let output = std::process::Command::new("python3")
                .args(["-c", &script, action, &epoch.to_string(), id, command])
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            serde_json::from_slice(&output.stdout).unwrap()
        }
    }

    impl Drop for Processes {
        fn drop(&mut self) {
            for entry in std::fs::read_dir(self.0.path()).unwrap().flatten() {
                if let Ok(bytes) = std::fs::read(entry.path().join("record.json")) {
                    let record: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                    let _ = std::process::Command::new("kill")
                        .args(["-KILL", "--", &format!("-{}", record["pid"])])
                        .stdout(std::process::Stdio::null())
                        .stderr(std::process::Stdio::null())
                        .status();
                }
            }
        }
    }

    #[test]
    fn process_records_outlive_the_launching_exec_and_reject_stale_pids() {
        let processes = Processes(tempfile::tempdir().unwrap());
        let id = swarmy_core::ProcessId::from_ulid(ulid::Ulid::generate()).to_string();
        processes.call("process_start", &id, "echo ready; sleep 300", 1);
        let listed = processes.call("process_list", "", "", 1);
        assert_eq!(listed[0]["status"], "running");
        let restarted = processes.call("process_list", "", "", 2);
        assert_eq!(restarted[0]["status"], "restarted");
        let path = processes.0.path().join(&id).join("record.json");
        let mut record: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let ticks = record["start_ticks"].clone();
        record["start_ticks"] = serde_json::json!("impossible start time");
        std::fs::write(&path, serde_json::to_vec(&record).unwrap()).unwrap();
        assert_eq!(
            processes.call("process_list", "", "", 1)[0]["status"],
            "exited"
        );
        processes.call("process_stop", &id, "", 1);
        record["start_ticks"] = ticks;
        std::fs::write(&path, serde_json::to_vec(&record).unwrap()).unwrap();
        assert_eq!(
            processes.call("process_list", "", "", 1)[0]["status"],
            "running"
        );
        processes.call("process_stop", &id, "", 1);
        assert_eq!(
            processes.call("process_list", "", "", 1)[0]["status"],
            "exited"
        );
    }
}
