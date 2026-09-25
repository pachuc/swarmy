use anyhow::{Context, Result};
use base64::Engine as _;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use swarmy_bus::{Bus, WorkQueue};
use swarmy_core::{
    LeaseOwnerId, NodeId, PlacedToolClaim, PlacementRecord, Sandbox, SandboxArguments, ToolJob,
    ToolResult, VolumeId,
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
                    // One lookup per tool call; the turn travels with the job
                    // so the execution path needs no further lookups.
                    let turn = store.request_turn_id(message.value.request_id).await?;
                    let result = tokio::select! {
                        result = hosting.call(message.value.clone(), turn) => result,
                        result = async {
                            loop {
                                tokio::time::sleep(ack_wait / 3).await;
                                if let Err(error) = message.extend_deadline().await { break Err(anyhow::Error::from(error)); }
                            }
                        } => result,
                    };
                    match result {
                        Ok(()) => {
                            // The completion stage lands in the same batch as
                            // the tool result inside `run`; only the live bus
                            // event is emitted here, never a second store write.
                            if let Some(turn) = turn {
                                bus.record_turn(&Bus::turn_event(message.value.session_id, turn,
                                    swarmy_core::TurnStage::ToolCompleted, Some(message.value.request_id))).await;
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
    runtime: Arc<RuncRuntime>,
    placement: &PlacementRecord,
    job: ToolJob,
    turn: Option<swarmy_core::MessageId>,
    needs_computer_sample: bool,
) -> Result<()> {
    store.ensure_session_computer(job.session_id).await?;
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
        result = run(store, runtime, &claim, turn, needs_computer_sample) => result,
        result = heartbeat(store, &claim) => result,
    }
}

async fn run(
    store: &Store,
    runtime: Arc<RuncRuntime>,
    claim: &PlacedToolClaim,
    turn: Option<swarmy_core::MessageId>,
    needs_computer_sample: bool,
) -> Result<()> {
    tracing::info!(request_id = %claim.job.request_id, epoch = claim.placement.epoch, "executing sandbox command");
    let sandbox = swarmy_core::Sandbox {
        agent_id: claim.placement.agent_id,
    };
    if claim.job.arguments.is_display_tool() {
        let agent = store
            .get_agent(claim.placement.agent_id)
            .await?
            .context("agent missing")?;
        anyhow::ensure!(
            store.image_display(&agent.image).await?,
            "display tool requires a display image"
        );
    }
    let outcome = run_command(store, &runtime, &sandbox, claim).await?;
    let mut result = outcome.result;
    if let ToolResult::Completed { metadata, .. } = &mut result
        && let Some(encoded) = metadata.remove("image_base64")
    {
        let encoded = encoded.as_str().context("image payload must be base64")?;
        let bytes = base64::engine::general_purpose::STANDARD.decode(encoded)?;
        let key = store.put_tool_blob(bytes).await?;
        metadata.insert("image_object_key".into(), serde_json::json!(key));
    }
    if store.interrupt_requested(claim.job.session_id).await? {
        stop_result_process(&runtime, &sandbox, claim.placement.epoch, &result).await?;
    }
    // A single call usually remains at its request head. Concurrent calls and
    // rebuild notices return the actual head from the same fenced transaction.
    let mut head = claim.job.step;
    loop {
        match store.complete_placed_tool(claim, head, &result).await {
            Ok(()) => break,
            Err(StoreError::StaleSequence { actual, .. }) => head = actual,
            Err(error) => return Err(error.into()),
        }
    }
    // The tool result and completion stage land right after the fenced
    // commit so the worker sees the completion without waiting for the
    // volume stat round trip. The first-tool computer re-sample follows in
    // its own transaction from a spawned task; the store keeps the first
    // re-sample per turn. Queueing versus execution stays distinguishable
    // through `started_ns` and `process_wall_ms`.
    observe_tool_completion(
        store,
        runtime,
        claim,
        turn,
        needs_computer_sample,
        &result,
        &outcome.summary,
    );
    tracing::info!(request_id = %claim.job.request_id, "committed sandbox tool output");
    Ok(())
}

/// Wall-clock start and process timing for one sandbox execution.
struct ToolTiming {
    exit_status: Option<i32>,
    started_ns: Option<i64>,
    process_wall_ms: Option<f64>,
}

struct CommandOutcome {
    result: ToolResult,
    summary: ToolTiming,
}

async fn run_command(
    store: &Store,
    runtime: &RuncRuntime,
    sandbox: &Sandbox,
    claim: &PlacedToolClaim,
) -> Result<CommandOutcome> {
    if let SandboxArguments::Checkpoint(_) = &claim.job.arguments {
        let manifest_id = runtime.checkpoint(sandbox).await?;
        return Ok(CommandOutcome {
            result: completed(
                "checkpoint",
                &serde_json::json!({"manifest_id": manifest_id}),
            ),
            summary: ToolTiming {
                exit_status: None,
                started_ns: None,
                process_wall_ms: None,
            },
        });
    }
    let arguments = &claim.job.arguments;
    let request = request(arguments, claim.placement.epoch, &claim.job.call_id.0);
    // The start time travels in memory to the completion batch below;
    // no separate start transaction is written.
    let started_wall = jiff::Timestamp::now();
    let started_ns = i64::try_from(started_wall.as_nanosecond()).ok();
    let started = Instant::now();
    let (exit, stdout, stderr) = exec(runtime, sandbox, request).await?;
    let summary = ToolTiming {
        exit_status: Some(exit.exit_code),
        started_ns,
        // Wall time is approximate; millisecond display precision is sufficient.
        process_wall_ms: Some(started.elapsed().as_secs_f64() * 1_000.0),
    };
    let result = if exit.timed_out {
        ToolResult::Error {
            error: format!(
                "{} helper timed out; managed processes may still be running. stdout: {stdout} stderr: {stderr}",
                arguments.name()
            ),
        }
    } else if exit.exit_code != 0 {
        ToolResult::Error {
            error: if exit.exit_code == 137 {
                format!(
                    "sandbox process killed (memory limit may have been exceeded); stderr: {stderr}"
                )
            } else {
                stderr
            },
        }
    } else if arguments.is_file_tool() {
        serde_json::from_str(&stdout)?
    } else {
        let mut value: serde_json::Value = serde_json::from_str(&stdout)?;
        if arguments.is_display_tool() {
            display_result(arguments.name(), value)?
        } else if matches!(arguments, SandboxArguments::Bash(_)) {
            value["manifest_id"] = serde_json::json!(
                store
                    .get_volume(VolumeId::from_ulid(sandbox.agent_id.as_ulid()))
                    .await?
                    .context("agent volume missing")?
                    .head_manifest
            );
            let metadata = serde_json::from_value(value.clone())?;
            ToolResult::Completed {
                title: "bash".into(),
                output: value.to_string(),
                metadata,
            }
        } else {
            completed(arguments.name(), &value)
        }
    };
    Ok(CommandOutcome { result, summary })
}

// Fetch histogram reads are approximate, so floating-point display precision is sufficient.
#[allow(clippy::cast_precision_loss)]
fn observe_tool_completion(
    store: &Store,
    runtime: Arc<RuncRuntime>,
    claim: &PlacedToolClaim,
    turn: Option<swarmy_core::MessageId>,
    needs_computer_sample: bool,
    result: &ToolResult,
    timing: &ToolTiming,
) {
    let Some(turn) = turn else { return };
    let (output_bytes, exit_status) = match result {
        ToolResult::Completed {
            output, metadata, ..
        } => (
            output.len() as u64,
            metadata
                .get("exit_code")
                .and_then(serde_json::Value::as_i64)
                .and_then(|code| i32::try_from(code).ok())
                .or(timing.exit_status),
        ),
        ToolResult::Error { error } => (error.len() as u64, timing.exit_status),
    };
    let tool = swarmy_api_types::ToolMetric {
        request_id: claim.job.request_id.to_string(),
        name: claim.job.arguments.name().into(),
        started_ns: timing.started_ns,
        exit_status,
        output_bytes: Some(output_bytes),
        process_wall_ms: timing.process_wall_ms,
        ..Default::default()
    };
    let completed = swarmy_bus::Bus::turn_event(
        claim.job.session_id,
        turn,
        swarmy_core::TurnStage::ToolCompleted,
        Some(claim.job.request_id),
    );
    // The result lands before the volume stat round trip so `run` returns
    // without waiting on the attach server socket.
    store.observe_turn_metrics(
        claim.job.session_id,
        turn,
        swarmy_store::completion_patches(tool, None, completed),
    );
    // Lazy hydration during the first command is invisible in the boot
    // sample, so re-sample after the result is committed. Only the first
    // completion per turn carries it; the store keeps the first re-sample
    // per turn as well.
    if needs_computer_sample {
        let store = store.clone();
        let session = claim.job.session_id;
        let volume = VolumeId::from_ulid(claim.placement.agent_id.as_ulid());
        tokio::spawn(async move {
            if let Ok(stats) = runtime.volume_stats(volume).await {
                store.observe_turn_metric(
                    session,
                    turn,
                    swarmy_store::MetricPatch::Computer(swarmy_api_types::ComputerMetric {
                        first_tool_chunks_fetched: Some(stats.fetched_chunks),
                        first_tool_bytes_fetched: Some(stats.fetched_bytes),
                        first_tool_fetch_p50_ms: stats.fetch_p50_us.map(|us| us as f64 / 1_000.0),
                        first_tool_fetch_p95_ms: stats.fetch_p95_us.map(|us| us as f64 / 1_000.0),
                        ..Default::default()
                    }),
                );
            }
        });
    }
}

async fn stop_result_process(
    runtime: &RuncRuntime,
    sandbox: &Sandbox,
    epoch: u64,
    result: &ToolResult,
) -> Result<()> {
    let ToolResult::Completed { title, output, .. } = result else {
        return Ok(());
    };
    if title != "bash" && title != "process_start" {
        return Ok(());
    }
    let value: serde_json::Value = serde_json::from_str(output)?;
    if value["backgrounded"] != true && title != "process_start" {
        return Ok(());
    }
    let Some(process_id) = value["process_id"].as_str() else {
        return Ok(());
    };
    let arguments = SandboxArguments::ProcessStop(serde_json::from_value(
        serde_json::json!({"process_id": process_id}),
    )?);
    let (exit, _, stderr) = exec(runtime, sandbox, request(&arguments, epoch, "")).await?;
    anyhow::ensure!(
        exit.exit_code == 0 && !exit.timed_out,
        "process stop failed: {stderr}"
    );
    Ok(())
}

fn display_result(name: &str, mut value: serde_json::Value) -> Result<ToolResult> {
    let mut metadata = std::collections::BTreeMap::new();
    if let Some(encoded) = value
        .as_object_mut()
        .context("display result is not an object")?
        .remove("image_base64")
    {
        metadata.insert("image_base64".into(), encoded);
        metadata.insert("image_media_type".into(), serde_json::json!("image/png"));
    }
    Ok(ToolResult::Completed {
        title: name.into(),
        output: value["output"].as_str().unwrap_or("Done").into(),
        metadata,
    })
}

fn completed(name: &str, value: &serde_json::Value) -> ToolResult {
    ToolResult::Completed {
        title: name.into(),
        output: value.to_string(),
        metadata: std::collections::BTreeMap::new(),
    }
}

fn request(arguments: &SandboxArguments, epoch: u64, call_id: &str) -> ExecRequest {
    if arguments.is_file_tool() {
        // Install the embedded version on the agent disk, including older images.
        // JSON travels on stdin so large writes do not hit the argv size limit.
        return ExecRequest {
            args: vec![
                "/usr/bin/python3".into(),
                "-c".into(),
                format!(
                    "import pathlib, runpy; p = pathlib.Path('/usr/local/lib/swarmy/files.py'); p.parent.mkdir(parents=True, exist_ok=True); p.write_text({}); runpy.run_path(str(p), run_name='__main__')",
                    serde_json::json!(include_str!("files.py"))
                ),
                arguments.name().into(),
            ],
            stdin: arguments.parameters().to_string().into_bytes(),
            timeout_ms: 120_000,
        };
    }
    if arguments.is_display_tool() {
        return ExecRequest {
            args: vec![
                "/usr/bin/python3".into(),
                "/usr/local/libexec/swarmy-browser".into(),
                arguments.name().into(),
            ],
            stdin: arguments.parameters().to_string().into_bytes(),
            timeout_ms: 40_000,
        };
    }
    if let SandboxArguments::WebFetch(arguments) = arguments {
        return ExecRequest {
            args: vec![
                "/usr/bin/python3".into(),
                "-c".into(),
                include_str!("web_fetch.py").into(),
                arguments.url.clone(),
            ],
            timeout_ms: 35_000,
            stdin: Vec::new(),
        };
    }
    let new_id = || swarmy_core::ProcessId::from_ulid(ulid::Ulid::generate()).to_string();
    let (id, command) = match arguments {
        SandboxArguments::Bash(arguments) => (new_id(), arguments.command.clone()),
        SandboxArguments::ProcessStart(arguments) => (new_id(), arguments.command.clone()),
        SandboxArguments::ProcessLog(arguments) | SandboxArguments::ProcessStop(arguments) => {
            (arguments.process_id.to_string(), String::new())
        }
        SandboxArguments::WriteStdin(arguments) => {
            (arguments.process_id.to_string(), arguments.text.clone())
        }
        _ => (String::new(), String::new()),
    };
    let mut options = arguments.parameters();
    options["call_id"] = serde_json::json!(call_id);
    let wait_ms = match arguments {
        SandboxArguments::Bash(arguments) => {
            (arguments.yield_seconds * 1000).min(arguments.timeout_ms)
        }
        _ => 0,
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
            options.to_string(),
        ],
        // The helper enforces the command's wait limit. This guard is only for
        // a stalled helper; detached managed commands remain in their own group.
        timeout_ms: wait_ms + 10_000,
        stdin: Vec::new(),
    }
}

pub(crate) async fn exec(
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
        request(&arguments, placement.epoch, ""),
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

#[cfg(test)]
mod display_tests {
    use super::display_result;
    use swarmy_core::ToolResult;

    #[test]
    fn screenshot_payload_becomes_image_metadata() {
        let result = display_result(
            "browser_screenshot",
            serde_json::json!({
                "output": "PNG screenshot", "image_base64": "iVBORw0KGgo="
            }),
        )
        .unwrap();
        let ToolResult::Completed {
            output, metadata, ..
        } = result
        else {
            panic!("expected screenshot result");
        };
        assert_eq!(output, "PNG screenshot");
        assert_eq!(metadata["image_media_type"], "image/png");
        assert_eq!(metadata["image_base64"], "iVBORw0KGgo=");
    }
}
