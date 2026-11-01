use crate::{
    heads::Heads,
    keeper::{self, Answered, ClaimHold, Keeper},
};
use anyhow::{Context, Result};
use base64::Engine as _;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use swarmy_bus::{Bus, WorkMessage, WorkQueue};
use swarmy_core::{
    LeaseOwnerId, NodeId, PlacedToolClaim, PlacementRecord, ProcessListArguments, Sandbox,
    SandboxArguments, ToolJob, ToolResult, VolumeId,
};
use swarmy_node_protocol::{
    ClientError, FenceKind, NodeClient, NodeRefusal, ToolClaimRequest, ToolCompleteRequest,
};
use swarmy_sandbox::{ExecOutput, ExecRequest, RuncRuntime};
use tokio::sync::mpsc;
const NODE_TOOL_BATCH: std::num::NonZeroUsize =
    std::num::NonZeroUsize::new(16).expect("16 is not zero");

/// Extra time for sandbox exec startup (cold image reads) and helper startup, not the command.
const EXEC_STARTUP_ALLOWANCE_MS: u64 = 60_000;

pub(crate) fn spawn(
    bus: Bus,
    node: NodeId,
    hosting: &Arc<crate::hosting::Hosting>,
    settings: &swarmy_config::Settings,
) -> tokio::task::JoinHandle<Result<()>> {
    tokio::spawn(serve(bus, node, hosting.clone(), settings.bus.ack_wait_ms))
}

async fn serve(
    bus: Bus,
    node: NodeId,
    hosting: Arc<crate::hosting::Hosting>,
    ack_wait: Duration,
) -> Result<()> {
    let queue = WorkQueue::NodeTools(node);
    bus.setup(std::slice::from_ref(&queue)).await?;
    let mut messages = bus
        .consume_batched::<ToolJob>(&queue, NODE_TOOL_BATCH)
        .await?;
    let mut calls = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            delivery = messages.next() => {
                let message = match delivery {
                    None => return Err(anyhow::anyhow!("node tool subscription closed")),
                    Some(Err(error)) => {
                        tracing::warn!(error = %swarmy_core::error_chain(&error), "node tool delivery failed");
                        continue;
                    }
                    Some(Ok(message)) => message,
                };
                let hosting = hosting.clone();
                calls.spawn(async move {
                    let outcome = handle_call(&message, &hosting, ack_wait).await;
                    settle(&message, outcome).await;
                });
            }
            Some(joined) = calls.join_next(), if !calls.is_empty() => {
                if let Err(error) = joined {
                    tracing::error!(error = %swarmy_core::error_chain(&error), "tool call task panicked; NATS redelivers after ack wait");
                }
            }
        }
    }
}

/// Run one tool call without acknowledging it. Every error returns to `settle`,
/// which releases the message for redelivery.
fn wall_ns() -> Option<i64> {
    i64::try_from(jiff::Timestamp::now().as_nanosecond()).ok()
}

pub(crate) struct CallMetrics {
    pub(crate) turn: Option<swarmy_core::MessageId>,
    pub(crate) needs_computer_sample: bool,
    pub(crate) delivered_ns: Option<i64>,
    pub(crate) claimed_ns: Option<i64>,
}

async fn handle_call(
    message: &WorkMessage<ToolJob>,
    hosting: &Arc<crate::hosting::Hosting>,
    ack_wait: Duration,
) -> Result<()> {
    let delivered_ns = wall_ns();
    message
        .keep_alive_while(
            ack_wait / 3,
            hosting.call(message.value.clone(), delivered_ns),
        )
        .await
}

/// The only place that acknowledges or releases a tool message. It runs
/// after `handle_call` returns, so any error means no acknowledgement was
/// ever sent and the message must be released for redelivery.
async fn settle(message: &WorkMessage<ToolJob>, outcome: Result<()>) {
    match outcome {
        Ok(()) => {
            if let Err(error) = message.acknowledge().await {
                tracing::warn!(error = %swarmy_core::error_chain(&error), request_id = %message.value.request_id, "tool call acknowledgement failed; NATS redelivers after ack wait");
            }
        }
        Err(error) => {
            tracing::warn!(error = %swarmy_core::error_chain(&*error), request_id = %message.value.request_id, "sandbox tool call failed; released for redelivery");
            release(message).await;
        }
    }
}

/// Release a failed call for redelivery after a short delay. A failed
/// release needs no further action: the acknowledgement deadline expires
/// and NATS redelivers anyway.
async fn release(message: &WorkMessage<ToolJob>) {
    if let Err(error) = message
        .negative_acknowledge(Some(Duration::from_secs(2)))
        .await
    {
        tracing::warn!(error = %swarmy_core::error_chain(&error), request_id = %message.value.request_id, "tool call release failed; NATS redelivers after ack wait");
    }
}

/// A tool call claimed through the node API.
pub(crate) struct Claimed {
    pub(crate) claim: PlacedToolClaim,
    pub(crate) hold: ClaimHold,
    pub(crate) turn: Option<swarmy_core::MessageId>,
    pub(crate) claimed_ns: Option<i64>,
}

pub(crate) async fn claim(
    client: &NodeClient,
    keeper: &Keeper,
    placement: &PlacementRecord,
    job: ToolJob,
    deadline: tokio::time::Instant,
) -> Result<Option<Claimed>> {
    let sent = tokio::time::Instant::now();
    let request = ToolClaimRequest {
        job,
        placement: placement.clone(),
        owner: LeaseOwnerId::from_ulid(ulid::Ulid::generate()),
    };
    match client.claim_tool(&request, deadline).await {
        Ok(answer) => {
            let local = keeper::local_deadline(
                Answered {
                    sent,
                    server_now: answer.server_now,
                },
                answer.claim.expires_at,
                keeper::margin(keeper.leases().tool_claim),
            );
            let hold = keeper.hold_claim(answer.claim.clone(), local);
            Ok(Some(Claimed {
                claim: answer.claim,
                hold,
                turn: answer.turn,
                claimed_ns: wall_ns(),
            }))
        }
        Err(ClientError::Refused(NodeRefusal::AlreadyDone)) => Ok(None),
        Err(ClientError::Refused(refusal)) => Err(claim_refusal(refusal)),
        Err(error) => Err(error.into()),
    }
}

fn claim_refusal(refusal: NodeRefusal) -> anyhow::Error {
    match refusal {
        NodeRefusal::PlacedElsewhere { .. } | NodeRefusal::EpochStopped { .. } => {
            crate::hosting::refusal_error(refusal)
        }
        NodeRefusal::Fenced(FenceKind::PlacedToolClaim) => {
            anyhow::anyhow!("tool call already claimed")
        }
        NodeRefusal::Fenced(FenceKind::DispatchPlacementStale { detail })
        | NodeRefusal::Unavailable(detail) => anyhow::anyhow!(detail),
        other => anyhow::anyhow!("node API refused the tool claim: {other}"),
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Completion {
    Committed { interrupt_requested: bool },
    AlreadyDone,
}

pub(crate) async fn complete(
    client: &NodeClient,
    claim: &PlacedToolClaim,
    hold: &ClaimHold,
    result: ToolResult,
    tool: swarmy_core::ToolMetric,
) -> Result<Completion> {
    let deadline = hold
        .deadline()
        .context("tool claim lost before completion")?;
    match client
        .complete_tool(
            &ToolCompleteRequest {
                claim: claim.clone(),
                result,
                tool,
            },
            deadline,
        )
        .await
    {
        Ok(answer) => Ok(Completion::Committed {
            interrupt_requested: answer.interrupt_requested,
        }),
        Err(ClientError::Refused(NodeRefusal::AlreadyDone)) => Ok(Completion::AlreadyDone),
        Err(ClientError::Refused(refusal)) => Err(anyhow::anyhow!(
            "node API refused the tool completion: {refusal}"
        )),
        Err(error) => Err(error.into()),
    }
}

pub(crate) async fn run(
    client: &NodeClient,
    keeper: &Keeper,
    runtime: Arc<RuncRuntime>,
    heads: &Heads,
    claimed: Claimed,
    metrics: CallMetrics,
) -> Result<()> {
    let _activity = swarmy_volume::priority::ToolActivity::begin();
    let Claimed {
        claim, mut hold, ..
    } = claimed;
    let sandbox = Sandbox {
        agent_id: claim.placement.agent_id,
    };
    let (result, timing) = tokio::select! {
        produced = produce(&runtime, heads, &claim) => produced?,
        reason = hold.lost() => anyhow::bail!("tool claim lost before the command finished: {reason}"),
    };
    let tool = node_tool_metric(&claim, &metrics, &result, &timing);
    let outcome = complete(client, &claim, &hold, result.clone(), tool).await?;
    if matches!(
        outcome,
        Completion::Committed {
            interrupt_requested: true
        }
    ) {
        stop_result_process(&runtime, &sandbox, claim.placement.epoch, &result).await?;
    }
    drop(hold);
    if matches!(outcome, Completion::Committed { .. }) {
        observe_first_tool(keeper, runtime, &claim, &metrics);
        tracing::info!(request_id = %claim.job.request_id, "committed sandbox tool output");
    } else {
        tracing::info!(request_id = %claim.job.request_id, "tool completion already committed by an earlier attempt");
    }
    Ok(())
}

async fn produce(
    runtime: &Arc<RuncRuntime>,
    heads: &Heads,
    claim: &PlacedToolClaim,
) -> Result<(ToolResult, ToolTiming)> {
    tracing::info!(request_id = %claim.job.request_id, epoch = claim.placement.epoch, "executing sandbox command");
    let sandbox = Sandbox {
        agent_id: claim.placement.agent_id,
    };
    let outcome = run_command(heads, runtime, &sandbox, claim).await?;
    let mut result = outcome.result;
    extract_image_blob(runtime.objects(), &mut result).await?;
    cap_completed_output(runtime, &sandbox, claim, &mut result).await;
    Ok((result, outcome.summary))
}

/// Move an inline base64 screenshot out of the result metadata into blob
/// storage, replacing it with the object key the worker downloads.
async fn extract_image_blob(
    objects: &dyn object_store::ObjectStore,
    result: &mut ToolResult,
) -> Result<()> {
    if let ToolResult::Completed { metadata, .. } = result {
        metadata.remove("image_object_key");
        if let Some(encoded) = metadata.remove("image_base64") {
            let encoded = encoded.as_str().context("image payload must be base64")?;
            let bytes = base64::engine::general_purpose::STANDARD.decode(encoded)?;
            let key = swarmy_volume::put_tool_blob(objects, bytes).await?;
            metadata.insert("image_object_key".into(), serde_json::json!(key));
        }
    }
    Ok(())
}

/// Cap completed-call output through the runtime so the stored size matches
/// what the worker recorded.
async fn cap_completed_output(
    runtime: &Arc<RuncRuntime>,
    sandbox: &Sandbox,
    claim: &PlacedToolClaim,
    result: &mut ToolResult,
) {
    if let ToolResult::Completed { output, title, .. } = result {
        let capped = cap_output(
            runtime,
            sandbox,
            title,
            &claim.job.call_id.0,
            std::mem::take(output),
        )
        .await;
        *output = capped;
    }
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
    heads: &Heads,
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
    let started_ns = wall_ns();
    let started = Instant::now();
    let process_id = match arguments {
        SandboxArguments::Bash(_)
        | SandboxArguments::ProcessStart(_)
        | SandboxArguments::ProcessLog(_)
        | SandboxArguments::ProcessStop(_)
        | SandboxArguments::WriteStdin(_) => request.args.get(5).cloned(),
        _ => None,
    };
    let guard_seconds = request.timeout_ms / 1_000;
    let (exit, stdout, stderr) = exec(runtime, sandbox, request).await?;
    let summary = ToolTiming {
        exit_status: Some(exit.exit_code),
        started_ns,
        // Wall time is approximate; millisecond display precision is sufficient.
        process_wall_ms: Some(started.elapsed().as_secs_f64() * 1_000.0),
    };
    let result = if exit.timed_out {
        ToolResult::Error {
            error: helper_timeout_error(
                arguments.name(),
                guard_seconds,
                process_id.as_deref(),
                &stdout,
                &stderr,
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
            let volume = VolumeId::from_ulid(sandbox.agent_id.as_ulid());
            let head = heads
                .get(volume)
                .context("no published head for the agent volume on this node")?;
            value["manifest_id"] = serde_json::json!(head);
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

#[expect(
    clippy::cast_precision_loss,
    reason = "fetch histogram reads are approximate, so floating-point display precision is sufficient"
)]
fn node_tool_metric(
    claim: &PlacedToolClaim,
    metrics: &CallMetrics,
    result: &ToolResult,
    timing: &ToolTiming,
) -> swarmy_core::ToolMetric {
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
    swarmy_core::ToolMetric {
        request_id: claim.job.request_id.to_string(),
        name: claim.job.arguments.name().into(),
        started_ns: timing.started_ns,
        delivered_ns: metrics.delivered_ns,
        claimed_ns: metrics.claimed_ns,
        exit_status,
        output_bytes: Some(output_bytes),
        process_wall_ms: timing.process_wall_ms,
        ..Default::default()
    }
}

#[expect(
    clippy::cast_precision_loss,
    reason = "fetch histogram reads are approximate, so floating-point display precision is sufficient"
)]
fn observe_first_tool(
    keeper: &Keeper,
    runtime: Arc<RuncRuntime>,
    claim: &PlacedToolClaim,
    metrics: &CallMetrics,
) {
    if metrics.needs_computer_sample
        && let Some(turn) = metrics.turn
    {
        let keeper = keeper.clone();
        let session = claim.job.session_id;
        let volume = VolumeId::from_ulid(claim.placement.agent_id.as_ulid());
        tokio::spawn(async move {
            if let Ok(stats) = runtime.volume_stats(volume).await {
                keeper.queue_metric(
                    session,
                    turn,
                    swarmy_node_protocol::NodeMetric::Computer(swarmy_core::ComputerMetric {
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

async fn cap_output(
    runtime: &RuncRuntime,
    sandbox: &Sandbox,
    tool: &str,
    call_id: &str,
    output: String,
) -> String {
    if output.len() <= swarmy_core::MAX_TOOL_OUTPUT_BYTES {
        return output;
    }
    let spill = swarmy_core::tool_spill_path(call_id);
    let script = format!(
        "import pathlib, sys; p = pathlib.Path({}); p.parent.mkdir(parents=True, exist_ok=True); p.write_bytes(sys.stdin.buffer.read())",
        serde_json::json!(spill)
    );
    let request = ExecRequest {
        args: vec!["/usr/bin/python3".into(), "-c".into(), script],
        stdin: output.clone().into_bytes(),
        timeout_ms: 10_000,
    };
    match exec(runtime, sandbox, request).await {
        Ok((exit, _, _)) if exit.exit_code == 0 && !exit.timed_out => {}
        Ok((exit, _, stderr)) => {
            tracing::warn!(tool, %spill, exit_code = exit.exit_code, timed_out = exit.timed_out, %stderr, "tool spill write failed; truncating anyway");
        }
        Err(error) => {
            tracing::warn!(error = %swarmy_core::error_chain(&*error), tool, %spill, "tool spill write failed; truncating anyway");
        }
    }
    swarmy_core::cap_tool_output(tool, call_id, output)
}

fn helper_timeout_error(
    tool: &str,
    seconds: u64,
    id: Option<&str>,
    stdout: &str,
    stderr: &str,
) -> String {
    let process = id.map_or_else(String::new, |id| format!(" as process {id}"));
    let recovery = if id.is_some() {
        "; use process_log or process_list"
    } else {
        ""
    };
    format!(
        "{tool} helper did not answer within {seconds} s; the command may still be running{process}{recovery}. stdout: {stdout} stderr: {stderr}"
    )
}

fn exec_guard(arguments: &SandboxArguments) -> Duration {
    let wait_ms = match arguments {
        SandboxArguments::Bash(arguments) => {
            (arguments.yield_seconds * 1_000).min(arguments.timeout_ms)
        }
        _ => 0,
    };
    Duration::from_millis(wait_ms + EXEC_STARTUP_ALLOWANCE_MS)
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
            // CDP may need a cold Chromium start (up to 60 s) after sandbox exec startup.
            timeout_ms: 120_000,
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
        timeout_ms: u64::try_from(exec_guard(arguments).as_millis())
            .expect("exec guard is bounded by a u64 wait and startup allowance"),
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

pub(crate) async fn has_processes(
    runtime: &RuncRuntime,
    placement: &PlacementRecord,
) -> Result<bool> {
    let arguments = SandboxArguments::ProcessList(ProcessListArguments {
        limit: 20,
        all: false,
    });
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

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::{exec_guard, helper_timeout_error};
    use swarmy_core::{NodeId, ignore_best_effort};

    #[test]
    fn bash_guard_reserves_startup_after_default_yield() {
        let args = swarmy_core::SandboxArguments::Bash(swarmy_core::BashArguments {
            command: "true".into(),
            timeout_ms: 120_000,
            yield_seconds: 10,
            output_budget_bytes: 32_768,
        });
        assert_eq!(exec_guard(&args), std::time::Duration::from_secs(70));
    }

    #[test]
    fn timed_out_helper_names_its_process() {
        let error = helper_timeout_error("bash", 70, Some("process-123"), "partial", "warning");
        assert!(error.contains("process process-123; use process_log or process_list"));
        assert!(error.contains("within 70 s"));
        assert!(error.contains("stdout: partial stderr: warning"));
    }

    struct Processes(tempfile::TempDir);

    impl Processes {
        fn call(&self, action: &str, id: &str, command: &str, epoch: u64) -> serde_json::Value {
            self.call_with_options(action, id, command, epoch, None)
        }

        fn call_with_options(
            &self,
            action: &str,
            id: &str,
            command: &str,
            epoch: u64,
            options: Option<serde_json::Value>,
        ) -> serde_json::Value {
            let script = include_str!("processes.py").replace(
                "Path('/var/lib/swarmy/processes')",
                &format!("Path({})", serde_json::json!(self.0.path())),
            );
            let mut args = vec![
                "-c".to_owned(),
                script,
                action.to_owned(),
                epoch.to_string(),
                id.to_owned(),
                command.to_owned(),
            ];
            if let Some(options) = options {
                args.push(options.to_string());
            }
            let output = std::process::Command::new("python3")
                .args(&args)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            serde_json::from_slice(&output.stdout).unwrap()
        }

        fn list(&self, options: serde_json::Value, epoch: u64) -> Vec<serde_json::Value> {
            let value = self.call_with_options("process_list", "", "", epoch, Some(options));
            value.as_array().cloned().unwrap_or_default()
        }

        async fn start_exited(&self, command: &str, epoch: u64) -> String {
            let id = swarmy_core::ProcessId::from_ulid(ulid::Ulid::generate()).to_string();
            self.call("process_start", &id, command, epoch);
            let exit = self.0.path().join(&id).join("exit.json");
            swarmy_testkit::eventually(
                "process did not exit",
                std::time::Duration::from_secs(5),
                async || exit.exists().then_some(()),
            )
            .await;
            id
        }
    }

    impl Drop for Processes {
        fn drop(&mut self) {
            for entry in std::fs::read_dir(self.0.path()).unwrap().flatten() {
                if let Ok(bytes) = std::fs::read(entry.path().join("record.json")) {
                    let record: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                    ignore_best_effort(
                        std::process::Command::new("kill")
                            .args(["-KILL", "--", &format!("-{}", record["pid"])])
                            .stdout(std::process::Stdio::null())
                            .stderr(std::process::Stdio::null())
                            .status(),
                        "kill stale sandbox process",
                    );
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

    async fn seed_exited(processes: &Processes, count: usize, epoch: u64) -> Vec<String> {
        let mut ids = Vec::with_capacity(count);
        for index in 0..count {
            ids.push(
                processes
                    .start_exited(&format!("true # {index}"), epoch)
                    .await,
            );
        }
        ids
    }

    fn assert_newest_first(records: &[serde_json::Value]) {
        // Running records lead; within each group the order is newest first.
        let mut seen_exited = false;
        for window in records.windows(2) {
            let running = window[0]["status"] == "running";
            assert!(
                !(running && seen_exited),
                "running process listed after an exited one"
            );
            seen_exited |= !running;
            if window[0]["status"] == window[1]["status"] {
                let first = window[0]["started_at"].as_f64().unwrap_or_default();
                let second = window[1]["started_at"].as_f64().unwrap_or_default();
                assert!(first >= second, "process list is not newest first");
            }
        }
    }

    #[tokio::test]
    async fn process_list_is_bounded_newest_first_with_overrides() {
        let processes = Processes(tempfile::tempdir().unwrap());
        let exited = seed_exited(&processes, 30, 1).await;
        let running = swarmy_core::ProcessId::from_ulid(ulid::Ulid::generate()).to_string();
        processes.call("process_start", &running, "sleep 300", 1);
        let default = processes.list(serde_json::json!({}), 1);
        assert_eq!(default.len(), 21);
        assert_newest_first(&default);
        assert!(default.iter().any(|item| item["process_id"] == running));
        assert_eq!(default[0]["process_id"], running);
        let newest_exited: Vec<&String> = exited.iter().rev().take(20).collect();
        for id in &newest_exited {
            assert!(default.iter().any(|item| item["process_id"] == **id));
        }
        let oldest = &exited[0];
        assert!(!default.iter().any(|item| item["process_id"] == *oldest));
        let limited = processes.list(serde_json::json!({"limit": 5}), 1);
        assert_eq!(limited.len(), 6);
        assert_newest_first(&limited);
        let all = processes.list(serde_json::json!({"all": true}), 1);
        assert_eq!(all.len(), 31);
        assert_newest_first(&all);
    }

    #[test]
    fn process_list_truncates_long_commands() {
        let processes = Processes(tempfile::tempdir().unwrap());
        let long = "x".repeat(2000);
        let id = swarmy_core::ProcessId::from_ulid(ulid::Ulid::generate()).to_string();
        processes.call("process_start", &id, &long, 1);
        let listed = processes.list(serde_json::json!({"all": true}), 1);
        let record = listed
            .iter()
            .find(|item| item["process_id"] == id)
            .expect("long command missing");
        let command = record["command"].as_str().unwrap_or_default();
        assert_eq!(command.len(), 512);
        assert!(command.ends_with("..."));
    }

    #[tokio::test]
    async fn failed_call_is_released_for_redelivery() {
        let Some(stack) = swarmy_testkit::Stack::load("toolserver") else {
            return;
        };
        let mut guard = swarmy_testkit::StackGuard::new(&stack);
        let bus = swarmy_bus::Bus::connect(
            &stack.nats_url,
            swarmy_bus::Config {
                prefix: Some(swarmy_bus::SubjectToken::new(&stack.prefix).unwrap()),
                ..swarmy_bus::Config::default()
            },
        )
        .await
        .unwrap();
        let node = NodeId::from_ulid(ulid::Ulid::generate());
        let queue = swarmy_bus::WorkQueue::NodeTools(node);
        bus.setup(std::slice::from_ref(&queue)).await.unwrap();
        let session = swarmy_core::SessionId::from_ulid(ulid::Ulid::generate());
        let job = swarmy_core::ToolJob {
            session_id: session,
            request_id: swarmy_core::RequestId::for_step(session, 1),
            call_id: swarmy_core::ToolCallId("bash".into()),
            step: 1,
            arguments: swarmy_core::SandboxArguments::Bash(swarmy_core::BashArguments {
                command: "true".into(),
                timeout_ms: 120_000,
                yield_seconds: 10,
                output_budget_bytes: 32_768,
            }),
        };
        bus.publish_work(&queue, &job).await.unwrap();
        let mut messages = bus.consume::<swarmy_core::ToolJob>(&queue).await.unwrap();
        let first = tokio::time::timeout(std::time::Duration::from_secs(10), messages.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(first.value.request_id, job.request_id);
        super::settle(&first, Err(anyhow::anyhow!("injected store failure"))).await;
        let second = tokio::time::timeout(std::time::Duration::from_secs(10), messages.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(second.value.request_id, job.request_id);
        super::settle(&second, Ok(())).await;
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(3), messages.next())
                .await
                .is_err()
        );
        guard.cleanup().await;
    }
}

#[cfg(test)]
mod display_tests {
    #![deny(clippy::disallowed_methods)]
    use super::display_result;
    use swarmy_core::ToolResult;

    #[tokio::test]
    async fn sandbox_supplied_image_key_is_removed() {
        let mut result = ToolResult::Completed {
            output: String::new(),
            title: String::new(),
            metadata: std::collections::BTreeMap::from([(
                "image_object_key".into(),
                serde_json::json!("blobs/untrusted"),
            )]),
        };
        super::extract_image_blob(&object_store::memory::InMemory::new(), &mut result)
            .await
            .unwrap();
        let ToolResult::Completed { metadata, .. } = result else {
            panic!("completed result changed")
        };
        assert!(!metadata.contains_key("image_object_key"));
    }

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
