use anyhow::{Context, Result};
use std::{sync::Arc, time::Duration};
use swarmy_bus::{Bus, WorkMessage, WorkQueue};
use swarmy_core::{BashResult, LeaseOwnerId, NodeId, ToolClaim, ToolJob, VolumeId};
use swarmy_sandbox::{
    BlockDevice, ExecOutput, ExecRequest, RuncRuntime, SandboxRuntime, SandboxSpec,
};
use swarmy_store::{Store, StoreError};
use tokio::sync::mpsc;

const LEASE: Duration = Duration::from_secs(30);

pub async fn serve(
    store: &Store,
    bus: &Bus,
    node: NodeId,
    runtime: &Arc<RuncRuntime>,
    ack_wait: Duration,
) -> Result<()> {
    let queue = WorkQueue::NodeTools(node);
    bus.setup(std::slice::from_ref(&queue)).await?;
    let mut messages = bus.consume::<ToolJob>(&queue).await?;
    while let Some(delivery) = messages.next().await {
        let message = delivery?;
        if let Err(error) = handle(store, node, runtime, &message, ack_wait).await {
            tracing::warn!(%error, request_id = %message.value.request_id, "sandbox tool failed; retrying after lease expiry");
            message
                .negative_acknowledge(Some(Duration::from_secs(2)))
                .await?;
        }
    }
    anyhow::bail!("node tool subscription closed")
}

async fn handle(
    store: &Store,
    node: NodeId,
    runtime: &RuncRuntime,
    message: &WorkMessage<ToolJob>,
    ack_wait: Duration,
) -> Result<()> {
    let claim = ToolClaim {
        job: message.value.clone(),
        owner: LeaseOwnerId::from_ulid(ulid::Ulid::generate()),
        node_id: node,
        expires_at: jiff::Timestamp::now().checked_add(LEASE)?,
        attempt_volume: VolumeId::from_ulid(ulid::Ulid::generate()),
    };
    if store.tool_completed(claim.job.request_id).await? {
        message.acknowledge().await?;
        return Ok(());
    }
    if !store.claim_tool(&claim).await? {
        message
            .negative_acknowledge(Some(Duration::from_secs(2)))
            .await?;
        return Ok(());
    }
    tracing::info!(request_id = %claim.job.request_id, attempt = %claim.owner, "claimed sandbox tool");
    tokio::select! {
        result = execute(store, runtime, &claim) => result?,
        result = heartbeat(store, &claim, message, ack_wait) => result?,
    }
    message.acknowledge().await?;
    Ok(())
}

async fn execute(store: &Store, runtime: &RuncRuntime, claim: &ToolClaim) -> Result<()> {
    let session = store
        .fetch_session(claim.job.session_id)
        .await?
        .context("session missing")?;
    // A failed attempt can leave a local sandbox. Its final flush affects only
    // its private volume; every new attempt starts from the committed manifest.
    runtime.discard_if_present(session.agent_id).await?;
    let sandbox = runtime
        .create(
            SandboxSpec {
                agent_id: session.agent_id,
            },
            BlockDevice {
                volume_id: claim.attempt_volume,
            },
        )
        .await?;
    tracing::info!(request_id = %claim.job.request_id, "executing sandbox command");
    let (send, mut receive) = mpsc::channel(16);
    let collect = async {
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        while let Some(output) = receive.recv().await {
            match output {
                ExecOutput::Stdout(bytes) => stdout.extend(bytes),
                ExecOutput::Stderr(bytes) => stderr.extend(bytes),
            }
        }
        (stdout, stderr)
    };
    let (exit, (stdout, stderr)) = tokio::join!(
        runtime.exec(
            &sandbox,
            ExecRequest {
                args: vec![
                    "/bin/bash".into(),
                    "-c".into(),
                    claim.job.arguments.command.clone()
                ],
                timeout_ms: claim.job.arguments.timeout_ms,
            },
            send
        ),
        collect
    );
    // Stop descendants and unmount before flushing. No guest process can change
    // the manifest between this boundary and the fenced completion transaction.
    let cleanup = runtime.destroy(sandbox).await;
    let exit = exit?;
    cleanup?;
    let manifest_id = store
        .get_volume(claim.attempt_volume)
        .await?
        .context("attempt volume missing")?
        .head_manifest;
    let result = BashResult {
        stdout: String::from_utf8_lossy(&stdout).into_owned(),
        stderr: String::from_utf8_lossy(&stderr).into_owned(),
        exit_code: exit.exit_code,
        timed_out: exit.timed_out,
        manifest_id,
    };
    loop {
        let head = store
            .fetch_session(claim.job.session_id)
            .await?
            .context("session missing")?
            .head_seq;
        match store.complete_tool(claim, head, &result).await {
            Ok(()) => break,
            Err(StoreError::StaleSequence { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
    tracing::info!(request_id = %claim.job.request_id, %manifest_id, "committed sandbox tool");
    Ok(())
}

async fn heartbeat(
    store: &Store,
    claim: &ToolClaim,
    message: &WorkMessage<ToolJob>,
    ack_wait: Duration,
) -> Result<()> {
    let mut ticks = tokio::time::interval((LEASE / 3).min(ack_wait / 3));
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticks.tick().await;
        store
            .renew_tool(claim, jiff::Timestamp::now().checked_add(LEASE)?)
            .await?;
        message.extend_deadline().await?;
    }
}
