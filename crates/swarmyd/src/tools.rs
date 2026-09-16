use anyhow::{Context, Result};
use std::{sync::Arc, time::Duration};
use swarmy_bus::{Bus, WorkQueue};
use swarmy_core::{
    BashResult, LeaseOwnerId, NodeId, PlacedToolClaim, PlacementRecord, ToolJob, VolumeId,
};
use swarmy_sandbox::{ExecOutput, ExecRequest, RuncRuntime, SandboxRuntime};
use swarmy_store::{Store, StoreError};
use tokio::sync::mpsc;

const LEASE: Duration = Duration::from_secs(30);

pub async fn serve(
    bus: &Bus,
    node: NodeId,
    hosting: &Arc<crate::hosting::Hosting>,
    ack_wait: Duration,
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
                calls.spawn(async move {
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
                        Ok(()) => message.acknowledge().await?,
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
    store
        .validate_placement(placement)
        .await
        .context("placement lease lost before execution")?;
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
    let exit = exit?;
    let manifest_id = store
        .get_volume(VolumeId::from_ulid(claim.placement.agent_id.as_ulid()))
        .await?
        .context("agent volume missing")?
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
        match store.complete_placed_tool(claim, head, &result).await {
            Ok(()) => break,
            Err(StoreError::StaleSequence { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
    tracing::info!(request_id = %claim.job.request_id, epoch = claim.placement.epoch, %manifest_id, "committed sandbox tool output");
    anyhow::ensure!(
        !exit.timed_out,
        "command timed out; sandbox processes stopped"
    );
    Ok(())
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
