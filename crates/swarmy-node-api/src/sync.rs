use crate::{
    NodeApiState,
    auth::AuthenticatedNode,
    authorize::{Echoed, authorize_token},
    failure::{failure, idempotent},
    register::node_record,
};
use axum::{Extension, Json, extract::State};
use futures::future::join_all;
use jiff::Timestamp;
use std::{future::Future, time::Duration};
use swarmy_core::{AgentCallStatus, AgentId, Lease, NodeId, PlacedToolClaim};
use swarmy_node_protocol::{
    EvictReason, Failure, HostedPlacement, MetricBatch, NodeAdvert, NodeMetric, PlacementRenewal,
    Renewal, SYNC_INTERVAL, ScratchEntry, ScratchVerdict, SyncRequest, SyncResponse,
    TOOL_CLAIM_LEASE, WRITER_LEASE, WriterToken,
};
use swarmy_store::{DomainError, FenceError, MetricPatch, ScratchRecord, StoreError};
const SYNC_DEADLINE: Duration = Duration::from_secs(2);
const STATUS_LIFETIME: Duration = SYNC_INTERVAL.saturating_mul(3);
const NOT_RENEWED: &str = "not renewed this cycle";

pub(crate) async fn handle(
    State(state): State<NodeApiState>,
    Extension(AuthenticatedNode(node)): Extension<AuthenticatedNode>,
    Json(request): Json<SyncRequest>,
) -> Json<SyncResponse> {
    let now = state.store.now();
    let deadline = tokio::time::Instant::now() + SYNC_DEADLINE;
    queue_metrics(&state, request.metrics);
    let (heartbeat_recorded, placements, claims, writers, scratch, ()) = tokio::join!(
        bounded(deadline, false, heartbeat(&state, node, &request.node, now)),
        join_all(request.placements.iter().map(|p| bounded(
            deadline,
            Renewal::Pending(NOT_RENEWED.into()),
            renew_placement(&state, node, p, now)
        ))),
        join_all(request.claims.iter().map(|c| bounded(
            deadline,
            Renewal::Pending(NOT_RENEWED.into()),
            renew_claim(&state, node, c, now)
        ))),
        join_all(request.writers.iter().map(|w| bounded(
            deadline,
            Renewal::Pending(NOT_RENEWED.into()),
            renew_writer(&state, node, w, now)
        ))),
        join_all(request.scratch.iter().map(|e| bounded(
            deadline,
            ScratchVerdict::Keep,
            scratch_verdict(&state, node, e)
        ))),
        bounded(
            deadline,
            (),
            cleared_scratch(&state, node, &request.scratch_evicted)
        ),
    );
    Json(SyncResponse {
        server_now: now,
        heartbeat_recorded,
        placements,
        claims,
        writers,
        scratch,
    })
}
async fn bounded<T>(
    deadline: tokio::time::Instant,
    on_timeout: T,
    work: impl Future<Output = T>,
) -> T {
    tokio::time::timeout_at(deadline, work)
        .await
        .unwrap_or(on_timeout)
}
fn item_failure<T>(error: &StoreError) -> Renewal<T> {
    match failure(error) {
        Failure::Refused { refusal } => Renewal::Lost(refusal),
        other => {
            tracing::warn!(error = %swarmy_core::error_chain(error), "node sync item failed");
            Renewal::Pending(other.to_string())
        }
    }
}
async fn heartbeat(
    state: &NodeApiState,
    node: NodeId,
    advert: &NodeAdvert,
    now: Timestamp,
) -> bool {
    let record = node_record(node, advert, now);
    match idempotent(|| state.store.put_node(&record)).await {
        Ok(()) => true,
        Err(error) => {
            tracing::warn!(%node, error = %swarmy_core::error_chain(&error), "node heartbeat failed");
            false
        }
    }
}
async fn renew_placement(
    state: &NodeApiState,
    node: NodeId,
    hosted: &HostedPlacement,
    now: Timestamp,
) -> Renewal<PlacementRenewal> {
    if let Err(refusal) = authorize_token(node, Echoed::Placement(&hosted.placement)) {
        return Renewal::Lost(refusal);
    }
    let Ok(at_least) = now.checked_add(state.placement_lease) else {
        return Renewal::Pending("placement expiry overflow".into());
    };
    let Ok(expires_at) = now.checked_add(STATUS_LIFETIME) else {
        return Renewal::Pending("status expiry overflow".into());
    };
    let status = AgentCallStatus {
        agent_id: hosted.placement.agent_id,
        node_id: node,
        epoch: hosted.placement.epoch,
        holder_session_id: hosted.holder_session_id,
        queued_calls: hosted.queued_calls,
        observed_at: now,
        expires_at,
    };
    let (renewed, reported) = tokio::join!(
        idempotent(|| state.store.renew_hosted(&hosted.placement, at_least)),
        idempotent(|| state.store.put_agent_call_status(&status)),
    );
    if let Err(error) = reported
        && !matches!(
            error,
            StoreError::Fence(FenceError::PlacementMismatch)
                | StoreError::Domain(DomainError::ComputerDeleted)
        )
    {
        tracing::warn!(error = %swarmy_core::error_chain(&error), "node call status failed");
    }
    match renewed {
        Ok((placement, release_requested)) => Renewal::Renewed(PlacementRenewal {
            placement,
            release_requested,
        }),
        Err(error) => item_failure(&error),
    }
}
async fn renew_claim(
    state: &NodeApiState,
    node: NodeId,
    claim: &PlacedToolClaim,
    now: Timestamp,
) -> Renewal<Timestamp> {
    if let Err(refusal) = authorize_token(node, Echoed::Claim(claim)) {
        return Renewal::Lost(refusal);
    }
    let Ok(expiry) = now.checked_add(TOOL_CLAIM_LEASE) else {
        return Renewal::Pending("claim expiry overflow".into());
    };
    match idempotent(|| state.store.renew_placed_tool(claim, expiry)).await {
        Ok(value) => Renewal::Renewed(value),
        Err(error) => item_failure(&error),
    }
}
async fn renew_writer(
    state: &NodeApiState,
    node: NodeId,
    writer: &WriterToken,
    now: Timestamp,
) -> Renewal<Lease> {
    if let Err(refusal) = authorize_token(node, Echoed::WriterLease(&writer.lease)) {
        return Renewal::Lost(refusal);
    }
    let Ok(expiry) = now.checked_add(WRITER_LEASE) else {
        return Renewal::Pending("writer expiry overflow".into());
    };
    match idempotent(|| {
        state
            .store
            .renew_writer_lease(writer.volume, &writer.lease, expiry)
    })
    .await
    {
        Ok(value) => Renewal::Renewed(value),
        Err(error) => item_failure(&error),
    }
}
async fn report_scratch(state: &NodeApiState, node: NodeId, entry: &ScratchEntry) {
    let record = ScratchRecord {
        node_id: node,
        bytes: entry.bytes,
    };
    if let Err(error) = idempotent(|| state.store.report_scratch(entry.agent, &record)).await {
        tracing::warn!(error = %swarmy_core::error_chain(&error), "node scratch report failed");
    }
}
async fn scratch_verdict(
    state: &NodeApiState,
    node: NodeId,
    entry: &ScratchEntry,
) -> ScratchVerdict {
    let deleted = match state.store.is_computer_deleted(entry.agent).await {
        Ok(value) => value,
        Err(error) => {
            tracing::warn!(error = %swarmy_core::error_chain(&error), "node scratch deletion check failed");
            return ScratchVerdict::Keep;
        }
    };
    if entry.hosted {
        if !deleted {
            report_scratch(state, node, entry).await;
        }
        return ScratchVerdict::Keep;
    }
    if deleted {
        return ScratchVerdict::Evict(EvictReason::Deleted);
    }
    match state.store.get_by_agent(entry.agent).await {
        Ok(Some(placement)) if placement.node_id != node => {
            return ScratchVerdict::Evict(EvictReason::Moved);
        }
        Ok(_) => (),
        Err(error) => {
            tracing::warn!(error = %swarmy_core::error_chain(&error), "node scratch placement check failed");
            return ScratchVerdict::Keep;
        }
    }
    if entry.idle {
        return ScratchVerdict::Evict(EvictReason::Idle);
    }
    report_scratch(state, node, entry).await;
    ScratchVerdict::Keep
}
async fn cleared_scratch(state: &NodeApiState, node: NodeId, evicted: &[AgentId]) {
    join_all(evicted.iter().map(|&agent| async move {
        if let Err(error) = idempotent(|| state.store.clear_scratch(agent, node)).await {
            tracing::warn!(error = %swarmy_core::error_chain(&error), "node scratch clear failed");
        }
    }))
    .await;
}
fn queue_metrics(state: &NodeApiState, batches: Vec<MetricBatch>) {
    for batch in batches {
        if batch.patches.is_empty() {
            continue;
        }
        let patches = batch
            .patches
            .into_iter()
            .map(|patch| match patch {
                NodeMetric::Tool(value) => MetricPatch::Tool(value),
                NodeMetric::Computer(value) => MetricPatch::Computer(value),
            })
            .collect();
        state
            .store
            .observe_turn_metrics(batch.session, batch.turn, patches);
    }
}
#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn bounded_deadline() {
        let now = tokio::time::Instant::now();
        assert_eq!(bounded(now, 9, std::future::pending()).await, 9);
        assert_eq!(
            bounded(now + Duration::from_secs(1), 9, async { 3 }).await,
            3
        );
    }
}
