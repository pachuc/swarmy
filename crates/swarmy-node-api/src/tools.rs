use crate::{NodeApiState, auth::AuthenticatedNode, explain, failure::Answer};
use axum::{Extension, Json, extract::State};
use swarmy_node_protocol::{NodeRefusal, ToolRouteRequest, ToolRouteResponse};
use swarmy_store::{FenceError, StoreError};
/// Route a tool job to its agent after checking dispatch ownership.
pub(crate) async fn route(
    State(state): State<NodeApiState>,
    Extension(AuthenticatedNode(node)): Extension<AuthenticatedNode>,
    Json(request): Json<ToolRouteRequest>,
) -> Result<Json<ToolRouteResponse>, Answer> {
    let now = state.store.now();
    let agent = match state.store.tool_agent(&request.job, node).await {
        Ok(Some(agent)) => agent,
        Ok(None) => return Err(NodeRefusal::AlreadyDone.into()),
        Err(StoreError::Fence(FenceError::PlacementMismatch)) => {
            return Err(explain::dispatch_fence(&state.store, node, &request.job).await);
        }
        Err(error) => return Err(error.into()),
    };
    Ok(Json(ToolRouteResponse {
        server_now: now,
        agent,
    }))
}

use crate::{
    authorize::{Echoed, authorize_token},
    failure::idempotent,
};
use swarmy_bus::Bus;
use swarmy_core::{AgentId, PlacedToolClaim, SessionState, ToolMetric, ToolResult, TurnStage};
use swarmy_node_protocol::{
    Failure, FenceKind, TOOL_CLAIM_LEASE, ToolClaimRequest, ToolClaimResponse, ToolCompleteRequest,
    ToolCompleteResponse,
};
use swarmy_store::{PlacedToolCompletion, StorageError, Store};

const MAX_HEAD_RETRIES: usize = 8;

pub(crate) async fn claim(
    State(state): State<NodeApiState>,
    Extension(AuthenticatedNode(node)): Extension<AuthenticatedNode>,
    Json(request): Json<ToolClaimRequest>,
) -> Result<Json<ToolClaimResponse>, Answer> {
    let store = &state.store;
    let now = store.now();
    authorize_token(node, Echoed::Placement(&request.placement))?;
    let (agent, turn) = tokio::join!(
        store.tool_agent(&request.job, node),
        store.request_turn_id(request.job.request_id)
    );
    let agent = match agent {
        Ok(Some(agent)) => agent,
        Ok(None) => return Err(NodeRefusal::AlreadyDone.into()),
        Err(StoreError::Fence(FenceError::PlacementMismatch)) => {
            return Err(explain::dispatch_fence(store, node, &request.job).await);
        }
        Err(error) => return Err(error.into()),
    };
    if agent != request.placement.agent_id {
        return Err(NodeRefusal::Fenced(FenceKind::Placement).into());
    }
    let turn = turn?;
    if request.job.arguments.is_display_tool() && !display_available(store, agent).await? {
        return Err(
            NodeRefusal::Unavailable("display tool requires a display image".into()).into(),
        );
    }
    let claim = PlacedToolClaim {
        job: request.job,
        owner: request.owner,
        placement: request.placement,
        expires_at: now.checked_add(TOOL_CLAIM_LEASE).map_err(|error| {
            Answer(Failure::Internal {
                message: error.to_string(),
            })
        })?,
    };
    match idempotent(|| store.claim_placed_tool(&claim)).await {
        Ok(true) => Ok(Json(ToolClaimResponse {
            server_now: now,
            claim,
            turn,
        })),
        Ok(false) => {
            if store.tool_completed(claim.job.request_id).await? {
                Err(NodeRefusal::AlreadyDone.into())
            } else {
                Err(NodeRefusal::Fenced(FenceKind::PlacedToolClaim).into())
            }
        }
        Err(StoreError::Fence(FenceError::PlacementMismatch)) => {
            let current = store.get_by_agent(claim.placement.agent_id).await?;
            Err(
                explain::placed_elsewhere(current.as_ref(), Some(&claim.placement), node)
                    .unwrap_or(NodeRefusal::Fenced(FenceKind::Placement))
                    .into(),
            )
        }
        Err(error) => Err(error.into()),
    }
}

async fn display_available(store: &Store, agent: AgentId) -> swarmy_store::Result<bool> {
    let Some(agent) = store.get_agent(agent).await? else {
        return Ok(false);
    };
    store.image_display(&agent.image).await
}

fn check_result_blobs(result: &ToolResult) -> Result<(), Answer> {
    if let ToolResult::Completed { metadata, .. } = result
        && (metadata.contains_key("image_base64")
            || metadata.get("image_object_key").is_some_and(|key| {
                key.as_str()
                    .is_none_or(|key| !swarmy_core::is_blob_key(key))
            }))
    {
        return Err(Answer(Failure::Rejected {
            message: "screenshot requires a valid image_object_key, not inline image_base64".into(),
        }));
    }
    Ok(())
}

enum Commit {
    Committed(Option<PlacedToolCompletion>),
    AlreadyDone,
}

async fn commit(
    store: &Store,
    claim: &PlacedToolClaim,
    result: &ToolResult,
) -> Result<Commit, Answer> {
    let mut head = claim.job.step;
    for _ in 0..MAX_HEAD_RETRIES {
        match store.complete_placed_tool(claim, head, result).await {
            Ok(done) => return Ok(Commit::Committed(Some(done))),
            Err(StoreError::Fence(FenceError::StaleSequence { actual, .. })) => head = actual,
            Err(StoreError::Storage(StorageError::CommitUnknown)) => {
                if store.tool_completed(claim.job.request_id).await? {
                    return Ok(Commit::Committed(None));
                }
            }
            Err(error @ StoreError::Fence(_)) => {
                if store.tool_completed(claim.job.request_id).await? {
                    return Ok(Commit::AlreadyDone);
                }
                return Err(error.into());
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(Answer(Failure::Unavailable {
        message: "session head kept moving; retry".into(),
    }))
}

pub(crate) async fn complete(
    State(state): State<NodeApiState>,
    Extension(AuthenticatedNode(node)): Extension<AuthenticatedNode>,
    Json(request): Json<ToolCompleteRequest>,
) -> Result<Json<ToolCompleteResponse>, Answer> {
    let now = state.store.now();
    authorize_token(node, Echoed::Claim(&request.claim))?;
    check_result_blobs(&request.result)?;
    match commit(&state.store, &request.claim, &request.result).await? {
        Commit::AlreadyDone => Err(NodeRefusal::AlreadyDone.into()),
        Commit::Committed(done) => {
            let interrupt_requested =
                after_commit(&state, &request.claim, request.tool, done).await;
            Ok(Json(ToolCompleteResponse {
                server_now: now,
                interrupt_requested,
            }))
        }
    }
}

fn observed<T>(result: swarmy_store::Result<T>, message: &'static str, fallback: T) -> T {
    match result {
        Ok(value) => value,
        Err(error) => {
            tracing::warn!(error = %swarmy_core::error_chain(&error), "{message}");
            fallback
        }
    }
}

async fn after_commit(
    state: &NodeApiState,
    claim: &PlacedToolClaim,
    mut tool: ToolMetric,
    done: Option<PlacedToolCompletion>,
) -> bool {
    let store = &state.store;
    let session = claim.job.session_id;
    let request_id = claim.job.request_id;
    let (turn, interrupt, fallback) = tokio::join!(
        store.request_turn_id(request_id),
        store.interrupt_requested(session),
        async {
            if done.is_none() {
                store.fetch_session(session).await
            } else {
                Ok(None)
            }
        },
    );
    let turn = observed(turn, "tool completion turn lookup failed", None);
    let interrupt = observed(interrupt, "tool completion interrupt lookup failed", false);
    let fallback = observed(fallback, "tool completion session lookup failed", None).map(|r| {
        PlacedToolCompletion {
            head_seq: r.head_seq,
            state: r.state,
        }
    });
    if let Some(turn) = turn {
        let event = Bus::turn_event(session, turn, TurnStage::ToolCompleted, Some(request_id));
        tool.request_id = request_id.to_string();
        tool.name = claim.job.arguments.name().into();
        tool.dispatched_ns = None;
        tool.completed_ns = None;
        tool.woken_ns = None;
        tool.queue_ms = None;
        store.observe_turn_metrics(
            session,
            turn,
            swarmy_store::completion_patches(tool, None, event.clone()),
        );
        state.bus.record_turn(&event).await;
    }
    if let Some(done) = done.or(fallback)
        && done.state == SessionState::Runnable
        && let Err(error) = state
            .bus
            .nudge(session, done.head_seq, turn, state.resend_interval, false)
            .await
    {
        tracing::warn!(error = %swarmy_core::error_chain(&error), "tool completion nudge failed; scheduler will recover");
    }
    interrupt
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use std::collections::BTreeMap;
    #[test]
    fn result_blobs_accept_only_valid_keys() {
        let mut metadata = BTreeMap::new();
        let result = |metadata| ToolResult::Completed {
            title: "screen".into(),
            output: String::new(),
            metadata,
        };
        assert!(check_result_blobs(&result(metadata.clone())).is_ok());
        assert!(
            check_result_blobs(&ToolResult::Error {
                error: "failed".into()
            })
            .is_ok()
        );
        metadata.insert(
            "image_object_key".into(),
            serde_json::json!(swarmy_core::blob_key(b"png")),
        );
        assert!(check_result_blobs(&result(metadata.clone())).is_ok());
        metadata.insert(
            "image_object_key".into(),
            serde_json::json!("blobs/not-a-hash"),
        );
        assert!(check_result_blobs(&result(metadata.clone())).is_err());
        metadata.insert("image_object_key".into(), serde_json::json!(5));
        assert!(check_result_blobs(&result(metadata.clone())).is_err());
        metadata.remove("image_object_key");
        metadata.insert("image_base64".into(), serde_json::json!("abc"));
        assert!(check_result_blobs(&result(metadata)).is_err());
    }
}
