//! Conversation mutations preserve the store-first, nudge-second client path.
use super::{ApiResult, AppState, error, id, replay, session_with_next, storage};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use jiff::Timestamp;
use serde::Deserialize;
use std::time::Duration;
use swarmy_api_types as api;
use swarmy_bus::{Bus, LiveFeed};
use swarmy_core::{
    AgentId, InferenceSelection, Message, MessageId, MessageRole, Part, SessionId, SessionState,
    TurnStage,
};
use tokio::time::{Instant, interval_at};
use ulid::Ulid;

fn keyed_id(key: &str) -> Ulid {
    let hash = blake3::hash(key.as_bytes());
    let mut bytes = [0; 16];
    bytes[..6].copy_from_slice(&[0x01, 0x90, 0, 0, 0, 0]);
    bytes[6..].copy_from_slice(&hash.as_bytes()[..10]);
    Ulid::from_bytes(bytes)
}
fn session_error(failure: swarmy_store::StoreError) -> (StatusCode, Json<api::ApiError>) {
    match failure {
        swarmy_store::StoreError::SessionMissing => {
            error(StatusCode::NOT_FOUND, "session_not_found")
        }
        swarmy_store::StoreError::NothingToInterrupt => (
            StatusCode::CONFLICT,
            Json(api::ApiError {
                code: "nothing_to_interrupt".into(),
                message: "session is idle or completed; there is nothing to interrupt".into(),
                provider_text: None,
            }),
        ),
        swarmy_store::StoreError::MainSessionClose => (
            StatusCode::CONFLICT,
            Json(api::ApiError {
                code: "main_session_close".into(),
                message: "cannot close an agent main session; use swarmy agent delete to delete the agent".into(),
                provider_text: None,
            }),
        ),
        swarmy_store::StoreError::InvalidState => error(StatusCode::CONFLICT, "session_not_idle"),
        swarmy_store::StoreError::StaleSequence { actual, .. } => (
            StatusCode::CONFLICT,
            Json(api::ApiError {
                code: "stale_head".into(),
                message: format!("stale head; actual head is {actual}"),
                provider_text: None,
            }),
        ),
        other => storage(other),
    }
}

fn invalid_selection(
    error_value: &swarmy_llm::selection::SelectionError,
) -> (StatusCode, Json<api::ApiError>) {
    (
        StatusCode::BAD_REQUEST,
        Json(api::ApiError {
            code: "invalid_selection".into(),
            message: error_value.to_string(),
            provider_text: None,
        }),
    )
}

fn check_key(key: &str) -> Result<(), (StatusCode, Json<api::ApiError>)> {
    if key.is_empty() || key.len() > 256 {
        return Err(error(StatusCode::BAD_REQUEST, "invalid_idempotency_key"));
    }
    Ok(())
}
// Parse effort with the core FromStr implementation so an invalid effort has
// the same error text as the CLI instead of an extractor-generated 422.
#[derive(Deserialize)]
pub struct CreateSessionBody {
    idempotency_key: String,
    agent_id: Option<String>,
    #[serde(default)]
    new: bool,
    image: Option<api::ImageRef>,
    provider: Option<String>,
    model: Option<String>,
    effort: Option<String>,
    route: Option<String>,
}

fn selection(
    body: &CreateSessionBody,
) -> Result<InferenceSelection, (StatusCode, Json<api::ApiError>)> {
    Ok(InferenceSelection {
        provider: body.provider.clone(),
        model: body.model.clone(),
        effort: body.effort.as_deref().map(str::parse).transpose().map_err(
            |failure: swarmy_core::InvalidReasoningEffort| {
                invalid_selection(&swarmy_llm::selection::SelectionError(failure.to_string()))
            },
        )?,
    })
}

pub async fn create(
    State(state): State<AppState>,
    Json(body): Json<CreateSessionBody>,
) -> ApiResult<api::Session> {
    check_key(&body.idempotency_key)?;
    if body.new && body.agent_id.is_none()
        || body.agent_id.is_some()
            && (body.image.is_some()
                || body.provider.is_some()
                || body.model.is_some()
                || body.effort.is_some()
                || body.route.is_some())
    {
        return Err(error(StatusCode::BAD_REQUEST, "invalid_session_selection"));
    }
    let choice = selection(&body)?;
    let choice = if body.agent_id.is_none() {
        let normalized = swarmy_llm::selection::normalize(choice, &state.catalog)
            .map_err(|failure| invalid_selection(&failure))?;
        swarmy_llm::selection::validate(&state.catalog, &normalized, &state.default_selection)
            .map_err(|failure| invalid_selection(&failure))?;
        normalized
    } else {
        choice
    };
    let default_image = state.default_image.clone();
    let store = state.store.clone();
    let replay_key = body.idempotency_key.clone();
    replay(&state, &replay_key, "sessions:create", async move {
        let agent = if let Some(text) = &body.agent_id {
            let record = if let Ok(id) = text.parse::<Ulid>() {
                store.get_agent(AgentId::from_ulid(id)).await
            } else {
                store.get_agent_by_name(text).await
            }
            .map_err(storage)?;
            Some(
                record
                    .ok_or_else(|| error(StatusCode::NOT_FOUND, "agent_not_found"))?
                    .agent_id,
            )
        } else {
            None
        };
        let id = if let Some(agent) = agent.filter(|_| !body.new) {
            store
                .open_main_session(agent, Timestamp::now())
                .await
                .map_err(storage)?
                .0
        } else {
            let image = body
                .image
                .as_ref()
                .map(|image| format!("{}:{}", image.name, image.tag));
            let image = if agent.is_none() {
                Some(
                    image
                        .or(default_image)
                        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "missing_image"))?,
                )
            } else {
                image
            };
            let id = store
                .api_session_id(&body.idempotency_key)
                .await
                .map_err(storage)?;
            match store
                .create_session_with_route(
                    id,
                    agent,
                    image.as_deref(),
                    Timestamp::now(),
                    &choice,
                    body.route.as_deref(),
                )
                .await
            {
                Ok(_) | Err(swarmy_store::StoreError::SessionExists) => id,
                Err(failure) => return Err(storage(failure)),
            }
        };
        let record = store
            .fetch_session(id)
            .await
            .map_err(storage)?
            .ok_or_else(|| error(StatusCode::NOT_FOUND, "session_not_found"))?;
        Ok(Json(session_with_next(&store, &record).await?))
    })
    .await
}

/// Assign or clear one session's route override without touching its agent.
/// The override applies to the next attempt; the attempt chain restarts.
pub async fn set_route(
    State(state): State<AppState>,
    Path(text): Path<String>,
    Json(body): Json<api::SetSessionRoute>,
) -> ApiResult<api::Session> {
    let session_id = id(&text, SessionId::from_ulid)?;
    let store = state.store.clone();
    replay(
        &state,
        &body.idempotency_key,
        &format!("sessions:{session_id}:route"),
        async move {
            store
                .set_session_route(session_id, body.route.as_deref())
                .await
                .map_err(session_error)?;
            let record = store
                .fetch_session(session_id)
                .await
                .map_err(storage)?
                .ok_or_else(|| error(StatusCode::NOT_FOUND, "session_not_found"))?;
            Ok(Json(session_with_next(&store, &record).await?))
        },
    )
    .await
}

/// The expected head is the client's idle observation. The store checks it and
/// the idle state in the same transaction as the append and replay marker.
pub async fn append(
    State(state): State<AppState>,
    Path(text): Path<String>,
    Json(body): Json<api::AppendMessage>,
) -> ApiResult<api::AppendedMessage> {
    check_key(&body.idempotency_key)?;
    if body.text.trim().is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "empty_message"));
    }
    let session_id = id(&text, SessionId::from_ulid)?;
    let scoped = format!("session:{session_id}:append:{}", body.idempotency_key);
    let turn = MessageId::from_ulid(keyed_id(&scoped));
    let message = Message {
        id: turn,
        role: MessageRole::User,
        parts: vec![Part::Text { text: body.text }],
    };
    let (sequence, fresh) = state
        .store
        .append_user_message_idempotent(session_id, body.expected_head, &message, &scoped)
        .await
        .map_err(session_error)?;
    if fresh {
        let submitted = Bus::turn_event(session_id, turn, TurnStage::Submitted, None);
        let appended = Bus::turn_event(session_id, turn, TurnStage::Appended, None);
        state.bus.record_turn(&submitted).await;
        state.bus.record_turn(&appended).await;
        // These writes follow admission and the nudge; they cannot delay either.
        let nudged = state
            .bus
            .nudge(
                session_id,
                sequence,
                Some(turn),
                state.resend_interval,
                false,
            )
            .await;
        state.store.observe_turn_stage(submitted);
        state.store.observe_turn_stage(appended);
        if let Err(failure) = nudged {
            tracing::warn!(%failure, "message nudge failed; scheduler will recover");
        } else {
            state.store.observe_turn_stage(Bus::turn_event(
                session_id,
                turn,
                TurnStage::Nudged,
                None,
            ));
        }
    }
    Ok(Json(api::AppendedMessage {
        sequence,
        turn_id: turn.to_string(),
    }))
}

pub async fn interrupt(
    State(state): State<AppState>,
    Path(text): Path<String>,
    Json(body): Json<api::InterruptSession>,
) -> ApiResult<api::InterruptOutcome> {
    let session_id = id(&text, SessionId::from_ulid)?;
    let store = state.store.clone();
    let bus = state.bus.clone();
    replay(
        &state,
        &body.idempotency_key,
        &format!("sessions:{session_id}:interrupt"),
        async move {
            let result = store
                .interrupt_session(session_id)
                .await
                .map_err(session_error)?;
            if result == swarmy_store::InterruptResult::Finished {
                let current = store
                    .fetch_session(session_id)
                    .await
                    .map_err(storage)?
                    .ok_or_else(|| error(StatusCode::NOT_FOUND, "session_not_found"))?;
                if let Some(event) = store
                    .read_events(session_id, current.head_seq.saturating_sub(1), 1)
                    .await
                    .map_err(storage)?
                    .pop()
                {
                    let _ = bus
                        .publish_live(LiveFeed::SessionEvents(session_id), &event)
                        .await;
                }
            }
            Ok(Json(api::InterruptOutcome {
                result: if result == swarmy_store::InterruptResult::Finished {
                    api::InterruptStatus::Finished
                } else {
                    api::InterruptStatus::Requested
                },
            }))
        },
    )
    .await
}

pub async fn close(
    State(state): State<AppState>,
    Path(text): Path<String>,
    Json(body): Json<api::CloseSession>,
) -> ApiResult<api::SessionClosed> {
    let session_id = id(&text, SessionId::from_ulid)?;
    let store = state.store.clone();
    replay(
        &state,
        &body.idempotency_key,
        &format!("sessions:{session_id}:close"),
        async move {
            store
                .close_session(session_id, Timestamp::now())
                .await
                .map_err(session_error)?;
            Ok(Json(api::SessionClosed { closed: true }))
        },
    )
    .await
}

#[derive(Deserialize)]
pub struct WaitQuery {
    after: Option<u64>,
    timeout_ms: Option<u64>,
}

pub async fn wait_idle(
    State(state): State<AppState>,
    Path(text): Path<String>,
    Query(query): Query<WaitQuery>,
) -> ApiResult<api::Session> {
    let session_id = id(&text, SessionId::from_ulid)?;
    // Register before the first read so a transition between registration and
    // observation cannot be missed. Store state remains authoritative.
    let mut live = state
        .bus
        .subscribe_live::<swarmy_core::Event>(LiveFeed::SessionEvents(session_id))
        .await
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "event_feed_unavailable"))?;
    let deadline =
        Instant::now() + Duration::from_millis(query.timeout_ms.unwrap_or(30_000).min(120_000));
    let mut fallback = interval_at(
        Instant::now() + Duration::from_secs(3),
        Duration::from_secs(3),
    );
    let mut live_open = true;
    loop {
        let record = state
            .store
            .fetch_session(session_id)
            .await
            .map_err(storage)?
            .ok_or_else(|| error(StatusCode::NOT_FOUND, "session_not_found"))?;
        if record.state == SessionState::Completed
            || (record.state == SessionState::Idle
                && query.after.is_none_or(|after| record.head_seq > after))
        {
            return Ok(Json(session_with_next(&state.store, &record).await?));
        }
        loop {
            tokio::select! {
                () = tokio::time::sleep_until(deadline) => return Err(error(StatusCode::REQUEST_TIMEOUT, "wait_timeout")),
                _ = fallback.tick() => break,
                event = live.next(), if live_open => match event {
                    Some(Ok(swarmy_core::Event::StateChanged { to: SessionState::Idle | SessionState::Completed, .. })) => break,
                    Some(Ok(_) | Err(_)) => {},
                    None => live_open = false,
                },
            }
        }
    }
}
