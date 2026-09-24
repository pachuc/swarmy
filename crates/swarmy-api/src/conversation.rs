//! Conversation mutations preserve the store-first, nudge-second client path.
use super::{ApiResult, AppState, error, id, replay, session, storage};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use jiff::Timestamp;
use serde::Deserialize;
use serde_json::{Value, json};
use std::time::{Duration, Instant};
use swarmy_api_types as api;
use swarmy_bus::{Bus, LiveFeed};
use swarmy_core::{
    AgentId, InferenceSelection, Message, MessageId, MessageRole, Part, SessionId, SessionState,
    TurnStage,
};
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
        swarmy_store::StoreError::NothingToInterrupt => {
            error(StatusCode::CONFLICT, "nothing_to_interrupt")
        }
        swarmy_store::StoreError::MainSessionClose => {
            error(StatusCode::CONFLICT, "main_session_close")
        }
        swarmy_store::StoreError::InvalidState | swarmy_store::StoreError::StaleSequence { .. } => {
            error(StatusCode::CONFLICT, "session_not_idle_or_stale")
        }
        other => storage(other),
    }
}

fn check_key(key: &str) -> Result<(), (StatusCode, Json<api::ApiError>)> {
    if key.is_empty() || key.len() > 256 {
        return Err(error(StatusCode::BAD_REQUEST, "invalid_idempotency_key"));
    }
    Ok(())
}
fn selection(
    body: &api::CreateSession,
) -> Result<InferenceSelection, (StatusCode, Json<api::ApiError>)> {
    Ok(InferenceSelection {
        provider: body.provider.clone(),
        model: body.model.clone(),
        effort: body
            .effort
            .as_ref()
            .map(|value| {
                serde_json::to_value(value)
                    .and_then(serde_json::from_value)
                    .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid_effort"))
            })
            .transpose()?,
    })
}

pub async fn create(
    State(state): State<AppState>,
    Json(body): Json<api::CreateSession>,
) -> ApiResult<api::Session> {
    check_key(&body.idempotency_key)?;
    if body.new && body.agent_id.is_none()
        || body.agent_id.is_some()
            && (body.image.is_some()
                || body.provider.is_some()
                || body.model.is_some()
                || body.effort.is_some())
    {
        return Err(error(StatusCode::BAD_REQUEST, "invalid_session_selection"));
    }
    let choice = selection(&body)?;
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
            let image = if agent.is_none() && image.is_none() {
                Some(
                    swarmy_config::Settings::load()
                        .map_err(|_| error(StatusCode::BAD_REQUEST, "missing_image"))?
                        .settings
                        .session_image(None)
                        .map_err(|_| error(StatusCode::BAD_REQUEST, "missing_image"))?
                        .to_owned(),
                )
            } else {
                image
            };
            let id = store
                .api_session_id(&body.idempotency_key)
                .await
                .map_err(storage)?;
            match store
                .create_session_with_inference(
                    id,
                    agent,
                    image.as_deref(),
                    Timestamp::now(),
                    &choice,
                )
                .await
            {
                Ok(_) | Err(swarmy_store::StoreError::SessionExists) => id,
                Err(failure) => return Err(storage(failure)),
            }
        };
        Ok(Json(session(
            &store
                .fetch_session(id)
                .await
                .map_err(storage)?
                .ok_or_else(|| error(StatusCode::NOT_FOUND, "session_not_found"))?,
        )))
    })
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
    state
        .bus
        .record_turn(&Bus::turn_event(
            session_id,
            turn,
            TurnStage::Submitted,
            None,
        ))
        .await;
    let (sequence, fresh) = state
        .store
        .append_user_message_idempotent(session_id, body.expected_head, &message, &scoped)
        .await
        .map_err(session_error)?;
    if fresh {
        state
            .bus
            .record_turn(&Bus::turn_event(
                session_id,
                turn,
                TurnStage::Appended,
                None,
            ))
            .await;
        // Runnable is durable even if the nudge fails. Do not add a store read
        // or make a transport failure invalidate the successful append.
        if let Err(failure) = state
            .bus
            .nudge(
                session_id,
                sequence,
                Some(turn),
                state.resend_interval,
                false,
            )
            .await
        {
            tracing::warn!(%failure, "message nudge failed; scheduler will recover");
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
) -> ApiResult<Value> {
    let session_id = id(&text, SessionId::from_ulid)?;
    let store = state.store.clone();
    let bus = state.bus.clone();
    replay(&state, &body.idempotency_key, &format!("sessions:{session_id}:interrupt"), async move {
        let result = store.interrupt_session(session_id).await.map_err(session_error)?;
        if result == swarmy_store::InterruptResult::Finished {
            let current = store.fetch_session(session_id).await.map_err(storage)?.ok_or_else(|| error(StatusCode::NOT_FOUND, "session_not_found"))?;
            if let Some(event) = store.read_events(session_id, current.head_seq.saturating_sub(1), 1).await.map_err(storage)?.pop() {
                let _ = bus.publish_live(LiveFeed::SessionEvents(session_id), &event).await;
            }
        }
        Ok(Json(json!({"result": if result == swarmy_store::InterruptResult::Finished { "finished" } else { "requested" }})))
    }).await
}

pub async fn close(
    State(state): State<AppState>,
    Path(text): Path<String>,
    Json(body): Json<api::CloseSession>,
) -> ApiResult<Value> {
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
            Ok(Json(json!({"closed": true})))
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
    let deadline =
        Instant::now() + Duration::from_millis(query.timeout_ms.unwrap_or(30_000).min(120_000));
    loop {
        let record = state
            .store
            .fetch_session(session_id)
            .await
            .map_err(storage)?
            .ok_or_else(|| error(StatusCode::NOT_FOUND, "session_not_found"))?;
        if query.after.is_none_or(|after| record.head_seq > after)
            && record.state == SessionState::Idle
        {
            return Ok(Json(session(&record)));
        }
        if Instant::now() >= deadline {
            return Err(error(StatusCode::REQUEST_TIMEOUT, "wait_timeout"));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
