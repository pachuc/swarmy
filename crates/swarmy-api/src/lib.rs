//! HTTP control plane. Public handlers use the versioned JSON contract.
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use jiff::Timestamp;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;
mod conversation;
mod stream;
use swarmy_api_types as api;
use swarmy_bus::Bus;
use swarmy_config::Keyring;
use swarmy_core::{AgentId, AgentSettings, CredentialScope, ImageTag, SessionId};
use swarmy_llm::catalog::Catalog;
use swarmy_store::{MAX_SCAN_LIMIT, Store};
use tokio::sync::Mutex;
use ulid::Ulid;
use utoipa::OpenApi;

#[derive(Clone)]
pub struct AppState {
    pub store: Store,
    pub bus: Bus,
    pub token: String,
    pub catalog: Catalog,
    // Serialize mutations so retries through this instance observe completed responses.
    mutations: Arc<Mutex<()>>,
    pub stream_poll_interval: std::time::Duration,
    pub resend_interval: std::time::Duration,
    pub default_image: Option<String>,
    pub default_selection: swarmy_core::ResolvedSelection,
    stream_connections:
        Arc<std::sync::Mutex<std::collections::HashMap<String, stream::Connection>>>,
}

impl AppState {
    #[must_use]
    pub fn new(store: Store, bus: Bus, token: String, catalog: Catalog) -> Self {
        Self {
            store,
            bus,
            token,
            catalog,
            mutations: Arc::new(Mutex::new(())),
            stream_poll_interval: std::time::Duration::from_secs(20),
            resend_interval: std::time::Duration::from_secs(5),
            default_image: None,
            default_selection: swarmy_core::ResolvedSelection {
                provider: "fake".into(),
                model: "scripted".into(),
                effort: swarmy_core::ReasoningEffort::Medium,
            },
            stream_connections: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }
}

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<api::ApiError>)>;
fn error(status: StatusCode, code: &str) -> (StatusCode, Json<api::ApiError>) {
    (
        status,
        Json(api::ApiError {
            code: code.into(),
            message: code.into(),
            provider_text: None,
        }),
    )
}
// `map_err` passes the owned error; taking a reference would require closures at every call site.
#[allow(clippy::needless_pass_by_value)]
fn storage(value: swarmy_store::StoreError) -> (StatusCode, Json<api::ApiError>) {
    use swarmy_store::StoreError;
    match value {
        StoreError::AgentExists => error(StatusCode::CONFLICT, "agent_exists"),
        StoreError::AgentMissing => error(StatusCode::NOT_FOUND, "agent_not_found"),
        StoreError::ImageMissing { .. } => error(StatusCode::NOT_FOUND, "image_not_found"),
        StoreError::InvalidAgentName | StoreError::InvalidImage => {
            error(StatusCode::BAD_REQUEST, "invalid_request")
        }
        _ => error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"),
    }
}
fn id<T>(text: &str, wrap: impl FnOnce(Ulid) -> T) -> Result<T, (StatusCode, Json<api::ApiError>)> {
    text.parse()
        .map(wrap)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid_id"))
}
fn image_ref(image: &api::ImageRef) -> String {
    format!("{}:{}", image.name, image.tag)
}
fn agent(record: swarmy_core::AgentRecord) -> api::Agent {
    api::Agent {
        id: record.agent_id.to_string(),
        name: record.name,
        description: record.description,
        image: api::ImageRef {
            name: record.image.name,
            tag: record.image.tag.0,
        },
        provider: record.provider,
        model: record.model,
        effort: record
            .reasoning_effort
            .and_then(|value| serde_json::to_value(value).ok())
            .and_then(|value| serde_json::from_value(value).ok()),
        system_prompt: record.system_prompt,
        created_at: record.created_at.to_string(),
        main_session_id: record.main_session.map(|value| value.to_string()),
    }
}
fn session(record: &swarmy_core::SessionRecord) -> api::Session {
    let named = matches!(record.kind, swarmy_core::SessionKind::Named { .. });
    let state = serde_json::to_value(record.state)
        .ok()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or(api::SessionState::Idle);
    api::Session {
        id: record.session_id.to_string(),
        agent_id: named.then(|| record.agent_id.to_string()),
        kind: if named {
            api::SessionKind::Named
        } else {
            api::SessionKind::Ephemeral
        },
        state,
        log_id: api::LogId::Session(record.session_id.to_string()),
        head_sequence: record.head_seq,
        created_at: Timestamp::try_from(record.session_id.as_ulid().datetime())
            .map(|value| value.to_string())
            .unwrap_or_default(),
        computer_deleted: record.computer_deleted,
        waiting: None,
    }
}
async fn authorize(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    let presented = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    if state.token.is_empty() || presented != Some(state.token.as_str()) {
        return error(StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    next.run(request).await
}

/// Construct the router without binding a socket so integration tests can serve it in-process.
pub fn router(state: AppState) -> Router {
    let protected = Router::new()
        .route("/v1/agents", get(agents).post(create_agent))
        .route(
            "/v1/agents/{id}",
            get(show_agent).patch(update_agent).delete(delete_agent),
        )
        .route("/v1/sessions", get(sessions).post(conversation::create))
        .route(
            "/v1/sessions/{id}",
            get(show_session).delete(conversation::close),
        )
        .route(
            "/v1/sessions/{id}/messages",
            axum::routing::post(conversation::append),
        )
        .route(
            "/v1/sessions/{id}/interrupt",
            axum::routing::post(conversation::interrupt),
        )
        .route("/v1/sessions/{id}/wait-idle", get(conversation::wait_idle))
        .route("/v1/sessions/{id}/events", get(events))
        .route("/v1/events", get(stream::subscribe))
        .route(
            "/v1/events/{connection_id}/subscription",
            axum::routing::put(stream::update),
        )
        .route("/v1/images", get(images))
        .route("/v1/images/{name}/{tag}", get(show_image))
        .route("/v1/models", get(models))
        .route("/v1/models/search", get(search_models))
        .route("/v1/models/{provider}/{model}", get(show_model))
        .route("/v1/providers", get(providers))
        .route("/v1/credentials", get(credentials).post(set_credential))
        .route(
            "/v1/credentials/{provider}",
            get(check_credential).delete(remove_credential),
        )
        .route("/v1/openapi.json", get(openapi))
        .route_layer(middleware::from_fn_with_state(state.clone(), authorize));
    Router::new()
        .route("/v1/health", get(health))
        .merge(protected)
        .with_state(state)
}
async fn health(State(state): State<AppState>) -> ApiResult<Value> {
    let services = state.store.list_services().await.map_err(storage)?;
    let node_count = services
        .iter()
        .filter(|s| matches!(s.heartbeat.role, swarmy_store::ServiceRole::Node) && s.alive)
        .count();
    let services: Vec<_> = services
        .into_iter()
        .map(|s| api::ServiceHealth {
            role: format!("{:?}", s.heartbeat.role).to_lowercase(),
            instance_id: s.heartbeat.instance_id,
            version: s.heartbeat.version,
            alive: s.alive,
            last_seen: s.heartbeat.last_seen.to_string(),
        })
        .collect();
    Ok(Json(
        json!({"version": env!("CARGO_PKG_VERSION"), "services": services, "node_count": node_count}),
    ))
}
async fn openapi() -> Json<Value> {
    Json(serde_json::to_value(api::ApiDocument::openapi()).unwrap_or_default())
}
#[derive(Deserialize)]
struct Page {
    after: Option<String>,
    limit: Option<usize>,
}
fn limit(value: Option<usize>) -> usize {
    value.unwrap_or(32).clamp(1, MAX_SCAN_LIMIT)
}
async fn agents(
    State(state): State<AppState>,
    Query(page): Query<Page>,
) -> ApiResult<Vec<api::Agent>> {
    let after = page
        .after
        .as_deref()
        .map(|v| id(v, AgentId::from_ulid))
        .transpose()?;
    Ok(Json(
        state
            .store
            .list_agents(after, limit(page.limit))
            .await
            .map_err(storage)?
            .into_iter()
            .map(agent)
            .collect(),
    ))
}
async fn show_agent(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<api::Agent> {
    let record = if let Ok(value) = name.parse::<Ulid>() {
        state.store.get_agent(AgentId::from_ulid(value)).await
    } else {
        state.store.get_agent_by_name(&name).await
    }
    .map_err(storage)?;
    Ok(Json(agent(record.ok_or_else(|| {
        error(StatusCode::NOT_FOUND, "agent_not_found")
    })?)))
}
async fn replay<T: serde::Serialize + serde::de::DeserializeOwned>(
    state: &AppState,
    key: &str,
    scope: &str,
    operation: impl std::future::Future<Output = ApiResult<T>>,
) -> ApiResult<T> {
    if key.is_empty() || key.len() > 256 {
        return Err(error(StatusCode::BAD_REQUEST, "invalid_idempotency_key"));
    }
    let _guard = state.mutations.lock().await;
    let key = format!("{scope}:{key}");
    if let Some(value) = state.store.api_replay(&key).await.map_err(storage)? {
        return serde_json::from_value(value)
            .map(Json)
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "corrupt_replay"));
    }
    let Json(result) = operation.await?;
    let value = serde_json::to_value(&result)
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "encoding_error"))?;
    state
        .store
        .put_api_replay(&key, value)
        .await
        .map_err(storage)?;
    Ok(Json(result))
}
async fn create_agent(
    State(state): State<AppState>,
    Json(body): Json<api::CreateAgent>,
) -> ApiResult<api::Agent> {
    let settings = AgentSettings {
        provider: body.provider,
        model: body.model,
        reasoning_effort: body
            .effort
            .and_then(|v| serde_json::to_value(v).ok())
            .and_then(|v| serde_json::from_value(v).ok()),
        system_prompt: body.system_prompt,
        // Sandbox sizing is not part of the v1 API types yet; the image default applies.
        memory_mib: None,
        gpu: None,
    };
    if body.idempotency_key.is_empty() || body.idempotency_key.len() > 256 {
        return Err(error(StatusCode::BAD_REQUEST, "invalid_idempotency_key"));
    }
    let record = state
        .store
        .create_agent_with_settings_replay(
            &body.name,
            &image_ref(&body.image),
            &body.description,
            &settings,
            Timestamp::now(),
            &format!("agents:create:{}", body.idempotency_key),
        )
        .await
        .map_err(storage)?;
    Ok(Json(agent(record)))
}
async fn update_agent(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<api::UpdateAgent>,
) -> ApiResult<api::Agent> {
    let record = state
        .store
        .get_agent_by_name(&name)
        .await
        .map_err(storage)?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "agent_not_found"))?;
    let settings = AgentSettings {
        provider: body.provider,
        model: body.model,
        reasoning_effort: body
            .effort
            .and_then(|v| serde_json::to_value(v).ok())
            .and_then(|v| serde_json::from_value(v).ok()),
        system_prompt: body.system_prompt,
        // Sandbox sizing is not part of the v1 API types yet; the image default applies.
        memory_mib: None,
        gpu: None,
    };
    let store = state.store.clone();
    replay(
        &state,
        &body.idempotency_key,
        &format!("agents:{name}:update"),
        async move {
            Ok(Json(agent(
                store
                    .set_agent(record.agent_id, &settings)
                    .await
                    .map_err(storage)?,
            )))
        },
    )
    .await
}
#[derive(Deserialize)]
struct DeleteKey {
    idempotency_key: String,
}
async fn delete_agent(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<DeleteKey>,
) -> ApiResult<Value> {
    let store = state.store.clone();
    replay(
        &state,
        &body.idempotency_key,
        &format!("agents:{name}:delete"),
        async move {
            if let Some(record) = store.get_agent_by_name(&name).await.map_err(storage)? {
                store.delete_agent(record.agent_id).await.map_err(storage)?;
            }
            Ok(Json(json!({"deleted": true})))
        },
    )
    .await
}
async fn sessions(
    State(state): State<AppState>,
    Query(page): Query<Page>,
) -> ApiResult<Vec<api::Session>> {
    let after = page
        .after
        .as_deref()
        .map(|v| id(v, SessionId::from_ulid))
        .transpose()?;
    Ok(Json(
        state
            .store
            .list_sessions(after, limit(page.limit))
            .await
            .map_err(storage)?
            .iter()
            .map(session)
            .collect(),
    ))
}
async fn show_session(
    State(state): State<AppState>,
    Path(text): Path<String>,
) -> ApiResult<api::Session> {
    let id = id(&text, SessionId::from_ulid)?;
    Ok(Json(session(
        &state
            .store
            .fetch_session(id)
            .await
            .map_err(storage)?
            .ok_or_else(|| error(StatusCode::NOT_FOUND, "session_not_found"))?,
    )))
}
#[derive(Deserialize)]
struct EventsPage {
    after: Option<u64>,
    limit: Option<usize>,
}
async fn events(
    State(state): State<AppState>,
    Path(text): Path<String>,
    Query(page): Query<EventsPage>,
) -> ApiResult<Vec<api::Event>> {
    let id = id(&text, SessionId::from_ulid)?;
    if state
        .store
        .fetch_session(id)
        .await
        .map_err(storage)?
        .is_none()
    {
        return Err(error(StatusCode::NOT_FOUND, "session_not_found"));
    }
    let records = state
        .store
        .read_events(id, page.after.unwrap_or(0), limit(page.limit))
        .await
        .map_err(storage)?;
    let events = records
        .into_iter()
        .map(|record| {
            let sequence = record.seq();
            let payload = api::EventPayload::StoreRecord {
                record: serde_json::to_value(record)
                    .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "encoding_error"))?,
            };
            Ok(api::Event {
                log_id: api::LogId::Session(text.clone()),
                sequence,
                payload,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(events))
}
async fn images(
    State(state): State<AppState>,
    Query(page): Query<Page>,
) -> ApiResult<Vec<api::Image>> {
    let after = page
        .after
        .as_deref()
        .and_then(|s| s.split_once(':'))
        .map(|(name, tag)| (name, ImageTag(tag.into())));
    Ok(Json(
        state
            .store
            .list_images(
                after.as_ref().map(|(name, tag)| (*name, tag)),
                limit(page.limit),
            )
            .await
            .map_err(storage)?
            .into_iter()
            .map(|v| api::Image {
                id: v.manifest_id.to_string(),
                name: v.name,
                tag: v.tag.0,
            })
            .collect(),
    ))
}
async fn show_image(
    State(state): State<AppState>,
    Path((name, tag)): Path<(String, String)>,
) -> ApiResult<api::Image> {
    let manifest = state
        .store
        .get_image(&name, &ImageTag(tag.clone()))
        .await
        .map_err(storage)?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "image_not_found"))?;
    Ok(Json(api::Image {
        id: manifest.to_string(),
        name,
        tag,
    }))
}
fn model(provider: &str, entry: &swarmy_llm::catalog::ModelInfo) -> api::Model {
    api::Model {
        id: entry.id.clone(),
        provider_id: provider.into(),
        context_window: entry.limit.context,
    }
}
async fn models(State(state): State<AppState>) -> Json<Vec<api::Model>> {
    Json(
        state
            .catalog
            .providers()
            .flat_map(|p| p.models.values().map(|m| model(&p.id, m)))
            .collect(),
    )
}
#[derive(Deserialize)]
struct Search {
    q: String,
}
async fn search_models(
    State(state): State<AppState>,
    Query(search): Query<Search>,
) -> Json<Vec<api::Model>> {
    Json(
        state
            .catalog
            .find(&search.q)
            .into_iter()
            .map(|(p, m)| model(&p.id, m))
            .collect(),
    )
}
async fn show_model(
    State(state): State<AppState>,
    Path((provider, name)): Path<(String, String)>,
) -> ApiResult<api::Model> {
    Ok(Json(model(
        &provider,
        state
            .catalog
            .model(&provider, &name)
            .ok_or_else(|| error(StatusCode::NOT_FOUND, "model_not_found"))?,
    )))
}
async fn providers(State(state): State<AppState>) -> Json<Vec<api::Provider>> {
    Json(
        state
            .catalog
            .providers()
            .map(|p| api::Provider {
                id: p.id.clone(),
                name: p.name.clone(),
                status: "available".into(),
            })
            .collect(),
    )
}
fn credential_store(
    state: &AppState,
) -> Result<swarmy_store::credentials::CredentialStore, (StatusCode, Json<api::ApiError>)> {
    let keyring = Keyring::load()
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "keyring_unavailable"))?;
    Ok(state.store.credentials(keyring))
}
fn credential(value: swarmy_store::credentials::CredentialSummary) -> api::Credential {
    api::Credential {
        provider: value.provider,
        kind: if value.kind == "api_key" {
            api::CredentialKind::ApiKey
        } else {
            api::CredentialKind::Subscription
        },
        label: value.label,
        status: match value.status {
            swarmy_core::CredentialStatus::Ready => api::CredentialStatus::Ready,
            swarmy_core::CredentialStatus::Expired => api::CredentialStatus::Expired,
            swarmy_core::CredentialStatus::NeedsLogin => api::CredentialStatus::NeedsLogin,
        },
        updated_at: value.updated_at.to_string(),
    }
}
async fn credentials(State(state): State<AppState>) -> ApiResult<Vec<api::Credential>> {
    Ok(Json(
        credential_store(&state)?
            .list_credentials(CredentialScope::Cluster)
            .await
            .map_err(storage)?
            .into_iter()
            .map(credential)
            .collect(),
    ))
}
async fn check_credential(
    State(state): State<AppState>,
    Path(provider): Path<String>,
) -> ApiResult<api::Credential> {
    let record = credential_store(&state)?
        .get_credential(CredentialScope::Cluster, &provider)
        .await
        .map_err(storage)?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "credential_not_found"))?;
    Ok(Json(credential(
        swarmy_store::credentials::CredentialSummary::new(provider, &record, Timestamp::now()),
    )))
}
async fn set_credential(
    State(state): State<AppState>,
    Json(body): Json<api::CreateCredential>,
) -> ApiResult<api::Credential> {
    use swarmy_core::{CredentialKind, CredentialRecord};
    if body.kind != api::CredentialKind::ApiKey {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "unsupported_credential_kind",
        ));
    }
    let store = credential_store(&state)?;
    let record = CredentialRecord {
        kind: CredentialKind::ApiKey {
            key: body.secret,
            extra: std::collections::BTreeMap::from([("label".into(), body.label)]),
        },
        updated_at: Timestamp::now(),
    };
    replay(
        &state,
        &body.idempotency_key,
        &format!("credentials:{}:set", body.provider),
        async move {
            store
                .put_credential(CredentialScope::Cluster, &body.provider, &record)
                .await
                .map_err(storage)?;
            Ok(Json(credential(
                swarmy_store::credentials::CredentialSummary::new(
                    body.provider,
                    &record,
                    Timestamp::now(),
                ),
            )))
        },
    )
    .await
}
async fn remove_credential(
    State(state): State<AppState>,
    Path(provider): Path<String>,
    Json(body): Json<DeleteKey>,
) -> ApiResult<Value> {
    let store = credential_store(&state)?;
    replay(
        &state,
        &body.idempotency_key,
        &format!("credentials:{provider}:remove"),
        async move {
            store
                .delete_credential(CredentialScope::Cluster, &provider)
                .await
                .map_err(storage)?;
            Ok(Json(json!({"deleted": true})))
        },
    )
    .await
}
