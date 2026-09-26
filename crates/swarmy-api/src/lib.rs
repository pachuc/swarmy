//! HTTP control plane. Public handlers use the versioned JSON contract.
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use jiff::Timestamp;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;
mod cli;
mod conversation;
mod gc;
pub mod images;
mod models;
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
use utoipa_swagger_ui::SwaggerUi;

#[derive(Clone)]
pub struct AppState {
    pub store: Store,
    pub bus: Bus,
    pub objects: std::sync::Arc<dyn object_store::ObjectStore>,
    pub token: String,
    pub credential_keyring: Option<Keyring>,
    pub catalog: Catalog,
    // Serialize mutations so retries through this instance observe completed responses.
    mutations: Arc<Mutex<()>>,
    pub stream_poll_interval: std::time::Duration,
    pub resend_interval: std::time::Duration,
    pub gc: swarmy_config::GarbageCollection,
    /// Directory on local disk (not tmpfs) where streamed image uploads are
    /// spooled before chunking. Set from the control node's data directory.
    pub upload_dir: std::path::PathBuf,
    /// Largest streamed image upload accepted, in bytes. Larger bodies get a
    /// 413 response after the validated prefix is drained.
    pub upload_max_bytes: u64,
    pub default_image: Option<String>,
    pub default_selection: swarmy_core::ResolvedSelection,
    stream_connections:
        Arc<std::sync::Mutex<std::collections::HashMap<String, stream::Connection>>>,
}

impl AppState {
    #[must_use]
    pub fn new(
        store: Store,
        bus: Bus,
        token: String,
        catalog: Catalog,
        objects: std::sync::Arc<dyn object_store::ObjectStore>,
    ) -> Self {
        Self {
            store,
            bus,
            objects,
            token,
            credential_keyring: None,
            catalog,
            mutations: Arc::new(Mutex::new(())),
            stream_poll_interval: std::time::Duration::from_secs(20),
            resend_interval: std::time::Duration::from_secs(5),
            gc: swarmy_config::GarbageCollection::default(),
            upload_dir: std::env::temp_dir().join("swarmy-uploads"),
            upload_max_bytes: 16 * 1024 * 1024 * 1024,
            default_image: None,
            default_selection: swarmy_core::ResolvedSelection {
                provider: "fake".into(),
                model: "scripted".into(),
                effort: swarmy_core::ReasoningEffort::Medium,
            },
            stream_connections: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Serialize idempotency checks and response stores without holding the
    /// lock across long work such as image uploads or collection sweeps.
    pub(crate) async fn mutation_guard(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.mutations.lock().await
    }
}

pub(crate) fn volume(value: swarmy_volume::VolumeError) -> (StatusCode, Json<api::ApiError>) {
    match value {
        swarmy_volume::VolumeError::Store(inner) => storage(inner),
        _ => error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"),
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
        StoreError::RouteMissing => error(StatusCode::BAD_REQUEST, "route_not_found"),
        StoreError::InvalidRoute(_) => error(StatusCode::BAD_REQUEST, "invalid_route"),
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
        route: record.route,
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
        provider: record.inference.provider.clone(),
        model: record.inference.model.clone(),
        effort: record
            .inference
            .effort
            .and_then(|v| serde_json::to_value(v).ok())
            .and_then(|v| serde_json::from_value(v).ok()),
        next_session: None,
        route: record.route.clone(),
    }
}
async fn session_with_next(
    store: &Store,
    record: &swarmy_core::SessionRecord,
) -> Result<api::Session, (StatusCode, Json<api::ApiError>)> {
    let mut result = session(record);
    if record.state == swarmy_core::SessionState::Completed {
        result.next_session = store
            .next_session(record.session_id)
            .await
            .map_err(storage)?
            .map(|id| id.to_string());
    }
    Ok(result)
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
        .route("/v1/cli/doctor", get(cli::doctor))
        .route("/v1/cli/sessions", get(cli::sessions))
        .route("/v1/cli/sessions/{id}", get(cli::session_show))
        .route("/v1/cli/agents", get(cli::agents).post(cli::agent_create))
        .route(
            "/v1/cli/agents/{name}/settings",
            axum::routing::patch(cli::agent_update),
        )
        .route(
            "/v1/cli/credentials",
            get(cli::credentials).post(cli::credential_set),
        )
        .route("/v1/cli/credentials/{provider}", get(cli::credential))
        .route("/v1/cli/routes", get(cli::routes).post(cli::route_set))
        .route(
            "/v1/cli/routes/{name}",
            get(cli::route_show).delete(cli::route_remove),
        )
        .route(
            "/v1/cli/sessions/{id}/route",
            axum::routing::patch(cli::session_set_route),
        )
        .route("/v1/cli/agents/{name}", get(cli::agent_show))
        .route("/v1/cli/images/{name}/{tag}", get(cli::image_show))
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
        .route(
            "/v1/sessions/{id}/route",
            axum::routing::patch(conversation::set_route),
        )
        .route("/v1/sessions/{id}/wait-idle", get(conversation::wait_idle))
        .route("/v1/sessions/{id}/events", get(events))
        .route("/v1/sessions/{id}/metrics", get(session_metrics))
        .route("/v1/agents/{id}/metrics", get(agent_metrics))
        .route("/v1/events", get(stream::subscribe))
        .route(
            "/v1/events/{connection_id}/subscription",
            axum::routing::put(stream::update),
        )
        .route("/v1/images", get(images))
        .route(
            "/v1/images/uploads",
            post(images::upload).layer(axum::extract::DefaultBodyLimit::disable()),
        )
        .route("/v1/images/{name}/{tag}", get(show_image))
        .route("/v1/gc/runs", post(gc::start))
        .route("/v1/gc/runs/{id}", get(gc::show))
        .route("/v1/models", get(models))
        .route("/v1/models/search", get(search_models))
        .route("/v1/models/probe", post(models::probe))
        .route("/v1/models/{provider}/{model}", get(show_model))
        .route("/v1/providers", get(providers))
        .route("/v1/credentials", get(credentials).post(set_credential))
        .route(
            "/v1/credentials/{provider}",
            get(check_credential).delete(remove_credential),
        )
        .route(
            "/v1/credentials/{provider}/{label}",
            get(check_credential_entry).delete(remove_credential_entry),
        )
        .route("/v1/routes", get(list_routes))
        .route(
            "/v1/routes/{name}",
            get(show_route).post(set_route).delete(delete_route),
        )
        .route(
            "/v1/credentials/{provider}/{label}/quota",
            get(entry_quota).post(set_entry_quota),
        )
        .route("/v1/usage", get(usage))
        .route_layer(middleware::from_fn_with_state(state.clone(), authorize));
    // Health, the OpenAPI document, and the rendered reference are public so
    // every swarm documents itself at its own version without a token.
    Router::new()
        .route("/v1/health", get(health))
        .merge(SwaggerUi::new("/v1/docs").url("/v1/openapi.json", api::ApiDocument::openapi()))
        .merge(protected)
        .with_state(state)
}
async fn health(State(state): State<AppState>) -> ApiResult<api::HealthResponse> {
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
            providers: match s.heartbeat.detail {
                swarmy_store::ServiceDetail::Providers(providers) => providers,
                _ => Vec::new(),
            },
        })
        .collect();
    Ok(Json(api::HealthResponse {
        version: swarmy_version::VERSION.into(),
        git_commit: swarmy_version::GIT_COMMIT.into(),
        api_version: api::API_VERSION.into(),
        default_provider: state.default_selection.provider.clone(),
        services,
        node_count: u64::try_from(node_count).unwrap_or(u64::MAX),
    }))
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
        route: body.route,
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
        route: body.route,
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
async fn delete_agent(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<api::DeleteRequest>,
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
    let records = state
        .store
        .list_sessions(after, limit(page.limit))
        .await
        .map_err(storage)?;
    let mut result = Vec::with_capacity(records.len());
    for record in &records {
        result.push(session_with_next(&state.store, record).await?);
    }
    Ok(Json(result))
}
async fn show_session(
    State(state): State<AppState>,
    Path(text): Path<String>,
) -> ApiResult<api::Session> {
    let id = id(&text, SessionId::from_ulid)?;
    let record = state
        .store
        .fetch_session(id)
        .await
        .map_err(storage)?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "session_not_found"))?;
    Ok(Json(session_with_next(&state.store, &record).await?))
}
async fn session_metrics(
    State(state): State<AppState>,
    Path(text): Path<String>,
    Query(page): Query<Page>,
) -> ApiResult<Vec<api::TurnMetrics>> {
    let session_id = id(&text, SessionId::from_ulid)?;
    if state
        .store
        .fetch_session(session_id)
        .await
        .map_err(storage)?
        .is_none()
    {
        return Err(error(StatusCode::NOT_FOUND, "session_not_found"));
    }
    let after = page
        .after
        .as_deref()
        .map(|value| id(value, swarmy_core::MessageId::from_ulid))
        .transpose()?;
    Ok(Json(
        state
            .store
            .list_turn_metrics(session_id, after, limit(page.limit))
            .await
            .map_err(storage)?,
    ))
}

#[derive(Deserialize)]
struct AgentMetricsPage {
    limit: Option<usize>,
    since: Option<String>,
}
async fn agent_metrics(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(page): Query<AgentMetricsPage>,
) -> ApiResult<api::AgentMetrics> {
    let record = if let Ok(value) = name.parse::<Ulid>() {
        state.store.get_agent(AgentId::from_ulid(value)).await
    } else {
        state.store.get_agent_by_name(&name).await
    }
    .map_err(storage)?
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "agent_not_found"))?;
    let since = page
        .since
        .as_deref()
        .map(|value| id(value, swarmy_core::MessageId::from_ulid))
        .transpose()?;
    Ok(Json(
        state
            .store
            .agent_turn_metrics(record.agent_id, page.limit.unwrap_or(200), since)
            .await
            .map_err(storage)?,
    ))
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
fn model(
    provider: &swarmy_llm::catalog::ProviderInfo,
    entry: &swarmy_llm::catalog::ModelInfo,
) -> api::Model {
    let mut catalog = serde_json::to_value(entry)
        .expect("catalog model serializes")
        .as_object()
        .expect("catalog model is an object")
        .clone();
    catalog.remove("id");
    catalog.insert(
        "key".into(),
        serde_json::json!(format!("{}/{}", provider.id, entry.id)),
    );
    catalog.insert("provider".into(), serde_json::json!(provider.id));
    catalog.insert(
        "effective_api".into(),
        serde_json::json!(entry.api.unwrap_or(provider.api)),
    );
    catalog.insert(
        "effective_base_url".into(),
        serde_json::json!(entry.base_url.as_deref().unwrap_or(&provider.base_url)),
    );
    catalog.insert(
        "supported_efforts".into(),
        serde_json::json!(entry.supported_efforts()),
    );
    api::Model {
        id: entry.id.clone(),
        provider_id: provider.id.clone(),
        context_window: entry.limit.context,
        catalog: catalog.into_iter().collect(),
    }
}
async fn models(
    State(state): State<AppState>,
    Query(query): Query<cli::ModelsQuery>,
) -> ApiResult<Vec<api::Model>> {
    if let Some(provider) = &query.provider
        && state.catalog.provider(provider).is_none()
    {
        return Err(error(StatusCode::BAD_REQUEST, "unknown_provider"));
    }
    Ok(Json(
        state
            .catalog
            .find(query.q.as_deref().unwrap_or(""))
            .into_iter()
            .filter(|(p, m)| {
                query.provider.as_ref().is_none_or(|id| id == &p.id)
                    && (!query.reasoning.unwrap_or(false)
                        || m.supported_efforts()
                            .iter()
                            .any(|e| *e != swarmy_core::ReasoningEffort::None))
            })
            .map(|(p, m)| model(p, m))
            .collect(),
    ))
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
            .map(|(p, m)| model(p, m))
            .collect(),
    )
}
async fn show_model(
    State(state): State<AppState>,
    Path((provider, name)): Path<(String, String)>,
) -> ApiResult<api::Model> {
    let p = state
        .catalog
        .provider(&provider)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "model_not_found"))?;
    Ok(Json(model(
        p,
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
                catalog: [
                    ("api".into(), serde_json::json!(p.api)),
                    ("auth_kinds".into(), serde_json::json!(p.auth_kinds)),
                    ("env_keys".into(), serde_json::json!(p.env_keys)),
                    ("credential".into(), serde_json::json!("unknown")),
                ]
                .into(),
            })
            .collect(),
    )
}
fn credential_store(
    state: &AppState,
) -> Result<swarmy_store::credentials::CredentialStore, (StatusCode, Json<api::ApiError>)> {
    let keyring = state
        .credential_keyring
        .clone()
        .map_or_else(Keyring::load, Ok)
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "keyring_unavailable"))?;
    Ok(state.store.credentials(keyring))
}
fn credential(value: swarmy_store::credentials::CredentialSummary) -> api::Credential {
    api::Credential {
        provider: value.provider,
        kind: match value.kind.as_str() {
            "api-key" => api::CredentialKind::ApiKey,
            "cloud" => api::CredentialKind::Cloud,
            _ => api::CredentialKind::Subscription,
        },
        label: value.label,
        status: match value.status {
            swarmy_core::CredentialStatus::Ready => api::CredentialStatus::Ready,
            swarmy_core::CredentialStatus::Expired => api::CredentialStatus::Expired,
            swarmy_core::CredentialStatus::NeedsLogin => api::CredentialStatus::NeedsLogin,
        },
        updated_at: value.updated_at.to_string(),
        created_at: value.created_at.to_string(),
        last_used_at: value.last_used_at.map(|at| at.to_string()),
    }
}
async fn credentials(State(state): State<AppState>) -> ApiResult<Vec<api::Credential>> {
    Ok(Json(
        credential_store(&state)?
            .list_entries(CredentialScope::Cluster)
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
    let summary = credential_store(&state)?
        .list_entries(CredentialScope::Cluster)
        .await
        .map_err(storage)?
        .into_iter()
        .find(|entry| entry.provider == provider)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "credential_not_found"))?;
    Ok(Json(credential(summary)))
}
async fn set_credential(
    State(state): State<AppState>,
    Json(body): Json<api::CreateCredential>,
) -> ApiResult<api::Credential> {
    use swarmy_core::{CredentialKind, CredentialRecord};
    if body.kind == api::CredentialKind::Subscription {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "use_auth_login_for_subscription",
        ));
    }
    let store = credential_store(&state)?;
    let record = CredentialRecord {
        kind: CredentialKind::ApiKey {
            key: body.secret,
            extra: std::collections::BTreeMap::from([
                ("label".into(), body.label.clone()),
                (
                    "auth_kind".into(),
                    if body.kind == api::CredentialKind::Cloud {
                        "cloud"
                    } else {
                        "api-key"
                    }
                    .into(),
                ),
            ]),
        },
        updated_at: Timestamp::now(),
    };
    replay(
        &state,
        &body.idempotency_key,
        &format!("credentials:{}:set", body.provider),
        async move {
            store
                .put_entry(
                    CredentialScope::Cluster,
                    &body.provider,
                    &body.label,
                    &record,
                )
                .await
                .map_err(storage)?;
            let summary = store
                .list_entries(CredentialScope::Cluster)
                .await
                .map_err(storage)?
                .into_iter()
                .find(|entry| entry.provider == body.provider && entry.label == body.label)
                .ok_or_else(|| error(StatusCode::INTERNAL_SERVER_ERROR, "credential_missing"))?;
            Ok(Json(credential(summary)))
        },
    )
    .await
}
async fn check_credential_entry(
    State(state): State<AppState>,
    Path((provider, label)): Path<(String, String)>,
) -> ApiResult<api::Credential> {
    let summary = credential_store(&state)?
        .list_entries(CredentialScope::Cluster)
        .await
        .map_err(storage)?
        .into_iter()
        .find(|entry| entry.provider == provider && entry.label == label)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "credential_not_found"))?;
    Ok(Json(credential(summary)))
}
fn quota_view(quota: swarmy_store::EntryQuota) -> api::EntryQuotaView {
    api::EntryQuotaView {
        source: match quota.source {
            swarmy_store::QuotaSource::Observed => "observed".into(),
            swarmy_store::QuotaSource::Configured => "configured".into(),
        },
        used: quota.used,
        free: quota.free,
        limit: quota.limit,
        window_seconds: quota.window_seconds,
        observed_at: quota.observed_at.map(|at| at.to_string()),
        remaining: quota.remaining,
        requests_remaining: quota.requests_remaining,
        tokens_remaining: quota.tokens_remaining,
    }
}

async fn require_entry(
    state: &AppState,
    provider: &str,
    label: &str,
) -> Result<(), (StatusCode, Json<api::ApiError>)> {
    let exists = credential_store(state)?
        .list_entries(CredentialScope::Cluster)
        .await
        .map_err(storage)?
        .into_iter()
        .any(|entry| entry.provider == provider && entry.label == label);
    if exists {
        Ok(())
    } else {
        Err(error(StatusCode::NOT_FOUND, "credential_not_found"))
    }
}

async fn entry_quota(
    State(state): State<AppState>,
    Path((provider, label)): Path<(String, String)>,
) -> ApiResult<api::EntryQuotaView> {
    require_entry(&state, &provider, &label).await?;
    let quota = state
        .store
        .entry_quota(&provider, &label)
        .await
        .map_err(storage)?;
    Ok(Json(quota_view(quota)))
}
async fn set_entry_quota(
    State(state): State<AppState>,
    Path((provider, label)): Path<(String, String)>,
    Json(body): Json<api::SetEntryQuota>,
) -> ApiResult<api::EntryQuotaView> {
    if body.limit == 0 || body.window_seconds == 0 {
        return Err(error(StatusCode::BAD_REQUEST, "invalid_quota"));
    }
    require_entry(&state, &provider, &label).await?;
    let store = state.store.clone();
    replay(
        &state,
        &body.idempotency_key,
        &format!("credentials:{provider}:{label}:quota"),
        async move {
            store
                .set_entry_quota_config(&provider, &label, body.limit, body.window_seconds)
                .await
                .map_err(storage)?;
            let quota = store
                .entry_quota(&provider, &label)
                .await
                .map_err(storage)?;
            Ok(Json(quota_view(quota)))
        },
    )
    .await
}
fn totals_view(totals: &swarmy_core::UsageTotals, completions: u64) -> api::UsageTotalsView {
    api::UsageTotalsView {
        input_tokens: totals.usage.input_tokens,
        cached_input_tokens: totals.usage.cached_input_tokens,
        cache_write_input_tokens: totals.usage.cache_write_input_tokens,
        output_tokens: totals.usage.output_tokens,
        reasoning_output_tokens: totals.usage.reasoning_output_tokens,
        total_tokens: totals.usage.total_tokens,
        cost_micros: totals.cost_micros,
        cost_dollars: totals.dollars(),
        completions,
    }
}

fn usage_group_view(group: &swarmy_store::UsageGroup) -> api::UsageGroupView {
    api::UsageGroupView {
        start: group.start.to_string(),
        end: group.end.to_string(),
        input_tokens: group.totals.usage.input_tokens,
        cached_input_tokens: group.totals.usage.cached_input_tokens,
        cache_write_input_tokens: group.totals.usage.cache_write_input_tokens,
        output_tokens: group.totals.usage.output_tokens,
        reasoning_output_tokens: group.totals.usage.reasoning_output_tokens,
        total_tokens: group.totals.usage.total_tokens,
        cost_micros: group.totals.cost_micros,
        cost_dollars: group.totals.dollars(),
        completions: group.completions,
    }
}

/// Split combined `{owner}/{entry}` rollup keys into per-entry views,
/// costliest first, with the distinct providers involved. Entries without
/// a slash predate the provider-label join and surface under themselves.
pub(crate) fn entry_breakdown(
    totals: Vec<swarmy_store::DimensionTotal>,
    owner: &str,
) -> (Vec<api::EntryUsageView>, Vec<String>) {
    let prefix = format!("{owner}/");
    let mut entries: Vec<api::EntryUsageView> = totals
        .into_iter()
        .filter_map(|total| {
            let entry = total.key.strip_prefix(&prefix)?;
            Some(api::EntryUsageView {
                entry: entry.into(),
                provider: entry
                    .split_once('/')
                    .map_or(entry, |(provider, _)| provider)
                    .into(),
                input_tokens: total.totals.usage.input_tokens,
                cached_input_tokens: total.totals.usage.cached_input_tokens,
                cache_write_input_tokens: total.totals.usage.cache_write_input_tokens,
                output_tokens: total.totals.usage.output_tokens,
                reasoning_output_tokens: total.totals.usage.reasoning_output_tokens,
                total_tokens: total.totals.usage.total_tokens,
                cost_micros: total.totals.cost_micros,
                cost_dollars: total.totals.dollars(),
                completions: total.completions,
            })
        })
        .collect();
    entries.sort_by(|left, right| {
        right
            .cost_micros
            .cmp(&left.cost_micros)
            .then_with(|| left.entry.cmp(&right.entry))
    });
    let mut providers: Vec<String> = entries
        .iter()
        .map(|entry| entry.provider.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    providers.sort();
    (entries, providers)
}

#[derive(Deserialize)]
struct UsageQuery {
    by: Option<String>,
    key: Option<String>,
    from: Option<String>,
    to: Option<String>,
    group: Option<String>,
}

/// Read a cost series from the metering rollups: one row per calendar
/// group plus the total. Without `key` the series aggregates every key in
/// the dimension, which is how `swarmy cost` shows the fleet-wide series.
async fn usage(
    State(state): State<AppState>,
    Query(query): Query<UsageQuery>,
) -> ApiResult<api::UsageResponse> {
    let by = query.by.as_deref().unwrap_or("agent");
    let dimension = swarmy_store::MeteringDimension::parse(by)
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid_dimension"))?;
    if matches!(
        dimension,
        swarmy_store::MeteringDimension::AgentEntry | swarmy_store::MeteringDimension::SessionEntry
    ) {
        return Err(error(StatusCode::BAD_REQUEST, "invalid_dimension"));
    }
    let group_by = match query.group.as_deref().unwrap_or("day") {
        "day" => swarmy_store::UsageGroupBy::Day,
        "week" => swarmy_store::UsageGroupBy::Week,
        "month" => swarmy_store::UsageGroupBy::Month,
        "year" => swarmy_store::UsageGroupBy::Year,
        _ => return Err(error(StatusCode::BAD_REQUEST, "invalid_group")),
    };
    let now = Timestamp::now();
    let to = query
        .to
        .as_deref()
        .map(|bound| {
            swarmy_core::time::parse_bound(bound, now)
                .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid_to"))
        })
        .transpose()?
        .unwrap_or(now);
    let from = query
        .from
        .as_deref()
        .map(|bound| {
            swarmy_core::time::parse_bound(bound, now)
                .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid_from"))
        })
        .transpose()?
        .unwrap_or_else(|| {
            to.checked_add(jiff::Span::new().days(-30))
                .unwrap_or(Timestamp::UNIX_EPOCH)
        });
    if to <= from {
        return Err(error(StatusCode::BAD_REQUEST, "invalid_range"));
    }
    let groups = match query.key.as_deref() {
        Some(key) if key.is_empty() => return Err(error(StatusCode::BAD_REQUEST, "invalid_key")),
        Some(key) => {
            state
                .store
                .usage(dimension, key, from, to, group_by)
                .await
                .map_err(storage)?
        }
        None => {
            state
                .store
                .usage_aggregate(dimension, from, to, group_by)
                .await
                .map_err(storage)?
        }
    };
    let mut total = swarmy_core::UsageTotals::default();
    let mut completions: u64 = 0;
    for group in &groups {
        total.add(&group.totals.usage, group.totals.cost_micros);
        completions = completions.saturating_add(group.completions);
    }
    Ok(Json(api::UsageResponse {
        by: dimension.as_str().into(),
        key: query.key,
        group: match group_by {
            swarmy_store::UsageGroupBy::Day => "day",
            swarmy_store::UsageGroupBy::Week => "week",
            swarmy_store::UsageGroupBy::Month => "month",
            swarmy_store::UsageGroupBy::Year => "year",
        }
        .into(),
        from: from.to_string(),
        to: to.to_string(),
        groups: groups.iter().map(usage_group_view).collect(),
        total: totals_view(&total, completions),
    }))
}

async fn remove_credential_entry(
    State(state): State<AppState>,
    Path((provider, label)): Path<(String, String)>,
    Json(body): Json<api::DeleteRequest>,
) -> ApiResult<Value> {
    let store = credential_store(&state)?;
    replay(
        &state,
        &body.idempotency_key,
        &format!("credentials:{provider}:{label}:remove"),
        async move {
            store
                .delete_entry(CredentialScope::Cluster, &provider, &label)
                .await
                .map_err(storage)?;
            Ok(Json(json!({"deleted":true})))
        },
    )
    .await
}
async fn remove_credential(
    State(state): State<AppState>,
    Path(provider): Path<String>,
    Json(body): Json<api::DeleteRequest>,
) -> ApiResult<Value> {
    let store = credential_store(&state)?;
    replay(
        &state,
        &body.idempotency_key,
        &format!("credentials:{provider}:remove"),
        async move {
            if let Some(entry) = store
                .list_entries(CredentialScope::Cluster)
                .await
                .map_err(storage)?
                .into_iter()
                .find(|entry| entry.provider == provider)
            {
                store
                    .delete_entry(CredentialScope::Cluster, &provider, &entry.label)
                    .await
                    .map_err(storage)?;
            }
            Ok(Json(json!({"deleted": true})))
        },
    )
    .await
}

fn api_route(record: swarmy_core::RouteRecord) -> api::Route {
    api::Route {
        name: record.name,
        steps: record
            .steps
            .into_iter()
            .map(|step| api::RouteStep {
                provider: step.provider,
                entry: step.entry,
                model: step.model,
            })
            .collect(),
        updated_at: record.updated_at.to_string(),
    }
}

fn api_route_steps(steps: &[api::RouteStep]) -> Vec<swarmy_core::RouteStep> {
    steps
        .iter()
        .map(|step| swarmy_core::RouteStep {
            provider: step.provider.clone(),
            entry: step.entry.clone(),
            model: step.model.clone(),
        })
        .collect()
}

async fn list_routes(State(state): State<AppState>) -> ApiResult<Vec<api::Route>> {
    Ok(Json(
        state
            .store
            .list_routes()
            .await
            .map_err(storage)?
            .into_iter()
            .map(api_route)
            .collect(),
    ))
}

async fn show_route(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<api::Route> {
    Ok(Json(api_route(
        state
            .store
            .get_route(&name)
            .await
            .map_err(storage)?
            .ok_or_else(|| error(StatusCode::NOT_FOUND, "route_not_found"))?,
    )))
}

async fn set_route(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<api::SetRoute>,
) -> ApiResult<api::Route> {
    for step in &body.steps {
        if state.catalog.provider(&step.provider).is_none() {
            return Err(error(StatusCode::BAD_REQUEST, "invalid_route"));
        }
    }
    let store = state.store.clone();
    replay(
        &state,
        &body.idempotency_key,
        &format!("routes:{name}:set"),
        async move {
            store
                .put_route(&name, &api_route_steps(&body.steps))
                .await
                .map_err(storage)?;
            Ok(Json(api_route(
                store
                    .get_route(&name)
                    .await
                    .map_err(storage)?
                    .ok_or_else(|| error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"))?,
            )))
        },
    )
    .await
}

async fn delete_route(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<api::DeleteRequest>,
) -> ApiResult<api::RouteDeleted> {
    let store = state.store.clone();
    replay(
        &state,
        &body.idempotency_key,
        &format!("routes:{name}:remove"),
        async move {
            let deleted = store.delete_route(&name).await.map_err(storage)?;
            if !deleted {
                return Err(error(StatusCode::NOT_FOUND, "route_not_found"));
            }
            Ok(Json(api::RouteDeleted { deleted }))
        },
    )
    .await
}
