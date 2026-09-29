#![deny(unreachable_pub)]
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
use std::sync::Arc;
mod conversation;
mod gc;
pub mod images;
mod models;
mod stream;
mod views;
use swarmy_api_types as api;
use swarmy_bus::Bus;
use swarmy_config::Keyring;
use swarmy_core::{AgentId, AgentSettings, CredentialScope, ImageTag, SessionId};
use swarmy_llm::catalog::Catalog;
use swarmy_store::{CreateAgentOptions, MAX_SCAN_LIMIT, Store};
use tokio::sync::Mutex;
use ulid::Ulid;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

#[derive(Clone)]
pub struct AppState {
    pub store: Store,
    pub bus: Bus,
    pub objects: Arc<dyn object_store::ObjectStore>,
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
    /// Scripted provider files on the API host, if this stack serves fake.
    pub fake_files: Option<(std::path::PathBuf, std::path::PathBuf)>,
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
        objects: Arc<dyn object_store::ObjectStore>,
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
            fake_files: None,
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
#[expect(clippy::needless_pass_by_value, reason = "`map_err` passes the owned error; taking a reference would require closures at every call site")]
fn storage(value: swarmy_store::StoreError) -> (StatusCode, Json<api::ApiError>) {
    use swarmy_store::StoreError;
    match value {
        StoreError::Domain(swarmy_store::DomainError::ActiveSandboxRequirements) => {
            error(StatusCode::CONFLICT, "agent_computer_placed")
        }
        StoreError::Domain(swarmy_store::DomainError::AgentExists) => {
            error(StatusCode::CONFLICT, "agent_exists")
        }
        StoreError::Domain(swarmy_store::DomainError::AgentMissing) => {
            error(StatusCode::NOT_FOUND, "agent_not_found")
        }
        StoreError::Domain(swarmy_store::DomainError::ImageMissing { .. }) => {
            error(StatusCode::NOT_FOUND, "image_not_found")
        }
        StoreError::Domain(
            swarmy_store::DomainError::InvalidAgentName | swarmy_store::DomainError::InvalidImage,
        ) => error(StatusCode::BAD_REQUEST, "invalid_request"),
        StoreError::Domain(swarmy_store::DomainError::RouteMissing) => {
            error(StatusCode::BAD_REQUEST, "route_not_found")
        }
        StoreError::Domain(swarmy_store::DomainError::InvalidRoute(_)) => {
            error(StatusCode::BAD_REQUEST, "invalid_route")
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
        .route("/v1/doctor", get(doctor))
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
            post(conversation::append),
        )
        .route(
            "/v1/sessions/{id}/interrupt",
            post(conversation::interrupt),
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
            "/v1/credentials/records",
            post(put_credential_record),
        )
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
        .route("/v1/quotas", get(quotas))
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
            role: views::service_role(&s.heartbeat.role),
            instance_id: s.heartbeat.instance_id,
            version: s.heartbeat.version,
            alive: s.alive,
            last_seen: s.heartbeat.last_seen.to_string(),
            providers: views::service_providers(&s.heartbeat.detail),
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
pub(crate) struct ModelsQuery {
    pub q: Option<String>,
    pub provider: Option<String>,
    pub reasoning: Option<bool>,
}

/// One bounded API read gives doctor a consistent view of service heartbeats.
async fn doctor(State(state): State<AppState>) -> ApiResult<api::DoctorSnapshot> {
    let nodes = registered_nodes(&state).await?;
    let services = state.store.list_services().await.map_err(storage)?;
    let services: Vec<_> = services
        .into_iter()
        .map(|service| api::DoctorService {
            role: views::service_role(&service.heartbeat.role),
            instance_id: service.heartbeat.instance_id,
            version: service.heartbeat.version,
            alive: service.alive,
            providers: views::service_providers(&service.heartbeat.detail),
            capacity: views::service_capacity(&service.heartbeat.detail),
        })
        .collect();
    let mut images = Vec::new();
    loop {
        let after = images
            .last()
            .map(|image: &swarmy_core::ImageRecord| (image.name.as_str(), &image.tag));
        let page = state
            .store
            .list_images(after, MAX_SCAN_LIMIT)
            .await
            .map_err(storage)?;
        let done = page.len() < MAX_SCAN_LIMIT;
        images.extend(page);
        if done {
            break;
        }
    }
    let images: Vec<_> = images
        .into_iter()
        .map(|image| format!("{}:{}", image.name, image.tag.0))
        .collect();
    let credentials = match credential_store(&state) {
        Ok(store) => Some(
            store
                .list_entries(CredentialScope::Cluster)
                .await
                .map_err(storage)?
                .into_iter()
                .map(views::credential)
                .collect::<Vec<_>>(),
        ),
        Err(error) => {
            tracing::warn!(?error, "doctor credential listing unavailable");
            None
        }
    };
    Ok(Json(api::DoctorSnapshot {
        services,
        images,
        default_image: state.default_image,
        credentials,
        nodes,
    }))
}

/// Registered nodes with committed sandbox memory for the doctor snapshot.
async fn registered_nodes(
    state: &AppState,
) -> Result<Vec<api::DoctorNode>, (http::StatusCode, Json<api::ApiError>)> {
    let mut nodes = Vec::new();
    let mut after = None;
    loop {
        let (page, next) = state
            .store
            .scan_live_nodes(after, Timestamp::MIN, MAX_SCAN_LIMIT)
            .await
            .map_err(storage)?;
        for record in page {
            let committed = state
                .store
                .committed_memory(record.node_id)
                .await
                .map_err(storage)?;
            nodes.push(api::DoctorNode {
                node_id: record.node_id.to_string(),
                roles: record.roles.into_iter().map(views::node_role).collect(),
                capacity: views::node_capacity(&record.capacity),
                last_heartbeat: record.last_heartbeat.to_string(),
                committed_memory_bytes: committed,
            });
        }
        after = next;
        if after.is_none() {
            break;
        }
    }
    Ok(nodes)
}

#[derive(Deserialize)]
struct Page {
    after: Option<String>,
    limit: Option<usize>,
    inference_limit: Option<usize>,
    tools_limit: Option<usize>,
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
    let records = state
        .store
        .list_agents(after, limit(page.limit))
        .await
        .map_err(storage)?;
    let mut result = Vec::with_capacity(records.len());
    for record in records {
        result.push(views::agent(record));
    }
    Ok(Json(result))
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
    views::agent_value(
        &state,
        record.ok_or_else(|| error(StatusCode::NOT_FOUND, "agent_not_found"))?,
    )
    .await
    .map(Json)
}
async fn replay<T: serde::Serialize + serde::de::DeserializeOwned>(
    state: &AppState,
    key: &str,
    scope: &str,
    operation: impl Future<Output = ApiResult<T>>,
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
    let selection = swarmy_llm::selection::normalize(
        swarmy_core::InferenceSelection {
            provider: body.provider,
            model: body.model,
            effort: body.effort.map(Into::into),
        },
        &state.catalog,
    )
    .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid_selection"))?;
    swarmy_llm::selection::validate(&state.catalog, &selection, &state.default_selection)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid_selection"))?;
    let settings = AgentSettings {
        provider: selection.provider,
        model: selection.model,
        reasoning_effort: selection.effort,
        system_prompt: body.system_prompt,
        memory_mib: body.memory_mib,
        gpu: body.gpu.map(Into::into),
        route: body.route,
    };
    if body.idempotency_key.is_empty() || body.idempotency_key.len() > 256 {
        return Err(error(StatusCode::BAD_REQUEST, "invalid_idempotency_key"));
    }
    let record = state
        .store
        .create_agent(
            &body.name,
            &image_ref(&body.image),
            &body.description,
            Timestamp::now(),
            Some(CreateAgentOptions {
                settings: Some(&settings),
                replay_key: Some(&format!("agents:create:{}", body.idempotency_key)),
                github_token: body.github_token.as_deref(),
            }),
        )
        .await
        .map_err(storage)?;
    // Create and get build the agent through the same conversion, so a
    // freshly created row reads back identical.
    views::agent_value(&state, record).await.map(Json)
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
    let selection = swarmy_llm::selection::normalize(
        swarmy_core::InferenceSelection {
            provider: body.provider,
            model: body.model,
            effort: body.effort.map(Into::into),
        },
        &state.catalog,
    )
    .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid_selection"))?;
    let settings = AgentSettings {
        provider: selection.provider,
        model: selection.model,
        reasoning_effort: selection.effort,
        system_prompt: body.system_prompt,
        memory_mib: body.memory_mib,
        gpu: body.gpu.map(Into::into),
        route: body.route,
    };
    let resets: Vec<swarmy_core::InferenceField> =
        body.resets.into_iter().map(Into::into).collect();
    let mut merged = record.clone();
    settings.apply_to(&mut merged, &resets);
    swarmy_llm::selection::validate(
        &state.catalog,
        &swarmy_core::InferenceSelection {
            provider: merged.provider,
            model: merged.model,
            effort: merged.reasoning_effort,
        },
        &state.default_selection,
    )
    .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid_selection"))?;
    let store = state.store.clone();
    let detail = state.clone();
    replay(
        &state,
        &body.idempotency_key,
        &format!("agents:{name}:update"),
        async move {
            let updated = store
                .set_agent_with_resets(record.agent_id, &settings, &resets)
                .await
                .map_err(storage)?;
            if body.github_token.is_some() || body.clear_github_token {
                store
                    .set_agent_github_token(record.agent_id, body.github_token.as_deref())
                    .await
                    .map_err(storage)?;
            }
            // Update returns the same detail shape as create and show, so the
            // CLI renders the response without a second fetch.
            views::agent_value(&detail, updated).await.map(Json)
        },
    )
    .await
}
async fn delete_agent(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<api::DeleteRequest>,
) -> ApiResult<api::AgentDeleted> {
    let store = state.store.clone();
    replay(
        &state,
        &body.idempotency_key,
        &format!("agents:{name}:delete"),
        async move {
            if let Some(record) = store.get_agent_by_name(&name).await.map_err(storage)? {
                store.delete_agent(record.agent_id).await.map_err(storage)?;
            }
            Ok(Json(api::AgentDeleted { deleted: true }))
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
    let mut agents = std::collections::HashMap::new();
    for record in &records {
        // List rows stay light: no usage, placement, or scratch reads per
        // row, so a full page costs about three store reads per session plus
        // one cached agent fetch. Show hydrates the rest.
        result.push(views::session_with_next(&state, record, &mut agents).await?);
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
    let mut agents = std::collections::HashMap::new();
    let mut result = views::session_with_next(&state, &record, &mut agents).await?;
    let agent = agents
        .get(&record.agent_id)
        .and_then(|entry| entry.as_ref());
    views::populate_session_detail(&state, &mut result, &record, agent).await?;
    Ok(Json(result))
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
            .list_turn_metrics_paged(
                session_id,
                after,
                limit(page.limit),
                page.inference_limit,
                page.tools_limit,
            )
            .await
            .map_err(storage)?
            .into_iter()
            .map(views::into_api_turn)
            .collect(),
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
    Ok(Json(views::into_api_agent(
        state
            .store
            .agent_turn_metrics(record.agent_id, page.limit.unwrap_or(200), since)
            .await
            .map_err(storage)?,
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
                record: api::RecordBody::Event(record),
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
                manifest_id: v.manifest_id.to_string(),
                name: v.name,
                tag: v.tag.0,
                header: None,
                scratch: None,
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
    let record = swarmy_core::ImageRecord {
        name: name.clone(),
        tag: ImageTag(tag.clone()),
        manifest_id: manifest,
    };
    let header = state
        .store
        .get_manifest(manifest)
        .await
        .map_err(storage)?
        .ok_or_else(|| error(StatusCode::INTERNAL_SERVER_ERROR, "image_manifest_missing"))?;
    let scratch = state.store.image_scratch(&record).await.map_err(storage)?;
    Ok(Json(api::Image {
        manifest_id: manifest.to_string(),
        name,
        tag,
        header: Some(views::image_header(&header)),
        scratch: Some(scratch),
    }))
}
async fn models(
    State(state): State<AppState>,
    Query(query): Query<ModelsQuery>,
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
            .map(|(p, m)| views::model(p, m))
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
            .map(|(p, m)| views::model(p, m))
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
    Ok(Json(views::model(
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
                api: views::provider_api(p.api),
                auth_kinds: p.auth_kinds.clone(),
                env_keys: p.env_keys.clone(),
                credential_env_keys: swarmy_llm::auth::provider_env_keys(&p.id)
                    .iter()
                    .map(|key| (*key).to_owned())
                    .collect(),
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
async fn credentials(State(state): State<AppState>) -> ApiResult<Vec<api::Credential>> {
    Ok(Json(
        credential_store(&state)?
            .list_entries(CredentialScope::Cluster)
            .await
            .map_err(storage)?
            .into_iter()
            .map(views::credential)
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
    Ok(Json(views::credential(summary)))
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
        bookkeeping: swarmy_core::CredentialBookkeeping {
            cloud: body.kind == api::CredentialKind::Cloud,
            label: Some(body.label.clone()),
            ..Default::default()
        },
        kind: CredentialKind::ApiKey {
            key: body.secret,
            extra: body.extra,
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
            Ok(Json(views::credential(summary)))
        },
    )
    .await
}
async fn put_credential_record(
    State(state): State<AppState>,
    Json(body): Json<api::PutCredentialRecord>,
) -> ApiResult<api::Credential> {
    let store = credential_store(&state)?;
    replay(
        &state,
        &body.idempotency_key,
        &format!("credentials:{}:{}:record", body.provider, body.label),
        async move {
            store
                .put_entry(
                    CredentialScope::Cluster,
                    &body.provider,
                    &body.label,
                    &body.record,
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
            Ok(Json(views::credential(summary)))
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
    Ok(Json(views::credential(summary)))
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
/// The span is capped at 400 days and the series at 500 groups; larger
/// requests fail with `span_too_large` or `too_many_groups` instead of
/// scanning unbounded history.
async fn usage(
    State(state): State<AppState>,
    Query(query): Query<UsageQuery>,
) -> ApiResult<api::UsageResponse> {
    let sent_by = query.by.clone().unwrap_or_else(|| "agent".into());
    let dimension = swarmy_store::MeteringDimension::parse(&sent_by)
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid_dimension"))?;
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
            to.checked_add(jiff::Span::new().hours(-30 * 24))
                .unwrap_or(Timestamp::UNIX_EPOCH)
        });
    if to <= from {
        return Err(error(StatusCode::BAD_REQUEST, "invalid_range"));
    }
    if to
        .as_second()
        .checked_sub(from.as_second())
        .is_none_or(|span| span > MAX_USAGE_SPAN_SECONDS)
    {
        return Err(error(StatusCode::BAD_REQUEST, "span_too_large"));
    }
    let groups = match query.key.as_deref() {
        Some("") => return Err(error(StatusCode::BAD_REQUEST, "invalid_key")),
        Some(key) => state
            .store
            .usage(dimension, key, from, to, group_by)
            .await
            .map_err(storage)?,
        None => state
            .store
            .usage_aggregate(dimension, from, to, group_by)
            .await
            .map_err(storage)?,
    };
    if groups.len() > MAX_USAGE_GROUPS {
        return Err(error(StatusCode::BAD_REQUEST, "too_many_groups"));
    }
    let mut total = swarmy_core::UsageTotals::default();
    let mut completions: u64 = 0;
    for group in &groups {
        total.add(&group.totals.usage, group.totals.cost_micros);
        completions = completions.saturating_add(group.completions);
    }
    Ok(Json(api::UsageResponse {
        by: sent_by,
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
        groups: groups.iter().map(views::usage_group_view).collect(),
        total: views::totals_view(&total, completions),
    }))
}

/// Longest usage span the API serves: 400 days. Longer windows fail with
/// `span_too_large` instead of scanning unbounded rollup history.
const MAX_USAGE_SPAN_SECONDS: i64 = 400 * 86_400;
/// Most calendar groups one usage response carries. The span cap keeps this
/// unreachable for day groups today; it guards future groupings.
const MAX_USAGE_GROUPS: usize = 500;

/// List every credential entry's quota in one request, so
/// `swarmy auth quota` needs no round trip per entry.
async fn quotas(State(state): State<AppState>) -> ApiResult<Vec<api::QuotaEntry>> {
    let summaries = credential_store(&state)?
        .list_entries(CredentialScope::Cluster)
        .await
        .map_err(storage)?;
    let mut entries = Vec::with_capacity(summaries.len());
    for summary in summaries {
        let quota = state
            .store
            .entry_quota(&summary.provider, &summary.label)
            .await
            .map_err(storage)?;
        entries.push(api::QuotaEntry {
            provider: summary.provider,
            label: summary.label,
            kind: summary.kind.as_str().to_owned(),
            quota: quota_view(quota),
        });
    }
    Ok(Json(entries))
}

async fn remove_credential_entry(
    State(state): State<AppState>,
    Path((provider, label)): Path<(String, String)>,
    Json(body): Json<api::DeleteRequest>,
) -> ApiResult<api::CredentialDeleted> {
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
            Ok(Json(api::CredentialDeleted { deleted: true }))
        },
    )
    .await
}
async fn remove_credential(
    State(state): State<AppState>,
    Path(provider): Path<String>,
    Json(body): Json<api::DeleteRequest>,
) -> ApiResult<api::CredentialDeleted> {
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
            Ok(Json(api::CredentialDeleted { deleted: true }))
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

#[cfg(test)]
mod store_error_tests {
    use super::*;
    use swarmy_store::StoreError;

    #[test]
    fn placed_agent_is_a_conflict() {
        let (status, Json(body)) = storage(StoreError::Domain(
            swarmy_store::DomainError::ActiveSandboxRequirements,
        ));
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body.code, "agent_computer_placed");
    }

    #[test]
    fn only_non_idle_sessions_get_that_code() {
        let (status, Json(body)) = conversation::session_error(StoreError::Domain(
            swarmy_store::DomainError::SessionNotIdle,
        ));
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body.code, "session_not_idle");
        let (_, Json(body)) = conversation::session_error(StoreError::Domain(
            swarmy_store::DomainError::InvalidTransition,
        ));
        assert_eq!(body.code, "storage_error");
    }
}
