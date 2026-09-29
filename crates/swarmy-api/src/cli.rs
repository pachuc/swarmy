//! Compatibility projections for the existing command-line output.
//! The API owns the store reads so clients never need a database connection.
use super::{ApiResult, AppState, Page, error, id, limit, storage};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use serde_json::{Value, json};
use swarmy_api_types as api;
use swarmy_core::{AgentId, SessionId, VolumeId};
use swarmy_store::MAX_SCAN_LIMIT;
use swarmy_store::{ServiceDetail, ServiceRole};
use ulid::Ulid;

/// One bounded API read gives doctor a consistent view of service heartbeats.
pub(crate) async fn doctor(State(state): State<AppState>) -> ApiResult<api::DoctorSnapshot> {
    let nodes = registered_nodes(&state).await?;
    let services = state.store.list_services().await.map_err(storage)?;
    let services: Vec<_> = services
        .into_iter()
        .map(|service| {
            let role = match service.heartbeat.role {
                ServiceRole::Scheduler => "scheduler",
                ServiceRole::Worker => "worker",
                ServiceRole::Gateway => "gateway",
                ServiceRole::Api => "api",
                ServiceRole::Node => "node",
            };
            let providers = match &service.heartbeat.detail {
                ServiceDetail::Providers(value) => value.clone(),
                _ => Vec::new(),
            };
            let capacity = match &service.heartbeat.detail {
                ServiceDetail::Capacity(value) => Some(value.clone()),
                _ => None,
            };
            api::DoctorService {
                role: role.into(),
                instance_id: service.heartbeat.instance_id,
                version: service.heartbeat.version,
                alive: service.alive,
                providers,
                capacity: capacity.map(|value| api::NodeCapacity {
                    cpu_millis: value.cpu_millis,
                    memory_bytes: value.memory_bytes,
                    disk_bytes: value.disk_bytes,
                    sandboxes: value.sandboxes,
                }),
            }
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
    let credentials = match super::credential_store(&state) {
        Ok(store) => Some(
            store
                .list_entries(swarmy_core::CredentialScope::Cluster)
                .await
                .map_err(storage)?
                .into_iter()
                .map(super::credential)
                .collect::<Vec<_>>(),
        ),
        Err(_) => None,
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
) -> Result<Vec<api::DoctorNode>, (axum::http::StatusCode, Json<api::ApiError>)> {
    let mut nodes = Vec::new();
    let mut after = None;
    loop {
        let (page, next) = state
            .store
            .scan_live_nodes(after, jiff::Timestamp::MIN, MAX_SCAN_LIMIT)
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
                roles: record
                    .roles
                    .into_iter()
                    .map(|role| match role {
                        swarmy_core::NodeRole::Sandbox => api::NodeRole::Sandbox,
                        swarmy_core::NodeRole::Volume => api::NodeRole::Volume,
                    })
                    .collect(),
                capacity: api::NodeCapacity {
                    cpu_millis: record.capacity.cpu_millis,
                    memory_bytes: record.capacity.memory_bytes,
                    disk_bytes: record.capacity.disk_bytes,
                    sandboxes: record.capacity.sandboxes,
                },
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

fn typed<T: serde::de::DeserializeOwned>(
    value: Value,
) -> Result<T, (StatusCode, Json<api::ApiError>)> {
    serde_json::from_value(value)
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "encoding_error"))
}

pub(crate) async fn session_show(
    State(state): State<AppState>,
    Path(text): Path<String>,
) -> ApiResult<api::SessionDetail> {
    let session_id = id(&text, SessionId::from_ulid)?;
    let record = state
        .store
        .fetch_session(session_id)
        .await
        .map_err(storage)?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "session_not_found"))?;
    let selection = super::resolve_selection(&state, &record).await?;
    let usage = state
        .store
        .session_usage(session_id)
        .await
        .map_err(storage)?;
    let scratch = state
        .store
        .scratch(record.agent_id)
        .await
        .map_err(storage)?;
    let agent = state
        .store
        .get_agent(record.agent_id)
        .await
        .map_err(storage)?;
    let requirements = if let Some(agent) = agent {
        agent.requirements
    } else if let Some(image) = state
        .store
        .pinned_image(session_id)
        .await
        .map_err(storage)?
    {
        swarmy_core::SandboxRequirements {
            memory_mib: state
                .store
                .image_memory(&image)
                .await
                .map_err(storage)?
                .unwrap_or(768),
            gpu: swarmy_core::GpuRequirement::default(),
        }
    } else {
        swarmy_core::SandboxRequirements::default()
    };
    let placement = state
        .store
        .get_by_agent(record.agent_id)
        .await
        .map_err(storage)?;
    let address = if let Some(placement) = &placement {
        state
            .store
            .placement_address(placement)
            .await
            .map_err(storage)?
    } else {
        None
    };
    let wait = state
        .store
        .inference_wait(session_id)
        .await
        .map_err(storage)?;
    let (entries, providers) = super::entry_breakdown(
        state
            .store
            .dimension_totals(
                swarmy_store::MeteringDimension::SessionEntry,
                &session_id.to_string(),
                None,
            )
            .await
            .map_err(storage)?,
    );
    let mut after = 0;
    let mut events = Vec::new();
    while after < record.head_seq {
        let page = state
            .store
            .read_events(session_id, after, MAX_SCAN_LIMIT)
            .await
            .map_err(storage)?;
        if page.is_empty() {
            return Err(error(StatusCode::INTERNAL_SERVER_ERROR, "truncated_log"));
        }
        after = page.last().map_or(after, swarmy_core::Event::seq);
        events.extend(page);
    }
    Ok(Json(api::SessionDetail {
        session: record,
        resolved: selection,
        cost_dollars: usage.dollars(),
        usage,
        entries,
        providers,
        scratch: scratch.map(|value| api::ScratchView {
            node_id: value.node_id.to_string(),
            bytes: value.bytes,
        }),
        requirements,
        placement,
        address: address.map(|value| value.to_string()),
        wait: wait.map(|value| api::InferenceWaitView {
            wake_at: value.wake_at.to_string(),
            reasons: value.reasons,
        }),
        events,
    }))
}

pub(crate) async fn agents(
    State(state): State<AppState>,
    Query(page): Query<Page>,
) -> ApiResult<Vec<api::CliAgent>> {
    let after = page
        .after
        .as_deref()
        .map(|value| id(value, AgentId::from_ulid))
        .transpose()?;
    let mut result = Vec::new();
    for record in state
        .store
        .list_agents(after, limit(page.limit))
        .await
        .map_err(storage)?
    {
        result.push(typed(agent_value(&state, record, false).await?)?);
    }
    Ok(Json(result))
}
pub(crate) async fn agent_show(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<api::CliAgent> {
    let record = state
        .store
        .get_agent_by_name(&name)
        .await
        .map_err(storage)?;
    let record = if let Some(record) = record {
        record
    } else if let Ok(id) = name.parse::<Ulid>() {
        state
            .store
            .get_agent(AgentId::from_ulid(id))
            .await
            .map_err(storage)?
            .ok_or_else(|| error(StatusCode::NOT_FOUND, "agent_not_found"))?
    } else {
        return Err(error(StatusCode::NOT_FOUND, "agent_not_found"));
    };
    Ok(Json(typed(agent_value(&state, record, true).await?)?))
}
async fn agent_value(
    state: &AppState,
    record: swarmy_core::AgentRecord,
    detail: bool,
) -> Result<Value, (StatusCode, Json<swarmy_api_types::ApiError>)> {
    let mut sessions = Vec::new();
    let mut after = None;
    loop {
        let page = state
            .store
            .list_sessions_by_agent(record.agent_id, after, MAX_SCAN_LIMIT)
            .await
            .map_err(storage)?;
        if page.is_empty() {
            break;
        }
        after = page.last().map(|session| session.session_id);
        sessions.extend(page);
    }
    let placement = state
        .store
        .get_by_agent(record.agent_id)
        .await
        .map_err(storage)?;
    let scratch = state
        .store
        .scratch(record.agent_id)
        .await
        .map_err(storage)?;
    let node_id = placement.as_ref().map(|placement| placement.node_id);
    let mut value = serde_json::to_value(&record)
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "encoding_error"))?;
    value["node_id"] = json!(node_id);
    value["scratch"] = json!(scratch);
    value["session_count"] = json!(sessions.len());
    if detail {
        agent_detail(state, &record, placement.as_ref(), sessions, &mut value).await?;
    }
    Ok(value)
}
async fn agent_detail(
    state: &AppState,
    record: &swarmy_core::AgentRecord,
    placement: Option<&swarmy_core::PlacementRecord>,
    sessions: Vec<swarmy_core::SessionRecord>,
    value: &mut Value,
) -> Result<(), (StatusCode, Json<swarmy_api_types::ApiError>)> {
    let totals = state
        .store
        .agent_usage(record.agent_id)
        .await
        .map_err(storage)?;
    let volume = state
        .store
        .get_volume(VolumeId::from_ulid(record.agent_id.as_ulid()))
        .await
        .map_err(storage)?;
    let status = state
        .store
        .agent_call_status(record.agent_id)
        .await
        .map_err(storage)?;
    let address = if let Some(placement) = placement {
        state
            .store
            .placement_address(placement)
            .await
            .map_err(storage)?
    } else {
        None
    };
    value["usage"] = json!(totals);
    value["cost_dollars"] = json!(totals.dollars());
    let (entries, providers) = super::entry_breakdown(
        state
            .store
            .dimension_totals(
                swarmy_store::MeteringDimension::AgentEntry,
                &record.agent_id.to_string(),
                None,
            )
            .await
            .map_err(storage)?,
    );
    value["entries"] = json!(entries);
    value["providers"] = json!(providers);
    value["placement"] = json!(placement);
    value["sandbox_address"] = json!(address);
    let snapshot = volume
        .as_ref()
        .map(|volume| {
            jiff::Timestamp::from_millisecond(
                i64::try_from(volume.head_manifest.as_ulid().timestamp_ms()).unwrap_or(i64::MAX),
            )
        })
        .transpose()
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "snapshot_timestamp"))?;
    value["last_snapshot_at"] = json!(snapshot);
    value["last_snapshot_age_seconds"] =
        json!(snapshot.map(|at| jiff::Timestamp::now().duration_since(at).as_secs().max(0)));
    let busy = status
        .as_ref()
        .is_some_and(|status| status.holder_session_id.is_some() || status.queued_calls > 0);
    value["sandbox_state"] = json!(if busy {
        "busy"
    } else if status.is_some() {
        "idle"
    } else {
        "unknown"
    });
    value["sandbox_state_reason"] = json!(if status.is_some() {
        "sampled node call occupancy"
    } else {
        "no current node call observation"
    });
    value["call_status"] = json!(status);
    let mut listed = Vec::new();
    for session in sessions {
        let successor = state
            .store
            .next_session(session.session_id)
            .await
            .map_err(storage)?;
        let mut entry = json!(session);
        entry["archived"] = json!(successor.is_some());
        entry["next_session"] = json!(successor);
        listed.push(entry);
    }
    value["sessions"] = json!(listed);
    Ok(())
}
#[derive(serde::Deserialize)]
pub(crate) struct ModelsQuery {
    pub q: Option<String>,
    pub provider: Option<String>,
    pub reasoning: Option<bool>,
}
type AgentChoice = api::CliAgentChoice;
fn settings(
    body: &AgentChoice,
    catalog: &swarmy_llm::catalog::Catalog,
) -> Result<swarmy_core::AgentSettings, (StatusCode, Json<swarmy_api_types::ApiError>)> {
    let selection = swarmy_llm::selection::normalize(
        swarmy_core::InferenceSelection {
            provider: body.provider.clone(),
            model: body.model.clone(),
            effort: body
                .effort
                .as_deref()
                .map(str::parse)
                .transpose()
                .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid_effort"))?,
        },
        catalog,
    )
    .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid_selection"))?;
    let gpu = body
        .gpu
        .as_deref()
        .map(|gpu| match gpu {
            "shared" => Ok(swarmy_core::GpuRequirement::Shared),
            "dedicated" => Ok(swarmy_core::GpuRequirement::Dedicated),
            "none" => Ok(swarmy_core::GpuRequirement::None),
            _ => Err(error(StatusCode::BAD_REQUEST, "invalid_gpu")),
        })
        .transpose()?;
    Ok(swarmy_core::AgentSettings {
        provider: selection.provider,
        model: selection.model,
        reasoning_effort: selection.effort,
        system_prompt: body.system_prompt.clone(),
        memory_mib: body.memory,
        gpu,
        route: body.route.clone(),
    })
}
pub(crate) async fn agent_create(
    State(state): State<AppState>,
    Json(body): Json<AgentChoice>,
) -> ApiResult<api::CliAgent> {
    let choice = settings(&body, &state.catalog)?;
    let inference = swarmy_core::InferenceSelection {
        provider: choice.provider.clone(),
        model: choice.model.clone(),
        effort: choice.reasoning_effort,
    };
    swarmy_llm::selection::validate(&state.catalog, &inference, &state.default_selection)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid_selection"))?;
    let name = body
        .name
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "missing_name"))?;
    let image = body
        .image
        .or_else(|| state.default_image.clone())
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "missing_image"))?;
    let key = body.idempotency_key.clone();
    let store = state.store.clone();
    let store_key = format!("cli:agents:create:{key}");
    super::replay(&state, &key, "cli:agents:create", async move {
        let record = store
            .create_agent(
                &name,
                &image,
                body.description.as_deref().unwrap_or(""),
                jiff::Timestamp::now(),
                Some(swarmy_store::CreateAgentOptions {
                    settings: Some(&choice),
                    github_token: body.github_token.as_deref(),
                    replay_key: Some(&store_key),
                }),
            )
            .await
            .map_err(storage)?;
        Ok(Json(typed(json!(record))?))
    })
    .await
}
pub(crate) async fn agent_update(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<AgentChoice>,
) -> ApiResult<api::CliAgent> {
    let record = state
        .store
        .get_agent_by_name(&name)
        .await
        .map_err(storage)?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "agent_not_found"))?;
    let choice = settings(&body, &state.catalog)?;
    let resets = body
        .resets
        .unwrap_or_default()
        .iter()
        .map(|field| match field.as_str() {
            "provider" => Ok(swarmy_core::InferenceField::Provider),
            "model" => Ok(swarmy_core::InferenceField::Model),
            "effort" => Ok(swarmy_core::InferenceField::Effort),
            "route" => Ok(swarmy_core::InferenceField::Route),
            _ => Err(error(StatusCode::BAD_REQUEST, "invalid_reset")),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut merged = record.clone();
    choice.apply_to(&mut merged, &resets);
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
    let key = body.idempotency_key.clone();
    let store = state.store.clone();
    super::replay(
        &state,
        &key,
        &format!("cli:agents:{name}:update"),
        async move {
            let updated = store
                .set_agent_with_resets(record.agent_id, &choice, &resets)
                .await
                .map_err(storage)?;
            if body.github_token.is_some() || body.clear_github_token.unwrap_or(false) {
                store
                    .set_agent_github_token(record.agent_id, body.github_token.as_deref())
                    .await
                    .map_err(storage)?;
            }
            Ok(Json(typed(json!(updated))?))
        },
    )
    .await
}
