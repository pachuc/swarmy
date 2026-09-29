//! Typed resource views for session, agent, and service diagnostics.
//! The API owns storage reads so clients never need a database connection.
use super::{ApiResult, AppState, Page, error, id, limit, storage};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
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
    let events = session_events(&state, session_id, record.head_seq).await?;
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

async fn session_events(
    state: &AppState,
    session_id: SessionId,
    head_seq: u64,
) -> Result<Vec<swarmy_core::Event>, (StatusCode, Json<api::ApiError>)> {
    let mut after = 0;
    let mut events = Vec::new();
    while after < head_seq {
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
    Ok(events)
}

pub(crate) async fn agents(
    State(state): State<AppState>,
    Query(page): Query<Page>,
) -> ApiResult<Vec<api::AgentView>> {
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
        result.push(agent_value(&state, record, false).await?);
    }
    Ok(Json(result))
}
pub(crate) async fn agent_show(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<api::AgentView> {
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
    Ok(Json(agent_value(&state, record, true).await?))
}
async fn agent_value(
    state: &AppState,
    record: swarmy_core::AgentRecord,
    detail: bool,
) -> Result<api::AgentView, (StatusCode, Json<api::ApiError>)> {
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
    let mut view = api::AgentView {
        node_id: placement.as_ref().map(|value| value.node_id.to_string()),
        scratch: scratch.map(|value| api::ScratchView {
            node_id: value.node_id.to_string(),
            bytes: value.bytes,
        }),
        session_count: sessions.len(),
        record,
        usage: None,
        cost_dollars: None,
        entries: Vec::new(),
        providers: Vec::new(),
        placement: None,
        sandbox_address: None,
        last_snapshot_at: None,
        last_snapshot_age_seconds: None,
        sandbox_state: None,
        call_status: None,
        sessions: Vec::new(),
    };
    if detail {
        populate_agent_detail(state, &mut view, placement, sessions).await?;
    }
    Ok(view)
}
async fn populate_agent_detail(
    state: &AppState,
    view: &mut api::AgentView,
    placement: Option<swarmy_core::PlacementRecord>,
    sessions: Vec<swarmy_core::SessionRecord>,
) -> Result<(), (StatusCode, Json<api::ApiError>)> {
    let totals = state
        .store
        .agent_usage(view.record.agent_id)
        .await
        .map_err(storage)?;
    view.cost_dollars = Some(totals.dollars());
    view.usage = Some(totals);
    let (entries, providers) = super::entry_breakdown(
        state
            .store
            .dimension_totals(
                swarmy_store::MeteringDimension::AgentEntry,
                &view.record.agent_id.to_string(),
                None,
            )
            .await
            .map_err(storage)?,
    );
    view.entries = entries;
    view.providers = providers;
    view.sandbox_address = match &placement {
        Some(value) => state
            .store
            .placement_address(value)
            .await
            .map_err(storage)?
            .map(|address| address.to_string()),
        None => None,
    };
    view.placement = placement;
    let volume = state
        .store
        .get_volume(VolumeId::from_ulid(view.record.agent_id.as_ulid()))
        .await
        .map_err(storage)?;
    let snapshot = volume
        .as_ref()
        .map(|value| {
            jiff::Timestamp::from_millisecond(
                i64::try_from(value.head_manifest.as_ulid().timestamp_ms()).unwrap_or(i64::MAX),
            )
        })
        .transpose()
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "snapshot_timestamp"))?;
    view.last_snapshot_at = snapshot.map(|at| at.to_string());
    view.last_snapshot_age_seconds =
        snapshot.map(|at| jiff::Timestamp::now().duration_since(at).as_secs().max(0));
    let status = state
        .store
        .agent_call_status(view.record.agent_id)
        .await
        .map_err(storage)?;
    let busy = status
        .as_ref()
        .is_some_and(|status| status.holder_session_id.is_some() || status.queued_calls > 0);
    view.sandbox_state = Some(
        if busy {
            "busy"
        } else if status.is_some() {
            "idle"
        } else {
            "unknown"
        }
        .into(),
    );
    view.call_status = status;
    for session in sessions {
        let next = state
            .store
            .next_session(session.session_id)
            .await
            .map_err(storage)?;
        view.sessions.push(api::AgentSessionView {
            record: session,
            archived: next.is_some(),
            next_session: next.map(|id| id.to_string()),
        });
    }
    Ok(())
}

#[derive(serde::Deserialize)]
pub(crate) struct ModelsQuery {
    pub q: Option<String>,
    pub provider: Option<String>,
    pub reasoning: Option<bool>,
}
