//! Typed resource views for session, agent, and service diagnostics.
//! The API owns storage reads so clients never need a database connection.
use super::{ApiResult, AppState, error, storage};
use axum::{Json, extract::State, http::StatusCode};
use swarmy_api_types as api;
use swarmy_core::{AgentId, VolumeId};
use swarmy_store::MAX_SCAN_LIMIT;
use swarmy_store::{ServiceDetail, ServiceRole};

/// One bounded API read gives doctor a consistent view of service heartbeats.
pub(crate) async fn doctor(State(state): State<AppState>) -> ApiResult<api::DoctorSnapshot> {
    let nodes = registered_nodes(&state).await?;
    let services = state.store.list_services().await.map_err(storage)?;
    let services: Vec<_> = services
        .into_iter()
        .map(|service| {
            let role = match service.heartbeat.role {
                ServiceRole::Scheduler => api::ServiceRole::Scheduler,
                ServiceRole::Worker => api::ServiceRole::Worker,
                ServiceRole::Gateway => api::ServiceRole::Gateway,
                ServiceRole::Api => api::ServiceRole::Api,
                ServiceRole::Node => api::ServiceRole::Node,
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
                role,
                instance_id: service.heartbeat.instance_id,
                version: service.heartbeat.version,
                alive: service.alive,
                providers,
                capacity: capacity.map(|value| node_capacity(&value)),
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
                capacity: node_capacity(&record.capacity),
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

pub(crate) async fn agent_value(
    state: &AppState,
    record: swarmy_core::AgentRecord,
    detail: bool,
) -> Result<api::Agent, (StatusCode, Json<api::ApiError>)> {
    let agent_id = record.agent_id;
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
    let mut view = super::agent(record);
    view.node_id = placement.as_ref().map(|value| value.node_id.to_string());
    view.scratch = scratch.as_ref().map(scratch_view);
    view.session_count = sessions.len();
    if detail {
        populate_agent_detail(state, &mut view, agent_id, placement, sessions).await?;
    }
    Ok(view)
}
async fn populate_agent_detail(
    state: &AppState,
    view: &mut api::Agent,
    agent_id: AgentId,
    placement: Option<swarmy_core::PlacementRecord>,
    sessions: Vec<swarmy_core::SessionRecord>,
) -> Result<(), (StatusCode, Json<api::ApiError>)> {
    let totals = state.store.agent_usage(agent_id).await.map_err(storage)?;
    view.usage = Some(super::totals_view(&totals, 0));
    let (entries, providers) = super::entry_breakdown(
        state
            .store
            .dimension_totals(
                swarmy_store::MeteringDimension::AgentEntry,
                &agent_id.to_string(),
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
    view.placement = placement.as_ref().map(|value| api::PlacementView {
        node_id: value.node_id.to_string(),
        epoch: value.epoch,
    });
    let volume = state
        .store
        .get_volume(VolumeId::from_ulid(agent_id.as_ulid()))
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
        .agent_call_status(agent_id)
        .await
        .map_err(storage)?;
    let busy = status
        .as_ref()
        .is_some_and(|status| status.holder_session_id.is_some() || status.queued_calls > 0);
    view.sandbox_state = Some(if busy {
        api::SandboxState::Busy
    } else if status.is_some() {
        api::SandboxState::Idle
    } else {
        api::SandboxState::Unknown
    });
    view.call_status = status.map(|status| api::AgentCallView {
        node_id: status.node_id.to_string(),
        epoch: status.epoch,
        holder_session_id: status.holder_session_id.map(|id| id.to_string()),
        queued_calls: status.queued_calls,
        observed_at: status.observed_at.to_string(),
        expires_at: status.expires_at.to_string(),
    });
    for session in sessions {
        let next = state
            .store
            .next_session(session.session_id)
            .await
            .map_err(storage)?;
        let mut item = super::session(&session);
        item.next_session = next.map(|id| id.to_string());
        item.main = view.main_session_id.as_deref() == Some(item.id.as_str());
        view.sessions.push(item);
    }
    Ok(())
}

#[derive(serde::Deserialize)]
pub(crate) struct ModelsQuery {
    pub q: Option<String>,
    pub provider: Option<String>,
    pub reasoning: Option<bool>,
}

fn node_capacity(value: &swarmy_core::NodeCapacity) -> api::NodeCapacity {
    api::NodeCapacity {
        cpu_millis: value.cpu_millis,
        memory_bytes: value.memory_bytes,
        disk_bytes: value.disk_bytes,
        sandboxes: value.sandboxes,
    }
}

pub(crate) fn scratch_view(value: &swarmy_store::ScratchRecord) -> api::ScratchView {
    api::ScratchView {
        node_id: value.node_id.to_string(),
        bytes: value.bytes,
    }
}

pub(crate) fn requirements(value: swarmy_core::SandboxRequirements) -> api::SandboxRequirements {
    let gpu = match value.gpu {
        swarmy_core::GpuRequirement::None => api::GpuMode::None,
        swarmy_core::GpuRequirement::Shared => api::GpuMode::Shared,
        swarmy_core::GpuRequirement::Dedicated => api::GpuMode::Dedicated,
    };
    api::SandboxRequirements {
        memory_mib: value.memory_mib,
        gpu,
    }
}
