//! Compatibility projections for the existing command-line output.
//! The API owns the store reads so clients never need a database connection.
use super::{ApiResult, AppState, Page, error, id, limit, storage};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use serde_json::{Value, json};
use swarmy_core::{AgentId, ImageTag, SessionId, VolumeId};
use swarmy_store::MAX_SCAN_LIMIT;
use ulid::Ulid;

pub async fn sessions(
    State(state): State<AppState>,
    Query(page): Query<Page>,
) -> ApiResult<Vec<Value>> {
    let after = page
        .after
        .as_deref()
        .map(|value| id(value, SessionId::from_ulid))
        .transpose()?;
    let records = state
        .store
        .list_sessions(after, limit(page.limit))
        .await
        .map_err(storage)?;
    let mut result = Vec::new();
    for session in records {
        let agent = if matches!(session.kind, swarmy_core::SessionKind::Named { .. }) {
            state
                .store
                .get_agent(session.agent_id)
                .await
                .map_err(storage)?
        } else {
            None
        };
        let name = agent.as_ref().map(|agent| agent.name.clone());
        let main = agent
            .as_ref()
            .is_some_and(|agent| agent.main_session == Some(session.session_id));
        let selection = selection(&state, &session).await?;
        let successor = state
            .store
            .next_session(session.session_id)
            .await
            .map_err(storage)?;
        let mut value = serde_json::to_value(&session)
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "encoding_error"))?;
        value["state_since"] = json!(
            state
                .store
                .session_state_since(session.session_id)
                .await
                .map_err(storage)?
        );
        value["archived"] = json!(successor.is_some());
        value["next_session"] = json!(successor);
        value["previous_session"] = json!(
            state
                .store
                .previous_session(session.session_id)
                .await
                .map_err(storage)?
        );
        value["resolved_inference"] = json!(selection);
        value["main"] = json!(main);
        value["agent_name"] = json!(name);
        result.push(value);
    }
    Ok(Json(result))
}
async fn selection(
    state: &AppState,
    record: &swarmy_core::SessionRecord,
) -> Result<swarmy_core::ResolvedSelection, (StatusCode, Json<swarmy_api_types::ApiError>)> {
    let agent = state
        .store
        .get_agent(record.agent_id)
        .await
        .map_err(storage)?;
    let mut selected = state.default_selection.clone();
    if let Some(agent) = agent {
        if let Some(provider) = agent.provider {
            selected.provider = provider;
        }
        if let Some(model) = agent.model {
            selected.model = model;
        }
        if let Some(effort) = agent.reasoning_effort {
            selected.effort = effort;
        }
    }
    if let Some(provider) = &record.inference.provider {
        selected.provider.clone_from(provider);
    }
    if let Some(model) = &record.inference.model {
        selected.model.clone_from(model);
    }
    if let Some(effort) = record.inference.effort {
        selected.effort = effort;
    }
    Ok(selected)
}
pub async fn session_show(
    State(state): State<AppState>,
    Path(text): Path<String>,
) -> ApiResult<Value> {
    let session_id = id(&text, SessionId::from_ulid)?;
    let record = state
        .store
        .fetch_session(session_id)
        .await
        .map_err(storage)?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "session_not_found"))?;
    let selection = selection(&state, &record).await?;
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
    Ok(Json(
        json!({"session":record,"resolved":selection,"usage":usage,"cost_dollars":usage.dollars(),"scratch":scratch,
        "requirements":requirements,"placement":placement,"address":address,"wait":wait,"events":events}),
    ))
}

pub async fn agents(
    State(state): State<AppState>,
    Query(page): Query<Page>,
) -> ApiResult<Vec<Value>> {
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
pub async fn agent_show(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<Value> {
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
pub async fn image_show(
    State(state): State<AppState>,
    Path((name, tag)): Path<(String, String)>,
) -> ApiResult<Value> {
    let manifest_id = state
        .store
        .get_image(&name, &ImageTag(tag.clone()))
        .await
        .map_err(storage)?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "image_not_found"))?;
    let scratch = state
        .store
        .image_scratch(&swarmy_core::ImageRecord {
            name: name.clone(),
            tag: ImageTag(tag.clone()),
            manifest_id,
        })
        .await
        .map_err(storage)?;
    let header = state
        .store
        .get_manifest(manifest_id)
        .await
        .map_err(storage)?
        .ok_or_else(|| error(StatusCode::INTERNAL_SERVER_ERROR, "image_manifest_missing"))?;
    Ok(Json(
        json!({"name":name,"tag":tag,"manifest_id":manifest_id,"header":header,"scratch":scratch}),
    ))
}

#[derive(serde::Deserialize)]
pub struct ModelsQuery {
    q: Option<String>,
    provider: Option<String>,
    reasoning: Option<bool>,
}
pub async fn models(
    State(state): State<AppState>,
    Query(query): Query<ModelsQuery>,
) -> ApiResult<Vec<Value>> {
    if let Some(provider) = &query.provider
        && state.catalog.provider(provider).is_none()
    {
        return Err(error(StatusCode::BAD_REQUEST, "unknown_provider"));
    }
    let mut rows = Vec::new();
    for (provider, model) in state.catalog.find(query.q.as_deref().unwrap_or("")) {
        if query.provider.as_ref().is_some_and(|id| *id != provider.id) {
            continue;
        }
        if query.reasoning.unwrap_or(false)
            && model
                .supported_efforts()
                .iter()
                .all(|effort| *effort == swarmy_core::ReasoningEffort::None)
        {
            continue;
        }
        let mut row = serde_json::to_value(model)
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "encoding_error"))?;
        row["key"] = json!(format!("{}/{}", provider.id, model.id));
        row["provider"] = json!(provider.id);
        row["effective_api"] = json!(model.api.unwrap_or(provider.api));
        row["effective_base_url"] = json!(model.base_url.as_deref().unwrap_or(&provider.base_url));
        row["supported_efforts"] = json!(model.supported_efforts());
        rows.push(row);
    }
    Ok(Json(rows))
}
pub async fn providers(State(state): State<AppState>) -> ApiResult<Vec<Value>> {
    let rows = state
        .catalog
        .providers()
        .map(|provider| {
            json!({
                "id":provider.id,"api":provider.api,"auth_kinds":provider.auth_kinds,
                "env_keys":provider.env_keys,"credential":"unknown"
            })
        })
        .collect();
    Ok(Json(rows))
}

#[derive(serde::Deserialize)]
pub struct AgentChoice {
    idempotency_key: String,
    name: Option<String>,
    image: Option<String>,
    description: Option<String>,
    provider: Option<String>,
    model: Option<String>,
    effort: Option<String>,
    system_prompt: Option<String>,
    memory: Option<u64>,
    gpu: Option<String>,
    github_token: Option<String>,
    clear_github_token: Option<bool>,
    resets: Option<Vec<String>>,
}
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
    })
}
pub async fn agent_create(
    State(state): State<AppState>,
    Json(body): Json<AgentChoice>,
) -> ApiResult<Value> {
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
            .create_agent_with_token_replay(
                &name,
                &image,
                body.description.as_deref().unwrap_or(""),
                &choice,
                jiff::Timestamp::now(),
                swarmy_store::AgentCreationReplay {
                    key: &store_key,
                    github_token: body.github_token.as_deref(),
                },
            )
            .await
            .map_err(storage)?;
        Ok(Json(json!(record)))
    })
    .await
}
pub async fn agent_update(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<AgentChoice>,
) -> ApiResult<Value> {
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
            _ => Err(error(StatusCode::BAD_REQUEST, "invalid_reset")),
        })
        .collect::<Result<Vec<_>, _>>()?;
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
            Ok(Json(json!(updated)))
        },
    )
    .await
}
#[derive(serde::Deserialize)]
pub struct CredentialInput {
    idempotency_key: String,
    provider: String,
    record: swarmy_core::CredentialRecord,
}
pub async fn credentials(State(state): State<AppState>) -> ApiResult<Vec<Value>> {
    let store = super::credential_store(&state)?;
    Ok(Json(
        store
            .list_credentials(swarmy_core::CredentialScope::Cluster)
            .await
            .map_err(storage)?
            .into_iter()
            .map(|summary| json!(summary))
            .collect(),
    ))
}
pub async fn credential(
    State(state): State<AppState>,
    Path(provider): Path<String>,
) -> ApiResult<Value> {
    let store = super::credential_store(&state)?;
    let record = store
        .get_credential(swarmy_core::CredentialScope::Cluster, &provider)
        .await
        .map_err(storage)?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "credential_not_found"))?;
    Ok(Json(json!(
        swarmy_store::credentials::CredentialSummary::new(
            provider,
            &record,
            jiff::Timestamp::now()
        )
    )))
}
pub async fn credential_set(
    State(state): State<AppState>,
    Json(body): Json<CredentialInput>,
) -> ApiResult<Value> {
    let store = super::credential_store(&state)?;
    let provider = body.provider.clone();
    super::replay(
        &state,
        &body.idempotency_key,
        &format!("cli:credentials:{provider}:set"),
        async move {
            store
                .put_credential(
                    swarmy_core::CredentialScope::Cluster,
                    &provider,
                    &body.record,
                )
                .await
                .map_err(storage)?;
            Ok(Json(json!({"saved":true})))
        },
    )
    .await
}
