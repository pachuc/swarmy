//! Store-to-API conversions: the single module that maps persistence
//! shapes onto the versioned JSON contract.
//!
//! Handlers render typed `swarmy_api_types` structs built here, so a renamed
//! field fails to compile at the call site. Metric shapes stay identical so
//! stored rows keep decoding, and the conversion stays here so the store
//! never compiles the `OpenAPI` tooling.

use super::{AppState, error, storage};
use axum::{Json, http::StatusCode};
use jiff::Timestamp;
use swarmy_api_types as api;
use swarmy_store as store;

#[must_use]
pub(crate) fn into_api_stage(value: store::StageTiming) -> api::StageTiming {
    api::StageTiming {
        stage: value.stage,
        request_id: value.request_id,
        clock_id: value.clock_id,
        monotonic_ns: value.monotonic_ns,
        unix_ns: value.unix_ns,
    }
}

#[must_use]
pub(crate) fn into_api_inference(value: store::InferenceMetric) -> api::InferenceMetric {
    api::InferenceMetric {
        request_id: value.request_id,
        provider: value.provider,
        model: value.model,
        input_tokens: value.input_tokens,
        cached_input_tokens: value.cached_input_tokens,
        output_tokens: value.output_tokens,
        reasoning_tokens: value.reasoning_tokens,
        cost_micros: value.cost_micros,
        time_to_first_token_ms: value.time_to_first_token_ms,
        streaming_duration_ms: value.streaming_duration_ms,
        request_duration_ms: value.request_duration_ms,
        streamed: value.streamed,
        output_tokens_per_second: value.output_tokens_per_second,
        retries: value.retries,
        rate_limit_waits: value.rate_limit_waits,
        gateway_waits: value.gateway_waits,
        provider_failures: value.provider_failures,
        error: value.error,
    }
}

#[must_use]
pub(crate) fn into_api_tool(value: store::ToolMetric) -> api::ToolMetric {
    api::ToolMetric {
        request_id: value.request_id,
        name: value.name,
        dispatched_ns: value.dispatched_ns,
        started_ns: value.started_ns,
        completed_ns: value.completed_ns,
        exit_status: value.exit_status,
        output_bytes: value.output_bytes,
        queue_ms: value.queue_ms,
        process_wall_ms: value.process_wall_ms,
    }
}

#[must_use]
pub(crate) fn into_api_computer(value: &store::ComputerMetric) -> api::ComputerMetric {
    api::ComputerMetric {
        placement_ms: value.placement_ms,
        cold: value.cold,
        chunks_fetched: value.chunks_fetched,
        bytes_fetched: value.bytes_fetched,
        fetch_p50_ms: value.fetch_p50_ms,
        fetch_p95_ms: value.fetch_p95_ms,
        first_tool_chunks_fetched: value.first_tool_chunks_fetched,
        first_tool_bytes_fetched: value.first_tool_bytes_fetched,
        first_tool_fetch_p50_ms: value.first_tool_fetch_p50_ms,
        first_tool_fetch_p95_ms: value.first_tool_fetch_p95_ms,
    }
}

#[must_use]
pub(crate) fn into_api_turn(value: store::TurnMetrics) -> api::TurnMetrics {
    api::TurnMetrics {
        session_id: value.session_id,
        turn_id: value.turn_id,
        stages: value.stages.into_iter().map(into_api_stage).collect(),
        inference: value
            .inference
            .into_iter()
            .map(into_api_inference)
            .collect(),
        tools: value.tools.into_iter().map(into_api_tool).collect(),
        computer: value.computer.as_ref().map(into_api_computer),
        append_to_first_token_ms: value.append_to_first_token_ms,
        inference_duration_ms: value.inference_duration_ms,
        append_to_idle_ms: value.append_to_idle_ms,
        error: value.error,
        dropped_stages: value.dropped_stages,
        dropped_inference: value.dropped_inference,
        dropped_tools: value.dropped_tools,
    }
}

#[must_use]
pub(crate) fn into_api_latency(value: &store::LatencyPercentiles) -> api::LatencyPercentiles {
    api::LatencyPercentiles {
        p50_ms: value.p50_ms,
        p95_ms: value.p95_ms,
    }
}

#[must_use]
pub(crate) fn into_api_agent(value: store::AgentMetrics) -> api::AgentMetrics {
    api::AgentMetrics {
        agent_id: value.agent_id,
        main_session_id: value.main_session_id,
        turns: value.turns,
        latencies: value
            .latencies
            .into_iter()
            .map(|(name, sample)| (name, into_api_latency(&sample)))
            .collect(),
        input_tokens: value.input_tokens,
        cached_input_tokens: value.cached_input_tokens,
        output_tokens: value.output_tokens,
        reasoning_tokens: value.reasoning_tokens,
        mean_output_tokens_per_second: value.mean_output_tokens_per_second,
        retries: value.retries,
        errors: value.errors,
        cost_micros: value.cost_micros,
    }
}

#[must_use]
pub(crate) fn agent(record: swarmy_core::AgentRecord) -> api::Agent {
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
        effort: record.reasoning_effort.map(Into::into),
        system_prompt: record.system_prompt,
        created_at: record.created_at.to_string(),
        main_session_id: record.main_session.map(|value| value.to_string()),
        route: record.route,
        requirements: requirements(record.requirements),
        node_id: None,
        scratch: None,
        session_count: 0,
        usage: None,
        entries: Vec::new(),
        providers: Vec::new(),
        placement: None,
        sandbox_address: None,
        last_snapshot_at: None,
        last_snapshot_age_seconds: None,
        sandbox_state: None,
        call_status: None,
        sessions: Vec::new(),
    }
}

#[must_use]
pub(crate) fn session(record: &swarmy_core::SessionRecord) -> api::Session {
    let kind = match record.kind {
        swarmy_core::SessionKind::Ephemeral => api::SessionKind::Ephemeral,
        swarmy_core::SessionKind::Named { .. } => api::SessionKind::Named,
    };
    let state = record.state.into();
    api::Session {
        id: record.session_id.to_string(),
        agent_id: matches!(kind, api::SessionKind::Named).then(|| record.agent_id.to_string()),
        kind,
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
        effort: record.inference.effort.map(Into::into),
        next_session: None,
        route: record.route.clone(),
        resolved: None,
        main: false,
        agent_name: None,
        previous_session: None,
        state_since: None,
        interrupt_requested: record.interrupt_requested,
        usage: None,
        entries: Vec::new(),
        providers: Vec::new(),
        scratch: None,
        requirements: None,
        placement: None,
        sandbox_address: None,
    }
}

#[must_use]
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

#[must_use]
pub(crate) fn scratch_view(value: &store::ScratchRecord) -> api::ScratchView {
    api::ScratchView {
        node_id: value.node_id.to_string(),
        bytes: value.bytes,
    }
}

#[must_use]
pub(crate) fn node_capacity(value: &swarmy_core::NodeCapacity) -> api::NodeCapacity {
    api::NodeCapacity {
        cpu_millis: value.cpu_millis,
        memory_bytes: value.memory_bytes,
        disk_bytes: value.disk_bytes,
        sandboxes: value.sandboxes,
    }
}

#[must_use]
pub(crate) fn service_role(value: &store::ServiceRole) -> api::ServiceRole {
    match value {
        store::ServiceRole::Scheduler => api::ServiceRole::Scheduler,
        store::ServiceRole::Worker => api::ServiceRole::Worker,
        store::ServiceRole::Gateway => api::ServiceRole::Gateway,
        store::ServiceRole::Api => api::ServiceRole::Api,
        store::ServiceRole::Node => api::ServiceRole::Node,
    }
}

#[must_use]
pub(crate) fn node_role(value: swarmy_core::NodeRole) -> api::NodeRole {
    match value {
        swarmy_core::NodeRole::Sandbox => api::NodeRole::Sandbox,
        swarmy_core::NodeRole::Volume => api::NodeRole::Volume,
    }
}

/// Provider names behind one heartbeat. Capacity heartbeats carry none, so
/// both the health and doctor views share this instead of matching twice.
#[must_use]
pub(crate) fn service_providers(detail: &store::ServiceDetail) -> Vec<String> {
    match detail {
        store::ServiceDetail::Providers(value) => value.clone(),
        _ => Vec::new(),
    }
}

/// Node capacity behind one heartbeat, if the service reports any.
#[must_use]
pub(crate) fn service_capacity(detail: &store::ServiceDetail) -> Option<api::NodeCapacity> {
    match detail {
        store::ServiceDetail::Capacity(value) => Some(node_capacity(value)),
        _ => None,
    }
}

#[must_use]
pub(crate) fn credential(value: store::credentials::CredentialSummary) -> api::Credential {
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
        expires_at: value.expires_at.map(|at| at.to_string()),
    }
}

#[must_use]
pub(crate) fn totals_view(
    totals: &swarmy_core::UsageTotals,
    completions: u64,
) -> api::UsageTotalsView {
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

#[must_use]
pub(crate) fn usage_group_view(group: &store::UsageGroup) -> api::UsageGroupView {
    api::UsageGroupView {
        start: group.start.to_string(),
        end: group.end.to_string(),
        totals: totals_view(&group.totals, group.completions),
    }
}

/// Split one owner's entry totals into per-entry views, costliest first,
/// with the distinct providers involved. Totals arrive already scoped to
/// the owner, so the key is the entry name (`provider/label`); the
/// provider is its leading segment.
pub(crate) fn entry_breakdown(
    totals: Vec<store::DimensionTotal>,
) -> (Vec<api::EntryUsageView>, Vec<String>) {
    let mut entries: Vec<api::EntryUsageView> = totals
        .into_iter()
        .map(|total| {
            let provider = total
                .key
                .split_once('/')
                .map_or_else(|| total.key.clone(), |(provider, _)| provider.to_owned());
            api::EntryUsageView {
                entry: total.key,
                provider,
                totals: totals_view(&total.totals, total.completions),
            }
        })
        .collect();
    entries.sort_by(|left, right| {
        right
            .totals
            .cost_micros
            .cmp(&left.totals.cost_micros)
            .then_with(|| left.entry.cmp(&right.entry))
    });
    let providers: Vec<String> = entries
        .iter()
        .map(|entry| entry.provider.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    (entries, providers)
}

#[must_use]
pub(crate) fn model(
    provider: &swarmy_llm::catalog::ProviderInfo,
    entry: &swarmy_llm::catalog::ModelInfo,
) -> api::Model {
    api::Model {
        id: entry.id.clone(),
        provider: provider.id.clone(),
        context_window: entry.limit.context,
        key: format!("{}/{}", provider.id, entry.id),
        name: entry.name.clone(),
        limit: api::ModelLimit {
            context: entry.limit.context,
            output: entry.limit.output,
        },
        cost: api::ModelCost {
            input: entry.cost.input,
            output: entry.cost.output,
        },
        supported_efforts: entry
            .supported_efforts()
            .into_iter()
            .map(Into::into)
            .collect(),
        effective_api: serde_json::to_value(entry.api.unwrap_or(provider.api))
            .expect("catalog api serializes")
            .as_str()
            .expect("catalog api is a string")
            .to_owned(),
        effective_base_url: entry
            .base_url
            .clone()
            .unwrap_or_else(|| provider.base_url.clone()),
        compat: entry.compat.0.clone(),
    }
}

fn resolve_with_agent(
    record: &swarmy_core::SessionRecord,
    agent: Option<&swarmy_core::AgentRecord>,
    default: &swarmy_core::ResolvedSelection,
) -> swarmy_core::ResolvedSelection {
    let selected = agent.map_or_else(
        || default.clone(),
        |agent| agent.inference().resolve(default),
    );
    record.inference.resolve(&selected)
}

/// Cached agent rows for one session page. A missing agent is cached as
/// `None` so an unknown id costs one store read per page, not one per row.
pub(crate) type AgentCache =
    std::collections::HashMap<swarmy_core::AgentId, Option<swarmy_core::AgentRecord>>;

pub(crate) async fn session_with_next(
    state: &AppState,
    record: &swarmy_core::SessionRecord,
    agents: &mut AgentCache,
) -> Result<api::Session, (StatusCode, Json<api::ApiError>)> {
    let mut result = session(record);
    if record.state == swarmy_core::SessionState::Completed {
        result.next_session = state
            .store
            .next_session(record.session_id)
            .await
            .map_err(storage)?
            .map(|id| id.to_string());
    }
    result.state_since = state
        .store
        .session_state_since(record.session_id)
        .await
        .map_err(storage)?
        .map(|at| at.to_string());
    result.previous_session = state
        .store
        .previous_session(record.session_id)
        .await
        .map_err(storage)?
        .map(|id| id.to_string());
    let cached = match agents.entry(record.agent_id) {
        std::collections::hash_map::Entry::Vacant(entry) => entry.insert(
            state
                .store
                .get_agent(record.agent_id)
                .await
                .map_err(storage)?,
        ),
        std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
    };
    let agent = cached.as_ref();
    if let Some(agent) = agent {
        result.main = agent.main_session == Some(record.session_id);
        result.agent_name = Some(agent.name.clone());
    }
    let resolved = resolve_with_agent(record, agent, &state.default_selection);
    result.resolved = Some(resolved.into());
    Ok(result)
}

pub(crate) async fn populate_session_detail(
    state: &AppState,
    result: &mut api::Session,
    record: &swarmy_core::SessionRecord,
    agent: Option<&swarmy_core::AgentRecord>,
) -> Result<(), (StatusCode, Json<api::ApiError>)> {
    if let Some(wait) = state
        .store
        .inference_wait(record.session_id)
        .await
        .map_err(storage)?
    {
        result.waiting = Some(api::WaitingReason {
            wake_at: Some(wait.wake_at.to_string()),
            reasons: wait.reasons,
        });
    }
    let usage = state
        .store
        .session_usage(record.session_id)
        .await
        .map_err(storage)?;
    result.usage = Some(totals_view(&usage, 0));
    let (entries, providers) = entry_breakdown(
        state
            .store
            .dimension_totals(
                store::MeteringDimension::SessionEntry,
                &record.session_id.to_string(),
                None,
            )
            .await
            .map_err(storage)?,
    );
    result.entries = entries;
    result.providers = providers;
    if let Some(scratch) = state
        .store
        .scratch(record.agent_id)
        .await
        .map_err(storage)?
    {
        result.scratch = Some(scratch_view(&scratch));
    }
    if let Some(agent) = agent {
        result.requirements = Some(requirements(agent.requirements));
    } else if let Some(image) = state
        .store
        .pinned_image(record.session_id)
        .await
        .map_err(storage)?
    {
        let memory_mib = state
            .store
            .image_memory(&image)
            .await
            .map_err(storage)?
            .unwrap_or(swarmy_core::SandboxRequirements::default().memory_mib);
        result.requirements = Some(requirements(swarmy_core::SandboxRequirements {
            memory_mib,
            gpu: swarmy_core::GpuRequirement::default(),
        }));
    } else {
        result.requirements = Some(requirements(swarmy_core::SandboxRequirements::default()));
    }
    let placement = state
        .store
        .get_by_agent(record.agent_id)
        .await
        .map_err(storage)?;
    if let Some(placement) = &placement {
        result.sandbox_address = state
            .store
            .placement_address(placement)
            .await
            .map_err(storage)?
            .map(|address| address.to_string());
        result.placement = Some(api::PlacementView {
            node_id: placement.node_id.to_string(),
            epoch: placement.epoch,
        });
    }
    Ok(())
}

async fn agent_base(
    state: &AppState,
    record: swarmy_core::AgentRecord,
) -> Result<
    (
        api::Agent,
        swarmy_core::AgentId,
        Option<swarmy_core::PlacementRecord>,
    ),
    (StatusCode, Json<api::ApiError>),
> {
    let agent_id = record.agent_id;
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
    let mut view = agent(record);
    view.node_id = placement.as_ref().map(|value| value.node_id.to_string());
    view.scratch = scratch.as_ref().map(scratch_view);
    Ok((view, agent_id, placement))
}

/// List view: placement and scratch plus the session count from one index
/// scan. Hydrating every session just to count them cost one fetch per
/// session on every agent row.
pub(crate) async fn agent_summary(
    state: &AppState,
    record: swarmy_core::AgentRecord,
) -> Result<api::Agent, (StatusCode, Json<api::ApiError>)> {
    let (mut view, agent_id, _) = agent_base(state, record).await?;
    view.session_count = state
        .store
        .count_sessions_by_agent(agent_id)
        .await
        .map_err(storage)?;
    Ok(view)
}

/// Detail view: the full session list with usage, placement, and sandbox
/// state for `agent show`.
pub(crate) async fn agent_detail(
    state: &AppState,
    record: swarmy_core::AgentRecord,
) -> Result<api::Agent, (StatusCode, Json<api::ApiError>)> {
    let (mut view, agent_id, placement) = agent_base(state, record).await?;
    let mut sessions = Vec::new();
    let mut after = None;
    loop {
        let page = state
            .store
            .list_sessions_by_agent(agent_id, after, store::MAX_SCAN_LIMIT)
            .await
            .map_err(storage)?;
        if page.is_empty() {
            break;
        }
        after = page.last().map(|session| session.session_id);
        sessions.extend(page);
    }
    view.session_count = sessions.len();
    populate_agent_detail(state, &mut view, agent_id, placement, sessions).await?;
    Ok(view)
}

async fn populate_agent_detail(
    state: &AppState,
    view: &mut api::Agent,
    agent_id: swarmy_core::AgentId,
    placement: Option<swarmy_core::PlacementRecord>,
    sessions: Vec<swarmy_core::SessionRecord>,
) -> Result<(), (StatusCode, Json<api::ApiError>)> {
    let totals = state.store.agent_usage(agent_id).await.map_err(storage)?;
    view.usage = Some(totals_view(&totals, 0));
    let (entries, providers) = entry_breakdown(
        state
            .store
            .dimension_totals(
                store::MeteringDimension::AgentEntry,
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
        .get_volume(swarmy_core::VolumeId::from_ulid(agent_id.as_ulid()))
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
    for record in sessions {
        let next = state
            .store
            .next_session(record.session_id)
            .await
            .map_err(storage)?;
        let mut item = session(&record);
        item.next_session = next.map(|id| id.to_string());
        item.main = view.main_session_id.as_deref() == Some(item.id.as_str());
        view.sessions.push(item);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Pin the full API JSON for one fully populated turn: every converted
    /// field appears, so a dropped or renamed field fails here instead of
    /// silently vanishing from the contract.
    // The pinned literal must stay adjacent to the fixture it pins;
    // splitting them into helpers would let the two drift apart unseen.
    #[expect(
        clippy::too_many_lines,
        reason = "the exact-JSON pin needs its full fixture and literal inline"
    )]
    #[test]
    fn stage_view_pins_every_field() {
        let converted = into_api_stage(store::StageTiming {
            stage: "appended".into(),
            request_id: Some("req-1".into()),
            clock_id: "boot".into(),
            monotonic_ns: 11,
            unix_ns: 22,
        });
        assert_eq!(
            converted,
            api::StageTiming {
                stage: "appended".into(),
                request_id: Some("req-1".into()),
                clock_id: "boot".into(),
                monotonic_ns: 11,
                unix_ns: 22,
            }
        );
    }

    #[test]
    fn inference_view_pins_literal_values() {
        let converted = into_api_inference(store::InferenceMetric {
            request_id: "r-7".into(),
            provider: "fake".into(),
            model: "scripted".into(),
            input_tokens: 10,
            cached_input_tokens: 1,
            output_tokens: 4,
            cost_micros: 1_200,
            streamed: Some(false),
            retries: 2,
            ..store::InferenceMetric::default()
        });
        assert_eq!(converted.request_id, "r-7");
        assert_eq!(converted.provider, "fake");
        assert_eq!(converted.model, "scripted");
        assert_eq!(converted.input_tokens, 10);
        assert_eq!(converted.cached_input_tokens, 1);
        assert_eq!(converted.output_tokens, 4);
        assert_eq!(converted.cost_micros, 1_200);
        assert_eq!(converted.streamed, Some(false));
        assert_eq!(converted.retries, 2);
        assert_eq!(converted.rate_limit_waits, 0);
    }

    #[test]
    fn tool_view_pins_literal_values() {
        let converted = into_api_tool(store::ToolMetric {
            request_id: "c-3".into(),
            name: "bash".into(),
            dispatched_ns: Some(5),
            started_ns: Some(6),
            completed_ns: Some(9),
            exit_status: Some(0),
            output_bytes: Some(128),
            ..store::ToolMetric::default()
        });
        assert_eq!(converted.request_id, "c-3");
        assert_eq!(converted.name, "bash");
        assert_eq!(converted.dispatched_ns, Some(5));
        assert_eq!(converted.started_ns, Some(6));
        assert_eq!(converted.completed_ns, Some(9));
        assert_eq!(converted.exit_status, Some(0));
        assert_eq!(converted.output_bytes, Some(128));
    }

    #[test]
    fn computer_view_pins_literal_values() {
        let converted = into_api_computer(&store::ComputerMetric {
            placement_ms: Some(31.5),
            cold: Some(true),
            chunks_fetched: 7,
            bytes_fetched: 9,
            ..store::ComputerMetric::default()
        });
        assert_eq!(converted.placement_ms, Some(31.5));
        assert_eq!(converted.cold, Some(true));
        assert_eq!(converted.chunks_fetched, 7);
        assert_eq!(converted.bytes_fetched, 9);
        assert_eq!(converted.fetch_p50_ms, None);
    }

    #[test]
    fn turn_view_keeps_counts_and_children() {
        let converted = into_api_turn(store::TurnMetrics {
            session_id: "s-1".into(),
            turn_id: "t-2".into(),
            stages: vec![store::StageTiming {
                stage: "appended".into(),
                request_id: None,
                clock_id: "boot".into(),
                monotonic_ns: 11,
                unix_ns: 22,
            }],
            inference: vec![store::InferenceMetric {
                request_id: "r-7".into(),
                provider: "fake".into(),
                model: "scripted".into(),
                ..store::InferenceMetric::default()
            }],
            tools: vec![store::ToolMetric {
                request_id: "c-3".into(),
                name: "bash".into(),
                ..store::ToolMetric::default()
            }],
            computer: None,
            dropped_stages: 1,
            dropped_inference: 2,
            dropped_tools: 3,
            ..store::TurnMetrics::default()
        });
        assert_eq!(converted.session_id, "s-1");
        assert_eq!(converted.turn_id, "t-2");
        assert_eq!(converted.stages.len(), 1);
        assert_eq!(converted.stages[0].monotonic_ns, 11);
        assert_eq!(converted.stages[0].unix_ns, 22);
        assert_eq!(converted.inference.len(), 1);
        assert_eq!(converted.inference[0].provider, "fake");
        assert_eq!(converted.tools.len(), 1);
        assert_eq!(converted.tools[0].name, "bash");
        assert!(converted.computer.is_none());
        assert_eq!(converted.dropped_stages, 1);
        assert_eq!(converted.dropped_inference, 2);
        assert_eq!(converted.dropped_tools, 3);
    }

    #[test]
    fn agent_metrics_view_pins_latency() {
        let converted = into_api_agent(store::AgentMetrics {
            agent_id: "a-9".into(),
            main_session_id: Some("s-1".into()),
            turns: 3,
            latencies: std::collections::BTreeMap::from([(
                "append_to_idle".into(),
                store::LatencyPercentiles {
                    p50_ms: 1.5,
                    p95_ms: 2.5,
                },
            )]),
            input_tokens: 100,
            cost_micros: 50,
            ..store::AgentMetrics::default()
        });
        assert_eq!(converted.agent_id, "a-9");
        assert_eq!(converted.main_session_id.as_deref(), Some("s-1"));
        assert_eq!(converted.turns, 3);
        assert_eq!(converted.input_tokens, 100);
        assert_eq!(converted.cost_micros, 50);
        let latency = converted.latencies["append_to_idle"].clone();
        assert!((latency.p50_ms - 1.5).abs() < f64::EPSILON);
        assert!((latency.p95_ms - 2.5).abs() < f64::EPSILON);
    }
}
