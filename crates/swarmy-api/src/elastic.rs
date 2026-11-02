//! API switchboard for elastic group registration and desired count.
use crate::{ApiFailure, ApiResult, AppState, IdempotencyKey, replay, storage, views};
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use swarmy_api_types as api;
use swarmy_config::{is_opentofu_name, swarm::Module};
use swarmy_core::{ElasticGroupDefinition, HourlyPrice};

pub(super) async fn show(
    State(state): State<AppState>,
    Path(group): Path<String>,
) -> ApiResult<api::ElasticGroup> {
    let record = state
        .store
        .get_elastic_group(&group)
        .await
        .map_err(storage)?
        .ok_or_else(|| {
            ApiFailure::new(
                StatusCode::NOT_FOUND,
                "elastic_group_not_found",
                "elastic group not found",
            )
        })?;
    Ok(Json(views::elastic_group(
        &record,
        state
            .store
            .elastic_group_node_memory(&group)
            .await
            .map_err(storage)?,
    )))
}
fn invalid(message: impl Into<String>) -> ApiFailure {
    ApiFailure::new(StatusCode::BAD_REQUEST, "invalid_elastic_group", message)
}
fn definition(
    group: String,
    body: &api::PutElasticGroup,
) -> Result<ElasticGroupDefinition, ApiFailure> {
    if !is_opentofu_name(&group) || !is_opentofu_name(&body.swarm) {
        return Err(invalid(
            "swarm and group names must be 1-20 lowercase letters, digits and hyphens, starting with a letter and not ending in a hyphen",
        ));
    }
    let module = Module::try_from(body.module.clone()).map_err(invalid)?;
    if matches!(module, Module::Existing | Module::Local | Module::Path(_)) {
        return Err(invalid("elastic groups need a built-in machine module"));
    }
    if body.size.is_empty() || body.location.is_empty() {
        return Err(invalid("size and location must not be empty"));
    }
    let hourly_price = HourlyPrice::parse(&body.price_per_hour, &body.currency)
        .map_err(|e| invalid(e.to_string()))?;
    let primary_address = body
        .primary_address
        .parse::<std::net::Ipv4Addr>()
        .map_err(|_| invalid("primary_address must be the stack host's public IPv4 address"))?;
    swarmy_config::validate_service_user(&body.service_user)
        .map_err(|error| invalid(error.to_string()))?;
    if body.node_memory_mib == Some(0) {
        return Err(invalid("node_memory_mib must be at least 1"));
    }
    if body.image.as_deref() == Some("") {
        return Err(invalid("image must not be empty"));
    }
    Ok(ElasticGroupDefinition {
        swarm: body.swarm.clone(),
        group,
        module: body.module.clone(),
        size: body.size.clone(),
        location: body.location.clone(),
        sandboxes: body.sandboxes,
        local_storage: body.local_storage.clone(),
        max: body.max,
        hourly_price,
        primary_address,
        service_user: body.service_user.clone(),
        autoscale: body.autoscale,
        external_bucket: body.external_bucket,
        network_id: body.network_id.clone(),
        image: body.image.clone(),
    })
}
pub(super) async fn put(
    State(state): State<AppState>,
    Path(group): Path<String>,
    Json(body): Json<api::PutElasticGroup>,
) -> ApiResult<api::ElasticGroup> {
    let definition = definition(group.clone(), &body)?;
    let key = IdempotencyKey::parse(&body.idempotency_key)?;
    let store = state.store.clone();
    replay(
        &state,
        &key,
        &format!("swarm-groups:{group}:put"),
        async move {
            let size = body.node_memory_mib.or(store.elastic_group_node_memory(&group).await.map_err(storage)?);
            if body.autoscale && size.is_none() {
                return Err(invalid("autoscale needs node_memory_mib: add node_memory_mib = MIB to the group in the swarm spec (the memory one machine offers to computers after the node's reserve; swarmy doctor --json shows capacity.memory_bytes once a machine has joined) and run swarmy swarm scale again"));
            }
            let record = store.put_elastic_group(&definition).await.map_err(storage)?;
            if let Some(memory_mib) = body.node_memory_mib {
                store.set_elastic_group_node_memory(&group, memory_mib).await.map_err(storage)?;
            }
            Ok(Json(views::elastic_group(&record, size)))
        },
    )
    .await
}
pub(super) async fn set_desired(
    State(state): State<AppState>,
    Path(group): Path<String>,
    Json(body): Json<api::SetElasticDesired>,
) -> ApiResult<api::ElasticGroup> {
    let key = IdempotencyKey::parse(&body.idempotency_key)?;
    let store = state.store.clone();
    replay(
        &state,
        &key,
        &format!("swarm-groups:{group}:desired"),
        async move {
            Ok(Json(views::elastic_group(
                &store
                    .set_elastic_desired(
                        &group,
                        body.desired,
                        &swarmy_core::MachineCause::from_reason(
                            body.reason.as_deref().unwrap_or("set through the API"),
                        ),
                    )
                    .await
                    .map_err(storage)?,
                store
                    .elastic_group_node_memory(&group)
                    .await
                    .map_err(storage)?,
            )))
        },
    )
    .await
}
