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
    Ok(Json(views::elastic_group(&record)))
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
            Ok(Json(views::elastic_group(
                &store
                    .put_elastic_group(&definition)
                    .await
                    .map_err(storage)?,
            )))
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
            )))
        },
    )
    .await
}
