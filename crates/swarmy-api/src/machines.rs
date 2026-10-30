//! Read-only machine-ledger API.
use crate::{ApiFailure, ApiResult, AppState, storage, time_bound, views};
use axum::extract::{Query, State};
use axum::{Json, http::StatusCode};
use jiff::Timestamp;
use serde::Deserialize;
use std::collections::HashMap;
use swarmy_api_types as api;
use swarmy_core::AgentId;
use swarmy_store::MachineFilter;

#[derive(Deserialize)]
pub(crate) struct MachinesQuery {
    state: Option<String>,
    from: Option<String>,
    to: Option<String>,
}

pub(crate) async fn list(
    State(state): State<AppState>,
    Query(query): Query<MachinesQuery>,
) -> ApiResult<api::MachineList> {
    let as_of = Timestamp::now();
    let filter = match query.state.as_deref().unwrap_or("active") {
        "active" => MachineFilter::Active,
        "all" => MachineFilter::All,
        _ => {
            return Err(ApiFailure::new(
                StatusCode::BAD_REQUEST,
                "invalid_state",
                "state must be active or all",
            ));
        }
    };
    let window = match (query.from.as_deref(), query.to.as_deref()) {
        (None, None) => None,
        (Some(from), Some(to)) => {
            let from = time_bound(from, as_of, "invalid_from", "from is not a valid time")?;
            let to = time_bound(to, as_of, "invalid_to", "to is not a valid time")?;
            if to <= from {
                return Err(ApiFailure::new(
                    StatusCode::BAD_REQUEST,
                    "invalid_range",
                    "to must be after from",
                ));
            }
            Some((from, to))
        }
        _ => {
            return Err(ApiFailure::new(
                StatusCode::BAD_REQUEST,
                "invalid_range",
                "from and to must be supplied together",
            ));
        }
    };
    let records = state
        .store
        .list_machines(None, filter)
        .await
        .map_err(storage)?;
    let mut names: HashMap<AgentId, Option<String>> = HashMap::new();
    let mut open_rows = Vec::new();
    for record in &records {
        let open = state
            .store
            .open_machine_assignments(record.machine_id)
            .await
            .map_err(storage)?;
        for id in open.iter().map(|row| row.agent).chain(record.cause.agent) {
            if let std::collections::hash_map::Entry::Vacant(entry) = names.entry(id) {
                entry.insert(
                    state
                        .store
                        .get_agent(id)
                        .await
                        .map_err(storage)?
                        .map(|agent| agent.name),
                );
            }
        }
        open_rows.push(open);
    }
    let context = views::MachineContext {
        as_of,
        window,
        names: &names,
    };
    let machines: Vec<_> = records
        .iter()
        .zip(&open_rows)
        .map(|(record, open)| views::machine_view(record, open, &context))
        .collect();
    let totals = views::machine_totals(&machines);
    Ok(Json(api::MachineList {
        as_of: as_of.to_string(),
        from: window.map(|(from, _)| from.to_string()),
        to: window.map(|(_, to)| to.to_string()),
        machines,
        totals,
    }))
}
