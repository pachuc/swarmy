//! Chunk collection runs. Starting a run acquires the collector lease and
//! returns immediately; the sweep continues in the background and the client
//! follows its durable run record.
use super::{ApiResult, AppState, error, storage, volume};
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use swarmy_api_types as api;
use swarmy_core::LeaseOwnerId;
use ulid::Ulid;

fn snapshot(run: &swarmy_core::GcRun) -> api::GcRun {
    api::GcRun {
        run_id: run.owner.to_string(),
        started_at: run.started_at.to_string(),
        dry_run: run.dry_run,
        finished: run.finished,
        error: run.error.clone(),
        manifests: run.manifests,
        scanned: run.scanned,
        candidates: run.candidates,
        candidate_bytes: run.candidate_bytes,
        deleted: run.deleted,
        bytes_freed: run.bytes_freed,
        duration_ms: run.duration_ms,
    }
}

fn busy() -> (StatusCode, Json<api::ApiError>) {
    (
        StatusCode::CONFLICT,
        Json(api::ApiError {
            code: "gc_busy".into(),
            message: "another collection run holds the lease".into(),
            provider_text: None,
        }),
    )
}

pub async fn start(
    State(state): State<AppState>,
    Json(body): Json<api::StartGcRun>,
) -> ApiResult<api::GcRun> {
    if body.idempotency_key.is_empty() || body.idempotency_key.len() > 256 {
        return Err(error(StatusCode::BAD_REQUEST, "invalid_idempotency_key"));
    }
    let replay_key = format!("gc:runs:{}", body.idempotency_key);
    {
        let _guard = state.mutation_guard().await;
        if let Some(value) = state.store.api_replay(&replay_key).await.map_err(storage)? {
            let run_id: String = serde_json::from_value(value)
                .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "corrupt_replay"))?;
            let owner = run_id
                .parse::<Ulid>()
                .map(LeaseOwnerId::from_ulid)
                .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "corrupt_replay"))?;
            let run = state
                .store
                .get_gc_run(owner)
                .await
                .map_err(storage)?
                .ok_or_else(|| error(StatusCode::INTERNAL_SERVER_ERROR, "corrupt_replay"))?;
            return Ok(Json(snapshot(&run)));
        }
    }
    let policy = state.gc;
    let (run, lease, cutoff, started) =
        swarmy_volume::gc::begin(&state.store, policy, body.dry_run)
            .await
            .map_err(|error| {
                if matches!(
                    error,
                    swarmy_volume::VolumeError::Store(swarmy_store::StoreError::LeaseMismatch)
                ) {
                    busy()
                } else {
                    volume(error)
                }
            })?;
    {
        let _guard = state.mutation_guard().await;
        let value = serde_json::to_value(run.owner.to_string())
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "encoding_error"))?;
        state
            .store
            .put_api_replay(&replay_key, value)
            .await
            .map_err(storage)?;
    }
    let initial = snapshot(&run);
    let background = state.clone();
    tokio::spawn(async move {
        if let Err(error) = swarmy_volume::gc::complete(
            &background.store,
            background.objects.clone(),
            policy,
            run,
            lease,
            cutoff,
            started,
        )
        .await
        {
            tracing::warn!(%error, "background collection run failed");
        }
    });
    Ok(Json(initial))
}

pub async fn show(
    State(state): State<AppState>,
    Path(text): Path<String>,
) -> ApiResult<api::GcRun> {
    let owner = text
        .parse::<Ulid>()
        .map(LeaseOwnerId::from_ulid)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid_id"))?;
    let run = state
        .store
        .get_gc_run(owner)
        .await
        .map_err(storage)?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "gc_run_not_found"))?;
    Ok(Json(snapshot(&run)))
}
