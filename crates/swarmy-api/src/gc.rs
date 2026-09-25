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
    // Reserve the replay key before acquiring the collector lease. If the
    // store fails after `begin`, the sweep is already running under the lease
    // and a retry would only get a conflict; reserving first means a retry
    // with the same key observes this attempt instead.
    let owner = LeaseOwnerId::from_ulid(Ulid::generate());
    let reserved = serde_json::to_value(owner.to_string()).expect("run id serializes");
    {
        let _guard = state.mutation_guard().await;
        state
            .store
            .put_api_replay(&replay_key, reserved.clone())
            .await
            .map_err(storage)?;
    }
    // Benchmarks on isolated namespaces pass a short grace for one run; a
    // zero grace would collect chunks still being published, so reject it.
    let mut policy = state.gc;
    if let Some(grace) = body.grace_seconds {
        policy.grace_seconds = std::num::NonZeroU64::new(grace)
            .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid_request"))?;
    }
    let (run, lease, cutoff, started) = match swarmy_volume::gc::begin_with_owner(
        &state.store,
        policy,
        body.dry_run,
        owner,
    )
    .await
    {
        Ok(started) => started,
        Err(error) => {
            // The sweep never started; release the reservation so a retry
            // with the same key starts a fresh attempt instead of
            // replaying a run id that was never recorded.
            let _ = state.store.remove_api_replay(&replay_key, &reserved).await;
            return Err(match error {
                swarmy_volume::VolumeError::Store(swarmy_store::StoreError::LeaseMismatch) => {
                    busy()
                }
                other => volume(other),
            });
        }
    };
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
            // `complete` already wrote the failure to the run record without
            // the lease when it lost it; this covers the gap where the record
            // write itself failed, so followers still stop with an error.
            tracing::warn!(%error, "background collection run failed");
            if let Ok(Some(mut current)) = background.store.get_gc_run(owner).await
                && !current.finished
            {
                current.finished = true;
                if current.error.is_none() {
                    current.error = Some(error.to_string());
                }
                let _ = background.store.fail_gc_run(owner, &current).await;
            }
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
