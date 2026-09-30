//! Chunk collection runs. Starting a run acquires the collector lease and
//! returns immediately; the sweep continues in the background and the client
//! follows its durable run record.
use super::idempotency::{IdempotencyKey, record_replay, replayed};
use super::{ApiFailure, ApiResult, AppState, storage, volume};
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

fn busy() -> ApiFailure {
    ApiFailure::new(
        StatusCode::CONFLICT,
        "gc_busy",
        "another collection run holds the lease",
    )
}

pub(crate) async fn start(
    State(state): State<AppState>,
    Json(body): Json<api::StartGcRun>,
) -> ApiResult<api::GcRun> {
    let key = IdempotencyKey::parse(&body.idempotency_key)?;
    let replay_key = key.scoped("gc:runs");
    {
        let _guard = state.mutation_guard().await;
        if let Some(run_id) = replayed::<String>(&state, &replay_key).await? {
            let owner = run_id
                .parse::<Ulid>()
                .map(LeaseOwnerId::from_ulid)
                .map_err(|cause| {
                    ApiFailure::caused(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "corrupt_replay",
                        "stored replay response is corrupt",
                        &cause,
                    )
                })?;
            // Between the replay-key reservation and the lease acquisition
            // the run record does not exist yet; a concurrent start with the
            // same key observes the reservation, so answer 409 rather than a
            // 500 for a record that is still being created.
            let run = state
                .store
                .get_gc_run(owner)
                .await
                .map_err(storage)?
                .ok_or_else(busy)?;
            return Ok(Json(snapshot(&run)));
        }
    }
    // Reserve the replay key before acquiring the collector lease. If the
    // store fails after `begin`, the sweep is already running under the lease
    // and a retry would only get a conflict; reserving first means a retry
    // with the same key observes this attempt instead.
    let owner = LeaseOwnerId::from_ulid(Ulid::generate());
    let reserved = {
        let _guard = state.mutation_guard().await;
        record_replay(&state, &replay_key, &owner.to_string()).await?
    };
    // Benchmarks on isolated namespaces pass a short grace for one run; a
    // zero grace would collect chunks still being published, so reject it.
    let mut policy = state.gc;
    if let Some(grace) = body.grace_seconds {
        if grace == 0 {
            return Err(ApiFailure::new(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "grace_seconds must be greater than zero",
            ));
        }
        policy.grace_secs = std::time::Duration::from_secs(grace);
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
            if let Err(error) = state.store.remove_api_replay(&replay_key, &reserved).await {
                tracing::warn!(error = %swarmy_core::error_chain(&error), "gc replay reservation release failed");
            }
            return Err(match error {
                swarmy_volume::VolumeError::Store(swarmy_store::StoreError::Fence(
                    swarmy_store::FenceError::GcLeaseMismatch,
                )) => busy(),
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
            tracing::warn!(error = %swarmy_core::error_chain(&error), "background collection run failed");
            if let Ok(Some(mut current)) = background.store.get_gc_run(owner).await
                && !current.finished
            {
                current.finished = true;
                if current.error.is_none() {
                    current.error = Some(error.to_string());
                }
                if let Err(error) = background.store.fail_gc_run(owner, &current).await {
                    tracing::warn!(error = %swarmy_core::error_chain(&error), "gc failure record write failed");
                }
            }
        }
    });
    Ok(Json(initial))
}

pub(crate) async fn show(
    State(state): State<AppState>,
    Path(text): Path<String>,
) -> ApiResult<api::GcRun> {
    let owner = text
        .parse::<Ulid>()
        .map(LeaseOwnerId::from_ulid)
        .map_err(|_| {
            ApiFailure::new(
                StatusCode::BAD_REQUEST,
                "invalid_id",
                "id is not a valid ULID",
            )
        })?;
    let run = state
        .store
        .get_gc_run(owner)
        .await
        .map_err(storage)?
        .ok_or_else(|| {
            ApiFailure::new(
                StatusCode::NOT_FOUND,
                "gc_run_not_found",
                "collection run not found",
            )
        })?;
    Ok(Json(snapshot(&run)))
}
