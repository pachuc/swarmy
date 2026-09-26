//! Each scheduler attempts collection on a timer; the store lease elects a runner.
use std::{sync::Arc, time::Duration};

use object_store::ObjectStore;
use swarmy_config::{GarbageCollection, Metering};
use swarmy_store::{Store, StoreError};
use swarmy_volume::VolumeError;

pub async fn run(
    store: &Store,
    objects: Arc<dyn ObjectStore>,
    policy: GarbageCollection,
    metering: Metering,
) {
    loop {
        tokio::time::sleep(Duration::from_secs(policy.interval_seconds.get())).await;
        match swarmy_volume::gc::collect(store, objects.clone(), policy, false).await {
            Ok(run) => tracing::info!(?run, "chunk collection finished"),
            Err(VolumeError::Store(StoreError::LeaseMismatch)) => {
                tracing::debug!("collector lease busy or lost; retry next interval");
            }
            Err(error) => tracing::error!(%error, "chunk collection failed; retry next interval"),
        }
        prune_metering(store, metering).await;
    }
}

/// Delete raw metering records older than the retention window, keeping
/// hourly rollups. Failures are logged without failing chunk collection.
async fn prune_metering(store: &Store, metering: Metering) {
    let days = metering.raw_retention_days.get();
    let Some(cutoff) = jiff::Timestamp::now()
        .as_second()
        .checked_sub(
            i64::try_from(days)
                .unwrap_or(i64::MAX)
                .saturating_mul(86_400),
        )
        .and_then(|second| jiff::Timestamp::from_second(second).ok())
    else {
        return;
    };
    match store
        .prune_metering_raw(cutoff, swarmy_store::MAX_SCAN_LIMIT)
        .await
    {
        Ok(0) => {}
        Ok(pruned) => tracing::info!(pruned, "metering raw records pruned"),
        Err(error) => tracing::warn!(%error, "metering prune failed; will retry"),
    }
}
