//! Each scheduler attempts collection on a timer; the store lease elects a runner.
use std::{sync::Arc, time::Duration};

use object_store::ObjectStore;
use swarmy_config::GarbageCollection;
use swarmy_store::{Store, StoreError};
use swarmy_volume::VolumeError;

pub async fn run(store: &Store, objects: Arc<dyn ObjectStore>, policy: GarbageCollection) {
    loop {
        tokio::time::sleep(Duration::from_secs(policy.interval_seconds.get())).await;
        match swarmy_volume::gc::collect(store, objects.clone(), policy, false).await {
            Ok(run) => tracing::info!(?run, "chunk collection finished"),
            Err(VolumeError::Store(StoreError::LeaseMismatch)) => {
                tracing::debug!("collector lease busy or lost; retry next interval");
            }
            Err(error) => tracing::error!(%error, "chunk collection failed; retry next interval"),
        }
    }
}
