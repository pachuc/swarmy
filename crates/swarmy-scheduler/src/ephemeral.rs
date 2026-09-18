//! Competing schedulers can sweep safely because each closure rechecks durable state.
use std::{num::NonZeroU64, time::Duration};
use swarmy_store::Store;

pub async fn run(store: &Store, retention_seconds: NonZeroU64) {
    let retention = Duration::from_secs(retention_seconds.get());
    let interval = retention.min(Duration::from_secs(60));
    loop {
        tokio::time::sleep(interval).await;
        match store
            .sweep_ephemeral_sessions(jiff::Timestamp::now(), retention)
            .await
        {
            Ok(closed) => tracing::info!(closed, "ephemeral session sweep finished"),
            Err(error) => {
                tracing::error!(%error, "ephemeral session sweep failed; retry next interval");
            }
        }
    }
}
