//! Competing schedulers can sweep safely because each closure rechecks durable state.
use std::time::Duration;
use swarmy_store::Store;

pub(crate) async fn run(store: &Store, retention: Duration) {
    let interval = retention.min(Duration::from_secs(60));
    loop {
        tokio::time::sleep(interval).await;
        match store
            .sweep_ephemeral_sessions(jiff::Timestamp::now(), retention)
            .await
        {
            Ok(closed) => tracing::info!(closed, "ephemeral session sweep finished"),
            Err(error) => {
                tracing::error!(error = %swarmy_core::error_chain(&error), "ephemeral session sweep failed; retry next interval");
            }
        }
    }
}
