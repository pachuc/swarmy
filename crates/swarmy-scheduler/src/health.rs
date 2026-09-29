//! Scheduler health reporting: the periodic heartbeat and expiry sweep.
//!
//! Extracted from `main.rs` so the binary stays wiring over the scheduler,
//! collector, and ephemeral-reaper loops.

use jiff::Timestamp;
use swarmy_store::{ServiceDetail, ServiceHeartbeat, ServiceRole, Store};
use tokio::time::{Duration, interval};

/// How often the scheduler advertises itself and expires stale services.
pub const HEALTH_INTERVAL: Duration = Duration::from_secs(30);

/// Report this instance and expire stale service records until the process
/// ends. Heartbeat and expiry failures only warn; the next tick retries.
pub async fn run(
    store: &Store,
    instance_id: String,
    started: Timestamp,
    partitions: Vec<u16>,
) {
    let mut ticks = interval(HEALTH_INTERVAL);
    loop {
        ticks.tick().await;
        let record = ServiceHeartbeat {
            role: ServiceRole::Scheduler,
            instance_id: instance_id.clone(),
            version: env!("CARGO_PKG_VERSION").into(),
            host: std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into()),
            started_at: started,
            last_seen: Timestamp::now(),
            detail: ServiceDetail::Partitions(partitions.clone()),
        };
        if let Err(error) = store.put_service_heartbeat(&record).await {
            tracing::warn!(%error, "scheduler health heartbeat failed");
        }
        if let Err(error) = store.expire_services().await {
            tracing::warn!(%error, "service health expiry failed");
        }
    }
}
