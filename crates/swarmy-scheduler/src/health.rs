//! Scheduler health reporting: the periodic heartbeat and expiry sweep.
//!
//! Extracted from `main.rs` so the binary stays wiring over the scheduler,
//! collector, and ephemeral-reaper loops. The loop itself lives on
//! [`swarmy_store::Store::heartbeat_loop`]; this wrapper supplies the
//! scheduler role, version, and expiry behaviour.

use jiff::Timestamp;
use swarmy_store::{ServiceDetail, ServiceRole, Store};

/// Report this instance and expire stale service records until the process
/// ends. Heartbeat and expiry failures only warn; the next tick retries.
pub async fn run(store: &Store, instance_id: String, started: Timestamp, partitions: Vec<u16>) {
    store
        .heartbeat_loop(
            ServiceRole::Scheduler,
            instance_id,
            env!("CARGO_PKG_VERSION").into(),
            started,
            ServiceDetail::Partitions(partitions),
            true,
        )
        .await;
}
