mod config;
mod scheduler;

use std::sync::Arc;

use anyhow::Context;
use swarmy_bus::{Bus, SubjectToken};
use swarmy_store::{Store, blob::ObjectBlobStore};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let config = config::Config::from_env()?;
    let cluster =
        std::env::var("SWARMY_FDB_CLUSTER_FILE").context("SWARMY_FDB_CLUSTER_FILE must be set")?;
    let url = std::env::var("SWARMY_NATS_URL").context("SWARMY_NATS_URL must be set")?;
    let directory: Vec<String> = config::setting("SWARMY_STORE_DIRECTORY", "swarmy")?
        .split('/')
        .map(str::to_owned)
        .collect();
    anyhow::ensure!(
        directory.iter().all(|part| !part.is_empty()),
        "SWARMY_STORE_DIRECTORY must contain nonempty path components"
    );
    let prefix = config::setting("SWARMY_BUS_PREFIX", "")?;
    let bus_config = swarmy_bus::Config {
        prefix: if prefix.is_empty() {
            None
        } else {
            Some(SubjectToken::new(prefix)?)
        },
        ..Default::default()
    };
    let blobs = Arc::new(ObjectBlobStore::from_env()?);
    let _network = swarmy_store::boot();
    let store = Store::open(Some(&cluster), Some(&directory), blobs).await?;
    let bus = Bus::connect(&url, bus_config).await?;
    // Workers create consumers for their routes; the scheduler only needs streams.
    bus.setup(&[]).await?;
    tracing::info!(partitions = ?config.partitions, "scheduler started");
    let scheduler = scheduler::Scheduler::new(store, bus, config);
    tokio::select! {
        result = scheduler.run() => result?,
        result = tokio::signal::ctrl_c() => result?,
    }
    Ok(())
}
