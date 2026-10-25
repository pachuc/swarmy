use crate::{config, ephemeral, gc, health, scheduler};
use std::ffi::OsString;

use swarmy_bus::Bus;
use swarmy_store::Store;

/// Run the scheduler role.
///
/// # Errors
/// Returns startup or service failures.
pub fn run(args: Vec<OsString>) -> anyhow::Result<()> {
    tokio::runtime::Runtime::new()?.block_on(Box::pin(serve(args)))
}

async fn serve(args: Vec<OsString>) -> anyhow::Result<()> {
    swarmy_version::parse_from::<swarmy_version::ServiceArgs>("swarmy-core scheduler", args)?;
    swarmy_config::init_tracing();
    let config = config::Config::from_env()?;
    let settings = swarmy_config::Settings::load()?.settings;
    let url = settings.bus.nats_url.clone();
    let bus_config = settings.bus.bus_config()?;
    let _network = swarmy_store::boot();
    let opened = Store::open_store(&settings).await?;
    let store = opened.store;
    let objects = opened.blobs.object_store();
    let bus = Bus::connect(&url, bus_config).await?;
    // Workers create consumers for their routes; the scheduler only needs streams.
    bus.setup(&[]).await?;
    tracing::info!(partitions = ?config.partitions, "scheduler started");
    let partitions: Vec<_> = config.partitions.iter().copied().collect();
    let scheduler = scheduler::Scheduler::new(store.clone(), bus, config);
    let started = jiff::Timestamp::now();
    let id = ulid::Ulid::generate().to_string();
    let outcome = tokio::select! {
        result = scheduler.run() => result,
        () = health::run(&store, id.clone(), started, partitions) => Ok(()),
        () = gc::run(&store, objects, settings.gc, settings.metering) => Ok(()),
        () = ephemeral::run(&store, settings.scheduler.ephemeral_retention_secs) => Ok(()),
        () = swarmy_config::shutdown_signal() => Ok(()),
    };
    finish(&store, &id, started).await;
    outcome
}

// Flush even when the scheduler loop failed so turn metrics survive shutdown.
async fn flush_metrics(store: &Store) {
    if let Err(error) = store.flush_turn_metrics().await {
        tracing::warn!(error = %swarmy_core::error_chain(&error), "scheduler metric flush failed");
    }
}

// A stopped scheduler must not remain healthy until its lease ages out.
async fn finish(store: &Store, id: &str, started: jiff::Timestamp) {
    flush_metrics(store).await;
    if let Err(error) = store
        .remove_service_heartbeat(&swarmy_store::ServiceRole::Scheduler, id, started)
        .await
    {
        tracing::warn!(error = %swarmy_core::error_chain(&error), "scheduler heartbeat cleanup failed");
    }
}
