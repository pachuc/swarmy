mod config;
mod ephemeral;
mod gc;
mod health;
mod scheduler;

use swarmy_bus::Bus;
use swarmy_store::Store;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    swarmy_version::parse::<swarmy_version::ServiceArgs>("swarmy-scheduler")?;
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
        () = health::run(&store, id, started, partitions) => Ok(()),
        () = gc::run(&store, objects, settings.gc, settings.metering) => Ok(()),
        () = ephemeral::run(&store, settings.scheduler.ephemeral_retention_secs) => Ok(()),
        () = swarmy_config::shutdown_signal() => Ok(()),
    };
    // Drain queued turn metrics before exit so shutdown keeps every write.
    // The flush runs on every path, including a scheduler error, so a
    // failing run still keeps the metrics it queued.
    if let Err(error) = store.flush_turn_metrics().await {
        tracing::warn!(%error, "scheduler metric flush failed");
    }
    outcome
}
