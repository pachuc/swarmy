mod config;
mod ephemeral;
mod gc;
mod scheduler;

use jiff::Timestamp;
use swarmy_bus::Bus;
use swarmy_store::{ServiceDetail, ServiceHeartbeat, ServiceRole, Store};
use tokio::time::{Duration, interval};

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
    let started = Timestamp::now();
    let id = ulid::Ulid::generate().to_string();
    let health = async {
        let mut ticks = interval(Duration::from_secs(30));
        loop {
            ticks.tick().await;
            let record = ServiceHeartbeat {
                role: ServiceRole::Scheduler,
                instance_id: id.clone(),
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
    };
    tokio::select! {
        result = scheduler.run() => result?,
        () = health => {},
        () = gc::run(&store, objects, settings.gc, settings.metering) => {},
        () = ephemeral::run(&store, settings.scheduler.ephemeral_retention_secs) => {},
        result = tokio::signal::ctrl_c() => result?,
    }
    Ok(())
}
