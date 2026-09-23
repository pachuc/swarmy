mod config;
mod ephemeral;
mod gc;
mod scheduler;

use std::sync::Arc;

use jiff::Timestamp;
use swarmy_bus::{Bus, SubjectToken};
use swarmy_store::{ServiceDetail, ServiceHeartbeat, ServiceRole, Store, blob::ObjectBlobStore};
use tokio::time::{Duration, interval};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    swarmy_version::parse::<swarmy_version::ServiceArgs>("swarmy-scheduler")?;
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let config = config::Config::from_env()?;
    let settings = swarmy_config::Settings::load()?.settings;
    let cluster = settings.fdb_cluster_file;
    let url = settings.nats_url;
    let directory: Vec<String> = settings
        .store_directory
        .split('/')
        .map(str::to_owned)
        .collect();
    anyhow::ensure!(
        directory.iter().all(|part| !part.is_empty()),
        "empty store directory component"
    );
    let bus_config = swarmy_bus::Config {
        prefix: if settings.bus_prefix.is_empty() {
            None
        } else {
            Some(SubjectToken::new(settings.bus_prefix)?)
        },
        ack_wait: std::time::Duration::from_millis(settings.bus_ack_wait_ms),
        max_deliver: settings.bus_max_deliver,
    };
    let blobs = Arc::new(ObjectBlobStore::from_env()?);
    let objects = blobs.object_store();
    let _network = swarmy_store::boot();
    let store = Store::open(Some(&cluster), Some(&directory), blobs).await?;
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
        () = gc::run(&store, objects, settings.gc) => {},
        () = ephemeral::run(&store, settings.ephemeral_retention_seconds) => {},
        result = tokio::signal::ctrl_c() => result?,
    }
    Ok(())
}
