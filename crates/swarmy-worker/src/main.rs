mod config;
mod placement;
mod worker;

use std::sync::Arc;

use anyhow::{Result, bail};
use jiff::Timestamp;
use swarmy_bus::{Bus, WorkQueue};
use swarmy_core::Nudge;
use swarmy_store::{ServiceDetail, ServiceHeartbeat, ServiceRole, Store, blob::ObjectBlobStore};
use tokio::time::{Duration, interval};
use tokio::{sync::mpsc, task::JoinSet};

fn main() -> Result<()> {
    swarmy_version::parse::<swarmy_version::ServiceArgs>("swarmy-worker")?;
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let config = config::Config::from_env()?;
    let _network = swarmy_store::boot();
    tokio::runtime::Runtime::new()?.block_on(run(config))
}

async fn run(config: config::Config) -> Result<()> {
    let blobs = Arc::new(ObjectBlobStore::from_env()?);
    let store = Store::open(
        Some(&config.cluster),
        Some(&config.directory),
        blobs.clone(),
    )
    .await?;
    let bus = Bus::connect(&config.nats, config.bus.clone()).await?;
    bus.setup(&[]).await?;
    let (send, mut receive) = mpsc::channel(1);
    let mut consumers = JoinSet::new();
    for partition in &config.partitions {
        let mut messages = bus
            .consume::<Nudge>(&WorkQueue::Runnable(*partition))
            .await?;
        let send = send.clone();
        consumers.spawn(async move {
            while let Some(message) = messages.next().await {
                if send.send(message).await.is_err() {
                    return;
                }
            }
        });
    }
    drop(send);
    let partitions: Vec<_> = config.partitions.iter().copied().collect();
    let heartbeat_store = store.clone();
    let worker = worker::Worker::new(store, bus, blobs, config);
    let started = Timestamp::now();
    let health = async {
        let mut ticks = interval(Duration::from_secs(30));
        loop {
            ticks.tick().await;
            let record = ServiceHeartbeat {
                role: ServiceRole::Worker,
                instance_id: worker.owner.to_string(),
                version: env!("CARGO_PKG_VERSION").into(),
                host: hostname(),
                started_at: started,
                last_seen: Timestamp::now(),
                detail: ServiceDetail::Partitions(partitions.clone()),
            };
            if let Err(error) = heartbeat_store.put_service_heartbeat(&record).await {
                tracing::warn!(%error, "worker health heartbeat failed");
            }
        }
    };
    tracing::info!(owner = %worker.owner, "worker ready");
    let consume = async {
        while let Some(delivery) = receive.recv().await {
            match delivery {
                Ok(message) => {
                    if let Err(error) = worker.handle(&message).await {
                        tracing::warn!(%error, "step left for recovery");
                    }
                }
                Err(error) => tracing::warn!(%error, "invalid nudge"),
            }
        }
        bail!("runnable streams ended")
    };
    tokio::select! {
        result = consume => result,
        () = health => bail!("health loop ended"),
        () = worker.recovery_loop() => bail!("recovery loop ended"),
        _ = consumers.join_next() => bail!("runnable consumer ended"),
        result = tokio::signal::ctrl_c() => Ok(result?),
    }
}

#[cfg(test)]
mod tests;

fn hostname() -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into())
}
