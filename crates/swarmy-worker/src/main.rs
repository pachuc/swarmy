mod config;
mod placement;
mod worker;

use std::sync::Arc;

use anyhow::{Result, anyhow};
use jiff::Timestamp;
use swarmy_bus::{Bus, WorkQueue};
use swarmy_core::Nudge;
use swarmy_store::{ServiceDetail, ServiceRole, Store};
use tokio::{sync::mpsc, task::JoinSet};

fn main() -> Result<()> {
    swarmy_version::parse::<swarmy_version::ServiceArgs>("swarmy-worker")?;
    swarmy_config::init_tracing();
    let settings = swarmy_config::Settings::load()?.settings;
    let config = config::Config::from_settings(&settings)?;
    let _network = swarmy_store::boot();
    tokio::runtime::Runtime::new()?.block_on(run(config, settings))
}

async fn run(config: config::Config, settings: swarmy_config::Settings) -> Result<()> {
    let opened = Store::open_store(&settings).await?;
    let store = opened.store;
    let blobs: Arc<dyn swarmy_store::blob::BlobStore> = opened.blobs;
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
    let health = heartbeat_store.heartbeat_loop(
        ServiceRole::Worker,
        worker.owner.to_string(),
        env!("CARGO_PKG_VERSION").into(),
        started,
        ServiceDetail::Partitions(partitions.clone()),
        false,
    );
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
        Err::<(), _>(anyhow!("runnable streams ended"))
    };
    let outcome: Result<()> = tokio::select! {
        result = consume => result,
        () = health => Err(anyhow!("health loop ended")),
        () = worker.recovery_loop() => Err(anyhow!("recovery loop ended")),
        _ = consumers.join_next() => Err(anyhow!("runnable consumer ended")),
        () = swarmy_config::shutdown_signal() => Ok(()),
    };
    // Drain queued turn metrics before exit so shutdown keeps every write.
    if let Err(error) = heartbeat_store.flush_turn_metrics().await {
        tracing::warn!(%error, "worker metric flush failed");
    }
    outcome
}

#[cfg(test)]
mod tests;
