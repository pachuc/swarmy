mod config;
mod placement;
mod worker;

use std::sync::Arc;

use anyhow::{Result, anyhow};
use jiff::Timestamp;
use swarmy_bus::{Bus, WorkQueue};
use swarmy_core::Nudge;
use swarmy_store::{HeartbeatSpec, ServiceDetail, ServiceRole, Store};
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
    let services = connect_services(&settings, &config).await?;
    let (send, receive) = mpsc::channel(1);
    let partitions: Vec<u16> = config.partitions.iter().copied().collect();
    let mut consumers = subscribe_partitions(&services.bus, &partitions, send).await?;
    let worker = worker::Worker::new(services.store.clone(), services.bus, services.blobs, config);
    let health = services.store.heartbeat_loop(HeartbeatSpec {
        role: ServiceRole::Worker,
        instance_id: worker.owner.to_string(),
        version: env!("CARGO_PKG_VERSION").into(),
        started_at: Timestamp::now(),
        detail: ServiceDetail::Partitions(partitions),
        expire_stale: false,
    });
    tracing::info!(owner = %worker.owner, "worker ready");
    // The supervised loops hold their buffers and subscriptions inline; keep
    // the combined future off the parent's stack.
    Box::pin(supervise(
        &services.store,
        &worker,
        health,
        &mut consumers,
        receive,
    ))
    .await
}

/// Open connections shared by every worker loop: the store, the object
/// blobs, and the bus with its static queues installed.
async fn connect_services(
    settings: &swarmy_config::Settings,
    config: &config::Config,
) -> Result<Services> {
    let opened = Store::open_store(settings).await?;
    let bus = Bus::connect(&config.nats, config.bus.clone()).await?;
    bus.setup(&[]).await?;
    Ok(Services {
        store: opened.store,
        bus,
        blobs: opened.blobs,
    })
}

struct Services {
    store: Store,
    bus: Bus,
    blobs: Arc<dyn swarmy_store::blob::BlobStore>,
}

/// Drive the worker's loops until one ends or shutdown arrives, then drain
/// queued turn metrics before exit so shutdown keeps every write.
async fn supervise(
    store: &Store,
    worker: &worker::Worker,
    health: impl std::future::Future<Output = ()>,
    consumers: &mut JoinSet<()>,
    receive: mpsc::Receiver<Result<swarmy_bus::WorkMessage<Nudge>, swarmy_bus::Error>>,
) -> Result<()> {
    let outcome: Result<()> = tokio::select! {
        result = consume_nudges(worker, receive) => result,
        () = health => Err(anyhow!("health loop ended")),
        () = worker.recovery_loop() => Err(anyhow!("recovery loop ended")),
        _ = consumers.join_next() => Err(anyhow!("runnable consumer ended")),
        () = swarmy_config::shutdown_signal() => Ok(()),
    };
    if let Err(error) = store.flush_turn_metrics().await {
        tracing::warn!(%error, "worker metric flush failed");
    }
    outcome
}

/// Forward one partition's runnable nudges into the shared channel. A closed
/// receiver ends the forwarder; the supervisor treats that as a crash.
async fn subscribe_partitions(
    bus: &Bus,
    partitions: &[u16],
    send: mpsc::Sender<Result<swarmy_bus::WorkMessage<Nudge>, swarmy_bus::Error>>,
) -> Result<JoinSet<()>> {
    let mut consumers = JoinSet::new();
    for partition in partitions {
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
    Ok(consumers)
}

/// Step every delivered nudge until the channel closes. A failed step only
/// warns; the recovery loop republishes work the step did not finish.
async fn consume_nudges(
    worker: &worker::Worker,
    mut receive: mpsc::Receiver<Result<swarmy_bus::WorkMessage<Nudge>, swarmy_bus::Error>>,
) -> Result<()> {
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
    Err(anyhow!("runnable streams ended"))
}

#[cfg(test)]
mod tests;
