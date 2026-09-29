//! Gateway service wiring: parse args, connect to the store, bus, and
//! providers, then serve until shutdown. The engine lives in the library
//! modules (`dispatch`, `attempt`, `commit`); this binary only assembles it.

use std::sync::Arc;

use anyhow::Result;
use swarmy_bus::Bus;
use swarmy_gateway::{config, dispatch::Gateway, providers::Providers};
use swarmy_store::Store;

// Boot before the runtime so the network guard outlives all database tasks.
fn main() -> Result<()> {
    swarmy_version::parse::<swarmy_version::ServiceArgs>("swarmy-gateway")?;
    swarmy_config::init_tracing();
    let config = config::Config::from_env()?;
    let _network = swarmy_store::boot();
    tokio::runtime::Runtime::new()?.block_on(run(config))
}

async fn run(config: config::Config) -> Result<()> {
    let opened = Store::open_store(&config.settings).await?;
    let store = opened.store;
    let blobs = opened.blobs;
    let blobs: Arc<dyn swarmy_store::blob::BlobStore> = blobs;
    let providers = Providers::discover(store.clone(), &config.settings).await?;
    let bus = Bus::connect(&config.nats, config.bus.clone()).await?;
    let gateway = Arc::new(Gateway::new(store, blobs, bus, providers, &config));
    gateway.serve(config.concurrency).await
}
