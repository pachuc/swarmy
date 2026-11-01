use crate::{NodeApiState, router};
use clap::{Parser, Subcommand};
use std::{ffi::OsString, time::Duration};
use swarmy_bus::Bus;
use swarmy_config::Settings;
use swarmy_core::NodeId;
use swarmy_store::{HeartbeatSpec, ServiceDetail, ServiceRole, Store};
/// Service startup failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Settings(#[from] swarmy_config::Error),
    #[error(transparent)]
    Bus(#[from] swarmy_bus::Error),
    #[error(transparent)]
    Store(#[from] swarmy_store::StoreError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("bind node API listener {listen}")]
    Bind {
        listen: String,
        #[source]
        source: std::io::Error,
    },
}
/// Run the node API service.
/// # Errors
/// Startup or service failures.
pub fn run(args: Vec<OsString>) -> Result<(), Error> {
    swarmy_version::parse_from::<swarmy_version::ServiceArgs>("swarmy-core node-api", args)?;
    swarmy_config::init_tracing();
    let _network = swarmy_store::boot();
    tokio::runtime::Runtime::new()?.block_on(start())
}
async fn flush_metrics(store: &Store) {
    if let Err(error) = store.flush_turn_metrics().await {
        tracing::warn!(error = %swarmy_core::error_chain(&error), "node api metrics flush failed");
    }
}
async fn start() -> Result<(), Error> {
    let settings = Settings::load()?.settings;
    let opened = Store::open_store(&settings).await?;
    let store = opened.store;
    let bus = Bus::connect(&settings.bus.nats_url, settings.bus.bus_config()?).await?;
    bus.setup(&[]).await?;
    let started = jiff::Timestamp::now();
    let instance_id = ulid::Ulid::generate().to_string();
    let heartbeat_id = instance_id.clone();
    let heartbeat_store = store.clone();
    let heartbeat = tokio::spawn(async move {
        heartbeat_store
            .heartbeat_loop(HeartbeatSpec {
                role: ServiceRole::NodeApi,
                instance_id,
                version: env!("CARGO_PKG_VERSION").into(),
                started_at: started,
                detail: ServiceDetail::None,
                expire_stale: false,
            })
            .await;
    });
    let state = NodeApiState::new(
        store.clone(),
        bus,
        crate::NodeApiPolicy::from_settings(&settings),
    );
    let listen = settings.node_api.listen;
    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .map_err(|source| Error::Bind { listen, source })?;
    tracing::info!(address = %listener.local_addr()?, "node api ready");
    let server = axum::serve(listener, router(state))
        .with_graceful_shutdown(swarmy_config::shutdown_signal())
        .into_future();
    let outcome = tokio::select! {
        result = server => result,
        () = async { swarmy_config::shutdown_signal().await; tokio::time::sleep(Duration::from_secs(5)).await; } => Ok(()),
    };
    finish(&store, heartbeat, heartbeat_id, started).await;
    outcome?;
    Ok(())
}
async fn finish(
    store: &Store,
    heartbeat: tokio::task::JoinHandle<()>,
    heartbeat_id: String,
    started: jiff::Timestamp,
) {
    flush_metrics(store).await;
    heartbeat.abort();
    swarmy_core::ignore_best_effort(heartbeat.await, "stop node API heartbeat task");
    if let Err(error) = store
        .remove_service_heartbeat(&ServiceRole::NodeApi, &heartbeat_id, started)
        .await
    {
        tracing::warn!(error = %swarmy_core::error_chain(&error), "node api heartbeat cleanup failed");
    }
}
#[derive(Parser)]
struct CredentialArgs {
    #[command(subcommand)]
    action: CredentialAction,
}
#[derive(Subcommand)]
enum CredentialAction {
    /// Issue a fresh token for this node.
    Issue { node: ulid::Ulid },
    /// Delete this node's credential.
    Revoke { node: ulid::Ulid },
}
/// Issue or revoke a node credential.
/// # Errors
/// Settings, store or output failures.
pub fn credential(args: Vec<OsString>) -> Result<(), Error> {
    let args = swarmy_version::parse_from::<CredentialArgs>("swarmy-core node-credential", args)?;
    swarmy_config::init_tracing();
    let _network = swarmy_store::boot();
    tokio::runtime::Runtime::new()?.block_on(async {
        let settings = Settings::load()?.settings;
        let store = Store::open_store(&settings).await?.store;
        match args.action {
            CredentialAction::Issue { node } => {
                let token = store.issue_node_credential(NodeId::from_ulid(node)).await?;
                println!("{}", token.reveal());
            }
            CredentialAction::Revoke { node } => {
                let existed = store
                    .revoke_node_credential(NodeId::from_ulid(node))
                    .await?;
                tracing::info!(%node, existed, "node credential revoked");
            }
        }
        Ok(())
    })
}
