mod service;
mod tools;

use anyhow::{Result, ensure};
use std::{os::unix::fs::PermissionsExt, sync::Arc, time::Duration};
use swarmy_core::NodeRecord;
use swarmy_sandbox::RuncRuntime;
use swarmy_store::{Store, blob::ObjectBlobStore};
use swarmy_volume::server::ServerConfig;
use tokio::{net::UnixListener, task::JoinSet};

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let loaded = swarmy_config::Settings::load()?;
    let _network = swarmy_store::boot();
    tokio::runtime::Runtime::new()?.block_on(run(loaded))
}

async fn run(loaded: swarmy_config::Loaded) -> Result<()> {
    let settings = &loaded.settings;
    ensure!(
        settings.node_heartbeat_interval_ms > 0,
        "node heartbeat interval must be positive"
    );
    ensure!(
        !settings.node_roles.is_empty(),
        "node roles must not be empty"
    );
    let (store, objects) = storage(settings).await?;
    let bus_config = swarmy_bus::Config {
        prefix: if settings.bus_prefix.is_empty() {
            None
        } else {
            Some(swarmy_bus::SubjectToken::new(&settings.bus_prefix)?)
        },
        ack_wait: Duration::from_millis(settings.bus_ack_wait_ms),
        max_deliver: settings.bus_max_deliver,
    };
    let bus = swarmy_bus::Bus::connect(&settings.nats_url, bus_config.clone()).await?;
    let node = loaded.node_id()?;
    let root = loaded.root.join(".swarmy/node");
    std::fs::create_dir_all(&root)?;
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
    let runtime = Arc::new(
        RuncRuntime::open(
            root.clone(),
            ServerConfig {
                directory: loaded.root.join(".swarmy/volumes"),
                node,
                store: store.clone(),
                objects,
            },
        )
        .await?,
    );
    let socket = root.join("control.sock");
    if socket.exists() {
        std::fs::remove_file(&socket)?;
    }
    let listener = UnixListener::bind(&socket)?;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
    let mut record = NodeRecord {
        node_id: node,
        roles: settings.node_roles.clone(),
        capacity: settings.node_capacity.clone(),
        last_heartbeat: jiff::Timestamp::now(),
        cached_images: Vec::new(),
    };
    store.put_node(&record).await?;
    tracing::info!(%node, socket = %socket.display(), "node registered and ready");
    let mut heartbeat =
        tokio::time::interval(Duration::from_millis(settings.node_heartbeat_interval_ms));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut clients = JoinSet::new();
    let (shutdown, _) = tokio::sync::watch::channel(false);
    let mut tool_server = {
        let store = store.clone();
        let runtime = runtime.clone();
        tokio::spawn(async move {
            tools::serve(&store, &bus, node, &runtime, bus_config.ack_wait).await
        })
    };
    let serving: Result<()> = async {
        loop {
            tokio::select! {
                result = &mut tool_server => { result??; break; }
                connection = listener.accept() => {
                    let (socket, _) = connection?;
                    let runtime = runtime.clone();
                    let shutdown = shutdown.subscribe();
                    clients.spawn(async move {
                        if let Err(error) = service::handle(socket, runtime, shutdown).await { tracing::warn!(%error, "node control request failed"); }
                    });
                }
                _ = heartbeat.tick() => {
                    record.last_heartbeat = jiff::Timestamp::now();
                    store.put_node(&record).await?;
                }
                Some(result) = clients.join_next(), if !clients.is_empty() => { result?; }
                result = tokio::signal::ctrl_c() => { result?; break; }
                _ = terminate.recv() => break,
            }
        }
        Ok(())
    }.await;
    tool_server.abort();
    if !tool_server.is_finished() {
        let _ = tool_server.await;
    }
    let _ = shutdown.send(true);
    while clients.join_next().await.is_some() {}
    let cleanup = runtime.shutdown().await;
    std::fs::remove_file(socket)?;
    cleanup?;
    serving
}

async fn storage(
    settings: &swarmy_config::Settings,
) -> Result<(Store, Arc<dyn object_store::ObjectStore>)> {
    let objects = Arc::new(
        object_store::aws::AmazonS3Builder::new()
            .with_endpoint(&settings.s3_endpoint)
            .with_access_key_id(&settings.s3_access_key)
            .with_secret_access_key(&settings.s3_secret_key)
            .with_bucket_name(&settings.s3_bucket)
            .with_region(&settings.s3_region)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .build()?,
    );
    let directory: Vec<_> = settings
        .store_directory
        .split('/')
        .map(str::to_owned)
        .collect();
    let store = Store::open(
        Some(&settings.fdb_cluster_file),
        Some(&directory),
        Arc::new(ObjectBlobStore::new(objects.clone())),
    )
    .await?;
    Ok((store, objects))
}
