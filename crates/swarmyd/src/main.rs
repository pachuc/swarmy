mod hosting;
mod memory;
mod service;
mod tools;
mod upgrade;

use anyhow::{Result, ensure};
use std::{os::unix::fs::PermissionsExt, sync::Arc, time::Duration};
use swarmy_core::NodeRecord;
use swarmy_sandbox::{RuncRuntime, ScratchPolicy};
use swarmy_store::{Store, blob::ObjectBlobStore};
use swarmy_volume::server::ServerConfig;
use tokio::{net::UnixListener, task::JoinSet};

fn main() -> Result<()> {
    let upgrade_processes =
        std::env::args_os().nth(1).as_deref() == Some(std::ffi::OsStr::new("--upgrade-processes"));
    if !upgrade_processes {
        swarmy_version::parse::<swarmy_version::ServiceArgs>("swarmyd")?;
    }
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let loaded = swarmy_config::Settings::load()?;
    let _network = swarmy_store::boot();
    let runtime = tokio::runtime::Runtime::new()?;
    if upgrade_processes {
        runtime.block_on(upgrade::run(&loaded))
    } else {
        runtime.block_on(run(loaded))
    }
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
    let runtime = open_runtime(&loaded, root.clone(), node, store.clone(), objects).await?;
    let hosting = hosting::Hosting::new(store.clone(), runtime.clone(), node, settings).await?;
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
    if let Some(reserve) = settings.node_memory_reserve_mib {
        record.capacity.memory_bytes = advertised_memory(reserve)?;
    }
    store.put_node(&record).await?;
    tracing::info!(%node, socket = %socket.display(), "node registered and ready");
    let mut heartbeat =
        tokio::time::interval(Duration::from_millis(settings.node_heartbeat_interval_ms));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut scratch_sweep = tokio::time::interval(Duration::from_secs(10));
    scratch_sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut clients = JoinSet::new();
    let (shutdown, _) = tokio::sync::watch::channel(false);
    let mut memory_server = memory::spawn(bus.clone(), store.clone(), runtime.clone(), node);
    let mut tool_server = tools::spawn(bus, &store, node, &hosting, settings);
    let serving: Result<()> = async {
        loop {
            tokio::select! {
                result = &mut memory_server => { result??; break; }
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
                    hosting.report_status(Duration::from_millis(settings.node_heartbeat_interval_ms).saturating_mul(3)).await?;
                }
                _ = scratch_sweep.tick() => {
                    if let Err(error) = runtime.sweep_scratch().await { tracing::warn!(%error, "scratch sweep failed"); }
                }
                Some(result) = clients.join_next(), if !clients.is_empty() => { result?; }
                result = tokio::signal::ctrl_c() => { result?; break; }
                _ = terminate.recv() => break,
            }
        }
        Ok(())
    }.await;
    memory_server.abort();
    tool_server.abort();
    if !tool_server.is_finished() {
        let _ = tool_server.await;
    }
    let _ = shutdown.send(true);
    while clients.join_next().await.is_some() {}
    hosting.shutdown().await;
    let cleanup = runtime.shutdown().await;
    std::fs::remove_file(socket)?;
    cleanup?;
    serving
}

async fn open_runtime(
    loaded: &swarmy_config::Loaded,
    root: std::path::PathBuf,
    node: swarmy_core::NodeId,
    store: Store,
    objects: Arc<dyn object_store::ObjectStore>,
) -> Result<Arc<RuncRuntime>> {
    let settings = &loaded.settings;
    Ok(Arc::new(
        RuncRuntime::open_with_scratch(
            root,
            ServerConfig {
                directory: loaded.root.join(".swarmy/volumes"),
                node,
                store,
                objects,
            },
            ScratchPolicy {
                idle_days: settings.sandbox.scratch_idle_days,
                high_water: settings.sandbox.scratch_high_water,
                low_water: settings.sandbox.scratch_low_water,
            },
        )
        .await?,
    ))
}

async fn storage(
    settings: &swarmy_config::Settings,
) -> Result<(Store, Arc<dyn object_store::ObjectStore>)> {
    let objects = settings.object_store()?;
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

fn advertised_memory(reserve_mib: u64) -> Result<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo")?;
    let total_kib: u64 = meminfo
        .lines()
        .find_map(|line| {
            line.strip_prefix("MemTotal:")?
                .split_whitespace()
                .next()?
                .parse()
                .ok()
        })
        .ok_or_else(|| anyhow::anyhow!("MemTotal missing from /proc/meminfo"))?;
    Ok(total_kib
        .saturating_sub(reserve_mib.saturating_mul(1024))
        .saturating_mul(1024))
}
