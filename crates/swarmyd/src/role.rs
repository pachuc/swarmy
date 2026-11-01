use crate::{
    api_metadata::NodeVolumeMetadata, gpu, hosting, keeper::Keeper, memory, service, tools,
    upgrade, vol,
};
use std::ffi::OsString;

use anyhow::{Result, ensure};
use std::{os::unix::fs::PermissionsExt, sync::Arc, time::Duration};
use swarmy_core::ignore_best_effort;
use swarmy_node_protocol::{
    ClientError, NodeAdvert, NodeClient, RegisterRequest, RegisterResponse,
};
use swarmy_sandbox::{RuncRuntime, RuntimeOptions, ScratchPolicy};
use swarmy_store::Store;
use swarmy_volume::server::ServerConfig;
use tokio::sync::watch;
use tokio::{net::UnixListener, task::JoinSet};

/// Run the node role, including the upgrade drain command.
///
/// # Errors
/// Returns startup or service failures.
pub fn run(args: Vec<OsString>) -> Result<()> {
    let upgrade_processes = args.first().is_some_and(|arg| arg == "--upgrade-processes");
    if !upgrade_processes {
        swarmy_version::parse_from::<swarmy_version::ServiceArgs>("swarmy-core node", args)?;
    }
    swarmy_config::init_tracing();
    let loaded = swarmy_config::Settings::load()?;
    let _network = swarmy_store::boot();
    let runtime = tokio::runtime::Runtime::new()?;
    if upgrade_processes {
        let busy = runtime.block_on(upgrade::run(&loaded, |line| eprintln!("{line}")))?;
        println!("{}", serde_json::to_string(&busy)?);
        Ok(())
    } else {
        runtime.block_on(serve(loaded))
    }
}

/// Run local volume commands.
///
/// # Errors
/// Returns startup or volume operation failures.
pub fn vol(args: Vec<OsString>) -> Result<()> {
    swarmy_config::init_tracing();
    let _settings = swarmy_config::Settings::load()?;
    let _network = swarmy_store::boot();
    tokio::runtime::Runtime::new()?.block_on(vol::run_cli(args))
}

async fn serve(loaded: swarmy_config::Loaded) -> Result<()> {
    let settings = &loaded.settings;
    ensure!(
        !settings.node.roles.is_empty(),
        "node roles must not be empty"
    );
    let (store, objects) = storage(settings).await?;
    let bus_config = settings.bus.bus_config()?;
    let bus = swarmy_bus::Bus::connect(&settings.bus.nats_url, bus_config.clone()).await?;
    let node = loaded.node_id()?;
    let client = NodeClient::from_token_file(&settings.node.api_url, &settings.node.token_file)?;
    ensure!(
        client.node() == node,
        "node token {} names node {}, but this node is {node}",
        settings.node.token_file.display(),
        client.node()
    );
    let root = loaded.root.join(".swarmy/node");
    std::fs::create_dir_all(&root)?;
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
    let (cards, node_gpu) = gpu::devices(&settings.node, gpu::nvidia_smi()).await;
    let advert = advert(settings, cards)?;
    let stop = swarmy_config::shutdown_signal();
    tokio::pin!(stop);
    let registered = tokio::select! { result = register(&client, &advert) => result?, () = &mut stop => return Ok(()) };
    let keeper = Keeper::new(
        Arc::new(client.clone()),
        advert,
        registered.leases,
        settings.node.heartbeat_interval_ms,
    );
    let runtime = open_runtime(
        &loaded,
        root.clone(),
        node,
        store.clone(),
        client.clone(),
        objects,
        node_gpu,
    )
    .await?;
    let hosting = hosting::Hosting::new(
        store.clone(),
        client,
        keeper.clone(),
        runtime.clone(),
        node,
        settings,
        &registered.previous,
    );
    let socket = root.join("control.sock");
    if socket.exists() {
        std::fs::remove_file(&socket)?;
    }
    let listener = UnixListener::bind(&socket)?;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
    tracing::info!(%node, socket = %socket.display(), "node registered and ready");
    let (keeping_stop, keeping_signal) = watch::channel(false);
    let keeping = tokio::spawn(async move { keeper.run(keeping_signal).await });
    let mut scratch_sweep = tokio::time::interval(Duration::from_secs(10));
    scratch_sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
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
                        if let Err(error) = service::handle(socket, runtime, shutdown).await { tracing::warn!(error = %swarmy_core::error_chain(&*error), "node control request failed"); }
                    });
                }
                _ = scratch_sweep.tick() => {
                    if let Err(error) = runtime.sweep_scratch().await { tracing::warn!(error = %swarmy_core::error_chain(&error), "scratch sweep failed"); }
                }
                Some(result) = clients.join_next(), if !clients.is_empty() => { result?; }
                () = &mut stop => break,
            }
        }
        Ok(())
    }.await;
    memory_server.abort();
    tool_server.abort();
    if !tool_server.is_finished() {
        ignore_best_effort(tool_server.await, "await tool server shutdown");
    }
    ignore_best_effort(shutdown.send(true), "signal shutdown");
    while clients.join_next().await.is_some() {}
    hosting.shutdown().await;
    let cleanup = runtime.shutdown().await;
    keeping_stop.send_replace(true);
    keeping.await?;
    std::fs::remove_file(socket)?;
    cleanup?;
    serving
}

async fn open_runtime(
    loaded: &swarmy_config::Loaded,
    root: std::path::PathBuf,
    node: swarmy_core::NodeId,
    store: Store,
    client: NodeClient,
    objects: Arc<dyn object_store::ObjectStore>,
    gpu: swarmy_sandbox::NodeGpu,
) -> Result<Arc<RuncRuntime>> {
    let settings = &loaded.settings;
    Ok(Arc::new(
        RuncRuntime::open_with(
            root,
            ServerConfig {
                directory: loaded.root.join(".swarmy/volumes"),
                node,
                metadata: Arc::new(NodeVolumeMetadata::new(client, store.clone())),
                objects,
            },
            swarmy_store::StoreSandboxMetadata::new(store, node),
            RuntimeOptions {
                scratch: ScratchPolicy {
                    idle_days: settings.sandbox.scratch_idle_days,
                    high_water: settings.sandbox.scratch_high_water,
                    low_water: settings.sandbox.scratch_low_water,
                },
                gpu,
            },
        )
        .await?,
    ))
}

async fn storage(
    settings: &swarmy_config::Settings,
) -> Result<(Store, Arc<dyn object_store::ObjectStore>)> {
    let opened = Store::open_store(settings).await?;
    Ok((opened.store, opened.blobs.object_store()))
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

fn advert(
    settings: &swarmy_config::Settings,
    cards: Option<swarmy_core::GpuCards>,
) -> Result<NodeAdvert> {
    let mut capacity = settings.node.capacity.clone();
    if let Some(reserve) = settings.node.memory_reserve_mib {
        capacity.memory_bytes = advertised_memory(reserve)?;
    }
    Ok(NodeAdvert {
        roles: settings.node.roles.clone(),
        capacity,
        gpu: cards,
    })
}

async fn register(client: &NodeClient, advert: &NodeAdvert) -> Result<RegisterResponse> {
    loop {
        match client
            .register(&RegisterRequest {
                node: advert.clone(),
            })
            .await
        {
            Ok(registered) => return Ok(registered),
            Err(ClientError::Unauthorized) => anyhow::bail!(
                "node token rejected; run swarmy-core node-credential issue {}",
                client.node()
            ),
            Err(error) => {
                tracing::warn!(error = %swarmy_core::error_chain(&error), "node register failed; retrying")
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
