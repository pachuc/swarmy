use crate::{
    api_metadata::{NodeSandboxMetadata, NodeVolumeMetadata},
    gpu,
    heads::Heads,
    hosting,
    keeper::{Keeper, ScratchSkipped},
    memory, probe, service, tools, upgrade,
};
use std::ffi::OsString;

use anyhow::{Result, ensure};
use std::{os::unix::fs::PermissionsExt, sync::Arc, time::Duration};
use swarmy_core::ignore_best_effort;
use swarmy_node_protocol::{
    ClientError, NodeAdvert, NodeClient, RegisterRequest, RegisterResponse, ScratchEntry,
    ScratchVerdict,
};
use swarmy_sandbox::{RuncRuntime, RuntimeOptions, ScratchPolicy};
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
        swarmy_version::parse_from::<swarmy_version::ServiceArgs>("swarmy-node", args)?;
    }
    swarmy_config::init_tracing();
    let loaded = swarmy_config::Settings::load()?;
    let runtime = tokio::runtime::Runtime::new()?;
    if upgrade_processes {
        let busy = runtime.block_on(upgrade::run(&loaded, |line| eprintln!("{line}")))?;
        println!("{}", serde_json::to_string(&busy)?);
        Ok(())
    } else {
        runtime.block_on(serve(loaded))
    }
}

async fn serve(loaded: swarmy_config::Loaded) -> Result<()> {
    let settings = &loaded.settings;
    ensure!(
        !settings.node.roles.is_empty(),
        "node roles must not be empty"
    );
    let objects = swarmy_volume::objects::from_settings(settings)?;
    let bus_config = settings.bus.bus_config()?;
    let bus = swarmy_bus::Bus::connect(&settings.bus.nats_url, bus_config.clone()).await?;
    let node = loaded.node_id()?;
    let client = node_client(settings, node)?;
    let root = loaded.root.join(".swarmy/node");
    std::fs::create_dir_all(&root)?;
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
    let (cards, node_gpu) = gpu::devices(&settings.node, gpu::nvidia_smi()).await;
    let advert = advert(settings, cards)?;
    let stop = swarmy_config::shutdown_signal();
    tokio::pin!(stop);
    let registered = tokio::select! { result = register(&client, &advert, &settings.node.token_file) => result?, () = &mut stop => return Ok(()) };
    let keeper = Keeper::new(
        Arc::new(client.clone()),
        advert,
        registered.leases,
        settings.node.heartbeat_interval_ms,
    );
    let (runtime, heads) = open_runtime(
        &loaded,
        root.clone(),
        node,
        client.clone(),
        objects,
        node_gpu,
    )
    .await?;
    let hosting = hosting::Hosting::new(
        heads,
        client.clone(),
        keeper.clone(),
        runtime.clone(),
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
    let keeping = tokio::spawn({
        let keeper = keeper.clone();
        async move { keeper.run(keeping_signal).await }
    });
    let (shutdown, _) = watch::channel(false);
    let sweeping = tokio::spawn(sweep_scratch(
        runtime.clone(),
        keeper.clone(),
        shutdown.subscribe(),
    ));
    let mut clients = JoinSet::new();
    let mut memory_server = memory::spawn(bus.clone(), keeper.clone(), runtime.clone(), node);
    let mut probe_server = probe::spawn(
        bus.clone(),
        client.clone(),
        keeper.clone(),
        runtime.clone(),
        settings.bus.ack_wait_ms,
    );
    let mut tool_server = tools::spawn(bus, node, &hosting, settings);
    let serving: Result<()> = async {
        loop {
            tokio::select! {
                result = &mut memory_server => { result??; break; }
                result = &mut tool_server => { result??; break; }
                result = &mut probe_server => { result??; break; }
                connection = listener.accept() => {
                    let (socket, _) = connection?;
                    let runtime = runtime.clone();
                    let shutdown = shutdown.subscribe();
                    let keeper = keeper.clone();
                    clients.spawn(async move {
                        if let Err(error) = service::handle(socket, runtime, keeper, shutdown).await { tracing::warn!(error = %swarmy_core::error_chain(&*error), "node control request failed"); }
                    });
                }
                Some(result) = clients.join_next(), if !clients.is_empty() => { result?; }
                () = &mut stop => break,
            }
        }
        Ok(())
    }.await;
    memory_server.abort();
    tool_server.abort();
    probe_server.abort();
    if !tool_server.is_finished() {
        ignore_best_effort(tool_server.await, "await tool server shutdown");
    }
    ignore_best_effort(shutdown.send(true), "signal shutdown");
    while clients.join_next().await.is_some() {}
    // The keeper stops last: a scratch round waits for its next sync, and shutdown
    // flushes volumes and releases placements under leases only it renews.
    // When it stops, the keeper sends one last sync with the metric patches and scratch
    // removals queued during shutdown.
    sweeping.await?;
    hosting.shutdown().await;
    let cleanup = runtime.shutdown().await;
    keeper.report_evicted(runtime.take_evicted());
    keeping_stop.send_replace(true);
    keeping.await?;
    std::fs::remove_file(socket)?;
    cleanup?;
    serving
}

/// Sweep independently so a sync wait cannot block control-socket accepts.
async fn sweep_scratch(runtime: Arc<RuncRuntime>, keeper: Keeper, mut stop: watch::Receiver<bool>) {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                match scratch_round(&runtime, &keeper).await {
                    Ok(()) => {},
                    Err(error) if error.downcast_ref::<ScratchSkipped>().is_some() => {
                        tracing::debug!("scratch round skipped after sync failure");
                    }
                    Err(error) => tracing::warn!(error = %swarmy_core::error_chain(&*error), "scratch round failed"),
                }
            }
            changed = stop.changed() => { if changed.is_err() || *stop.borrow() { break; } }
        }
    }
}

async fn scratch_round(runtime: &RuncRuntime, keeper: &Keeper) -> Result<()> {
    keeper.report_evicted(runtime.take_evicted());
    let inventory = runtime.scratch_inventory().await?;
    let entries = inventory
        .iter()
        .map(|usage| ScratchEntry {
            agent: usage.agent,
            bytes: usage.bytes,
            hosted: usage.hosted,
            idle: usage.idle,
        })
        .collect();
    let verdicts = keeper.scratch_verdicts(entries).await?;
    let evict = inventory
        .iter()
        .zip(verdicts)
        .filter_map(|(usage, verdict)| {
            (!usage.hosted && matches!(verdict, ScratchVerdict::Evict(_))).then_some(usage.agent)
        })
        .collect::<Vec<_>>();
    let result = runtime.apply_scratch(&inventory, &evict).await;
    keeper.report_evicted(runtime.take_evicted());
    result?;
    Ok(())
}

async fn open_runtime(
    loaded: &swarmy_config::Loaded,
    root: std::path::PathBuf,
    node: swarmy_core::NodeId,
    client: NodeClient,
    objects: Arc<dyn object_store::ObjectStore>,
    gpu: swarmy_sandbox::NodeGpu,
) -> Result<(Arc<RuncRuntime>, Heads)> {
    let settings = &loaded.settings;
    let heads = Heads::default();
    let runtime = Arc::new(
        RuncRuntime::open_with(
            root,
            ServerConfig {
                directory: loaded.root.join(".swarmy/volumes"),
                node,
                metadata: Arc::new(NodeVolumeMetadata::new(client.clone(), heads.clone())),
                objects,
            },
            NodeSandboxMetadata::new(client),
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
    );
    Ok((runtime, heads))
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

async fn register(
    client: &NodeClient,
    advert: &NodeAdvert,
    token_file: &std::path::Path,
) -> Result<RegisterResponse> {
    loop {
        match client
            .register(&RegisterRequest {
                node: advert.clone(),
            })
            .await
        {
            Ok(registered) => return Ok(registered),
            Err(ClientError::Unauthorized) => anyhow::bail!(
                "node token {} rejected; run swarmy-core node-credential issue {}",
                token_file.display(),
                client.node()
            ),
            Err(error) => {
                tracing::warn!(error = %swarmy_core::error_chain(&error), "node register failed; retrying");
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

fn node_client(
    settings: &swarmy_config::Settings,
    node: swarmy_core::NodeId,
) -> Result<NodeClient> {
    let client = NodeClient::from_token_file(&settings.node.api_url, &settings.node.token_file)?;
    ensure!(
        client.node() == node,
        "node token {} names node {}, but this node is {node}",
        settings.node.token_file.display(),
        client.node()
    );
    Ok(client)
}
#[cfg(test)]
mod node_api_tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use std::num::NonZeroUsize;
    use swarmy_bus::{Bus, Config, SubjectToken};
    use swarmy_core::{AgentId, NodeCapacity, NodeId, NodeRecord, NodeRole};
    use swarmy_node_api::{NodeApiPolicy, NodeApiState, router, test_support::TestApi};
    use swarmy_node_protocol::RegisterRequest;
    use swarmy_sandbox::SandboxMetadata;
    use swarmy_store::{CreateAgentOptions, Store, blob::MemoryBlobStore};

    async fn fixture(
        tag: &str,
    ) -> Option<(
        Store,
        NodeClient,
        Keeper,
        NodeId,
        TestApi,
        swarmy_testkit::StackGuard,
    )> {
        let stack = swarmy_testkit::Stack::load(tag)?;
        let (store, guard) = stack.open_store(Arc::new(MemoryBlobStore::default())).await;
        let node = NodeId::from_ulid(ulid::Ulid::generate());
        let record = NodeRecord {
            node_id: node,
            roles: vec![NodeRole::Sandbox],
            capacity: NodeCapacity {
                cpu_millis: 1000,
                memory_bytes: 1 << 30,
                disk_bytes: 1 << 30,
                sandboxes: 1,
            },
            last_heartbeat: store.now(),
            cached_images: vec![],
            gpu: None,
        };
        store.put_node(&record).await.unwrap();
        let token = store.issue_node_credential(node).await.unwrap();
        let bus = Bus::connect(
            &stack.nats_url,
            Config {
                prefix: Some(SubjectToken::new(&stack.prefix).unwrap()),
                ..Config::default()
            },
        )
        .await
        .unwrap();
        bus.setup(&[]).await.unwrap();
        let api = TestApi::start(router(NodeApiState::new(
            store.clone(),
            bus,
            NodeApiPolicy {
                placement_lease: Duration::from_secs(60),
                snapshot_retention: NonZeroUsize::new(10).unwrap(),
                resend_interval: Duration::from_secs(5),
            },
        )))
        .await;
        let client = NodeClient::new(api.url(), &token).unwrap();
        let advert = NodeAdvert {
            roles: record.roles,
            capacity: record.capacity,
            gpu: None,
        };
        let registered = client
            .register(&RegisterRequest {
                node: advert.clone(),
            })
            .await
            .unwrap();
        let keeper = Keeper::new(
            Arc::new(client.clone()),
            advert,
            registered.leases,
            Duration::from_secs(5),
        );
        Some((store, client, keeper, node, api, guard))
    }

    #[tokio::test]
    async fn github_token_through_the_node_api() {
        let Some((store, client, _keeper, node, _api, _guard)) = fixture("na7c-github").await
        else {
            return;
        };
        let agent = store
            .create_agent(
                "na7c-github",
                swarmy_testkit::image(&store).await,
                "",
                store.now(),
                Some(CreateAgentOptions {
                    github_token: Some("ghp_test_token"),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        store
            .place(
                agent.agent_id,
                node,
                store.now().checked_add(Duration::from_secs(30)).unwrap(),
            )
            .await
            .unwrap();
        let metadata = NodeSandboxMetadata::new(client);
        assert_eq!(
            metadata
                .github_token(agent.agent_id)
                .await
                .unwrap()
                .as_deref(),
            Some("ghp_test_token")
        );
        let other = NodeId::from_ulid(ulid::Ulid::generate());
        let record = NodeRecord {
            node_id: other,
            roles: vec![NodeRole::Sandbox],
            capacity: NodeCapacity {
                cpu_millis: 1000,
                memory_bytes: 1 << 30,
                disk_bytes: 1 << 30,
                sandboxes: 1,
            },
            last_heartbeat: store.now(),
            cached_images: vec![],
            gpu: None,
        };
        store.put_node(&record).await.unwrap();
        let moved = store
            .create_agent(
                "na7c-moved",
                swarmy_testkit::image(&store).await,
                "",
                store.now(),
                Some(CreateAgentOptions {
                    github_token: Some("ghp_test_token"),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        store
            .place(
                moved.agent_id,
                other,
                store.now().checked_add(Duration::from_secs(30)).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(metadata.github_token(moved.agent_id).await.unwrap(), None);
    }

    #[tokio::test]
    async fn node_scratch_round_reports_and_evicts() {
        let Some((store, client, keeper, node, _api, _guard)) = fixture("na7c-scratch").await
        else {
            return;
        };
        let deleted = AgentId::from_ulid(ulid::Ulid::generate());
        let moved = AgentId::from_ulid(ulid::Ulid::generate());
        store.delete_computer(deleted).await.unwrap();
        let other = NodeId::from_ulid(ulid::Ulid::generate());
        store
            .put_node(&NodeRecord {
                node_id: other,
                roles: vec![NodeRole::Sandbox],
                capacity: NodeCapacity {
                    cpu_millis: 1000,
                    memory_bytes: 1 << 30,
                    disk_bytes: 1 << 30,
                    sandboxes: 1,
                },
                last_heartbeat: store.now(),
                cached_images: vec![],
                gpu: None,
            })
            .await
            .unwrap();
        store
            .place(
                moved,
                other,
                store.now().checked_add(Duration::from_secs(30)).unwrap(),
            )
            .await
            .unwrap();
        let root = tempfile::tempdir().unwrap();
        let runtime = RuncRuntime::open(
            root.path().join("runtime"),
            ServerConfig {
                directory: root.path().join("volumes"),
                node,
                metadata: Arc::new(NodeVolumeMetadata::new(client.clone(), Heads::default())),
                objects: Arc::new(object_store::memory::InMemory::new()),
            },
            NodeSandboxMetadata::new(client),
        )
        .await
        .unwrap();
        let scratch = root.path().join("scratch");
        for agent in [deleted, moved] {
            std::fs::create_dir(scratch.join(agent.to_string())).unwrap();
        }
        let round = tokio::spawn({
            let keeper = keeper.clone();
            async move { scratch_round(&runtime, &keeper).await }
        });
        // Wait for the inventory to be handed to the keeper rather than racing sync.
        swarmy_testkit::eventually(
            "scratch inventory pending",
            Duration::from_secs(5),
            async || keeper.scratch_pending().then_some(()),
        )
        .await;
        keeper.sync_once().await.unwrap();
        round.await.unwrap().unwrap();
        for agent in [deleted, moved] {
            assert!(!scratch.join(agent.to_string()).exists());
        }
        keeper.sync_once().await.unwrap();
        for agent in [deleted, moved] {
            assert!(store.scratch(agent).await.unwrap().is_none());
        }
    }
}
