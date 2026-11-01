
#![deny(clippy::disallowed_methods)]
use super::*;
use crate::keeper::{Answered, Occupancy};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{num::NonZeroUsize, path::Path};
use swarmy_bus::{Bus, Config, SubjectToken};
use swarmy_core::{NodeCapacity, NodeId, NodeRecord, NodeRole, PlacementChangeReason, VolumeId};
use swarmy_node_api::test_support::drop_first_response;
use swarmy_node_api::{NodeApiPolicy, NodeApiState, router, test_support::TestApi};
use swarmy_node_protocol::{NodeAdvert, RegisterRequest, SYNC_PATH};
use swarmy_store::Store;

pub(crate) async fn node_api(
    store: &Store,
    stack: &swarmy_testkit::Stack,
    record: &NodeRecord,
    fault: bool,
) -> (NodeClient, Keeper, TestApi, Arc<AtomicUsize>) {
    let token = store.issue_node_credential(record.node_id).await.unwrap();
    let bus = Bus::connect(
        &stack.nats_url,
        Config {
            prefix: Some(SubjectToken::new(&stack.prefix).unwrap()),
            ..Config::default()
        },
    )
    .await
    .unwrap();
    let router = router(NodeApiState::new(
        store.clone(),
        bus,
        NodeApiPolicy {
            placement_lease: Duration::from_secs(60),
            snapshot_retention: NonZeroUsize::new(10).unwrap(),
            resend_interval: Duration::from_secs(5),
        },
    ));
    let (router, count) = if fault {
        drop_first_response(router, SYNC_PATH)
    } else {
        (router, Arc::new(AtomicUsize::new(0)))
    };
    let api = TestApi::start(router).await;
    let client = NodeClient::new(api.url(), &token).unwrap();
    let advert = NodeAdvert {
        roles: record.roles.clone(),
        capacity: record.capacity.clone(),
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
    (client, keeper, api, count)
}

async fn placed(
    store: &Store,
    stack: &swarmy_testkit::Stack,
    root: &Path,
) -> (Arc<Hosting>, AgentId, PlacementRecord, Opening, TestApi) {
    let node_id = NodeId::from_ulid(ulid::Ulid::generate());
    let record = NodeRecord {
        node_id,
        roles: vec![NodeRole::Sandbox],
        capacity: NodeCapacity {
            cpu_millis: 1000,
            memory_bytes: 4 * 1024 * 1024 * 1024,
            disk_bytes: 1024,
            sandboxes: 1,
        },
        last_heartbeat: store.now(),
        cached_images: vec![],
        gpu: None,
    };
    store.put_node(&record).await.unwrap();
    let (client, keeper, api, _) = node_api(store, stack, &record, false).await;
    let agent = AgentId::from_ulid(ulid::Ulid::generate());
    let placement = store
        .place(
            agent,
            node_id,
            store.now().checked_add(Duration::from_secs(60)).unwrap(),
        )
        .await
        .unwrap();
    store.claim_placement(&placement).await.unwrap();
    let runtime = RuncRuntime::open(
        root.join("runtime"),
        swarmy_volume::server::ServerConfig {
            directory: root.join("volumes"),
            node: node_id,
            metadata: Arc::new(store.clone()),
            objects: Arc::new(object_store::memory::InMemory::new()),
        },
        swarmy_store::StoreSandboxMetadata::new(store.clone(), node_id),
    )
    .await
    .unwrap();
    let settings = swarmy_config::Settings::default();
    let hosting = Hosting::new(
        Heads::default(),
        client,
        keeper.clone(),
        Arc::new(runtime),
        &settings,
        std::slice::from_ref(&placement),
    );
    let answered = Answered {
        sent: Instant::now(),
        server_now: store.now(),
    };
    let hold = keeper.hold_placement(
        placement.clone(),
        answered,
        Arc::new(std::sync::Mutex::new(Occupancy::default())),
    );
    let volume = VolumeId::from_ulid(agent.as_ulid());
    let head = ManifestId::from_ulid(ulid::Ulid::generate());
    let header = ManifestHeader {
        size: u64::from(swarmy_core::CHUNK_SIZE),
        chunk_size: swarmy_core::CHUNK_SIZE,
        root_hash: swarmy_core::ContentHash::ZERO,
    };
    store.put_manifest(head, &header).await.unwrap();
    store.create_volume(volume, head).await.unwrap();
    let writer_lease = store
        .open_writer_lease(
            volume,
            swarmy_core::LeaseOwnerId::from(node_id),
            Duration::from_secs(60),
        )
        .await
        .unwrap()
        .lease;
    let writer = keeper.hold_writer(volume, writer_lease, answered);
    let opening = Opening {
        placement: placement.clone(),
        hold,
        volume,
        writer: Some(writer),
        head,
        header,
        spec: SandboxSpec {
            agent_id: agent,
            scratch: vec![],
            requirements: swarmy_core::SandboxRequirements::default(),
        },
    };
    (hosting, agent, placement, opening, api)
}

#[tokio::test]
async fn cleanup_without_a_sandbox_releases_the_placement() {
    let Some(stack) = swarmy_testkit::Stack::load("hosting-release") else {
        return;
    };
    let (store, mut guard) = stack
        .open_store(Arc::new(swarmy_store::blob::MemoryBlobStore::default()))
        .await;
    let root = tempfile::tempdir().unwrap();
    let (hosting, agent, placement, opening, _api) = placed(&store, &stack, root.path()).await;
    hosting
        .stop_and_release(agent, &opening, "eviction")
        .await
        .unwrap();
    assert!(store.get_by_agent(agent).await.unwrap().is_none());
    assert!(!hosting.previous.lock().await.contains_key(&agent));
    assert!(
        store
            .get_volume(opening.volume)
            .await
            .unwrap()
            .unwrap()
            .writer_lease
            .is_none()
    );
    let next = store
        .place(
            agent,
            placement.node_id,
            jiff::Timestamp::now()
                .checked_add(Duration::from_secs(60))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(next.epoch, placement.epoch + 1);
    assert_eq!(next.last_change_reason, PlacementChangeReason::Eviction);
    guard.cleanup().await;
}

#[tokio::test]
async fn cleanup_keeps_fencing_when_the_placement_is_stale() {
    let Some(stack) = swarmy_testkit::Stack::load("hosting-release") else {
        return;
    };
    let (store, mut guard) = stack
        .open_store(Arc::new(swarmy_store::blob::MemoryBlobStore::default()))
        .await;
    let root = tempfile::tempdir().unwrap();
    let (hosting, agent, placement, opening, _api) = placed(&store, &stack, root.path()).await;
    store.release(&placement).await.unwrap();
    let replacement = store
        .place(
            agent,
            placement.node_id,
            jiff::Timestamp::now()
                .checked_add(Duration::from_secs(60))
                .unwrap(),
        )
        .await
        .unwrap();
    let error = hosting
        .stop_and_release(agent, &opening, "eviction")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("fenced"), "{error}");
    assert_eq!(store.get_by_agent(agent).await.unwrap(), Some(replacement));
    assert_eq!(
        hosting.previous.lock().await.get(&agent),
        Some(&placement.epoch)
    );
    guard.cleanup().await;
}
#[tokio::test]
async fn keeper_renews_on_the_server_clock() {
    let Some(stack) = swarmy_testkit::Stack::load("hosting-server-clock") else {
        return;
    };
    let (store, mut guard) = stack
        .open_store(Arc::new(swarmy_store::blob::MemoryBlobStore::default()))
        .await;
    let clock = Arc::new(std::sync::Mutex::new(
        jiff::Timestamp::now()
            .checked_sub(Duration::from_secs(3600))
            .unwrap(),
    ));
    let manual = clock.clone();
    let store = store.with_clock(move || *manual.lock().unwrap());
    let root = tempfile::tempdir().unwrap();
    let (hosting, agent, placement, opening, _api) = placed(&store, &stack, root.path()).await;
    *clock.lock().unwrap() = placement
        .expires_at
        .checked_sub(Duration::from_secs(50))
        .unwrap();
    hosting.keeper.sync_once().await.unwrap();
    let stored = store.get_by_agent(agent).await.unwrap().unwrap();
    assert_eq!(
        stored.expires_at,
        clock
            .lock()
            .unwrap()
            .checked_add(Duration::from_secs(60))
            .unwrap()
    );
    let remaining = opening
        .hold
        .deadline()
        .unwrap()
        .duration_since(Instant::now());
    assert!(
        remaining > Duration::from_secs(53) && remaining <= Duration::from_secs(54),
        "{remaining:?}"
    );
    guard.cleanup().await;
}
#[tokio::test]
async fn keeper_rides_out_a_node_api_outage() {
    let Some(stack) = swarmy_testkit::Stack::load("hosting-api-outage") else {
        return;
    };
    let (store, mut guard) = stack
        .open_store(Arc::new(swarmy_store::blob::MemoryBlobStore::default()))
        .await;
    let root = tempfile::tempdir().unwrap();
    let (hosting, agent, old, opening, mut api) = placed(&store, &stack, root.path()).await;
    api.pause();
    let mut sync = Box::pin(hosting.keeper.sync_once());
    assert!(
        tokio::time::timeout(Duration::from_secs(2), &mut sync)
            .await
            .is_err()
    );
    api.resume().await;
    tokio::time::timeout(Duration::from_secs(10), sync)
        .await
        .unwrap()
        .unwrap();
    let current = store.get_by_agent(agent).await.unwrap().unwrap();
    assert!(current.expires_at > old.expires_at);
    assert!(opening.hold.deadline().is_some());
    guard.cleanup().await;
}
#[tokio::test]
async fn hosting_remembers_registered_epochs() {
    let Some(stack) = swarmy_testkit::Stack::load("hosting-previous") else {
        return;
    };
    let (store, mut guard) = stack
        .open_store(Arc::new(swarmy_store::blob::MemoryBlobStore::default()))
        .await;
    let root = tempfile::tempdir().unwrap();
    let (hosting, agent, placement, _opening, _api) = placed(&store, &stack, root.path()).await;
    assert_eq!(
        hosting.previous.lock().await.get(&agent),
        Some(&placement.epoch)
    );
    guard.cleanup().await;
}
#[tokio::test]
async fn sync_retry_after_a_dropped_answer() {
    let Some(stack) = swarmy_testkit::Stack::load("hosting-sync-retry") else {
        return;
    };
    let (store, mut guard) = stack
        .open_store(Arc::new(swarmy_store::blob::MemoryBlobStore::default()))
        .await;
    let node_id = NodeId::from_ulid(ulid::Ulid::generate());
    let record = NodeRecord {
        node_id,
        roles: vec![NodeRole::Sandbox],
        capacity: NodeCapacity {
            cpu_millis: 1000,
            memory_bytes: 4 * 1024 * 1024 * 1024,
            disk_bytes: 1000,
            sandboxes: 1,
        },
        last_heartbeat: store.now(),
        cached_images: vec![],
        gpu: None,
    };
    store.put_node(&record).await.unwrap();
    let (_client, keeper, _api, count) = node_api(&store, &stack, &record, true).await;
    let agent = AgentId::from_ulid(ulid::Ulid::generate());
    let placement = store
        .place(
            agent,
            node_id,
            store.now().checked_add(Duration::from_secs(60)).unwrap(),
        )
        .await
        .unwrap();
    store.claim_placement(&placement).await.unwrap();
    let hold = keeper.hold_placement(
        placement,
        Answered {
            sent: Instant::now(),
            server_now: store.now(),
        },
        Arc::default(),
    );
    keeper.sync_once().await.unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert_eq!(hold.current(), store.get_by_agent(agent).await.unwrap());
    guard.cleanup().await;
}
