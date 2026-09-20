use std::{
    num::NonZeroUsize,
    sync::{Arc, OnceLock},
    time::Duration,
};

use foundationdb::{Database, api::NetworkAutoStop, tuple::Subspace};
use futures::TryStreamExt;
use jiff::Timestamp;
use object_store::{ObjectStore, local::LocalFileSystem, path::Path};
use swarmy_config::GarbageCollection;
use swarmy_core::{
    CHUNK_SIZE, ContentHash, GcRun, ImageTag, Lease, LeaseOwnerId, ManifestId, VolumeId,
};
use swarmy_store::{Store, StoreError, blob::MemoryBlobStore};
use swarmy_volume::{
    ChunkStore, Manifest, ManifestBuilder, VolumeDevice, VolumeError, VolumeWriter, gc::collect,
};
use ulid::Ulid;

struct Fixture {
    store: Store,
    objects: Arc<LocalFileSystem>,
    directory: tempfile::TempDir,
    db: Arc<Database>,
    root: Subspace,
}
impl Fixture {
    fn new() -> Option<Self> {
        static NETWORK: OnceLock<NetworkAutoStop> = OnceLock::new();
        let Ok(cluster) = std::env::var("SWARMY_FDB_CLUSTER_FILE") else {
            eprintln!("skipping GC integration test: SWARMY_FDB_CLUSTER_FILE is unset");
            return None;
        };
        NETWORK.get_or_init(swarmy_store::boot);
        let directory = tempfile::tempdir().unwrap();
        let objects = Arc::new(LocalFileSystem::new_with_prefix(directory.path()).unwrap());
        let db = Arc::new(Database::new(Some(&cluster)).unwrap());
        let root = Subspace::all().subspace(&(format!("swarmy-gc-test-{}", Ulid::generate()),));
        let store = Store::with_subspace(
            db.clone(),
            root.clone(),
            Arc::new(MemoryBlobStore::default()),
        );
        Some(Self {
            store,
            objects,
            directory,
            db,
            root,
        })
    }

    async fn volume(&self) -> (VolumeId, ManifestId, Lease) {
        let id = VolumeId::from_ulid(Ulid::generate());
        let base = manifest_id();
        self.store
            .put_manifest(
                base,
                Manifest::empty(u64::from(CHUNK_SIZE) * 2).unwrap().header(),
            )
            .await
            .unwrap();
        self.store.create_volume(id, base).await.unwrap();
        let now = Timestamp::now();
        let lease = self
            .store
            .acquire_writer_lease(
                id,
                owner(),
                now,
                now.checked_add(Duration::from_secs(600)).unwrap(),
            )
            .await
            .unwrap();
        (id, base, lease)
    }

    async fn age_chunks(&self) {
        for object in self
            .objects
            .list(Some(&Path::from("chunks")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
        {
            let file =
                std::fs::File::open(self.directory.path().join(object.location.as_ref())).unwrap();
            file.set_times(std::fs::FileTimes::new().set_modified(std::time::UNIX_EPOCH))
                .unwrap();
        }
    }

    async fn clear(&self) {
        self.db
            .run(|trx, _| async move {
                let (begin, end) = self.root.range();
                trx.clear_range(&begin, &end);
                Ok(())
            })
            .await
            .unwrap();
    }
}
fn manifest_id() -> ManifestId {
    ManifestId::from_ulid(Ulid::generate())
}
fn owner() -> LeaseOwnerId {
    LeaseOwnerId::from_ulid(Ulid::generate())
}
fn policy() -> GarbageCollection {
    GarbageCollection {
        filter_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
        ..GarbageCollection::default()
    }
}
fn run_record() -> GcRun {
    GcRun {
        owner: owner(),
        started_at: Timestamp::now(),
        dry_run: false,
        finished: false,
        error: None,
        manifests: 0,
        scanned: 0,
        candidates: 0,
        candidate_bytes: 0,
        deleted: 0,
        bytes_freed: 0,
        duration_ms: 0,
    }
}

async fn retained_revisions(test: &Fixture, chunks: &ChunkStore) -> Vec<(ManifestId, ContentHash)> {
    let (volume, mut head, lease) = test.volume().await;
    let mut manifest = Manifest::empty(u64::from(CHUNK_SIZE) * 2).unwrap();
    let shared = chunks
        .put_chunk(&vec![99; CHUNK_SIZE as usize])
        .await
        .unwrap()
        .hash;
    let mut revisions = Vec::new();
    for value in 1..=8_u8 {
        let hash = chunks
            .put_chunk(&vec![value; CHUNK_SIZE as usize])
            .await
            .unwrap()
            .hash;
        let mut builder = ManifestBuilder::new(test.objects.clone(), manifest);
        builder.set_chunk(0, hash).unwrap();
        builder.set_chunk(1, shared).unwrap();
        manifest = builder.build().await.unwrap();
        let next = manifest_id();
        test.store
            .advance_volume_retained(
                volume,
                &lease,
                head,
                next,
                manifest.header(),
                NonZeroUsize::new(3).unwrap(),
            )
            .await
            .unwrap();
        head = next;
        revisions.push((next, hash));
        if value == 1 {
            test.store
                .clone_volume(volume, VolumeId::from_ulid(Ulid::generate()))
                .await
                .unwrap();
        }
        if value == 2 {
            test.store
                .put_image("base", &ImageTag("stable".into()), next)
                .await
                .unwrap();
        }
    }
    // Both metadata scans cross their 64-row boundary.
    for index in 0..70 {
        test.store
            .clone_volume(volume, VolumeId::from_ulid(Ulid::generate()))
            .await
            .unwrap();
        test.store
            .put_image(&format!("image-{index}"), &ImageTag("stable".into()), head)
            .await
            .unwrap();
    }
    assert_eq!(test.store.volume_snapshots(volume).await.unwrap().len(), 3);
    revisions
}

#[tokio::test]
async fn pruning_dry_run_and_paged_live_set_preserve_readable_snapshots() {
    let Some(test) = Fixture::new() else {
        return;
    };
    let chunks = ChunkStore::new(test.objects.clone());
    let revisions = retained_revisions(&test, &chunks).await;
    let zero = ContentHash::ZERO.to_string();
    test.objects
        .put(&Path::from(format!("chunks/00/{zero}")), vec![0; 16].into())
        .await
        .unwrap();
    test.age_chunks().await;
    let recent = chunks
        .put_chunk(&vec![200; CHUNK_SIZE as usize])
        .await
        .unwrap()
        .hash;
    let before: Vec<_> = test.objects.list(None).try_collect().await.unwrap();
    let dry = collect(&test.store, test.objects.clone(), policy(), true)
        .await
        .unwrap();
    assert_eq!(dry.candidates, 3);
    assert_eq!(dry.candidate_bytes, u64::from(CHUNK_SIZE) * 3);
    assert_eq!(dry.deleted, 0);
    assert_eq!(dry.bytes_freed, 0);
    assert_eq!(
        test.objects
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap(),
        before
    );
    let run = collect(&test.store, test.objects.clone(), policy(), false)
        .await
        .unwrap();
    assert_eq!(run.deleted, 3);
    assert_eq!(run.bytes_freed, u64::from(CHUNK_SIZE) * 3);
    assert_eq!(test.store.get_gc_run(run.owner).await.unwrap(), Some(run));
    for (index, (_, hash)) in revisions.iter().enumerate() {
        assert_eq!(
            chunks.get_chunk(*hash).await.is_ok(),
            !(2..5).contains(&index)
        );
    }
    assert!(chunks.get_chunk(recent).await.is_ok());
    for id in test.store.live_manifests().await.unwrap() {
        let live = Manifest::load(
            &*test.objects,
            test.store.get_manifest(id).await.unwrap().unwrap(),
        )
        .await
        .unwrap();
        for block in 0..2 {
            chunks
                .get_chunk(live.chunk_hash(&*test.objects, block).await.unwrap())
                .await
                .unwrap();
        }
    }
    for object in before
        .iter()
        .filter(|object| object.location.as_ref().starts_with("manifests/"))
    {
        test.objects.get(&object.location).await.unwrap();
    }
    test.objects
        .get(&Path::from(format!("chunks/00/{zero}")))
        .await
        .unwrap();
    test.clear().await;
}

#[tokio::test]
async fn background_upload_survives_sweep_before_manifest_publication() {
    let Some(test) = Fixture::new() else {
        return;
    };
    let (volume, head, lease) = test.volume().await;
    let local = tempfile::tempdir().unwrap();
    let device = VolumeDevice::open(
        ChunkStore::new(test.objects.clone()),
        Manifest::empty(u64::from(CHUNK_SIZE) * 2).unwrap(),
        local.path().join("cache"),
        local.path().join("dirty"),
        0,
    )
    .await
    .unwrap();
    let writer = VolumeWriter::new(device.clone(), test.store.clone(), volume, lease, head);
    let uploader = writer.background(Duration::from_millis(5));
    let data = vec![42; CHUNK_SIZE as usize];
    device.write(0, &data).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while device.upload_stats().chunks_uploaded == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        test.store
            .get_volume(volume)
            .await
            .unwrap()
            .unwrap()
            .head_manifest,
        head
    );
    let run = collect(&test.store, test.objects.clone(), policy(), false)
        .await
        .unwrap();
    assert_eq!(run.scanned, 1);
    assert_eq!(run.candidates, 0);
    let published = writer.checkpoint(None).await.unwrap();
    let manifest = Manifest::load(
        &*test.objects,
        test.store.get_manifest(published).await.unwrap().unwrap(),
    )
    .await
    .unwrap();
    let hash = manifest.chunk_hash(&*test.objects, 0).await.unwrap();
    assert_eq!(
        ChunkStore::new(test.objects.clone())
            .get_chunk(hash)
            .await
            .unwrap(),
        data
    );
    drop(uploader);
    writer.release().await.unwrap();
    test.clear().await;
}

#[tokio::test]
async fn collector_lease_excludes_competitors_and_recovers_after_crash() {
    let Some(test) = Fixture::new() else {
        return;
    };
    test.store.get_gc_run(owner()).await.unwrap();
    let first = run_record();
    let second = run_record();
    let expires = Timestamp::now()
        .checked_add(Duration::from_secs(1))
        .unwrap();
    let (a, b) = tokio::join!(
        test.store.acquire_gc_lease(&first, expires),
        test.store.acquire_gc_lease(&second, expires)
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let lease = a.or(b).unwrap();
    assert!(matches!(
        collect(&test.store, test.objects.clone(), policy(), false).await,
        Err(VolumeError::Store(StoreError::LeaseMismatch))
    ));
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let next = run_record();
    let replacement = test
        .store
        .acquire_gc_lease(
            &next,
            Timestamp::now()
                .checked_add(Duration::from_secs(10))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(replacement.seq > lease.seq);
    assert!(matches!(
        test.store
            .renew_gc_lease(&lease, replacement.expires_at)
            .await,
        Err(StoreError::LeaseMismatch)
    ));
    assert!(matches!(
        test.store.finish_gc_run(&lease, &first).await,
        Err(StoreError::LeaseMismatch)
    ));
    let renewed = test
        .store
        .renew_gc_lease(
            &replacement,
            replacement
                .expires_at
                .checked_add(Duration::from_secs(1))
                .unwrap(),
        )
        .await
        .unwrap();
    test.store.finish_gc_run(&renewed, &next).await.unwrap();
    collect(&test.store, test.objects.clone(), policy(), false)
        .await
        .unwrap();
    test.clear().await;
}

#[tokio::test]
async fn missing_live_root_aborts_before_any_deletion() {
    let Some(test) = Fixture::new() else {
        return;
    };
    let (volume, head, lease) = test.volume().await;
    let orphan = ChunkStore::new(test.objects.clone())
        .put_chunk(&vec![33; CHUNK_SIZE as usize])
        .await
        .unwrap()
        .hash;
    test.age_chunks().await;
    let mut header = test.store.get_manifest(head).await.unwrap().unwrap();
    header.root_hash = ContentHash([1; 32]);
    test.store
        .advance_volume(volume, &lease, head, manifest_id(), &header)
        .await
        .unwrap();
    assert!(
        collect(&test.store, test.objects.clone(), policy(), false)
            .await
            .is_err()
    );
    ChunkStore::new(test.objects.clone())
        .get_chunk(orphan)
        .await
        .unwrap();
    test.clear().await;
}

#[tokio::test]
async fn deduplicated_old_chunk_is_protected_before_new_manifest_exists() {
    let Some(test) = Fixture::new() else {
        return;
    };
    let data = vec![83; CHUNK_SIZE as usize];
    let chunks = ChunkStore::with_gc_protection(test.objects.clone(), test.store.clone());
    let old = chunks.put_chunk(&data).await.unwrap();
    test.age_chunks().await;
    let local = tempfile::tempdir().unwrap();
    let device = VolumeDevice::open(
        chunks.clone(),
        Manifest::empty(u64::from(CHUNK_SIZE)).unwrap(),
        local.path().join("cache"),
        local.path().join("dirty"),
        0,
    )
    .await
    .unwrap();
    device.write(0, &data).await.unwrap();
    device.upload_dirty().await.unwrap();
    assert_eq!(device.upload_stats().chunks_uploaded, 0);
    let run = collect(&test.store, test.objects.clone(), policy(), false)
        .await
        .unwrap();
    assert_eq!(run.scanned, 1);
    assert_eq!(run.deleted, 0);
    assert_eq!(chunks.get_chunk(old.hash).await.unwrap(), data);
    test.clear().await;
}

#[tokio::test]
async fn reuse_waits_for_reserved_deletion_then_recreates_the_chunk() {
    let Some(test) = Fixture::new() else {
        return;
    };
    let data = vec![91; CHUNK_SIZE as usize];
    let chunks = ChunkStore::with_gc_protection(test.objects.clone(), test.store.clone());
    let old = chunks.put_chunk(&data).await.unwrap();
    test.age_chunks().await;
    let run = run_record();
    let lease = test
        .store
        .acquire_gc_lease(
            &run,
            Timestamp::now()
                .checked_add(Duration::from_secs(30))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        test.store
            .claim_gc_chunk(run.owner, old.hash, run.started_at, false)
            .await
            .unwrap()
    );
    assert!(matches!(
        test.store.protect_reused_chunk(old.hash).await,
        Err(StoreError::LeaseMismatch)
    ));
    let uploading = chunks.put_chunk(&data);
    tokio::pin!(uploading);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut uploading)
            .await
            .is_err()
    );
    let hex = old.hash.to_string();
    test.objects
        .delete(&Path::from(format!("chunks/{}/{hex}", &hex[..2])))
        .await
        .unwrap();
    test.store
        .finish_gc_chunk(run.owner, old.hash)
        .await
        .unwrap();
    let recreated = uploading.await.unwrap();
    assert!(recreated.uploaded);
    assert_eq!(recreated.hash, old.hash);
    test.store.finish_gc_run(&lease, &run).await.unwrap();
    let collected = collect(&test.store, test.objects.clone(), policy(), false)
        .await
        .unwrap();
    assert_eq!(collected.deleted, 0);
    assert_eq!(chunks.get_chunk(old.hash).await.unwrap(), data);
    test.clear().await;
}

#[tokio::test]
async fn deleted_computer_releases_placement_and_collects_all_disk_revisions() {
    use swarmy_core::AgentId;
    let Some(test) = Fixture::new() else {
        return;
    };
    let store = &test.store;
    let (volume, head, lease) = test.volume().await;
    let agent = AgentId::from_ulid(volume.as_ulid());
    let (session, placement) = computer_session(store, agent, head).await;
    let (roots, head) = deletion_revisions(&test, volume, head, &lease).await;
    let live = store.live_manifests().await.unwrap();
    assert!(roots.iter().all(|root| live.contains(root)));
    test.age_chunks().await;
    assert_eq!(
        collect(store, test.objects.clone(), policy(), false)
            .await
            .unwrap()
            .deleted,
        0
    );
    store.delete_computer(agent).await.unwrap();
    store.delete_computer(agent).await.unwrap();
    assert!(store.get_by_agent(agent).await.unwrap().is_none());
    assert!(store.get_volume(volume).await.unwrap().is_none());
    assert!(matches!(
        store.volume_snapshots(volume).await,
        Err(StoreError::VolumeMissing)
    ));
    let live = store.live_manifests().await.unwrap();
    assert!(roots.iter().all(|root| !live.contains(root)));
    assert!(matches!(
        store
            .renew(
                &placement,
                placement
                    .expires_at
                    .checked_add(Duration::from_secs(1))
                    .unwrap()
            )
            .await,
        Err(StoreError::ComputerDeleted)
    ));
    assert!(matches!(
        store
            .place(agent, placement.node_id, placement.expires_at)
            .await,
        Err(StoreError::ComputerDeleted)
    ));
    assert!(matches!(
        store.create_volume(volume, head).await,
        Err(StoreError::ComputerDeleted)
    ));
    // Capacity was returned exactly once despite repeating deletion.
    store
        .place(
            AgentId::from_ulid(Ulid::generate()),
            placement.node_id,
            placement.expires_at,
        )
        .await
        .unwrap();
    assert_eq!(
        collect(store, test.objects.clone(), policy(), false)
            .await
            .unwrap()
            .deleted,
        2
    );
    assert_eq!(
        store
            .read_events(session.session_id, 0, 10)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        store
            .fetch_session(session.session_id)
            .await
            .unwrap()
            .unwrap()
            .computer_deleted
    );
    test.clear().await;
}

async fn computer_session(
    store: &Store,
    agent: swarmy_core::AgentId,
    head: ManifestId,
) -> (swarmy_core::SessionRecord, swarmy_core::PlacementRecord) {
    use swarmy_core::{
        Event, Message, MessageId, MessageRole, NodeCapacity, NodeId, NodeRecord, NodeRole, Part,
        SessionId,
    };
    store
        .put_image("base", &ImageTag("test".into()), head)
        .await
        .unwrap();
    let session = swarmy_core::SessionRecord {
        session_id: SessionId::from_ulid(Ulid::generate()),
        agent_id: agent,
        kind: swarmy_core::SessionKind::Ephemeral,
        computer_deleted: false,
        plan: Vec::new(),
        state: swarmy_core::SessionState::Idle,
        head_seq: 0,
        snapshot_ref: None,
        inference: swarmy_core::InferenceSelection::default(),
    };
    store
        .create_session(&session, Timestamp::now(), "base:test")
        .await
        .unwrap();
    store
        .append_events(
            session.session_id,
            0,
            &[Event::MessageAppended {
                seq: 0,
                message: Message {
                    id: MessageId::from_ulid(Ulid::generate()),
                    role: MessageRole::User,
                    parts: vec![Part::Text {
                        text: "transcript survives deletion".into(),
                    }],
                },
            }],
        )
        .await
        .unwrap();
    let node = NodeRecord {
        node_id: NodeId::from_ulid(Ulid::generate()),
        roles: vec![NodeRole::Sandbox],
        capacity: NodeCapacity {
            sandboxes: 1,
            cpu_millis: 4000,
            memory_bytes: 1024 * 1024,
            disk_bytes: 1024 * 1024,
        },
        last_heartbeat: Timestamp::now(),
        cached_images: vec![],
    };
    store.put_node(&node).await.unwrap();
    let placement = store
        .place(
            agent,
            node.node_id,
            Timestamp::now()
                .checked_add(Duration::from_secs(600))
                .unwrap(),
        )
        .await
        .unwrap();
    (session, placement)
}

async fn deletion_revisions(
    test: &Fixture,
    volume: VolumeId,
    mut head: ManifestId,
    lease: &Lease,
) -> (Vec<ManifestId>, ManifestId) {
    let store = &test.store;
    let chunks = ChunkStore::new(test.objects.clone());
    let mut manifest = Manifest::empty(u64::from(CHUNK_SIZE) * 2).unwrap();
    let mut roots = Vec::new();
    for value in [21, 22] {
        let hash = chunks
            .put_chunk(&vec![value; CHUNK_SIZE as usize])
            .await
            .unwrap()
            .hash;
        let mut builder = ManifestBuilder::new(test.objects.clone(), manifest);
        builder.set_chunk(0, hash).unwrap();
        manifest = builder.build().await.unwrap();
        let next = manifest_id();
        store
            .advance_volume_retained(
                volume,
                lease,
                head,
                next,
                manifest.header(),
                NonZeroUsize::new(10).unwrap(),
            )
            .await
            .unwrap();
        head = next;
        roots.push(next);
    }
    (roots, head)
}
