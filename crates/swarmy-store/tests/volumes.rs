use foundationdb::{Database, api::NetworkAutoStop, tuple::Subspace};
use jiff::Timestamp;
use std::{
    collections::BTreeSet,
    num::NonZeroUsize,
    sync::{Arc, OnceLock},
};
use swarmy_core::{
    CHUNK_SIZE, ContentHash, ImageTag, Lease, LeaseOwnerId, ManifestHeader, ManifestId, VolumeId,
};
use swarmy_store::{Store, blob::MemoryBlobStore};
use swarmy_volume::{ChunkStore, Manifest, SnapshotLoop, VolumeDevice, VolumeWriter};
use ulid::Ulid;

struct Fixture {
    store: Store,
    directory: std::path::PathBuf,
    db: Arc<Database>,
    root: Subspace,
}
impl Fixture {
    fn new() -> Option<Self> {
        static NETWORK: OnceLock<NetworkAutoStop> = OnceLock::new();
        let Ok(cluster) = std::env::var("SWARMY_FDB_CLUSTER_FILE") else {
            eprintln!("skipping snapshot integration test: SWARMY_FDB_CLUSTER_FILE is unset");
            return None;
        };
        NETWORK.get_or_init(swarmy_store::boot);
        let name = format!("swarmy-snapshot-test-{}", Ulid::generate());
        let db = Arc::new(Database::new(Some(&cluster)).unwrap());
        let root = Subspace::all().subspace(&(name.clone(),));
        Some(Self {
            store: Store::with_subspace(
                db.clone(),
                root.clone(),
                Arc::new(MemoryBlobStore::default()),
            ),
            directory: std::env::temp_dir().join(name),
            db,
            root,
        })
    }

    async fn volume(&self) -> (VolumeId, ManifestId, ManifestHeader, Lease) {
        let id = VolumeId::from_ulid(Ulid::generate());
        let base = manifest_id();
        let header = ManifestHeader {
            size: u64::from(CHUNK_SIZE),
            chunk_size: CHUNK_SIZE,
            root_hash: ContentHash::ZERO,
        };
        self.store.put_manifest(base, &header).await.unwrap();
        self.store.create_volume(id, base).await.unwrap();
        let now = Timestamp::now();
        let lease = self
            .store
            .acquire_writer_lease(
                id,
                LeaseOwnerId::from_ulid(Ulid::generate()),
                now,
                now.checked_add(std::time::Duration::from_secs(120))
                    .unwrap(),
            )
            .await
            .unwrap();
        (id, base, header, lease)
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

    async fn manifest_count(&self) -> usize {
        use futures::TryStreamExt;
        self.db
            .run(|trx, _| async move {
                let range = self.root.subspace(&("manifest",)).range();
                let rows: Vec<_> = trx
                    .get_ranges_keyvalues(range.into(), false)
                    .try_collect()
                    .await?;
                Ok(rows.len())
            })
            .await
            .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}
fn manifest_id() -> ManifestId {
    ManifestId::from_ulid(Ulid::generate())
}

async fn tick(
    received: &mut tokio::sync::mpsc::UnboundedReceiver<Option<ManifestId>>,
) -> Option<ManifestId> {
    tokio::time::timeout(std::time::Duration::from_secs(5), received.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn periodic_snapshots_skip_idle_staged_changes_publish_and_checkpoint_is_immediate() {
    let Some(test) = Fixture::new() else {
        return;
    };
    let (id, base, header, lease) = test.volume().await;
    let objects = Arc::new(object_store::memory::InMemory::new());
    let device = VolumeDevice::open(
        ChunkStore::new(objects.clone()),
        Manifest::load(&*objects, header).await.unwrap(),
        test.directory.join("cache"),
        test.directory.join("dirty"),
        0,
    )
    .await
    .unwrap();
    let writer = VolumeWriter::new(device.clone(), test.store.clone(), id, lease, base);
    let (ticks, mut received) = tokio::sync::mpsc::unbounded_channel();
    let task_writer = writer.clone();
    let periodic = SnapshotLoop::spawn(std::time::Duration::from_millis(20), move || {
        let writer = task_writer.clone();
        let ticks = ticks.clone();
        async move {
            let result = writer.flush_if_dirty(None).await?;
            ticks.send(result.map(|flush| flush.manifest_id)).unwrap();
            Ok::<_, swarmy_volume::VolumeError>(())
        }
    });
    assert_eq!(tick(&mut received).await, None);
    assert_eq!(test.manifest_count().await, 1);
    device.write(0, &[7; 4096]).await.unwrap();
    // Background staging must not make a changed volume appear clean.
    device.upload_dirty().await.unwrap();
    let published = loop {
        if let Some(id) = tick(&mut received).await {
            break id;
        }
    };
    assert_ne!(published, base);
    assert_eq!(
        test.store
            .get_volume(id)
            .await
            .unwrap()
            .unwrap()
            .head_manifest,
        published
    );
    for _ in 0..3 {
        assert_eq!(tick(&mut received).await, None);
    }
    assert_eq!(test.manifest_count().await, 2);
    assert_eq!(
        test.store.volume_snapshots(id).await.unwrap(),
        [published, base]
    );
    drop(periodic);
    // Checkpoint is independent of the configured timer, even on an idle disk.
    let task_writer = writer.clone();
    let _long_period = SnapshotLoop::spawn(std::time::Duration::from_secs(600), move || {
        let writer = task_writer.clone();
        async move { writer.flush_if_dirty(None).await.map(|_| ()) }
    });
    let checkpoint =
        tokio::time::timeout(std::time::Duration::from_secs(5), writer.checkpoint(None))
            .await
            .unwrap()
            .unwrap();
    assert_ne!(checkpoint, published);
    assert_eq!(
        test.store
            .get_volume(id)
            .await
            .unwrap()
            .unwrap()
            .head_manifest,
        checkpoint
    );
    assert_eq!(test.manifest_count().await, 3);
    test.clear().await;
}

#[tokio::test]
async fn retention_is_atomic_ordered_idempotent_and_does_not_prune_clones() {
    let Some(test) = Fixture::new() else {
        return;
    };
    let (id, base, header, lease) = test.volume().await;
    let clone = VolumeId::from_ulid(Ulid::generate());
    test.store.clone_volume(id, clone).await.unwrap();
    let mut published = vec![base];
    for value in (1..=13_u128).rev() {
        let previous = *published.last().unwrap();
        // IDs deliberately sort opposite to publication order.
        let next = ManifestId::from_ulid(Ulid::from(value));
        for _ in 0..2 {
            test.store
                .advance_volume(id, &lease, previous, next, &header)
                .await
                .unwrap();
        }
        published.push(next);
        let expected: Vec<_> = published.iter().rev().take(10).copied().collect();
        assert_eq!(test.store.volume_snapshots(id).await.unwrap(), expected);
        assert_eq!(
            test.store
                .get_volume(id)
                .await
                .unwrap()
                .unwrap()
                .head_manifest,
            expected[0]
        );
    }
    assert_eq!(test.store.volume_snapshots(id).await.unwrap().len(), 10);
    assert_eq!(test.store.volume_snapshots(clone).await.unwrap(), [base]);
    for &id in &published {
        assert!(test.store.get_manifest(id).await.unwrap().is_some());
    }
    // Simulate a volume written before per-volume retention was introduced.
    test.db
        .run(|trx, _| {
            let root = &test.root;
            async move {
                trx.clear(&root.pack(&("volume_snapshots", id.as_ulid().to_bytes().as_slice())));
                Ok(())
            }
        })
        .await
        .unwrap();
    assert_eq!(
        test.store.volume_snapshots(id).await.unwrap(),
        published.iter().rev().take(10).copied().collect::<Vec<_>>()
    );
    let previous = *published.last().unwrap();
    let next = manifest_id();
    test.store
        .advance_volume_retained(
            id,
            &lease,
            previous,
            next,
            &header,
            NonZeroUsize::new(3).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        test.store.volume_snapshots(id).await.unwrap(),
        [next, previous, published[published.len() - 2]]
    );
    test.clear().await;
}

#[tokio::test]
async fn live_roots_are_retained_snapshots_attached_heads_and_images_only() {
    let Some(test) = Fixture::new() else {
        return;
    };
    let (id, base, header, lease) = test.volume().await;
    let retained = manifest_id();
    test.store
        .advance_volume_retained(
            id,
            &lease,
            base,
            retained,
            &header,
            NonZeroUsize::new(1).unwrap(),
        )
        .await
        .unwrap();
    let newest = manifest_id();
    test.store
        .advance_volume_retained(
            id,
            &lease,
            retained,
            newest,
            &header,
            NonZeroUsize::new(2).unwrap(),
        )
        .await
        .unwrap();
    test.store
        .release_writer_lease(id, &lease, Timestamp::now())
        .await
        .unwrap();
    let (attached, attached_head, _, _) = test.volume().await;
    let (detached, detached_head, _, detached_lease) = test.volume().await;
    test.store
        .release_writer_lease(detached, &detached_lease, Timestamp::now())
        .await
        .unwrap();
    // Independently exercise the attached-head rule with no retained snapshots.
    test.db
        .run(|trx, _| {
            let root = &test.root;
            async move {
                for volume in [attached, detached] {
                    let key =
                        root.pack(&("volume_snapshots", volume.as_ulid().to_bytes().as_slice()));
                    trx.set(
                        &key,
                        &swarmy_core::encode(&Vec::<ManifestId>::new()).unwrap(),
                    );
                }
                Ok(())
            }
        })
        .await
        .unwrap();
    let orphan = manifest_id();
    test.store.put_manifest(orphan, &header).await.unwrap();
    let mut expected = BTreeSet::from([retained, newest, attached_head]);
    // Cross the store's scan page boundary, and deduplicate shared image roots.
    for index in 0..70 {
        let image = manifest_id();
        test.store.put_manifest(image, &header).await.unwrap();
        test.store
            .put_image(&format!("image-{index}"), &ImageTag("v1".into()), image)
            .await
            .unwrap();
        expected.insert(image);
    }
    test.store
        .put_image("shared", &ImageTag("v1".into()), retained)
        .await
        .unwrap();
    let live = test.store.live_manifests().await.unwrap();
    assert_eq!(live, expected);
    for excluded in [base, detached_head, orphan] {
        assert!(!live.contains(&excluded));
    }
    test.clear().await;
}
