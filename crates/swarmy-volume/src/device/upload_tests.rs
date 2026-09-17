use super::*;
use std::{
    sync::{Mutex as StdMutex, Weak},
    time::Duration,
};

use futures::stream::BoxStream;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path as ObjectPath,
};
use tokio::time::Instant;

#[derive(Debug)]
struct RecordingStore {
    inner: InMemory,
    delay: Duration,
    active: AtomicU64,
    peak: AtomicU64,
    checks: StdMutex<HashMap<ObjectPath, usize>>,
    failures: StdMutex<HashSet<ObjectPath>>,
    timing: StdMutex<Vec<(Instant, Instant)>>,
    device: StdMutex<Weak<VolumeDevice>>,
    limit: u64,
    gate: StdMutex<Option<Arc<tokio::sync::Semaphore>>>,
}

impl std::fmt::Display for RecordingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("recording store")
    }
}

#[async_trait::async_trait]
impl ObjectStore for RecordingStore {
    async fn put_opts(
        &self,
        path: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        if !path.as_ref().starts_with("chunks/") {
            return self.inner.put_opts(path, payload, options).await;
        }
        let started = Instant::now();
        let active = self.active.fetch_add(1, Ordering::Relaxed) + 1;
        let _guard = UploadGuard(&self.active);
        self.peak.fetch_max(active, Ordering::Relaxed);
        let device = self.device.lock().unwrap().upgrade().unwrap();
        let reported = device.stats().uploads_in_flight;
        assert!(reported >= active && reported <= self.limit);
        let gate = self.gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.acquire().await.unwrap().forget();
        }
        tokio::time::sleep(self.delay).await;
        self.timing.lock().unwrap().push((started, Instant::now()));
        if self.failures.lock().unwrap().remove(path) {
            return Err(object_store::Error::Generic {
                store: "recording store",
                source: std::io::Error::other("injected upload failure").into(),
            });
        }
        self.inner.put_opts(path, payload, options).await
    }

    async fn head(&self, path: &ObjectPath) -> object_store::Result<ObjectMeta> {
        if path.as_ref().starts_with("chunks/") {
            *self.checks.lock().unwrap().entry(path.clone()).or_default() += 1;
        }
        self.inner.head(path).await
    }

    async fn put_multipart_opts(
        &self,
        path: &ObjectPath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(
        &self,
        path: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(path, options).await
    }

    async fn delete(&self, path: &ObjectPath) -> object_store::Result<()> {
        self.inner.delete(path).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

async fn fixture(
    count: u8,
    limit: usize,
    delay: Duration,
) -> (tempfile::TempDir, Arc<RecordingStore>, Arc<VolumeDevice>) {
    let dir = tempfile::tempdir().unwrap();
    let objects = Arc::new(RecordingStore {
        inner: InMemory::new(),
        delay,
        active: AtomicU64::new(0),
        peak: AtomicU64::new(0),
        checks: StdMutex::new(HashMap::new()),
        failures: StdMutex::new(HashSet::new()),
        timing: StdMutex::new(Vec::new()),
        device: StdMutex::new(Weak::new()),
        limit: limit as u64,
        gate: StdMutex::new(None),
    });
    let device = VolumeDevice::open_with_upload_concurrency(
        ChunkStore::new(objects.clone()),
        Manifest::empty(u64::from(count) * u64::from(CHUNK_SIZE)).unwrap(),
        dir.path().join("cache"),
        dir.path().join("dirty"),
        0,
        NonZeroUsize::new(limit).unwrap(),
    )
    .await
    .unwrap();
    *objects.device.lock().unwrap() = Arc::downgrade(&device);
    for number in 0..count {
        device
            .write(
                u64::from(number) * u64::from(CHUNK_SIZE),
                &[number + 1; 4096],
            )
            .await
            .unwrap();
    }
    (dir, objects, device)
}

async fn upload(device: &VolumeDevice, publish: bool) -> Result<()> {
    if publish {
        device.publish(|_| async { Ok(()) }).await?;
    } else {
        device.upload_dirty().await?;
    }
    Ok(())
}

#[tokio::test]
async fn both_paths_overlap_requests_within_the_configured_bound() {
    for publish in [false, true] {
        let mut elapsed = Vec::new();
        for limit in [1, 3, 32] {
            let (_dir, objects, device) = fixture(12, limit, Duration::from_millis(40)).await;
            let started = Instant::now();
            upload(&device, publish).await.unwrap();
            elapsed.push(started.elapsed());
            assert_eq!(device.stats().uploads_in_flight, 0);
            assert_eq!(objects.peak.load(Ordering::Relaxed), limit.min(12) as u64);
            let timing = objects.timing.lock().unwrap();
            assert_eq!(timing.len(), 12);
            // Count overlapping request intervals independently of the counter.
            for &(start, _) in timing.iter() {
                let overlapping = timing
                    .iter()
                    .filter(|&&(a, b)| a <= start && start < b)
                    .count();
                assert!(overlapping <= limit);
            }
            assert_eq!(device.stats().dirty_bytes, 12 * BLOCK_SIZE);
        }
        eprintln!("publish={publish}, 12 chunks, 40ms PUT delay, limits 1/3/32: {elapsed:?}");
        assert!(elapsed[1] * 2 < elapsed[0], "{elapsed:?}");
    }
}

#[tokio::test]
async fn failed_batches_retain_successes_and_only_retry_failed_versions() {
    for publish in [false, true] {
        let (dir, objects, device) = fixture(10, 4, Duration::from_millis(5)).await;
        let mut bytes = vec![0; CHUNK_SIZE as usize];
        bytes[..4096].fill(1);
        let failed_path = crate::chunk_path(crate::content_hash(&bytes).unwrap());
        objects.failures.lock().unwrap().insert(failed_path.clone());
        if publish {
            assert!(
                device
                    .publish(|_| async { panic!("must not commit") })
                    .await
                    .is_err()
            );
        } else {
            assert!(device.upload_dirty().await.is_err());
        }
        {
            let dirty = device.dirty.lock().await;
            assert_eq!(dirty.pending.len(), 10);
            assert_eq!(dirty.uploaded.len(), 3);
            assert!(!dirty.uploaded.contains_key(&0));
            assert_eq!(dirty.published.header().root_hash, ContentHash::ZERO);
        }
        assert_eq!(device.stats().uploads_in_flight, 0);
        assert_eq!(
            objects
                .inner
                .list(Some(&ObjectPath::from("manifests")))
                .count()
                .await,
            0
        );
        assert_eq!(device.read(0, 4096).await.unwrap(), vec![1; 4096]);
        assert_eq!(fs::read(dir.path().join("dirty/map")).await.unwrap()[0], 1);
        upload(&device, publish).await.unwrap();
        upload(&device, publish).await.unwrap();
        {
            let checks = objects.checks.lock().unwrap();
            assert_eq!(checks.len(), 10);
            for (path, &count) in checks.iter() {
                assert_eq!(count, if *path == failed_path { 2 } else { 1 });
            }
        }
        // Rewriting one chunk invalidates only that version, even after pre-upload.
        device.write(4096, &[99; 4096]).await.unwrap();
        let manifest = device.publish(|_| async { Ok(()) }).await.unwrap();
        assert_eq!(objects.checks.lock().unwrap().values().sum::<usize>(), 12);
        assert!(device.dirty.lock().await.pending.is_empty());
        let reopened = VolumeDevice::open(
            ChunkStore::new(objects.clone()),
            manifest,
            dir.path().join("reopened-cache"),
            dir.path().join("reopened-dirty"),
            0,
        )
        .await
        .unwrap();
        assert_eq!(
            reopened.read(0, 8192).await.unwrap(),
            [vec![1; 4096], vec![99; 4096]].concat()
        );
        for number in 1..10_u8 {
            assert_eq!(
                reopened
                    .read(u64::from(number) * u64::from(CHUNK_SIZE), 4096)
                    .await
                    .unwrap(),
                vec![number + 1; 4096]
            );
        }
    }
}

#[tokio::test]
async fn cancellation_resets_in_flight_and_retains_pending_data() {
    let (_dir, objects, device) = fixture(8, 4, Duration::from_secs(30)).await;
    let uploading = device.clone();
    let task = tokio::spawn(async move { uploading.upload_dirty().await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while objects.active.load(Ordering::Relaxed) != 4 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(device.stats().uploads_in_flight, 4);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(device.stats().uploads_in_flight, 0);
    assert_eq!(objects.active.load(Ordering::Relaxed), 0);
    let dirty = device.dirty.lock().await;
    assert_eq!(dirty.pending.len(), 8);
    assert!(dirty.uploaded.is_empty());
}

#[tokio::test]
async fn simultaneous_background_and_publish_share_the_bound_and_versions() {
    let (_dir, objects, device) = fixture(12, 4, Duration::from_millis(5)).await;
    let (background, published) =
        tokio::join!(device.upload_dirty(), device.publish(|_| async { Ok(()) }),);
    background.unwrap();
    published.unwrap();
    assert_eq!(objects.peak.load(Ordering::Relaxed), 4);
    assert_eq!(device.stats().uploads_in_flight, 0);
    assert_eq!(objects.checks.lock().unwrap().len(), 12);
    assert!(
        objects
            .checks
            .lock()
            .unwrap()
            .values()
            .all(|&count| count == 1)
    );
    assert!(device.dirty.lock().await.pending.is_empty());
}

#[tokio::test]
async fn uploaded_versions_do_not_take_space_in_later_batches() {
    for publish in [false, true] {
        let (_dir, objects, device) = fixture(16, 4, Duration::from_millis(5)).await;
        device.upload_dirty().await.unwrap();
        for number in [0, 4, 8, 12] {
            device
                .write(number * u64::from(CHUNK_SIZE) + BLOCK_SIZE, &[99; 4096])
                .await
                .unwrap();
        }
        objects.peak.store(0, Ordering::Relaxed);
        // Hold the requests until every slot is occupied. Preparation yields to
        // foreground I/O, so a fixed 5 ms PUT delay is not an overlap barrier.
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        *objects.gate.lock().unwrap() = Some(gate.clone());
        let uploading = device.clone();
        let task = tokio::spawn(async move { upload(&uploading, publish).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while objects.active.load(Ordering::Relaxed) != 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        gate.add_permits(4);
        task.await.unwrap().unwrap();
        assert_eq!(objects.peak.load(Ordering::Relaxed), 4);
        assert_eq!(objects.checks.lock().unwrap().values().sum::<usize>(), 20);
        assert_eq!(device.stats().uploads_in_flight, 0);
    }
}

#[tokio::test]
async fn writes_proceed_during_upload_and_stale_generations_are_retried() {
    let (dir, objects, device) = fixture(1, 1, Duration::ZERO).await;
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    *objects.gate.lock().unwrap() = Some(gate.clone());
    let uploading = device.clone();
    let task = tokio::spawn(async move { uploading.upload_dirty().await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while objects.active.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // The PUT cannot finish until we release the gate. A lock held across the
    // request would make this write time out, regardless of network speed.
    tokio::time::timeout(Duration::from_secs(2), device.write(0, &[9; 4096]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(objects.active.load(Ordering::Relaxed), 1);
    gate.add_permits(1);
    task.await.unwrap().unwrap();
    {
        let dirty = device.dirty.lock().await;
        assert!(dirty.uploaded.is_empty());
        assert!(dirty.pending.contains(&0));
    }
    gate.add_permits(1);
    let manifest = device.publish(|_| async { Ok(()) }).await.unwrap();
    assert_eq!(objects.checks.lock().unwrap().values().sum::<usize>(), 2);
    let reopened = VolumeDevice::open(
        ChunkStore::new(objects),
        manifest,
        dir.path().join("new-cache"),
        dir.path().join("new-dirty"),
        0,
    )
    .await
    .unwrap();
    assert_eq!(reopened.read(0, 4096).await.unwrap(), vec![9; 4096]);
}

#[tokio::test]
async fn debounce_defers_active_chunks_but_final_publication_drains_them() {
    let (_dir, objects, device) = fixture(4, 2, Duration::ZERO).await;
    device
        .upload_settled(Duration::from_secs(60))
        .await
        .unwrap();
    assert!(objects.checks.lock().unwrap().is_empty());
    device.publish(|_| async { Ok(()) }).await.unwrap();
    assert_eq!(objects.checks.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn process_death_after_uploads_preserves_overlay_and_published_baseline() {
    const CHILD_DIR: &str = "SWARMY_UPLOAD_CRASH_TEST_DIR";
    if let Ok(path) = std::env::var(CHILD_DIR) {
        let dir = std::path::Path::new(&path);
        let store = ChunkStore::new(Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(dir.join("objects")).unwrap(),
        ));
        let device = VolumeDevice::open(
            store,
            Manifest::empty(u64::from(CHUNK_SIZE)).unwrap(),
            dir.join("cache"),
            dir.join("dirty"),
            0,
        )
        .await
        .unwrap();
        device.write(0, &[19; 4096]).await.unwrap();
        device.flush().await.unwrap();
        device
            .publish(|_| async {
                // Exit after both chunks and manifest objects exist, before the
                // authoritative head transaction. No Rust destructors run.
                std::process::exit(77);
            })
            .await
            .unwrap();
        unreachable!();
    }
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("objects")).unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "device::upload_tests::process_death_after_uploads_preserves_overlay_and_published_baseline"])
        .env(CHILD_DIR, dir.path()).status().unwrap();
    assert_eq!(status.code(), Some(77));
    let objects = Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(dir.path().join("objects")).unwrap(),
    );
    assert!(
        objects
            .list(Some(&ObjectPath::from("chunks")))
            .count()
            .await
            > 0
    );
    let store = ChunkStore::new(objects);
    let baseline = Manifest::empty(u64::from(CHUNK_SIZE)).unwrap();
    let recovered = VolumeDevice::open(
        store.clone(),
        baseline.clone(),
        dir.path().join("recovery-cache"),
        dir.path().join("recovery-dirty"),
        0,
    )
    .await
    .unwrap();
    assert_eq!(recovered.read(0, 4096).await.unwrap(), vec![0; 4096]);
    let resumed = VolumeDevice::open(
        store.clone(),
        baseline,
        dir.path().join("cache"),
        dir.path().join("dirty"),
        0,
    )
    .await
    .unwrap();
    assert_eq!(resumed.read(0, 4096).await.unwrap(), vec![19; 4096]);
    let published = resumed.publish(|_| async { Ok(()) }).await.unwrap();
    let committed = VolumeDevice::open(
        store,
        published,
        dir.path().join("committed-cache"),
        dir.path().join("committed-dirty"),
        0,
    )
    .await
    .unwrap();
    assert_eq!(committed.read(0, 4096).await.unwrap(), vec![19; 4096]);
}

#[tokio::test]
async fn lease_loss_during_flush_rejects_publication_and_keeps_dirty_data() {
    use jiff::Timestamp;
    use swarmy_core::{LeaseOwnerId, ManifestId, VolumeId};
    use swarmy_store::{Store, StoreError, blob::MemoryBlobStore};

    let Ok(cluster) = std::env::var("SWARMY_FDB_CLUSTER_FILE") else {
        eprintln!("skipping lease loss during flush: SWARMY_FDB_CLUSTER_FILE is unset");
        return;
    };
    let _network = swarmy_store::boot();
    let store = Store::open(
        Some(&cluster),
        Some(&[format!("swarmy-upload-fencing-{}", ulid::Ulid::generate())]),
        Arc::new(MemoryBlobStore::default()),
    )
    .await
    .unwrap();
    let (dir, objects, device) = fixture(1, 1, Duration::ZERO).await;
    let base = ManifestId::from_ulid(ulid::Ulid::generate());
    let volume = VolumeId::from_ulid(ulid::Ulid::generate());
    store
        .put_manifest(base, device.manifest.header())
        .await
        .unwrap();
    store.create_volume(volume, base).await.unwrap();
    let now = Timestamp::now();
    let expiry = now.checked_add(Duration::from_secs(60)).unwrap();
    let lease = store
        .acquire_writer_lease(
            volume,
            LeaseOwnerId::from_ulid(ulid::Ulid::generate()),
            now,
            expiry,
        )
        .await
        .unwrap();
    let writer =
        crate::VolumeWriter::new(device.clone(), store.clone(), volume, lease.clone(), base);
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    *objects.gate.lock().unwrap() = Some(gate.clone());
    device.flush().await.unwrap();
    let flushing = writer.clone();
    let task = tokio::spawn(async move { flushing.flush(None).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while objects.active.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Revoke the token while the chunk PUT is waiting, then let upload finish.
    store
        .release_writer_lease(volume, &lease, Timestamp::now())
        .await
        .unwrap();
    gate.add_permits(1);
    assert!(matches!(
        task.await.unwrap(),
        Err(VolumeError::Store(StoreError::LeaseMismatch))
    ));
    let record = store.get_volume(volume).await.unwrap().unwrap();
    assert_eq!(record.head_manifest, base);
    assert_eq!(device.read(0, 4096).await.unwrap(), vec![1; 4096]);
    assert_eq!(device.dirty.lock().await.pending.len(), 1);
    assert_eq!(device.dirty.lock().await.uploaded.len(), 1);
    let recovered = VolumeDevice::open(
        ChunkStore::new(objects.clone()),
        device.manifest.clone(),
        dir.path().join("recovered-cache"),
        dir.path().join("recovered-dirty"),
        0,
    )
    .await
    .unwrap();
    assert_eq!(recovered.read(0, 4096).await.unwrap(), vec![0; 4096]);
    let renewed = store
        .acquire_writer_lease(volume, lease.owner, Timestamp::now(), expiry)
        .await
        .unwrap();
    let retry = crate::VolumeWriter::new(device.clone(), store.clone(), volume, renewed, base);
    let committed = retry.flush(None).await.unwrap();
    assert_eq!(committed.frozen_chunks_uploaded, 0);
    assert_eq!(committed.uploads.chunks_uploaded, 0);
    let record = store.get_volume(volume).await.unwrap().unwrap();
    assert_eq!(record.head_manifest, committed.manifest_id);
    assert!(device.dirty.lock().await.pending.is_empty());
}

#[tokio::test]
async fn boundary_overwrites_preserve_memory_and_spilled_generations() {
    // One blocked upload leaves more than the 8 MiB copy budget awaiting upload.
    let (dir, objects, device) = fixture(40, 1, Duration::ZERO).await;
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    *objects.gate.lock().unwrap() = Some(gate.clone());
    let publishing = device.clone();
    let task = tokio::spawn(async move { publishing.publish(|_| async { Ok(()) }).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while objects.active.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for number in 0..40 {
        device
            .write(number * u64::from(CHUNK_SIZE), &[99; 8192])
            .await
            .unwrap();
    }
    {
        let dirty = device.dirty.lock().await;
        let boundary = dirty.boundary.as_ref().unwrap();
        assert_eq!(boundary.memory, 8 * 1024 * 1024);
        assert!(boundary.spill.metadata().await.unwrap().len() > 0);
    }
    gate.add_permits(80);
    let manifest = task.await.unwrap().unwrap();
    let restored = VolumeDevice::open(
        ChunkStore::new(objects.clone()),
        manifest,
        dir.path().join("restore-cache"),
        dir.path().join("restore-dirty"),
        0,
    )
    .await
    .unwrap();
    for number in 0..40_u8 {
        let offset = u64::from(number) * u64::from(CHUNK_SIZE);
        let expected = [vec![number + 1; 4096], vec![0; CHUNK_SIZE as usize - 4096]].concat();
        assert_eq!(
            blake3::hash(&restored.read(offset, CHUNK_SIZE as usize).await.unwrap()),
            blake3::hash(&expected)
        );
        assert_eq!(device.read(offset, 8192).await.unwrap(), vec![99; 8192]);
    }
    assert_eq!(device.dirty.lock().await.pending.len(), 40);
    device.publish(|_| async { Ok(()) }).await.unwrap();
    assert!(!device.has_unpublished_changes().await);
}

#[tokio::test]
async fn cancelled_boundary_is_abandoned_and_commit_does_not_block_writes() {
    let (_dir, objects, device) = fixture(2, 1, Duration::ZERO).await;
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    *objects.gate.lock().unwrap() = Some(gate.clone());
    let publishing = device.clone();
    let task = tokio::spawn(async move { publishing.publish(|_| async { Ok(()) }).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while objects.active.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    device
        .write(u64::from(CHUNK_SIZE), &[77; 4096])
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    device
        .write(u64::from(CHUNK_SIZE), &[88; 4096])
        .await
        .unwrap();
    assert!(device.dirty.lock().await.boundary.is_none());
    gate.add_permits(2);
    device
        .publish(|_| async {
            // The metadata commit itself can stall without excluding live writes.
            device.write(0, &[99; 4096]).await?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(device.has_unpublished_changes().await);
    assert_eq!(device.read(0, 4096).await.unwrap(), vec![99; 4096]);
}

#[tokio::test]
async fn every_retained_boundary_matches_the_independent_write_model() {
    let (dir, objects, device) = fixture(48, 4, Duration::ZERO).await;
    let mut retained = Vec::new();
    for round in 0..4_u8 {
        let mut expected = Vec::new();
        for number in 0..48_u8 {
            let bytes = vec![number + round + 1; CHUNK_SIZE as usize];
            device
                .write(u64::from(number) * u64::from(CHUNK_SIZE), &bytes)
                .await
                .unwrap();
            expected.push(blake3::hash(&bytes));
        }
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        *objects.gate.lock().unwrap() = Some(gate.clone());
        let publishing = device.clone();
        let task = tokio::spawn(async move { publishing.publish(|_| async { Ok(()) }).await });
        // Wait for boundary capture, including rounds whose first chunks dedup.
        tokio::time::timeout(Duration::from_secs(5), async {
            while device.dirty.lock().await.boundary.is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for number in 0..48 {
            device
                .write(
                    number * u64::from(CHUNK_SIZE),
                    &vec![200 + round; CHUNK_SIZE as usize],
                )
                .await
                .unwrap();
        }
        gate.add_permits(48);
        retained.push((task.await.unwrap().unwrap(), expected));
    }
    for (index, (manifest, expected)) in retained.into_iter().enumerate() {
        let restored = VolumeDevice::open(
            ChunkStore::new(objects.clone()),
            manifest,
            dir.path().join(format!("model-cache-{index}")),
            dir.path().join(format!("model-dirty-{index}")),
            0,
        )
        .await
        .unwrap();
        for (number, hash) in expected.into_iter().enumerate() {
            let bytes = restored
                .read(number as u64 * u64::from(CHUNK_SIZE), CHUNK_SIZE as usize)
                .await
                .unwrap();
            assert_eq!(
                blake3::hash(&bytes),
                hash,
                "snapshot {index}, chunk {number}"
            );
        }
    }
}

#[tokio::test]
async fn spill_failure_abandons_backup_without_rejecting_live_writes() {
    let (dir, objects, device) = fixture(40, 1, Duration::ZERO).await;
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    *objects.gate.lock().unwrap() = Some(gate.clone());
    let publishing = device.clone();
    let task = tokio::spawn(async move {
        publishing
            .publish(|_| async { panic!("invalid boundary must not commit") })
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while objects.active.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    device.dirty.lock().await.boundary.as_mut().unwrap().spill =
        fs::File::open(dir.path().join("dirty/data")).await.unwrap();
    for number in 0..40 {
        device
            .write(number * u64::from(CHUNK_SIZE), &[88; 4096])
            .await
            .unwrap();
    }
    gate.add_permits(40);
    assert!(task.await.unwrap().is_err());
    assert!(device.has_unpublished_changes().await);
    assert_eq!(
        device.read(39 * u64::from(CHUNK_SIZE), 4096).await.unwrap(),
        vec![88; 4096]
    );
}
