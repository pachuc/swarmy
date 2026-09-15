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
        upload(&device, publish).await.unwrap();
        assert_eq!(objects.peak.load(Ordering::Relaxed), 4);
        assert_eq!(objects.checks.lock().unwrap().values().sum::<usize>(), 20);
        assert_eq!(device.stats().uploads_in_flight, 0);
    }
}
