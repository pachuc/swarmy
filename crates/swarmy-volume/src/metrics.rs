//! Per-device object-store calls. Internal HTTP retries are owned by `object_store`
//! and are not separate calls here. The volume path uses HEAD, GET, and PUT only.
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, path::Path,
};
use serde::{Deserialize, Serialize};

/// Monotonic totals for one attachment, including background uploads and reads.
/// Bytes and chunks count successful PUTs; requests count attempted API calls,
/// including deduplication HEADs and manifest I/O, but excluding HTTP retries.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct UploadStats {
    pub chunks_uploaded: u64,
    pub object_store_requests: u64,
    pub bytes_uploaded: u64,
    /// Sum across dirty-store acquisitions, including reads and writes.
    pub dirty_lock_wait: Duration,
    /// Sum of HEAD/GET/PUT call durations; overlapping calls can exceed wall time.
    pub object_store_time: Duration,
}

impl UploadStats {
    pub(crate) fn since(self, before: Self) -> Self {
        Self {
            chunks_uploaded: self.chunks_uploaded - before.chunks_uploaded,
            object_store_requests: self.object_store_requests - before.object_store_requests,
            bytes_uploaded: self.bytes_uploaded - before.bytes_uploaded,
            dirty_lock_wait: self.dirty_lock_wait.saturating_sub(before.dirty_lock_wait),
            object_store_time: self
                .object_store_time
                .saturating_sub(before.object_store_time),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct UploadCounters {
    chunks: AtomicU64,
    requests: AtomicU64,
    bytes: AtomicU64,
    lock_wait_ns: AtomicU64,
    request_ns: AtomicU64,
}

impl UploadCounters {
    pub(crate) fn snapshot(&self) -> UploadStats {
        UploadStats {
            chunks_uploaded: self.chunks.load(Ordering::Relaxed),
            object_store_requests: self.requests.load(Ordering::Relaxed),
            bytes_uploaded: self.bytes.load(Ordering::Relaxed),
            dirty_lock_wait: Duration::from_nanos(self.lock_wait_ns.load(Ordering::Relaxed)),
            object_store_time: Duration::from_nanos(self.request_ns.load(Ordering::Relaxed)),
        }
    }

    pub(crate) fn record_lock_wait(&self, start: Instant) {
        add_duration(&self.lock_wait_ns, start.elapsed());
    }
}

fn add_duration(counter: &AtomicU64, elapsed: Duration) {
    counter.fetch_add(
        u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
}

#[derive(Debug)]
pub(crate) struct MeteredStore {
    pub inner: Arc<dyn ObjectStore>,
    pub counters: Arc<UploadCounters>,
}

impl std::fmt::Display for MeteredStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "metered({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for MeteredStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.counters.requests.fetch_add(1, Ordering::Relaxed);
        let bytes = payload.content_length() as u64;
        let start = Instant::now();
        let result = self.inner.put_opts(location, payload, opts).await;
        add_duration(&self.counters.request_ns, start.elapsed());
        if result.is_ok() {
            self.counters.bytes.fetch_add(bytes, Ordering::Relaxed);
            if location.as_ref().starts_with("chunks/") {
                self.counters.chunks.fetch_add(1, Ordering::Relaxed);
            }
        }
        result
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.counters.requests.fetch_add(1, Ordering::Relaxed);
        let start = Instant::now();
        let result = self.inner.get_opts(location, options).await;
        add_duration(&self.counters.request_ns, start.elapsed());
        result
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.counters.requests.fetch_add(1, Ordering::Relaxed);
        let start = Instant::now();
        let result = self.inner.head(location).await;
        add_duration(&self.counters.request_ns, start.elapsed());
        result
    }

    // These methods are required by ObjectStore but unused by the volume path.
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
