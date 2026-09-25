use std::{
    collections::{BTreeSet, HashMap, HashSet},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use std::time::{Duration, Instant};

use crate::metrics::{MeteredStore, UploadCounters, UploadStats};
use bytes::Bytes;
use futures::{StreamExt, stream::FuturesUnordered};
use swarmy_core::{CHUNK_SIZE, ContentHash, encode};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{Mutex, MutexGuard, Notify},
};

#[cfg(test)]
use crate::ManifestBuilder;
use crate::{BLOCKS_PER_LEAF, ChunkStore, Manifest, Result, VolumeError};

const FETCH_BUCKET_US: [u64; 12] = [
    250, 500, 1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 64_000, 128_000, 256_000, 512_000,
];

fn fetch_percentile(buckets: &[AtomicU64; 12], percentile: u64) -> Option<u64> {
    let total: u64 = buckets
        .iter()
        .map(|bucket| bucket.load(Ordering::Relaxed))
        .sum();
    if total == 0 {
        return None;
    }
    let target = (total * percentile).div_ceil(100);
    let mut cumulative = 0;
    for (bound, bucket) in FETCH_BUCKET_US.iter().zip(buckets) {
        cumulative += bucket.load(Ordering::Relaxed);
        if cumulative >= target {
            return Some(*bound);
        }
    }
    Some(FETCH_BUCKET_US[FETCH_BUCKET_US.len() - 1])
}

pub const BLOCK_SIZE: u64 = 4096;
pub(crate) const MAX_REQUEST: usize = 32 * 1024 * 1024;
const DEFAULT_UPLOAD_CONCURRENCY: NonZeroUsize = NonZeroUsize::new(32).unwrap();

/// Counters are per device. Cold reads count foreground object fetches;
/// readahead fetches count speculative object fetches separately.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeviceStats {
    pub cache_hits: u64,
    pub cold_reads: u64,
    pub readahead_hits: u64,
    pub readahead_fetches: u64,
    pub fetched_chunks: u64,
    pub fetched_bytes: u64,
    pub fetch_p50_us: Option<u64>,
    pub fetch_p95_us: Option<u64>,
    pub dirty_bytes: u64,
    /// Chunk uploads currently waiting for object storage, including existence checks.
    pub uploads_in_flight: u64,
    pub upload_concurrency_limit: usize,
    /// Zero means no bandwidth cap.
    pub upload_bytes_per_second: u64,
    pub tool_priority_uploads: u64,
}

#[derive(Default)]
struct Counters {
    cache_hits: AtomicU64,
    cold_reads: AtomicU64,
    readahead_hits: AtomicU64,
    readahead_fetches: AtomicU64,
    fetched_chunks: AtomicU64,
    fetched_bytes: AtomicU64,
    fetch_histogram: [AtomicU64; 12],
    dirty_bytes: AtomicU64,
    uploads_in_flight: AtomicU64,
    tool_priority_uploads: AtomicU64,
}

// Decrement on cancellation as well as success or failure.
struct UploadGuard<'a>(&'a AtomicU64);

impl Drop for UploadGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

struct Dirty {
    data: fs::File,
    map: fs::File,
    directory: fs::File,
    present: Vec<u8>,
    previous_end: Option<u64>,
    pending: BTreeSet<u64>,
    uploaded: HashMap<u64, ContentHash>,
    // Generations only fence in-flight requests. Reopening reuploads the overlay,
    // so they need not change the persistent dirty-store format.
    generations: HashMap<u64, u64>,
    last_write: HashMap<u64, Instant>,
    published: Manifest,
    boundary: Option<boundary::Boundary>,
    // Hold an exclusive advisory lock for the lifetime of this writer.
    _lock: std::fs::File,
}

// Wake background preparation after the lock has actually been released. It
// does not join the mutex queue ahead of the next foreground request.
struct DirtyGuard<'a> {
    guard: Option<MutexGuard<'a, Dirty>>,
    available: &'a Notify,
}
impl std::ops::Deref for DirtyGuard<'_> {
    type Target = Dirty;
    fn deref(&self) -> &Dirty {
        self.guard.as_deref().unwrap()
    }
}
impl std::ops::DerefMut for DirtyGuard<'_> {
    fn deref_mut(&mut self) -> &mut Dirty {
        self.guard.as_deref_mut().unwrap()
    }
}
impl Drop for DirtyGuard<'_> {
    fn drop(&mut self) {
        self.guard.take();
        self.available.notify_waiters();
    }
}

impl Dirty {
    fn pending_uploads(&self) -> Vec<u64> {
        self.pending
            .iter()
            .filter(|&number| !self.uploaded.contains_key(number))
            .copied()
            .collect()
    }
}

/// A single local writer over an immutable manifest and a persistent overlay.
pub struct VolumeDevice {
    store: ChunkStore,
    manifest: Manifest,
    cache_dir: PathBuf,
    dirty: Mutex<Dirty>,
    dirty_available: Notify,
    // Serialize upload batches, including publication, without excluding writes.
    uploading: Mutex<()>,
    dirty_dir: PathBuf,
    leaves: Mutex<HashMap<usize, Vec<ContentHash>>>,
    prefetched: Mutex<HashSet<ContentHash>>,
    fills: [Mutex<()>; 64],
    readahead: Arc<Mutex<()>>,
    readahead_chunks: u32,
    counters: Counters,
    uploads: Arc<UploadCounters>,
    upload_concurrency: NonZeroUsize,
}

impl VolumeDevice {
    /// Open or resume local dirty data. A directory belongs to exactly one
    /// manifest and volume; callers must enforce the durable writer lease.
    /// # Errors
    /// Returns storage errors, mismatched dirty metadata, or a busy writer lock.
    pub async fn open(
        store: ChunkStore,
        manifest: Manifest,
        cache_dir: impl AsRef<Path>,
        dirty_dir: impl AsRef<Path>,
        readahead_chunks: u32,
    ) -> Result<Arc<Self>> {
        Self::open_with_upload_concurrency(
            store,
            manifest,
            cache_dir,
            dirty_dir,
            readahead_chunks,
            DEFAULT_UPLOAD_CONCURRENCY,
        )
        .await
    }

    /// Open with a maximum number of concurrent chunk uploads. The default in
    /// `open` is 32, limiting prepared chunk data to 8 MiB per batch. Object
    /// storage may allocate additional request buffers.
    /// # Errors
    /// Returns storage errors, mismatched dirty metadata, or a busy writer lock.
    pub async fn open_with_upload_concurrency(
        store: ChunkStore,
        manifest: Manifest,
        cache_dir: impl AsRef<Path>,
        dirty_dir: impl AsRef<Path>,
        readahead_chunks: u32,
        upload_concurrency: NonZeroUsize,
    ) -> Result<Arc<Self>> {
        let cache_dir = cache_dir.as_ref().to_owned();
        let dirty_dir = dirty_dir.as_ref();
        fs::create_dir_all(&cache_dir).await?;
        fs::create_dir_all(dirty_dir).await?;
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dirty_dir.join("lock"))?;
        fs2::FileExt::try_lock_exclusive(&lock)?;
        let identity = encode(manifest.header())?;
        let identity_path = dirty_dir.join("manifest");
        match fs::read(&identity_path).await {
            Ok(existing) if existing != identity => return Err(VolumeError::Corrupt),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::write(&identity_path, identity).await?;
                fs::File::open(&identity_path).await?.sync_all().await?;
            }
            Err(error) => return Err(error.into()),
        }
        let count = usize::try_from(manifest.header().size / BLOCK_SIZE)
            .map_err(|_| VolumeError::InvalidDiskSize)?;
        let mut map = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dirty_dir.join("map"))
            .await?;
        let mut present = Vec::new();
        map.read_to_end(&mut present).await?;
        if present.is_empty() {
            present.resize(count, 0);
            map.write_all(&present).await?;
        }
        if present.len() != count || present.iter().any(|&value| value > 1) {
            return Err(VolumeError::Corrupt);
        }
        let data = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dirty_dir.join("data"))
            .await?;
        if present.contains(&1) && data.metadata().await?.len() != manifest.header().size {
            return Err(VolumeError::Corrupt);
        }
        data.set_len(manifest.header().size).await?;
        let dirty_bytes = present.iter().copied().map(u64::from).sum::<u64>() * BLOCK_SIZE;
        let uploads = Arc::new(UploadCounters::default());
        let store = ChunkStore {
            inner: Arc::new(MeteredStore {
                inner: store.inner,
                counters: uploads.clone(),
            }),
            metadata: store.metadata,
        };
        Ok(Arc::new(Self {
            store,
            uploads,
            manifest: manifest.clone(),
            cache_dir,
            dirty: Mutex::new(Dirty {
                data,
                map,
                directory: fs::File::open(dirty_dir).await?,
                pending: present
                    .chunks(CHUNK_SIZE as usize / 4096)
                    .enumerate()
                    .filter(|(_, blocks)| blocks.contains(&1))
                    .map(|(index, _)| index as u64)
                    .collect(),
                uploaded: HashMap::new(),
                generations: HashMap::new(),
                last_write: HashMap::new(),
                published: manifest,
                boundary: None,
                present,
                previous_end: None,
                _lock: lock,
            }),
            dirty_available: Notify::new(),
            uploading: Mutex::new(()),
            dirty_dir: dirty_dir.to_owned(),
            leaves: Mutex::new(HashMap::new()),
            prefetched: Mutex::new(HashSet::new()),
            fills: std::array::from_fn(|_| Mutex::new(())),
            readahead: Arc::new(Mutex::new(())),
            readahead_chunks: readahead_chunks.min(32),
            upload_concurrency,
            counters: Counters {
                dirty_bytes: AtomicU64::new(dirty_bytes),
                ..Counters::default()
            },
        }))
    }

    #[must_use]
    pub const fn size(&self) -> u64 {
        self.manifest.header().size
    }

    pub(crate) fn protect_uploads(&self, store: swarmy_store::Store) {
        self.store.protect_uploads(store);
    }

    #[must_use]
    pub fn stats(&self) -> DeviceStats {
        let tool_active = crate::priority::tool_active();
        DeviceStats {
            cache_hits: self.counters.cache_hits.load(Ordering::Relaxed),
            cold_reads: self.counters.cold_reads.load(Ordering::Relaxed),
            readahead_hits: self.counters.readahead_hits.load(Ordering::Relaxed),
            readahead_fetches: self.counters.readahead_fetches.load(Ordering::Relaxed),
            fetched_chunks: self.counters.fetched_chunks.load(Ordering::Relaxed),
            fetched_bytes: self.counters.fetched_bytes.load(Ordering::Relaxed),
            fetch_p50_us: fetch_percentile(&self.counters.fetch_histogram, 50),
            fetch_p95_us: fetch_percentile(&self.counters.fetch_histogram, 95),
            dirty_bytes: self.counters.dirty_bytes.load(Ordering::Relaxed),
            uploads_in_flight: self.counters.uploads_in_flight.load(Ordering::Relaxed),
            tool_priority_uploads: self.counters.tool_priority_uploads.load(Ordering::Relaxed),
            upload_concurrency_limit: if tool_active {
                self.upload_concurrency.get().min(4)
            } else {
                self.upload_concurrency.get()
            },
            upload_bytes_per_second: if tool_active {
                crate::priority::TOOL_UPLOAD_BYTES_PER_SECOND
            } else {
                0
            },
        }
    }

    /// Cumulative activity since this device was opened, including background work.
    #[must_use]
    pub fn upload_stats(&self) -> UploadStats {
        self.uploads.snapshot()
    }

    async fn lock_dirty(&self) -> DirtyGuard<'_> {
        let start = Instant::now();
        let guard = self.dirty.lock().await;
        self.uploads.record_lock_wait(start);
        DirtyGuard {
            guard: Some(guard),
            available: &self.dirty_available,
        }
    }

    // Upload preparation must not queue a batch of disk reads ahead of the
    // next NBD request. Foreground readers and writers use the fair mutex queue;
    // backup work retries only when that queue has drained.
    async fn lock_upload_dirty(&self) -> DirtyGuard<'_> {
        let start = Instant::now();
        loop {
            let available = self.dirty_available.notified();
            tokio::pin!(available);
            available.as_mut().enable();
            if let Ok(guard) = self.dirty.try_lock() {
                self.uploads.record_lock_wait(start);
                return DirtyGuard {
                    guard: Some(guard),
                    available: &self.dirty_available,
                };
            }
            tokio::select! {
                () = &mut available => {},
                // Cancellation cleanup and test probes can take the raw mutex.
                () = tokio::time::sleep(Duration::from_millis(1)) => {},
            }
        }
    }

    pub(crate) fn validate(&self, offset: u64, length: usize) -> Result<()> {
        if length > MAX_REQUEST {
            return Err(VolumeError::InvalidRequest);
        }
        self.validate_range(offset, length)
    }

    fn validate_range(&self, offset: u64, length: usize) -> Result<()> {
        if !offset.is_multiple_of(BLOCK_SIZE)
            || !(length as u64).is_multiple_of(BLOCK_SIZE)
            || offset
                .checked_add(length as u64)
                .is_none_or(|end| end > self.size())
        {
            return Err(VolumeError::InvalidRequest);
        }
        Ok(())
    }

    /// Read dirty blocks before consulting the shared immutable chunk cache.
    /// # Errors
    /// Rejects invalid ranges and propagates local or remote storage errors.
    pub async fn read(self: &Arc<Self>, offset: u64, length: usize) -> Result<Vec<u8>> {
        self.validate(offset, length)?;
        let mut dirty = self.lock_dirty().await;
        let mut result = vec![0; length];
        let mut chunk = None;
        for (index, output) in result.chunks_mut(4096).enumerate() {
            let position = offset + index as u64 * BLOCK_SIZE;
            if dirty.present[(position / BLOCK_SIZE) as usize] == 1 {
                dirty.data.seek(std::io::SeekFrom::Start(position)).await?;
                dirty.data.read_exact(output).await?;
            } else {
                let number = position / u64::from(CHUNK_SIZE);
                if chunk
                    .as_ref()
                    .is_none_or(|(previous, _)| *previous != number)
                {
                    chunk = Some((number, self.chunk(number, false).await?));
                }
                if let Some((_, bytes)) = &chunk {
                    let start = usize::try_from(position % u64::from(CHUNK_SIZE))
                        .map_err(|_| VolumeError::InvalidRequest)?;
                    output.copy_from_slice(&bytes[start..start + output.len()]);
                }
            }
        }
        let end = offset + length as u64;
        if dirty.previous_end == Some(offset) && length != 0 {
            self.prefetch(end.div_ceil(u64::from(CHUNK_SIZE)));
        }
        dirty.previous_end = Some(end);
        Ok(result)
    }

    /// Write aligned blocks to local disk without uploading them.
    /// # Errors
    /// Rejects invalid ranges and propagates local storage errors.
    /// # Panics
    /// Panics if a chunk exhausts its u64 generation counter in one attachment.
    pub async fn write(&self, offset: u64, bytes: &[u8]) -> Result<()> {
        self.validate(offset, bytes.len())?;
        let mut dirty = self.lock_dirty().await;
        // Invalidate before writing: a partial failed write must never retain
        // an uploaded hash for bytes that are no longer present locally.
        for number in offset / u64::from(CHUNK_SIZE)
            ..(offset + bytes.len() as u64).div_ceil(u64::from(CHUNK_SIZE))
        {
            Self::preserve_boundary(&mut dirty, number).await;
            let generation = dirty.generations.entry(number).or_default();
            *generation = generation
                .checked_add(1)
                .expect("chunk generation exhausted");
            dirty.last_write.insert(number, Instant::now());
            dirty.pending.insert(number);
            dirty.uploaded.remove(&number);
        }
        dirty.data.seek(std::io::SeekFrom::Start(offset)).await?;
        dirty.data.write_all(bytes).await?;
        // Complete Tokio's buffered write before publishing the dirty bits so
        // delayed filesystem errors are returned for this NBD request.
        dirty.data.flush().await?;
        let first = (offset / BLOCK_SIZE) as usize;
        let count = bytes.len() / 4096;
        let added = dirty.present[first..first + count]
            .iter()
            .map(|&value| usize::from(1 - value))
            .sum::<usize>();
        dirty.present[first..first + count].fill(1);
        self.counters
            .dirty_bytes
            .fetch_add(added as u64 * BLOCK_SIZE, Ordering::Relaxed);
        dirty
            .map
            .seek(std::io::SeekFrom::Start(first as u64))
            .await?;
        dirty.map.write_all(&vec![1; count]).await?;
        dirty.map.flush().await?;
        dirty.previous_end = None;
        Ok(())
    }

    /// Discard is represented by dirty zero blocks so old manifest data cannot
    /// reappear, including after reopening the dirty directory.
    /// # Errors
    /// Rejects invalid ranges and propagates local storage errors.
    pub async fn trim(&self, offset: u64, length: usize) -> Result<()> {
        self.validate_range(offset, length)?;
        // Linux can discard the entire export in one request regardless of
        // the maximum read/write size. Keep memory bounded while zeroing it.
        let zeros = vec![0; length.min(MAX_REQUEST)];
        let mut completed = 0;
        while completed < length {
            let count = (length - completed).min(zeros.len());
            self.write(offset + completed as u64, &zeros[..count])
                .await?;
            completed += count;
        }
        Ok(())
    }

    /// Persist local data before the block map. This does not publish a manifest.
    /// # Errors
    /// Returns local synchronization failures.
    pub async fn flush(&self) -> Result<()> {
        let mut dirty = self.lock_dirty().await;
        dirty.data.flush().await?;
        dirty.data.sync_all().await?;
        dirty.map.flush().await?;
        dirty.map.sync_all().await?;
        dirty.directory.sync_all().await?;
        Ok(())
    }

    /// Includes staged uploads that have not yet been published in a manifest.
    pub async fn has_unpublished_changes(&self) -> bool {
        !self.lock_dirty().await.pending.is_empty()
    }

    /// Upload pending chunks without publishing a snapshot. Remote requests do
    /// not hold the dirty-store lock; overwritten generations remain pending.
    /// # Errors
    /// Returns local read or object storage errors. A later call retries failures.
    pub async fn upload_dirty(&self) -> Result<()> {
        self.upload_settled(Duration::ZERO).await
    }

    pub(crate) async fn upload_settled(&self, debounce: Duration) -> Result<()> {
        let before = self.upload_stats();
        let pending = {
            let dirty = self.lock_dirty().await;
            dirty
                .pending_uploads()
                .into_iter()
                .filter(|number| {
                    dirty
                        .last_write
                        .get(number)
                        .is_none_or(|time| time.elapsed() >= debounce)
                })
                .collect::<Vec<_>>()
        };
        for batch in pending.chunks(self.upload_concurrency.get()) {
            // Release between batches so a flush waits for at most one batch.
            let _uploading = self.uploading.lock().await;
            self.upload_batch(batch, debounce).await?;
        }
        tracing::debug!(stats = ?self.upload_stats().since(before), "background upload complete");
        Ok(())
    }

    async fn upload_batch(&self, numbers: &[u64], debounce: Duration) -> Result<()> {
        // Only this bounded batch owns chunk bytes; the rest remain on disk.
        let mut uploads: FuturesUnordered<_> = numbers
            .iter()
            .map(|&number| self.upload_chunk(number, debounce))
            .collect();
        let mut failure = None;
        while let Some(result) = uploads.next().await {
            if let Err(error) = result {
                failure.get_or_insert(error);
            }
            // Ready object-store futures can otherwise consume an entire batch
            // on one executor turn before a ready NBD request gets to run.
            tokio::task::yield_now().await;
        }
        // Drain even on failure so a retry remembers acknowledged generations.
        failure.map_or(Ok(()), Err)
    }

    async fn admit_upload(&self) -> crate::priority::UploadAdmission {
        let permit = crate::priority::admit().await;
        if permit.throttled {
            self.counters
                .tool_priority_uploads
                .fetch_add(1, Ordering::Relaxed);
        }
        permit
    }

    async fn upload_chunk(&self, number: u64, debounce: Duration) -> Result<()> {
        let _priority = self.admit_upload().await;
        // Fetch immutable baseline data before locking the overlay: a cold GET
        // must not block sandbox writes either.
        let mut bytes = self.chunk(number, false).await?.to_vec();
        let generation = {
            let mut dirty = self.lock_upload_dirty().await;
            if !dirty.pending.contains(&number)
                || dirty.uploaded.contains_key(&number)
                || dirty
                    .last_write
                    .get(&number)
                    .is_some_and(|time| time.elapsed() < debounce)
            {
                return Ok(());
            }
            Self::dirty_chunk(&mut dirty, number, &mut bytes).await?;
            *dirty.generations.get(&number).unwrap_or(&0)
        };
        self.counters
            .uploads_in_flight
            .fetch_add(1, Ordering::Relaxed);
        let _guard = UploadGuard(&self.counters.uploads_in_flight);
        let result = self.store.put_chunk(&bytes).await?;
        let mut dirty = self.lock_upload_dirty().await;
        if dirty.generations.get(&number).copied().unwrap_or(0) == generation {
            dirty.uploaded.insert(number, result.hash);
        }
        Ok(())
    }

    async fn dirty_chunk(dirty: &mut Dirty, number: u64, bytes: &mut [u8]) -> Result<()> {
        let first = usize::try_from(number).map_err(|_| VolumeError::InvalidBlock(number))?
            * (CHUNK_SIZE as usize / 4096);
        let mut index = 0;
        while index < bytes.len() / 4096 {
            if dirty.present[first + index] == 0 {
                index += 1;
                continue;
            }
            let start = index;
            while index < bytes.len() / 4096 && dirty.present[first + index] == 1 {
                index += 1;
            }
            // Copy contiguous dirty blocks in one disk read to keep lock hold
            // time proportional to data size rather than to Tokio dispatches.
            dirty
                .data
                .seek(std::io::SeekFrom::Start(
                    (first + start) as u64 * BLOCK_SIZE,
                ))
                .await?;
            dirty
                .data
                .read_exact(&mut bytes[start * 4096..index * 4096])
                .await?;
        }
        Ok(())
    }

    async fn hash(&self, number: u64) -> Result<ContentHash> {
        let number = usize::try_from(number).map_err(|_| VolumeError::InvalidBlock(number))?;
        let index = number / BLOCKS_PER_LEAF;
        let mut leaves = self.leaves.lock().await;
        if let std::collections::hash_map::Entry::Vacant(entry) = leaves.entry(index) {
            entry.insert(self.manifest.leaf(&*self.store.inner, index).await?);
        }
        Ok(leaves[&index][number % BLOCKS_PER_LEAF])
    }

    async fn chunk(&self, number: u64, prefetch: bool) -> Result<Bytes> {
        let hash = self.hash(number).await?;
        if hash == ContentHash::ZERO {
            return self.store.get_chunk(hash).await;
        }
        // Striped locks prevent duplicate fetches without serializing the whole
        // readahead batch behind one object-storage round trip.
        let _fill = self.fills[usize::from(hash.0[0]) % self.fills.len()]
            .lock()
            .await;
        let path = self.cache_dir.join(hash.to_string());
        match fs::read(&path).await {
            Ok(bytes)
                if bytes.len() == CHUNK_SIZE as usize && crate::content_hash(&bytes)? == hash =>
            {
                if !prefetch {
                    self.counters.cache_hits.fetch_add(1, Ordering::Relaxed);
                    if self.prefetched.lock().await.remove(&hash) {
                        self.counters.readahead_hits.fetch_add(1, Ordering::Relaxed);
                    }
                }
                return Ok(Bytes::from(bytes));
            }
            Ok(_) => {} // A corrupt cache entry is replaceable from object storage.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let counter = if prefetch {
            &self.counters.readahead_fetches
        } else {
            &self.counters.cold_reads
        };
        counter.fetch_add(1, Ordering::Relaxed);
        let start = std::time::Instant::now();
        let bytes = self.store.get_chunk(hash).await?;
        self.counters.fetched_chunks.fetch_add(1, Ordering::Relaxed);
        self.counters
            .fetched_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        let elapsed = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
        let bucket = FETCH_BUCKET_US
            .partition_point(|bound| *bound < elapsed)
            .min(FETCH_BUCKET_US.len() - 1);
        self.counters.fetch_histogram[bucket].fetch_add(1, Ordering::Relaxed);
        // Atomic replacement lets other devices share this directory safely.
        let temporary = self
            .cache_dir
            .join(format!("{}.{}", hash, ulid::Ulid::generate()));
        fs::write(&temporary, &bytes).await?;
        fs::rename(&temporary, &path).await?;
        if prefetch {
            self.prefetched.lock().await.insert(hash);
        }
        Ok(bytes)
    }

    fn prefetch(self: &Arc<Self>, first: u64) {
        if self.readahead_chunks == 0 {
            return;
        }
        let Ok(guard) = Arc::clone(&self.readahead).try_lock_owned() else {
            return;
        };
        let device = Arc::clone(self);
        tokio::spawn(async move {
            let _guard = guard;
            let end = (first + u64::from(device.readahead_chunks))
                .min(device.size() / u64::from(CHUNK_SIZE));
            let results =
                futures::future::join_all((first..end).map(|number| device.chunk(number, true)))
                    .await;
            for result in results {
                if let Err(error) = result {
                    tracing::debug!(%error, "readahead failed; demand reads will retry");
                }
            }
        });
    }
}

#[cfg(test)]
mod upload_tests;

mod boundary;

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn publication_counts_new_deduplicated_zero_and_staged_chunks() {
        use futures::TryStreamExt;
        use object_store::ObjectStore;

        let objects = Arc::new(InMemory::new());
        let dir = tempfile::tempdir().unwrap();
        let device = VolumeDevice::open(
            ChunkStore::new(objects.clone()),
            Manifest::empty(4 * u64::from(CHUNK_SIZE)).unwrap(),
            dir.path().join("cache"),
            dir.path().join("dirty"),
            0,
        )
        .await
        .unwrap();
        // Two distinct chunks, a duplicate, and a zero chunk require five HEAD/PUT
        // calls for data and four for the new manifest's leaf and root.
        for (number, byte) in [7, 8, 7, 0].into_iter().enumerate() {
            device
                .write(
                    number as u64 * u64::from(CHUNK_SIZE),
                    &vec![byte; CHUNK_SIZE as usize],
                )
                .await
                .unwrap();
        }
        device.publish(|_| async { Ok(()) }).await.unwrap();
        let stats = device.upload_stats();
        assert_eq!(stats.chunks_uploaded, 2);
        assert_eq!(stats.referenced_chunk_bytes, 3 * u64::from(CHUNK_SIZE));
        assert_eq!(stats.object_store_requests, 9);
        let stored = objects.list(None).try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(
            stats.bytes_uploaded,
            stored.iter().map(|object| object.size).sum::<u64>()
        );
        assert!(stats.dirty_lock_wait > std::time::Duration::ZERO);
        assert!(stats.object_store_time > std::time::Duration::ZERO);
        device.publish(|_| async { Ok(()) }).await.unwrap();
        assert_eq!(device.upload_stats().object_store_requests, 9);

        device.write(0, &[9; 4096]).await.unwrap();
        device.upload_dirty().await.unwrap();
        let staged = device.upload_stats();
        assert_eq!(staged.chunks_uploaded, 3);
        // A failed commit retains the staged hash; retry must not upload again.
        assert!(
            device
                .publish(|_| async { Err(VolumeError::InvalidRequest) })
                .await
                .is_err()
        );
        assert_eq!(
            device.upload_stats().referenced_chunk_bytes,
            staged.referenced_chunk_bytes
        );
        device.publish(|_| async { Ok(()) }).await.unwrap();
        assert_eq!(
            device.upload_stats().chunks_uploaded,
            staged.chunks_uploaded
        );
        assert_eq!(
            device.upload_stats().referenced_chunk_bytes,
            staged.referenced_chunk_bytes + u64::from(CHUNK_SIZE)
        );
    }

    #[tokio::test]
    async fn snapshots_reuse_leaves_and_retry_rejected_publications() {
        let objects = Arc::new(InMemory::new());
        let store = ChunkStore::new(objects.clone());
        let size = 2 * BLOCKS_PER_LEAF as u64 * u64::from(CHUNK_SIZE);
        let mut builder = ManifestBuilder::new(objects.clone(), Manifest::empty(size).unwrap());
        let untouched = store
            .put_chunk(&vec![3; CHUNK_SIZE as usize])
            .await
            .unwrap()
            .hash;
        builder
            .set_chunk(BLOCKS_PER_LEAF as u64, untouched)
            .unwrap();
        let base = builder.build().await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let device = VolumeDevice::open(
            store.clone(),
            base.clone(),
            dir.path().join("cache"),
            dir.path().join("dirty"),
            0,
        )
        .await
        .unwrap();
        device.write(0, &[7; 4096]).await.unwrap();
        device.upload_dirty().await.unwrap();
        assert_eq!(device.dirty.lock().await.uploaded.len(), 1);
        device.write(4096, &[9; 4096]).await.unwrap();
        assert!(device.dirty.lock().await.uploaded.is_empty());
        assert!(
            device
                .publish(|_| async { Err(VolumeError::InvalidRequest) })
                .await
                .is_err()
        );
        assert_eq!(device.dirty.lock().await.pending.len(), 1);
        let first = device.publish(|_| async { Ok(()) }).await.unwrap();
        assert_eq!(first.leaf_hashes()[1], base.leaf_hashes()[1]);
        assert!(device.dirty.lock().await.pending.is_empty());
        device
            .write(u64::from(CHUNK_SIZE), &[11; 4096])
            .await
            .unwrap();
        let second = device.publish(|_| async { Ok(()) }).await.unwrap();
        let reopened = VolumeDevice::open(
            store,
            second,
            dir.path().join("new-cache"),
            dir.path().join("new-dirty"),
            0,
        )
        .await
        .unwrap();
        assert_eq!(reopened.read(0, 4096).await.unwrap(), vec![7; 4096]);
        assert_eq!(reopened.read(4096, 4096).await.unwrap(), vec![9; 4096]);
        assert_eq!(
            reopened.read(u64::from(CHUNK_SIZE), 4096).await.unwrap(),
            vec![11; 4096]
        );
        device.trim(0, 4096).await.unwrap();
        let trimmed = device.publish(|_| async { Ok(()) }).await.unwrap();
        let hash = trimmed.chunk_hash(&*objects, 0).await.unwrap();
        let chunk = ChunkStore::new(objects).get_chunk(hash).await.unwrap();
        assert_eq!(&chunk[..4096], &[0; 4096]);
        assert_eq!(&chunk[4096..8192], &[9; 4096]);
    }

    #[tokio::test]
    async fn delayed_write_errors_do_not_publish_dirty_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let dirty_dir = dir.path().join("dirty");
        let device = VolumeDevice::open(
            ChunkStore::new(Arc::new(InMemory::new())),
            Manifest::empty(u64::from(CHUNK_SIZE)).unwrap(),
            dir.path().join("cache"),
            &dirty_dir,
            0,
        )
        .await
        .unwrap();
        // A read-only descriptor makes the background filesystem write fail
        // after Tokio has accepted the data into its buffer.
        device.dirty.lock().await.data = fs::File::open(dirty_dir.join("data")).await.unwrap();
        assert!(device.write(0, &[7; 4096]).await.is_err());
        assert_eq!(device.stats().dirty_bytes, 0);
        assert_eq!(device.read(0, 4096).await.unwrap(), vec![0; 4096]);
    }
}

#[cfg(test)]
mod fetch_histogram_tests {
    use super::*;

    #[test]
    fn percentiles_are_bounded_and_empty_histograms_have_no_latency() {
        let buckets: [AtomicU64; 12] = std::array::from_fn(|_| AtomicU64::new(0));
        assert_eq!(fetch_percentile(&buckets, 50), None);
        buckets[0].store(2, Ordering::Relaxed);
        buckets[3].store(1, Ordering::Relaxed);
        assert_eq!(fetch_percentile(&buckets, 50), Some(250));
        assert_eq!(fetch_percentile(&buckets, 95), Some(2_000));
    }
}
