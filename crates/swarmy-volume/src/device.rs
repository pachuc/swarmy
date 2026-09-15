use std::{
    collections::{BTreeSet, HashMap, HashSet},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use futures::{StreamExt, stream::FuturesUnordered};
use swarmy_core::{CHUNK_SIZE, ContentHash, encode};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
};

use crate::{BLOCKS_PER_LEAF, ChunkStore, Manifest, ManifestBuilder, Result, VolumeError};

pub const BLOCK_SIZE: u64 = 4096;
pub(crate) const MAX_REQUEST: usize = 32 * 1024 * 1024;
const DEFAULT_UPLOAD_CONCURRENCY: NonZeroUsize = NonZeroUsize::new(32).unwrap();

/// Counters are per device. Cold reads count foreground object fetches;
/// readahead fetches count speculative object fetches separately.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DeviceStats {
    pub cache_hits: u64,
    pub cold_reads: u64,
    pub readahead_hits: u64,
    pub readahead_fetches: u64,
    pub dirty_bytes: u64,
    /// Chunk uploads currently waiting for object storage, including existence checks.
    pub uploads_in_flight: u64,
}

#[derive(Default)]
struct Counters {
    cache_hits: AtomicU64,
    cold_reads: AtomicU64,
    readahead_hits: AtomicU64,
    readahead_fetches: AtomicU64,
    dirty_bytes: AtomicU64,
    uploads_in_flight: AtomicU64,
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
    published: Manifest,
    // Hold an exclusive advisory lock for the lifetime of this writer.
    _lock: std::fs::File,
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
    leaves: Mutex<HashMap<usize, Vec<ContentHash>>>,
    prefetched: Mutex<HashSet<ContentHash>>,
    fills: [Mutex<()>; 64],
    readahead: Arc<Mutex<()>>,
    readahead_chunks: u32,
    counters: Counters,
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
        Ok(Arc::new(Self {
            store,
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
                published: manifest,
                present,
                previous_end: None,
                _lock: lock,
            }),
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

    #[must_use]
    pub fn stats(&self) -> DeviceStats {
        DeviceStats {
            cache_hits: self.counters.cache_hits.load(Ordering::Relaxed),
            cold_reads: self.counters.cold_reads.load(Ordering::Relaxed),
            readahead_hits: self.counters.readahead_hits.load(Ordering::Relaxed),
            readahead_fetches: self.counters.readahead_fetches.load(Ordering::Relaxed),
            dirty_bytes: self.counters.dirty_bytes.load(Ordering::Relaxed),
            uploads_in_flight: self.counters.uploads_in_flight.load(Ordering::Relaxed),
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
        let mut dirty = self.dirty.lock().await;
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
    pub async fn write(&self, offset: u64, bytes: &[u8]) -> Result<()> {
        self.validate(offset, bytes.len())?;
        let mut dirty = self.dirty.lock().await;
        // Invalidate before writing: a partial failed write must never retain
        // an uploaded hash for bytes that are no longer present locally.
        for number in offset / u64::from(CHUNK_SIZE)
            ..(offset + bytes.len() as u64).div_ceil(u64::from(CHUNK_SIZE))
        {
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
        let mut dirty = self.dirty.lock().await;
        dirty.data.flush().await?;
        dirty.data.sync_all().await?;
        dirty.map.flush().await?;
        dirty.map.sync_all().await?;
        dirty.directory.sync_all().await?;
        Ok(())
    }

    /// Upload pending chunks without publishing a snapshot. Writes can proceed
    /// between batches; a subsequent write invalidates that chunk's uploaded hash.
    /// # Errors
    /// Returns local read or object storage errors. A later call retries failures.
    pub async fn upload_dirty(&self) -> Result<()> {
        let pending = self.dirty.lock().await.pending_uploads();
        for batch in pending.chunks(self.upload_concurrency.get()) {
            let mut dirty = self.dirty.lock().await;
            self.upload_batch(&mut dirty, batch).await?;
        }
        Ok(())
    }

    async fn upload_batch(&self, dirty: &mut Dirty, numbers: &[u64]) -> Result<()> {
        let mut prepared = Vec::with_capacity(numbers.len());
        for &number in numbers {
            if dirty.pending.contains(&number) && !dirty.uploaded.contains_key(&number) {
                prepared.push((number, self.dirty_chunk(dirty, number).await?));
            }
        }
        // Read through the shared file cursor before starting the requests.
        // Only this bounded batch owns chunk bytes; the rest remain on disk.
        let mut uploads: FuturesUnordered<_> = prepared
            .into_iter()
            .map(|(number, bytes)| async move {
                self.counters
                    .uploads_in_flight
                    .fetch_add(1, Ordering::Relaxed);
                let _guard = UploadGuard(&self.counters.uploads_in_flight);
                (number, self.store.put_chunk(&bytes).await)
            })
            .collect();
        let mut failure = None;
        while let Some((number, result)) = uploads.next().await {
            match result {
                Ok(result) => {
                    dirty.uploaded.insert(number, result.hash);
                }
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
        }
        // Drain the batch even on failure so acknowledged chunks are remembered
        // and a retry only uploads the remaining versions.
        failure.map_or(Ok(()), Err)
    }

    async fn dirty_chunk(&self, dirty: &mut Dirty, number: u64) -> Result<Vec<u8>> {
        let mut bytes = self.chunk(number, false).await?.to_vec();
        let first = usize::try_from(number).map_err(|_| VolumeError::InvalidBlock(number))?
            * (CHUNK_SIZE as usize / 4096);
        for (index, block) in bytes.chunks_mut(4096).enumerate() {
            if dirty.present[first + index] == 1 {
                dirty
                    .data
                    .seek(std::io::SeekFrom::Start(
                        (first + index) as u64 * BLOCK_SIZE,
                    ))
                    .await?;
                dirty.data.read_exact(block).await?;
            }
        }
        Ok(bytes)
    }

    /// Hold writes across the snapshot and publication so journal fallback is a
    /// single point in the block stream. On failure all changes remain pending.
    pub(crate) async fn publish<F, Fut>(&self, commit: F) -> Result<Manifest>
    where
        F: FnOnce(swarmy_core::ManifestHeader) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let mut dirty = self.dirty.lock().await;
        let pending = dirty.pending_uploads();
        for batch in pending.chunks(self.upload_concurrency.get()) {
            self.upload_batch(&mut dirty, batch).await?;
        }
        let mut builder = ManifestBuilder::new(self.store.inner.clone(), dirty.published.clone());
        for &number in &dirty.pending {
            builder.set_chunk(number, dirty.uploaded[&number])?;
        }
        let manifest = builder.build().await?;
        commit(manifest.header().clone()).await?;
        dirty.published = manifest.clone();
        dirty.pending.clear();
        dirty.uploaded.clear();
        // The original manifest remains the read baseline. Retain the overlay
        // until detach; new attachments start from the committed manifest.
        Ok(manifest)
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
        let bytes = self.store.get_chunk(hash).await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

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
