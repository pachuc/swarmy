use super::{Dirty, UploadGuard, VolumeDevice};
use crate::{Manifest, ManifestBuilder, Result, VolumeError};
use futures::{StreamExt, stream::FuturesUnordered};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use swarmy_core::{CHUNK_SIZE, ContentHash};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
};

const MEMORY_BUDGET: usize = 8 * 1024 * 1024;
const BLOCKS: usize = CHUNK_SIZE as usize / 4096;

pub(super) struct Boundary {
    active: Arc<AtomicBool>,
    failed: bool,
    chunks: HashMap<u64, Version>,
    pub(super) memory: usize,
    pub(super) spill: fs::File,
}

struct Version {
    generation: u64,
    hash: Option<ContentHash>,
    prepared: bool,
    saved: Option<Saved>,
}

struct Saved {
    present: Vec<u8>,
    // None means the bytes are in the sparse spill file at the chunk offset.
    bytes: Option<Vec<u8>>,
}

struct BoundaryGuard<'a> {
    active: Arc<AtomicBool>,
    dirty: &'a Mutex<Dirty>,
}
impl Drop for BoundaryGuard<'_> {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
        // Usually cancellation owns no dirty lock. If a write is copying, it
        // releases its cancelled boundary on the next write instead.
        if let Ok(mut dirty) = self.dirty.try_lock() {
            dirty.boundary = None;
        }
    }
}

impl VolumeDevice {
    pub(super) async fn preserve_boundary(dirty: &mut Dirty, number: u64) {
        if let Err(error) = Self::copy_boundary(dirty, number).await {
            // Backup is best effort: a full spill disk abandons this snapshot,
            // while the foreground write still gets its normal storage result.
            if let Some(boundary) = &mut dirty.boundary {
                boundary.failed = true;
            }
            tracing::warn!(%error, "abandoning snapshot after boundary copy failed");
        }
    }

    async fn copy_boundary(dirty: &mut Dirty, number: u64) -> Result<()> {
        if dirty
            .boundary
            .as_ref()
            .is_some_and(|b| !b.active.load(Ordering::Acquire))
        {
            dirty.boundary = None;
        }
        let preserve = dirty.boundary.as_ref().is_some_and(|b| {
            !b.failed
                && b.chunks
                    .get(&number)
                    .is_some_and(|v| v.hash.is_none() && !v.prepared && v.saved.is_none())
        });
        if !preserve {
            return Ok(());
        }
        // Only copy local overlay blocks. Clean blocks remain in the immutable
        // baseline, so preserving an overwrite never waits for a remote GET.
        let mut bytes = vec![0; CHUNK_SIZE as usize];
        Self::dirty_chunk(dirty, number, &mut bytes).await?;
        let first =
            usize::try_from(number).map_err(|_| VolumeError::InvalidBlock(number))? * BLOCKS;
        let present = dirty.present[first..first + BLOCKS].to_vec();
        let boundary = dirty.boundary.as_mut().unwrap();
        let bytes = if boundary.memory + bytes.len() <= MEMORY_BUDGET {
            boundary.memory += bytes.len();
            Some(bytes)
        } else {
            boundary
                .spill
                .seek(std::io::SeekFrom::Start(number * u64::from(CHUNK_SIZE)))
                .await?;
            boundary.spill.write_all(&bytes).await?;
            boundary.spill.flush().await?;
            None
        };
        boundary.chunks.get_mut(&number).unwrap().saved = Some(Saved { present, bytes });
        Ok(())
    }

    /// Capture a point in the block stream without excluding subsequent writes.
    /// A failed or cancelled publication leaves live generations pending.
    pub(crate) async fn publish<F, Fut>(&self, commit: F) -> Result<Manifest>
    where
        F: FnOnce(swarmy_core::ManifestHeader) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let _uploading = self.uploading.lock().await;
        let spill = fs::File::from_std(tempfile::tempfile_in(&self.dirty_dir)?);
        let active = Arc::new(AtomicBool::new(true));
        let _boundary = BoundaryGuard {
            active: active.clone(),
            dirty: &self.dirty,
        };
        let (base, pending) = {
            let mut dirty = self.lock_dirty().await;
            let chunks = dirty
                .pending
                .iter()
                .map(|&number| {
                    (
                        number,
                        Version {
                            generation: dirty.generations.get(&number).copied().unwrap_or(0),
                            hash: dirty.uploaded.get(&number).copied(),
                            prepared: false,
                            saved: None,
                        },
                    )
                })
                .collect();
            let pending = dirty.pending_uploads();
            dirty.boundary = Some(Boundary {
                active,
                failed: false,
                chunks,
                memory: 0,
                spill,
            });
            (dirty.published.clone(), pending)
        };
        let result = self.publish_boundary(base, &pending, commit).await;
        self.lock_dirty().await.boundary = None;
        result
    }

    async fn publish_boundary<F, Fut>(
        &self,
        base: Manifest,
        pending: &[u64],
        commit: F,
    ) -> Result<Manifest>
    where
        F: FnOnce(swarmy_core::ManifestHeader) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        for batch in pending.chunks(self.upload_concurrency.get()) {
            let mut uploads: FuturesUnordered<_> =
                batch.iter().map(|&n| self.upload_boundary(n)).collect();
            let mut failure = None;
            while let Some(result) = uploads.next().await {
                if let Err(error) = result {
                    failure.get_or_insert(error);
                }
                tokio::task::yield_now().await;
            }
            if let Some(error) = failure {
                return Err(error);
            }
        }
        let mut builder = ManifestBuilder::new(self.store.inner.clone(), base);
        {
            let dirty = self.lock_dirty().await;
            if dirty.boundary.as_ref().unwrap().failed {
                return Err(std::io::Error::other("snapshot boundary copy failed").into());
            }
            for (&number, version) in &dirty.boundary.as_ref().unwrap().chunks {
                builder.set_chunk(number, version.hash.ok_or(VolumeError::Corrupt)?)?;
            }
        }
        let manifest = builder.build().await?;
        commit(manifest.header().clone()).await?;
        let mut dirty = self.lock_dirty().await;
        let boundary = dirty.boundary.take().unwrap();
        self.uploads.record_referenced(
            boundary
                .chunks
                .values()
                .filter(|v| v.hash != Some(ContentHash::ZERO))
                .count() as u64
                * u64::from(CHUNK_SIZE),
        );
        for (number, version) in boundary.chunks {
            if dirty.generations.get(&number).copied().unwrap_or(0) == version.generation {
                dirty.pending.remove(&number);
                dirty.uploaded.remove(&number);
            }
        }
        dirty.published = manifest.clone();
        Ok(manifest)
    }

    async fn upload_boundary(&self, number: u64) -> Result<()> {
        let _priority = self.admit_upload().await;
        let mut bytes = self.chunk(number, false).await?.to_vec();
        let generation = {
            let mut dirty = self.lock_upload_dirty().await;
            let boundary = dirty.boundary.as_mut().unwrap();
            let version = boundary.chunks.get_mut(&number).unwrap();
            let generation = version.generation;
            if let Some(saved) = version.saved.take() {
                let overlay = if let Some(bytes) = saved.bytes {
                    boundary.memory -= bytes.len();
                    bytes
                } else {
                    let mut bytes = vec![0; CHUNK_SIZE as usize];
                    boundary
                        .spill
                        .seek(std::io::SeekFrom::Start(number * u64::from(CHUNK_SIZE)))
                        .await?;
                    boundary.spill.read_exact(&mut bytes).await?;
                    bytes
                };
                for (index, present) in saved.present.into_iter().enumerate() {
                    if present == 1 {
                        bytes[index * 4096..(index + 1) * 4096]
                            .copy_from_slice(&overlay[index * 4096..(index + 1) * 4096]);
                    }
                }
            } else {
                Self::dirty_chunk(&mut dirty, number, &mut bytes).await?;
            }
            dirty
                .boundary
                .as_mut()
                .unwrap()
                .chunks
                .get_mut(&number)
                .unwrap()
                .prepared = true;
            generation
        };
        self.counters
            .uploads_in_flight
            .fetch_add(1, Ordering::Relaxed);
        let _guard = UploadGuard(&self.counters.uploads_in_flight);
        let result = self.store.put_chunk(&bytes).await?;
        let mut dirty = self.lock_upload_dirty().await;
        dirty
            .boundary
            .as_mut()
            .unwrap()
            .chunks
            .get_mut(&number)
            .unwrap()
            .hash = Some(result.hash);
        if dirty.generations.get(&number).copied().unwrap_or(0) == generation {
            dirty.uploaded.insert(number, result.hash);
        }
        Ok(())
    }
}
