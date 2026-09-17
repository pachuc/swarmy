//! Content-addressed disk chunks and immutable two-level manifests.
mod device;
pub mod gc;
mod metrics;
pub mod priority;
pub use metrics::UploadStats;
mod snapshot;
pub use snapshot::SnapshotLoop;
mod flush;
pub use flush::{BackgroundUploader, FlushResult, VolumeWriter};
pub mod image;
#[cfg(target_os = "linux")]
pub mod kernel;
mod manifest;
pub mod nbd;
pub mod server;
pub use device::{BLOCK_SIZE, DeviceStats, VolumeDevice};

use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use object_store::{ObjectStore, PutMode, path::Path};
use swarmy_core::{CHUNK_SIZE, ContentHash, EncodingError};

pub use manifest::{BLOCKS_PER_LEAF, Manifest, ManifestBuilder};

#[derive(Debug, thiserror::Error)]
pub enum VolumeError {
    #[error("durable volume metadata: {0}")]
    Store(#[from] swarmy_store::StoreError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("request is unaligned, too large, or outside the disk")]
    InvalidRequest,
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    Encoding(#[from] EncodingError),
    #[error("chunk must contain exactly 256 KiB")]
    InvalidChunkSize,
    #[error("disk must be a positive multiple of 256 KiB and fit the address space")]
    InvalidDiskSize,
    #[error("block index {0} is outside the disk")]
    InvalidBlock(u64),
    #[error("stored content does not match its hash or manifest shape")]
    Corrupt,
    #[error("invalid garbage collection policy or reference allocation failed")]
    InvalidGcPolicy,
    #[error("nonzero content hashed to the reserved zero sentinel")]
    ReservedHash,
}

pub type Result<T> = std::result::Result<T, VolumeError>;

#[derive(Clone)]
pub struct ChunkStore {
    inner: Arc<dyn ObjectStore>,
    metadata: Arc<OnceLock<swarmy_store::Store>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PutChunkResult {
    pub hash: ContentHash,
    pub uploaded: bool,
}

impl ChunkStore {
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            metadata: Arc::new(OnceLock::new()),
        }
    }

    /// Bind uploads to the metadata namespace whose collector owns this bucket.
    #[must_use]
    pub fn with_gc_protection(inner: Arc<dyn ObjectStore>, metadata: swarmy_store::Store) -> Self {
        let chunks = Self::new(inner);
        chunks.protect_uploads(metadata);
        chunks
    }

    pub(crate) fn protect_uploads(&self, metadata: swarmy_store::Store) {
        // An attachment binds once, before its uploader starts. Clones share it.
        let _ = self.metadata.set(metadata);
    }

    async fn protect_reuse(&self, hash: ContentHash) -> Result<()> {
        if let Some(metadata) = self.metadata.get() {
            loop {
                match metadata.protect_reused_chunk(hash).await {
                    Err(swarmy_store::StoreError::LeaseMismatch) => {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    result => {
                        result?;
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    /// Hash one block and check for its object before uploading a versioned payload.
    /// Conditional creation also prevents concurrent calls from overwriting it.
    /// # Errors
    /// Rejects incorrect sizes, reserved hashes, and storage failures.
    pub async fn put_chunk(&self, bytes: &[u8]) -> Result<PutChunkResult> {
        if bytes.len() != CHUNK_SIZE as usize {
            return Err(VolumeError::InvalidChunkSize);
        }
        if bytes.iter().all(|&byte| byte == 0) {
            return Ok(PutChunkResult {
                hash: ContentHash::ZERO,
                uploaded: false,
            });
        }
        let hash = content_hash(bytes)?;
        let path = chunk_path(hash);
        if exists(&*self.inner, &path).await? {
            self.protect_reuse(hash).await?;
            // A collector may have won before the reuse guard. Recheck after
            // acquiring it so a completed deletion causes a fresh upload.
            if self.metadata.get().is_none() || exists(&*self.inner, &path).await? {
                return Ok(PutChunkResult {
                    hash,
                    uploaded: false,
                });
            }
        }
        // Chunks are stored as raw block bytes: the content hash in the object
        // name already verifies them, and raw objects can be read by range and
        // by other tools without knowing our encoding.
        let uploaded = create(&*self.inner, &path, bytes.to_vec()).await?;
        Ok(PutChunkResult { hash, uploaded })
    }

    /// Read and verify one chunk, synthesizing implicit zeros without any I/O.
    /// # Errors
    /// Returns missing-object, decoding, size, or content-integrity errors.
    pub async fn get_chunk(&self, hash: ContentHash) -> Result<Bytes> {
        if hash == ContentHash::ZERO {
            return Ok(Bytes::from(vec![0; CHUNK_SIZE as usize]));
        }
        let bytes = self.inner.get(&chunk_path(hash)).await?.bytes().await?;
        if bytes.len() != CHUNK_SIZE as usize || content_hash(&bytes)? != hash {
            return Err(VolumeError::Corrupt);
        }
        Ok(bytes)
    }
}

fn chunk_path(hash: ContentHash) -> Path {
    let hex = hash.to_string();
    Path::from(format!("chunks/{}/{hex}", &hex[..2]))
}

fn content_hash(bytes: &[u8]) -> Result<ContentHash> {
    let hash = ContentHash(*blake3::hash(bytes).as_bytes());
    if hash == ContentHash::ZERO {
        return Err(VolumeError::ReservedHash);
    }
    Ok(hash)
}

async fn exists(store: &dyn ObjectStore, path: &Path) -> Result<bool> {
    match store.head(path).await {
        Ok(_) => Ok(true),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

async fn create(store: &dyn ObjectStore, path: &Path, bytes: Vec<u8>) -> Result<bool> {
    match store
        .put_opts(path, bytes.into(), PutMode::Create.into())
        .await
    {
        Ok(_) => Ok(true),
        Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
        Err(error) => Err(error.into()),
    }
}
