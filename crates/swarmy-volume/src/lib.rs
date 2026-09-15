//! Content-addressed disk chunks and immutable two-level manifests.
mod manifest;

use std::sync::Arc;

use bytes::Bytes;
use object_store::{ObjectStore, PutMode, path::Path};
use swarmy_core::{CHUNK_SIZE, ContentHash, EncodingError, decode, encode};

pub use manifest::{BLOCKS_PER_LEAF, Manifest, ManifestBuilder};

#[derive(Debug, thiserror::Error)]
pub enum VolumeError {
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
    #[error("nonzero content hashed to the reserved zero sentinel")]
    ReservedHash,
}

pub type Result<T> = std::result::Result<T, VolumeError>;

#[derive(Clone)]
pub struct ChunkStore {
    inner: Arc<dyn ObjectStore>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PutChunkResult {
    pub hash: ContentHash,
    pub uploaded: bool,
}

impl ChunkStore {
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
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
        // Encoding is deferred until HEAD confirms an upload is needed.
        if exists(&*self.inner, &path).await? {
            return Ok(PutChunkResult {
                hash,
                uploaded: false,
            });
        }
        let uploaded = create(&*self.inner, &path, encode(bytes)?).await?;
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
        let decoded: Vec<u8> = decode(&bytes)?;
        if decoded.len() != CHUNK_SIZE as usize || content_hash(&decoded)? != hash {
            return Err(VolumeError::Corrupt);
        }
        Ok(decoded.into())
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
