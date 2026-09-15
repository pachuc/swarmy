use std::{collections::BTreeMap, sync::Arc};

use object_store::{ObjectStore, path::Path};
use serde::{Deserialize, Serialize};
use swarmy_core::{CHUNK_SIZE, ContentHash, ManifestHeader, decode, encode};

use crate::{Result, VolumeError, content_hash, create, exists};

pub const BLOCKS_PER_LEAF: usize = 4096;

// Distinct tags prevent a root from being interpreted as a leaf.
#[derive(Serialize, Deserialize)]
enum ManifestObject {
    Leaf(Vec<ContentHash>),
    Root(Vec<ContentHash>),
}

/// A loaded root. Leaves are fetched only when read or changed.
#[derive(Clone, Debug)]
pub struct Manifest {
    header: ManifestHeader,
    leaves: Vec<ContentHash>,
}

impl Manifest {
    /// Construct an empty disk without storing any objects.
    /// # Errors
    /// Rejects unsupported disk sizes.
    pub fn empty(size: u64) -> Result<Self> {
        let header = ManifestHeader {
            size,
            chunk_size: CHUNK_SIZE,
            root_hash: ContentHash::ZERO,
        };
        let count = block_count(&header)?.div_ceil(BLOCKS_PER_LEAF);
        Ok(Self {
            header,
            leaves: vec![ContentHash::ZERO; count],
        })
    }

    /// Load and validate a root using its durable header.
    /// # Errors
    /// Rejects invalid headers, corrupt objects, and storage failures.
    pub async fn load(store: &dyn ObjectStore, header: ManifestHeader) -> Result<Self> {
        let count = block_count(&header)?.div_ceil(BLOCKS_PER_LEAF);
        let leaves = if header.root_hash == ContentHash::ZERO {
            vec![ContentHash::ZERO; count]
        } else {
            let ManifestObject::Root(leaves) = read_object(store, header.root_hash).await? else {
                return Err(VolumeError::Corrupt);
            };
            if leaves.len() != count || leaves.iter().all(|&hash| hash == ContentHash::ZERO) {
                return Err(VolumeError::Corrupt);
            }
            leaves
        };
        Ok(Self { header, leaves })
    }

    #[must_use]
    pub const fn header(&self) -> &ManifestHeader {
        &self.header
    }

    #[must_use]
    pub fn leaf_hashes(&self) -> &[ContentHash] {
        &self.leaves
    }

    /// Resolve a block without fetching any chunk data.
    /// # Errors
    /// Rejects invalid indices, corrupt leaves, and storage failures.
    pub async fn chunk_hash(&self, store: &dyn ObjectStore, block: u64) -> Result<ContentHash> {
        let index = self.index(block)?;
        Ok(self.leaf(store, index / BLOCKS_PER_LEAF).await?[index % BLOCKS_PER_LEAF])
    }

    fn index(&self, block: u64) -> Result<usize> {
        if block >= self.header.size / u64::from(CHUNK_SIZE) {
            return Err(VolumeError::InvalidBlock(block));
        }
        usize::try_from(block).map_err(|_| VolumeError::InvalidBlock(block))
    }

    pub(crate) async fn leaf(
        &self,
        store: &dyn ObjectStore,
        index: usize,
    ) -> Result<Vec<ContentHash>> {
        let hash = self.leaves[index];
        if hash == ContentHash::ZERO {
            return Ok(vec![ContentHash::ZERO; BLOCKS_PER_LEAF]);
        }
        let ManifestObject::Leaf(chunks) = read_object(store, hash).await? else {
            return Err(VolumeError::Corrupt);
        };
        let used = (block_count(&self.header)? - index * BLOCKS_PER_LEAF).min(BLOCKS_PER_LEAF);
        if chunks.len() != BLOCKS_PER_LEAF
            || chunks.iter().all(|&hash| hash == ContentHash::ZERO)
            || chunks[used..].iter().any(|&hash| hash != ContentHash::ZERO)
        {
            return Err(VolumeError::Corrupt);
        }
        Ok(chunks)
    }
}

/// Batches updates so each touched leaf is fetched and encoded at most once.
/// Repeated block updates use the last supplied hash.
pub struct ManifestBuilder {
    store: Arc<dyn ObjectStore>,
    previous: Manifest,
    changes: BTreeMap<usize, BTreeMap<usize, ContentHash>>,
}

impl ManifestBuilder {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, previous: Manifest) -> Self {
        Self {
            store,
            previous,
            changes: BTreeMap::new(),
        }
    }

    /// Queue a chunk hash at a disk block index.
    /// # Errors
    /// Rejects indices outside the disk.
    pub fn set_chunk(&mut self, block: u64, hash: ContentHash) -> Result<()> {
        let index = self.previous.index(block)?;
        self.changes
            .entry(index / BLOCKS_PER_LEAF)
            .or_default()
            .insert(index % BLOCKS_PER_LEAF, hash);
        Ok(())
    }

    /// Persist changed leaves followed by the root. Publish the returned header
    /// under a fresh `ManifestId` in `FoundationDB` only after this succeeds.
    /// # Errors
    /// Returns decoding, integrity, and object storage errors.
    pub async fn build(self) -> Result<Manifest> {
        let mut leaves = self.previous.leaves.clone();
        for (index, changes) in self.changes {
            let mut chunks = self.previous.leaf(&*self.store, index).await?;
            let mut changed = false;
            for (block, hash) in changes {
                changed |= chunks[block] != hash;
                chunks[block] = hash;
            }
            if !changed {
                continue;
            }
            leaves[index] = if chunks.iter().all(|&hash| hash == ContentHash::ZERO) {
                ContentHash::ZERO
            } else {
                write_object(&*self.store, &ManifestObject::Leaf(chunks)).await?
            };
        }
        if leaves == self.previous.leaves {
            return Ok(self.previous);
        }
        let root_hash = if leaves.iter().all(|&hash| hash == ContentHash::ZERO) {
            ContentHash::ZERO
        } else {
            write_object(&*self.store, &ManifestObject::Root(leaves.clone())).await?
        };
        Ok(Manifest {
            header: ManifestHeader {
                root_hash,
                ..self.previous.header
            },
            leaves,
        })
    }
}

fn block_count(header: &ManifestHeader) -> Result<usize> {
    if header.chunk_size != CHUNK_SIZE
        || header.size == 0
        || !header.size.is_multiple_of(u64::from(CHUNK_SIZE))
    {
        return Err(VolumeError::InvalidDiskSize);
    }
    usize::try_from(header.size / u64::from(CHUNK_SIZE)).map_err(|_| VolumeError::InvalidDiskSize)
}

fn object_path(hash: ContentHash) -> Path {
    Path::from(format!("manifests/{hash}"))
}

async fn read_object(store: &dyn ObjectStore, hash: ContentHash) -> Result<ManifestObject> {
    let bytes = store.get(&object_path(hash)).await?.bytes().await?;
    if content_hash(&bytes)? != hash {
        return Err(VolumeError::Corrupt);
    }
    Ok(decode(&bytes)?)
}

async fn write_object(store: &dyn ObjectStore, object: &ManifestObject) -> Result<ContentHash> {
    let bytes = encode(object)?;
    let hash = content_hash(&bytes)?;
    let path = object_path(hash);
    if !exists(store, &path).await? {
        create(store, &path, bytes).await?;
    }
    Ok(hash)
}
