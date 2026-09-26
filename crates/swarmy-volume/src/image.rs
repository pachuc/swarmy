//! Chunk ingestion for locally built images.
//!
//! Recipes and ext4 construction live in `swarmy_image` so the client can
//! build without storage dependencies. This module publishes a built file's
//! chunks and manifest objects before its header is registered.
use std::{fs::File, io::Read, path::Path, sync::Arc};

use object_store::ObjectStore;
use serde::Serialize;
use swarmy_core::{CHUNK_SIZE, ContentHash, ManifestHeader};

use crate::{ChunkStore, Manifest, ManifestBuilder, VolumeError};

pub use swarmy_image::{BuiltImage, Recipe, SandboxRecipe, Source, build_ext4, validate_label};

#[derive(Debug, thiserror::Error)]
pub enum ImageError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Volume(#[from] VolumeError),
    #[error("invalid recipe: {0}")]
    Invalid(String),
    #[error("{program} failed with {status}")]
    Command {
        program: String,
        status: std::process::ExitStatus,
    },
}

type Result<T> = std::result::Result<T, ImageError>;

#[derive(Debug, Serialize)]
pub struct ImageManifest {
    pub header: ManifestHeader,
    pub chunks_total: u64,
    pub chunks_stored: u64,
    pub chunks_uploaded: u64,
}

/// Upload an image's chunks and manifest objects before publishing its header.
/// Zero chunks do not consume object storage. Counts describe data chunks only.
/// For a bucket managed by GC, use `upload_image_protected` instead.
/// # Errors
/// Returns invalid dimensions, local read errors, or object storage failures.
pub async fn upload_image(path: &Path, objects: Arc<dyn ObjectStore>) -> Result<ImageManifest> {
    upload_image_inner(path, objects, None).await
}

/// Upload with reuse guards for a bucket managed by the chunk collector.
/// # Errors
/// Returns image, object storage, and metadata errors.
pub async fn upload_image_protected(
    path: &Path,
    objects: Arc<dyn ObjectStore>,
    metadata: swarmy_store::Store,
) -> Result<ImageManifest> {
    upload_image_inner(path, objects, Some(metadata)).await
}

async fn upload_image_inner(
    path: &Path,
    objects: Arc<dyn ObjectStore>,
    metadata: Option<swarmy_store::Store>,
) -> Result<ImageManifest> {
    let mut file = File::open(path)?;
    let size = file.metadata()?.len();
    let mut builder = ManifestBuilder::new(objects.clone(), Manifest::empty(size)?);
    let chunks = match metadata {
        Some(metadata) => ChunkStore::with_gc_protection(objects, metadata),
        None => ChunkStore::new(objects),
    };
    let chunks_total = size / u64::from(CHUNK_SIZE);
    let mut chunks_stored = 0;
    let mut chunks_uploaded = 0;
    let mut buffer = vec![0; CHUNK_SIZE as usize];
    for block in 0..chunks_total {
        file.read_exact(&mut buffer)?;
        let put = chunks.put_chunk(&buffer).await?;
        if put.hash != ContentHash::ZERO {
            chunks_stored += 1;
            builder.set_chunk(block, put.hash)?;
        }
        chunks_uploaded += u64::from(put.uploaded);
    }
    let manifest = builder.build().await?;
    Ok(ImageManifest {
        header: manifest.header().clone(),
        chunks_total,
        chunks_stored,
        chunks_uploaded,
    })
}
