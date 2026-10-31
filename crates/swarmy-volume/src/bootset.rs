use crate::{Result, VolumeError, create};
use object_store::{ObjectStore, path::Path};
use swarmy_core::{BootSet, MAX_BOOT_SET_CHUNKS, boot_set_key, is_boot_set_key};

/// Write a nonempty boot set under its content address, creating it only once.
/// # Errors
/// Encoding, empty sets, or object storage failures.
pub async fn put_boot_set(objects: &dyn ObjectStore, set: &BootSet) -> Result<String> {
    if set.chunks.is_empty() || set.chunks.len() > MAX_BOOT_SET_CHUNKS {
        return Err(VolumeError::InvalidRequest);
    }
    let bytes = swarmy_core::encode(set)?;
    let key = boot_set_key(&bytes);
    create(objects, &Path::from(key.as_str()), bytes).await?;
    Ok(key)
}

/// Read and verify a content-addressed boot set.
/// # Errors
/// Invalid key, corrupt content, or object storage failures.
pub async fn get_boot_set(objects: &dyn ObjectStore, key: &str) -> Result<BootSet> {
    if !is_boot_set_key(key) {
        return Err(VolumeError::InvalidRequest);
    }
    let bytes = objects.get(&Path::from(key)).await?.bytes().await?;
    if boot_set_key(&bytes) != key {
        return Err(VolumeError::Corrupt);
    }
    let set: BootSet = swarmy_core::decode(&bytes).map_err(|_| VolumeError::Corrupt)?;
    if set.chunks.len() > MAX_BOOT_SET_CHUNKS {
        return Err(VolumeError::Corrupt);
    }
    Ok(set)
}
