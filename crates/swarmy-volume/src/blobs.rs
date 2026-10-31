//! Binary tool content (screenshots) written next to chunks.
use object_store::{ObjectStore, path::Path};

/// Store tool output bytes under their content address and return the key.
/// Repeating the call with the same bytes is safe.
/// # Errors
/// Returns the object storage error.
pub async fn put_tool_blob(
    objects: &dyn ObjectStore,
    bytes: Vec<u8>,
) -> Result<String, object_store::Error> {
    let key = swarmy_core::blob_key(&bytes);
    objects.put(&Path::from(key.as_str()), bytes.into()).await?;
    Ok(key)
}
