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

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    #[tokio::test]
    async fn put_tool_blob_writes_under_the_content_address() {
        let objects = object_store::memory::InMemory::new();
        let bytes = b"screenshot".to_vec();
        let key = put_tool_blob(&objects, bytes.clone()).await.unwrap();
        assert_eq!(key, swarmy_core::blob_key(&bytes));
        assert_eq!(
            objects
                .get(&Path::from(key.as_str()))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            bytes
        );
        assert_eq!(put_tool_blob(&objects, bytes).await.unwrap(), key);
    }
}
