//! Bulk payload storage. Uploads happen before `FoundationDB` transactions start.
use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use object_store::{ObjectStore, aws::AmazonS3Builder, path::Path};
use tokio::sync::RwLock;

#[derive(Debug, thiserror::Error)]
pub enum BlobError {
    #[error("blob not found: {0}")]
    Missing(String),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error("missing or invalid environment variable {0}")]
    Environment(&'static str),
}

/// Immutable payloads are addressed by key. Repeating a put must be safe.
#[async_trait]
pub trait BlobStore: Send + Sync {
    /// # Errors
    /// Returns an error if the payload cannot be stored.
    async fn put(&self, key: &str, value: Bytes) -> Result<(), BlobError>;
    /// # Errors
    /// Returns an error if the key is absent or cannot be read.
    async fn get(&self, key: &str) -> Result<Bytes, BlobError>;
}

#[derive(Default)]
pub struct MemoryBlobStore {
    values: RwLock<HashMap<String, Bytes>>,
}

#[async_trait]
impl BlobStore for MemoryBlobStore {
    async fn put(&self, key: &str, value: Bytes) -> Result<(), BlobError> {
        self.values.write().await.insert(key.to_owned(), value);
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes, BlobError> {
        self.values
            .read()
            .await
            .get(key)
            .cloned()
            .ok_or_else(|| BlobError::Missing(key.to_owned()))
    }
}

pub struct ObjectBlobStore {
    inner: Arc<dyn ObjectStore>,
}

impl ObjectBlobStore {
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }

    /// Read the five `SWARMY_S3_*` settings used by the dev stack.
    /// # Errors
    /// Returns an error for missing settings or an invalid S3 configuration.
    pub fn from_env() -> Result<Self, BlobError> {
        fn setting(name: &'static str) -> Result<String, BlobError> {
            std::env::var(name).map_err(|_| BlobError::Environment(name))
        }
        let inner = AmazonS3Builder::new()
            .with_endpoint(setting("SWARMY_S3_ENDPOINT")?)
            .with_access_key_id(setting("SWARMY_S3_ACCESS_KEY")?)
            .with_secret_access_key(setting("SWARMY_S3_SECRET_KEY")?)
            .with_bucket_name(setting("SWARMY_S3_BUCKET")?)
            .with_region(setting("SWARMY_S3_REGION")?)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .build()?;
        Ok(Self::new(Arc::new(inner)))
    }

    /// Remove a blob once no durable pointer references it.
    /// # Errors
    /// Returns the underlying object storage error.
    pub async fn delete(&self, key: &str) -> Result<(), BlobError> {
        self.inner.delete(&Path::from(key)).await?;
        Ok(())
    }
}

#[async_trait]
impl BlobStore for ObjectBlobStore {
    async fn put(&self, key: &str, value: Bytes) -> Result<(), BlobError> {
        self.inner.put(&Path::from(key), value.into()).await?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes, BlobError> {
        Ok(self.inner.get(&Path::from(key)).await?.bytes().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_blobs_round_trip_and_report_missing_keys() {
        let store = MemoryBlobStore::default();
        assert!(matches!(
            store.get("absent").await,
            Err(BlobError::Missing(_))
        ));
        let payload = Bytes::from(vec![17; 200 * 1024]);
        store.put("payload", payload.clone()).await.unwrap();
        store.put("payload", payload.clone()).await.unwrap();
        assert_eq!(store.get("payload").await.unwrap(), payload);
    }
}
