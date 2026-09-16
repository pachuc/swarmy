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
    #[error(transparent)]
    Configuration(#[from] swarmy_config::Error),
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

    /// Read shared S3 settings, including `SWARMY_S3_*` overrides.
    /// # Errors
    /// Returns an error for an unreadable configuration or invalid S3 settings.
    pub fn from_env() -> Result<Self, BlobError> {
        let settings = swarmy_config::Settings::load()?.settings;
        let inner = AmazonS3Builder::new()
            .with_endpoint(settings.s3_endpoint)
            .with_access_key_id(settings.s3_access_key)
            .with_secret_access_key(settings.s3_secret_key)
            .with_bucket_name(settings.s3_bucket)
            .with_region(settings.s3_region)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .build()?;
        Ok(Self::new(Arc::new(inner)))
    }

    #[must_use]
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.inner.clone()
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
