//! Image metadata is stored separately so legacy session headers stay readable.
use crate::{MAX_SCAN_LIMIT, Result, Store, StoreError, read};
use swarmy_core::{ImageRecord, ImageTag, ManifestId, SessionId};

pub(crate) type ImageCache =
    std::sync::Arc<tokio::sync::Mutex<std::collections::HashMap<SessionId, ManifestId>>>;

impl Store {
    pub(crate) fn session_image_key(&self, id: SessionId) -> Vec<u8> {
        self.root
            .pack(&("session_image", id.as_ulid().to_bytes().as_slice()))
    }

    /// Read a session's pinned image. Only legacy sessions can lack this row.
    /// # Errors
    /// Returns storage or decoding failures.
    pub async fn session_image(&self, id: SessionId) -> Result<Option<ManifestId>> {
        if let Some(manifest) = self.images.lock().await.get(&id) {
            return Ok(Some(*manifest));
        }
        let manifest = self
            .transaction(|trx| async move {
                Ok(read::<ImageRecord>(&trx, &self.session_image_key(id))
                    .await?
                    .map(|image| image.manifest_id))
            })
            .await?;
        // Only committed, immutable pins are cached. A missing legacy row may
        // be populated later, and an image tag can move without changing a pin.
        if let Some(manifest) = manifest {
            let mut cache = self.images.lock().await;
            if cache.len() >= 4096 {
                cache.clear();
            }
            cache.insert(id, manifest);
        }
        Ok(manifest)
    }

    pub(crate) async fn unregistered_image(&self, image: &str) -> Result<StoreError> {
        let mut registered = Vec::new();
        let mut after: Option<(String, ImageTag)> = None;
        loop {
            let page = self
                .list_images(
                    after.as_ref().map(|(name, tag)| (name.as_str(), tag)),
                    MAX_SCAN_LIMIT,
                )
                .await?;
            if page.is_empty() {
                break;
            }
            after = page
                .last()
                .map(|image| (image.name.clone(), image.tag.clone()));
            registered.extend(
                page.into_iter()
                    .map(|image| format!("{}:{}", image.name, image.tag.0)),
            );
        }
        Ok(StoreError::ImageMissing {
            image: image.into(),
            registered: if registered.is_empty() {
                "(none)".into()
            } else {
                registered.join(", ")
            },
        })
    }
}
