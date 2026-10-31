use crate::{Result, Store, read, write};
use swarmy_core::ManifestId;

impl Store {
    /// Record the first boot set for this manifest; concurrent writers cannot replace it.
    /// # Errors
    /// Storage failures.
    pub async fn record_boot_set(&self, manifest: ManifestId, key: &str) -> Result<bool> {
        self.transaction(|trx| async move {
            let row = self.keys().boot_set(manifest);
            if read::<String>(&trx, &row).await?.is_some() {
                return Ok(false);
            }
            write(&trx, &row, &key.to_owned())?;
            Ok(true)
        })
        .await
    }

    /// Return the boot-set object key for a manifest, if any.
    /// # Errors
    /// Storage and decoding failures.
    pub async fn boot_set(&self, manifest: ManifestId) -> Result<Option<String>> {
        self.transaction(|trx| async move { read(&trx, &self.keys().boot_set(manifest)).await })
            .await
    }
}
