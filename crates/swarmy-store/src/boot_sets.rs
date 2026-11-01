use crate::{Result, Store, read, write};
use foundationdb::Transaction;
use swarmy_core::{ManifestId, VolumeId, VolumeRecord};

/// The throwaway volume used to boot an image once.
#[must_use]
pub fn image_probe_volume(manifest: ManifestId) -> VolumeId {
    VolumeId::from_ulid(manifest.as_ulid())
}

impl Store {
    /// Record the first boot set for this manifest; concurrent writers cannot replace it.
    /// # Errors
    /// Storage failures.
    pub async fn record_boot_set(&self, manifest: ManifestId, key: &str) -> Result<bool> {
        self.transaction(|trx| async move { self.record_boot_set_in(&trx, manifest, key).await })
            .await
    }

    pub(crate) async fn record_boot_set_in(
        &self,
        trx: &Transaction,
        manifest: ManifestId,
        key: &str,
    ) -> Result<bool> {
        let row = self.keys().boot_set(manifest);
        if read::<String>(trx, &row).await?.is_some() {
            return Ok(false);
        }
        write(trx, &row, &key.to_owned())?;
        Ok(true)
    }

    /// Create or reopen the one probe volume for an image without a boot set.
    /// # Errors
    /// Storage failures or an unknown manifest.
    pub async fn open_image_probe(&self, manifest: ManifestId) -> Result<Option<VolumeId>> {
        self.transaction(|trx| async move {
            if read::<String>(&trx, &self.keys().boot_set(manifest))
                .await?
                .is_some()
            {
                return Ok(None);
            }
            let volume = image_probe_volume(manifest);
            if read::<VolumeRecord>(&trx, &self.keys().volume(volume))
                .await?
                .is_none()
            {
                self.require_manifest(&trx, manifest).await?;
                self.insert_volume(
                    &trx,
                    volume,
                    &VolumeRecord {
                        head_manifest: manifest,
                        writer_lease: None,
                        parent: None,
                    },
                )
                .await?;
            }
            Ok(Some(volume))
        })
        .await
    }

    /// Record the probe's first boot set and remove its throwaway volume.
    /// # Errors
    /// Storage failures.
    pub async fn finish_image_probe(
        &self,
        manifest: ManifestId,
        key: Option<&str>,
    ) -> Result<bool> {
        self.transaction(|trx| async move {
            let recorded = if let Some(key) = key {
                self.record_boot_set_in(&trx, manifest, key).await?
            } else {
                false
            };
            let volume = image_probe_volume(manifest);
            trx.clear(&self.keys().volume(volume));
            trx.clear(&self.keys().volume_snapshots(volume));
            Ok(recorded)
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
