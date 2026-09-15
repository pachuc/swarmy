//! Volume metadata stays inline so cloning and fencing never need object storage.
use foundationdb::Transaction;
use jiff::Timestamp;
use swarmy_core::{
    CHUNK_SIZE, ImageRecord, ImageTag, Lease, LeaseOwnerId, ManifestHeader, ManifestId, VolumeId,
    VolumeRecord,
};

use crate::{Result, Store, StoreError, read, scan, write};

impl Store {
    /// Register an immutable header after its objects have been uploaded.
    /// Repeating the same registration is safe.
    /// # Errors
    /// Rejects invalid dimensions, conflicting headers, and transaction failures.
    pub async fn put_manifest(&self, id: ManifestId, header: &ManifestHeader) -> Result<()> {
        if header.chunk_size != CHUNK_SIZE
            || header.size == 0
            || !header.size.is_multiple_of(u64::from(CHUNK_SIZE))
        {
            return Err(StoreError::InvalidManifest);
        }
        self.transaction(|trx| async move {
            let key = self.manifest_key(id);
            if let Some(existing) = read::<ManifestHeader>(&trx, &key).await? {
                return if &existing == header {
                    Ok(())
                } else {
                    Err(StoreError::ManifestExists)
                };
            }
            write(&trx, &key, header)
        })
        .await
    }

    /// # Errors
    /// Returns decoding and transaction errors.
    pub async fn get_manifest(&self, id: ManifestId) -> Result<Option<ManifestHeader>> {
        self.transaction(|trx| async move { read(&trx, &self.manifest_key(id)).await })
            .await
    }

    /// Create a disk pointing at an existing manifest, with no writer or parent.
    /// # Errors
    /// Rejects missing manifests, duplicate volume ids, and transaction failures.
    pub async fn create_volume(&self, id: VolumeId, manifest: ManifestId) -> Result<()> {
        self.transaction(|trx| async move {
            self.require_manifest(&trx, manifest).await?;
            self.insert_volume(
                &trx,
                id,
                &VolumeRecord {
                    head_manifest: manifest,
                    writer_lease: None,
                    parent: None,
                },
            )
            .await
        })
        .await
    }

    /// Clone in one transaction containing only two fixed-size volume records.
    /// Neither the manifest header nor any object is read. A clone starts unleased.
    /// # Errors
    /// Rejects missing sources, duplicate destination ids, and transaction failures.
    pub async fn clone_volume(&self, source: VolumeId, destination: VolumeId) -> Result<()> {
        self.transaction(|trx| async move {
            let source_record = self.volume(&trx, source).await?;
            self.insert_volume(
                &trx,
                destination,
                &VolumeRecord {
                    head_manifest: source_record.head_manifest,
                    writer_lease: None,
                    parent: Some(source),
                },
            )
            .await
        })
        .await
    }

    /// # Errors
    /// Returns decoding and transaction errors.
    pub async fn get_volume(&self, id: VolumeId) -> Result<Option<VolumeRecord>> {
        self.transaction(|trx| async move { read(&trx, &self.volume_key(id)).await })
            .await
    }

    /// Map an image name and tag to an existing immutable manifest.
    /// # Errors
    /// Rejects missing manifests, oversized keys, and transaction failures.
    pub async fn put_image(&self, name: &str, tag: &ImageTag, manifest: ManifestId) -> Result<()> {
        let key = self.image_key(name, tag);
        if key.len() > 10_000 {
            return Err(StoreError::TooLarge);
        }
        self.transaction(|trx| {
            let key = &key;
            async move {
                self.require_manifest(&trx, manifest).await?;
                write(&trx, key, &manifest)
            }
        })
        .await
    }

    /// # Errors
    /// Returns decoding and transaction errors.
    pub async fn get_image(&self, name: &str, tag: &ImageTag) -> Result<Option<ManifestId>> {
        self.transaction(|trx| async move { read(&trx, &self.image_key(name, tag)).await })
            .await
    }

    /// List image registrations in name/tag order with an exclusive cursor.
    /// # Errors
    /// Rejects invalid limits, malformed records, and transaction failures.
    pub async fn list_images(
        &self,
        after: Option<(&str, &ImageTag)>,
        limit: usize,
    ) -> Result<Vec<ImageRecord>> {
        self.transaction(|trx| async move {
            let space = self.root.subspace(&("image",));
            let (mut begin, end) = space.range();
            if let Some((name, tag)) = after {
                begin = self.image_key(name, tag);
                begin.push(0);
            }
            let mut images = Vec::new();
            for (key, value) in scan(&trx, (begin, end), limit).await? {
                let (name, tag): (String, String) =
                    space.unpack(&key).map_err(|_| StoreError::Corrupt)?;
                images.push(ImageRecord {
                    name,
                    tag: ImageTag(tag),
                    manifest_id: swarmy_core::decode(&value)?,
                });
            }
            Ok(images)
        })
        .await
    }

    /// Acquire only when no writer exists or the previous writer has expired.
    /// The caller supplies time, as with session leases. Every grant increments
    /// a retained counter so releasing and reacquiring cannot recreate an old token.
    /// # Errors
    /// Rejects live writers, invalid expiry times, missing volumes, and transaction failures.
    pub async fn acquire_writer_lease(
        &self,
        id: VolumeId,
        owner: LeaseOwnerId,
        now: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Lease> {
        if expires_at <= now {
            return Err(StoreError::LeaseMismatch);
        }
        self.transaction(|trx| async move {
            let mut volume = self.volume(&trx, id).await?;
            if volume
                .writer_lease
                .as_ref()
                .is_some_and(|lease| lease.expires_at > now)
            {
                return Err(StoreError::LeaseMismatch);
            }
            let seq_key = self.volume_lease_seq_key(id);
            let seq = read::<u64>(&trx, &seq_key)
                .await?
                .unwrap_or(0)
                .checked_add(1)
                .ok_or(StoreError::SequenceOverflow)?;
            let lease = Lease {
                owner,
                expires_at,
                seq,
            };
            volume.writer_lease = Some(lease.clone());
            write(&trx, &seq_key, &seq)?;
            write(&trx, &self.volume_key(id), &volume)?;
            Ok(lease)
        })
        .await
    }

    /// Release only with the complete, still-live fencing token.
    /// # Errors
    /// Rejects absent, expired, or replaced leases and transaction failures.
    pub async fn release_writer_lease(
        &self,
        id: VolumeId,
        expected: &Lease,
        now: Timestamp,
    ) -> Result<()> {
        self.transaction(|trx| async move {
            let mut volume = self.volume(&trx, id).await?;
            if volume.writer_lease.as_ref() != Some(expected) || expected.expires_at <= now {
                return Err(StoreError::LeaseMismatch);
            }
            volume.writer_lease = None;
            write(&trx, &self.volume_key(id), &volume)
        })
        .await
    }

    async fn require_manifest(&self, trx: &Transaction, id: ManifestId) -> Result<()> {
        read::<ManifestHeader>(trx, &self.manifest_key(id))
            .await?
            .ok_or(StoreError::ManifestMissing)?;
        Ok(())
    }

    async fn volume(&self, trx: &Transaction, id: VolumeId) -> Result<VolumeRecord> {
        read(trx, &self.volume_key(id))
            .await?
            .ok_or(StoreError::VolumeMissing)
    }

    async fn insert_volume(
        &self,
        trx: &Transaction,
        id: VolumeId,
        record: &VolumeRecord,
    ) -> Result<()> {
        let key = self.volume_key(id);
        if read::<VolumeRecord>(trx, &key).await?.is_some() {
            return Err(StoreError::VolumeExists);
        }
        write(trx, &key, record)
    }
}
