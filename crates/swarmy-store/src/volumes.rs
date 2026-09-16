//! Volume metadata stays inline so cloning and fencing never need object storage.
use foundationdb::Transaction;
use jiff::Timestamp;
use std::{collections::BTreeSet, num::NonZeroUsize};
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

    /// Clone using volume records and one initial snapshot reference.
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
            self.check_volume_placement(&trx, id, owner).await?;
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

    /// Renew a live writer without changing its fencing sequence.
    /// # Errors
    /// Rejects expired or replaced tokens and non-increasing expiry times.
    pub async fn renew_writer_lease(
        &self,
        id: VolumeId,
        expected: &Lease,
        expires_at: Timestamp,
    ) -> Result<Lease> {
        self.transaction(|trx| async move {
            self.check_volume_placement(&trx, id, expected.owner)
                .await?;
            let mut volume = self.volume(&trx, id).await?;
            if volume.writer_lease.as_ref() != Some(expected)
                || expected.expires_at <= Timestamp::now()
                || expires_at <= expected.expires_at
            {
                return Err(StoreError::LeaseMismatch);
            }
            let lease = Lease {
                expires_at,
                ..expected.clone()
            };
            volume.writer_lease = Some(lease.clone());
            write(&trx, &self.volume_key(id), &volume)?;
            Ok(lease)
        })
        .await
    }

    /// Publish an uploaded manifest, its predecessor, and the head atomically.
    /// The clock is checked on each transaction attempt so retries cannot extend
    /// a writer's authority beyond its expiry. The id makes commit retries safe.
    /// # Errors
    /// Rejects stale heads, invalid headers, reused ids, and absent or stale leases.
    pub async fn advance_volume(
        &self,
        id: VolumeId,
        expected: &Lease,
        previous: ManifestId,
        next: ManifestId,
        header: &ManifestHeader,
    ) -> Result<()> {
        self.advance_volume_retained(
            id,
            expected,
            previous,
            next,
            header,
            swarmy_config::VolumeSnapshots::default().retention,
        )
        .await
    }

    /// Publish and trim this volume's snapshot history in the same fenced transaction.
    /// Retention never deletes manifest headers, parent links, or chunk objects.
    /// # Errors
    /// Rejects stale heads, invalid headers, reused ids, and absent or stale leases.
    pub async fn advance_volume_retained(
        &self,
        id: VolumeId,
        expected: &Lease,
        previous: ManifestId,
        next: ManifestId,
        header: &ManifestHeader,
        retention: NonZeroUsize,
    ) -> Result<()> {
        self.transaction(|trx| async move {
            self.check_volume_placement(&trx, id, expected.owner)
                .await?;
            let mut volume = self.volume(&trx, id).await?;
            if volume.writer_lease.as_ref() != Some(expected)
                || expected.expires_at <= Timestamp::now()
            {
                return Err(StoreError::LeaseMismatch);
            }
            let parent_key = self.manifest_parent_key(next);
            if volume.head_manifest == next
                && read::<ManifestId>(&trx, &parent_key).await? == Some(previous)
                && read::<ManifestHeader>(&trx, &self.manifest_key(next))
                    .await?
                    .as_ref()
                    == Some(header)
            {
                return Ok(());
            }
            if volume.head_manifest != previous {
                return Err(StoreError::VolumeHeadMismatch);
            }
            let old: ManifestHeader = read(&trx, &self.manifest_key(previous))
                .await?
                .ok_or(StoreError::ManifestMissing)?;
            if header.size != old.size || header.chunk_size != old.chunk_size {
                return Err(StoreError::InvalidManifest);
            }
            if read::<ManifestHeader>(&trx, &self.manifest_key(next))
                .await?
                .is_some()
            {
                return Err(StoreError::ManifestExists);
            }
            write(&trx, &self.manifest_key(next), header)?;
            write(&trx, &parent_key, &previous)?;
            let mut snapshots = self.snapshots(&trx, id, previous, retention.get()).await?;
            snapshots.insert(0, next);
            snapshots.truncate(retention.get());
            write(&trx, &self.volume_snapshots_key(id), &snapshots)?;
            volume.head_manifest = next;
            write(&trx, &self.volume_key(id), &volume)
        })
        .await
    }

    /// Follow immutable provenance, not retained snapshot history or GC liveness.
    /// Image manifests have no predecessor.
    /// # Errors
    /// Returns decoding and transaction errors.
    pub async fn manifest_parent(&self, id: ManifestId) -> Result<Option<ManifestId>> {
        self.transaction(|trx| async move { read(&trx, &self.manifest_parent_key(id)).await })
            .await
    }

    /// List all volumes in id order with an exclusive cursor.
    /// # Errors
    /// Rejects invalid limits, malformed keys, and transaction failures.
    pub async fn list_volumes(
        &self,
        after: Option<VolumeId>,
        limit: usize,
    ) -> Result<Vec<(VolumeId, VolumeRecord)>> {
        self.transaction(|trx| async move {
            let space = self.root.subspace(&("volume",));
            let (mut begin, end) = space.range();
            if let Some(id) = after {
                begin = self.volume_key(id);
                begin.push(0);
            }
            let mut volumes = Vec::new();
            for (key, value) in scan(&trx, (begin, end), limit).await? {
                let (bytes,): (Vec<u8>,) = space.unpack(&key).map_err(|_| StoreError::Corrupt)?;
                let bytes: [u8; 16] = bytes.try_into().map_err(|_| StoreError::Corrupt)?;
                volumes.push((
                    VolumeId::from_ulid(u128::from_be_bytes(bytes).into()),
                    swarmy_core::decode(&value)?,
                ));
            }
            Ok(volumes)
        })
        .await
    }

    /// Retained snapshots in publication order, newest first, including the head.
    /// Older records import the newest ten entries from immutable parent links.
    /// # Errors
    /// Returns missing-volume, decoding, or transaction errors.
    pub async fn volume_snapshots(&self, id: VolumeId) -> Result<Vec<ManifestId>> {
        self.transaction(|trx| async move {
            let volume = self.volume(&trx, id).await?;
            self.snapshots(
                &trx,
                id,
                volume.head_manifest,
                swarmy_config::VolumeSnapshots::default().retention.get(),
            )
            .await
        })
        .await
    }

    /// Return the live manifest roots at one database read version: retained
    /// snapshots of every volume, heads with an attached writer (including an
    /// expired lease until explicitly released), and every registered image.
    /// Parent links and unreferenced manifest headers do not confer liveness.
    /// A collector must also protect publications concurrent with its sweep.
    /// # Errors
    /// Returns decoding and transaction errors, including transaction size/time limits.
    pub async fn live_manifests(&self) -> Result<BTreeSet<ManifestId>> {
        self.transaction(|trx| async move {
            let mut live = BTreeSet::new();
            for kind in ["volume", "image"] {
                let space = self.root.subspace(&(kind,));
                let (mut begin, end) = space.range();
                loop {
                    let page =
                        scan(&trx, (begin.clone(), end.clone()), crate::MAX_SCAN_LIMIT).await?;
                    if page.is_empty() {
                        break;
                    }
                    for (key, value) in page {
                        if kind == "image" {
                            live.insert(swarmy_core::decode::<ManifestId>(&value)?);
                        } else {
                            let volume: VolumeRecord = swarmy_core::decode(&value)?;
                            let (bytes,): (Vec<u8>,) =
                                space.unpack(&key).map_err(|_| StoreError::Corrupt)?;
                            let bytes: [u8; 16] =
                                bytes.try_into().map_err(|_| StoreError::Corrupt)?;
                            let id = VolumeId::from_ulid(u128::from_be_bytes(bytes).into());
                            live.extend(
                                self.snapshots(
                                    &trx,
                                    id,
                                    volume.head_manifest,
                                    swarmy_config::VolumeSnapshots::default().retention.get(),
                                )
                                .await?,
                            );
                            if volume.writer_lease.is_some() {
                                live.insert(volume.head_manifest);
                            }
                        }
                        begin = key;
                        begin.push(0);
                    }
                }
            }
            Ok(live)
        })
        .await
    }

    async fn snapshots(
        &self,
        trx: &Transaction,
        id: VolumeId,
        head: ManifestId,
        legacy_limit: usize,
    ) -> Result<Vec<ManifestId>> {
        if let Some(snapshots) = read(trx, &self.volume_snapshots_key(id)).await? {
            return Ok(snapshots);
        }
        // Import only the retained portion of pre-retention history. Never
        // traverse a whole legacy chain in a publication transaction.
        let mut snapshots = vec![head];
        while snapshots.len() < legacy_limit {
            let Some(parent) = read::<ManifestId>(
                trx,
                &self.manifest_parent_key(*snapshots.last().ok_or(StoreError::Corrupt)?),
            )
            .await?
            else {
                break;
            };
            if snapshots.contains(&parent) {
                return Err(StoreError::Corrupt);
            }
            snapshots.push(parent);
        }
        Ok(snapshots)
    }

    fn volume_snapshots_key(&self, id: VolumeId) -> Vec<u8> {
        self.root
            .pack(&("volume_snapshots", id.as_ulid().to_bytes().as_slice()))
    }

    fn manifest_parent_key(&self, id: ManifestId) -> Vec<u8> {
        self.root
            .pack(&("manifest_parent", id.as_ulid().to_bytes().as_slice()))
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
        write(
            trx,
            &self.volume_snapshots_key(id),
            &vec![record.head_manifest],
        )?;
        write(trx, &key, record)
    }
}
