//! Volume metadata stays inline so cloning and fencing never need object storage.
use foundationdb::Transaction;
use jiff::Timestamp;
#[cfg(any(test, feature = "test-support"))]
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use swarmy_core::{
    CHUNK_SIZE, ImageRecord, ImageTag, Lease, LeaseOwnerId, ManifestHeader, ManifestId, VolumeId,
    VolumeRecord,
};

#[cfg(any(test, feature = "test-support"))]
use crate::scan_all;
use crate::{Result, Store, StoreError, read, scan, write};

/// Which key space a collector row comes from. The live-manifest scan
/// decodes each source differently, so the loop carries this instead of a
/// string tag matched back to the same spaces.
#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Copy, PartialEq, Eq)]
enum ManifestSource {
    Volume,
    Image,
    Agent,
}

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
            return Err(StoreError::Domain(crate::DomainError::InvalidManifest));
        }
        self.transaction(|trx| async move {
            let key = self.keys().manifest(id);
            if let Some(existing) = read::<ManifestHeader>(&trx, &key).await? {
                return if &existing == header {
                    Ok(())
                } else {
                    Err(StoreError::Domain(crate::DomainError::ManifestExists))
                };
            }
            write(&trx, &key, header)
        })
        .await
    }

    /// # Errors
    /// Returns decoding and transaction errors.
    pub async fn get_manifest(&self, id: ManifestId) -> Result<Option<ManifestHeader>> {
        self.transaction(|trx| async move { read(&trx, &self.keys().manifest(id)).await })
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
        self.transaction(|trx| async move { read(&trx, &self.keys().volume(id)).await })
            .await
    }
}

/// Options for registering an image name and tag against an immutable manifest.
#[derive(Clone, Debug, Default)]
pub struct PutImageOptions {
    /// Node-local scratch mount paths declared by the image recipe.
    pub scratch: Vec<String>,
    /// Required sandbox memory in MiB; rejects zero.
    pub memory_mib: Option<u64>,
    /// Whether the image ships a display server for browser and screen tools.
    pub display: bool,
}

impl Store {
    /// Register image defaults atomically with the immutable image manifest.
    /// # Errors
    /// Rejects missing manifests, oversized keys, and database failures.
    pub async fn put_image(
        &self,
        name: &str,
        tag: &ImageTag,
        manifest: ManifestId,
        options: Option<PutImageOptions>,
    ) -> Result<()> {
        let options = options.unwrap_or_default();
        if options.memory_mib == Some(0) {
            return Err(StoreError::Domain(
                crate::DomainError::InvalidMemoryRequirement,
            ));
        }
        let key = self.keys().image(name, tag);
        if key.len() > 10_000 {
            return Err(StoreError::Storage(crate::StorageError::TooLarge));
        }
        self.transaction(|trx| {
            let key = &key;
            let scratch = &options.scratch;
            let memory_mib = &options.memory_mib;
            let display = &options.display;
            async move {
                self.require_manifest(&trx, manifest).await?;
                write(&trx, key, &manifest)?;
                write(
                    &trx,
                    &self.keys().image_scratch(name, tag, manifest),
                    scratch,
                )?;
                write(
                    &trx,
                    &self.keys().image_memory(name, tag, manifest),
                    memory_mib,
                )?;
                write(
                    &trx,
                    &self.keys().image_display(name, tag, manifest),
                    display,
                )
            }
        })
        .await
    }

    /// Whether the immutable image exposes a graphical display.
    /// # Errors
    /// Returns database failures.
    pub async fn image_display(&self, image: &ImageRecord) -> Result<bool> {
        self.transaction(|trx| async move {
            Ok(read::<bool>(
                &trx,
                &self
                    .keys()
                    .image_display(&image.name, &image.tag, image.manifest_id),
            )
            .await?
            .unwrap_or(false))
        })
        .await
    }

    /// Image default, if its recipe supplied one.
    /// # Errors
    /// Returns database failures.
    pub async fn image_memory(&self, image: &ImageRecord) -> Result<Option<u64>> {
        self.transaction(|trx| async move {
            Ok(read::<Option<u64>>(
                &trx,
                &self
                    .keys()
                    .image_memory(&image.name, &image.tag, image.manifest_id),
            )
            .await?
            .flatten())
        })
        .await
    }

    /// Read mount paths pinned to an image manifest.
    /// # Errors
    /// Returns storage or decoding failures.
    pub async fn image_scratch(&self, image: &ImageRecord) -> Result<Vec<String>> {
        self.transaction(|trx| async move {
            Ok(read(
                &trx,
                &self
                    .keys()
                    .image_scratch(&image.name, &image.tag, image.manifest_id),
            )
            .await?
            .unwrap_or_default())
        })
        .await
    }

    /// # Errors
    /// Returns decoding and transaction errors.
    pub async fn get_image(&self, name: &str, tag: &ImageTag) -> Result<Option<ManifestId>> {
        self.transaction(|trx| async move { read(&trx, &self.keys().image(name, tag)).await })
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
            let space = self.keys().image_space();
            let (mut begin, end) = space.range();
            if let Some((name, tag)) = after {
                begin = crate::next_cursor(&self.keys().image(name, tag));
            }
            let mut images = Vec::new();
            for (key, value) in scan(&trx, (begin, end), limit).await? {
                let (name, tag): (String, String) = space
                    .unpack(&key)
                    .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?;
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
            return Err(StoreError::Fence(crate::FenceError::VolumeLeaseMismatch));
        }
        self.transaction(|trx| async move {
            let mut volume = self.volume(&trx, id).await?;
            self.check_volume_placement(&trx, id, owner).await?;
            if volume
                .writer_lease
                .as_ref()
                .is_some_and(|lease| lease.expires_at > now)
            {
                return Err(StoreError::Fence(crate::FenceError::VolumeLeaseMismatch));
            }
            let seq_key = self.keys().volume_lease_seq(id);
            let seq = read::<u64>(&trx, &seq_key)
                .await?
                .unwrap_or(0)
                .checked_add(1)
                .ok_or(StoreError::Storage(crate::StorageError::SequenceOverflow))?;
            let lease = Lease {
                owner,
                expires_at,
                seq,
            };
            volume.writer_lease = Some(lease.clone());
            write(&trx, &seq_key, &seq)?;
            write(&trx, &self.keys().volume(id), &volume)?;
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
                return Err(StoreError::Fence(crate::FenceError::VolumeLeaseMismatch));
            }
            volume.writer_lease = None;
            write(&trx, &self.keys().volume(id), &volume)
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
                || expected.expires_at <= self.now()
                || expires_at <= expected.expires_at
            {
                return Err(StoreError::Fence(crate::FenceError::VolumeLeaseMismatch));
            }
            let lease = Lease {
                expires_at,
                ..expected.clone()
            };
            volume.writer_lease = Some(lease.clone());
            write(&trx, &self.keys().volume(id), &volume)?;
            Ok(lease)
        })
        .await
    }

    /// Publish an uploaded manifest, its predecessor, and the head atomically.
    /// The clock is checked on each transaction attempt so retries cannot extend
    /// a writer's authority beyond its expiry. The id makes commit retries safe.
    /// # Errors
    /// Rejects stale heads, invalid headers, reused ids, and absent or stale leases.
    /// Test-only entry point, also available with the `test-support` feature.
    #[cfg(any(test, feature = "test-support"))]
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
            if volume.writer_lease.as_ref() != Some(expected) || expected.expires_at <= self.now() {
                return Err(StoreError::Fence(crate::FenceError::VolumeLeaseMismatch));
            }
            let parent_key = self.keys().manifest_parent(next);
            if volume.head_manifest == next
                && read::<ManifestId>(&trx, &parent_key).await? == Some(previous)
                && read::<ManifestHeader>(&trx, &self.keys().manifest(next))
                    .await?
                    .as_ref()
                    == Some(header)
            {
                return Ok(());
            }
            if volume.head_manifest != previous {
                return Err(StoreError::Fence(crate::FenceError::VolumeHeadMismatch));
            }
            let old: ManifestHeader = read(&trx, &self.keys().manifest(previous))
                .await?
                .ok_or(StoreError::Domain(crate::DomainError::ManifestMissing))?;
            if header.size != old.size || header.chunk_size != old.chunk_size {
                return Err(StoreError::Domain(crate::DomainError::InvalidManifest));
            }
            if read::<ManifestHeader>(&trx, &self.keys().manifest(next))
                .await?
                .is_some()
            {
                return Err(StoreError::Domain(crate::DomainError::ManifestExists));
            }
            write(&trx, &self.keys().manifest(next), header)?;
            write(&trx, &parent_key, &previous)?;
            let mut snapshots = self.snapshots(&trx, id).await?;
            snapshots.insert(0, next);
            snapshots.truncate(retention.get());
            write(&trx, &self.keys().volume_snapshots(id), &snapshots)?;
            volume.head_manifest = next;
            write(&trx, &self.keys().volume(id), &volume)
        })
        .await
    }

    /// Follow immutable provenance, not retained snapshot history or GC liveness.
    /// Image manifests have no predecessor.
    /// # Errors
    /// Returns decoding and transaction errors.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn manifest_parent(&self, id: ManifestId) -> Result<Option<ManifestId>> {
        self.transaction(|trx| async move { read(&trx, &self.keys().manifest_parent(id)).await })
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
            let space = self.keys().volume_space();
            let (mut begin, end) = space.range();
            if let Some(id) = after {
                begin = crate::next_cursor(&self.keys().volume(id));
            }
            let mut volumes = Vec::new();
            for (key, value) in scan(&trx, (begin, end), limit).await? {
                let (bytes,): (Vec<u8>,) = space
                    .unpack(&key)
                    .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?;
                let bytes: [u8; 16] = bytes
                    .try_into()
                    .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?;
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
    /// # Errors
    /// Returns missing-volume, decoding, or transaction errors.
    pub async fn volume_snapshots(&self, id: VolumeId) -> Result<Vec<ManifestId>> {
        self.transaction(|trx| async move {
            self.volume(&trx, id).await?;
            self.snapshots(&trx, id).await
        })
        .await
    }

    /// Read one volume's live roots in a bounded transaction. Fleet collectors
    /// must page volume ids and call this separately for each volume.
    /// # Errors
    /// Returns missing-volume, decoding, and transaction errors.
    pub async fn volume_live_manifests(&self, id: VolumeId) -> Result<Vec<ManifestId>> {
        self.transaction(|trx| async move {
            let volume = self.volume(&trx, id).await?;
            let mut roots = self.snapshots(&trx, id).await?;
            if volume.writer_lease.is_some() && !roots.contains(&volume.head_manifest) {
                roots.push(volume.head_manifest);
            }
            Ok(roots)
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
    #[cfg(any(test, feature = "test-support"))]
    pub async fn live_manifests(&self) -> Result<BTreeSet<ManifestId>> {
        self.transaction(|trx| async move {
            let mut live = BTreeSet::new();
            // Each manifest source decodes its rows differently, so the loop
            // carries the source enum instead of a string tag.
            for kind in [
                ManifestSource::Volume,
                ManifestSource::Image,
                ManifestSource::Agent,
            ] {
                let space = match kind {
                    ManifestSource::Volume => self.keys().volume_space(),
                    ManifestSource::Image => self.keys().image_space(),
                    ManifestSource::Agent => self.keys().agent_space(),
                };
                let (begin, end) = space.range();
                for (key, value) in scan_all(&trx, (begin, end)).await? {
                    if kind == ManifestSource::Agent {
                        live.insert(
                            swarmy_core::decode::<swarmy_core::AgentRecord>(&value)?
                                .image
                                .manifest_id,
                        );
                    } else if kind == ManifestSource::Image {
                        live.insert(swarmy_core::decode::<ManifestId>(&value)?);
                    } else {
                        let volume: VolumeRecord = swarmy_core::decode(&value)?;
                        let (bytes,): (Vec<u8>,) = space
                            .unpack(&key)
                            .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?;
                        let bytes: [u8; 16] = bytes
                            .try_into()
                            .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?;
                        let id = VolumeId::from_ulid(u128::from_be_bytes(bytes).into());
                        live.extend(self.snapshots(&trx, id).await?);
                        if volume.writer_lease.is_some() {
                            live.insert(volume.head_manifest);
                        }
                    }
                }
            }
            Ok(live)
        })
        .await
    }

    async fn snapshots(&self, trx: &Transaction, id: VolumeId) -> Result<Vec<ManifestId>> {
        read(trx, &self.keys().volume_snapshots(id))
            .await?
            .ok_or(StoreError::Storage(crate::StorageError::Corrupt))
    }

    async fn require_manifest(&self, trx: &Transaction, id: ManifestId) -> Result<()> {
        read::<ManifestHeader>(trx, &self.keys().manifest(id))
            .await?
            .ok_or(StoreError::Domain(crate::DomainError::ManifestMissing))?;
        Ok(())
    }

    async fn volume(&self, trx: &Transaction, id: VolumeId) -> Result<VolumeRecord> {
        read(trx, &self.keys().volume(id))
            .await?
            .ok_or(StoreError::Domain(crate::DomainError::VolumeMissing))
    }

    async fn insert_volume(
        &self,
        trx: &Transaction,
        id: VolumeId,
        record: &VolumeRecord,
    ) -> Result<()> {
        self.check_computer(trx, swarmy_core::AgentId::from_ulid(id.as_ulid()))
            .await?;
        let key = self.keys().volume(id);
        if read::<VolumeRecord>(trx, &key).await?.is_some() {
            return Err(StoreError::Domain(crate::DomainError::VolumeExists));
        }
        self.write_new_volume(trx, id, record)
    }

    pub(crate) fn write_new_volume(
        &self,
        trx: &Transaction,
        id: VolumeId,
        record: &VolumeRecord,
    ) -> Result<()> {
        write(
            trx,
            &self.keys().volume_snapshots(id),
            &vec![record.head_manifest],
        )?;
        write(trx, &self.keys().volume(id), record)
    }
}
