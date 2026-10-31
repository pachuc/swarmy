//! Store-backed volume metadata and the primary's writer lease keeper.
use crate::{DomainError, FenceError, Result, StorageError, Store, StoreError};
use async_trait::async_trait;
use jiff::Timestamp;
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use swarmy_core::{
    ContentHash, FenceKind, Lease, LeaseOwnerId, ManifestHeader, ManifestId, VolumeId,
};
use swarmy_volume::{
    LeaseStatus, MetadataError, RenewalTask, VolumeMetadata, WriterGrant, WriterLease,
};
use tokio::time::Instant;

const WRITER_LEASE: Duration = Duration::from_secs(60);
const WRITER_RENEW_EVERY: Duration = Duration::from_secs(15);

impl FenceError {
    /// The token kind a node-side caller can be told about.
    #[must_use]
    pub fn kind(&self) -> Option<FenceKind> {
        match self {
            Self::PlacementMismatch => Some(FenceKind::Placement),
            Self::PlacedToolClaimMismatch => Some(FenceKind::PlacedToolClaim),
            Self::VolumeLeaseMismatch => Some(FenceKind::VolumeLease),
            Self::VolumeHeadMismatch => Some(FenceKind::VolumeHead),
            _ => None,
        }
    }
}
impl From<StoreError> for MetadataError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::Fence(ref fence) if fence.kind().is_some() => {
                Self::Fenced(fence.kind().expect("checked above"))
            }
            StoreError::Storage(StorageError::CommitUnknown) => Self::CommitUnknown,
            StoreError::Domain(DomainError::VolumeMissing) => Self::VolumeMissing,
            StoreError::Domain(DomainError::ManifestMissing) => Self::ManifestMissing,
            other => Self::Failed(Box::new(other)),
        }
    }
}
#[async_trait]
impl VolumeMetadata for Store {
    async fn publish(
        &self,
        volume: VolumeId,
        lease: &Lease,
        previous: ManifestId,
        next: ManifestId,
        header: &ManifestHeader,
        retention: NonZeroUsize,
    ) -> std::result::Result<(), MetadataError> {
        Ok(
            Store::advance_volume_retained(self, volume, lease, previous, next, header, retention)
                .await?,
        )
    }
    async fn protect_reused_chunks(
        &self,
        hashes: &[ContentHash],
    ) -> std::result::Result<Vec<ContentHash>, MetadataError> {
        Ok(Store::protect_reused_chunks(self, hashes).await?)
    }
    async fn release_writer_lease(
        &self,
        volume: VolumeId,
        lease: &Lease,
    ) -> std::result::Result<(), MetadataError> {
        Ok(Store::release_writer_lease(self, volume, lease, Timestamp::now()).await?)
    }
}
impl Store {
    /// Acquire a writer lease, read the head, and renew until the grant drops.
    /// # Errors
    /// Rejects another writer, missing metadata, and transaction failures.
    pub async fn open_volume_writer(
        &self,
        volume: VolumeId,
        owner: LeaseOwnerId,
    ) -> Result<WriterGrant> {
        self.open_volume_writer_inner(volume, owner, WRITER_LEASE, WRITER_RENEW_EVERY)
            .await
    }
    /// Open a writer with explicit lease timing for tests.
    /// # Errors
    /// Rejects another writer, missing metadata, and transaction failures.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn open_volume_writer_timed(
        &self,
        volume: VolumeId,
        owner: LeaseOwnerId,
        lease: Duration,
        renew_every: Duration,
    ) -> Result<WriterGrant> {
        self.open_volume_writer_inner(volume, owner, lease, renew_every)
            .await
    }
    async fn open_volume_writer_inner(
        &self,
        volume: VolumeId,
        owner: LeaseOwnerId,
        length: Duration,
        every: Duration,
    ) -> Result<WriterGrant> {
        let sent = Instant::now();
        let now = Timestamp::now();
        let expiry = now
            .checked_add(length)
            .map_err(|_| StoreError::Fence(FenceError::VolumeLeaseMismatch))?;
        let lease = self
            .acquire_writer_lease(volume, owner, now, expiry)
            .await?;
        let read = async {
            let record = self
                .get_volume(volume)
                .await?
                .ok_or(StoreError::Domain(DomainError::VolumeMissing))?;
            let header = self
                .get_manifest(record.head_manifest)
                .await?
                .ok_or(StoreError::Domain(DomainError::ManifestMissing))?;
            Ok::<_, StoreError>((record.head_manifest, header))
        }
        .await;
        let (head, header) = match read {
            Ok(value) => value,
            Err(error) => {
                if let Err(release) = self
                    .release_writer_lease(volume, &lease, Timestamp::now())
                    .await
                {
                    tracing::warn!(error = %swarmy_core::error_chain(&release), "writer lease release after open failure failed");
                }
                return Err(error);
            }
        };
        let (renewal, watch) = self.spawn_writer_renewal(volume, lease, sent, length, every);
        Ok(WriterGrant {
            volume,
            head,
            header,
            lease: watch,
            renewal: Some(renewal),
        })
    }
    fn spawn_writer_renewal(
        &self,
        volume: VolumeId,
        lease: Lease,
        sent: Instant,
        length: Duration,
        every: Duration,
    ) -> (RenewalTask, WriterLease) {
        let (sender, watch) = WriterLease::channel(lease.clone(), sent + length);
        let store = self.clone();
        // The grant owns and aborts this renewal task on drop.
        // ast-grep-ignore: no-spawn-in-libraries
        let handle = tokio::spawn(async move {
            let mut current = lease;
            loop {
                tokio::time::sleep(every).await;
                let sent = Instant::now();
                let result = match Timestamp::now().checked_add(length) {
                    Ok(expiry) => store.renew_writer_lease(volume, &current, expiry).await,
                    Err(_) => Err(StoreError::Fence(FenceError::VolumeLeaseMismatch)),
                };
                match result {
                    Ok(next) => {
                        current = next.clone();
                        if sender
                            .send(LeaseStatus::Held {
                                lease: next,
                                deadline: sent + length,
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(error) => {
                        swarmy_core::ignore_best_effort(
                            sender.send(LeaseStatus::Lost(Arc::new(error.into()))),
                            "report lost writer lease",
                        );
                        break;
                    }
                }
            }
        });
        (RenewalTask::new(handle), watch)
    }
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    #[test]
    fn store_errors_map_to_metadata_errors() {
        assert!(matches!(
            MetadataError::from(StoreError::Fence(FenceError::VolumeLeaseMismatch)),
            MetadataError::Fenced(FenceKind::VolumeLease)
        ));
        assert!(matches!(
            MetadataError::from(StoreError::Fence(FenceError::VolumeHeadMismatch)),
            MetadataError::Fenced(FenceKind::VolumeHead)
        ));
        assert!(matches!(
            MetadataError::from(StoreError::Storage(StorageError::CommitUnknown)),
            MetadataError::CommitUnknown
        ));
        assert!(matches!(
            MetadataError::from(StoreError::Fence(FenceError::GcLeaseMismatch)),
            MetadataError::Failed(_)
        ));
    }
}
