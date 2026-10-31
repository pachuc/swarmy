//! Durable volume metadata without a direct store dependency.
use std::{future::Future, num::NonZeroUsize, sync::Arc};
use swarmy_core::{ContentHash, FenceKind, Lease, ManifestHeader, ManifestId, VolumeId};
use tokio::{sync::watch, time::Instant};

#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    #[error(transparent)]
    Fenced(#[from] FenceKind),
    #[error("volume does not exist")]
    VolumeMissing,
    #[error("manifest does not exist")]
    ManifestMissing,
    #[error("metadata commit outcome is unknown; repeat the request to read the durable outcome")]
    CommitUnknown,
    #[error("volume metadata request failed")]
    Failed(#[source] Box<dyn std::error::Error + Send + Sync>),
}

#[async_trait::async_trait]
pub trait VolumeMetadata: Send + Sync {
    /// Publish a head under the writer lease; repeating a committed publication succeeds.
    /// # Errors
    /// Returns a stale fence, unknown commit, or metadata failure.
    async fn publish(
        &self,
        volume: VolumeId,
        lease: &Lease,
        previous: ManifestId,
        next: ManifestId,
        header: &ManifestHeader,
        retention: NonZeroUsize,
    ) -> Result<(), MetadataError>;
    /// Protect reused chunks against a running collector.
    /// # Errors
    /// Returns metadata failures.
    async fn protect_reused_chunks(
        &self,
        hashes: &[ContentHash],
    ) -> Result<Vec<ContentHash>, MetadataError>;
    /// Release the lease after disconnecting the device.
    /// # Errors
    /// Returns a stale fence or metadata failure.
    async fn release_writer_lease(
        &self,
        volume: VolumeId,
        lease: &Lease,
    ) -> Result<(), MetadataError>;
}

#[derive(Clone, Debug)]
pub enum LeaseStatus {
    Held { lease: Lease, deadline: Instant },
    Lost(Arc<MetadataError>),
}
#[derive(Clone, Debug)]
pub struct WriterLease(watch::Receiver<LeaseStatus>);
#[derive(Debug)]
pub struct RenewalTask(tokio::task::JoinHandle<()>);
#[derive(Debug)]
pub struct WriterGrant {
    pub volume: VolumeId,
    pub head: ManifestId,
    pub header: ManifestHeader,
    pub lease: WriterLease,
    pub renewal: Option<RenewalTask>,
}
impl WriterLease {
    #[must_use]
    pub fn channel(lease: Lease, deadline: Instant) -> (watch::Sender<LeaseStatus>, Self) {
        let (sender, receiver) = watch::channel(LeaseStatus::Held { lease, deadline });
        (sender, Self(receiver))
    }
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn fixed(lease: Lease, deadline: Instant) -> Self {
        Self::channel(lease, deadline).1
    }
    /// Read a still-live local lease.
    /// # Errors
    /// Returns a fenced error when lost or expired.
    pub fn current(&self) -> Result<Lease, MetadataError> {
        match &*self.0.borrow() {
            LeaseStatus::Held { lease, deadline } if *deadline > Instant::now() => {
                Ok(lease.clone())
            }
            _ => Err(MetadataError::Fenced(FenceKind::VolumeLease)),
        }
    }
    /// Wait for lease loss, including a silent renewal timeout.
    pub async fn lost(&mut self) -> Arc<MetadataError> {
        loop {
            let status = self.0.borrow_and_update().clone();
            match status {
                LeaseStatus::Lost(error) => return error,
                LeaseStatus::Held { deadline, .. } => {
                    tokio::select! {
                        changed = self.0.changed() => {
                            if changed.is_ok() { continue; }
                            tokio::time::sleep_until(deadline).await;
                        }
                        () = tokio::time::sleep_until(deadline) => {}
                    }
                    return Arc::new(MetadataError::Fenced(FenceKind::VolumeLease));
                }
            }
        }
    }
}
impl RenewalTask {
    #[must_use]
    pub fn new(handle: tokio::task::JoinHandle<()>) -> Self {
        Self(handle)
    }
}
impl Drop for RenewalTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(crate) async fn with_current_lease<T, F, Fut>(
    lease: &WriterLease,
    call: F,
) -> Result<T, MetadataError>
where
    F: Fn(Lease) -> Fut,
    Fut: Future<Output = Result<T, MetadataError>>,
{
    let first = lease.current()?;
    let result = call(first.clone()).await;
    // Renewal may have committed between reading the lease and publishing.
    if matches!(result, Err(MetadataError::Fenced(FenceKind::VolumeLease)))
        && let Ok(next) = lease.current()
        && next != first
        && next.owner == first.owner
        && next.seq == first.seq
    {
        return call(next).await;
    }
    result
}
