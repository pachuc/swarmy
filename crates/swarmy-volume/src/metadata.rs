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

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    use swarmy_core::LeaseOwnerId;

    fn lease() -> Lease {
        Lease {
            owner: LeaseOwnerId::from_ulid(ulid::Ulid::generate()),
            seq: 1,
            expires_at: jiff::Timestamp::now(),
        }
    }
    fn fenced(error: &MetadataError) -> bool {
        matches!(error, MetadataError::Fenced(FenceKind::VolumeLease))
    }

    #[tokio::test]
    async fn lost_resolves_with_the_reported_error() {
        let (sender, mut watch) =
            WriterLease::channel(lease(), Instant::now() + Duration::from_secs(60));
        sender
            .send(LeaseStatus::Lost(Arc::new(MetadataError::Fenced(
                FenceKind::VolumeLease,
            ))))
            .unwrap();
        assert!(fenced(&*watch.lost().await));
    }
    #[tokio::test]
    async fn lost_resolves_when_the_deadline_passes() {
        let (_, mut watch) = WriterLease::channel(lease(), Instant::now());
        assert!(fenced(&*watch.lost().await));
    }
    #[tokio::test]
    async fn held_lease_with_a_future_deadline_is_not_lost() {
        let held = lease();
        let (_, mut watch) =
            WriterLease::channel(held.clone(), Instant::now() + Duration::from_secs(3600));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), watch.lost())
                .await
                .is_err()
        );
        assert_eq!(watch.current().unwrap(), held);
    }
    #[tokio::test]
    async fn renewal_moves_the_deadline() {
        let held = lease();
        let (sender, mut watch) =
            WriterLease::channel(held.clone(), Instant::now() + Duration::from_millis(100));
        sender
            .send(LeaseStatus::Held {
                lease: held,
                deadline: Instant::now() + Duration::from_secs(3600),
            })
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(300), watch.lost())
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn current_refuses_a_lost_or_expired_lease() {
        let (_, expired) = WriterLease::channel(lease(), Instant::now());
        assert!(matches!(
            expired.current(),
            Err(MetadataError::Fenced(FenceKind::VolumeLease))
        ));
        let (sender, lost) =
            WriterLease::channel(lease(), Instant::now() + Duration::from_secs(60));
        sender
            .send(LeaseStatus::Lost(Arc::new(MetadataError::Fenced(
                FenceKind::VolumeLease,
            ))))
            .unwrap();
        assert!(matches!(
            lost.current(),
            Err(MetadataError::Fenced(FenceKind::VolumeLease))
        ));
    }
    #[tokio::test]
    async fn a_renewal_racing_a_call_retries_once() {
        for same_sequence in [true, false] {
            let first = lease();
            let (sender, watch) =
                WriterLease::channel(first.clone(), Instant::now() + Duration::from_secs(60));
            let calls = AtomicUsize::new(0);
            let result = with_current_lease(&watch, |current| {
                let sender = sender.clone();
                let calls = &calls;
                let first = first.clone();
                async move {
                    if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        let next = Lease {
                            seq: first.seq + u64::from(!same_sequence),
                            expires_at: first
                                .expires_at
                                .checked_add(Duration::from_secs(30))
                                .unwrap(),
                            ..first
                        };
                        sender
                            .send(LeaseStatus::Held {
                                lease: next,
                                deadline: Instant::now() + Duration::from_secs(60),
                            })
                            .unwrap();
                        Err(MetadataError::Fenced(FenceKind::VolumeLease))
                    } else {
                        Ok(current)
                    }
                }
            })
            .await;
            if same_sequence {
                assert!(result.is_ok());
                assert_eq!(calls.load(Ordering::SeqCst), 2);
            } else {
                assert!(matches!(
                    result,
                    Err(MetadataError::Fenced(FenceKind::VolumeLease))
                ));
                assert_eq!(calls.load(Ordering::SeqCst), 1);
            }
        }
    }
}
