//! Durable publication and writer fencing over a locally attached device.
use crate::{MetadataError, VolumeMetadata, WriterLease, metadata::with_current_lease};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use swarmy_core::{ManifestId, VolumeId};
use tokio::sync::Mutex;

use crate::{Result, UploadStats, VolumeDevice, VolumeError};

/// Publication timings and device activity during this call and over its lifetime.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FlushResult {
    pub manifest_id: ManifestId,
    /// Includes freeze acquisition, publication, and thaw, excluding writer queueing.
    pub elapsed: Duration,
    /// Time acquiring the filesystem freeze, including kernel writeback.
    pub freeze_wait: Duration,
    /// From successful freeze acquisition through successful thaw; zero without a mount.
    pub frozen: Duration,
    /// Successful chunk PUTs completed after freeze acquisition, including
    /// background requests that were already in flight. Zero without a mount.
    pub frozen_chunks_uploaded: u64,
    pub uploads: UploadStats,
    pub device_total: UploadStats,
    pub upload_concurrency_limit: usize,
    pub upload_bytes_per_second: u64,
    pub tool_priority_uploads: u64,
}

/// Follows the supplied lease and committed head; renewal belongs to its holder.
pub struct VolumeWriter {
    device: Arc<VolumeDevice>,
    metadata: Arc<dyn VolumeMetadata>,
    id: VolumeId,
    lease: WriterLease,
    head: Mutex<ManifestId>,
    retention: std::num::NonZeroUsize,
}

impl VolumeWriter {
    #[must_use]
    pub fn new(
        device: Arc<VolumeDevice>,
        metadata: Arc<dyn VolumeMetadata>,
        id: VolumeId,
        lease: WriterLease,
        head: ManifestId,
    ) -> Arc<Self> {
        Self::with_retention(
            device,
            metadata,
            id,
            lease,
            head,
            swarmy_config::VolumeSnapshots::default().retention,
        )
    }

    /// Stop recording boot reads for this attachment.
    #[must_use]
    pub fn take_boot_set(&self) -> swarmy_core::BootSet {
        self.device.take_boot_set()
    }

    #[must_use]
    pub fn device_stats(&self) -> crate::DeviceStats {
        self.device.stats()
    }

    #[must_use]
    pub fn with_retention(
        device: Arc<VolumeDevice>,
        metadata: Arc<dyn VolumeMetadata>,
        id: VolumeId,
        lease: WriterLease,
        head: ManifestId,
        retention: std::num::NonZeroUsize,
    ) -> Arc<Self> {
        device.protect_uploads(metadata.clone());
        Arc::new(Self {
            device,
            metadata,
            id,
            lease,
            head: Mutex::new(head),
            retention,
        })
    }

    /// Freeze a known mount, upload changed chunks, and atomically publish the
    /// next manifest under this writer's live lease. Without a mount, ext4 must
    /// replay its journal when this point-in-time block image is mounted.
    /// # Errors
    /// Returns freeze, storage, or lease errors. Always attempts to unfreeze.
    pub async fn flush(&self, mount: Option<&Path>) -> Result<FlushResult> {
        let mut head = self.head.lock().await;
        self.publish(mount, &mut head).await
    }

    /// Sync the mount without freezing, publish a block boundary, and return its head.
    /// # Errors
    /// Returns sync, storage, or writer fencing errors.
    pub async fn checkpoint(&self, mount: Option<&Path>) -> Result<ManifestId> {
        sync_mount(mount).await?;
        Ok(self.flush(None).await?.manifest_id)
    }

    /// Skip idle periods without freezing or contacting the metadata store.
    /// The check shares the publication lock with checkpoints and legacy flushes.
    /// # Errors
    /// Returns storage or writer fencing errors. The mount is never frozen.
    pub async fn flush_if_dirty(&self) -> Result<Option<FlushResult>> {
        let mut head = self.head.lock().await;
        if !self.device.has_unpublished_changes().await {
            return Ok(None);
        }
        self.publish(None, &mut head).await.map(Some)
    }

    async fn publish(&self, mount: Option<&Path>, head: &mut ManifestId) -> Result<FlushResult> {
        let start = Instant::now();
        let before = self.device.upload_stats();
        let priority_before = self.device.stats().tool_priority_uploads;
        let frozen = FrozenMount::freeze(mount).await?;
        let freeze_wait = if mount.is_some() {
            start.elapsed()
        } else {
            Duration::ZERO
        };
        let frozen_start = Instant::now();
        let frozen_before = self.device.upload_stats();
        let next = ManifestId::from_ulid(ulid::Ulid::generate());
        let previous = *head;
        let result = self
            .device
            .publish(|header| async move {
                loop {
                    match with_current_lease(&self.lease, |lease| {
                        let header = header.clone();
                        async move {
                            self.metadata
                                .publish(self.id, &lease, previous, next, &header, self.retention)
                                .await
                        }
                    })
                    .await
                    {
                        // The immutable id lets a retry recognize a publication
                        // whose commit acknowledgement was lost.
                        Err(MetadataError::CommitUnknown) => {
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                        result => {
                            result?;
                            break;
                        }
                    }
                }
                Ok(())
            })
            .await;
        if result.is_ok() {
            *head = next;
        }
        let thaw = frozen.unfreeze();
        result?;
        thaw?;
        let frozen_time = frozen_start.elapsed();
        let device_total = self.device.upload_stats();
        let limits = self.device.stats();
        let result = FlushResult {
            manifest_id: next,
            elapsed: start.elapsed(),
            freeze_wait,
            frozen: if mount.is_some() {
                frozen_time
            } else {
                Duration::ZERO
            },
            frozen_chunks_uploaded: if mount.is_some() {
                device_total.chunks_uploaded - frozen_before.chunks_uploaded
            } else {
                0
            },
            uploads: device_total.since(before),
            device_total,
            tool_priority_uploads: limits.tool_priority_uploads - priority_before,
            upload_concurrency_limit: limits.upload_concurrency_limit,
            upload_bytes_per_second: limits.upload_bytes_per_second,
        };
        tracing::info!(
            volume = %self.id,
            upload_concurrency_limit = result.upload_concurrency_limit,
            upload_bytes_per_second = result.upload_bytes_per_second,
            tool_priority_uploads = result.tool_priority_uploads,
            chunks_uploaded = result.uploads.chunks_uploaded,
            object_store_requests = result.uploads.object_store_requests,
            bytes_uploaded = result.uploads.bytes_uploaded,
            dirty_lock_wait_seconds = result.uploads.dirty_lock_wait.as_secs_f64(),
            elapsed_seconds = result.elapsed.as_secs_f64(),
            freeze_wait_seconds = result.freeze_wait.as_secs_f64(),
            frozen_seconds = result.frozen.as_secs_f64(),
            frozen_chunks_uploaded = result.frozen_chunks_uploaded,
            device_total = ?result.device_total,
            "volume flush complete"
        );
        Ok(result)
    }

    /// Release after the device has disconnected.
    /// # Errors
    /// Rejects a lost lease or metadata failure.
    pub async fn release(&self) -> Result<()> {
        with_current_lease(&self.lease, |lease| async move {
            self.metadata.release_writer_lease(self.id, &lease).await
        })
        .await?;
        Ok(())
    }

    /// Continuously pre-upload chunks quiet for at least `interval`, sleeping
    /// between passes to debounce active writes and failed requests. Dropping
    /// the handle stops it.
    #[must_use]
    pub fn background(self: &Arc<Self>, interval: Duration) -> BackgroundUploader {
        let device = self.device.clone();
        // The settled-chunk uploader; BackgroundUploader stops it on drop.
        // ast-grep-ignore: no-spawn-in-libraries
        BackgroundUploader(tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if let Err(error) = device.upload_settled(interval).await {
                    tracing::warn!(error = %swarmy_core::error_chain(&error), "background upload failed; final flush will retry");
                }
            }
        }))
    }
}

pub struct BackgroundUploader(tokio::task::JoinHandle<()>);
impl Drop for BackgroundUploader {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct FrozenMount(Option<PathBuf>);
impl FrozenMount {
    async fn freeze(mount: Option<&Path>) -> Result<Self> {
        if mount.is_none() {
            return Ok(Self(None));
        }
        let path = mount.map(Path::to_owned);
        tokio::task::spawn_blocking(move || {
            if let Some(path) = &path {
                freeze_command("--freeze", path)?;
            }
            Ok(Self(path))
        })
        .await
        .map_err(std::io::Error::other)?
    }

    fn unfreeze(mut self) -> Result<()> {
        if let Some(path) = &self.0 {
            freeze_command("--unfreeze", path)?;
        }
        self.0 = None;
        Ok(())
    }
}
impl Drop for FrozenMount {
    fn drop(&mut self) {
        if let Some(path) = &self.0
            && let Err(error) = freeze_command("--unfreeze", path)
        {
            tracing::error!(error = %swarmy_core::error_chain(&error), path = %path.display(), "could not unfreeze filesystem");
        }
    }
}

fn freeze_command(operation: &str, path: &Path) -> Result<()> {
    let output = std::process::Command::new("fsfreeze")
        .arg(operation)
        .arg("--")
        .arg(path)
        .output()?;
    if !output.status.success() {
        return Err(VolumeError::Io(std::io::Error::other(format!(
            "fsfreeze {operation} {}: {}",
            path.display(),
            String::from_utf8_lossy(&output.stderr)
        ))));
    }
    Ok(())
}

// Checkpoints drain preceding buffered writes without excluding new writes.
pub(crate) async fn sync_mount(mount: Option<&Path>) -> Result<()> {
    if let Some(mount) = mount {
        let output = tokio::process::Command::new("sync")
            .arg("-f")
            .arg("--")
            .arg(mount)
            .output()
            .await?;
        if !output.status.success() {
            return Err(std::io::Error::other(
                String::from_utf8_lossy(&output.stderr).into_owned(),
            )
            .into());
        }
    }
    Ok(())
}
