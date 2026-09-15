//! Durable publication and writer fencing over a locally attached device.
use jiff::Timestamp;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use swarmy_core::{Lease, ManifestId, VolumeId};
use swarmy_store::Store;
use tokio::sync::Mutex;

use crate::{Result, VolumeDevice, VolumeError};

/// Owns a writer's fencing token and committed head. Renew independently of
/// uploads so a large flush does not let the writer lease expire.
pub struct VolumeWriter {
    device: Arc<VolumeDevice>,
    store: Store,
    id: VolumeId,
    lease: Mutex<Lease>,
    head: Mutex<ManifestId>,
}

impl VolumeWriter {
    #[must_use]
    pub fn new(
        device: Arc<VolumeDevice>,
        store: Store,
        id: VolumeId,
        lease: Lease,
        head: ManifestId,
    ) -> Arc<Self> {
        Arc::new(Self {
            device,
            store,
            id,
            lease: Mutex::new(lease),
            head: Mutex::new(head),
        })
    }

    /// Freeze a known mount, upload changed chunks, and atomically publish the
    /// next manifest under this writer's live lease. Without a mount, ext4 must
    /// replay its journal when this point-in-time block image is mounted.
    /// # Errors
    /// Returns freeze, storage, or lease errors. Always attempts to unfreeze.
    pub async fn flush(&self, mount: Option<&Path>) -> Result<ManifestId> {
        let mut head = self.head.lock().await;
        let frozen = FrozenMount::freeze(mount).await?;
        let next = ManifestId::from_ulid(ulid::Ulid::generate());
        let previous = *head;
        let result = self
            .device
            .publish(|header| async move {
                let lease = self.lease.lock().await;
                loop {
                    match self
                        .store
                        .advance_volume(self.id, &lease, previous, next, &header)
                        .await
                    {
                        // The immutable id lets a retry recognize a publication
                        // whose commit acknowledgement was lost.
                        Err(swarmy_store::StoreError::CommitUnknown) => {}
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
        Ok(next)
    }

    /// Extend the live lease. Call periodically while the NBD server is active.
    /// # Errors
    /// Rejects a lost lease or returns database errors.
    pub async fn renew(&self, duration: Duration) -> Result<()> {
        let mut lease = self.lease.lock().await;
        let expiry = Timestamp::now()
            .checked_add(duration)
            .map_err(std::io::Error::other)?;
        *lease = self
            .store
            .renew_writer_lease(self.id, &lease, expiry)
            .await?;
        Ok(())
    }

    /// Release after the device has disconnected, preventing a new writer from
    /// acquiring while the old kernel device can still accept requests.
    /// # Errors
    /// Rejects a lost lease or returns database errors.
    pub async fn release(&self) -> Result<()> {
        let lease = self.lease.lock().await;
        self.store
            .release_writer_lease(self.id, &lease, Timestamp::now())
            .await?;
        Ok(())
    }

    /// Continuously pre-upload local changes. Dropping the handle stops it.
    #[must_use]
    pub fn background(self: &Arc<Self>, interval: Duration) -> BackgroundUploader {
        let device = self.device.clone();
        BackgroundUploader(tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if let Err(error) = device.upload_dirty().await {
                    tracing::warn!(%error, "background upload failed; final flush will retry");
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
            tracing::error!(%error, path = %path.display(), "could not unfreeze filesystem");
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
