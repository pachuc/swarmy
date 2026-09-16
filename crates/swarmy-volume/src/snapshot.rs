//! Periodic publication task owned by an attachment, alongside its uploader.
use std::time::Duration;

/// Dropping the handle cancels the loop. Attachment shutdown must serialize
/// against its operation before unmounting or disconnecting the device.
pub struct SnapshotLoop(tokio::task::JoinHandle<()>);

impl SnapshotLoop {
    /// Wait a full period before the first attempt and between attempts. Failed
    /// publications leave dirty chunks pending and are retried next period.
    #[must_use]
    pub fn spawn<F, Fut, E>(period: Duration, mut snapshot: F) -> Self
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<(), E>> + Send,
        E: std::fmt::Display,
    {
        Self(tokio::spawn(async move {
            loop {
                tokio::time::sleep(period).await;
                if let Err(error) = snapshot().await {
                    tracing::warn!(%error, "periodic snapshot failed; will retry next period");
                }
            }
        }))
    }
}

impl Drop for SnapshotLoop {
    fn drop(&mut self) {
        self.0.abort();
    }
}
