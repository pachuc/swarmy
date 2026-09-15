//! Fixed-newstyle NBD over Unix sockets, with bounded simple replies.
mod wire;

use crate::VolumeDevice;
use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::net::{UnixListener, UnixStream};
pub(crate) use wire::{EXPORT_FLAGS, negotiate_kernel};

/// A single unnamed export. Dropping the listener removes its socket path.
pub struct NbdServer {
    listener: UnixListener,
    path: PathBuf,
    device: Arc<VolumeDevice>,
}

impl NbdServer {
    /// Bind a fresh socket path. Existing paths are never removed on bind.
    /// # Errors
    /// Returns Unix socket errors.
    pub fn bind(path: impl AsRef<Path>, device: Arc<VolumeDevice>) -> io::Result<Self> {
        let path = path.as_ref().to_owned();
        Ok(Self {
            listener: UnixListener::bind(&path)?,
            path,
            device,
        })
    }

    /// Serve connections in order. One connection owns the export at a time.
    /// Cancel this future and drop the server to stop listening.
    /// # Errors
    /// Returns listener errors; failed clients are logged and disconnected.
    pub async fn run(&self) -> io::Result<()> {
        loop {
            let (stream, _) = self.listener.accept().await?;
            if let Err(error) = serve_connection(stream, Arc::clone(&self.device)).await {
                tracing::warn!(%error, "NBD client disconnected with an error");
            }
        }
    }
}

impl Drop for NbdServer {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Negotiate and serve one client, including a socketpair used for attachment.
/// # Errors
/// Returns malformed protocol and socket errors. Device errors become NBD errno replies.
pub async fn serve_connection(mut stream: UnixStream, device: Arc<VolumeDevice>) -> io::Result<()> {
    if wire::handshake(&mut stream, device.size()).await? {
        wire::transmission(&mut stream, &device).await?;
    }
    Ok(())
}
