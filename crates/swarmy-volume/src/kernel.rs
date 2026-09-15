//! Linux NBD attachment. Only the ioctl wrapper uses unsafe code because Linux
//! exposes this block-device interface through variadic C system calls.
use crate::{BLOCK_SIZE, VolumeDevice, nbd};
use std::{
    fs::{File, OpenOptions},
    io,
    os::{fd::AsRawFd, unix::fs::MetadataExt},
    path::Path,
    sync::Arc,
    thread::JoinHandle,
};
use tokio::net::UnixStream;

const SET_SOCK: libc::c_ulong = 0xab00;
const SET_BLKSIZE: libc::c_ulong = 0xab01;
const DO_IT: libc::c_ulong = 0xab03;
const CLEAR_SOCK: libc::c_ulong = 0xab04;
const SET_SIZE_BLOCKS: libc::c_ulong = 0xab07;
const DISCONNECT: libc::c_ulong = 0xab08;
const SET_TIMEOUT: libc::c_ulong = 0xab09;
const SET_FLAGS: libc::c_ulong = 0xab0a;

// The kernel requires ioctl; all arguments here are integer values, not pointers.
#[allow(unsafe_code)]
fn ioctl(file: &File, request: libc::c_ulong, value: libc::c_ulong) -> io::Result<()> {
    // SAFETY: file holds a live descriptor throughout the call. These NBD
    // requests accept an integer or no argument and never dereference value.
    if unsafe { libc::ioctl(file.as_raw_fd(), request, value) } < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// An attached kernel device. Unmount before detaching or dropping this handle.
/// Drop disconnects and joins the kernel thread even during error unwinding.
pub struct Attachment {
    file: Arc<File>,
    thread: Option<JoinHandle<io::Result<()>>>,
    server: Option<tokio::task::JoinHandle<io::Result<()>>>,
    socket: std::os::unix::net::UnixStream,
    connected: bool,
}

impl Attachment {
    /// Connect an unused `/dev/nbdX` to an in-process async server.
    /// Requires root and a running Tokio runtime until detach completes.
    /// # Errors
    /// Returns negotiation, thread creation, or kernel configuration failures.
    pub async fn attach(path: impl AsRef<Path>, device: Arc<VolumeDevice>) -> io::Result<Self> {
        let size = device.size();
        let blocks = libc::c_ulong::try_from(size / BLOCK_SIZE)
            .map_err(|_| io::Error::other("disk too large for kernel"))?;
        let file = Arc::new(OpenOptions::new().read(true).write(true).open(path)?);
        fs2::FileExt::try_lock_exclusive(&*file)?;
        let sysfs = sysfs_path(&file)?;
        if sysfs.join("pid").exists()
            || tokio::fs::read_to_string(sysfs.join("size")).await?.trim() != "0"
        {
            return Err(io::Error::from_raw_os_error(libc::EBUSY));
        }
        let (mut client, server_socket) = UnixStream::pair()?;
        let server = tokio::spawn(async move {
            let result = nbd::serve_connection(server_socket, device).await;
            if let Err(error) = &result {
                tracing::error!(%error, "attached NBD server failed");
            }
            result
        });
        if let Err(error) = nbd::negotiate_kernel(&mut client, size).await {
            server.abort();
            return Err(error);
        }
        let socket = client.into_std()?;
        socket.set_nonblocking(false)?;
        let retained_socket = socket.try_clone()?;
        if let Err(error) = ioctl(
            &file,
            SET_SOCK,
            libc::c_ulong::try_from(socket.as_raw_fd()).map_err(io::Error::other)?,
        ) {
            server.abort();
            return Err(error);
        }
        let mut attachment = Self {
            file,
            thread: None,
            server: Some(server),
            socket: retained_socket,
            connected: true,
        };
        ioctl(
            &attachment.file,
            SET_BLKSIZE,
            libc::c_ulong::try_from(BLOCK_SIZE).map_err(io::Error::other)?,
        )?;
        ioctl(&attachment.file, SET_SIZE_BLOCKS, blocks)?;
        ioctl(&attachment.file, SET_FLAGS, nbd::EXPORT_FLAGS.into())?;
        // A failed server must not leave filesystem callers blocked forever.
        ioctl(&attachment.file, SET_TIMEOUT, 30)?;
        let kernel_file = Arc::clone(&attachment.file);
        attachment.thread = Some(
            std::thread::Builder::new()
                .name("swarmy-nbd".into())
                .spawn(move || {
                    let _socket = socket;
                    let result = ioctl(&kernel_file, DO_IT, 0);
                    // Linux reports EPIPE on an ordinary protocol disconnect.
                    match result {
                        Err(error) if error.raw_os_error() == Some(libc::EPIPE) => Ok(()),
                        result => result,
                    }
                })?,
        );
        attachment.wait_ready(size).await?;
        Ok(attachment)
    }

    /// Disconnect, wait for the kernel loop, and clear the socket for reuse.
    /// Run after unmount; local flush must complete before calling this method.
    /// # Errors
    /// Returns kernel errors or a failed kernel thread.
    pub async fn detach(mut self) -> io::Result<()> {
        let (mut attachment, result) = tokio::task::spawn_blocking(move || {
            let result = self.disconnect();
            (self, result)
        })
        .await
        .map_err(io::Error::other)?;
        let server_result = if let Some(server) = attachment.server.take() {
            match server.await {
                Ok(result) => result,
                Err(error) if error.is_cancelled() => Ok(()),
                Err(error) => Err(io::Error::other(error)),
            }
        } else {
            Ok(())
        };
        result.and(server_result)
    }

    async fn wait_ready(&self, size: u64) -> io::Result<()> {
        let sysfs = sysfs_path(&self.file)?;
        // NBD_DO_IT publishes capacity asynchronously. Returning sooner lets
        // mkfs observe a zero-sized disk and makes immediate detach race startup.
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if self.thread.as_ref().is_some_and(JoinHandle::is_finished) {
                    return Err(io::Error::other("NBD kernel loop exited during startup"));
                }
                if sysfs.join("pid").exists()
                    && tokio::fs::read_to_string(sysfs.join("size"))
                        .await?
                        .trim()
                        .parse::<u64>()
                        .ok()
                        == Some(size / 512)
                {
                    return Ok(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "NBD startup timed out"))?
    }

    fn disconnect(&mut self) -> io::Result<()> {
        if !self.connected {
            return Ok(());
        }
        let disconnect = ioctl(&self.file, DISCONNECT, 0);
        // Cancel before closing the transport so intentional shutdown does not
        // get reported as a server failure while a kernel probe is in flight.
        if let Some(server) = &self.server {
            server.abort();
        }
        // Also close the transport if DO_IT failed or has not entered yet.
        let _ = self.socket.shutdown(std::net::Shutdown::Both);
        // Clear queued requests before joining: a failed request can hold the
        // block device open while DO_IT waits to reset its capacity.
        let clear = ioctl(&self.file, CLEAR_SOCK, 0);
        let joined = self.thread.take().map_or(Ok(()), |thread| {
            thread
                .join()
                .map_err(|_| io::Error::other("NBD kernel thread panicked"))?
        });
        // Release the local reservation before returning the device for reuse.
        let unlock = fs2::FileExt::unlock(&*self.file);
        self.connected = false;
        disconnect.and(joined).and(clear).and(unlock)
    }
}

impl Drop for Attachment {
    fn drop(&mut self) {
        if let Err(error) = self.disconnect() {
            tracing::error!(%error, "NBD detach failed");
        }
    }
}

fn sysfs_path(file: &File) -> io::Result<std::path::PathBuf> {
    let rdev = file.metadata()?.rdev();
    Ok(std::path::PathBuf::from(format!(
        "/sys/dev/block/{}:{}",
        libc::major(rdev),
        libc::minor(rdev)
    )))
}
