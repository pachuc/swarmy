use super::{ChunkStore, FlushResult, Manifest, VolumeDevice, VolumeWriter, kernel::Attachment};
pub use error::{Error, Result};
mod error;

macro_rules! ensure {
    ($condition:expr, $($message:tt)*) => {
        if !$condition { return Err(Error::Message(format!($($message)*))); }
    };
}
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use swarmy_core::{LeaseOwnerId, ManifestId, NodeId, VolumeId};

use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
};

const LEASE_DURATION: Duration = Duration::from_secs(60);

#[derive(Serialize, Deserialize)]
struct Request {
    node: NodeId,
    mount: Option<PathBuf>,
    detach: bool,
}
#[derive(Serialize, Deserialize)]
struct Reply {
    flush: Option<FlushResult>,
    error: Option<String>,
}

/// Shared attachment inputs. Both the CLI and swarmyd serve disks in process.
#[derive(Clone)]
pub struct ServerConfig {
    pub directory: PathBuf,
    pub node: NodeId,
    pub store: swarmy_store::Store,
    pub objects: Arc<dyn object_store::ObjectStore>,
}

/// Send a flush or detach request to a local attachment.
/// # Errors
/// Returns transport, publication, or attachment errors.
pub async fn control(
    config: &ServerConfig,
    id: VolumeId,
    mount: Option<PathBuf>,
    detach: bool,
) -> Result<ManifestId> {
    Ok(control_flush(config, id, mount, detach).await?.manifest_id)
}

/// Request an immediate checkpoint through the attachment's control socket.
/// This uses the same fenced publication protocol as the legacy flush request.
/// # Errors
/// Returns transport, publication, or attachment errors.
pub async fn checkpoint(
    config: &ServerConfig,
    id: VolumeId,
    mount: Option<PathBuf>,
) -> Result<ManifestId> {
    control(config, id, mount, false).await
}

/// Send a control request and return publication timings and counters.
/// # Errors
/// Returns transport, publication, or attachment errors.
pub async fn control_flush(
    config: &ServerConfig,
    id: VolumeId,
    mount: Option<PathBuf>,
    detach: bool,
) -> Result<FlushResult> {
    let request = Request {
        node: config.node,
        mount,
        detach,
    };
    let mut socket = UnixStream::connect(config.directory.join(format!("{id}.sock"))).await?;
    socket.write_all(&serde_json::to_vec(&request)?).await?;
    socket.write_all(b"\n").await?;
    let mut line = String::new();
    BufReader::new(socket.take(65536))
        .read_line(&mut line)
        .await?;
    let reply: Reply = serde_json::from_str(&line)?;
    if let Some(error) = reply.error {
        return Err(Error::Message(error));
    }
    reply
        .flush
        .ok_or_else(|| Error::Message("attach server returned no manifest".into()))
}

struct SocketGuard(PathBuf);
impl Drop for SocketGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Serve one kernel attachment until detached or shutdown is requested.
/// The ready callback runs only after the kernel has published its capacity.
/// # Errors
/// Returns setup, lease, control transport, or final flush errors.
pub async fn attach(
    config: ServerConfig,
    id: VolumeId,
    path: Option<PathBuf>,
    background: bool,
    ready: impl FnOnce(&Path) -> Result<()> + Send,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<()> {
    let node = config.node;
    let dir = &config.directory;
    std::fs::create_dir_all(dir)?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(dir.join(format!("{id}.lock")))?;
    fs2::FileExt::try_lock_exclusive(&lock)?;
    let socket_path = dir.join(format!("{id}.sock"));
    match std::fs::remove_file(&socket_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let listener = UnixListener::bind(&socket_path)?;
    let _socket_guard = SocketGuard(socket_path);
    let store = &config.store;
    let now = Timestamp::now();
    let lease = store
        .acquire_writer_lease(
            id,
            LeaseOwnerId::from_ulid(node.as_ulid()),
            now,
            now.checked_add(LEASE_DURATION)?,
        )
        .await?;
    let result = serve(
        &config,
        id,
        path,
        background,
        listener,
        (&lease, ready, shutdown),
    )
    .await;
    // Setup errors must not strand a lease. The running server releases its
    // renewed token itself, so this fallback can only release the initial grant.
    if result.is_err() {
        let _ = store
            .release_writer_lease(id, &lease, Timestamp::now())
            .await;
    }
    result
}

// Keep renewal, control, and shutdown in one lifecycle for all callers.
async fn serve(
    config: &ServerConfig,
    id: VolumeId,
    path: Option<PathBuf>,
    background: bool,
    listener: UnixListener,
    lifecycle: (
        &swarmy_core::Lease,
        impl FnOnce(&Path) -> Result<()> + Send,
        impl Future<Output = ()> + Send,
    ),
) -> Result<()> {
    let (lease, ready, shutdown) = lifecycle;
    let store = &config.store;
    let node = config.node;
    let objects = config.objects.clone();
    let record = store
        .get_volume(id)
        .await?
        .ok_or_else(|| Error::Message("volume not found".into()))?;
    let header = store
        .get_manifest(record.head_manifest)
        .await?
        .ok_or_else(|| Error::Message("manifest missing".into()))?;
    let manifest = Manifest::load(&*objects, header).await?;
    // A new grant must discard every write after the last committed snapshot,
    // including local writes from a previous server on this same machine.
    let dirty = tempfile::Builder::new()
        .prefix(&format!("{id}-"))
        .tempdir_in(&config.directory)?;
    let device = VolumeDevice::open(
        ChunkStore::new(objects),
        manifest,
        config.directory.join("cache"),
        dirty.path(),
        4,
    )
    .await?;
    let policy = swarmy_config::Settings::load()
        .map_err(|error| Error::Message(error.to_string()))?
        .settings
        .volume_snapshots;
    let writer = VolumeWriter::with_retention(
        device.clone(),
        store.clone(),
        id,
        lease.clone(),
        record.head_manifest,
        policy.retention,
    );
    let (path, attachment) = attach_kernel(path, device.clone()).await?;
    let mut attachment = Some(attachment);
    let _background = background.then(|| writer.background(Duration::from_millis(250)));
    // Hold this gate through mount discovery, freeze, and detach so a periodic
    // publication cannot race unmounting or the final publication.
    let operations = Arc::new(tokio::sync::Mutex::new(()));
    let snapshots = start_snapshots(
        writer.clone(),
        device,
        path.clone(),
        operations.clone(),
        policy,
    );
    let (_renewal, mut lost_rx) = start_renewal(writer.clone());
    ready(&path)?;
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            result = listener.accept() => {
                let (socket, _) = result?;
                let _operation = operations.lock().await;
                if handle(socket, node, &path, &writer, &mut attachment).await? {
                    drop(snapshots);
                    break;
                }
            }
            error = &mut lost_rx => {
                let _operation = operations.lock().await;
                drop(snapshots);
                return Err(Error::Message(format!("writer lease lost; device disconnected: {}", error?)));
            }
            () = &mut shutdown => {
                let _operation = operations.lock().await;
                // Cancel while holding the gate; no next tick can start after
                // final publication and race lease release or device teardown.
                drop(snapshots);
                finish(&path, &writer, &mut attachment).await?;
                break;
            }
        }
    }
    Ok(())
}

fn start_snapshots(
    writer: Arc<VolumeWriter>,
    device: Arc<VolumeDevice>,
    path: PathBuf,
    operations: Arc<tokio::sync::Mutex<()>>,
    policy: swarmy_config::VolumeSnapshots,
) -> crate::SnapshotLoop {
    crate::SnapshotLoop::spawn(
        Duration::from_secs(policy.period_seconds.get()),
        move || {
            let operations = operations.clone();
            let writer = writer.clone();
            let path = path.clone();
            let device = device.clone();
            async move {
                let _operation = operations.lock().await;
                if device.has_unpublished_changes().await {
                    let mount = mountpoint(&path).await?;
                    writer.flush_if_dirty(mount.as_deref()).await?;
                }
                Ok::<_, Error>(())
            }
        },
    )
}

fn start_renewal(
    writer: Arc<VolumeWriter>,
) -> (
    AbortTask,
    tokio::sync::oneshot::Receiver<crate::VolumeError>,
) {
    let (lost_tx, lost_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(15)).await;
            if let Err(error) = writer.renew(LEASE_DURATION).await {
                let _ = lost_tx.send(error);
                break;
            }
        }
    });
    (AbortTask(task), lost_rx)
}

struct AbortTask(tokio::task::JoinHandle<()>);
impl Drop for AbortTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn attach_kernel(
    path: Option<PathBuf>,
    device: Arc<VolumeDevice>,
) -> Result<(PathBuf, Attachment)> {
    if let Some(path) = path {
        let attachment = Attachment::attach(&path, device).await?;
        return Ok((path, attachment));
    }
    for index in 0..16 {
        let path = PathBuf::from(format!("/dev/nbd{index}"));
        if let Ok(attachment) = Attachment::attach(&path, device.clone()).await {
            return Ok((path, attachment));
        }
    }
    Err(Error::Message(
        "no usable /dev/nbdX; load the nbd module and check that a device is free".into(),
    ))
}

async fn handle(
    socket: UnixStream,
    node: NodeId,
    path: &Path,
    writer: &VolumeWriter,
    attachment: &mut Option<Attachment>,
) -> Result<bool> {
    let (read, mut write) = socket.into_split();
    let mut line = String::new();
    let result = async {
        tokio::time::timeout(
            Duration::from_secs(5),
            BufReader::new(read.take(65536)).read_line(&mut line),
        )
        .await??;
        let request: Request = serde_json::from_str(&line)?;
        ensure!(
            request.node == node,
            "writer lease belongs to another node ({node})"
        );
        let manifest = if request.detach {
            finish(path, writer, attachment).await?
        } else {
            let detected = mountpoint(path).await?;
            if let Some(requested) = &request.mount {
                ensure!(
                    detected
                        .as_ref()
                        .is_some_and(|mount| std::fs::canonicalize(mount).ok()
                            == std::fs::canonicalize(requested).ok()),
                    "--mount is not this volume's mount point"
                );
            }
            writer
                .flush(request.mount.as_deref().or(detected.as_deref()))
                .await?
        };
        Ok::<_, Error>((manifest, request.detach))
    }
    .await;
    let (reply, detached) = match result {
        Ok((manifest, detached)) => (
            Reply {
                flush: Some(manifest),
                error: None,
            },
            detached,
        ),
        Err(error) => (
            Reply {
                flush: None,
                error: Some(format!("{error:#}")),
            },
            attachment.is_none(),
        ),
    };
    // A caller may disconnect after submitting a command; completing publication
    // and detach does not depend on whether it receives the reply.
    let _ = write.write_all(&serde_json::to_vec(&reply)?).await;
    let _ = write.write_all(b"\n").await;
    Ok(detached)
}

async fn mountpoint(path: &Path) -> Result<Option<PathBuf>> {
    let output = tokio::process::Command::new("findmnt")
        .args(["--json", "--list", "--source"])
        .arg(path)
        .args(["--output", "TARGET"])
        .output()
        .await?;
    if output.status.code() == Some(1) && output.stdout.is_empty() {
        return Ok(None);
    }
    ensure!(
        output.status.success(),
        "findmnt failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let mounts = value["filesystems"]
        .as_array()
        .ok_or_else(|| Error::Message("invalid findmnt response".into()))?;
    ensure!(
        mounts.len() <= 1,
        "volume has multiple mounts; unmount additional mounts first"
    );
    mounts
        .first()
        .map(|mount| {
            mount["target"]
                .as_str()
                .map(PathBuf::from)
                .ok_or_else(|| Error::Message("invalid mount target".into()))
        })
        .transpose()
}

async fn finish(
    path: &Path,
    writer: &VolumeWriter,
    attachment: &mut Option<Attachment>,
) -> Result<FlushResult> {
    if let Some(mount) = mountpoint(path).await? {
        let output = tokio::process::Command::new("umount")
            .arg("--")
            .arg(mount)
            .output()
            .await?;
        ensure!(
            output.status.success(),
            "unmount failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let manifest = writer.flush(None).await?;
    attachment
        .take()
        .ok_or_else(|| Error::Message("device already disconnected".into()))?
        .detach()
        .await?;
    writer.release().await?;
    Ok(manifest)
}
