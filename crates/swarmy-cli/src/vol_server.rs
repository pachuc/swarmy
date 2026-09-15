use anyhow::{Context, Result, ensure};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use swarmy_core::{LeaseOwnerId, ManifestId, NodeId, VolumeId};
use swarmy_volume::{ChunkStore, Manifest, VolumeDevice, VolumeWriter, kernel::Attachment};
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
    manifest_id: Option<ManifestId>,
    error: Option<String>,
}

fn directory(loaded: &swarmy_config::Loaded) -> PathBuf {
    loaded.root.join(".swarmy/volumes")
}

pub async fn control(id: VolumeId, mount: Option<PathBuf>, detach: bool, json: bool) -> Result<()> {
    let loaded = swarmy_config::Settings::load()?;
    let request = Request {
        node: loaded.node_id()?,
        mount,
        detach,
    };
    let mut socket = UnixStream::connect(directory(&loaded).join(format!("{id}.sock"))).await
        .context("no local attach server; run swarmy vol attach on this node first (control commands may require sudo)")?;
    socket.write_all(&serde_json::to_vec(&request)?).await?;
    socket.write_all(b"\n").await?;
    let mut line = String::new();
    BufReader::new(socket.take(65536))
        .read_line(&mut line)
        .await?;
    let reply: Reply = serde_json::from_str(&line)?;
    if let Some(error) = reply.error {
        anyhow::bail!(error);
    }
    let manifest = reply
        .manifest_id
        .context("attach server returned no manifest")?;
    crate::vol::output(
        &serde_json::json!({"volume_id": id, "manifest_id": manifest, "detached": detach}),
        &manifest.to_string(),
        json,
    )
}

struct SocketGuard(PathBuf);
impl Drop for SocketGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

pub async fn attach(
    id: VolumeId,
    path: Option<PathBuf>,
    background: bool,
    json: bool,
) -> Result<()> {
    ensure!(
        rustix::process::geteuid().is_root(),
        "vol attach requires root; run it with sudo -E"
    );
    let loaded = swarmy_config::Settings::load()?;
    let node = loaded.node_id()?;
    let dir = directory(&loaded);
    std::fs::create_dir_all(&dir)?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(dir.join(format!("{id}.lock")))?;
    fs2::FileExt::try_lock_exclusive(&lock).context("volume already has a local attach server")?;
    let socket_path = dir.join(format!("{id}.sock"));
    match std::fs::remove_file(&socket_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let listener = UnixListener::bind(&socket_path)?;
    let _socket_guard = SocketGuard(socket_path);
    let store = crate::conversation::store().await?;
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
        id,
        node,
        path,
        background,
        json,
        listener,
        (&loaded, &store, &lease),
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

// Group immutable setup inputs so the server's lifecycle stays in one place.
async fn serve(
    id: VolumeId,
    node: NodeId,
    path: Option<PathBuf>,
    background: bool,
    json: bool,
    listener: UnixListener,
    config: (
        &swarmy_config::Loaded,
        &swarmy_store::Store,
        &swarmy_core::Lease,
    ),
) -> Result<()> {
    let (loaded, store, lease) = config;
    let settings = &loaded.settings;
    let objects = Arc::new(
        object_store::aws::AmazonS3Builder::new()
            .with_endpoint(&settings.s3_endpoint)
            .with_access_key_id(&settings.s3_access_key)
            .with_secret_access_key(&settings.s3_secret_key)
            .with_bucket_name(&settings.s3_bucket)
            .with_region(&settings.s3_region)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .build()?,
    );
    let record = store.get_volume(id).await?.context("volume not found")?;
    let header = store
        .get_manifest(record.head_manifest)
        .await?
        .context("manifest missing")?;
    let manifest = Manifest::load(&*objects, header).await?;
    // A new grant must discard every write after the last committed snapshot,
    // including local writes from a previous server on this same machine.
    let dirty = tempfile::Builder::new()
        .prefix(&format!("{id}-"))
        .tempdir_in(directory(loaded))?;
    let device = VolumeDevice::open(
        ChunkStore::new(objects),
        manifest,
        directory(loaded).join("cache"),
        dirty.path(),
        4,
    )
    .await?;
    let writer = VolumeWriter::new(
        device.clone(),
        store.clone(),
        id,
        lease.clone(),
        record.head_manifest,
    );
    let (path, attachment) = attach_kernel(path, device).await?;
    let mut attachment = Some(attachment);
    let _background = background.then(|| writer.background(Duration::from_millis(250)));
    let (lost_tx, mut lost_rx) = tokio::sync::oneshot::channel();
    let renew_writer = writer.clone();
    let renewal = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(15)).await;
            if let Err(error) = renew_writer.renew(LEASE_DURATION).await {
                let _ = lost_tx.send(error);
                break;
            }
        }
    });
    let _renewal = AbortTask(renewal);
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    crate::vol::output(
        &serde_json::json!({"volume_id": id, "node_id": node, "device": path}),
        &path.display().to_string(),
        json,
    )?;
    loop {
        tokio::select! {
            result = listener.accept() => {
                let (socket, _) = result?;
                if handle(socket, node, &path, &writer, &mut attachment).await? { break; }
            }
            error = &mut lost_rx => { anyhow::bail!("writer lease lost; device disconnected: {}", error?); }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                finish(&path, &writer, &mut attachment).await?;
                break;
            }
            _ = terminate.recv() => {
                finish(&path, &writer, &mut attachment).await?;
                break;
            }
        }
    }
    Ok(())
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
    anyhow::bail!("no usable /dev/nbdX; load the nbd module and check that a device is free")
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
        Ok::<_, anyhow::Error>((manifest, request.detach))
    }
    .await;
    let (reply, detached) = match result {
        Ok((manifest, detached)) => (
            Reply {
                manifest_id: Some(manifest),
                error: None,
            },
            detached,
        ),
        Err(error) => (
            Reply {
                manifest_id: None,
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
        .context("invalid findmnt response")?;
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
                .context("invalid mount target")
        })
        .transpose()
}

async fn finish(
    path: &Path,
    writer: &VolumeWriter,
    attachment: &mut Option<Attachment>,
) -> Result<ManifestId> {
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
        .context("device already disconnected")?
        .detach()
        .await?;
    writer.release().await?;
    Ok(manifest)
}
