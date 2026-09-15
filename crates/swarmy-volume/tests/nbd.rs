#![cfg(target_os = "linux")]

use object_store::memory::InMemory;
use std::{
    fs::File,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::Duration,
};
use swarmy_core::CHUNK_SIZE;
use swarmy_volume::{ChunkStore, Manifest, ManifestBuilder, VolumeDevice, kernel::Attachment};

struct Cleanup {
    mount: PathBuf,
    mounted: bool,
    attachment: Option<Attachment>,
    _reservation: File,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        if self.mounted {
            let result = Command::new("umount").arg(&self.mount).status();
            if !result.is_ok_and(|status| status.success()) {
                tracing::error!(path = %self.mount.display(), "test cleanup could not unmount");
            }
        }
        self.attachment.take();
    }
}

async fn command(program: &str, args: &[&str]) -> String {
    let output = tokio::time::timeout(
        Duration::from_secs(120),
        tokio::process::Command::new(program)
            .args(args)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap_or_else(|_| panic!("{program} timed out"))
    .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    assert!(
        output.status.success(),
        "{program} {args:?}: {stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    stdout
}

fn available_device() -> (String, File) {
    for index in 0..16 {
        let reservation = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(format!("/tmp/swarmy-nbd-test-{index}.lock"))
            .unwrap();
        if fs2::FileExt::try_lock_exclusive(&reservation).is_ok()
            && !Path::new(&format!("/sys/class/block/nbd{index}/pid")).exists()
            && std::fs::read_to_string(format!("/sys/class/block/nbd{index}/size"))
                .is_ok_and(|size| size.trim() == "0")
        {
            return (format!("/dev/nbd{index}"), reservation);
        }
    }
    panic!("no unused NBD device available");
}

async fn assert_detached(path: &str) {
    let name = Path::new(path).file_name().unwrap().to_str().unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if !Path::new(&format!("/sys/class/block/{name}/pid")).exists()
                && std::fs::read_to_string(format!("/sys/class/block/{name}/size"))
                    .unwrap()
                    .trim()
                    == "0"
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("stale NBD attachment");
}

async fn write_files(mount: &Path) -> Vec<PathBuf> {
    use tokio::io::AsyncWriteExt;
    let mut remaining = 200 * 1024 * 1024;
    let sizes = [
        4096,
        17 * 1024,
        65537,
        1024 * 1024,
        7 * 1024 * 1024,
        32 * 1024 * 1024,
    ];
    let mut files = Vec::new();
    while remaining > 0 {
        let length = sizes[files.len() % sizes.len()].min(remaining);
        let path = mount.join(format!("file-{:04}", files.len()));
        let mut file = tokio::fs::File::create(&path).await.unwrap();
        let mut data = vec![0; length];
        let mut hasher = blake3::Hasher::new();
        hasher.update(&files.len().to_le_bytes());
        hasher.finalize_xof().fill(&mut data);
        file.write_all(&data).await.unwrap();
        file.sync_all().await.unwrap();
        files.push(path);
        remaining -= length;
    }
    files
}

async fn checksums(files: &[PathBuf]) -> String {
    let args: Vec<_> = files.iter().map(|file| file.to_str().unwrap()).collect();
    command("sha256sum", &args).await
}

async fn sequential_comparison(path: &str, cleanup: &mut Cleanup, root: &Path) {
    let memory = Arc::new(InMemory::new());
    let chunks = ChunkStore::new(memory.clone());
    let mut builder =
        ManifestBuilder::new(memory, Manifest::empty(64 * u64::from(CHUNK_SIZE)).unwrap());
    for index in 0..64_u8 {
        let hash = chunks
            .put_chunk(&vec![index + 1; CHUNK_SIZE as usize])
            .await
            .unwrap()
            .hash;
        builder.set_chunk(u64::from(index), hash).unwrap();
    }
    let manifest = builder.build().await.unwrap();
    let mut stats = Vec::new();
    for ahead in [0, 4] {
        let device = VolumeDevice::open(
            chunks.clone(),
            manifest.clone(),
            root.join(format!("seq-cache-{ahead}")),
            root.join(format!("seq-dirty-{ahead}")),
            ahead,
        )
        .await
        .unwrap();
        cleanup.attachment = Some(Attachment::attach(path, Arc::clone(&device)).await.unwrap());
        let output = command(
            "dd",
            &[
                &format!("if={path}"),
                "of=/dev/null",
                "bs=256K",
                "count=64",
                "iflag=direct",
                "status=none",
            ],
        )
        .await;
        assert!(output.is_empty());
        cleanup.attachment.take().unwrap().detach().await.unwrap();
        assert_detached(path).await;
        let measured = device.stats();
        tracing::info!(
            readahead_chunks = ahead,
            ?measured,
            "sequential read statistics"
        );
        stats.push(measured);
    }
    assert!(stats[1].cold_reads < stats[0].cold_reads, "{stats:?}");
    assert!(stats[1].readahead_hits > 0, "{stats:?}");
    assert_eq!(
        stats[0].cold_reads + stats[0].readahead_fetches,
        stats[1].cold_reads + stats[1].readahead_fetches
    );
}

async fn run_fio(mount: &Path) {
    let fio_path = mount.join("fio.bin");
    for mode in ["randwrite", "randread"] {
        let summary = command(
            "fio",
            &[
                "--name=nbd-4k",
                &format!("--filename={}", fio_path.display()),
                &format!("--rw={mode}"),
                "--bs=4k",
                "--size=64m",
                "--direct=1",
                "--ioengine=libaio",
                "--iodepth=16",
                "--runtime=5",
                "--time_based=1",
                "--group_reporting=1",
            ],
        )
        .await;
        tracing::info!(%mode, %summary, "fio completed");
        assert!(summary.contains("err= 0"), "{summary}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ext4_files_fio_detach_and_readahead() {
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::INFO)
        .try_init();
    if command("id", &["-u"]).await.trim() != "0" {
        tracing::warn!(
            "skipping NBD integration test: root is required; execute the built test binary with sudo"
        );
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let (path, reservation) = available_device();
    let mount = root.path().join("mount");
    tokio::fs::create_dir(&mount).await.unwrap();
    let mut cleanup = Cleanup {
        mount: mount.clone(),
        mounted: false,
        attachment: None,
        _reservation: reservation,
    };
    let memory = Arc::new(InMemory::new());
    let chunks = ChunkStore::new(memory);
    let manifest = Manifest::empty(512 * 1024 * 1024).unwrap();
    let cache = root.path().join("cache");
    let dirty = root.path().join("dirty");
    let device = VolumeDevice::open(chunks.clone(), manifest.clone(), &cache, &dirty, 4)
        .await
        .unwrap();
    cleanup.attachment = Some(
        Attachment::attach(&path, Arc::clone(&device))
            .await
            .unwrap(),
    );
    assert!(
        Attachment::attach(&path, Arc::clone(&device))
            .await
            .is_err(),
        "a busy device must not be stolen"
    );
    command("mkfs.ext4", &["-F", "-q", &path]).await;
    command("mount", &[&path, mount.to_str().unwrap()]).await;
    cleanup.mounted = true;
    let files = write_files(&mount).await;
    let before = checksums(&files).await;
    command("umount", &[mount.to_str().unwrap()]).await;
    cleanup.mounted = false;
    command("mount", &[&path, mount.to_str().unwrap()]).await;
    cleanup.mounted = true;
    assert_eq!(checksums(&files).await, before);
    tracing::info!(files = files.len(), bytes = 200 * 1024 * 1024, stats = ?device.stats(), "file checksums match after remount");
    run_fio(&mount).await;
    command("umount", &[mount.to_str().unwrap()]).await;
    cleanup.mounted = false;
    device.flush().await.unwrap();
    cleanup.attachment.take().unwrap().detach().await.unwrap();
    assert_detached(&path).await;
    drop(device);
    let reopened = VolumeDevice::open(chunks, manifest, &cache, &dirty, 0)
        .await
        .unwrap();
    cleanup.attachment = Some(Attachment::attach(&path, reopened).await.unwrap());
    command("mount", &[&path, mount.to_str().unwrap()]).await;
    cleanup.mounted = true;
    assert_eq!(checksums(&files).await, before);
    command("umount", &[mount.to_str().unwrap()]).await;
    cleanup.mounted = false;
    cleanup.attachment.take().unwrap().detach().await.unwrap();
    assert_detached(&path).await;
    sequential_comparison(&path, &mut cleanup, root.path()).await;
    let empty = VolumeDevice::open(
        ChunkStore::new(Arc::new(InMemory::new())),
        Manifest::empty(u64::from(CHUNK_SIZE)).unwrap(),
        root.path().join("drop-cache"),
        root.path().join("drop-dirty"),
        0,
    )
    .await
    .unwrap();
    // No I/O intervenes: this catches the startup/disconnect race and exercises
    // the same Drop fallback used by failed test cleanup.
    drop(Attachment::attach(&path, empty).await.unwrap());
    assert_detached(&path).await;
}
