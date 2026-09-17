use super::*;
use swarmy_core::{LeaseOwnerId, ManifestId, VolumeId};
use swarmy_volume::VolumeWriter;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn continuous_writes_and_retained_crash_images() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    if command("id", &["-u"]).await.trim() != "0" {
        eprintln!("skipping snapshot NBD test: execute the built binary with sudo");
        return;
    }
    let Ok(cluster) = std::env::var("SWARMY_FDB_CLUSTER_FILE") else {
        eprintln!("skipping snapshot NBD test: SWARMY_FDB_CLUSTER_FILE is unset");
        return;
    };
    let _network = swarmy_store::boot();
    let store = swarmy_store::Store::open(
        Some(&cluster),
        Some(&[format!("boundary-nbd-{}", ulid::Ulid::generate())]),
        Arc::new(swarmy_store::blob::MemoryBlobStore::default()),
    )
    .await
    .unwrap();
    let root = tempfile::tempdir().unwrap();
    let (path, reservation) = available_device();
    let mount = root.path().join("mount");
    std::fs::create_dir(&mount).unwrap();
    let mut cleanup = Cleanup {
        mount: mount.clone(),
        mounted: false,
        attachment: None,
        _reservation: reservation,
    };
    let objects = Arc::new(InMemory::new());
    let chunks = ChunkStore::new(objects.clone());
    let base = Manifest::empty(256 * 1024 * 1024).unwrap();
    let base_id = ManifestId::from_ulid(ulid::Ulid::generate());
    let volume = VolumeId::from_ulid(ulid::Ulid::generate());
    store.put_manifest(base_id, base.header()).await.unwrap();
    store.create_volume(volume, base_id).await.unwrap();
    let now = jiff::Timestamp::now();
    let lease = store
        .acquire_writer_lease(
            volume,
            LeaseOwnerId::from_ulid(ulid::Ulid::generate()),
            now,
            now.checked_add(Duration::from_secs(300)).unwrap(),
        )
        .await
        .unwrap();
    let device = VolumeDevice::open(
        chunks.clone(),
        base,
        root.path().join("cache"),
        root.path().join("dirty"),
        0,
    )
    .await
    .unwrap();
    let writer = VolumeWriter::with_retention(
        device.clone(),
        store.clone(),
        volume,
        lease,
        base_id,
        std::num::NonZeroUsize::new(4).unwrap(),
    );
    cleanup.attachment = Some(Attachment::attach(&path, device.clone()).await.unwrap());
    let block_gap = measure_block_writes(&writer, &device, &path, root.path()).await;
    command("mkfs.ext4", &["-F", "-q", &path]).await;
    command("mount", &[&path, mount.to_str().unwrap()]).await;
    cleanup.mounted = true;
    let stable = mount.join("boundary-data");
    std::fs::write(&stable, vec![73; 1024 * 1024]).unwrap();
    command("sync", &["-f", mount.to_str().unwrap()]).await;
    let expected = checksums(std::slice::from_ref(&stable)).await;
    // Publish formatting and initial data before measuring ongoing snapshots.
    let clean = writer.flush(Some(&mount)).await.unwrap();
    assert!(clean.frozen > Duration::ZERO);
    let writer_gap = measure_writes(&writer, &mount, root.path()).await;
    command("umount", &[mount.to_str().unwrap()]).await;
    cleanup.mounted = false;
    cleanup.attachment.take().unwrap().detach().await.unwrap();
    restore_snapshots(
        &store,
        volume,
        objects,
        root.path(),
        &mut cleanup,
        &path,
        (&stable, &expected),
    )
    .await;
    writer.release().await.unwrap();
    // Verify every retained image before reporting a latency failure.
    assert!(
        block_gap < 5_000_000,
        "direct write exceeds 5 ms: {block_gap} ns"
    );
    assert!(
        writer_gap < 30_000_000,
        "writer gap exceeds 30 ms: {writer_gap} ns"
    );
    eprintln!(
        "all four retained crash images mounted, ran Bash checks, matched boundary-data SHA-256, and passed e2fsck"
    );
}

async fn restore_snapshots(
    store: &swarmy_store::Store,
    volume: VolumeId,
    objects: Arc<InMemory>,
    root: &Path,
    cleanup: &mut Cleanup,
    path: &str,
    expected: (&Path, &str),
) {
    let chunks = ChunkStore::new(objects.clone());
    let mount = cleanup.mount.clone();
    let (stable, expected) = expected;
    let retained = store.volume_snapshots(volume).await.unwrap();
    assert_eq!(retained.len(), 4);
    for (index, snapshot) in retained.iter().enumerate() {
        let header = store.get_manifest(*snapshot).await.unwrap().unwrap();
        let manifest = Manifest::load(&*objects, header).await.unwrap();
        let restored = VolumeDevice::open(
            chunks.clone(),
            manifest,
            root.join(format!("cache-{index}")),
            root.join(format!("dirty-{index}")),
            0,
        )
        .await
        .unwrap();
        cleanup.attachment = Some(Attachment::attach(&path, restored).await.unwrap());
        command("mount", &[path, mount.to_str().unwrap()]).await;
        cleanup.mounted = true;
        assert_eq!(checksums(&[stable.to_path_buf()]).await, expected);
        command(
            "bash",
            &[
                "-c",
                "test -s \"$1/changing\" && test -s \"$1/timestamps\"",
                "boundary",
                mount.to_str().unwrap(),
            ],
        )
        .await;
        command("umount", &[mount.to_str().unwrap()]).await;
        cleanup.mounted = false;
        command("e2fsck", &["-f", "-n", path]).await;
        cleanup.attachment.take().unwrap().detach().await.unwrap();
    }
}

async fn measure_writes(writer: &VolumeWriter, mount: &Path, root: &Path) -> u64 {
    // Same monotonic timestamp/fsync loop as the persistent-volume benchmark,
    // with continuous direct overwrites across 8 MiB to exercise copy-on-write.
    // Allocate before timing so extent allocation is not counted as a pause.
    std::fs::write(mount.join("changing"), vec![0; 8 * 1024 * 1024]).unwrap();
    command("sync", &["-f", mount.to_str().unwrap()]).await;
    let script = r"
import os, sys, time, threading, json, mmap
root, stop, output = sys.argv[1:]
def heavy():
    f = os.open(root + '/changing', os.O_RDWR | os.O_DIRECT)
    buf = mmap.mmap(-1, 65536)
    offset = 0
    try:
        while not os.path.exists(stop):
            buf[:] = os.urandom(65536)
            os.pwrite(f, buf, offset)
            offset = (offset + len(buf)) % (8 * 1024 * 1024)
    finally:
        os.close(f)
t = threading.Thread(target=heavy); t.start()
times = []
with open(root + '/timestamps', 'ab', buffering=0) as f:
    while not os.path.exists(stop):
        f.write((str(time.monotonic_ns()) + '\n').encode()); os.fsync(f.fileno())
        times.append(time.monotonic_ns()); time.sleep(.01)
t.join()
json.dump(times, open(output, 'w'))
";
    let stop = root.join("stop");
    let output = root.join("times.json");
    let mut workload = tokio::process::Command::new("python3")
        .arg("-c")
        .arg(script)
        .arg(mount)
        .arg(&stop)
        .arg(&output)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    for _ in 0..4 {
        let result = writer.flush(None).await.unwrap();
        assert_eq!(result.frozen, Duration::ZERO);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    std::fs::write(stop, []).unwrap();
    assert!(workload.wait().await.unwrap().success());
    let times: Vec<u64> = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
    let gap = times.windows(2).map(|w| w[1] - w[0]).max().unwrap();
    eprintln!(
        "snapshot writer: observations={}, largest_gap_ms={:.3}",
        times.len(),
        Duration::from_nanos(gap).as_secs_f64() * 1000.0
    );
    // Include the intentional 10 ms cadence and ordinary fsync/scheduler cost.
    gap
}

async fn measure_block_writes(
    writer: &VolumeWriter,
    device: &VolumeDevice,
    path: &str,
    root: &Path,
) -> u64 {
    for number in 0..64_u8 {
        device
            .write(
                u64::from(number) * u64::from(CHUNK_SIZE),
                &vec![number + 1; CHUNK_SIZE as usize],
            )
            .await
            .unwrap();
    }
    let stop = root.join("block-stop");
    let output = root.join("block-times.json");
    let script = r"
import os, sys, time, mmap, json
path, stop, output = sys.argv[1:]
f = os.open(path, os.O_RDWR | os.O_DIRECT)
buf = mmap.mmap(-1, 4096); buf[:] = b'x' * 4096
times = []
while not os.path.exists(stop):
    start = time.monotonic_ns()
    os.pwrite(f, buf, 128 * 1024 * 1024)
    times.append(time.monotonic_ns() - start)
    time.sleep(.001)
os.close(f)
json.dump(times, open(output, 'w'))
";
    let mut workload = tokio::process::Command::new("python3")
        .arg("-c")
        .arg(script)
        .arg(path)
        .arg(&stop)
        .arg(&output)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    for _ in 0..4 {
        writer.flush(None).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    std::fs::write(stop, []).unwrap();
    assert!(workload.wait().await.unwrap().success());
    let times: Vec<u64> = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
    let largest = *times.iter().max().unwrap();
    eprintln!(
        "direct NBD writer: observations={}, largest_write_ms={:.3}",
        times.len(),
        Duration::from_nanos(largest).as_secs_f64() * 1000.0
    );
    largest
}
