#![cfg(target_os = "linux")]
use serde_json::Value;
use std::{
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, OnceLock},
    time::Duration,
};
use swarmy_core::{ImageTag, LeaseOwnerId, ManifestId, VolumeId};
use swarmy_store::{Store, StoreError, blob::MemoryBlobStore};

#[test]
fn commands_are_forwarded_and_root_is_required() {
    for binary in [
        env!("CARGO_BIN_EXE_swarmy"),
        env!("CARGO_BIN_EXE_swarmy-session"),
    ] {
        let output = Command::new(binary)
            .args(["vol", "--help"])
            .output()
            .unwrap();
        assert!(output.status.success());
        let help = String::from_utf8(output.stdout).unwrap();
        for name in [
            "create",
            "attach",
            "flush",
            "checkpoint",
            "snapshot",
            "clone",
            "detach",
            "ls",
            "show",
        ] {
            assert!(help.contains(name));
        }
        if !rustix::process::geteuid().is_root() {
            let output = Command::new(binary)
                .args(["vol", "attach", &ulid::Ulid::generate().to_string()])
                .output()
                .unwrap();
            assert!(!output.status.success());
            assert!(String::from_utf8_lossy(&output.stderr).contains("requires root"));
        }
    }
}

struct Fixture {
    root: tempfile::TempDir,
    namespace: String,
    store: Store,
    node_a: String,
    node_b: String,
    snapshot_period: u64,
    snapshot_retention: usize,
}
impl Fixture {
    async fn new() -> Option<Self> {
        static NETWORK: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();
        if !rustix::process::geteuid().is_root() {
            eprintln!("skipping volume acceptance: run the built test binary with sudo");
            return None;
        }
        for variable in ["SWARMY_FDB_CLUSTER_FILE", "SWARMY_S3_ENDPOINT"] {
            if std::env::var_os(variable).is_none() {
                eprintln!("skipping volume acceptance: {variable} is unset");
                return None;
            }
        }
        NETWORK.get_or_init(swarmy_store::boot);
        let settings = swarmy_config::Settings::load().unwrap().settings;
        let namespace = format!("swarmy-vol-test-{}", ulid::Ulid::generate());
        let store = Store::open(
            Some(&settings.fdb_cluster_file),
            Some(std::slice::from_ref(&namespace)),
            Arc::new(MemoryBlobStore::default()),
        )
        .await
        .unwrap();
        let fixture = Self {
            root: tempfile::tempdir().unwrap(),
            namespace,
            store,
            node_a: ulid::Ulid::generate().to_string(),
            node_b: ulid::Ulid::generate().to_string(),
            snapshot_period: 600,
            snapshot_retention: 10,
        };
        fixture.base_image(&settings).await;
        Some(fixture)
    }

    async fn base_image(&self, settings: &swarmy_config::Settings) {
        let raw = self.root.path().join("base.ext4");
        std::fs::File::create(&raw)
            .unwrap()
            .set_len(64 * 1024 * 1024)
            .unwrap();
        system("mkfs.ext4", &["-F", "-q", raw.to_str().unwrap()]);
        let objects = settings.object_store().unwrap();
        let image = swarmy_volume::image::upload_image(&raw, objects)
            .await
            .unwrap();
        let id = ManifestId::from_ulid(ulid::Ulid::generate());
        self.store.put_manifest(id, &image.header).await.unwrap();
        self.store
            .put_image("test", &ImageTag("base".into()), id)
            .await
            .unwrap();
    }

    fn command(&self, node: &str, arguments: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_swarmy"));
        command
            .current_dir(self.root.path())
            .env("SWARMY_STORE_DIRECTORY", &self.namespace)
            .env("SWARMY_NODE_ID", node)
            .env(
                "SWARMY_VOLUME_SNAPSHOT_PERIOD_SECONDS",
                self.snapshot_period.to_string(),
            )
            .env(
                "SWARMY_VOLUME_SNAPSHOT_RETENTION",
                self.snapshot_retention.to_string(),
            )
            .args(["--json", "vol"])
            .args(arguments);
        command
    }
    fn json(&self, node: &str, arguments: &[&str]) -> Value {
        let output = self.command(node, arguments).output().unwrap();
        assert!(
            output.status.success(),
            "{arguments:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    }
    fn attach(&self, node: &str, id: &str, name: &str) -> Server {
        let mount = self.root.path().join(name);
        std::fs::create_dir_all(&mount).unwrap();
        let child = self
            .command(node, &["attach", id, "--background"])
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut server = Server {
            child,
            mount,
            mounted: false,
            device: String::new(),
        };
        let mut line = String::new();
        BufReader::new(server.child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let ready: Value = serde_json::from_str(&line).expect("attach server did not become ready");
        server.device = ready["device"].as_str().unwrap().into();
        system("mount", &[&server.device, server.mount.to_str().unwrap()]);
        server.mounted = true;
        server
    }
}

struct Server {
    child: Child,
    mount: PathBuf,
    mounted: bool,
    device: String,
}
impl Server {
    fn write(&self, name: &str, value: &[u8]) {
        std::fs::write(self.mount.join(name), value).unwrap();
        std::fs::File::open(self.mount.join(name))
            .unwrap()
            .sync_all()
            .unwrap();
    }
    fn read(&self, name: &str) -> Vec<u8> {
        std::fs::read(self.mount.join(name)).unwrap()
    }
    fn stopped(&mut self) {
        self.mounted = false;
        assert!(self.child.wait().unwrap().success());
        assert_free(&self.device);
    }
    fn crash(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
        system("umount", &["-l", self.mount.to_str().unwrap()]);
        self.mounted = false;
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        if self.mounted {
            let _ = Command::new("umount").arg(&self.mount).status();
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn system(program: &str, args: &[&str]) {
    let output = Command::new(program).args(args).output().unwrap();
    assert!(
        output.status.success(),
        "{program} {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
fn assert_free(path: &str) {
    let name = Path::new(path).file_name().unwrap().to_str().unwrap();
    assert!(!Path::new(&format!("/sys/class/block/{name}/pid")).exists());
    assert_eq!(
        std::fs::read_to_string(format!("/sys/class/block/{name}/size"))
            .unwrap()
            .trim(),
        "0"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn root_volume_durability_clone_crash_fencing_and_history() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    let created = fixture.json(&fixture.node_a, &["create", "test:base"]);
    let volume = created["volume_id"].as_str().unwrap();
    let mut node_a = fixture.attach(&fixture.node_a, volume, "a");
    node_a.write("durable", b"committed on A");
    let flushed = fixture.json(
        &fixture.node_a,
        &["flush", volume, "--mount", node_a.mount.to_str().unwrap()],
    );
    assert!(flushed["manifest_id"].is_string());
    let stats: swarmy_volume::FlushResult = serde_json::from_value(flushed.clone()).unwrap();
    assert!(stats.device_total.chunks_uploaded > 0);
    assert!(stats.device_total.object_store_requests >= stats.device_total.chunks_uploaded);
    assert!(stats.device_total.bytes_uploaded >= u64::from(swarmy_core::CHUNK_SIZE));
    assert!(stats.device_total.dirty_lock_wait > Duration::ZERO);
    assert_eq!(stats.frozen, Duration::ZERO);
    assert_eq!(stats.freeze_wait, Duration::ZERO);
    assert_eq!(stats.frozen_chunks_uploaded, 0);
    assert!(stats.frozen_chunks_uploaded <= stats.uploads.chunks_uploaded);
    assert!(stats.elapsed >= stats.freeze_wait + stats.frozen);
    fixture.json(&fixture.node_a, &["detach", volume]);
    node_a.stopped();
    let mut node_b = fixture.attach(&fixture.node_b, volume, "b");
    assert_eq!(node_b.read("durable"), b"committed on A");
    eprintln!("acceptance 1 passed: durable data moved from node A to node B");
    let frozen = fixture.json(
        &fixture.node_b,
        &[
            "flush",
            volume,
            "--mount",
            node_b.mount.to_str().unwrap(),
            "--freeze",
        ],
    );
    let stats: swarmy_volume::FlushResult = serde_json::from_value(frozen).unwrap();
    assert!(stats.frozen > Duration::ZERO);
    assert!(stats.frozen_chunks_uploaded <= stats.uploads.chunks_uploaded);
    assert!(stats.elapsed >= stats.freeze_wait + stats.frozen);
    let snapshot = fixture.json(&fixture.node_b, &["checkpoint", volume]);
    assert_ne!(snapshot["manifest_id"], flushed["manifest_id"]);
    let cloned = fixture.json(&fixture.node_a, &["clone", volume]);
    let clone = cloned["volume_id"].as_str().unwrap();
    let mut clone_server = fixture.attach(&fixture.node_a, clone, "clone");
    node_b.write("original-only", b"original");
    clone_server.write("clone-only", b"clone");
    assert!(!node_b.mount.join("clone-only").exists());
    assert!(!clone_server.mount.join("original-only").exists());
    fixture.json(&fixture.node_a, &["detach", clone]);
    clone_server.stopped();
    eprintln!("acceptance 2 passed: simultaneous read-write clone is isolated");
    reject_wrong_writer(&fixture, volume, &snapshot).await;
    node_b.write("uncommitted", b"discard after crash");
    node_b.crash();
    let id: VolumeId = serde_json::from_value(created["volume_id"].clone()).unwrap();
    let expiry = fixture
        .store
        .get_volume(id)
        .await
        .unwrap()
        .unwrap()
        .writer_lease
        .unwrap()
        .expires_at;
    let remaining = expiry
        .duration_since(jiff::Timestamp::now())
        .as_secs()
        .max(0);
    eprintln!("waiting for the crashed writer's lease to expire ({remaining}s)");
    tokio::time::sleep(Duration::from_secs(u64::try_from(remaining).unwrap() + 2)).await;
    let mut recovered = fixture.attach(&fixture.node_a, volume, "recovered");
    assert_eq!(recovered.read("durable"), b"committed on A");
    assert!(!recovered.mount.join("uncommitted").exists());
    assert!(!recovered.mount.join("original-only").exists());
    eprintln!("acceptance 3 passed: killed server recovers exactly the last snapshot");
    fixture.json(&fixture.node_a, &["detach", volume]);
    recovered.stopped();
    check_history(&fixture, volume, clone, &created);
}

async fn reject_wrong_writer(fixture: &Fixture, volume: &str, snapshot: &Value) {
    let id = VolumeId::from_ulid(volume.parse().unwrap());
    let before = fixture.store.get_volume(id).await.unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(before.head_manifest).unwrap(),
        snapshot["manifest_id"]
    );
    let wrong = swarmy_core::Lease {
        owner: LeaseOwnerId::from_ulid(fixture.node_a.parse().unwrap()),
        ..before.writer_lease.clone().unwrap()
    };
    let next = ManifestId::from_ulid(ulid::Ulid::generate());
    let header = fixture
        .store
        .get_manifest(before.head_manifest)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .advance_volume(id, &wrong, before.head_manifest, next, &header)
            .await,
        Err(StoreError::LeaseMismatch)
    ));
    let rejected = fixture
        .command(&fixture.node_a, &["flush", volume])
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("another node"));
    assert_eq!(fixture.store.get_volume(id).await.unwrap(), Some(before));
    assert_eq!(fixture.store.get_manifest(next).await.unwrap(), None);
    eprintln!("acceptance 4 passed: wrong node rejected by transaction and control socket");
}

fn check_history(fixture: &Fixture, volume: &str, clone: &str, created: &Value) {
    let shown = fixture.json(&fixture.node_a, &["show", volume]);
    let chain = shown["manifests"].as_array().unwrap();
    assert!(chain.len() >= 5);
    assert_eq!(chain.last().unwrap()["manifest_id"], created["manifest_id"]);
    let output = fixture.command(&fixture.node_a, &["ls"]).output().unwrap();
    assert!(output.status.success());
    let listed: Vec<Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(listed.len(), 2);
    for id in [volume, clone] {
        assert!(listed.iter().any(|record| record["volume_id"] == id));
    }
    let output = Command::new(env!("CARGO_BIN_EXE_swarmy"))
        .current_dir(fixture.root.path())
        .env("SWARMY_STORE_DIRECTORY", &fixture.namespace)
        .args(["vol", "show", volume])
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    for entry in chain {
        assert!(text.contains(entry["manifest_id"].as_str().unwrap()));
    }
    eprintln!("acceptance 5 passed: manifest history and every volume are listed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn root_periodic_snapshot_checkpoint_and_configured_retention() {
    let Some(mut fixture) = Fixture::new().await else {
        return;
    };
    fixture.snapshot_period = 1;
    fixture.snapshot_retention = 3;
    let created = fixture.json(&fixture.node_a, &["create", "test:base"]);
    let volume = created["volume_id"].as_str().unwrap();
    let id = VolumeId::from_ulid(volume.parse().unwrap());
    let mut server = fixture.attach(&fixture.node_a, volume, "periodic");
    server.write("periodic", b"published by timer");
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let head = fixture
                .store
                .get_volume(id)
                .await
                .unwrap()
                .unwrap()
                .head_manifest;
            if serde_json::to_value(head).unwrap() != created["manifest_id"] {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("periodic snapshot did not publish");
    for index in 0..12 {
        server.write("checkpoint", index.to_string().as_bytes());
        let checkpoint = fixture.json(&fixture.node_a, &["checkpoint", volume]);
        let head = fixture
            .store
            .get_volume(id)
            .await
            .unwrap()
            .unwrap()
            .head_manifest;
        assert_eq!(
            serde_json::to_value(head).unwrap(),
            checkpoint["manifest_id"]
        );
    }
    let shown = fixture.json(&fixture.node_a, &["show", volume]);
    assert_eq!(shown["manifests"].as_array().unwrap().len(), 3);
    fixture.json(&fixture.node_a, &["detach", volume]);
    server.stopped();
    let mut reopened = fixture.attach(&fixture.node_b, volume, "reopened");
    assert_eq!(reopened.read("periodic"), b"published by timer");
    assert_eq!(reopened.read("checkpoint"), b"11");
    fixture.json(&fixture.node_b, &["detach", volume]);
    reopened.stopped();
}
