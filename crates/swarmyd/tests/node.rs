#![cfg(target_os = "linux")]
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};
use swarmy_core::{
    AgentId, BlockDevice, ExecOutput, ExecRequest, ImageTag, ManifestId, NodeId, Sandbox,
    SandboxSpec, VolumeId,
};
use swarmy_store::{Store, blob::ObjectBlobStore};
use swarmyd::{Request, Response};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
};

#[path = "node/persistent.rs"]
mod persistent;

struct Node {
    root: tempfile::TempDir,
    child: Option<Child>,
    settings: swarmy_config::Settings,
    id: NodeId,
}

impl Node {
    fn new(mut settings: swarmy_config::Settings) -> Self {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join(".swarmy")).unwrap();
        let id = NodeId::from_ulid(ulid::Ulid::generate());
        settings.node_id = Some(id);
        settings.node_heartbeat_interval_ms = 100;
        settings.node_capacity.cpu_millis = 4000;
        settings.node_capacity.memory_bytes = 15 * 1024 * 1024 * 1024;
        std::fs::write(
            root.path().join(".swarmy/config.toml"),
            settings.to_toml().unwrap(),
        )
        .unwrap();
        Self {
            root,
            child: None,
            settings,
            id,
        }
    }

    fn start(&mut self) {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.path().join("node.log"))
            .unwrap();
        self.child = Some(
            Command::new(env!("CARGO_BIN_EXE_swarmyd"))
                .current_dir(self.root.path())
                .envs(self.settings.environment())
                .stdin(Stdio::null())
                .stdout(log.try_clone().unwrap())
                .stderr(log)
                .spawn()
                .unwrap(),
        );
    }

    fn socket(&self) -> PathBuf {
        self.root.path().join(".swarmy/node/control.sock")
    }

    async fn ready(&mut self, store: &Store, after: jiff::Timestamp) {
        tokio::time::timeout(Duration::from_secs(45), async {
            loop {
                assert!(
                    self.child.as_mut().unwrap().try_wait().unwrap().is_none(),
                    "swarmyd exited during startup"
                );
                if store
                    .get_node(self.id)
                    .await
                    .unwrap()
                    .is_some_and(|node| node.last_heartbeat > after)
                    && UnixStream::connect(self.socket()).await.is_ok()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("node did not register");
    }

    async fn request(&self, request: Request) -> Response {
        let mut reader = connect(&self.socket(), request).await;
        read(&mut reader).await
    }

    async fn create(&self, volume_id: VolumeId) -> Sandbox {
        match self
            .request(Request::Create {
                spec: SandboxSpec {
                    agent_id: AgentId::from_ulid(ulid::Ulid::generate()),
                },
                disk: BlockDevice { volume_id },
            })
            .await
        {
            Response::Sandbox(sandbox) => sandbox,
            response => panic!("create: {response:?}"),
        }
    }

    async fn exec(
        &self,
        sandbox: &Sandbox,
        command: &str,
        timeout_ms: u64,
    ) -> (swarmy_core::ExecResult, Vec<u8>, Vec<u8>) {
        let mut reader = connect(&self.socket(), exec(sandbox, command, timeout_ms)).await;
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        loop {
            match read(&mut reader).await {
                Response::Output(ExecOutput::Stdout(bytes)) => stdout.extend(bytes),
                Response::Output(ExecOutput::Stderr(bytes)) => stderr.extend(bytes),
                Response::Exited(result) => return (result, stdout, stderr),
                response => panic!("exec: {response:?}"),
            }
        }
    }

    async fn destroy(&self, sandbox: Sandbox) {
        assert!(matches!(
            self.request(Request::Destroy(sandbox)).await,
            Response::Destroyed
        ));
    }

    fn kill(&mut self) {
        let mut child = self.child.take().unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
    }

    async fn stop(&mut self) {
        let child = self.child.as_mut().unwrap();
        assert!(
            Command::new("kill")
                .args(["-TERM", &child.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        tokio::time::timeout(Duration::from_secs(45), async {
            loop {
                if let Some(status) = self.child.as_mut().unwrap().try_wait().unwrap() {
                    assert!(status.success());
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap();
        self.child = None;
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!(
                "node log: {}",
                std::fs::read_to_string(self.root.path().join("node.log")).unwrap_or_default()
            );
        }
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let root = self.root.path().join(".swarmy/node");
        // Cleanup does not rely on swarmyd being alive or the test succeeding.
        if let Ok(bundles) = std::fs::read_dir(root.join("bundles")) {
            for bundle in bundles.flatten() {
                let _ = Command::new("runc")
                    .arg("--root")
                    .arg(root.join("runc"))
                    .args(["delete", "--force"])
                    .arg(bundle.file_name())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
                let mount = bundle.path().join("rootfs");
                let source = Command::new("findmnt")
                    .args(["--noheadings", "--output", "SOURCE", "--mountpoint"])
                    .arg(&mount)
                    .output()
                    .ok()
                    .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned());
                let _ = Command::new("umount")
                    .arg(&mount)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
                if let Some(source) = source.filter(|source| {
                    source
                        .strip_prefix("/dev/nbd")
                        .is_some_and(|suffix| suffix.parse::<u32>().is_ok())
                }) {
                    let _ = swarmy_volume::kernel::cleanup_stale(Path::new(&source));
                }
            }
        }
    }
}

fn exec(sandbox: &Sandbox, command: &str, timeout_ms: u64) -> Request {
    Request::Exec {
        sandbox: sandbox.clone(),
        request: ExecRequest {
            args: vec!["/bin/bash".into(), "-c".into(), command.into()],
            timeout_ms,
            stdin: Vec::new(),
        },
    }
}

async fn connect(path: &Path, request: Request) -> BufReader<UnixStream> {
    let mut socket = UnixStream::connect(path).await.unwrap();
    let mut bytes = serde_json::to_vec(&request).unwrap();
    bytes.push(b'\n');
    socket.write_all(&bytes).await.unwrap();
    BufReader::new(socket)
}

async fn read(reader: &mut BufReader<UnixStream>) -> Response {
    let mut line = String::new();
    let n = tokio::time::timeout(Duration::from_secs(600), reader.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    assert!(n > 0, "node closed connection without a response");
    serde_json::from_str(&line).unwrap()
}

async fn store(settings: &swarmy_config::Settings) -> Store {
    let directory: Vec<_> = settings
        .store_directory
        .split('/')
        .map(str::to_owned)
        .collect();
    Store::open(
        Some(&settings.fdb_cluster_file),
        Some(&directory),
        // The node reads large tool payloads in a separate process.
        Arc::new(ObjectBlobStore::new(settings.object_store().unwrap())),
    )
    .await
    .unwrap()
}

async fn base_image(settings: &swarmy_config::Settings, store: &Store) -> ManifestId {
    let tag = ImageTag("test".into());
    if let Some(manifest) = store.get_image("base-ubuntu", &tag).await.unwrap() {
        return manifest;
    }
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let binary = std::env::var_os("SWARMY_TEST_CLI")
        .map_or_else(|| workspace.join("target/debug/swarmy"), PathBuf::from);
    assert!(
        binary.is_file(),
        "build the CLI with cargo build -p swarmy-cli before running root tests"
    );
    let output = Command::new(binary)
        .current_dir(&workspace)
        .envs(settings.environment())
        .args([
            "--json",
            "image",
            "build",
            "images/base-ubuntu",
            "--tag",
            "test",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "image build: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    eprintln!(
        "base image built: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    store.get_image("base-ubuntu", &tag).await.unwrap().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn root_node_registration_runc_persistence_and_crash_recovery() {
    if Command::new("id").arg("-u").output().unwrap().stdout != b"0\n" {
        eprintln!("skipping node acceptance: run the built executable with sudo");
        return;
    }
    for variable in ["SWARMY_FDB_CLUSTER_FILE", "SWARMY_S3_ENDPOINT"] {
        if std::env::var_os(variable).is_none() {
            eprintln!("skipping node acceptance: {variable} is unset");
            return;
        }
    }
    boot_network();
    let mut settings = swarmy_config::Settings::load().unwrap().settings;
    let images = store(&settings).await;
    let base = base_image(&settings, &images).await;
    settings.store_directory = format!("swarmy-node-test-{}", ulid::Ulid::generate());
    let store = store(&settings).await;
    store
        .put_manifest(base, &images.get_manifest(base).await.unwrap().unwrap())
        .await
        .unwrap();
    let mut node = Node::new(settings);
    let before = jiff::Timestamp::now();
    node.start();
    node.ready(&store, before).await;
    registration(&node, &store).await;
    let caps = node.request(Request::Capabilities).await;
    assert!(matches!(
        caps,
        Response::Capabilities(swarmy_core::RuntimeCaps {
            memory_pause: false,
            kvm: false
        })
    ));
    eprintln!("acceptance 5 passed: no memory pause or hardware virtualization");
    let volume = VolumeId::from_ulid(ulid::Ulid::generate());
    store.create_volume(volume, base).await.unwrap();
    let missing = VolumeId::from_ulid(ulid::Ulid::generate());
    assert!(matches!(
        node.request(Request::Create {
            spec: SandboxSpec {
                agent_id: AgentId::from_ulid(ulid::Ulid::generate())
            },
            disk: BlockDevice { volume_id: missing },
        })
        .await,
        Response::Error(_)
    ));
    assert_eq!(
        std::fs::read_dir(node.root.path().join(".swarmy/node/bundles"))
            .unwrap()
            .count(),
        0
    );
    let sandbox = node.create(volume).await;
    let (result, _, stderr) = node.exec(&sandbox, "apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y jq && jq --version", 600_000).await;
    assert_eq!(result.exit_code, 0, "{}", String::from_utf8_lossy(&stderr));
    assert!(!result.timed_out);
    eprintln!("acceptance 2 passed: installed jq through streamed exec");
    node.destroy(sandbox).await;
    let manifest = store.get_volume(volume).await.unwrap().unwrap();
    assert!(manifest.writer_lease.is_none());
    assert_ne!(manifest.head_manifest, base);
    let next = VolumeId::from_ulid(ulid::Ulid::generate());
    store
        .create_volume(next, manifest.head_manifest)
        .await
        .unwrap();
    let sandbox = node.create(next).await;
    assert_eq!(
        node.exec(&sandbox, "jq --version", 10_000)
            .await
            .0
            .exit_code,
        0
    );
    eprintln!("acceptance 3 passed: a new volume from the final manifest contains jq");
    snapshot_tool_latency(&node, &store, &sandbox, next).await;
    pause_resume_timeout(&node, sandbox).await;
    crash_recovery(&mut node, &store, volume).await;
    node.stop().await;
    // Settings now carries the merged remote configuration, so this future is boxed.
    Box::pin(persistent::run(node.settings.clone(), &store, base)).await;
}

async fn registration(node: &Node, store: &Store) {
    let first = store.get_node(node.id).await.unwrap().unwrap();
    assert_eq!(first.roles, node.settings.node_roles);
    assert_eq!(first.capacity, node.settings.node_capacity);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let second = store.get_node(node.id).await.unwrap().unwrap();
    assert!(second.last_heartbeat > first.last_heartbeat);
    let (live, cursor) = store
        .scan_live_nodes(None, first.last_heartbeat, 1)
        .await
        .unwrap();
    assert_eq!(live[0].node_id, node.id);
    assert_eq!(cursor, Some(node.id));
    store.put_node(&first).await.unwrap();
    assert!(
        store
            .get_node(node.id)
            .await
            .unwrap()
            .unwrap()
            .last_heartbeat
            >= second.last_heartbeat
    );
    let future = jiff::Timestamp::now()
        .checked_add(Duration::from_secs(60))
        .unwrap();
    let (expired, cursor) = store.scan_live_nodes(None, future, 1).await.unwrap();
    assert!(expired.is_empty());
    assert_eq!(cursor, Some(node.id));
    assert!(
        store
            .scan_live_nodes(cursor, future, 1)
            .await
            .unwrap()
            .1
            .is_none()
    );
    assert!(store.scan_live_nodes(None, future, 0).await.is_err());
    eprintln!(
        "acceptance 1 passed: node roles, capacity, advancing heartbeat, and live-node scans"
    );
}

async fn pause_resume_timeout(node: &Node, sandbox: Sandbox) {
    let pause = match node.request(Request::Pause(sandbox)).await {
        Response::Paused(handle) => handle,
        response => panic!("pause: {response:?}"),
    };
    let sandbox = match node.request(Request::Resume(pause)).await {
        Response::Sandbox(sandbox) => sandbox,
        response => panic!("resume: {response:?}"),
    };
    let (result, stdout, stderr) = node
        .exec(&sandbox, "jq --version; printf stderr >&2; exit 7", 10_000)
        .await;
    assert_eq!(result.exit_code, 7);
    assert!(stdout.starts_with(b"jq-"));
    assert_eq!(stderr, b"stderr");
    let (result, _, _) = node.exec(&sandbox, "sleep 300 & wait", 100).await;
    assert!(result.timed_out);
    node.destroy(sandbox).await;
    eprintln!(
        "additional checks passed: pause/resume, stdout/stderr, exit status, and descendant timeout"
    );
}

async fn crash_recovery(node: &mut Node, store: &Store, volume: VolumeId) {
    let sandbox = node.create(volume).await;
    let mut reader = connect(
        &node.socket(),
        exec(
            &sandbox,
            "touch /uncommitted; sync; printf running; sleep 300",
            600_000,
        ),
    )
    .await;
    assert!(
        matches!(read(&mut reader).await, Response::Output(ExecOutput::Stdout(bytes)) if bytes == b"running")
    );
    let before = store
        .get_node(node.id)
        .await
        .unwrap()
        .unwrap()
        .last_heartbeat;
    node.kill();
    drop(reader);
    node.start();
    node.ready(store, before).await;
    let expiry = store
        .get_volume(volume)
        .await
        .unwrap()
        .unwrap()
        .writer_lease
        .unwrap()
        .expires_at;
    let wait = u64::try_from(
        expiry
            .duration_since(jiff::Timestamp::now())
            .as_secs()
            .max(0),
    )
    .unwrap()
        + 2;
    eprintln!("waiting {wait}s for the crashed writer lease to expire");
    tokio::time::sleep(Duration::from_secs(wait)).await;
    let recovered = node.create(volume).await;
    let (result, _, stderr) = node
        .exec(
            &recovered,
            "test ! -e /uncommitted && jq --version && touch /after-restart",
            10_000,
        )
        .await;
    assert_eq!(result.exit_code, 0, "{}", String::from_utf8_lossy(&stderr));
    node.destroy(recovered).await;
    eprintln!(
        "acceptance 4 passed: SIGKILL during exec, fresh registration, clean mount, and committed-head recovery"
    );
}

async fn snapshot_tool_latency(node: &Node, store: &Store, sandbox: &Sandbox, volume: VolumeId) {
    use swarmy_volume::server::{self, ServerConfig};
    let config = ServerConfig {
        directory: node.root.path().join(".swarmy/volumes"),
        node: node.id,
        store: store.clone(),
        objects: node.settings.object_store().unwrap(),
    };
    let command = "sleep 0.1; echo tool-priority";
    let start = std::time::Instant::now();
    assert_eq!(node.exec(sandbox, command, 10_000).await.0.exit_code, 0);
    let baseline = start.elapsed();
    // Priority is shared by every volume in the node. Keep a real tool open
    // in another sandbox until publication finishes, so the reported limit
    // cannot race the measured tool's exit or depend on new upload admissions.
    let priority_volume = VolumeId::from_ulid(ulid::Ulid::generate());
    let head = store
        .get_volume(volume)
        .await
        .unwrap()
        .unwrap()
        .head_manifest;
    store.create_volume(priority_volume, head).await.unwrap();
    let priority_sandbox = node.create(priority_volume).await;
    let gate = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = gate.local_addr().unwrap().port();
    let mut priority_tool = connect(
        &node.socket(),
        exec(
            &priority_sandbox,
            &format!("read -r release </dev/tcp/127.0.0.1/{port}"),
            60_000,
        ),
    )
    .await;
    let (mut release, _) = tokio::time::timeout(Duration::from_secs(10), gate.accept())
        .await
        .expect("priority tool did not reach its gate")
        .unwrap();
    assert_eq!(
        node.exec(
            sandbox,
            "dd if=/dev/urandom of=/snapshot-load bs=1M count=32 status=none; sync",
            30_000
        )
        .await
        .0
        .exit_code,
        0
    );
    let tool = async {
        let start = std::time::Instant::now();
        let result = node.exec(sandbox, command, 10_000).await;
        assert_eq!(result.0.exit_code, 0);
        start.elapsed()
    };
    let snapshot = async {
        server::control_flush(&config, volume, None, false)
            .await
            .unwrap()
    };
    let (during, flushed) = tokio::join!(tool, snapshot);
    release.write_all(b"release\n").await.unwrap();
    assert!(
        matches!(read(&mut priority_tool).await, Response::Exited(result) if result.exit_code == 0 && !result.timed_out)
    );
    node.destroy(priority_sandbox).await;
    eprintln!(
        "snapshot tool latency: baseline_ms={:.3}, during_ms={:.3}, snapshot_ms={:.3}, priority_uploads={}, final_limit={}",
        baseline.as_secs_f64() * 1000.0,
        during.as_secs_f64() * 1000.0,
        flushed.elapsed.as_secs_f64() * 1000.0,
        flushed.tool_priority_uploads,
        flushed.upload_concurrency_limit
    );
    assert_eq!(flushed.upload_concurrency_limit, 4);
    assert_eq!(
        flushed.upload_bytes_per_second,
        swarmy_volume::priority::TOOL_UPLOAD_BYTES_PER_SECOND
    );
    assert_eq!(flushed.frozen, Duration::ZERO);
    assert!(
        during <= baseline + Duration::from_millis(50),
        "baseline={baseline:?}, during={during:?}"
    );
}

fn boot_network() {
    static NETWORK: std::sync::OnceLock<foundationdb::api::NetworkAutoStop> =
        std::sync::OnceLock::new();
    NETWORK.get_or_init(swarmy_store::boot);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn root_deleted_computer_stops_call_destroys_sandbox_and_detaches_device() {
    if Command::new("id").arg("-u").output().unwrap().stdout != b"0\n" {
        eprintln!("skipping computer deletion acceptance: run the built executable with sudo");
        return;
    }
    for variable in [
        "SWARMY_FDB_CLUSTER_FILE",
        "SWARMY_S3_ENDPOINT",
        "SWARMY_NATS_URL",
    ] {
        if std::env::var_os(variable).is_none() {
            eprintln!("skipping computer deletion acceptance: {variable} is unset");
            return;
        }
    }
    boot_network();
    let mut settings = swarmy_config::Settings::load().unwrap().settings;
    let images = store(&settings).await;
    let base = base_image(&settings, &images).await;
    settings.store_directory = format!("swarmy-deletion-test-{}", ulid::Ulid::generate());
    let store = store(&settings).await;
    store
        .put_manifest(base, &images.get_manifest(base).await.unwrap().unwrap())
        .await
        .unwrap();
    // Node's Drop guard also destroys containers and detaches devices on assertion failure.
    let (mut node, bus) = persistent::start(settings, &store, base, 3).await;
    persistent::deleted_computer(&node, &store, &bus).await;
    node.stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn root_named_agent_calls_serialize_and_report_occupancy() {
    if Command::new("id").arg("-u").output().unwrap().stdout != b"0\n" {
        eprintln!("skipping shared calls acceptance: run the built executable with sudo");
        return;
    }
    for variable in [
        "SWARMY_FDB_CLUSTER_FILE",
        "SWARMY_S3_ENDPOINT",
        "SWARMY_NATS_URL",
    ] {
        if std::env::var_os(variable).is_none() {
            eprintln!("skipping shared calls acceptance: {variable} is unset");
            return;
        }
    }
    boot_network();
    let mut settings = swarmy_config::Settings::load().unwrap().settings;
    let images = store(&settings).await;
    let base = base_image(&settings, &images).await;
    settings.store_directory = format!("swarmy-shared-calls-test-{}", ulid::Ulid::generate());
    let store = store(&settings).await;
    store
        .put_manifest(base, &images.get_manifest(base).await.unwrap().unwrap())
        .await
        .unwrap();
    // Node's Drop guard also destroys containers and detaches devices on assertion failure.
    let (mut node, bus) = persistent::start(settings, &store, base, 3).await;
    persistent::shared_calls(&node, &store, &bus).await;
    node.stop().await;
}

#[path = "node/yield_tools.rs"]
mod yield_tools;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn root_bash_yield_spill_stdin_and_web_fetch() {
    if Command::new("id").arg("-u").output().unwrap().stdout != b"0\n" {
        eprintln!("skipping bash yield acceptance: run the built executable with sudo");
        return;
    }
    for variable in [
        "SWARMY_FDB_CLUSTER_FILE",
        "SWARMY_S3_ENDPOINT",
        "SWARMY_NATS_URL",
    ] {
        if std::env::var_os(variable).is_none() {
            eprintln!("skipping bash yield acceptance: {variable} is unset");
            return;
        }
    }
    boot_network();
    let mut settings = swarmy_config::Settings::load().unwrap().settings;
    let images = store(&settings).await;
    let base = base_image(&settings, &images).await;
    settings.store_directory = format!("swarmy-yield-test-{}", ulid::Ulid::generate());
    let store = store(&settings).await;
    store
        .put_manifest(base, &images.get_manifest(base).await.unwrap().unwrap())
        .await
        .unwrap();
    // Node's Drop guard destroys the container and detaches its disk on failure.
    let (mut node, bus) = persistent::start(settings, &store, base, 30).await;
    yield_tools::run(&node, &store, &bus).await;
    node.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn root_file_tools_run_on_agent_disk() {
    if Command::new("id").arg("-u").output().unwrap().stdout != b"0\n" {
        eprintln!("skipping file tools acceptance: run the built executable with sudo");
        return;
    }
    for variable in [
        "SWARMY_FDB_CLUSTER_FILE",
        "SWARMY_S3_ENDPOINT",
        "SWARMY_NATS_URL",
    ] {
        if std::env::var_os(variable).is_none() {
            eprintln!("skipping file tools acceptance: {variable} is unset");
            return;
        }
    }
    boot_network();
    let mut settings = swarmy_config::Settings::load().unwrap().settings;
    let images = store(&settings).await;
    let base = base_image(&settings, &images).await;
    settings.store_directory = format!("swarmy-file-tools-test-{}", ulid::Ulid::generate());
    let store = store(&settings).await;
    store
        .put_manifest(base, &images.get_manifest(base).await.unwrap().unwrap())
        .await
        .unwrap();
    // Node's Drop guard destroys containers and detaches devices on failure.
    let (mut node, bus) = persistent::start(settings, &store, base, 3).await;
    persistent::file_tools(&node, &store, &bus).await;
    node.stop().await;
}
