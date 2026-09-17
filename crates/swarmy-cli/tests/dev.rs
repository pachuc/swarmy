#[path = "../../swarmy-store/tests/support/mod.rs"]
mod image_fixture;

use std::{
    collections::BTreeMap,
    fs,
    os::unix::process::CommandExt,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    time::Duration,
};

use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command as AsyncCommand,
    time::{Instant, timeout},
};
use ulid::Ulid;

const CLI: &str = env!("CARGO_BIN_EXE_swarmy");

struct Fixture {
    files: tempfile::TempDir,
    repo: PathBuf,
    restore_stack: bool,
}

impl Fixture {
    fn command(&self, args: &[&str]) -> AsyncCommand {
        let mut command = AsyncCommand::new(CLI);
        command
            .current_dir(self.files.path())
            .args(args)
            .kill_on_drop(true);
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("SWARMY_") {
                command.env_remove(key);
            }
        }
        command
    }

    async fn output(&self, args: &[&str]) -> Output {
        let output = timeout(Duration::from_secs(60), self.command(args).output())
            .await
            .unwrap()
            .unwrap();
        assert!(
            output.status.success(),
            "{args:?}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }

    fn state(&self) -> PathBuf {
        self.files.path().join(".swarmy/dev")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let mut command = Command::new(CLI);
        command.current_dir(self.files.path()).args(["dev", "down"]);
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("SWARMY_") {
                command.env_remove(key);
            }
        }
        let _ = command.output();
        if self.restore_stack {
            let result = Command::new(self.repo.join("scripts/dev-stack.sh"))
                .process_group(0)
                .arg("start")
                .output()
                .unwrap();
            assert!(
                result.status.success() || std::thread::panicking(),
                "failed to restore shared CI stack: {}",
                String::from_utf8_lossy(&result.stderr)
            );
        }
    }
}

#[tokio::test]
async fn dev_up_run_recover_reconfigure_and_down() {
    if std::env::var_os("SWARMY_FDB_CLUSTER_FILE").is_none()
        || std::env::var_os("SWARMY_NATS_URL").is_none()
    {
        eprintln!(
            "skipping dev integration test: SWARMY_FDB_CLUSTER_FILE or SWARMY_NATS_URL is unset"
        );
        return;
    }
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();
    let status = Command::new("cargo")
        .current_dir(&repo)
        .args([
            "build",
            "--locked",
            "-p",
            "swarmy-scheduler",
            "-p",
            "swarmy-worker",
            "-p",
            "swarmy-gateway",
        ])
        .status()
        .unwrap();
    assert!(status.success());
    let fixture = Fixture {
        files: tempfile::tempdir().unwrap(),
        repo,
        restore_stack: true,
    };
    fs::create_dir(fixture.files.path().join(".swarmy")).unwrap();
    let config = fixture.files.path().join(".swarmy/config.toml");
    let prefix = format!("dev_test_{}", Ulid::generate());
    let settings = swarmy_config::Settings {
        default_image: Some("fixture:test".into()),
        store_directory: prefix.clone(),
        bus_prefix: prefix.clone(),
        ..Default::default()
    };
    fs::write(&config, settings.to_toml().unwrap()).unwrap();
    check_startup(&fixture).await;
    register_image(&fixture).await;
    assert!(
        String::from_utf8(fixture.output(&["run", "hello"]).await.stdout)
            .unwrap()
            .contains("Hello from swarmy!")
    );
    check_uptime(&fixture).await;
    let first = fixture.output(&["--json", "session", "list"]).await.stdout;
    assert_eq!(
        first
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .count(),
        1
    );

    check_logs(&fixture).await;

    let old = identities(&fixture.state());
    signal(old["supervisor"].0, "-KILL").await;
    let up = String::from_utf8(fixture.output(&["dev", "up"]).await.stdout).unwrap();
    assert!(up.contains("replacing recorded processes:"), "{up}");
    assert!(up.contains("scheduler, worker, gateway"), "{up}");
    assert_gone(&old);
    assert!(
        String::from_utf8(fixture.output(&["run", "after recovery"]).await.stdout)
            .unwrap()
            .contains("Hello from swarmy!")
    );

    let mut settings = swarmy_config::Settings::read(&config).unwrap();
    settings.store_directory = format!("{prefix}_changed");
    fs::write(&config, settings.to_toml().unwrap()).unwrap();
    fixture.output(&["dev", "up"]).await;
    assert!(fixture.output(&["session", "list"]).await.stdout.is_empty());
    register_image(&fixture).await;
    fixture.output(&["run", "new directory"]).await;
    let second = fixture.output(&["--json", "session", "list"]).await.stdout;
    assert_ne!(first, second);
    assert_eq!(
        second
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .count(),
        1
    );
    let running = identities(&fixture.state());
    fixture.output(&["dev", "down"]).await;
    fixture.output(&["dev", "down"]).await;
    let status = String::from_utf8(fixture.output(&["dev", "status"]).await.stdout).unwrap();
    assert!(
        status.lines().all(|line| line.ends_with(": down")),
        "{status}"
    );
    assert_gone(&running);
}

async fn check_uptime(fixture: &Fixture) {
    // A fast turn no longer guarantees that the uptime counter has advanced.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let state = String::from_utf8(fixture.output(&["dev", "status"]).await.stdout).unwrap();
    assert_eq!(state.matches("uptime").count(), 7, "{state}");
    assert!(
        !state.contains("uptime 0s"),
        "uptime did not advance: {state}"
    );
}

async fn check_startup(fixture: &Fixture) {
    let stack_pids = ["fdb", "nats", "seaweed"]
        .map(|name| fs::read_to_string(fixture.repo.join(format!(".dev/{name}.pid"))).unwrap());
    let started = Instant::now();
    let up = fixture.output(&["dev", "up"]).await;
    assert!(started.elapsed() < Duration::from_secs(30));
    for (name, previous) in ["fdb", "nats", "seaweed"].into_iter().zip(stack_pids) {
        assert_eq!(
            fs::read_to_string(fixture.repo.join(format!(".dev/{name}.pid"))).unwrap(),
            previous
        );
    }
    let up = String::from_utf8(up.stdout).unwrap();
    for name in ["stack", "scheduler", "worker", "gateway"] {
        assert!(up.contains(&format!("{name}: ready")), "{up}");
    }
}

async fn check_logs(fixture: &Fixture) {
    let mut log = fixture
        .command(&["dev", "logs", "worker"])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(log.stdout.take().unwrap()).lines();
    timeout(Duration::from_secs(5), async {
        while let Some(line) = lines.next_line().await.unwrap() {
            if line.contains("worker ready") {
                return;
            }
        }
        panic!("worker log ended before readiness");
    })
    .await
    .unwrap();
    signal(log.id().unwrap(), "-INT").await;
    assert!(
        timeout(Duration::from_secs(5), log.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
}

async fn signal(pid: u32, signal: &str) {
    assert!(
        AsyncCommand::new("kill")
            .args([signal, &pid.to_string()])
            .status()
            .await
            .unwrap()
            .success()
    );
}

fn identities(state: &Path) -> BTreeMap<String, (u32, String)> {
    ["supervisor", "scheduler", "worker", "gateway"]
        .into_iter()
        .map(|name| {
            let file = fs::read_to_string(state.join(format!("{name}.pid"))).unwrap();
            let (pid, start) = file.trim().split_once(' ').unwrap();
            (name.into(), (pid.parse().unwrap(), start.into()))
        })
        .collect()
}

fn assert_gone(identities: &BTreeMap<String, (u32, String)>) {
    for (name, (pid, start)) in identities {
        if let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) {
            let fields = stat
                .rsplit_once(") ")
                .unwrap()
                .1
                .split_whitespace()
                .collect::<Vec<_>>();
            assert!(
                fields[0] == "Z" || fields[19] != start,
                "{name} pid {pid} survived"
            );
        }
    }
}

async fn register_image(fixture: &Fixture) {
    static NETWORK: std::sync::OnceLock<foundationdb::api::NetworkAutoStop> =
        std::sync::OnceLock::new();
    NETWORK.get_or_init(swarmy_store::boot);
    let settings =
        swarmy_config::Settings::read(&fixture.files.path().join(".swarmy/config.toml")).unwrap();
    let store = swarmy_store::Store::open(
        Some(&settings.fdb_cluster_file),
        Some(&[settings.store_directory]),
        std::sync::Arc::new(swarmy_store::blob::MemoryBlobStore::default()),
    )
    .await
    .unwrap();
    image_fixture::image(&store).await;
}
