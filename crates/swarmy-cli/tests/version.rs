#![deny(clippy::disallowed_methods)]
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Command, Output},
};

use serde_json::Value;
use swarmy_testkit::FakeStack;

const CLI: &str = env!("CARGO_BIN_EXE_swarmy");
const SERVICES: [(&str, &str); 4] = [
    ("scheduler", "scheduler started"),
    ("worker", "worker ready"),
    ("gateway", "gateway ready"),
    ("api", "api ready"),
];

fn script(path: &Path, body: &str) {
    fs::write(path, format!("#!/bin/sh\n{body}\n")).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

struct Fixture {
    root: tempfile::TempDir,
    stack: FakeStack,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir(root.path().join("bin")).unwrap();
        let stack = FakeStack::start(&root.path().join("data/swarmy/stack"));
        let fixture = Self { root, stack };
        fixture.core(swarmy_version::IDENTITY);
        fixture
    }

    fn core(&self, identity: &str) {
        script(
            &self.root.path().join("bin/swarmy-core"),
            &format!(
                "if [ \"$1\" = --version ]; then echo 'swarmy-core {identity}'; exit 0; fi
case \"$1\" in
  scheduler) echo 'scheduler started';;
  worker) echo 'worker ready';;
  gateway) echo 'gateway ready';;
  api) echo 'api ready';;
esac
exec /bin/sleep 600"
            ),
        );
    }

    fn run(&self, args: &[&str]) -> Output {
        let mut command = Command::new(CLI);
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("SWARMY_") {
                command.env_remove(key);
            }
        }
        command
            .args(args)
            .current_dir(self.root.path())
            .envs(swarmy_testkit::isolated_home(self.root.path()))
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin", self.root.path().join("bin").display()),
            )
            .output()
            .unwrap()
    }

    fn assert_started(&self, override_mismatch: bool) {
        let args: &[&str] = if override_mismatch {
            &["dev", "up", "--allow-version-mismatch"]
        } else {
            &["dev", "up"]
        };
        let output = self.run(args);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        for (name, _) in SERVICES {
            assert!(stdout.contains(&format!("{name}: ready")), "{stdout}");
        }
        if override_mismatch {
            assert!(
                String::from_utf8_lossy(&output.stderr)
                    .contains("warning: swarmy-core version mismatch")
            );
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.run(&["dev", "down"]);
    }
}

#[test]
fn matching_path_services_start() {
    Fixture::new().assert_started(false);
}

#[test]
fn stale_path_service_is_refused_before_starting_or_stopping_anything() {
    let mut fixture = Fixture::new();
    fixture.core("0.1.0 (stale)");
    let output = fixture.run(&["dev", "up"]);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("swarmy-core version mismatch"), "{stderr}");
    assert!(stderr.contains(swarmy_version::IDENTITY), "{stderr}");
    assert!(stderr.contains("0.1.0 (stale)"), "{stderr}");
    assert!(stderr.contains("cargo install --locked --path"), "{stderr}");
    assert!(stderr.contains("--allow-version-mismatch"), "{stderr}");
    assert!(fixture.stack.running());
    assert!(
        !fixture
            .root
            .path()
            .join("state/swarmy/dev/supervisor.pid")
            .exists()
    );
}

#[test]
fn override_starts_stale_path_service() {
    let fixture = Fixture::new();
    fixture.core("0.1.0 (stale)");
    fixture.assert_started(true);
}

#[test]
fn doctor_reports_each_identity_and_flags_stale_service() {
    let fixture = Fixture::new();
    // Avoid unrelated network probes: an invalid config still permits version checks.
    fs::create_dir_all(fixture.root.path().join(".swarmy")).unwrap();
    fs::write(fixture.root.path().join(".swarmy/config.toml"), "invalid").unwrap();
    fixture.core("0.1.0 (stale)");
    let output = fixture.run(&["doctor", "--json"]);
    assert!(!output.status.success());
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    let check = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["name"] == "swarmy-core")
        .unwrap();
    assert_eq!(check["ok"], false, "{check}");
    let detail = check["detail"].as_str().unwrap();
    assert!(detail.contains(swarmy_version::IDENTITY), "{detail}");
    assert!(detail.contains("0.1.0 (stale)"), "{detail}");
    assert!(check["fix"].as_str().unwrap().contains("cargo install"));
    let output = fixture.run(&["doctor"]);
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("fix swarmy-core:"), "{stdout}");
}

fn isolated_cli_command(path: &str, root: &Path) -> Command {
    let mut command = Command::new(path);
    command.envs(swarmy_testkit::isolated_home(root));
    command
}

#[test]
fn cli_reports_identity_in_text_and_json() {
    let root = tempfile::tempdir().unwrap();
    for (name, path) in [("swarmy", CLI)] {
        let output = isolated_cli_command(path, root.path())
            .arg("--version")
            .output()
            .unwrap();
        assert!(output.status.success());
        assert_eq!(
            String::from_utf8(output.stdout).unwrap().trim(),
            format!("{name} {}", swarmy_version::IDENTITY)
        );
        for args in [
            ["--version", "--json"],
            ["--json", "--version"],
            ["-V", "--json"],
        ] {
            let output = isolated_cli_command(path, root.path())
                .args(args)
                .output()
                .unwrap();
            assert!(output.status.success());
            let version: Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(version["binary"], name);
            assert_eq!(version["version"], swarmy_version::VERSION);
            assert_eq!(version["git_commit"], swarmy_version::GIT_COMMIT);
            assert_eq!(version["identity"], swarmy_version::IDENTITY);
        }
    }
}

#[test]
fn failed_version_probe_is_refused() {
    let mut fixture = Fixture::new();
    script(
        &fixture.root.path().join("bin/swarmy-core"),
        "echo 'old worker does not understand --version' >&2\nexit 2",
    );
    let output = fixture.run(&["dev", "up"]);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("swarmy-core version check failed"),
        "{stderr}"
    );
    assert!(stderr.contains("cargo install"), "{stderr}");
    assert!(fixture.stack.running());
}

#[test]
fn mismatch_keeps_existing_supervisor_running() {
    let fixture = Fixture::new();
    fixture.assert_started(false);
    let pid_file = fixture.root.path().join("state/swarmy/dev/supervisor.pid");
    let before = fs::read(&pid_file).unwrap();
    fixture.core("0.0.1 (old)");
    let output = fixture.run(&["dev", "up"]);
    assert!(!output.status.success());
    assert_eq!(fs::read(pid_file).unwrap(), before);
    let status = fixture.run(&["dev", "status"]);
    assert!(status.status.success());
    let stdout = String::from_utf8(status.stdout).unwrap();
    assert!(stdout.contains("supervisor: up"), "{stdout}");
}

#[test]
fn version_release_prints_the_matching_tag() {
    let root = tempfile::tempdir().unwrap();
    let output = isolated_cli_command(CLI, root.path())
        .args(["version", "--release"])
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap().trim(),
        format!("v{}", swarmy_version::VERSION)
    );
    let output = isolated_cli_command(CLI, root.path())
        .args(["--json", "version", "--release"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let version: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(version["tag"], swarmy_version::RELEASE_TAG);
    assert_eq!(version["version"], swarmy_version::VERSION);
    assert_eq!(version["release_build"], false);
}
