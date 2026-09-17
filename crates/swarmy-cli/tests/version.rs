use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Command, Output},
};

use serde_json::Value;

const CLI: &str = env!("CARGO_BIN_EXE_swarmy");
const SERVICES: [(&str, &str); 3] = [
    ("scheduler", "scheduler started"),
    ("worker", "worker ready"),
    ("gateway", "gateway ready"),
];

fn script(path: &Path, body: &str) {
    fs::write(path, format!("#!/bin/sh\n{body}\n")).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

struct Fixture(tempfile::TempDir);

impl Fixture {
    fn new() -> Self {
        let fixture = Self(tempfile::tempdir().unwrap());
        for directory in ["scripts", "bin", ".dev"] {
            fs::create_dir(fixture.0.path().join(directory)).unwrap();
        }
        fs::write(fixture.0.path().join(".dev/env"), "").unwrap();
        script(
            &fixture.0.path().join("scripts/dev-stack.sh"),
            "echo called >> stack-calls\nif [ \"$1\" = status ]; then\n  echo 'fdb: up (pid 1)'\n  echo 'nats: up (pid 2)'\n  echo 'seaweed: up (pid 3)'\nfi",
        );
        for (name, ready) in SERVICES {
            fixture.service(name, ready, swarmy_version::IDENTITY);
        }
        fixture
    }

    fn service(&self, name: &str, ready: &str, identity: &str) {
        script(
            &self.0.path().join(format!("bin/swarmy-{name}")),
            &format!(
                "if [ \"$1\" = --version ]; then\n  echo 'swarmy-{name} {identity}'\n  exit 0\nfi\necho '{ready}'\nexec /bin/sleep 600"
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
            .current_dir(self.0.path())
            .env("HOME", self.0.path())
            .env("XDG_CONFIG_HOME", self.0.path().join("config"))
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin", self.0.path().join("bin").display()),
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
                    .contains("warning: swarmy-worker version mismatch")
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
    let fixture = Fixture::new();
    fixture.service("worker", "worker ready", "0.1.0 (stale)");
    let output = fixture.run(&["dev", "up"]);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("swarmy-worker version mismatch"),
        "{stderr}"
    );
    assert!(stderr.contains(swarmy_version::IDENTITY), "{stderr}");
    assert!(stderr.contains("0.1.0 (stale)"), "{stderr}");
    assert!(stderr.contains("cargo install --locked --path"), "{stderr}");
    assert!(stderr.contains("--allow-version-mismatch"), "{stderr}");
    assert!(!fixture.0.path().join("stack-calls").exists());
    assert!(!fixture.0.path().join(".swarmy/dev/supervisor.pid").exists());
}

#[test]
fn override_starts_stale_path_service() {
    let fixture = Fixture::new();
    fixture.service("worker", "worker ready", "0.1.0 (stale)");
    fixture.assert_started(true);
}

#[test]
fn doctor_reports_each_identity_and_flags_stale_service() {
    let fixture = Fixture::new();
    // Avoid unrelated network probes: an invalid config still permits version checks.
    fs::create_dir(fixture.0.path().join(".swarmy")).unwrap();
    fs::write(fixture.0.path().join(".swarmy/config.toml"), "invalid").unwrap();
    fixture.service("worker", "worker ready", "0.1.0 (stale)");
    let output = fixture.run(&["doctor", "--json"]);
    assert!(!output.status.success());
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    for name in [
        "swarmy-session",
        "swarmy-scheduler",
        "swarmy-worker",
        "swarmy-gateway",
    ] {
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == name)
            .unwrap();
        assert_eq!(check["ok"], name != "swarmy-worker", "{check}");
        let detail = check["detail"].as_str().unwrap();
        assert!(detail.contains(swarmy_version::IDENTITY), "{detail}");
        if name == "swarmy-worker" {
            assert!(detail.contains("0.1.0 (stale)"), "{detail}");
            assert!(check["fix"].as_str().unwrap().contains("cargo install"));
        }
    }
    let output = fixture.run(&["doctor"]);
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("fix swarmy-worker:"), "{stdout}");
    assert!(stdout.contains("ok swarmy-scheduler:"), "{stdout}");
}

#[test]
fn cli_and_companion_report_same_identity_in_text_and_json() {
    for (name, path) in [
        ("swarmy", CLI),
        ("swarmy-session", env!("CARGO_BIN_EXE_swarmy-session")),
    ] {
        let output = Command::new(path).arg("--version").output().unwrap();
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
            let output = Command::new(path).args(args).output().unwrap();
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
    let fixture = Fixture::new();
    script(
        &fixture.0.path().join("bin/swarmy-worker"),
        "echo 'old worker does not understand --version' >&2\nexit 2",
    );
    let output = fixture.run(&["dev", "up"]);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("swarmy-worker version check failed"),
        "{stderr}"
    );
    assert!(stderr.contains("cargo install"), "{stderr}");
    assert!(!fixture.0.path().join("stack-calls").exists());
}

#[test]
fn mismatch_keeps_existing_supervisor_running() {
    let fixture = Fixture::new();
    fixture.assert_started(false);
    let pid_file = fixture.0.path().join(".swarmy/dev/supervisor.pid");
    let before = fs::read(&pid_file).unwrap();
    fixture.service("worker", "worker ready", "0.0.1 (old)");
    let output = fixture.run(&["dev", "up"]);
    assert!(!output.status.success());
    assert_eq!(fs::read(pid_file).unwrap(), before);
    let status = fixture.run(&["dev", "status"]);
    assert!(status.status.success());
    let stdout = String::from_utf8(status.stdout).unwrap();
    assert!(stdout.contains("supervisor: up"), "{stdout}");
}
