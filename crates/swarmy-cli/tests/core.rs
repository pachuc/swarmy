#![cfg(target_os = "linux")]
#![deny(clippy::disallowed_methods)]
use serde_json::Value;
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};
use swarmy_testkit::release::{fake_release, host_target, serve_files};

const CLI: &str = env!("CARGO_BIN_EXE_swarmy");
struct Fixture(tempfile::TempDir);
impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir_all(root.path().join("scripts")).unwrap();
        fs::create_dir_all(root.path().join(".dev")).unwrap();
        fs::write(root.path().join(".dev/env"), "").unwrap();
        fs::write(
            root.path().join("scripts/dev-stack.sh"),
            "#!/bin/sh\necho called >> stack-calls\n",
        )
        .unwrap();
        executable(&root.path().join("scripts/dev-stack.sh"));
        Self(root)
    }
    fn run(&self, args: &[&str], release: Option<&str>, core: Option<&Path>) -> Output {
        let mut command = Command::new(CLI);
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("SWARMY_") {
                command.env_remove(key);
            }
        }
        if let Some(url) = release {
            command.env("SWARMY_RELEASE_BASE_URL", url);
        }
        if let Some(path) = core {
            command.env("SWARMY_CORE_BIN", path);
        }
        command
            .args(args)
            .current_dir(self.0.path())
            .env("HOME", self.0.path())
            .env("XDG_CONFIG_HOME", self.0.path().join("config"))
            .env("PATH", "/usr/bin:/bin")
            .output()
            .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.run(&["dev", "down"], None, None);
    }
}
fn executable(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}
fn core_script(identity: &str) -> String {
    format!(
        "#!/bin/sh\nif [ \"$1\" = --version ]; then echo 'swarmy-core {identity}'; exit 0; fi\ncase \"$1\" in scheduler) echo 'scheduler started';; worker) echo 'worker ready';; gateway) echo 'gateway ready';; api) echo 'api ready';; esac\nexec /bin/sleep 600\n"
    )
}
fn release(identity: &str) -> String {
    let addr = serve_files(fake_release(
        swarmy_version::RELEASE_TAG,
        &host_target(),
        &core_script(identity),
    ));
    format!("http://{addr}")
}
#[test]
fn core_ensure_fetches_verifies_and_reuses_the_release() {
    let fixture = Fixture::new();
    let url = release(swarmy_version::IDENTITY);
    let args = ["--json", "core", "ensure"];
    let output = fixture.run(&args, Some(&url), None);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    let path = fixture.0.path().join(format!(
        ".swarmy/core/{}/swarmy-core-{}/swarmy-core",
        swarmy_version::RELEASE_TAG,
        host_target()
    ));
    assert_eq!(value["path"], path.to_str().unwrap());
    assert_eq!(value["source"], "release");
    assert_eq!(value["identity"], swarmy_version::IDENTITY);
    assert_eq!(value["tag"], swarmy_version::RELEASE_TAG);
    assert_eq!(value["target"], host_target());
    assert!(path.exists());
    assert!(fixture.run(&args, Some(&url), None).status.success());
}
#[test]
fn core_ensure_refuses_a_corrupt_archive() {
    let fixture = Fixture::new();
    let mut files = fake_release(
        swarmy_version::RELEASE_TAG,
        &host_target(),
        &core_script(swarmy_version::IDENTITY),
    );
    files[0].1 = b"corrupt".to_vec();
    let url = format!("http://{}", serve_files(files));
    let output = fixture.run(&["core", "ensure"], Some(&url), None);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("checksum mismatch"));
}
#[test]
fn dev_up_launches_the_downloaded_core() {
    let fixture = Fixture::new();
    let url = release(swarmy_version::IDENTITY);
    let output = fixture.run(&["dev", "up"], Some(&url), None);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let state = fixture.0.path().join(".swarmy/dev");
    assert!(state.join("supervisor.pid").is_file());
    let pid = fs::read_to_string(state.join("supervisor.pid")).unwrap();
    let pid = pid.split_whitespace().next().unwrap();
    let command = fs::read(format!("/proc/{pid}/cmdline")).unwrap_or_default();
    assert!(String::from_utf8_lossy(&command).contains("swarmy-core-"));
}
#[test]
fn dev_up_uses_core_bin_flag_and_env() {
    let fixture = Fixture::new();
    let path = fixture.0.path().join("core");
    fs::write(&path, core_script(swarmy_version::IDENTITY)).unwrap();
    executable(&path);
    let output = fixture.run(
        &["dev", "up", "--core-bin", path.to_str().unwrap()],
        None,
        None,
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = fixture.run(&["dev", "up"], None, Some(&path));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
#[test]
fn downloaded_core_with_other_identity_is_refused() {
    let fixture = Fixture::new();
    let url = release("0.0.1 (other)");
    let output = fixture.run(&["dev", "up"], Some(&url), None);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    for needle in [
        "swarmy-core version mismatch",
        "0.0.1 (other)",
        swarmy_version::IDENTITY,
        "--allow-version-mismatch",
        "Remove",
    ] {
        assert!(stderr.contains(needle), "{stderr}");
    }
    assert!(!fixture.0.path().join("stack-calls").exists());
}
#[test]
fn run_refuses_a_mismatched_api_core() {
    let fixture = Fixture::new();
    let body = serde_json::json!({"version":"9.9.9","git_commit":"other","api_version":swarmy_api_types::API_VERSION,"default_provider":"fake","services":[],"node_count":0}).to_string();
    let address = serve_files(vec![("/v1/health".to_owned(), body.into_bytes())]);
    fs::create_dir_all(fixture.0.path().join(".swarmy")).unwrap();
    fs::write(
        fixture.0.path().join(".swarmy/config.toml"),
        format!("[api]\nurl = 'http://{address}'\ntoken = 'x'\n"),
    )
    .unwrap();
    let output = fixture.run(&["run", "hello"], None, None);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    for needle in [
        "9.9.9 (other)",
        swarmy_version::IDENTITY,
        "--allow-version-mismatch",
    ] {
        assert!(stderr.contains(needle), "{stderr}");
    }
    let output = fixture.run(&["--allow-version-mismatch", "run", "hello"], None, None);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("warning:") && stderr.contains("(--allow-version-mismatch)"),
        "{stderr}"
    );
}
