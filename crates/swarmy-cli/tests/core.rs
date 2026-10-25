#![cfg(target_os = "linux")]
#![deny(clippy::disallowed_methods)]
use serde_json::Value;
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};
use swarmy_testkit::FakeStack;
use swarmy_testkit::release::{
    FAKE_IMAGE_RECIPE, fake_release, host_target, image_archive, serve_files, sha256sums,
};
#[path = "support/fake_api.rs"]
mod fake_api;

const CLI: &str = env!("CARGO_BIN_EXE_swarmy");
struct Fixture {
    root: tempfile::TempDir,
    stack: FakeStack,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let stack = FakeStack::start(&root.path().join(".swarmy/stack"));
        Self { root, stack }
    }
    fn run(&self, args: &[&str], release: Option<&str>, core: Option<&Path>) -> Output {
        let mut command = Command::new(CLI);
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("SWARMY_") {
                command.env_remove(key);
            }
        }
        command.env(
            "SWARMY_CORE_TEST_DEFAULT_IMAGE_LOG",
            self.root.path().join("default-images"),
        );
        if let Some(url) = release {
            command.env("SWARMY_RELEASE_BASE_URL", url);
        }
        if let Some(path) = core {
            command.env("SWARMY_CORE_BIN", path);
        }
        command
            .args(args)
            .current_dir(self.root.path())
            .env("HOME", self.root.path())
            .env("XDG_CONFIG_HOME", self.root.path().join("config"))
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
        "#!/bin/sh\nif [ \"$1\" = --version ]; then echo 'swarmy-core {identity}'; exit 0; fi\ncase \"$1\" in scheduler) echo 'scheduler started';; worker) echo 'worker ready';; gateway) echo 'gateway ready';; api) if [ -n \"$SWARMY_CORE_TEST_DEFAULT_IMAGE_LOG\" ]; then printf '%s\n' \"$SWARMY_DEFAULT_IMAGE\" >> \"$SWARMY_CORE_TEST_DEFAULT_IMAGE_LOG\"; fi; echo 'api ready';; esac\nexec /bin/sleep 600\n"
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
    let path = fixture.root.path().join(format!(
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
fn core_ensure_refuses_cross_target_local_core() {
    let fixture = Fixture::new();
    let other = if host_target() == "x86_64-unknown-linux-gnu" {
        "aarch64-unknown-linux-gnu"
    } else {
        "x86_64-unknown-linux-gnu"
    };
    let path = fixture.root.path().join("core");
    fs::write(&path, core_script(swarmy_version::IDENTITY)).unwrap();
    executable(&path);
    for core in [Some(path.as_path()), None] {
        let output = fixture.run(&["core", "ensure", "--target", other], None, core);
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains(
            "--target only selects a release download; --core-bin and development builds use this machine's binary"
        ));
    }
    let output = fixture.run(
        &[
            "core",
            "ensure",
            "--target",
            other,
            "--core-bin",
            path.to_str().unwrap(),
        ],
        None,
        None,
    );
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains(
        "--target only selects a release download; --core-bin and development builds use this machine's binary"
    ));
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
    let output = fixture.run(&["dev", "up", "--no-image"], Some(&url), None);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let state = fixture.root.path().join(".swarmy/dev");
    assert!(state.join("supervisor.pid").is_file());
    let pid = fs::read_to_string(state.join("supervisor.pid")).unwrap();
    let pid = pid.split_whitespace().next().unwrap();
    let command = fs::read(format!("/proc/{pid}/cmdline")).unwrap_or_default();
    let path = fixture.root.path().join(format!(
        ".swarmy/core/{}/swarmy-core-{}/swarmy-core",
        swarmy_version::RELEASE_TAG,
        host_target()
    ));
    assert!(
        String::from_utf8_lossy(&command).contains(path.to_str().unwrap()),
        "{}",
        String::from_utf8_lossy(&command)
    );
}
#[test]
fn dev_up_registers_the_release_image() {
    let fixture = Fixture::new();
    let (api, requests) = fake_api::serve(false);
    fs::create_dir_all(fixture.root.path().join(".swarmy")).unwrap();
    fs::write(
        fixture.root.path().join(".swarmy/config.toml"),
        format!("[api]\nurl = \"http://{api}\"\ntoken = \"x\"\n"),
    )
    .unwrap();
    let tag = swarmy_version::RELEASE_TAG;
    let mut files = fake_release(tag, &host_target(), &core_script(swarmy_version::IDENTITY));
    let name = format!("swarmy-image-base-ubuntu-{}", host_target());
    let asset = format!("{name}.tar.gz");
    let bytes = image_archive(
        &name,
        16 * 1024 * 1024,
        &[(0, &[1; 4096]), (8 * 1024 * 1024, &[2; 4096])],
        FAKE_IMAGE_RECIPE,
    );
    let sums = sha256sums(&[(&asset, &bytes)]);
    let original = String::from_utf8(
        files
            .iter()
            .find(|(path, _)| path.ends_with("SHA256SUMS"))
            .unwrap()
            .1
            .clone(),
    )
    .unwrap();
    files.retain(|(path, _)| !path.ends_with("SHA256SUMS"));
    files.push((format!("/{tag}/{asset}"), bytes));
    files.push((
        format!("/{tag}/SHA256SUMS"),
        format!("{original}{sums}").into_bytes(),
    ));
    let release = format!("http://{}", serve_files(files));
    let output = fixture.run(&["dev", "up"], Some(&release), None);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(&format!(
            "default image: base-ubuntu:{tag}; restarting services"
        )),
        "{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let settings =
        swarmy_config::Settings::read(&fixture.root.path().join(".swarmy/config.toml")).unwrap();
    assert_eq!(
        settings.selection.default_image.as_deref(),
        Some(format!("base-ubuntu:{tag}").as_str())
    );
    let images = fs::read_to_string(fixture.root.path().join("default-images")).unwrap();
    assert_eq!(
        images.lines().last(),
        Some(format!("base-ubuntu:{tag}").as_str())
    );
    let requests = requests.lock().unwrap();
    let posts: Vec<_> = requests
        .iter()
        .filter(|request| request.method == "POST")
        .collect();
    assert_eq!(posts.len(), 1);
    assert!(posts[0].path_and_query.contains("name=base-ubuntu"));
    assert!(posts[0].body_len < 16 * 1024 * 1024);
    assert_eq!(posts[0].first_offset, Some(0));
    assert!(
        fixture
            .root
            .path()
            .join(".swarmy/dev/supervisor.pid")
            .exists()
    );
}
#[test]
fn dev_up_uses_core_bin_flag_and_env() {
    let fixture = Fixture::new();
    let path = fixture.root.path().join("core");
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
    let pid = fs::read_to_string(fixture.root.path().join(".swarmy/dev/supervisor.pid")).unwrap();
    let pid = pid.split_whitespace().next().unwrap();
    let command = fs::read(format!("/proc/{pid}/cmdline")).unwrap();
    assert!(String::from_utf8_lossy(&command).contains(path.to_str().unwrap()));
    let output = fixture.run(&["dev", "up"], None, Some(&path));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let pid = fs::read_to_string(fixture.root.path().join(".swarmy/dev/supervisor.pid")).unwrap();
    let pid = pid.split_whitespace().next().unwrap();
    let command = fs::read(format!("/proc/{pid}/cmdline")).unwrap();
    assert!(String::from_utf8_lossy(&command).contains(path.to_str().unwrap()));
}
#[test]
fn downloaded_core_with_other_identity_is_refused() {
    let mut fixture = Fixture::new();
    let url = release("0.0.1 (other)");
    let output = fixture.run(&["dev", "up", "--no-image"], Some(&url), None);
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
    assert!(fixture.stack.running());
    assert!(
        !fixture
            .root
            .path()
            .join(".swarmy/dev/supervisor.pid")
            .exists()
    );
}
#[test]
fn run_refuses_a_mismatched_api_core() {
    let fixture = Fixture::new();
    let body = serde_json::json!({"version":"9.9.9","git_commit":"other","api_version":swarmy_api_types::API_VERSION,"default_provider":"fake","services":[{"role":"worker","instance_id":"w","version":"test","alive":true,"last_seen":"now"},{"role":"scheduler","instance_id":"s","version":"test","alive":true,"last_seen":"now"},{"role":"gateway","instance_id":"g","version":"test","alive":true,"last_seen":"now","providers":["fake"]}],"node_count":0}).to_string();
    let address = serve_files(vec![("/v1/health".to_owned(), body.into_bytes())]);
    fs::create_dir_all(fixture.root.path().join(".swarmy")).unwrap();
    fs::write(
        fixture.root.path().join(".swarmy/config.toml"),
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

#[test]
fn run_reports_missing_gateway_before_creating_a_session() {
    use std::io::{BufRead, BufReader};
    use std::process::Stdio;

    let root = tempfile::tempdir().unwrap();
    let health = serde_json::json!({
        "version": swarmy_version::VERSION,
        "git_commit": swarmy_version::GIT_COMMIT,
        "api_version": swarmy_api_types::API_VERSION,
        "default_provider": "fake",
        "services": [
            {"role":"worker","instance_id":"w","version":"test","alive":true,"last_seen":"now"},
            {"role":"scheduler","instance_id":"s","version":"test","alive":true,"last_seen":"now"}
        ],
        "node_count": 0
    });
    let address = serve_files(vec![(
        "/v1/health".to_owned(),
        health.to_string().into_bytes(),
    )]);
    fs::create_dir_all(root.path().join(".swarmy")).unwrap();
    fs::write(
        root.path().join(".swarmy/config.toml"),
        format!("[api]\nurl = 'http://{address}'\ntoken = 'x'\n"),
    )
    .unwrap();
    let mut child = Command::new(CLI)
        .args(["--json", "run", "hello"])
        .env("HOME", root.path())
        .env("XDG_CONFIG_HOME", root.path().join("config"))
        .env("SWARMY_API_URL", format!("http://{address}"))
        .env("SWARMY_API_TOKEN", "test-token")
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut line = String::new();
    BufReader::new(child.stderr.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(
        line.contains(
            "No gateway serves this session's provider (fake); waiting for service health"
        ),
        "{line}"
    );
    assert!(line.ends_with("\n"));
    // The waiting notice is stderr-only; JSON stdout is untouched.
    let mut stdout = String::new();
    use std::io::Read;
    child
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut stdout)
        .unwrap();
    assert!(stdout.is_empty(), "{stdout}");
}
