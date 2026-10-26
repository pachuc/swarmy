#![deny(clippy::disallowed_methods)]
use std::{
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Command, Output},
};

fn cli_command(root: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_swarmy"));
    command
        .current_dir(root)
        .env("HOME", root)
        .env("XDG_CONFIG_HOME", root)
        .env("SWARMY_STATE_DIR", root.join(".swarmy"))
        .env_remove("SWARMY_REMOTE")
        .env("PATH", format!("{}:/usr/bin:/bin", root.display()))
        .args(args);
    command
}

fn cli_with_env(root: &Path, args: &[&str], extra_env: &[(&str, String)]) -> Output {
    let mut command = cli_command(root, args);
    command.envs(extra_env.iter().map(|(key, value)| (*key, value)));
    command.output().unwrap()
}

fn cli(root: &Path, args: &[&str]) -> Output {
    cli_with_env(root, args, &[])
}

fn fixture(root: &Path) {
    std::fs::create_dir_all(root.join(".swarmy/remote")).unwrap();
    std::fs::write(root.join(".swarmy/remote/test.json"), r#"{"name":"test","region":"local","instance_id":"i-test","public_ip":"127.0.0.1","private_ip":"127.0.0.1","key_path":"/tmp/a key","launch_attempted":true,"created_at":"now"}"#).unwrap();
}

#[test]
fn logs_passes_fixed_journal_command_and_preserves_exit_failure() {
    let root = tempfile::tempdir().unwrap();
    fixture(root.path());
    let ssh = root.path().join("ssh");
    std::fs::write(
        &ssh,
        "#!/bin/sh\nprintf '%s\\n' \"$@\" > ssh-args\nprintf 'fake swarmyd journal\\n'\nexit 0\n",
    )
    .unwrap();
    std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o700)).unwrap();
    let output = cli(root.path(), &["remote", "logs", "test"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("fake swarmyd journal"));
    let args = std::fs::read_to_string(root.path().join("ssh-args")).unwrap();
    assert!(args.contains("/tmp/a key\n"));
    assert!(
        args.ends_with("127.0.0.1\njournalctl --unit swarmyd --follow --no-pager --lines 100\n")
    );
    std::fs::write(&ssh, "#!/bin/sh\nexit 7\n").unwrap();
    assert!(
        !cli(root.path(), &["remote", "logs", "test"])
            .status
            .success()
    );
}

#[test]
fn disconnect_is_idempotent_and_names_cannot_escape_state() {
    let root = tempfile::tempdir().unwrap();
    fixture(root.path());
    for _ in 0..2 {
        assert!(
            cli(root.path(), &["remote", "disconnect", "test"])
                .status
                .success()
        );
    }
    assert!(root.path().join(".swarmy/remote/test.json").exists());
    assert!(
        !cli(root.path(), &["remote", "connect", "../../escape"])
            .status
            .success()
    );
}

#[test]
fn disconnected_status_uses_fake_state_without_opening_a_store() {
    let root = tempfile::tempdir().unwrap();
    fixture(root.path());
    let ssh = root.path().join("ssh");
    std::fs::write(&ssh, "#!/bin/sh\nexit 1\n").unwrap();
    std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o700)).unwrap();
    let path = root.path().join(".swarmy/remote/test.json");
    let mut node: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let mut child = node.clone();
    child["name"] = "test-2".into();
    child["instance_id"] = "i-second".into();
    node["launch_settings"] = serde_json::json!({"aws":{"instance_type":"m6i.large"},"disk_gb":40});
    child["launch_settings"] =
        serde_json::json!({"aws":{"instance_type":"m6id.4xlarge"},"disk_gb":100});
    node["nodes"] = serde_json::json!([child]);
    std::fs::write(path, serde_json::to_vec(&node).unwrap()).unwrap();
    let output = cli(root.path(), &["remote", "ls", "--json"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value[0]["name"], "test");
    assert_eq!(value[0]["nodes"][0]["instance_id"], "i-second");
    assert_eq!(value[0]["instance_type"], "m6i.large");
    assert_eq!(value[0]["nodes"][0]["instance_type"], "m6id.4xlarge");
    let human = cli(root.path(), &["remote", "ls"]);
    let text = String::from_utf8(human.stdout).unwrap();
    assert!(text.contains("test instance=i-test type=m6i.large"));
    assert!(text.contains("api_token=not-applicable"));
    assert!(text.contains("test-2 instance=i-second type=m6id.4xlarge"));
    assert_eq!(
        value[0]["nodes"][0]["instance_state"],
        "unknown (SSH unreachable)"
    );
    assert_eq!(value[0]["tunnel"], false);
    assert_eq!(value[0]["api_token"], "not-applicable");
    assert_eq!(value[0]["images"], serde_json::json!([]));
    assert_eq!(value[0]["image_error"], "unknown: tunnel disconnected");
    assert_eq!(
        value[0]["registration_error"],
        "unknown: tunnel disconnected"
    );
}

#[test]
fn failed_ssh_startup_never_publishes_a_profile() {
    let root = tempfile::tempdir().unwrap();
    fixture(root.path());
    let ssh = root.path().join("ssh");
    std::fs::write(&ssh, "#!/bin/sh\nexit 1\n").unwrap();
    std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(
        !cli(root.path(), &["remote", "connect", "test"])
            .status
            .success()
    );
    assert!(
        !root
            .path()
            .join(".swarmy/remote/test.profile.json")
            .exists()
    );
    assert!(!root.path().join(".swarmy/remote/test.cluster").exists());
    assert!(
        !root
            .path()
            .join(".swarmy/remote/test.tunnel/tunnel.json")
            .exists()
    );
}

#[test]
fn doctor_reports_a_missing_profile_as_disconnected_without_checking_local_services() {
    let root = tempfile::tempdir().unwrap();
    fixture(root.path());
    std::fs::write(root.path().join(".swarmy/config.toml"), "").unwrap();
    let output = cli(root.path(), &["doctor", "--remote", "test", "--json"]);
    assert!(!output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let checks = report["checks"].as_array().unwrap();
    assert!(
        checks
            .iter()
            .any(|check| check["name"] == "config" && check["ok"] == true)
    );
    let tunnel = checks
        .iter()
        .find(|check| check["name"] == "remote tunnel")
        .unwrap();
    assert_eq!(tunnel["ok"], false);
    assert!(
        tunnel["fix"]
            .as_str()
            .unwrap()
            .contains("remote connect test")
    );
    assert!(
        !checks
            .iter()
            .any(|check| check["name"] == "remote FoundationDB")
    );
}

#[test]
fn status_uses_private_ssh_when_public_address_is_unreachable() {
    let root = tempfile::tempdir().unwrap();
    fixture(root.path());
    let path = root.path().join(".swarmy/remote/test.json");
    let content = std::fs::read_to_string(&path).unwrap().replace(
        "\"private_ip\":\"127.0.0.1\"",
        "\"private_ip\":\"127.0.0.2\"",
    );
    std::fs::write(path, content).unwrap();
    let ssh = root.path().join("ssh");
    std::fs::write(
        &ssh,
        "#!/bin/sh\ncase \"$*\" in *127.0.0.2*) exit 0;; *) exit 1;; esac\n",
    )
    .unwrap();
    std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o700)).unwrap();
    let output = cli(root.path(), &["remote", "ls", "--json"]);
    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value[0]["instance_state"], "running (SSH reachable)");
}

#[test]
fn connect_reports_timing_in_json_and_human_output_when_reusing_a_tunnel() {
    let root = tempfile::tempdir().unwrap();
    fixture(root.path());
    let profile = swarmy_config::RemoteProfile {
        name: "test".into(),
        socket_path: root.path().join("socket"),
        pid: 123,
        ports: swarmy_config::RemotePorts::default(),
        remote_ports: swarmy_config::RemotePorts::default(),
        fdb_cluster_file: root.path().join("cluster"),
        nats_url: "nats://127.0.0.1:4222".into(),
        s3_endpoint: "http://127.0.0.1:8333".into(),
        api_url: None,
        api_token: None,
        bucket: None,
        default_image: Some("base-ubuntu:test".into()),
    };
    std::fs::write(
        root.path().join(".swarmy/remote/test.profile.json"),
        serde_json::to_vec(&profile).unwrap(),
    )
    .unwrap();
    let tunnel = root.path().join(".swarmy/remote/test.tunnel");
    std::fs::create_dir(&tunnel).unwrap();
    std::fs::write(
        tunnel.join("tunnel.json"),
        serde_json::to_vec(&serde_json::json!({
            "socket": profile.socket_path, "pid": 123, "address": "127.0.0.1",
            "addresses": ["127.0.0.1", "127.0.0.1"],
            "forwards": [
                {"service":"foundation_db","local":4500,"remote":4500},
                {"service":"nats","local":4222,"remote":4222},
                {"service":"object_store","local":8333,"remote":8333}
            ]
        }))
        .unwrap(),
    )
    .unwrap();
    let ssh = root.path().join("ssh");
    std::fs::write(&ssh, "#!/bin/sh\nsleep 0.03\nexit 0\n").unwrap();
    std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o700)).unwrap();
    let output = cli(root.path(), &["remote", "connect", "test", "--json"]);
    assert!(output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["name"], "test");
    assert_eq!(report["default_image"], "base-ubuntu:test");
    assert_eq!(report["timing"]["reused"], true);
    assert_eq!(report["timing"]["address_probe_seconds"], 0.0);
    assert_eq!(report["timing"]["tunnel_startup_seconds"], 0.0);
    assert!(report["timing"]["elapsed_seconds"].as_f64().unwrap() >= 0.03);
    let output = cli(root.path(), &["remote", "connect", "test"]);
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains("# Default image: base-ubuntu:test"));
    assert!(text.contains("# Connected in "));
    assert!(text.contains("address probing: 0.000s; tunnel startup: 0.000s; reused: true"));
}

#[test]
fn connect_opens_an_api_only_tunnel_for_node_services_and_disconnect_removes_it() {
    let root = tempfile::tempdir().unwrap();
    fixture(root.path());
    let path = root.path().join(".swarmy/remote/test.json");
    let mut node: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    node["launch_settings"] = serde_json::json!({"services":"node"});
    std::fs::write(path, serde_json::to_vec(&node).unwrap()).unwrap();
    let ssh = root.path().join("ssh");
    std::fs::write(
        &ssh,
        include_str!("../../swarmy-cloud/tests/fixtures/fake-ssh"),
    )
    .unwrap();
    std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o700)).unwrap();
    let output = cli(root.path(), &["remote", "connect", "test", "--json"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let profile: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let record_path = root.path().join(".swarmy/remote/test.tunnel/tunnel.json");
    let record: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&record_path).unwrap()).unwrap();
    let pid = record["pid"].as_u64().unwrap().to_string();
    struct Kill(String);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = Command::new("kill").arg(&self.0).status();
        }
    }
    let _guard = Kill(pid);
    let port = record["forwards"][0]["local"].as_u64().unwrap();
    assert_eq!(record["forwards"][0]["service"], "api");
    assert_eq!(record["forwards"][0]["remote"], 8742);
    assert_eq!(record["forwards"].as_array().unwrap().len(), 1);
    assert_eq!(profile["api_url"], format!("http://127.0.0.1:{port}"));
    assert_eq!(
        profile["ports"],
        serde_json::json!({"fdb":0,"nats":0,"s3":0})
    );
    let saved: serde_json::Value = serde_json::from_slice(
        &std::fs::read(root.path().join(".swarmy/remote/test.profile.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(saved["api_token"], "fake-token");
    let masters = std::fs::read_to_string(root.path().join("ssh-masters")).unwrap();
    assert!(masters.contains(&format!("-L 127.0.0.1:{port}:127.0.0.1:8742")));
    assert!(masters.contains("BatchMode=yes"));
    assert!(masters.contains("ExitOnForwardFailure=yes"));
    let again = cli(root.path(), &["remote", "connect", "test", "--json"]);
    assert!(
        again.status.success(),
        "{}",
        String::from_utf8_lossy(&again.stderr)
    );
    let again: serde_json::Value = serde_json::from_slice(&again.stdout).unwrap();
    assert_eq!(again["timing"]["reused"], true);
    assert_eq!(
        std::fs::read_to_string(root.path().join("ssh-masters"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    let output = cli(root.path(), &["remote", "disconnect", "test"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!record_path.exists());
    assert!(
        !root
            .path()
            .join(".swarmy/remote/test.profile.json")
            .exists()
    );
    assert!(!Path::new(record["socket"].as_str().unwrap()).exists());
}

#[test]
fn node_services_dev_up_needs_no_local_binaries_and_preserves_remote_on_down() {
    let root = tempfile::tempdir().unwrap();
    fixture(root.path());
    let path = root.path().join(".swarmy/remote/test.json");
    let mut node: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    node["launch_settings"] = serde_json::json!({"services": "node"});
    std::fs::write(path, serde_json::to_vec(&node).unwrap()).unwrap();
    std::fs::write(root.path().join(".swarmy/config.toml"), "").unwrap();
    std::fs::write(root.path().join(".swarmy/remote/test.profile.json"), serde_json::to_vec(&serde_json::json!({
        "name": "test", "socket_path": "test.socket", "pid": 1, "ports": {},
        "fdb_cluster_file": "test.cluster", "nats_url": "nats://localhost:4222", "s3_endpoint": "http://localhost:8333"
    })).unwrap()).unwrap();
    let ssh = root.path().join("ssh");
    std::fs::write(&ssh, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o700)).unwrap();
    let output = cli(root.path(), &["dev", "up", "--remote", "test"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("no local services started"));
    for service in ["supervisor", "worker", "gateway", "scheduler"] {
        assert!(
            !root
                .path()
                .join(format!(".swarmy/dev/{service}.pid"))
                .exists()
        );
    }
    let down = cli(root.path(), &["dev", "down"]);
    assert!(down.status.success());
    assert!(String::from_utf8_lossy(&down.stdout).contains("remote stack preserved"));
}

fn fake_upgrade_host(root: &Path) {
    let ssh = root.join("ssh");
    std::fs::write(
        &ssh,
        format!(
            r#"#!/bin/bash
last="${{@: -1}}"
printf '%s\n' "$last" >> '{}/ssh-commands'
case "$last" in
  'bash -s')
    file='{}/piped-'$(ls '{}/piped-'* 2>/dev/null | wc -l)
    cat > "$file"
    if ! grep -q '^list_installed_control_units$' "$file"; then printf '/home/fake\n'; fi ;;
  'uname -m') uname -m ;;
  *'swarmy-core --version'*) printf 'swarmy-core test\n' ;;
  *'remote-upgrade.sh'*) printf '{{"node":"","changed":[],"restarted":[],"elapsed_seconds":0}}\n' ;;
esac
"#,
            root.display(),
            root.display(),
            root.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o755)).unwrap();
    let rsync = root.join("rsync");
    std::fs::write(
        &rsync,
        format!(
            r#"#!/bin/bash
printf '%s\n' "$@" >> '{}/rsync-args'
mkdir -p '{}/uploaded-files' '{}/uploaded-release'
for a in "$@"; do
    if [[ -d $a ]]; then cp -R "$a/." '{}/uploaded-files/'; fi
    if [[ -f $a ]]; then cp "$a" '{}/uploaded-release/'; fi
done
"#,
            root.display(),
            root.display(),
            root.display(),
            root.display(),
            root.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&rsync, std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[test]
fn upgrade_without_a_checkout_uploads_node_files_and_release_binaries() {
    use swarmy_testkit::release::{fake_release, host_target, serve_files};
    let root = tempfile::tempdir().unwrap();
    fixture(root.path());
    fake_upgrade_host(root.path());
    let addr = serve_files(fake_release(
        swarmy_version::RELEASE_TAG,
        &host_target(),
        "#!/bin/sh\n",
    ));
    let output = cli_with_env(
        root.path(),
        &["remote", "upgrade", "test"],
        &[("SWARMY_RELEASE_BASE_URL", format!("http://{addr}"))],
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    assert_eq!(
        std::fs::read(root.path().join("uploaded-files/scripts/remote-upgrade.sh")).unwrap(),
        std::fs::read(repo.join("scripts/remote-upgrade.sh")).unwrap()
    );
    assert!(
        root.path()
            .join("uploaded-files/images/base-ubuntu/recipe.toml")
            .exists()
    );
    assert!(!root.path().join("uploaded-files/Cargo.toml").exists());
    assert!(!root.path().join("uploaded-files/crates").exists());
    for file in ["swarmy-core", "libfdb_c.so"] {
        assert!(root.path().join("uploaded-release").join(file).exists());
    }
    let args = std::fs::read_to_string(root.path().join("rsync-args")).unwrap();
    assert!(args.contains("--include=/scripts/***"));
    assert!(args.contains("/home/fake/swarmy/"));
    assert!(
        std::fs::read_to_string(root.path().join("ssh-commands"))
            .unwrap()
            .contains("bash scripts/remote-upgrade.sh node all 600 prebuilt")
    );
}

#[test]
fn development_client_outside_a_checkout_refuses_before_ssh() {
    let root = tempfile::tempdir().unwrap();
    fixture(root.path());
    fake_upgrade_host(root.path());
    let output = cli_command(root.path(), &["remote", "upgrade", "test"])
        .env_remove("SWARMY_RELEASE_BASE_URL")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("SWARMY_RELEASE_BASE_URL") && stderr.contains("checkout"),
        "{stderr}"
    );
    assert!(!root.path().join("ssh-commands").exists());
}
