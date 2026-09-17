use std::{
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Command, Output},
};

fn cli(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_swarmy"))
        .current_dir(root)
        .env("HOME", root)
        .env("XDG_CONFIG_HOME", root)
        .env("SWARMY_STATE_DIR", root.join(".swarmy"))
        .env_remove("SWARMY_REMOTE")
        .env("PATH", format!("{}:/usr/bin:/bin", root.display()))
        .args(args)
        .output()
        .unwrap()
}

fn fixture(root: &Path) {
    std::fs::create_dir_all(root.join(".swarmy/remote")).unwrap();
    std::fs::write(root.join(".swarmy/remote/test.json"), r#"{"name":"test","region":"local","instance_id":"i-test","public_ip":"127.0.0.1","private_ip":"127.0.0.1","key_path":"/tmp/a key","created_at":"now"}"#).unwrap();
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
    node["nodes"] = serde_json::json!([child]);
    std::fs::write(path, serde_json::to_vec(&node).unwrap()).unwrap();
    let output = cli(root.path(), &["remote", "status", "--json"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value[0]["name"], "test");
    assert_eq!(value[0]["nodes"][0]["instance_id"], "i-second");
    assert_eq!(
        value[0]["nodes"][0]["instance_state"],
        "unknown (SSH unreachable)"
    );
    assert_eq!(value[0]["tunnel"], false);
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
    let output = cli(root.path(), &["remote", "status", "--json"]);
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
        default_image: Some("base-ubuntu:test".into()),
    };
    std::fs::write(
        root.path().join(".swarmy/remote/test.profile.json"),
        serde_json::to_vec(&profile).unwrap(),
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
