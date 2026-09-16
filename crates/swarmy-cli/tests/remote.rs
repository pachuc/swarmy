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
    let output = cli(root.path(), &["remote", "status", "--json"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value[0]["name"], "test");
    assert_eq!(value[0]["tunnel"], false);
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
