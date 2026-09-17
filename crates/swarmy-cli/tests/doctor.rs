use std::{fs, net::TcpListener, os::unix::fs::PermissionsExt, process::Command};

use serde_json::Value;

struct Fixture(tempfile::TempDir);

impl Fixture {
    fn new() -> Self {
        let fixture = Self(tempfile::tempdir().unwrap());
        fs::create_dir(fixture.0.path().join(".swarmy")).unwrap();
        fixture
    }

    fn config(&self, content: &str) {
        fs::write(self.0.path().join(".swarmy/config.toml"), content).unwrap();
    }

    fn doctor(&self, json: bool) -> std::process::Output {
        self.command(json).output().unwrap()
    }

    fn command(&self, json: bool) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_swarmy"));
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("SWARMY_") {
                command.env_remove(key);
            }
        }
        command
            .current_dir(self.0.path())
            .env("HOME", self.0.path())
            .env("XDG_CONFIG_HOME", self.0.path().join("config"))
            .env("PATH", self.0.path().join("bin"))
            .arg("doctor");
        if json {
            command.arg("--json");
        }
        command
    }
}

fn check<'a>(report: &'a Value, name: &str) -> &'a Value {
    report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["name"] == name)
        .unwrap()
}

#[test]
fn reports_missing_and_invalid_config_without_leaking_values() {
    let fixture = Fixture::new();
    let output = fixture.doctor(true);
    assert_eq!(output.status.code(), Some(1));
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(check(&report, "config")["ok"], false);
    assert!(
        check(&report, "config")["fix"]
            .as_str()
            .unwrap()
            .contains("swarmy dev up")
    );
    fixture.config("s3_secret_key = 'DO_NOT_PRINT'\ngateway_concurrency = 'DO_NOT_PRINT'");
    let output = fixture.doctor(false);
    assert_eq!(output.status.code(), Some(1));
    assert!(!String::from_utf8_lossy(&output.stdout).contains("DO_NOT_PRINT"));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("DO_NOT_PRINT"));
    assert!(String::from_utf8_lossy(&output.stdout).contains("fix config:"));
}

#[test]
fn validates_chatgpt_credentials_without_printing_secrets() {
    let fixture = Fixture::new();
    fixture.config("provider = 'chatgpt'\ncredential_file = 'auth.json'");
    let output = fixture.doctor(true);
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(check(&report, "credentials")["ok"], false);
    fs::write(
        fixture.0.path().join("auth.json"),
        r#"{"auth_mode":"chatgpt","tokens":{"access_token":"DO_NOT_PRINT"}}"#,
    )
    .unwrap();
    let output = fixture.doctor(true);
    assert!(!String::from_utf8_lossy(&output.stdout).contains("DO_NOT_PRINT"));
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(check(&report, "credentials")["ok"], false);
    let credentials = include_bytes!("../../swarmy-llm/tests/fixtures/auth.json");
    let auth = fixture.0.path().join("auth.json");
    fs::write(&auth, credentials).unwrap();
    let report: Value = serde_json::from_slice(&fixture.doctor(true).stdout).unwrap();
    assert_eq!(check(&report, "credentials")["ok"], true);
    assert_eq!(fs::read(auth).unwrap(), credentials);
    fixture.config("provider = 'fake'");
    let report: Value = serde_json::from_slice(&fixture.doctor(true).stdout).unwrap();
    assert!(
        !report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check["name"] == "credentials")
    );
}

#[test]
fn finds_home_tools_but_tcp_listeners_do_not_prove_service_usability() {
    let fixture = Fixture::new();
    let bin = fixture.0.path().join(".local/bin");
    fs::create_dir_all(&bin).unwrap();
    for name in ["fdbserver", "fdbcli", "nats-server", "weed"] {
        let path = bin.join(name);
        fs::write(&path, "#!/bin/sh\necho test-version\n").unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }
    let fdb = TcpListener::bind("127.0.0.1:0").unwrap();
    let nats = TcpListener::bind("127.0.0.1:0").unwrap();
    let s3 = TcpListener::bind("127.0.0.1:0").unwrap();
    fs::create_dir(fixture.0.path().join(".dev")).unwrap();
    fs::write(
        fixture.0.path().join(".dev/fdb.cluster"),
        format!("test:test@{}", fdb.local_addr().unwrap()),
    )
    .unwrap();
    fixture.config(&format!(
        "nats_url = 'nats://{}'\ns3_endpoint = 'http://{}'",
        nats.local_addr().unwrap(),
        s3.local_addr().unwrap()
    ));
    let report: Value = serde_json::from_slice(&fixture.doctor(true).stdout).unwrap();
    assert_eq!(check(&report, "config")["ok"], true);
    assert!(
        check(&report, "config")["detail"]
            .as_str()
            .unwrap()
            .contains(fixture.0.path().to_str().unwrap())
    );
    for name in ["fdbserver", "fdbcli", "nats-server", "weed"] {
        assert_eq!(check(&report, name)["ok"], true);
        assert!(
            check(&report, name)["detail"]
                .as_str()
                .unwrap()
                .contains("test-version")
        );
    }
    assert_eq!(check(&report, "dev stack S3")["ok"], true);
    for name in ["dev stack FoundationDB", "dev stack NATS"] {
        assert_eq!(check(&report, name)["ok"], false);
    }
    drop((fdb, nats, s3));
    let output = fixture.doctor(true);
    assert_eq!(output.status.code(), Some(1));
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    for name in ["dev stack FoundationDB", "dev stack NATS", "dev stack S3"] {
        assert_eq!(check(&report, name)["ok"], false);
        assert!(
            check(&report, name)["fix"]
                .as_str()
                .unwrap()
                .contains("swarmy dev up")
        );
    }
}

#[test]
#[cfg(target_os = "linux")]
fn doctor_starts_even_when_the_client_library_cannot_load() {
    let fixture = Fixture::new();
    fixture.config("provider = 'fake'");
    fs::write(
        fixture.0.path().join("libfdb_c.so"),
        "invalid client library",
    )
    .unwrap();
    let output = fixture
        .command(true)
        .env("LD_LIBRARY_PATH", fixture.0.path())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(check(&report, "libfdb_c")["ok"], false);
    assert!(
        check(&report, "libfdb_c")["fix"]
            .as_str()
            .unwrap()
            .contains("scripts/install-dev-tools.sh")
    );
    assert_eq!(check(&report, "config")["ok"], true);
}

fn remote_fixture(cluster: &str, nats_url: &str) -> Fixture {
    let fixture = Fixture::new();
    fixture.config("provider = 'fake'");
    let remote = fixture.0.path().join(".swarmy/remote");
    fs::create_dir_all(&remote).unwrap();
    let cluster_path = remote.join("test.cluster");
    fs::write(&cluster_path, cluster).unwrap();
    let profile = swarmy_config::RemoteProfile {
        name: "test".into(),
        socket_path: remote.join("socket"),
        pid: 123,
        ports: swarmy_config::RemotePorts::default(),
        remote_ports: swarmy_config::RemotePorts::default(),
        fdb_cluster_file: cluster_path,
        nats_url: nats_url.into(),
        s3_endpoint: "http://127.0.0.1:8333".into(),
    };
    fs::write(
        remote.join("test.profile.json"),
        serde_json::to_vec(&profile).unwrap(),
    )
    .unwrap();
    let bin = fixture.0.path().join("bin");
    fs::create_dir(&bin).unwrap();
    fs::write(bin.join("ssh"), "#!/bin/sh\nexit 0\n").unwrap();
    fs::set_permissions(bin.join("ssh"), fs::Permissions::from_mode(0o700)).unwrap();
    fixture
}

#[test]
fn remote_doctor_rejects_unusable_database_even_with_healthy_tunnel_and_open_port() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let fixture = remote_fixture(
        &format!("test:test@{}", listener.local_addr().unwrap()),
        "nats://127.0.0.1:1",
    );
    let output = fixture
        .command(true)
        .args(["--remote", "test"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(check(&report, "remote tunnel")["ok"], true);
    assert_eq!(check(&report, "remote FoundationDB")["ok"], false);
    assert!(
        check(&report, "remote FoundationDB")["detail"]
            .as_str()
            .unwrap()
            .contains("transaction")
    );
}

#[test]
fn remote_doctor_proves_real_database_and_nats_through_profile() {
    let (Ok(cluster_path), Ok(nats_url)) = (
        std::env::var("SWARMY_FDB_CLUSTER_FILE"),
        std::env::var("SWARMY_NATS_URL"),
    ) else {
        eprintln!("Skipping real doctor probes: start dev stack and source .dev/env");
        return;
    };
    let fixture = remote_fixture(&fs::read_to_string(cluster_path).unwrap(), &nats_url);
    let output = fixture
        .command(true)
        .args(["--remote", "test"])
        .output()
        .unwrap();
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(output.status.success(), "{report}");
    for name in ["remote tunnel", "remote FoundationDB", "remote NATS"] {
        assert_eq!(check(&report, name)["ok"], true, "{report}");
    }
    assert!(
        check(&report, "remote FoundationDB")["detail"]
            .as_str()
            .unwrap()
            .contains("transaction succeeded")
    );
    assert!(
        check(&report, "remote NATS")["detail"]
            .as_str()
            .unwrap()
            .contains("round trip succeeded")
    );
}
