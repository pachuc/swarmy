use std::{fs, os::unix::fs::PermissionsExt, process::Command};

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
fn no_api_fails_without_service_lines_and_does_not_leak_secrets() {
    let fixture = Fixture::new();
    fixture.config("provider = 'fake'\n[api]\nurl = 'http://127.0.0.1:1'\ntoken = 'fixture'");
    let output = fixture
        .command(true)
        .env("OPENAI_API_KEY", "DO_NOT_PRINT")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(!String::from_utf8_lossy(&output.stdout).contains("DO_NOT_PRINT"));
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(check(&report, "API")["status"], "fail");
    assert!(
        !report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["name"] == "scheduler")
    );
}

#[test]
fn doctor_reports_no_client_library_check() {
    // The client links no database library, so doctor must not report a
    // libfdb_c check at all, even with an unloadable library on the path.
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
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(
        report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .all(|check| check["name"] != "libfdb_c")
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
        api_url: None,
        api_token: None,
        s3_bucket: None,
        s3_region: None,
        default_image: None,
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
fn remote_doctor_requires_api_even_with_healthy_tunnel() {
    let fixture = remote_fixture("test:test@127.0.0.1:4500", "nats://127.0.0.1:1");
    let output = fixture
        .command(true)
        .args(["--remote", "test"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(check(&report, "remote tunnel")["ok"], true);
    assert_eq!(check(&report, "API")["status"], "fail");
}

#[test]
fn reports_keyring_presence_and_permissions() {
    let fixture = Fixture::new();
    fixture.config("provider = 'fake'");
    let absent: Value = serde_json::from_slice(&fixture.doctor(true).stdout).unwrap();
    assert!(
        check(&absent, "keyring")["detail"]
            .as_str()
            .unwrap()
            .contains("absent")
    );
    let path = fixture.0.path().join(".swarmy/keyring");
    swarmy_config::Keyring::generate_at(&path).unwrap();
    let present: Value = serde_json::from_slice(&fixture.doctor(true).stdout).unwrap();
    assert_eq!(check(&present, "keyring")["ok"], true);
    assert!(
        check(&present, "keyring")["detail"]
            .as_str()
            .unwrap()
            .contains("mode 600")
    );
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    let invalid: Value = serde_json::from_slice(&fixture.doctor(true).stdout).unwrap();
    assert_eq!(check(&invalid, "keyring")["ok"], false);
}

fn api_fixture(scheduler_alive: bool) -> (Fixture, std::thread::JoinHandle<()>) {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let fixture = Fixture::new();
    fixture.config(&format!(
        "provider = 'fake'\n[api]\nurl = 'http://{}'\ntoken = 'fixture'\n",
        listener.local_addr().unwrap()
    ));
    let handle = std::thread::spawn(move || {
        for body in [
            format!(
                "{{\"version\":\"{}\",\"git_commit\":\"{}\"}}",
                swarmy_version::VERSION,
                swarmy_version::GIT_COMMIT
            ),
            format!(
                "{{\"services\":[{{\"role\":\"scheduler\",\"instance_id\":\"s1\",\"version\":\"0.1.0\",\"alive\":{scheduler_alive},\"providers\":[],\"capacity\":null}},{{\"role\":\"worker\",\"instance_id\":\"w1\",\"version\":\"0.1.0\",\"alive\":true,\"providers\":[],\"capacity\":null}},{{\"role\":\"gateway\",\"instance_id\":\"g1\",\"version\":\"0.1.0\",\"alive\":true,\"providers\":[\"fake\"],\"capacity\":null}}],\"images\":[\"fixture:test\"],\"default_image\":\"fixture:test\",\"credentials\":[]}}"
            ),
        ] {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).unwrap();
            write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
        }
    });
    (fixture, handle)
}

#[test]
fn api_snapshot_reports_live_services_and_scheduler_failure() {
    for alive in [true, false] {
        let (fixture, server) = api_fixture(alive);
        let output = fixture.doctor(true);
        server.join().unwrap();
        let report: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(check(&report, "API")["status"], "pass", "{report}");
        assert_eq!(
            check(&report, "scheduler")["status"],
            if alive { "pass" } else { "fail" }
        );
        assert_eq!(check(&report, "worker")["status"], "pass");
        assert_eq!(check(&report, "gateway")["status"], "pass");
        assert!(check(&report, "scheduler")["detail"].is_string());
        if !alive {
            assert!(!output.status.success());
            assert!(
                check(&report, "scheduler")["fix"]
                    .as_str()
                    .unwrap()
                    .contains("Restart")
            );
        }
    }
}

#[test]
fn api_check_accepts_same_major_api_despite_binary_drift() {
    use std::io::{Read, Write};
    for (api_version, status) in [("1.9.0", "pass"), ("2.0.0", "fail")] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let fixture = Fixture::new();
        fixture.config(&format!(
            "provider = 'fake'\n[api]\nurl = 'http://{}'\ntoken = 'fixture'\n",
            listener.local_addr().unwrap()
        ));
        let server = std::thread::spawn(move || {
            let health = format!(
                "{{\"version\":\"9.9.9\",\"git_commit\":\"other\",\
                \"api_version\":\"{api_version}\",\"services\":[],\"node_count\":0}}"
            );
            let snapshot = "{\"services\":[{\"role\":\"scheduler\",\"instance_id\":\"s1\",\
                \"version\":\"0.1.0\",\"alive\":true,\"providers\":[],\"capacity\":null}],\
                \"images\":[],\"default_image\":null,\"credentials\":[]}";
            for body in [health, snapshot.to_owned()] {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                let mut request = [0_u8; 4096];
                let _ = stream.read(&mut request).unwrap();
                write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
            }
        });
        let output = fixture.doctor(true);
        server.join().unwrap();
        let report: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(check(&report, "API")["status"], status, "{report}");
    }
}

#[test]
fn text_doctor_renders_ok_warn_fix_and_providers() {
    let (fixture, server) = api_fixture(false);
    let output = fixture.doctor(false);
    server.join().unwrap();
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains("ok API:"), "{text}");
    assert!(text.contains("warn nodes:"), "{text}");
    assert!(text.contains("fix scheduler:"), "{text}");
    assert!(text.contains("Providers:"), "{text}");
}
