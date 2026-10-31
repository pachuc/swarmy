#![deny(clippy::disallowed_methods)]
#[path = "support/fake_api.rs"]
mod fake_api;
use std::{
    fs,
    io::Write,
    os::unix::fs::PermissionsExt,
    process::{Command, Output, Stdio},
};
use swarmy_config::{
    paths::Dirs,
    swarm::{RecordedHost, Registry, SwarmApi, SwarmRecord},
};

struct Fixture(tempfile::TempDir);
impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        for dir in ["home", "config", "data", "state", "cache"] {
            fs::create_dir(temp.path().join(dir)).unwrap();
        }
        Self(temp)
    }
    fn dirs(&self) -> Dirs {
        let root = self.0.path();
        Dirs::from_env(
            &[
                ("HOME".into(), root.join("home").display().to_string()),
                (
                    "XDG_CONFIG_HOME".into(),
                    root.join("config").display().to_string(),
                ),
                (
                    "XDG_DATA_HOME".into(),
                    root.join("data").display().to_string(),
                ),
                (
                    "XDG_STATE_HOME".into(),
                    root.join("state").display().to_string(),
                ),
                (
                    "XDG_CACHE_HOME".into(),
                    root.join("cache").display().to_string(),
                ),
            ]
            .into(),
        )
        .unwrap()
    }
    fn registry(&self) -> Registry {
        Registry::new(self.dirs())
    }
    fn command(&self) -> Command {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_swarmy"));
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("SWARMY_") {
                cmd.env_remove(key);
            }
        }
        let root = self.0.path();
        cmd.env("HOME", root.join("home"))
            .env("XDG_CONFIG_HOME", root.join("config"))
            .env("XDG_DATA_HOME", root.join("data"))
            .env("XDG_STATE_HOME", root.join("state"))
            .env("XDG_CACHE_HOME", root.join("cache"));
        cmd
    }
    fn attach(&self, name: &str, url: &str, token: &str) -> Output {
        let mut child = self
            .command()
            .args(["swarm", "attach", name, "--url", url, "--token-stdin"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(token.as_bytes())
            .unwrap();
        child.wait_with_output().unwrap()
    }
}
#[test]
fn precedence_table() {
    let fixture = Fixture::new();
    let stubs = [
        fake_api::serve(true),
        fake_api::serve(true),
        fake_api::serve(true),
        fake_api::serve(true),
    ];
    let urls: Vec<_> = stubs
        .iter()
        .map(|(addr, _)| format!("http://{addr}"))
        .collect();
    for (name, index) in [("flag", 0), ("env", 1), ("cur", 3)] {
        assert!(
            fixture
                .attach(name, &urls[index], &format!("token-{name}"))
                .status
                .success()
        );
    }
    assert!(
        fixture
            .command()
            .args(["swarm", "use", "cur"])
            .output()
            .unwrap()
            .status
            .success()
    );
    for (flag, env, pair, expected) in [
        (Some("flag"), Some("env"), Some(true), 0),
        (None, Some("env"), Some(true), 1),
        (None, None, Some(true), 2),
        (None, None, None, 3),
        (None, Some(""), Some(true), 2),
        (None, None, Some(false), 3),
    ] {
        let mut command = fixture.command();
        command.args(["--json", "image", "show", "base-ubuntu:v1"]);
        if let Some(flag) = flag {
            command.args(["--swarm", flag]);
        }
        if let Some(env) = env {
            command.env("SWARMY_SWARM", env);
        }
        if let Some(pair) = pair {
            command
                .env("SWARMY_API_URL", if pair { &urls[2] } else { "" })
                .env("SWARMY_API_TOKEN", "token-pair");
        }
        let before: Vec<_> = stubs
            .iter()
            .map(|(_, requests)| requests.lock().unwrap().len())
            .collect();
        let _output = command.output().unwrap();
        for (index, (_, requests)) in stubs.iter().enumerate() {
            let requests = requests.lock().unwrap();
            assert_eq!(
                requests.len(),
                before[index] + usize::from(index == expected),
                "row expected {expected}, stub {index}"
            );
            if index == expected {
                assert_eq!(
                    requests.last().unwrap().authorization.as_deref(),
                    Some(
                        format!("Bearer token-{}", ["flag", "env", "pair", "cur"][expected])
                            .as_str()
                    )
                );
            }
        }
    }
}
#[test]
fn incomplete_endpoint_beats_current_and_no_target_names_fix() {
    let fixture = Fixture::new();
    let (address, requests) = fake_api::serve(true);
    assert!(
        fixture
            .attach("cur", &format!("http://{address}"), "secret")
            .status
            .success()
    );
    assert!(
        fixture
            .command()
            .args(["swarm", "use", "cur"])
            .output()
            .unwrap()
            .status
            .success()
    );
    let output = fixture
        .command()
        .args(["image", "show", "base-ubuntu:v1"])
        .env("SWARMY_API_URL", "http://127.0.0.1:1")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("SWARMY_API_TOKEN is empty"));
    assert!(requests.lock().unwrap().is_empty());
    let fresh = Fixture::new();
    let output = fresh
        .command()
        .args(["image", "show", "base-ubuntu:v1"])
        .output()
        .unwrap();
    let error = String::from_utf8_lossy(&output.stderr);
    for hint in ["swarmy swarm ls", "swarmy swarm use", "swarmy swarm create"] {
        assert!(error.contains(hint), "{error}");
    }
}
#[test]
fn unknown_invalid_and_stale_names() {
    let fixture = Fixture::new();
    let output = fixture
        .command()
        .args(["--swarm", "nope", "image", "show", "base-ubuntu:v1"])
        .output()
        .unwrap();
    assert!(String::from_utf8_lossy(&output.stderr).contains("no swarm named \"nope\""));
    let output = fixture
        .command()
        .args(["--swarm", "../x", "image", "show", "base-ubuntu:v1"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("1-64"));
    fs::create_dir_all(fixture.dirs().state.join("swarmy")).unwrap();
    fs::write(fixture.dirs().current(), "gone\n").unwrap();
    let output = fixture
        .command()
        .args(["image", "show", "base-ubuntu:v1"])
        .output()
        .unwrap();
    assert!(String::from_utf8_lossy(&output.stderr).contains("swarmy swarm use NAME"));
}
#[test]
fn ls_show_use_rm_round_trip() {
    let f = Fixture::new();
    for name in ["a", "b"] {
        assert!(
            f.attach(name, "https://example.test", &format!("secret-{name}"))
                .status
                .success()
        );
    }
    let ls = f
        .command()
        .args(["swarm", "ls", "--json"])
        .output()
        .unwrap();
    let entries: serde_json::Value = serde_json::from_slice(&ls.stdout).unwrap();
    assert_eq!(
        entries
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["name"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    assert!(!String::from_utf8_lossy(&ls.stdout).contains("secret-"));
    assert!(
        entries
            .as_array()
            .unwrap()
            .iter()
            .all(|entry| entry["current"] == false)
    );
    let use_output = f.command().args(["swarm", "use", "b"]).output().unwrap();
    assert!(use_output.status.success());
    assert!(!String::from_utf8_lossy(&use_output.stdout).contains("secret-"));
    let ls_current = f.command().args(["swarm", "ls"]).output().unwrap();
    assert!(ls_current.status.success());
    assert!(
        String::from_utf8_lossy(&ls_current.stdout)
            .lines()
            .any(|line| line.starts_with("* b "))
    );
    assert!(!String::from_utf8_lossy(&ls_current.stdout).contains("secret-"));
    let show = f
        .command()
        .args(["swarm", "show", "--json"])
        .output()
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&show.stdout).unwrap()["name"],
        "b"
    );
    assert!(!String::from_utf8_lossy(&show.stdout).contains("secret-"));
    let show = f
        .command()
        .args(["swarm", "show", "a", "--json"])
        .output()
        .unwrap();
    let show_text = String::from_utf8(show.stdout).unwrap();
    assert!(show_text.contains("attached"));
    assert!(!show_text.contains("secret"));
    let removed = f.command().args(["swarm", "rm", "b"]).output().unwrap();
    assert!(removed.status.success());
    assert!(!String::from_utf8_lossy(&removed.stdout).contains("secret-"));
    assert_eq!(f.registry().current().unwrap(), None);
    let missing = f.command().args(["swarm", "rm", "b"]).output().unwrap();
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("no swarm named"));
    assert!(!String::from_utf8_lossy(&missing.stderr).contains("secret-"));
}
#[test]
fn attach_is_private_and_validated() {
    let f = Fixture::new();
    assert!(
        f.attach("a", "https://example.test", "secret")
            .status
            .success()
    );
    let r = f.registry();
    assert_eq!(
        fs::metadata(r.record_path("a").unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(r.swarm_dir("a").unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    let empty = f.attach("empty", "https://example.test", "");
    assert!(!empty.status.success());
    assert!(String::from_utf8_lossy(&empty.stderr).contains("--token-stdin"));
    assert!(!f.attach("ftp", "ftp://x", "secret").status.success());
    let spec = "name = 'm'\nlifecycle = 'persistent'\n[groups.box]\nmodule = 'existing'\n[[groups.box.hosts]]\naddress = '203.0.113.10'\nssh_user = 'root'\nssh_key = '/keys/box'\n[placement]\nstack = 'box'\nservices = 'box'\nnodes = ['box']\n";
    fs::create_dir_all(r.dirs().specs()).unwrap();
    fs::write(r.spec_path("m").unwrap(), spec).unwrap();
    let managed = f.attach("m", "https://example.test", "secret");
    assert!(!managed.status.success());
    assert!(String::from_utf8_lossy(&managed.stderr).contains("managed by swarmy"));
    assert_eq!(fs::read_dir(f.0.path().join("home")).unwrap().count(), 0);
}
#[test]
fn rm_refuses_machines_without_force() {
    let f = Fixture::new();
    let r = f.registry();
    r.write_record(&SwarmRecord {
        bucket_keys: None,
        name: "m".into(),
        created_at: "2026-10-26T00:00:00Z".parse().unwrap(),
        api: SwarmApi::default(),
        hosts: [(
            "box".into(),
            vec![RecordedHost {
                provisioned: false,
                name: "box".into(),
                public_address: "203.0.113.10".into(),
                private_address: None,
                ssh_user: "root".into(),
                ssh_key: "/keys/box".into(),
            }],
        )]
        .into(),
        tofu_state: None,
        tofu_bucket: None,
        tofu_bucket_endpoint: None,
        tofu_bucket_region: None,
        tofu_networks: BTreeMap::new(),
    })
    .unwrap();
    let output = f.command().args(["swarm", "rm", "m"]).output().unwrap();
    assert!(String::from_utf8_lossy(&output.stderr).contains("--force"));
    assert!(
        f.command()
            .args(["swarm", "rm", "m", "--force"])
            .output()
            .unwrap()
            .status
            .success()
    );
}
