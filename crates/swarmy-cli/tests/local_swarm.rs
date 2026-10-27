#![cfg(target_os = "linux")]
#![deny(clippy::disallowed_methods)]
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output},
};
use swarmy_config::{
    Settings,
    paths::Dirs,
    swarm::{Lifecycle, Registry, SwarmSpec},
};
use swarmy_testkit::FakeStack;
const CLI: &str = env!("CARGO_BIN_EXE_swarmy");
struct Fixture {
    root: tempfile::TempDir,
    names: Vec<String>,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        for dir in ["home", "config", "data", "state", "cache", "bin", "work"] {
            fs::create_dir(root.path().join(dir)).unwrap();
        }
        fs::write(root.path().join("core-identity"), swarmy_version::IDENTITY).unwrap();
        std::os::unix::fs::symlink(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/local-core"),
            root.path().join("bin/swarmy-core"),
        )
        .unwrap();
        Self {
            root,
            names: vec![],
        }
    }
    fn path(&self, p: &str) -> PathBuf {
        self.root.path().join(p)
    }
    fn isolated(&self, cmd: &mut Command, cwd: &Path) {
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("SWARMY_") {
                cmd.env_remove(key);
            }
        }
        cmd.current_dir(cwd)
            .env("HOME", self.path("home"))
            .env("XDG_CONFIG_HOME", self.path("config"))
            .env("XDG_DATA_HOME", self.path("data"))
            .env("XDG_STATE_HOME", self.path("state"))
            .env("XDG_CACHE_HOME", self.path("cache"))
            .env("SWARMY_CORE_TEST_IDENTITY", swarmy_version::IDENTITY)
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin", self.path("bin").display()),
            );
    }
    fn run(&self, cwd: &Path, args: &[&str]) -> Output {
        let mut cmd = Command::new(CLI);
        self.isolated(&mut cmd, cwd);
        cmd.args(args).output().unwrap()
    }
    fn ok(&self, cwd: &Path, args: &[&str]) -> String {
        let out = self.run(cwd, args);
        assert!(
            out.status.success(),
            "{}: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap()
    }
    fn create(&mut self, name: &str, extra: &[&str]) -> String {
        let args = [&["swarm", "create", name, "--local"][..], extra].concat();
        let out = self.ok(&self.path("work"), &args);
        self.names.push(name.into());
        out
    }
    fn registry(&self) -> Registry {
        Registry::new(Dirs {
            config: self.path("config/swarmy"),
            data: self.path("data/swarmy"),
            state: self.path("state/swarmy"),
            cache: self.path("cache/swarmy"),
        })
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        for name in &self.names {
            let _ = self.run(&self.path("work"), &["swarm", "down", name, "--destroy"]);
        }
    }
}
#[test]
fn create_writes_a_local_spec_and_selects_it() {
    let mut f = Fixture::new();
    assert!(f.create("demo", &[]).contains("swarmy swarm up demo"));
    let spec = f.registry().read_spec("demo").unwrap().unwrap();
    assert_eq!(
        spec,
        SwarmSpec::local("demo", Lifecycle::Ephemeral, None).unwrap()
    );
    assert_eq!(
        fs::metadata(f.path("config/swarmy/swarms/demo.toml"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(
        fs::read_to_string(f.path("state/swarmy/current")).unwrap(),
        "demo\n"
    );
    assert!(
        String::from_utf8_lossy(
            &f.run(&f.path("work"), &["swarm", "create", "demo", "--local"])
                .stderr
        )
        .contains("already exists")
    );
    f.create("p", &["--persistent", "--stack-dir", "rel"]);
    let p = f.registry().read_spec("p").unwrap().unwrap();
    assert_eq!(p.lifecycle, Lifecycle::Persistent);
    assert_eq!(p.local_group().unwrap().stack_dir, Some(f.path("work/rel")));
    assert_eq!(f.registry().current().unwrap().as_deref(), Some("demo"));
    assert_eq!(
        f.run(&f.path("work"), &["swarm", "create", "x"])
            .status
            .code(),
        Some(2)
    );
    let json = f.ok(
        &f.path("work"),
        &["--json", "swarm", "create", "j", "--local"],
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&json).unwrap()["created"],
        "j"
    );
    f.names.push("j".into());
}
fn verify_files(f: &Fixture, dir: &Path) -> swarmy_config::swarm::SwarmRecord {
    let settings = Settings::read(&f.path("data/swarmy/swarms/demo/services.toml")).unwrap();
    assert_eq!(settings.state_dir, f.path("state/swarmy/swarms/demo"));
    assert_eq!(settings.store.cluster_file, dir.join("fdb.cluster"));
    assert_eq!(
        settings.selection.credential_file,
        f.path("data/swarmy/auth.json")
    );
    assert_eq!(
        settings.fake.script,
        f.path("state/swarmy/swarms/demo/fake.json")
    );
    assert!(settings.fake.script.exists());
    let record = f.registry().read_record("demo").unwrap().unwrap();
    assert_eq!(record.api.url.as_deref(), Some("http://127.0.0.1:8742"));
    assert_eq!(
        record.api.token.as_deref(),
        Some(settings.api.token.as_str())
    );
    assert_eq!(
        fs::metadata(f.path("data/swarmy/swarms/demo/record.toml"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(f.path("data/swarmy/swarms/demo"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(f.path("data/swarmy/keyring"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert!(f.path("state/swarmy/swarms/demo/supervisor.pid").exists());
    assert!(!f.path("work/.swarmy").exists());
    assert_eq!(fs::read_dir(f.path("home")).unwrap().count(), 0);
    let show = f.ok(&f.path("work"), &["--json", "swarm", "show", "demo"]);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&show).unwrap()["api_url"],
        "http://127.0.0.1:8742"
    );
    assert!(!show.contains(&settings.api.token));
    record
}
#[test]
fn lifecycle_without_a_checkout() {
    let mut f = Fixture::new();
    f.create("demo", &[]);
    let dir = f.path("data/swarmy/swarms/demo/stack");
    let mut stack = FakeStack::start(&dir);
    let up = f.ok(&f.path("work"), &["swarm", "up", "demo", "--no-image"]);
    assert!(
        up.contains(&format!(
            "stack: ready (fdb={}, nats={}, seaweed={})",
            stack.pid("fdb"),
            stack.pid("nats"),
            stack.pid("seaweed")
        )),
        "{up}"
    );
    assert!(
        up.contains("api: ready") && up.contains("swarm demo: API http://127.0.0.1:8742"),
        "{up}"
    );
    let record = verify_files(&f, &dir);
    let status = f.ok(&f.path("home"), &["swarm", "status", "demo"]);
    assert!(
        status.contains("swarm: demo (local, ephemeral)")
            && status.contains("stack: fdb pid")
            && status.contains("api: up")
    );
    let mut log_command = Command::new("timeout");
    f.isolated(&mut log_command, &f.path("work"));
    let logs = log_command
        .args(["-s", "INT", "2", CLI, "swarm", "logs", "demo", "api"])
        .output()
        .unwrap();
    assert!(String::from_utf8_lossy(&logs.stdout).contains("api ready"));
    f.ok(&f.path("work"), &["swarm", "stop", "demo"]);
    assert!(!stack.running());
    assert!(!f.path("state/swarmy/swarms/demo/supervisor.pid").exists());
    assert!(f.registry().read_record("demo").unwrap().is_some());
    let _stack = FakeStack::start(&dir);
    f.ok(&f.path("work"), &["swarm", "start", "demo"]);
    assert_eq!(
        f.registry().read_record("demo").unwrap().unwrap().api.token,
        record.api.token
    );
    let down = f.ok(&f.path("work"), &["swarm", "down", "demo"]);
    assert!(down.contains("services and stack: down") && down.contains("data deleted"));
    assert!(!f.path("data/swarmy/swarms/demo").exists());
    assert!(!f.path("state/swarmy/swarms/demo").exists());
    assert!(f.path("config/swarmy/swarms/demo.toml").exists());
}
#[test]
fn guards_and_lifecycle_errors() {
    let mut f = Fixture::new();
    f.create("fresh", &[]);
    assert!(
        String::from_utf8_lossy(&f.run(&f.path("work"), &["swarm", "start", "fresh"]).stderr)
            .contains("has not been brought up")
    );
    f.create("p", &["--persistent"]);
    assert!(
        String::from_utf8_lossy(&f.run(&f.path("work"), &["swarm", "down", "p"]).stderr)
            .contains("--destroy")
    );
    f.ok(&f.path("work"), &["swarm", "down", "p", "--destroy"]);
    assert!(
        String::from_utf8_lossy(
            &f.run(&f.path("work"), &["--json", "swarm", "status", "nope"])
                .stderr
        )
        .contains("--json is not supported for swarm")
    );
    f.create("a", &[]);
    f.create("b", &[]);
    let _stack = FakeStack::start(&f.path("data/swarmy/swarms/a/stack"));
    f.ok(&f.path("work"), &["swarm", "up", "a", "--no-image"]);
    assert!(
        String::from_utf8_lossy(
            &f.run(&f.path("work"), &["swarm", "up", "b", "--no-image"])
                .stderr
        )
        .contains("local swarm \"a\" is running")
    );
    assert!(!f.path("state/swarmy/swarms/b/supervisor.pid").exists());
    assert!(
        String::from_utf8_lossy(&f.run(&f.path("work"), &["swarm", "rm", "a"]).stderr)
            .contains("is running on this machine")
    );
    f.ok(&f.path("work"), &["swarm", "stop", "a"]);
    f.ok(&f.path("work"), &["swarm", "rm", "a"]);
    assert!(!f.path("data/swarmy/swarms/a").exists());
    assert!(!f.path("state/swarmy/swarms/a").exists());
}
#[test]
fn checkout_uses_its_dev_stack() {
    let mut f = Fixture::new();
    fs::create_dir_all(f.path("repo/scripts")).unwrap();
    fs::create_dir_all(f.path("repo/sub")).unwrap();
    fs::write(
        f.path("repo/scripts/dev-stack.sh"),
        format!("#!/bin/sh\ntouch {}\n", f.path("repo/marker-ran").display()),
    )
    .unwrap();
    let out = f.ok(&f.path("repo/sub"), &["swarm", "create", "co", "--local"]);
    f.names.push("co".into());
    assert!(out.contains("the checkout's .dev"));
    assert_eq!(
        f.registry()
            .read_spec("co")
            .unwrap()
            .unwrap()
            .local_group()
            .unwrap()
            .stack_dir,
        Some(f.path("repo/.dev"))
    );
    let mut stack = FakeStack::start(&f.path("repo/.dev"));
    f.ok(&f.path("repo/sub"), &["swarm", "up", "co", "--no-image"]);
    assert!(!f.path("repo/.swarmy").exists() && !f.path("repo/marker-ran").exists());
    assert!(!f.path("data/swarmy/swarms/co/stack").exists());
    let out = f.ok(&f.path("repo/sub"), &["swarm", "down", "co"]);
    assert!(!stack.running());
    assert!(f.path("repo/.dev").exists());
    assert!(out.contains("stack directory"));
}
#[test]
fn non_local_swarm_is_refused() {
    let f = Fixture::new();
    let spec = SwarmSpec::parse(
        r#"
name = "box"
lifecycle = "ephemeral"
[groups.box]
module = "existing"
hosts = [{ address = "203.0.113.10", ssh_user = "root", ssh_key = "/keys/box" }]
[placement]
stack = "box"
services = "box"
nodes = ["box"]
"#,
    )
    .unwrap();
    f.registry().write_spec(&spec).unwrap();
    let error =
        String::from_utf8_lossy(&f.run(&f.path("work"), &["swarm", "status", "box"]).stderr)
            .into_owned();
    assert!(error.contains("has no hosts recorded"), "{error}");
}
