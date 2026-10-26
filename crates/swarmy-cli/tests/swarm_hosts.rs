#![deny(clippy::disallowed_methods)]
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output},
};
use swarmy_testkit::release::{fake_release, host_target, serve_files};
struct Fixture {
    root: tempfile::TempDir,
    release_url: String,
    key: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let path = root.path();
        fs::create_dir_all(path.join("bin")).unwrap();
        fs::create_dir_all(path.join("config/swarmy")).unwrap();
        fs::write(
            path.join("config/swarmy/config.toml"),
            "[selection]\nprovider = 'fake'\n",
        )
        .unwrap();
        let key = path.join("bootstrap");
        assert!(
            Command::new("ssh-keygen")
                .args(["-q", "-t", "ed25519", "-N", "", "-f"])
                .arg(&key)
                .status()
                .unwrap()
                .success()
        );
        let ssh = format!(
            r#"#!/bin/bash
last="${{@: -1}}"
printf '%s\n' "$last" >> '{}/ssh-commands'
case "$last" in
  true) exit 0 ;;
  'bash -s') file='{}/piped-'$(ls '{}/piped-'* 2>/dev/null | wc -l); cat > "$file";
    if ! grep -q '^list_installed_control_units$' "$file"; then printf '/home/fake\n'; fi ;;
  'uname -m') /usr/bin/uname -m ;;
  'cat ~swarmy/swarmy/.dev/fdb.cluster') printf 'test:test@127.0.0.1:4500\n' ;;
  *'cat ~swarmy/swarmy/.swarmy/tunnel-key.pub') printf 'ssh-ed25519 AAAATEST test\n' ;;
  'cat /etc/ssh/ssh_host_ed25519_key.pub') printf 'ssh-ed25519 AAAATEST test\n' ;;
  'cat ~swarmy/swarmy/.swarmy/config.toml') printf '[api]\ntoken = "node-token"\n' ;;
  'sudo -n sh -c '*) cat >/dev/null ;;
esac
"#,
            path.display(),
            path.display(),
            path.display()
        );
        fs::write(path.join("bin/ssh"), ssh).unwrap();
        fs::write(path.join("bin/rsync"), "#!/bin/bash\nexit 0\n").unwrap();
        for program in ["ssh", "rsync"] {
            fs::set_permissions(
                path.join("bin").join(program),
                fs::Permissions::from_mode(0o755),
            )
            .unwrap();
        }
        let addr = serve_files(fake_release(
            swarmy_version::RELEASE_TAG,
            &host_target(),
            "#!/bin/sh\n",
        ));
        Self {
            root,
            release_url: format!("http://{addr}"),
            key,
        }
    }
    fn path(&self) -> &Path {
        self.root.path()
    }
    fn run(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_swarmy"))
            .env_clear()
            .current_dir(self.path())
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin", self.path().join("bin").display()),
            )
            .env("HOME", self.path().join("home"))
            .env("XDG_CONFIG_HOME", self.path().join("config"))
            .env("XDG_DATA_HOME", self.path().join("data"))
            .env("XDG_STATE_HOME", self.path().join("state"))
            .env("XDG_CACHE_HOME", self.path().join("cache"))
            .env("SWARMY_RELEASE_BASE_URL", &self.release_url)
            .args(args)
            .output()
            .unwrap()
    }
    fn ok(&self, args: &[&str]) -> Output {
        let output = self.run(args);
        assert!(
            output.status.success(),
            "{}: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }
    fn create(&self) {
        self.ok(&[
            "swarm",
            "create",
            "box",
            "--host",
            "203.0.113.10",
            "--ssh-key",
            self.key.to_str().unwrap(),
            "--local-storage",
            "dir:/srv/swarmy-local",
        ]);
    }
}
#[test]
fn create_up_status_stop_start_down_on_one_host() {
    let f = Fixture::new();
    f.create();
    let spec = fs::read_to_string(f.path().join("config/swarmy/swarms/box.toml")).unwrap();
    assert_eq!(
        swarmy_config::swarm::SwarmSpec::parse(&spec)
            .unwrap()
            .lifecycle,
        swarmy_config::swarm::Lifecycle::Persistent
    );
    let output = f.ok(&["--json", "swarm", "up", "box", "--no-image"]);
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["hosts"][0]["provisioned"], true);
    let commands = fs::read_to_string(f.path().join("ssh-commands")).unwrap();
    assert!(commands.contains("bash scripts/remote-provision.sh stack"));
    assert!(commands.contains("bash scripts/remote-services.sh"));
    let record_path = f.path().join("data/swarmy/swarms/box/record.toml");
    let record = fs::read_to_string(&record_path).unwrap();
    assert!(record.contains("node-token"));
    assert_eq!(
        fs::metadata(&record_path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(record_path.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(record_path.parent().unwrap().join("keys/box-box-1"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let shown = f.ok(&["--json", "swarm", "show", "box"]);
    assert!(!String::from_utf8_lossy(&shown.stdout).contains("node-token"));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&shown.stdout).unwrap()["kind"],
        "managed"
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &f.ok(&["--json", "swarm", "status", "box"]).stdout
        )
        .unwrap()["hosts"][0]["provisioned"],
        true
    );
    f.ok(&["swarm", "stop", "box"]);
    f.ok(&["swarm", "start", "box"]);
    let scripts = fs::read_dir(f.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with("piped-"))
                .then(|| fs::read_to_string(entry.path()).unwrap())
        })
        .collect::<String>();
    assert!(scripts.contains("stop_installed_units"));
    assert!(scripts.contains("start_installed_units"));
    assert!(
        String::from_utf8_lossy(&f.run(&["swarm", "down", "box"]).stderr).contains("--destroy")
    );
    f.ok(&["swarm", "down", "box", "--destroy"]);
    assert!(!record_path.parent().unwrap().exists());
    assert!(f.path().join("config/swarmy/swarms/box.toml").exists());
}
#[test]
fn node_add_provisions_only_the_new_host() {
    let f = Fixture::new();
    f.create();
    f.ok(&["swarm", "up", "box", "--no-image"]);
    let old = fs::read_to_string(f.path().join("ssh-commands")).unwrap();
    f.ok(&[
        "swarm",
        "node",
        "add",
        "box",
        "box",
        "--host",
        "203.0.113.11",
        "--ssh-key",
        f.key.to_str().unwrap(),
    ]);
    let commands = fs::read_to_string(f.path().join("ssh-commands")).unwrap();
    let added = &commands[old.len()..];
    assert_eq!(
        added
            .matches("bash scripts/remote-provision.sh node")
            .count(),
        1
    );
    assert!(!added.contains("remote-services.sh"));
    assert!(
        fs::read_to_string(f.path().join("data/swarmy/swarms/box/record.toml"))
            .unwrap()
            .contains("box-box-2")
    );
}
#[test]
fn up_refuses_bad_hosts_before_any_ssh() {
    let f = Fixture::new();
    f.create();
    let path = f.path().join("config/swarmy/swarms/box.toml");
    fs::write(
        &path,
        fs::read_to_string(&path)
            .unwrap()
            .replace(f.key.to_str().unwrap(), "relative"),
    )
    .unwrap();
    assert!(
        String::from_utf8_lossy(&f.run(&["swarm", "up", "box", "--no-image"]).stderr)
            .contains("must be an absolute path")
    );
    assert!(!f.path().join("ssh-commands").exists());
}
#[test]
fn module_groups_are_refused() {
    let f = Fixture::new();
    fs::create_dir_all(f.path().join("config/swarmy/swarms")).unwrap();
    fs::write(f.path().join("config/swarmy/swarms/box.toml"),"name = 'box'\nlifecycle = 'ephemeral'\n[groups.box]\nmodule = 'hetzner-cloud'\ncount = 1\nsize = 'small'\nlocation = 'test'\n[placement]\nstack = 'box'\nservices = 'box'\nnodes = ['box']\n").unwrap();
    assert!(
        String::from_utf8_lossy(&f.run(&["swarm", "up", "box", "--no-image"]).stderr)
            .contains("cannot create machines yet")
    );
}
