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
    if grep -q '^list_installed_control_units$' "$file"; then printf 'swarmy-api.service\n'; else printf '/home/fake\n'; fi ;;
  'uname -m') /usr/bin/uname -m ;;
  *'swarmy-core --version'*) printf 'swarmy-core test\n' ;;
  *'journalctl --unit swarmyd --follow --no-pager --lines 100'*) if [ -e '{}/fail-journal' ]; then echo journal-failed >&2; exit 23; fi; echo remote-output ;;
  *'remote-upgrade.sh'*) if [ -e '{}/fail-upgrade-primary' ] && [[ " $* " == *203.0.113.10* ]]; then echo upgrade-failed >&2; exit 23; fi; printf '{{"node":"","changed":[],"restarted":[],"elapsed_seconds":0}}\n' ;;
  'cat ~swarmy/swarmy/.dev/fdb.cluster') printf 'test:test@127.0.0.1:4500\n' ;;
  *'cat ~swarmy/swarmy/.swarmy/tunnel-key.pub') printf 'ssh-ed25519 AAAATEST test\n' ;;
  'cat /etc/ssh/ssh_host_ed25519_key.pub') printf 'ssh-ed25519 AAAATEST test\n' ;;
  'cat ~swarmy/swarmy/.swarmy/config.toml') printf '[api]\ntoken = "node-token"\n' ;;
  'sudo -n sh -c '*) cat >/dev/null ;;
  *) echo remote-output ;;
esac
"#,
            path.display(),
            path.display(),
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
    assert!(!f.path().join("home").exists());
    let parsed = swarmy_config::swarm::SwarmSpec::parse(&spec).unwrap();
    assert_eq!(parsed.groups.len(), 1);
    assert_eq!(
        parsed.groups["box"].module,
        swarmy_config::swarm::Module::Existing
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
        .filter_map(Result::ok)
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
    let output = f.ok(&[
        "--json",
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
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["hosts"][0]["provisioned"], true);
    assert_eq!(value["hosts"][1]["provisioned"], true);
    assert_eq!(value["hosts"][1]["name"], "box-box-2");
    let commands = fs::read_to_string(f.path().join("ssh-commands")).unwrap();
    let added = &commands[old.len()..];
    assert_eq!(
        added
            .matches("bash scripts/remote-provision.sh node")
            .count(),
        1
    );
    assert!(!added.contains("remote-services.sh"));
    let spec = fs::read_to_string(f.path().join("config/swarmy/swarms/box.toml")).unwrap();
    assert_eq!(
        swarmy_config::swarm::SwarmSpec::parse(&spec)
            .unwrap()
            .groups["box"]
            .hosts
            .len(),
        2
    );
    let record = fs::read_to_string(f.path().join("data/swarmy/swarms/box/record.toml")).unwrap();
    assert!(record.contains("box-box-2"));
    let parsed: swarmy_config::swarm::SwarmRecord =
        swarmy_config::swarm::SwarmRecord::parse(&record).unwrap();
    assert!(parsed.hosts["box"][1].provisioned);
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

#[test]
fn invalid_host_inputs_leave_specs_unchanged() {
    let f = Fixture::new();
    let create = f.run(&[
        "swarm",
        "create",
        "box",
        "--host",
        "myhost.example",
        "--ssh-key",
        f.key.to_str().unwrap(),
        "--local-storage",
        "dir:/tmp",
    ]);
    assert!(!create.status.success());
    assert!(String::from_utf8_lossy(&create.stderr).contains("IPv4 address"));
    assert!(!f.path().join("config/swarmy/swarms/box.toml").exists());
    f.create();
    f.ok(&["swarm", "up", "box", "--no-image"]);
    let path = f.path().join("config/swarmy/swarms/box.toml");
    let before = fs::read_to_string(&path).unwrap();
    let add = f.run(&[
        "swarm",
        "node",
        "add",
        "box",
        "box",
        "--host",
        "myhost.example",
        "--ssh-key",
        f.key.to_str().unwrap(),
    ]);
    assert!(!add.status.success());
    assert!(String::from_utf8_lossy(&add.stderr).contains("IPv4 address"));
    assert_eq!(fs::read_to_string(path).unwrap(), before);
}
#[test]
fn host_only_flags_are_rejected_for_local_create() {
    let f = Fixture::new();
    for flag in ["--lifecycle", "--ssh-key", "--local-storage"] {
        let value = if flag == "--lifecycle" {
            "ephemeral"
        } else if flag == "--ssh-key" {
            f.key.to_str().unwrap()
        } else {
            "dir:/tmp"
        };
        assert!(
            !f.run(&["swarm", "create", "box", "--local", flag, value])
                .status
                .success()
        );
    }
    for flag in ["--persistent", "--stack-dir"] {
        let mut args = vec![
            "swarm",
            "create",
            "box",
            "--host",
            "203.0.113.10",
            "--ssh-key",
            f.key.to_str().unwrap(),
            flag,
        ];
        if flag == "--stack-dir" {
            args.push("/tmp");
        }
        assert!(!f.run(&args).status.success());
    }
}
#[test]
fn lifecycle_uses_record_after_bad_spec_edit() {
    let f = Fixture::new();
    f.create();
    f.ok(&["swarm", "up", "box", "--no-image"]);
    let path = f.path().join("config/swarmy/swarms/box.toml");
    let original = fs::read_to_string(&path).unwrap();
    fs::write(
        &path,
        original.replace("203.0.113.10", "not-an-ipv4-address"),
    )
    .unwrap();
    assert!(
        String::from_utf8_lossy(&f.run(&["swarm", "up", "box", "--no-image"]).stderr)
            .contains("IPv4 address")
    );
    f.ok(&["swarm", "status", "box"]);
    f.ok(&["swarm", "stop", "box"]);
    f.ok(&["swarm", "start", "box"]);
    f.ok(&["swarm", "down", "box", "--destroy"]);
    assert!(!f.path().join("data/swarmy/swarms/box/record.toml").exists());
}

#[test]
fn logs_passes_fixed_journal_command_and_preserves_exit_failure() {
    let f = Fixture::new();
    f.create();
    f.ok(&["swarm", "up", "box", "--no-image"]);
    let out = f.ok(&["swarm", "logs", "box"]);
    assert!(String::from_utf8_lossy(&out.stdout).contains("remote-output"));
    let commands = fs::read_to_string(f.path().join("ssh-commands")).unwrap();
    assert!(commands.contains("journalctl --unit swarmyd --follow --no-pager --lines 100"));
    assert!(
        !f.run(&["swarm", "logs", "box", "--unit", "invalid"])
            .status
            .success()
    );
    fs::write(f.path().join("fail-journal"), "").unwrap();
    let failed = f.run(&["swarm", "logs", "box"]);
    assert!(!failed.status.success());
    assert!(String::from_utf8_lossy(&failed.stderr).contains("journal-failed"));
}
#[test]
fn upgrade_without_a_checkout_uploads_node_files_and_release_binaries() {
    let f = Fixture::new();
    f.create();
    f.ok(&["swarm", "up", "box", "--no-image"]);
    let result = f.run(&["--json", "swarm", "upgrade", "box"]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let summary: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(summary["node"], "box-box-1");
    let commands = fs::read_to_string(f.path().join("ssh-commands")).unwrap();
    assert!(
        commands.contains("bash scripts/remote-upgrade.sh stack all 600 prebuilt"),
        "{commands}"
    );
}
#[test]
fn development_client_outside_a_checkout_refuses_before_ssh() {
    let f = Fixture::new();
    f.create();
    f.ok(&["swarm", "up", "box", "--no-image"]);
    fs::remove_file(f.path().join("ssh-commands")).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_swarmy"))
        .env_clear()
        .env("HOME", f.path().join("home"))
        .env("XDG_CONFIG_HOME", f.path().join("config"))
        .env("XDG_DATA_HOME", f.path().join("data"))
        .env("XDG_STATE_HOME", f.path().join("state"))
        .env("XDG_CACHE_HOME", f.path().join("cache"))
        .env(
            "PATH",
            format!("{}:/usr/bin:/bin", f.path().join("bin").display()),
        )
        .current_dir(f.path())
        .args(["swarm", "upgrade", "box"])
        .output()
        .unwrap();
    if !swarmy_version::RELEASE_BUILD {
        assert!(!output.status.success());
        assert!(!f.path().join("ssh-commands").exists());
    }
}

#[cfg(not(feature = "remote"))]
#[test]
fn bucket_up_requires_s3_client_before_ssh() {
    let f = Fixture::new();
    f.create();
    let path = f.path().join("config/swarmy/swarms/box.toml");
    let mut spec = fs::read_to_string(&path).unwrap();
    spec.push_str("\n[bucket]\nname = 'test-bucket'\nendpoint = 'https://objects.example.invalid'\nregion = 'eu-west-1'\n");
    fs::write(path, spec).unwrap();
    let result = f.run(&["swarm", "up", "box", "--no-image"]);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("built without the S3 client"));
    assert!(!f.path().join("ssh-commands").exists());
}

#[test]
fn s3_flags_require_a_host_bucket_and_a_complete_key_pair() {
    let f = Fixture::new();
    fs::write(f.path().join("s3-secret"), "test-secret").unwrap();
    fs::set_permissions(
        f.path().join("s3-secret"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    f.ok(&["swarm", "create", "local-box", "--local"]);
    let local = f.run(&[
        "swarm",
        "up",
        "local-box",
        "--s3-access-key",
        "test-key",
        "--s3-secret-file",
        f.path().join("s3-secret").to_str().unwrap(),
    ]);
    assert!(!local.status.success());
    assert!(
        String::from_utf8_lossy(&local.stderr).contains("S3 keys apply only to swarms on hosts")
    );
    f.create();
    let secret_only = f.run(&[
        "swarm",
        "up",
        "box",
        "--s3-secret-file",
        f.path().join("s3-secret").to_str().unwrap(),
    ]);
    assert!(!secret_only.status.success());
    assert!(String::from_utf8_lossy(&secret_only.stderr).contains("require --s3-access-key"));
    let access_only = f.run(&["swarm", "up", "box", "--s3-access-key", "test-key"]);
    assert!(!access_only.status.success());
    assert!(String::from_utf8_lossy(&access_only.stderr).contains("requires --s3-secret"));
    let no_bucket = f.run(&[
        "swarm",
        "up",
        "box",
        "--s3-access-key",
        "test-key",
        "--s3-secret-file",
        f.path().join("s3-secret").to_str().unwrap(),
    ]);
    assert!(!no_bucket.status.success());
    assert!(String::from_utf8_lossy(&no_bucket.stderr).contains("has no [bucket]"));
    assert!(!f.path().join("data/swarmy/swarms/box/record.toml").exists());
}

#[test]
fn upgrade_json_reports_completed_host_before_later_host_fails() {
    let f = Fixture::new();
    f.create();
    f.ok(&["swarm", "up", "box", "--no-image"]);
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
    fs::write(f.path().join("fail-upgrade-primary"), "").unwrap();
    let result = f.run(&["--json", "swarm", "upgrade", "box"]);
    assert!(!result.status.success());
    assert!(
        String::from_utf8_lossy(&result.stderr).contains("upgrade host failed"),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let lines = String::from_utf8(result.stdout).unwrap();
    let summaries = lines
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(summaries.len(), 1, "{lines}");
    assert_eq!(summaries[0]["node"], "box-box-2");
}
