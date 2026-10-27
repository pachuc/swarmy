#![deny(clippy::disallowed_methods)]
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output},
};
use swarmy_config::{
    paths::Dirs,
    swarm::{Lifecycle, Module, Registry},
};

const SECRETS: [&str; 3] = [
    "node-token-7c1f",
    "access-fixture-3b8e",
    "secret-fixture-91d0",
];

struct Fixture {
    root: tempfile::TempDir,
    python: PathBuf,
    record: PathBuf,
    value: Value,
}
impl Fixture {
    fn new() -> Option<Self> {
        Self::new_with_prefix("swarmy-convert-")
    }
    fn new_with_prefix(prefix: &str) -> Option<Self> {
        let path = std::env::var_os("PATH")?;
        let Some(python) = std::env::split_paths(&path)
            .map(|p| p.join("python3"))
            .find(|p| p.is_file())
        else {
            println!("skip: python3 is missing");
            return None;
        };
        if !Command::new(&python)
            .args(["-c", "import tomllib"])
            .status()
            .ok()?
            .success()
        {
            println!("skip: python3 has no tomllib");
            return None;
        }
        let root = tempfile::Builder::new().prefix(prefix).tempdir().unwrap();
        let bin = root.path().join("bin");
        fs::create_dir(&bin).unwrap();
        let ssh = bin.join("ssh");
        std::os::unix::fs::symlink(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/convert-ssh"),
            &ssh,
        )
        .unwrap();
        let old = root.path().join("old/.swarmy/remote");
        fs::create_dir_all(&old).unwrap();
        let key = old.join("swarmy-01TESTKEY");
        for (suffix, bytes) in [
            ("", "private-key"),
            (".pub", "public-key"),
            (".known_hosts", "host-key"),
        ] {
            fs::write(format!("{}{suffix}", key.display()), bytes).unwrap();
        }
        let record = old.join("hz.json");
        let value = json!({
            "name":"hz", "region":"fsn1", "instance_id":"", "launch_attempted":false,
            "public_ip":"203.0.113.10", "private_ip":"10.0.0.2", "key_path":key,
            "ssh_user":"root", "ports":{"fdb":4500,"nats":4222,"s3":8333},
            "nodes":[], "sandboxes":4, "default_image":"base-ubuntu:hz",
            "created_at":"2026-10-02T00:00:00Z",
            "launch_settings":{
                "provider":"existing", "services":"node", "region":"fsn1", "disk_gb":100,
                "managed_by_tag":"swarmy", "service_user":"", "local_storage":"dir:/srv/swarmy-local",
                "aws":{"subnet":null,"security_group":null,"instance_type":"m6id.xlarge","image":null,"iam_role":null},
                "bucket":{"endpoint":"https://fsn1.your-objectstorage.com", "region":"",
                    "bucket":"swarmy-hz-fixture", "prefix":"swarmy/hz", "conditional_create":true,
                    "credentials":{"source":"static_keys", "access_key":SECRETS[1], "secret_key":SECRETS[2]}}
            }
        });
        fs::write(&record, serde_json::to_vec(&value).unwrap()).unwrap();
        Some(Self {
            root,
            python,
            record,
            value,
        })
    }
    fn save(&self) {
        fs::write(&self.record, serde_json::to_vec(&self.value).unwrap()).unwrap();
    }
    fn cmd(&self, program: &Path) -> Command {
        let mut cmd = Command::new(program);
        cmd.env_clear()
            .envs(swarmy_testkit::isolated_home(self.root.path()))
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin", self.root.path().join("bin").display()),
            )
            .current_dir(self.root.path());
        cmd
    }
    fn run(&self, dry: bool) -> Output {
        let script = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../scripts/convert-remote-record-2026-10.py");
        let mut cmd = self.cmd(&self.python);
        cmd.arg(script).arg("--old-record").arg(&self.record);
        if dry {
            cmd.arg("--dry-run");
        }
        let output = cmd.output().unwrap();
        no_secrets(&output);
        output
    }
    fn registry(&self) -> Registry {
        let env = swarmy_testkit::isolated_home(self.root.path())
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.to_string_lossy().into_owned()))
            .collect::<BTreeMap<_, _>>();
        Registry::new(Dirs::from_env(&env).unwrap())
    }
    fn ssh_lines(&self) -> Vec<String> {
        fs::read_to_string(self.root.path().join("ssh.log"))
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }
    fn assert_empty(&self) {
        assert!(self.ssh_lines().is_empty());
        for directory in ["config", "data"] {
            assert!(!self.root.path().join(directory).exists());
        }
    }
}
fn no_secrets(output: &Output) {
    for text in [&output.stdout, &output.stderr] {
        let text = String::from_utf8_lossy(text);
        for secret in SECRETS {
            assert!(!text.contains(secret), "secret leaked");
        }
    }
}
fn converted(f: &Fixture, node: bool) {
    let output = f.run(false);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let registry = f.registry();
    let swarm = registry.read("hz").unwrap();
    let spec = swarm.spec.unwrap();
    let record = swarm.record.unwrap();
    let dir = registry.swarm_dir("hz").unwrap();
    check_spec(&spec, &dir, node);
    check_record(&record);
    check_plan(&spec, &record, &dir);
    let lines = f.ssh_lines();
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("StrictHostKeyChecking=yes"));
    assert!(lines[0].contains("-l root 203.0.113.10"));
    assert!(lines[0].contains(if node {
        "cat ~swarmy/swarmy/.swarmy/config.toml"
    } else {
        "cat ~root/swarmy/.swarmy/config.toml"
    }));
}
fn check_spec(spec: &swarmy_config::swarm::SwarmSpec, dir: &Path, node: bool) {
    assert_eq!(spec.name, "hz");
    assert_eq!(spec.lifecycle, Lifecycle::Persistent);
    assert_eq!(
        spec.service_user.as_deref(),
        Some(if node { "swarmy" } else { "root" })
    );
    let main = &spec.groups["main"];
    assert_eq!(main.module, Module::Existing);
    assert_eq!(main.sandboxes, Some(4));
    assert_eq!(main.local_storage.as_deref(), Some("dir:/srv/swarmy-local"));
    assert_eq!(main.hosts[0].address, "203.0.113.10");
    assert_eq!(main.hosts[0].ssh_user, "root");
    assert_eq!(main.hosts[0].private_address.as_deref(), Some("10.0.0.2"));
    assert_eq!(main.hosts[0].ssh_key, dir.join("keys/hz-main-1"));
    assert_eq!(spec.placement.stack, "main");
    assert_eq!(spec.placement.services, "main");
    assert_eq!(
        spec.placement.nodes,
        if node {
            vec!["main", "nodes"]
        } else {
            vec!["main"]
        }
    );
    let bucket = spec.bucket.as_ref().unwrap();
    assert_eq!(bucket.name, "swarmy-hz-fixture");
    assert_eq!(bucket.endpoint, "https://fsn1.your-objectstorage.com");
    assert_eq!(bucket.region, "fsn1");
    assert_eq!(bucket.prefix.as_str(), "swarmy/hz");
    assert!(bucket.conditional_create);
}
fn check_record(record: &swarmy_config::swarm::SwarmRecord) {
    assert_eq!(record.api.token.as_deref(), Some(SECRETS[0]));
    assert!(record.api.url.is_none());
    assert_eq!(record.bucket_keys.as_ref().unwrap().access_key, SECRETS[1]);
    assert_eq!(record.bucket_keys.as_ref().unwrap().secret_key, SECRETS[2]);
    assert!(record.tofu_state.is_none());
}
fn check_plan(
    spec: &swarmy_config::swarm::SwarmSpec,
    record: &swarmy_config::swarm::SwarmRecord,
    dir: &Path,
) {
    let planned = swarmy_cloud::provision::plan(
        spec,
        swarmy_cloud::provision::existing_sources(spec, dir).unwrap(),
    )
    .unwrap();
    for host in &planned {
        let found = record.hosts[&host.group]
            .iter()
            .find(|h| h.name == host.host.name)
            .unwrap();
        assert!(found.provisioned);
        assert_eq!(found.public_address, host.host.public_address);
        assert_eq!(found.private_address, host.host.private_address);
        assert_eq!(found.ssh_user, host.host.ssh_user);
    }
    assert_eq!(planned[0].sandboxes, 4);
    assert_eq!(
        record.hosts.values().map(Vec::len).sum::<usize>(),
        planned.len()
    );
}
#[test]
fn dry_run_prints_the_plan_and_writes_nothing() {
    let Some(f) = Fixture::new() else { return };
    let output = f.run(true);
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    for word in [
        "name = \"hz\"",
        "module = \"existing\"",
        "service_user = \"root\"",
        "[placement]",
        "hz-main-1",
        "region = \"fsn1\"",
        "<redacted>",
    ] {
        assert!(text.contains(word), "missing {word}");
    }
    f.assert_empty();
}
#[test]
fn converts_into_a_spec_and_record_the_new_client_reads() {
    let Some(f) = Fixture::new() else { return };
    converted(&f, false);
    let registry = f.registry();
    let spec_path = registry.spec_path("hz").unwrap();
    let dir = registry.swarm_dir("hz").unwrap();
    for (path, mode) in [
        (&spec_path, 0o600),
        (&registry.dirs().specs(), 0o700),
        (&dir.join("record.toml"), 0o600),
        (&dir, 0o700),
        (&dir.join("keys"), 0o700),
        (&dir.join("keys/hz-main-1"), 0o600),
    ] {
        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            mode
        );
    }
    for suffix in ["", ".pub", ".known_hosts"] {
        let old = format!("{}{suffix}", f.value["key_path"].as_str().unwrap());
        let new = dir.join(format!("keys/hz-main-1{suffix}"));
        assert_eq!(fs::read(old).unwrap(), fs::read(new).unwrap());
    }
    let spec_text = fs::read_to_string(spec_path).unwrap();
    for secret in SECRETS {
        assert!(!spec_text.contains(secret));
    }
    let record_text = fs::read_to_string(dir.join("record.toml")).unwrap();
    for secret in SECRETS {
        assert!(record_text.contains(secret));
    }
    let mut entries = fs::read_dir(f.root.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    entries.sort();
    assert_eq!(entries, ["bin", "config", "data", "old", "ssh.log"]);
    for parent in [registry.dirs().specs(), registry.dirs().records()] {
        assert!(fs::read_dir(parent).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".hz.")
        }));
    }
    let output = f
        .cmd(Path::new(env!("CARGO_BIN_EXE_swarmy")))
        .args(["--json", "swarm", "show", "hz"])
        .output()
        .unwrap();
    no_secrets(&output);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let show: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(show["kind"], "managed");
    assert_eq!(show["api_url"], "ssh tunnel");
    assert_eq!(show["token_recorded"], true);
    assert_eq!(show["hosts"]["main"].as_array().unwrap().len(), 1);
}
#[test]
fn refuses_to_overwrite_and_makes_no_ssh_call() {
    let Some(f) = Fixture::new() else { return };
    converted(&f, false);
    let registry = f.registry();
    let paths = [
        registry.spec_path("hz").unwrap(),
        registry.record_path("hz").unwrap(),
    ];
    let before = paths
        .iter()
        .map(|p| {
            (
                fs::read(p).unwrap(),
                fs::metadata(p).unwrap().modified().unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let output = f.run(false);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("already exists") && stderr.contains("swarmy swarm rm hz --force"));
    for (path, (bytes, mtime)) in paths.iter().zip(before) {
        assert_eq!(fs::read(path).unwrap(), bytes);
        assert_eq!(fs::metadata(path).unwrap().modified().unwrap(), mtime);
    }
    assert_eq!(f.ssh_lines().len(), 1);
    let Some(other) = Fixture::new() else { return };
    let output = other
        .cmd(Path::new(env!("CARGO_BIN_EXE_swarmy")))
        .args([
            "swarm",
            "attach",
            "hz",
            "--url",
            "http://127.0.0.1:1",
            "--token-stdin",
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut child = output;
    child.stdin.take().unwrap().write_all(b"t\n").unwrap();
    let attached = child.wait_with_output().unwrap();
    no_secrets(&attached);
    assert!(
        attached.status.success(),
        "{}",
        String::from_utf8_lossy(&attached.stderr)
    );
    let result = other.run(false);
    assert!(!result.status.success());
    other.assert_empty_ssh();
}
impl Fixture {
    fn assert_empty_ssh(&self) {
        assert!(self.ssh_lines().is_empty());
    }
}
#[test]
fn ssh_failure_writes_nothing() {
    let Some(f) = Fixture::new() else { return };
    fs::write(f.root.path().join("ssh-fail"), "").unwrap();
    let output = f.run(false);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Connection refused"));
    assert!(!f.registry().spec_path("hz").unwrap().exists());
    assert!(!f.registry().swarm_dir("hz").unwrap().exists());
    for parent in [f.registry().dirs().specs(), f.registry().dirs().records()] {
        if parent.exists() {
            assert!(fs::read_dir(parent).unwrap().next().is_none());
        }
    }
}
#[test]
fn joined_nodes_become_a_node_group() {
    let Some(mut f) = Fixture::new() else { return };
    f.value["launch_settings"]["service_user"] = json!("swarmy");
    let key = f.record.parent().unwrap().join("swarmy-01NODEKEY");
    for suffix in ["", ".pub", ".known_hosts"] {
        fs::write(format!("{}{suffix}", key.display()), "node-key").unwrap();
    }
    f.value["nodes"] = json!([{"public_ip":"203.0.113.11", "private_ip":"10.0.0.3", "ssh_user":"root", "key_path":key, "sandboxes":8, "launch_settings":null}]);
    f.save();
    converted(&f, true);
    let swarm = f.registry().read("hz").unwrap();
    let spec = swarm.spec.unwrap();
    let record = swarm.record.unwrap();
    assert_eq!(spec.groups["nodes"].sandboxes, Some(8));
    assert_eq!(
        spec.groups["nodes"].local_storage.as_deref(),
        Some("dir:/srv/swarmy-local")
    );
    assert_eq!(spec.groups["nodes"].hosts.len(), 1);
    assert!(record.hosts["nodes"][0].provisioned);
    assert_eq!(
        record.hosts["nodes"][0].ssh_key,
        f.registry()
            .swarm_dir("hz")
            .unwrap()
            .join("keys/hz-nodes-1")
    );
}
#[test]
fn equal_private_address_is_omitted() {
    let Some(mut f) = Fixture::new() else { return };
    f.value["private_ip"] = json!("203.0.113.10");
    f.save();
    let output = f.run(false);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let swarm = f.registry().read("hz").unwrap();
    let (spec, record) = (swarm.spec.unwrap(), swarm.record.unwrap());
    assert_eq!(spec.groups["main"].hosts[0].private_address, None);
    assert_eq!(record.hosts["main"][0].private_address, None);
    check_plan(&spec, &record, &f.registry().swarm_dir("hz").unwrap());
}
#[test]
fn unusual_path_characters_render_valid_toml() {
    for (prefix, expected) in [("swarmy-😀-", "😀"), ("swarmy-\u{7f}-", "\\u007f")] {
        let Some(f) = Fixture::new_with_prefix(prefix) else {
            return;
        };
        let output = f.run(false);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let spec_text = fs::read_to_string(f.registry().spec_path("hz").unwrap()).unwrap();
        assert!(spec_text.contains(expected));
        let swarm = f.registry().read("hz").unwrap();
        assert!(swarm.spec.is_some() && swarm.record.is_some());
    }
}
#[test]
fn joined_node_with_another_service_user_is_refused_before_ssh() {
    let Some(mut f) = Fixture::new() else { return };
    f.value["nodes"] = json!([{
        "public_ip":"203.0.113.11", "private_ip":"10.0.0.3",
        "ssh_user":"root", "key_path":f.value["key_path"], "sandboxes":8,
        "launch_settings":{"service_user":"other", "local_storage":"dir:/srv/swarmy-local"}
    }]);
    f.save();
    let output = f.run(false);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("nodes[0].launch_settings.service_user")
    );
    f.assert_empty();
}
#[test]
fn conditional_create_is_checked_before_ssh() {
    let Some(mut f) = Fixture::new() else { return };
    f.value["launch_settings"]["bucket"]["conditional_create"] = Value::Null;
    f.save();
    let output = f.run(false);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("launch_settings.bucket.conditional_create")
    );
    f.assert_empty();
}
#[test]
fn unsupported_records_are_refused() {
    for case in 0..5 {
        let Some(mut f) = Fixture::new() else { return };
        match case {
            0 => f.value["launch_settings"]["services"] = json!("laptop"),
            1 => {
                f.value["launch_settings"]["bucket"]["credentials"] =
                    json!({"source":"instance_role"});
            }
            2 => f.value["launch_settings"]["bucket"] = json!("swarmy-hz-fixture"),
            3 => {
                fs::remove_file(format!(
                    "{}.known_hosts",
                    f.value["key_path"].as_str().unwrap()
                ))
                .unwrap();
            }
            _ => f.value["launch_settings"]["local_storage"] = json!(""),
        }
        f.save();
        let output = f.run(false);
        assert!(!output.status.success());
        let field = [
            "launch_settings.services",
            "launch_settings.bucket",
            "launch_settings.bucket",
            "key_path",
            "local_storage",
        ][case];
        assert!(String::from_utf8_lossy(&output.stderr).contains(field));
        f.assert_empty();
    }
}
