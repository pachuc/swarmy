#![deny(clippy::disallowed_methods)]
#[path = "support/fake_host.rs"]
mod fake_host;
use fake_host::Fixture;
use std::{
    fs,
    io::Write,
    os::unix::fs::PermissionsExt,
    process::{Command, Stdio},
};
use swarmy_config::{
    paths::Dirs,
    swarm::{Registry, SwarmSpec},
};

fn setup() -> Fixture {
    let f = Fixture::new();
    let root = f.path();
    let script = format!(
        r#"#!/bin/sh
set -eu
group=$(basename "$PWD")
printf '%s %s\n' "$group" "$1" >> '{root}/tofu.log'
env | grep -E '^(HCLOUD_TOKEN|MINIO_USER|MINIO_PASSWORD)=' >> '{root}/env.log' || true
case "$1" in
init) ;;
plan) echo "plan for $group token=$HCLOUD_TOKEN"; if test -f '{root}/no-changes'; then exit 0; fi; exit 2 ;;
apply) echo state > terraform.tfstate ;;
output)
 bucket=$(python3 -c 'import json; print(json.load(open("swarmy.tfvars.json"))["bucket_name"])')
 if test "$group" = control; then
 printf '{{"machines":{{"value":[{{"name":"fleet-control-1","public_ipv4":"203.0.113.10","public_ipv6":null,"private_ip":"10.0.0.10","ssh_user":"root"}}]}},"network_id":{{"value":"4711"}},"bucket":{{"value":{{"endpoint":"https://fsn1.your-objectstorage.com","name":"%s","region":"fsn1"}}}}}}\n' "$bucket"
 elif test -f '{root}/outputs-workers.json'; then
 cat '{root}/outputs-workers.json'
 else
 printf '{{"machines":{{"value":[{{"name":"fleet-workers-1","public_ipv4":"203.0.113.11","public_ipv6":null,"private_ip":"10.0.0.11","ssh_user":"root"}},{{"name":"fleet-workers-2","public_ipv4":"203.0.113.12","public_ipv6":null,"private_ip":"10.0.0.12","ssh_user":"root"}}]}}}}\n'
 fi ;;
destroy) cp swarmy.tfvars.json '{root}/destroyed-'"$group"'.json'; rm terraform.tfstate ;;
esac
"#,
        root = root.display()
    );
    fs::write(root.join("bin/tofu"), script).unwrap();
    fs::set_permissions(root.join("bin/tofu"), fs::Permissions::from_mode(0o755)).unwrap();
    f
}
fn login(f: &Fixture, provider: &str, input: &str) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_swarmy"))
        .env_clear()
        .current_dir(f.path())
        .env("HOME", f.path().join("home"))
        .env("XDG_CONFIG_HOME", f.path().join("config"))
        .env("XDG_DATA_HOME", f.path().join("data"))
        .env("XDG_STATE_HOME", f.path().join("state"))
        .env("XDG_CACHE_HOME", f.path().join("cache"))
        .args(["cloud", "login", provider])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}
fn create(f: &Fixture, lifecycle: &[&str]) {
    let mut args = vec![
        "swarm",
        "create",
        "fleet",
        "--on",
        "hetzner-cloud",
        "--size",
        "cpx41",
        "--nodes",
        "2",
    ];
    args.extend_from_slice(lifecycle);
    f.ok(&args);
}
fn credentials(f: &Fixture) {
    login(f, "hetzner-cloud", "hcloud-secret-5f2b9c\n");
    login(f, "hetzner-s3", "s3-access-7a1e\ns3-secret-91d0\n");
}
#[test]
fn create_plan_up_status_down_on_hetzner_cloud() {
    let f = setup();
    credentials(&f);
    create(&f, &["--lifecycle", "ephemeral"]);
    let spec = SwarmSpec::parse(
        &fs::read_to_string(f.path().join("config/swarmy/swarms/fleet.toml")).unwrap(),
    )
    .unwrap();
    assert_eq!(spec.groups["control"].count, Some(1));
    assert_eq!(spec.groups["workers"].count, Some(2));
    assert_eq!(spec.groups["workers"].size.as_deref(), Some("cpx41"));
    let plan = f.ok(&["--json", "swarm", "plan", "fleet"]);
    let value: serde_json::Value = serde_json::from_slice(&plan.stdout).unwrap();
    assert_eq!(value["groups"].as_array().unwrap().len(), 2);
    assert!(
        value["groups"]
            .as_array()
            .unwrap()
            .iter()
            .all(|g| g["changes"] == true)
    );
    assert!(
        value["groups"][0]["plan"]
            .as_str()
            .unwrap()
            .contains("<redacted>")
    );
    assert!(
        !fs::read_to_string(f.path().join("tofu.log"))
            .unwrap()
            .contains("apply")
    );
    let up = f.ok(&["--json", "swarm", "up", "fleet", "--no-image"]);
    let value: serde_json::Value = serde_json::from_slice(&up.stdout).unwrap();
    assert_eq!(value["hosts"].as_array().unwrap().len(), 3);
    assert_eq!(value["groups"], serde_json::json!(["control", "workers"]));
    assert!(
        value["hosts"]
            .as_array()
            .unwrap()
            .iter()
            .all(|h| h["provisioned"] == true)
    );
    assert_cloud_up(&f);
    let down = f.ok(&["--json", "swarm", "down", "fleet"]);
    let down: serde_json::Value = serde_json::from_slice(&down.stdout).unwrap();
    assert_eq!(down["swarm"], "fleet");
    assert_eq!(down["removed"], serde_json::json!([]));
    assert_eq!(down["destroyed_groups"], 2);
    let log = fs::read_to_string(f.path().join("tofu.log")).unwrap();
    assert!(log.find("workers destroy").unwrap() < log.find("control destroy").unwrap());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &fs::read(f.path().join("destroyed-control.json")).unwrap()
        )
        .unwrap()["bucket_force_destroy"],
        true
    );
    assert!(!f.path().join("data/swarmy/swarms/fleet").exists());
    assert!(f.path().join("config/swarmy/swarms/fleet.toml").exists());
    assert!(
        !f.path().join("home").exists()
            || fs::read_dir(f.path().join("home")).unwrap().count() == 0
    );
}
fn assert_cloud_up(f: &Fixture) {
    let ssh = fs::read_to_string(f.path().join("ssh-commands")).unwrap();
    assert!(ssh.contains("remote-provision.sh stack"));
    assert!(ssh.contains("https://fsn1.your-objectstorage.com"));
    assert_eq!(ssh.matches("remote-provision.sh node").count(), 2);
    let dir = f.path().join("data/swarmy/swarms/fleet");
    let record = dir.join("record.toml");
    assert_eq!(
        fs::metadata(&record).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let record_text = fs::read_to_string(&record).unwrap();
    assert!(record_text.contains("tofu_state"));
    let parsed = swarmy_config::swarm::SwarmRecord::parse(&record_text).unwrap();
    assert_eq!(parsed.hosts.values().map(Vec::len).sum::<usize>(), 3);
    let bucket = record_text
        .lines()
        .find_map(|line| line.strip_prefix("tofu_bucket = "))
        .unwrap()
        .trim_matches('"');
    assert!(ssh.contains(bucket));
    assert_eq!(
        fs::metadata(dir.join("keys/groups/control"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let status = f.ok(&["--json", "swarm", "status", "fleet"]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&status.stdout).unwrap()["hosts"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
}

#[test]
fn persistent_down_needs_destroy() {
    let f = setup();
    credentials(&f);
    create(&f, &[]);
    f.ok(&["swarm", "up", "fleet", "--no-image"]);
    let output = f.run(&["swarm", "down", "fleet"]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--destroy"));
    assert!(
        !fs::read_to_string(f.path().join("tofu.log"))
            .unwrap()
            .contains("destroy")
    );
    f.ok(&["swarm", "down", "fleet", "--destroy"]);
    assert!(
        fs::read_to_string(f.path().join("tofu.log"))
            .unwrap()
            .contains("control destroy")
    );
    assert!(!f.path().join("data/swarmy/swarms/fleet").exists());
}
#[test]
fn missing_cloud_login_is_reported_before_tofu_runs() {
    let f = Fixture::new();
    create(&f, &[]);
    assert!(!f.path().join("bin/tofu").exists());
    let output = f.run(&["swarm", "up", "fleet", "--no-image"]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("swarmy cloud login hetzner-cloud"));
    assert!(!f.path().join("tofu.log").exists());
    assert!(!f.path().join("cache/swarmy/tofu").exists());
}
#[test]
fn existing_host_swarms_never_touch_opentofu() {
    let f = Fixture::new();
    assert!(!f.path().join("bin/tofu").exists());
    f.create();
    f.ok(&["swarm", "up", "box", "--no-image"]);
    f.ok(&["swarm", "down", "box", "--destroy"]);
    assert!(!f.path().join("cache/swarmy/tofu").exists());
}
#[test]
fn create_on_rejects_existing_and_local() {
    let f = Fixture::new();
    for module in ["existing", "local"] {
        let output = f.run(&[
            "swarm", "create", "fleet", "--on", module, "--size", "small",
        ]);
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("--on takes hetzner-cloud, aws"));
    }
    let output = f.run(&["swarm", "create", "fleet", "--on", "aws", "--size", "small"]);
    assert_eq!(output.status.code(), Some(2));
    let missing_size = f.run(&[
        "swarm",
        "create",
        "fleet",
        "--on",
        "aws",
        "--location",
        "region-1",
    ]);
    assert_eq!(missing_size.status.code(), Some(2));
    f.ok(&[
        "swarm",
        "create",
        "fleet",
        "--on",
        "aws",
        "--size",
        "small",
        "--location",
        "region-1",
    ]);
    let spec = SwarmSpec::parse(
        &fs::read_to_string(f.path().join("config/swarmy/swarms/fleet.toml")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        spec.groups["control"].module,
        swarmy_config::swarm::Module::Aws
    );
}

fn check_files(dir: &std::path::Path, secrets: &[&str]) {
    if !dir.exists() {
        return;
    }
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            check_files(&path, secrets);
        } else if !path.ends_with("cloud-credentials.toml") {
            let bytes = fs::read(&path).unwrap();
            for secret in secrets {
                assert!(
                    !bytes.windows(secret.len()).any(|w| w == secret.as_bytes()),
                    "{}",
                    path.display()
                );
            }
        }
    }
}

#[test]
fn credentials_never_leave_the_store() {
    let f = setup();
    credentials(&f);
    create(&f, &["--lifecycle", "ephemeral"]);
    let mut outputs = Vec::new();
    for args in [
        vec!["--json", "swarm", "plan", "fleet"],
        vec!["--json", "swarm", "up", "fleet", "--no-image"],
        vec!["--json", "swarm", "status", "fleet"],
        vec!["swarm", "plan", "fleet"],
        vec!["--json", "swarm", "down", "fleet"],
    ] {
        let out = f.ok(&args);
        outputs.extend(out.stdout);
        outputs.extend(out.stderr);
    }
    let secrets = ["hcloud-secret-5f2b9c", "s3-access-7a1e", "s3-secret-91d0"];
    let output = String::from_utf8(outputs).unwrap();
    assert!(output.contains("<redacted>"));
    for secret in secrets {
        assert!(!output.contains(secret));
    }
    let ssh = fs::read_to_string(f.path().join("ssh-commands")).unwrap();
    for secret in secrets {
        assert!(!ssh.contains(secret));
    }
    assert!(
        fs::read_to_string(f.path().join("env.log"))
            .unwrap()
            .contains("HCLOUD_TOKEN=hcloud-secret-5f2b9c")
    );
    for name in ["config", "data", "state", "cache"] {
        check_files(&f.path().join(name), &secrets);
    }
}

#[test]
fn mixed_cloud_stack_and_existing_nodes() {
    let f = setup();
    credentials(&f);
    create(&f, &["--lifecycle", "ephemeral"]);
    let path = f.path().join("config/swarmy/swarms/fleet.toml");
    let mut spec = SwarmSpec::parse(&fs::read_to_string(&path).unwrap()).unwrap();
    spec.groups.remove("workers");
    spec.groups.insert(
        "box".into(),
        swarmy_config::swarm::GroupSpec {
            module: swarmy_config::swarm::Module::Existing,
            size: None,
            location: None,
            count: None,
            stack_dir: None,
            sandboxes: None,
            local_storage: Some("dir:/srv/swarmy-local".into()),
            hosts: vec![swarmy_config::swarm::HostSpec {
                address: "203.0.113.20".into(),
                ssh_user: "root".into(),
                ssh_key: f.key.clone(),
                private_address: None,
            }],
        },
    );
    spec.placement.nodes = vec!["box".into()];
    spec.validate().unwrap();
    let dirs = Dirs {
        config: f.path().join("config/swarmy"),
        data: f.path().join("data/swarmy"),
        state: f.path().join("state/swarmy"),
        cache: f.path().join("cache/swarmy"),
    };
    Registry::new(dirs).write_spec(&spec).unwrap();
    f.ok(&["swarm", "up", "fleet", "--no-image"]);
    let ssh = fs::read_to_string(f.path().join("ssh-commands")).unwrap();
    assert!(ssh.contains("remote-provision.sh stack"));
    assert!(ssh.contains("remote-provision.sh node"));
    f.ok(&["swarm", "down", "fleet", "--destroy"]);
    let log = fs::read_to_string(f.path().join("tofu.log")).unwrap();
    assert!(log.contains("control destroy"));
    assert!(!log.contains("box destroy"));
    let probes: Vec<_> = fs::read_dir(f.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("piped-"))
        .filter(|entry| {
            fs::read_to_string(entry.path()).is_ok_and(|text| {
                text.lines()
                    .any(|line| line.starts_with("decommission_probe "))
            })
        })
        .collect();
    assert_eq!(probes.len(), 1);
    let target = fs::read_to_string(format!("{}.args", probes[0].path().display())).unwrap();
    assert!(target.contains("203.0.113.20"));
}

#[test]
fn human_plan_no_changes_and_redacted_text() {
    let f = setup();
    credentials(&f);
    create(&f, &[]);
    fs::write(f.path().join("no-changes"), "").unwrap();
    let human = f.ok(&["swarm", "plan", "fleet"]);
    let text = String::from_utf8(human.stdout).unwrap();
    assert!(text.contains("== group control (hetzner-cloud): no changes =="));
    assert!(text.contains("== group workers (hetzner-cloud): no changes =="));
    assert!(text.contains("plan for control token=<redacted>"));
    assert!(!text.contains("hcloud-secret-5f2b9c"));
    let json = f.ok(&["--json", "swarm", "plan", "fleet"]);
    let value: serde_json::Value = serde_json::from_slice(&json.stdout).unwrap();
    assert!(
        value["groups"]
            .as_array()
            .unwrap()
            .iter()
            .all(|g| g["changes"] == false)
    );
    assert!(
        !String::from_utf8(json.stdout)
            .unwrap()
            .contains("hcloud-secret-5f2b9c")
    );
}

#[test]
fn one_box_create_and_next_steps() {
    let f = Fixture::new();
    let output = f.ok(&[
        "swarm",
        "create",
        "fleet",
        "--on",
        "hetzner-cloud",
        "--size",
        "cpx41",
        "--sandboxes",
        "3",
        "--local-storage",
        "dir:/srv/swarmy-local",
    ]);
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains("next: swarmy cloud login hetzner-cloud"));
    assert!(text.contains("next: swarmy cloud login hetzner-s3"));
    assert!(text.contains("next: swarmy swarm plan fleet"));
    let spec = SwarmSpec::parse(
        &fs::read_to_string(f.path().join("config/swarmy/swarms/fleet.toml")).unwrap(),
    )
    .unwrap();
    assert_eq!(spec.lifecycle, swarmy_config::swarm::Lifecycle::Persistent);
    assert_eq!(spec.groups.len(), 1);
    assert_eq!(spec.groups["control"].count, Some(1));
    assert_eq!(spec.groups["control"].location.as_deref(), Some("fsn1"));
    assert_eq!(spec.groups["control"].sandboxes, Some(3));
    assert_eq!(
        spec.groups["control"].local_storage.as_deref(),
        Some("dir:/srv/swarmy-local")
    );
    assert_eq!(spec.placement.nodes, ["control"]);
    assert!(
        !f.run(&[
            "swarm",
            "create",
            "another",
            "--on",
            "hetzner-cloud",
            "--size",
            "cpx41",
            "--node-size",
            "cpx51"
        ])
        .status
        .success()
    );
}

#[test]
fn create_bad_credentials_does_not_leave_a_spec() {
    let f = Fixture::new();
    let path = f.path().join("data/swarmy/cloud-credentials.toml");
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, "invalid").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    let result = f.run(&[
        "swarm",
        "create",
        "fleet",
        "--on",
        "hetzner-cloud",
        "--size",
        "cpx41",
    ]);
    assert!(!result.status.success());
    assert!(!f.path().join("config/swarmy/swarms/fleet.toml").exists());
}

#[test]
fn path_module_create_requires_location_and_accepts_absolute_path() {
    let f = Fixture::new();
    let module = f.path().join("module");
    fs::create_dir(&module).unwrap();
    let module = module.to_str().unwrap();
    let missing = f.run(&[
        "swarm", "create", "fleet", "--on", module, "--size", "large",
    ]);
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("--location is required"));
    f.ok(&[
        "swarm",
        "create",
        "fleet",
        "--on",
        module,
        "--size",
        "large",
        "--location",
        "region-1",
        "--nodes",
        "1",
        "--node-size",
        "medium",
    ]);
    let spec = SwarmSpec::parse(
        &fs::read_to_string(f.path().join("config/swarmy/swarms/fleet.toml")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        spec.groups["control"].module,
        swarmy_config::swarm::Module::Path(module.into())
    );
    assert_eq!(spec.groups["workers"].size.as_deref(), Some("medium"));
}

#[test]
fn missing_s3_login_fails_before_tofu_resolution() {
    let f = Fixture::new();
    login(&f, "hetzner-cloud", "hcloud-secret-5f2b9c\n");
    create(&f, &[]);
    let result = f.run(&["swarm", "up", "fleet", "--no-image"]);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("swarmy cloud login hetzner-s3"));
    assert!(!f.path().join("cache/swarmy/tofu").exists());
}

fn worker_outputs(f: &Fixture, count: usize) {
    let machines = (1..=count)
        .map(|i| {
            serde_json::json!({
                "name": format!("fleet-workers-{i}"),
                "public_ipv4": format!("203.0.113.{}", 10+i),
                "public_ipv6": null,
                "private_ip": format!("10.0.0.{}", 10+i),
                "ssh_user": "root"
            })
        })
        .collect::<Vec<_>>();
    fs::write(
        f.path().join("outputs-workers.json"),
        serde_json::json!({"machines":{"value":machines}}).to_string(),
    )
    .unwrap();
}
fn single_worker(f: &Fixture) {
    credentials(f);
    f.ok(&[
        "swarm",
        "create",
        "fleet",
        "--on",
        "hetzner-cloud",
        "--size",
        "cpx41",
        "--nodes",
        "1",
        "--lifecycle",
        "ephemeral",
    ]);
    worker_outputs(f, 1);
    f.ok(&["swarm", "up", "fleet", "--no-image"]);
}
#[test]
fn scale_workers_up_then_down() {
    let f = setup();
    single_worker(&f);
    worker_outputs(&f, 2);
    let output = f.ok(&["--json", "swarm", "scale", "fleet", "workers", "2"]);
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["from"], 1);
    assert_eq!(value["to"], 2);
    assert_eq!(value["added"], serde_json::json!(["fleet-workers-2"]));
    assert_eq!(
        fs::read_to_string(f.path().join("ssh-commands"))
            .unwrap()
            .matches("remote-provision.sh node")
            .count(),
        2
    );
    worker_outputs(&f, 1);
    fs::write(
        f.path()
            .join("data/swarmy/swarms/fleet/keys/groups/workers.known_hosts"),
        "fixture\n",
    )
    .unwrap();
    f.ok(&["swarm", "scale", "fleet", "workers", "1"]);
    let piped = fs::read_dir(f.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.file_name().to_string_lossy().starts_with("piped-"))
        .filter_map(|e| fs::read_to_string(e.path()).ok())
        .collect::<Vec<_>>();
    assert!(
        piped
            .iter()
            .any(|s| s.contains("drain_node_agent 'swarmy' 600 wait"))
    );
    let ssh = fs::read_to_string(f.path().join("ssh-commands")).unwrap();
    assert!(ssh.contains("grep -vxF"));
    let full = fs::read_to_string(f.path().join("ssh-full-commands")).unwrap();
    assert!(
        full.lines()
            .any(|line| line.contains("grep -vxF") && line.contains("203.0.113.10"))
    );
    assert!(
        fs::read_to_string(f.path().join("ssh-keygen-commands"))
            .unwrap()
            .contains("-R 203.0.113.12 -f")
    );
    let applied: serde_json::Value = serde_json::from_slice(
        &fs::read(
            f.path()
                .join("data/swarmy/swarms/fleet/tofu/workers/swarmy.tfvars.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(applied["machine_count"], 1);
    let status = f.ok(&["--json", "swarm", "status", "fleet"]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&status.stdout).unwrap()["hosts"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert!(
        !f.path().join("home").exists()
            || fs::read_dir(f.path().join("home"))
                .unwrap()
                .next()
                .is_none()
    );
}
#[test]
fn scale_flags_reach_the_node() {
    let f = setup();
    single_worker(&f);
    worker_outputs(&f, 2);
    f.ok(&["swarm", "scale", "fleet", "workers", "2"]);
    worker_outputs(&f, 1);
    f.ok(&[
        "swarm",
        "scale",
        "fleet",
        "workers",
        "1",
        "--drain-timeout",
        "5",
        "--force",
    ]);
    let piped = fs::read_dir(f.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.file_name().to_string_lossy().starts_with("piped-"))
        .filter_map(|e| fs::read_to_string(e.path()).ok())
        .collect::<Vec<_>>();
    assert!(
        piped
            .iter()
            .any(|s| s.contains("drain_node_agent 'swarmy' 5 force")),
        "{piped:?}"
    );
}
#[test]
fn busy_node_fails_clearly() {
    let f = setup();
    single_worker(&f);
    worker_outputs(&f, 2);
    f.ok(&["swarm", "scale", "fleet", "workers", "2"]);
    worker_outputs(&f, 1);
    fs::create_dir_all(f.path().join("fake")).unwrap();
    fs::write(
        f.path().join("fake/drain-line"),
        "drain {\"stopped\":false,\"busy\":[\"agent-a\"],\"unreleased\":[]}\n",
    )
    .unwrap();
    let output = f.run(&["swarm", "scale", "fleet", "workers", "1"]);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("still has running sandbox commands after 600s")
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("--force"));
    for entry in fs::read_dir(f.path()).unwrap().filter_map(Result::ok) {
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with("applied-workers-")
        {
            let vars: serde_json::Value =
                serde_json::from_slice(&fs::read(entry.path()).unwrap()).unwrap();
            assert_ne!(vars["machine_count"], 1);
        }
    }
}
#[test]
fn scale_refuses_stack_group_and_local_swarm() {
    let f = setup();
    single_worker(&f);
    let output = f.run(&["swarm", "scale", "fleet", "control", "2"]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("runs the stack"));
    f.ok(&["swarm", "create", "local", "--local"]);
    let output = f.run(&["swarm", "scale", "local", "control", "2"]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("runs on this machine"));
}

#[test]
fn same_count_scale_resumes_an_unprovisioned_worker() {
    let f = setup();
    single_worker(&f);
    worker_outputs(&f, 2);
    f.ok(&["swarm", "scale", "fleet", "workers", "2"]);
    let dirs = Dirs {
        config: f.path().join("config/swarmy"),
        data: f.path().join("data/swarmy"),
        state: f.path().join("state/swarmy"),
        cache: f.path().join("cache/swarmy"),
    };
    let registry = Registry::new(dirs);
    let mut record = registry.read_record("fleet").unwrap().unwrap();
    record.hosts.get_mut("workers").unwrap()[1].provisioned = false;
    registry.write_record(&record).unwrap();
    let before = fs::read_to_string(f.path().join("ssh-commands"))
        .unwrap()
        .matches("remote-provision.sh node")
        .count();
    f.ok(&["swarm", "scale", "fleet", "workers", "2"]);
    let after = fs::read_to_string(f.path().join("ssh-commands"))
        .unwrap()
        .matches("remote-provision.sh node")
        .count();
    assert_eq!(after, before + 1);
    assert!(registry.read_record("fleet").unwrap().unwrap().hosts["workers"][1].provisioned);
}

#[test]
fn scale_requires_a_provisioned_stack_before_cloud_login() {
    let f = Fixture::new();
    f.ok(&[
        "swarm",
        "create",
        "fleet",
        "--on",
        "hetzner-cloud",
        "--size",
        "cpx41",
        "--nodes",
        "1",
    ]);
    let output = f.run(&["swarm", "scale", "fleet", "workers", "2"]);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("swarm fleet is not up; run swarmy swarm up fleet first")
    );
    assert!(!f.path().join("cache/swarmy/tofu").exists());
}
