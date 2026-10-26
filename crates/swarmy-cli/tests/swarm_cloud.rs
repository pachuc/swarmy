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
plan) echo "plan for $group"; exit 2 ;;
apply) echo state > terraform.tfstate ;;
output)
 bucket=$(python3 -c 'import json; print(json.load(open("swarmy.tfvars.json"))["bucket_name"])')
 if test "$group" = control; then
 printf '{{"machines":{{"value":[{{"name":"fleet-control-1","public_ipv4":"203.0.113.10","public_ipv6":null,"private_ip":"10.0.0.10","ssh_user":"root"}}]}},"network_id":{{"value":"4711"}},"bucket":{{"value":{{"endpoint":"https://fsn1.your-objectstorage.com","name":"%s","region":"fsn1"}}}}}}\n' "$bucket"
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
    assert_eq!(value["groups"][0]["changes"], true);
    assert!(
        !fs::read_to_string(f.path().join("tofu.log"))
            .unwrap()
            .contains("apply")
    );
    let up = f.ok(&["--json", "swarm", "up", "fleet", "--no-image"]);
    let value: serde_json::Value = serde_json::from_slice(&up.stdout).unwrap();
    assert_eq!(value["hosts"].as_array().unwrap().len(), 3);
    assert!(
        value["hosts"]
            .as_array()
            .unwrap()
            .iter()
            .all(|h| h["provisioned"] == true)
    );
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
    f.ok(&["swarm", "down", "fleet"]);
    let log = fs::read_to_string(f.path().join("tofu.log")).unwrap();
    assert!(log.find("workers destroy").unwrap() < log.find("control destroy").unwrap());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &fs::read(f.path().join("destroyed-control.json")).unwrap()
        )
        .unwrap()["bucket_force_destroy"],
        true
    );
    assert!(!dir.exists());
    assert!(f.path().join("config/swarmy/swarms/fleet.toml").exists());
    assert!(
        !f.path().join("home").exists()
            || fs::read_dir(f.path().join("home")).unwrap().count() == 0
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
}
#[test]
fn missing_cloud_login_is_reported_before_tofu_runs() {
    let f = setup();
    create(&f, &[]);
    let output = f.run(&["swarm", "up", "fleet", "--no-image"]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("swarmy cloud login hetzner-cloud"));
    assert!(!f.path().join("tofu.log").exists());
}
#[test]
fn existing_host_swarms_never_touch_opentofu() {
    let f = Fixture::new();
    f.create();
    f.ok(&["swarm", "up", "box", "--no-image"]);
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
    assert!(!output.status.success());
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
    ] {
        let out = f.ok(&args);
        outputs.extend(out.stdout);
        outputs.extend(out.stderr);
    }
    let secrets = ["hcloud-secret-5f2b9c", "s3-access-7a1e", "s3-secret-91d0"];
    let output = String::from_utf8(outputs).unwrap();
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
    f.ok(&["swarm", "down", "fleet"]);
    let log = fs::read_to_string(f.path().join("tofu.log")).unwrap();
    assert!(log.contains("control destroy"));
    assert!(!log.contains("box destroy"));
    let probes = fs::read_dir(f.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("piped-"))
        .filter_map(|entry| fs::read_to_string(entry.path()).ok())
        .filter(|text| {
            text.lines()
                .any(|line| line.starts_with("decommission_probe "))
        })
        .count();
    assert_eq!(probes, 1);
}
