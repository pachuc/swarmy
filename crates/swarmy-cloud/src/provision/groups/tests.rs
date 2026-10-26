#![deny(clippy::disallowed_methods)]
use super::*;
use crate::provision::{UpRequest, decommission_swarm_with, provision_swarm_with, tests::FakeHost};
use std::os::unix::fs::PermissionsExt;
use swarmy_config::{Settings, cloud::CloudCredential, paths::Dirs};

struct Fixture {
    temp: tempfile::TempDir,
    registry: Registry,
    spec: SwarmSpec,
    tofu: Tofu,
    credentials: CloudCredentials,
}
impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let dirs = Dirs {
            data: temp.path().join("data"),
            config: temp.path().join("config"),
            state: temp.path().join("state"),
            cache: temp.path().join("cache"),
        };
        let registry = Registry::new(dirs);
        let spec = SwarmSpec::parse(
            r#"
name = "fleet"
lifecycle = "ephemeral"
[groups.control]
module = "hetzner-cloud"
size = "cx22"
location = "fsn1"
count = 1
[groups.workers]
module = "hetzner-cloud"
size = "cx22"
location = "fsn1"
count = 2
[placement]
stack = "control"
services = "control"
nodes = ["workers"]
"#,
        )
        .unwrap();
        registry.write_spec(&spec).unwrap();
        let script = temp.path().join("fake-tofu");
        let body = format!(
            r#"#!/bin/sh
set -eu
group=$(basename "$PWD")
printf 'call %s %s\n' "$group" "$1" >> '{fixture}/tofu.log'
env | grep -E '^(HCLOUD_TOKEN|MINIO_USER|MINIO_PASSWORD|AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SHARED_CREDENTIALS_FILE|AWS_CONFIG_FILE|AWS_EC2_METADATA_DISABLED)=' >> '{fixture}/tofu.log' || true
case "$1" in
  init) exit 0 ;;
  plan) echo "plan for $group"; exit 2 ;;
  apply)
    i=1
    while test -f '{fixture}/applied-'"$group"'-'"$i"'.json'; do i=$((i+1)); done
    cp swarmy.tfvars.json '{fixture}/applied-'"$group"'-'"$i"'.json'
    echo state > terraform.tfstate
    if test -f '{fixture}/fail-apply-'"$group"; then echo 'Error: apply failed' >&2; exit 1; fi ;;
  output)
    bucket=$(python3 -c 'import json; print(json.load(open("swarmy.tfvars.json"))["bucket_name"])')
    if test "$group" = control; then
      printf '{{"machines":{{"value":[{{"name":"fleet-control-1","public_ipv4":"203.0.113.10","public_ipv6":null,"private_ip":"10.0.0.10","ssh_user":"root"}}]}},"network_id":{{"value":"4711"}},"bucket":{{"value":{{"endpoint":"https://objects.example.com","name":"%s","region":"fsn1"}}}}}}\n' "$bucket"
    else
      printf '{{"machines":{{"value":[{{"name":"fleet-workers-1","public_ipv4":"203.0.113.11","public_ipv6":null,"private_ip":"10.0.0.11","ssh_user":"root"}},{{"name":"fleet-workers-2","public_ipv4":"203.0.113.12","public_ipv6":null,"private_ip":"10.0.0.12","ssh_user":"root"}}]}},"network_id":{{"value":null}}}}\n'
    fi ;;
  destroy) cp swarmy.tfvars.json '{fixture}/destroyed-'"$group"'.json'; rm terraform.tfstate ;;
esac
"#,
            fixture = temp.path().display()
        );
        fs::write(&script, body).unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
        let tofu = Tofu::from_binary(script, &temp.path().join("cache"));
        let mut credentials = CloudCredentials::default();
        credentials.set(
            CloudCredential::new(
                CloudProvider::HetznerCloud,
                vec!["hcloud-secret-5f2b9c".into()],
                None,
            )
            .unwrap(),
        );
        credentials.set(
            CloudCredential::new(
                CloudProvider::HetznerS3,
                vec!["s3-access-7a1e".into(), "s3-secret-91d0".into()],
                None,
            )
            .unwrap(),
        );
        Self {
            temp,
            registry,
            spec,
            tofu,
            credentials,
        }
    }
    fn machines(&self) -> MachineGroups<'_> {
        MachineGroups {
            tofu: &self.tofu,
            credentials: &self.credentials,
        }
    }
    fn up<'a>(
        &'a self,
        host: &'a FakeHost,
    ) -> impl Future<Output = Result<Vec<crate::provision::HostReport>>> + 'a {
        let mut settings = Settings::default();
        settings.selection.provider = "fake".into();
        // Settings is owned by the returned future, keeping its borrow alive.
        async move {
            provision_swarm_with(
                host,
                UpRequest {
                    registry: &self.registry,
                    spec: &self.spec,
                    sources: Vec::new(),
                    settings: &settings,
                    copy_credential: false,
                    recipe: None,
                    cache: self.temp.path(),
                    bucket_keys: None,
                    machines: Some(self.machines()),
                },
            )
            .await
        }
    }
    fn log(&self) -> String {
        fs::read_to_string(self.temp.path().join("tofu.log")).unwrap_or_default()
    }
}
use std::future::Future;

#[tokio::test]
async fn cloud_swarm_applies_in_order_and_provisions_through_the_shared_path() {
    let f = Fixture::new();
    let host = FakeHost::new();
    f.up(&host).await.unwrap();
    let log = f.log();
    let calls: Vec<_> = log
        .lines()
        .filter(|line| line.starts_with("call "))
        .collect();
    assert_eq!(
        calls,
        [
            "call control init",
            "call control apply",
            "call control output",
            "call workers init",
            "call workers apply",
            "call workers output"
        ]
    );
    let record = f.registry.read_record("fleet").unwrap().unwrap();
    let bucket = record.tofu_bucket.as_deref().unwrap();
    assert!(bucket.starts_with("swarmy-fleet-") && bucket.len() == 25);
    let control: Value =
        serde_json::from_slice(&fs::read(f.temp.path().join("applied-control-1.json")).unwrap())
            .unwrap();
    let workers: Value =
        serde_json::from_slice(&fs::read(f.temp.path().join("applied-workers-1.json")).unwrap())
            .unwrap();
    assert_eq!(control["bucket_name"], bucket);
    assert_eq!(workers["bucket_name"], "");
    assert_eq!(workers["network_id"], "4711");
    assert!(
        control["ssh_public_key"]
            .as_str()
            .unwrap()
            .starts_with("ssh-ed25519 ")
    );
    assert_eq!(record.hosts.values().map(Vec::len).sum::<usize>(), 3);
    assert!(record.hosts.values().flatten().all(|h| h.provisioned));
    assert_eq!(
        record.tofu_state.unwrap(),
        f.registry.swarm_dir("fleet").unwrap().join("tofu")
    );
    assert!(record.bucket_keys.is_none());
    assert_eq!(
        host.calls()
            .iter()
            .filter(|s| s.starts_with("ready "))
            .count(),
        3
    );
    for group in ["control", "workers"] {
        let key = f
            .registry
            .swarm_dir("fleet")
            .unwrap()
            .join("keys/groups")
            .join(group);
        assert_eq!(
            fs::metadata(key).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    assert!(f.log().contains("env fake") == false);
    assert!(f.log().contains("AWS_SHARED_CREDENTIALS_FILE=/dev/null"));
}

#[tokio::test]
async fn plan_and_destroy_use_recorded_state() {
    let f = Fixture::new();
    let plans = plan_groups(&f.registry, &f.spec, f.machines())
        .await
        .unwrap();
    assert_eq!(plans.len(), 2);
    assert!(
        plans
            .iter()
            .all(|p| p.changes && p.plan.contains("plan for"))
    );
    assert!(
        f.registry
            .read_record("fleet")
            .unwrap()
            .unwrap()
            .tofu_state
            .is_none()
    );
    let bucket = f
        .registry
        .read_record("fleet")
        .unwrap()
        .unwrap()
        .tofu_bucket;
    let host = FakeHost::new();
    f.up(&host).await.unwrap();
    assert_eq!(
        f.registry
            .read_record("fleet")
            .unwrap()
            .unwrap()
            .tofu_bucket,
        bucket
    );
    decommission_swarm_with(&host, &f.registry, &f.spec, Some(f.machines()))
        .await
        .unwrap();
    let log = f.log();
    let calls: Vec<_> = log
        .lines()
        .filter(|line| line.starts_with("call "))
        .collect();
    let worker_destroy = calls
        .iter()
        .position(|line| *line == "call workers destroy")
        .unwrap();
    let control_destroy = calls
        .iter()
        .position(|line| *line == "call control destroy")
        .unwrap();
    assert!(worker_destroy < control_destroy);
    let vars: Value =
        serde_json::from_slice(&fs::read(f.temp.path().join("destroyed-control.json")).unwrap())
            .unwrap();
    assert_eq!(vars["bucket_force_destroy"], true);
    assert!(f.registry.read_record("fleet").unwrap().is_none());
}
