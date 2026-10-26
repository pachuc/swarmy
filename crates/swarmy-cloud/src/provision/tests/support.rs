#![deny(clippy::disallowed_methods)]
use crate::Result;
use crate::provision::{
    UpRequest, existing_sources, groups::MachineGroups, provision_swarm_with, tests::FakeHost,
};
use std::{fs, future::Future, os::unix::fs::PermissionsExt};
use swarmy_config::{
    Settings,
    cloud::{CloudCredential, CloudCredentials, CloudProvider},
    paths::Dirs,
    swarm::{Module, Registry, SwarmSpec},
};
use swarmy_infra::Tofu;

pub(crate) struct Fixture {
    pub(crate) temp: tempfile::TempDir,
    pub(crate) registry: Registry,
    pub(crate) spec: SwarmSpec,
    pub(crate) tofu: Tofu,
    pub(crate) credentials: CloudCredentials,
}
impl Fixture {
    pub(crate) fn new() -> Self {
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
count = 1
[placement]
stack = "control"
services = "control"
nodes = ["workers"]
"#,
        )
        .unwrap();
        registry.write_spec(&spec).unwrap();
        let machines = serde_json::json!({"machines":{"value":[{"name":"fleet-workers-1","public_ipv4":"203.0.113.11","public_ipv6":null,"private_ip":"10.0.0.11","ssh_user":"root"}]},"network_id":{"value":null}});
        fs::write(
            temp.path().join("outputs-workers.json"),
            machines.to_string(),
        )
        .unwrap();
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
    if test "${{AWS_ACCESS_KEY_ID:-}}" != ""; then
      printf '{{"machines":{{"value":[{{"name":"fleet-control-1","public_ipv4":"203.0.113.10","public_ipv6":null,"private_ip":"10.0.0.10","ssh_user":"ubuntu"}}]}}}}\n'
    elif test "$group" = control && test "$bucket" = "" && test -f '{fixture}/control-no-bucket-private'; then
      printf '{{"machines":{{"value":[{{"name":"fleet-control-1","public_ipv4":"203.0.113.10","public_ipv6":null,"private_ip":"10.0.0.10","ssh_user":"root"}}]}}}}\n'
    elif test "$group" = control && test "$bucket" = ""; then
      printf '{{"machines":{{"value":[{{"name":"fleet-control-1","public_ipv4":"203.0.113.10","public_ipv6":null,"private_ip":null,"ssh_user":"root"}}]}}}}\n'
    elif test "$group" = control; then
      printf '{{"machines":{{"value":[{{"name":"fleet-control-1","public_ipv4":"203.0.113.10","public_ipv6":null,"private_ip":"10.0.0.10","ssh_user":"root"}}]}},"network_id":{{"value":"4711"}},"bucket":{{"value":{{"endpoint":"https://objects.example.com","name":"%s","region":"fsn1"}}}}}}\n' "$bucket"
    elif test -f '{fixture}/workers-renamed'; then
      printf '{{"machines":{{"value":[{{"name":"fleet-workers-renamed","public_ipv4":"203.0.113.11","public_ipv6":null,"private_ip":"10.0.0.11","ssh_user":"root"}},{{"name":"fleet-workers-2","public_ipv4":"203.0.113.12","public_ipv6":null,"private_ip":"10.0.0.12","ssh_user":"root"}}]}},"network_id":{{"value":null}}}}\n'
    elif test -f '{fixture}/workers-no-private'; then
      printf '{{"machines":{{"value":[{{"name":"fleet-workers-1","public_ipv4":"203.0.113.11","public_ipv6":null,"private_ip":null,"ssh_user":"root"}}]}}}}\n'
    elif test -f '{fixture}/outputs-workers.json'; then
      cat '{fixture}/outputs-workers.json'
    else
      printf '{{"machines":{{"value":[{{"name":"fleet-workers-1","public_ipv4":"203.0.113.11","public_ipv6":null,"private_ip":"10.0.0.11","ssh_user":"root"}},{{"name":"fleet-workers-2","public_ipv4":"203.0.113.12","public_ipv6":null,"private_ip":"10.0.0.12","ssh_user":"root"}}]}},"network_id":{{"value":null}}}}\n'
    fi ;;
  destroy) cp swarmy.tfvars.json '{fixture}/destroyed-'"$group"'.json'; if test -f '{fixture}/fail-destroy-'"$group"; then exit 1; fi; rm terraform.tfstate ;;
esac
"#,
            fixture = temp.path().display()
        );
        fs::write(&script, body).unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
        let tofu = Tofu::from_binary(script, &temp.path().join("cache"));
        let credentials = fake_credentials();
        Self {
            temp,
            registry,
            spec,
            tofu,
            credentials,
        }
    }
    pub(crate) fn add_existing_group(&mut self, name: &str) {
        let key = self.temp.path().join(format!("{name}-bootstrap"));
        fs::write(&key, "test-key").unwrap();
        let mut group = self.spec.groups["workers"].clone();
        group.module = Module::Existing;
        group.size = None;
        group.location = None;
        group.count = None;
        group.local_storage = Some("dir:/srv/swarmy-local".into());
        group.hosts = vec![swarmy_config::swarm::HostSpec {
            address: "203.0.113.21".into(),
            private_address: Some("10.0.0.21".into()),
            ssh_user: "root".into(),
            ssh_key: key,
        }];
        self.spec.groups.insert(name.into(), group);
        if !self.spec.placement.nodes.contains(&name.to_owned()) {
            self.spec.placement.nodes.push(name.into());
        }
        self.registry.write_spec(&self.spec).unwrap();
    }
    pub(crate) fn all_existing_workers(&mut self, count: usize) {
        self.add_existing_group("workers");
        let first = self.spec.groups["workers"].hosts[0].clone();
        let group = self.spec.groups.get_mut("workers").unwrap();
        for i in 2..=count {
            let mut host = first.clone();
            host.address = format!("203.0.113.{}", 20 + i);
            group.hosts.push(host);
        }
        let mut control = self.spec.groups["workers"].clone();
        control.hosts.truncate(1);
        control.hosts[0].address = "203.0.113.10".into();
        control.hosts[0].private_address = Some("10.0.0.10".into());
        control.local_storage = None;
        self.spec.groups.insert("control".into(), control);
        self.registry.write_spec(&self.spec).unwrap();
    }
    pub(crate) fn machines(&self) -> MachineGroups<'_> {
        MachineGroups {
            tofu: &self.tofu,
            credentials: &self.credentials,
        }
    }
    pub(crate) fn request<'a>(&'a self, settings: &'a Settings) -> Result<UpRequest<'a>> {
        Ok(UpRequest {
            registry: &self.registry,
            spec: &self.spec,
            sources: existing_sources(&self.spec, &self.registry.swarm_dir(&self.spec.name)?)?,
            settings,
            copy_credential: false,
            recipe: None,
            cache: self.temp.path(),
            bucket_keys: None,
            machines: Some(self.machines()),
        })
    }
    pub(crate) fn up<'a>(
        &'a self,
        host: &'a FakeHost,
    ) -> impl Future<Output = Result<Vec<crate::provision::HostReport>>> + 'a {
        let mut settings = Settings::default();
        settings.selection.provider = "fake".into();
        async move { provision_swarm_with(host, self.request(&settings)?).await }
    }
    pub(crate) fn workers(&self, count: usize) {
        let machines = (1..=count)
            .map(|i| {
                serde_json::json!({
                    "name":format!("fleet-workers-{i}"),
                    "public_ipv4":format!("203.0.113.{}", 10+i),
                    "public_ipv6":null,
                    "private_ip":format!("10.0.0.{}", 10+i),
                    "ssh_user":"root"
                })
            })
            .collect::<Vec<_>>();
        fs::write(
            self.temp.path().join("outputs-workers.json"),
            serde_json::json!({"machines":{"value":machines},"network_id":{"value":null}})
                .to_string(),
        )
        .unwrap();
    }
    pub(crate) fn log(&self) -> String {
        fs::read_to_string(self.temp.path().join("tofu.log")).unwrap_or_default()
    }
}

fn fake_credentials() -> CloudCredentials {
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
    credentials
}
