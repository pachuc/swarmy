#![deny(clippy::disallowed_methods)]
use super::*;
use crate::provision::{
    decommission_swarm_with, provision_swarm_with, test_support::Fixture, tests::FakeHost,
};
use std::os::unix::fs::PermissionsExt;
use swarmy_config::{Settings, cloud::CloudCredential};

#[tokio::test]
async fn cloud_swarm_applies_in_order_and_provisions_through_the_shared_path() {
    let mut f = Fixture::new();
    f.spec.groups.get_mut("workers").unwrap().count = Some(2);
    f.registry.write_spec(&f.spec).unwrap();
    f.workers(2);
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
    let suffix = bucket.strip_prefix("swarmy-fleet-").unwrap();
    assert_eq!(suffix.len(), 12);
    assert!(
        suffix
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    );
    let control: Value =
        serde_json::from_slice(&fs::read(f.temp.path().join("applied-control-1.json")).unwrap())
            .unwrap();
    let workers: Value =
        serde_json::from_slice(&fs::read(f.temp.path().join("applied-workers-1.json")).unwrap())
            .unwrap();
    let expected_keys = [
        "swarm",
        "group",
        "machine_count",
        "size",
        "location",
        "ssh_public_key",
        "ssh_allowed_cidrs",
        "bucket_name",
        "bucket_force_destroy",
        "network_id",
    ];
    for vars in [&control, &workers] {
        let keys: std::collections::BTreeSet<_> = vars
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(keys, expected_keys.into_iter().collect());
    }
    assert_eq!(control["network_id"], "");
    assert_eq!(control["bucket_name"], bucket);
    assert_eq!(workers["bucket_name"], "");
    assert_eq!(workers["network_id"], "4711");
    assert_eq!(
        record.tofu_networks,
        BTreeMap::from([("control".to_owned(), "4711".to_owned())])
    );
    assert!(
        control["ssh_public_key"]
            .as_str()
            .unwrap()
            .starts_with("ssh-ed25519 ")
    );
    assert_eq!(record.hosts.values().map(Vec::len).sum::<usize>(), 3);
    assert!(record.hosts.values().flatten().all(|h| h.provisioned));
    assert_eq!(
        record.tofu_state.as_ref().unwrap(),
        &f.registry.swarm_dir("fleet").unwrap().join("tofu")
    );
    assert!(record.bucket_keys.is_none());
    assert_eq!(
        host.calls(),
        [
            "ready fleet-control-1".to_owned(),
            format!("provision fleet-control-1 primary=none bucket={bucket}"),
            "services fleet-control-1".to_owned(),
            "token fleet-control-1".to_owned(),
            "ready fleet-workers-1".to_owned(),
            format!("provision fleet-workers-1 primary=fleet-control-1 bucket={bucket}"),
            "ready fleet-workers-2".to_owned(),
            format!("provision fleet-workers-2 primary=fleet-control-1 bucket={bucket}"),
        ]
    );
    assert_group_keys_and_environment(&f, &record);
}

fn assert_group_keys_and_environment(f: &Fixture, record: &SwarmRecord) {
    let swarm_dir = f.registry.swarm_dir("fleet").unwrap();
    assert_eq!(
        fs::metadata(swarm_dir.join("keys/groups"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    for group in ["control", "workers"] {
        assert!(
            record.hosts[group]
                .iter()
                .all(|h| h.ssh_key == swarm_dir.join("keys/groups").join(group))
        );
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
    for call in f.log().split("call ").skip(1) {
        assert!(call.contains("HCLOUD_TOKEN=hcloud-secret-5f2b9c"));
        assert!(call.contains("MINIO_PASSWORD=s3-secret-91d0"));
        assert!(call.contains("AWS_SHARED_CREDENTIALS_FILE=/dev/null"));
    }
}

#[tokio::test]
async fn second_up_reapplies_both_groups_without_reprovisioning() {
    let mut f = Fixture::new();
    f.spec.groups.get_mut("workers").unwrap().count = Some(2);
    f.registry.write_spec(&f.spec).unwrap();
    f.workers(2);
    let host = FakeHost::new();
    f.up(&host).await.unwrap();
    let bucket = f
        .registry
        .read_record("fleet")
        .unwrap()
        .unwrap()
        .tofu_bucket
        .unwrap();
    let before = host.calls();
    f.up(&host).await.unwrap();
    assert_eq!(host.calls(), before);
    assert_eq!(
        f.registry
            .read_record("fleet")
            .unwrap()
            .unwrap()
            .tofu_bucket
            .as_deref(),
        Some(bucket.as_str())
    );
    assert_eq!(
        f.log()
            .lines()
            .filter(|line| *line == "call control apply")
            .count(),
        2
    );
    assert_eq!(
        f.log()
            .lines()
            .filter(|line| *line == "call workers apply")
            .count(),
        2
    );
    assert!(f.temp.path().join("applied-control-2.json").is_file());
    assert!(f.temp.path().join("applied-workers-2.json").is_file());
}

#[tokio::test]
async fn incompatible_network_locations_fail_before_any_tofu_call() {
    let mut f = Fixture::new();
    f.spec.groups.get_mut("workers").unwrap().location = Some("ash".into());
    let error = f.up(&FakeHost::new()).await.unwrap_err();
    let text = error.to_string();
    assert!(
        text.contains("control")
            && text.contains("workers")
            && text.contains("fsn1")
            && text.contains("ash"),
        "{text}"
    );
    assert!(f.log().is_empty());
    assert!(f.registry.read_record("fleet").unwrap().is_none());
    // fsn1 and nbg1 are in the same Hetzner network zone.
    f.spec.groups.get_mut("workers").unwrap().location = Some("nbg1".into());
    f.up(&FakeHost::new()).await.unwrap();
    let mut aws = Fixture::new();
    aws.credentials.set(
        CloudCredential::new(
            CloudProvider::Aws,
            vec!["aws-access-example".into(), "aws-secret-example".into()],
            None,
        )
        .unwrap(),
    );
    for group in aws.spec.groups.values_mut() {
        group.module = Module::Aws;
    }
    aws.spec.groups.get_mut("control").unwrap().location = Some("eu-west-1".into());
    aws.spec.groups.get_mut("workers").unwrap().location = Some("us-east-1".into());
    let error = aws.up(&FakeHost::new()).await.unwrap_err();
    let text = error.to_string();
    assert!(
        text.contains("control")
            && text.contains("workers")
            && text.contains("eu-west-1")
            && text.contains("us-east-1"),
        "{text}"
    );
    assert!(aws.log().is_empty());
    assert!(aws.registry.read_record("fleet").unwrap().is_none());
}

#[tokio::test]
async fn mixed_cloud_stack_with_existing_nodes() {
    let mut f = Fixture::new();
    f.add_existing_group("workers");
    let host = FakeHost::new();
    let reports = f.up(&host).await.unwrap();
    assert_eq!(reports.len(), 2);
    assert_eq!(
        f.log()
            .lines()
            .filter(|line| line.starts_with("call "))
            .collect::<Vec<_>>(),
        [
            "call control init",
            "call control apply",
            "call control output"
        ]
    );
    let bucket = f
        .registry
        .read_record("fleet")
        .unwrap()
        .unwrap()
        .tofu_bucket
        .unwrap();
    assert_eq!(
        host.calls(),
        [
            "ready fleet-control-1".to_owned(),
            format!("provision fleet-control-1 primary=none bucket={bucket}"),
            "services fleet-control-1".to_owned(),
            "token fleet-control-1".to_owned(),
            "adopt fleet-workers-1".to_owned(),
            format!("provision fleet-workers-1 primary=fleet-control-1 bucket={bucket}"),
        ]
    );
    let record = f.registry.read_record("fleet").unwrap().unwrap();
    assert!(record.hosts["control"][0].provisioned);
    assert!(record.hosts["workers"][0].provisioned);
    assert_eq!(
        fs::read(&record.hosts["workers"][0].ssh_key).unwrap(),
        b"test-key"
    );
    assert!(
        !f.registry
            .swarm_dir("fleet")
            .unwrap()
            .join("tofu/workers")
            .exists()
    );
}

#[tokio::test]
async fn down_cleans_existing_hosts_and_destroys_groups_in_reverse() {
    let mut f = Fixture::new();
    f.add_existing_group("existing");
    let host = FakeHost::new();
    f.up(&host).await.unwrap();
    let (reports, destroyed) =
        decommission_swarm_with(&host, &f.registry, &f.spec, Some(f.machines()))
            .await
            .unwrap();
    assert_eq!(destroyed, 2);
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].name, "fleet-existing-1");
    let calls = host.calls();
    assert!(calls.contains(&"decommission fleet-existing-1".into()));
    assert!(
        !calls
            .iter()
            .any(|call| call == "decommission fleet-control-1")
    );
    assert!(
        !calls
            .iter()
            .any(|call| call == "decommission fleet-workers-1")
    );
    assert_eq!(
        f.log()
            .lines()
            .filter(|line| line.contains(" destroy"))
            .collect::<Vec<_>>(),
        ["call workers destroy", "call control destroy"]
    );
    assert!(f.registry.read_record("fleet").unwrap().is_none());
    assert!(!f.registry.swarm_dir("fleet").unwrap().exists());
    assert!(f.registry.spec_path("fleet").unwrap().is_file());
    let log = f.log();
    let tofu_calls: Vec<_> = log
        .lines()
        .filter(|line| line.starts_with("call "))
        .collect();
    assert_eq!(
        tofu_calls,
        [
            "call control init",
            "call control apply",
            "call control output",
            "call workers init",
            "call workers apply",
            "call workers output",
            "call workers init",
            "call workers destroy",
            "call control init",
            "call control apply",
            "call control destroy",
        ]
    );
}

#[tokio::test]
async fn down_destroys_orphan_group_before_groups_in_the_spec() {
    let mut f = Fixture::new();
    f.spec.groups.get_mut("workers").unwrap().count = Some(2);
    f.registry.write_spec(&f.spec).unwrap();
    f.workers(2);
    let host = FakeHost::new();
    f.up(&host).await.unwrap();
    let root = f.registry.swarm_dir("fleet").unwrap().join("tofu");
    let orphan = root.join("orphan");
    fs::create_dir(&orphan).unwrap();
    for entry in fs::read_dir(root.join("workers")).unwrap() {
        let entry = entry.unwrap();
        if entry.path().is_file() {
            fs::copy(entry.path(), orphan.join(entry.file_name())).unwrap();
        }
    }
    assert!(orphan.join("terraform.tfstate").exists());
    decommission_swarm_with(&host, &f.registry, &f.spec, Some(f.machines()))
        .await
        .unwrap();
    assert_eq!(
        f.log()
            .lines()
            .filter(|line| line.contains(" destroy"))
            .collect::<Vec<_>>(),
        [
            "call orphan destroy",
            "call workers destroy",
            "call control destroy"
        ]
    );
    assert!(f.registry.read_record("fleet").unwrap().is_none());
}

#[tokio::test]
async fn empty_tofu_root_needs_no_runner_to_decommission_existing_hosts() {
    let mut f = Fixture::new();
    f.add_existing_group("control");
    f.add_existing_group("workers");
    fs::create_dir_all(f.registry.swarm_dir("fleet").unwrap().join("tofu")).unwrap();
    let host = FakeHost::new();
    f.up(&host).await.unwrap();
    decommission_swarm_with(&host, &f.registry, &f.spec, None)
        .await
        .unwrap();
    assert!(
        host.calls()
            .contains(&"decommission fleet-control-1".into())
    );
    assert!(
        host.calls()
            .contains(&"decommission fleet-workers-1".into())
    );
    assert!(f.registry.read_record("fleet").unwrap().is_none());
}

#[tokio::test]
async fn plan_only_directory_does_not_hide_an_existing_host_from_down() {
    let mut f = Fixture::new();
    f.add_existing_group("workers");
    let host = FakeHost::new();
    f.up(&host).await.unwrap();
    fs::create_dir(f.registry.swarm_dir("fleet").unwrap().join("tofu/workers")).unwrap();
    decommission_swarm_with(&host, &f.registry, &f.spec, Some(f.machines()))
        .await
        .unwrap();
    assert!(
        host.calls()
            .contains(&"decommission fleet-workers-1".into())
    );
    assert_eq!(
        f.log()
            .lines()
            .filter(|line| line.contains(" destroy"))
            .collect::<Vec<_>>(),
        ["call control destroy"]
    );
}

#[tokio::test]
async fn state_free_directories_do_not_count_as_destroyed_machine_groups() {
    let mut f = Fixture::new();
    f.spec.groups.get_mut("workers").unwrap().count = Some(2);
    f.registry.write_spec(&f.spec).unwrap();
    f.workers(2);
    f.up(&FakeHost::new()).await.unwrap();
    fs::create_dir(
        f.registry
            .swarm_dir("fleet")
            .unwrap()
            .join("tofu/plan-only"),
    )
    .unwrap();
    let mut record = f.registry.read_record("fleet").unwrap().unwrap();
    let machines = f.machines();
    assert_eq!(
        destroy(Some(&machines), &f.registry, &f.spec, &mut record)
            .await
            .unwrap(),
        2
    );
    assert!(
        !f.registry
            .swarm_dir("fleet")
            .unwrap()
            .join("tofu/plan-only")
            .exists()
    );
}

#[tokio::test]
async fn plan_and_destroy_use_recorded_state() {
    let mut f = Fixture::new();
    f.spec.groups.get_mut("workers").unwrap().count = Some(2);
    f.registry.write_spec(&f.spec).unwrap();
    f.workers(2);
    let plans = plan_groups(&f.registry, &f.spec, f.machines())
        .await
        .unwrap();
    assert_eq!(plans.len(), 2);
    assert_eq!(
        plans
            .iter()
            .map(|p| (p.group.as_str(), p.plan.as_str(), p.changes))
            .collect::<Vec<_>>(),
        [
            ("control", "plan for control\n", true),
            ("workers", "plan for workers\n", true),
        ]
    );
    assert!(!f.log().lines().any(|line| line.contains(" apply")));
    assert!(
        f.registry
            .read_record("fleet")
            .unwrap()
            .unwrap()
            .hosts
            .is_empty()
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
    assert!(bucket.is_some());
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

#[tokio::test]
async fn guards_run_before_any_tofu_call() {
    let mut f = Fixture::new();
    let host = FakeHost::new();
    f.credentials = CloudCredentials::default();
    let error = f.up(&host).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("swarmy cloud login hetzner-cloud")
    );
    assert!(f.log().is_empty());
    assert!(f.registry.read_record("fleet").unwrap().is_none());
    f.credentials.set(
        CloudCredential::new(
            CloudProvider::HetznerCloud,
            vec!["hcloud-secret-5f2b9c".into()],
            None,
        )
        .unwrap(),
    );
    let error = f.up(&host).await.unwrap_err();
    assert!(error.to_string().contains("swarmy cloud login hetzner-s3"));
    assert!(f.log().is_empty());
    f.credentials.set(
        CloudCredential::new(
            CloudProvider::HetznerS3,
            vec!["s3-access-7a1e".into(), "s3-secret-91d0".into()],
            None,
        )
        .unwrap(),
    );
    f.spec.groups.get_mut("workers").unwrap().module = Module::Path("relative/module".into());
    let error = f.up(&host).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("module path relative/module must be absolute")
    );
    assert!(f.log().is_empty());
    f.spec.groups.get_mut("workers").unwrap().module = Module::HetznerCloud;
    f.spec.groups.get_mut("workers").unwrap().count = Some(1);
    f.up(&host).await.unwrap();
    let prior_calls = f.log().lines().filter(|s| s.contains(" apply")).count();
    f.spec.groups.get_mut("workers").unwrap().count = Some(0);
    let error = f.up(&host).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("lowering count in the spec would destroy machines")
    );
    assert_eq!(
        f.log().lines().filter(|s| s.contains(" apply")).count(),
        prior_calls
    );
}

#[tokio::test]
async fn invalid_names_layout_and_chatgpt_credential_refuse_before_apply() {
    let mut swarm = Fixture::new();
    swarm.spec.name = "Fleet_1".into();
    let error = swarm.up(&FakeHost::new()).await.unwrap_err();
    assert!(error.to_string().contains("swarm name"), "{error}");
    assert!(swarm.log().is_empty());
    assert!(swarm.registry.read_record("Fleet_1").unwrap().is_none());

    let mut group = Fixture::new();
    let workers = group.spec.groups.remove("workers").unwrap();
    group.spec.groups.insert("Workers".into(), workers);
    group.spec.placement.nodes = vec!["Workers".into()];
    let error = group.up(&FakeHost::new()).await.unwrap_err();
    assert!(error.to_string().contains("group name"), "{error}");
    assert!(group.log().is_empty());
    assert!(group.registry.read_record("fleet").unwrap().is_none());

    let mut layout = Fixture::new();
    layout.spec.placement.services = "workers".into();
    let error = layout.up(&FakeHost::new()).await.unwrap_err();
    assert!(error.to_string().contains("placement.services"), "{error}");
    assert!(layout.log().is_empty());
    assert!(layout.registry.read_record("fleet").unwrap().is_none());

    let mut f = Fixture::new();
    f.spec.groups.get_mut("workers").unwrap().count = Some(2);
    f.registry.write_spec(&f.spec).unwrap();
    f.workers(2);
    let mut settings = Settings::default();
    settings.selection.provider = "chatgpt".into();
    let error = provision_swarm_with(&FakeHost::new(), f.request(&settings).unwrap())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("credential"), "{error}");
    assert!(f.log().is_empty());
    assert!(f.registry.read_record("fleet").unwrap().is_none());
}

#[tokio::test]
async fn builtin_size_change_refuses_before_reapplying() {
    let mut f = Fixture::new();
    let host = FakeHost::new();
    f.up(&host).await.unwrap();
    let prior = f
        .log()
        .lines()
        .filter(|line| line.contains(" apply"))
        .count();
    f.spec.groups.get_mut("workers").unwrap().size = Some("cx32".into());
    let error = f.up(&host).await.unwrap_err();
    assert!(
        error.to_string().contains("size changed from cx22 to cx32"),
        "{error}"
    );
    assert_eq!(
        f.log()
            .lines()
            .filter(|line| line.contains(" apply"))
            .count(),
        prior
    );
}

#[tokio::test]
async fn failed_apply_can_be_destroyed() {
    let mut f = Fixture::new();
    f.spec.groups.get_mut("workers").unwrap().count = Some(2);
    f.registry.write_spec(&f.spec).unwrap();
    f.workers(2);
    fs::write(f.temp.path().join("fail-apply-workers"), "").unwrap();
    let host = FakeHost::new();
    assert!(f.up(&host).await.is_err());
    let record = f.registry.read_record("fleet").unwrap().unwrap();
    assert_eq!(record.hosts["control"].len(), 1);
    assert!(!record.hosts.contains_key("workers"));
    assert!(record.tofu_state.is_some());
    assert!(
        f.registry
            .swarm_dir("fleet")
            .unwrap()
            .join("tofu/workers/terraform.tfstate")
            .exists()
    );
    decommission_swarm_with(&host, &f.registry, &f.spec, Some(f.machines()))
        .await
        .unwrap();
    assert_eq!(
        f.log()
            .lines()
            .filter(|line| line.contains(" destroy"))
            .collect::<Vec<_>>(),
        ["call workers destroy", "call control destroy"]
    );
    assert!(f.registry.read_record("fleet").unwrap().is_none());
}

#[tokio::test]
async fn aws_region_equal_to_the_location_reaches_tofu() {
    let mut f = Fixture::new();
    f.spec.groups.remove("workers");
    f.spec.placement.nodes.clear();
    let control = f.spec.groups.get_mut("control").unwrap();
    control.module = Module::Aws;
    control.location = Some("us-west-2".into());
    f.credentials.set(
        CloudCredential::new(
            CloudProvider::Aws,
            vec!["aws-access-example".into(), "aws-secret-example".into()],
            Some("us-west-2".into()),
        )
        .unwrap(),
    );
    f.up(&FakeHost::new()).await.unwrap();
    let vars: Value =
        serde_json::from_slice(&fs::read(f.temp.path().join("applied-control-1.json")).unwrap())
            .unwrap();
    assert_eq!(vars["location"], "us-west-2");
    let log = f.log();
    assert!(log.contains("AWS_REGION=us-west-2"));
    assert!(log.contains("AWS_ACCESS_KEY_ID=aws-access-example"));
}

#[tokio::test]
async fn aws_receives_only_aws_credentials_and_no_bucket() {
    let mut f = Fixture::new();
    f.spec.groups.remove("workers");
    f.spec.placement.nodes.clear();
    f.spec.groups.get_mut("control").unwrap().module = Module::Aws;
    f.credentials.set(
        CloudCredential::new(
            CloudProvider::Aws,
            vec!["aws-access-example".into(), "aws-secret-example".into()],
            None,
        )
        .unwrap(),
    );
    f.up(&FakeHost::new()).await.unwrap();
    let vars: Value =
        serde_json::from_slice(&fs::read(f.temp.path().join("applied-control-1.json")).unwrap())
            .unwrap();
    assert_eq!(vars["bucket_name"], "");
    let log = f.log();
    assert!(log.contains("AWS_SHARED_CREDENTIALS_FILE=/dev/null"));
    assert!(log.contains("AWS_CONFIG_FILE=/dev/null"));
    assert!(log.contains("AWS_EC2_METADATA_DISABLED=true"));
    assert!(log.contains("AWS_ACCESS_KEY_ID=aws-access-example"));
    assert!(!log.contains("HCLOUD_TOKEN"));
    assert!(!log.contains("MINIO_USER"));
    assert!(!log.contains("MINIO_PASSWORD"));
    assert_eq!(
        f.registry.read_record("fleet").unwrap().unwrap().hosts["control"][0].ssh_user,
        "ubuntu"
    );
}

fn check_dir(dir: &Path) {
    if !dir.exists() {
        return;
    }
    for item in fs::read_dir(dir).unwrap() {
        let path = item.unwrap().path();
        if path.is_dir() {
            check_dir(&path);
        } else {
            let bytes = fs::read(&path).unwrap();
            for secret in ["hcloud-secret-5f2b9c", "s3-access-7a1e", "s3-secret-91d0"] {
                assert!(
                    !bytes.windows(secret.len()).any(|w| w == secret.as_bytes()),
                    "secret reached {}",
                    path.display()
                );
            }
        }
    }
}
#[tokio::test]
async fn credentials_never_reach_files() {
    let mut f = Fixture::new();
    f.spec.groups.get_mut("workers").unwrap().count = Some(2);
    f.registry.write_spec(&f.spec).unwrap();
    f.workers(2);
    f.up(&FakeHost::new()).await.unwrap();
    for dir in [
        f.registry.dirs().data.as_path(),
        f.registry.dirs().config.as_path(),
        f.registry.dirs().state.as_path(),
        f.registry.dirs().cache.as_path(),
    ] {
        check_dir(dir);
    }
}

#[tokio::test]
async fn path_module_uses_only_top_level_terraform_files() {
    let mut f = Fixture::new();
    f.spec.groups.remove("workers");
    f.spec.placement.nodes.clear();
    let path = f.temp.path().join("custom-module");
    fs::create_dir(&path).unwrap();
    fs::write(path.join("main.tf"), "# main").unwrap();
    fs::write(path.join("outputs.tf"), "# outputs").unwrap();
    fs::write(path.join("README.md"), "not a module file").unwrap();
    f.spec.groups.get_mut("control").unwrap().module = Module::Path(path);
    f.up(&FakeHost::new()).await.unwrap();
    let dir = f.registry.swarm_dir("fleet").unwrap().join("tofu/control");
    assert!(dir.join("main.tf").exists());
    assert!(dir.join("outputs.tf").exists());
    assert!(!dir.join("README.md").exists());
    assert!(
        f.registry.read_record("fleet").unwrap().unwrap().hosts["control"][0]
            .private_address
            .is_none()
    );
    let log = f.log();
    assert!(log.contains("HCLOUD_TOKEN=hcloud-secret-5f2b9c"));
    assert!(log.contains("MINIO_PASSWORD=s3-secret-91d0"));
}

#[tokio::test]
async fn failed_destroy_keeps_remaining_hosts_recorded() {
    let mut f = Fixture::new();
    f.spec.groups.get_mut("workers").unwrap().count = Some(2);
    f.registry.write_spec(&f.spec).unwrap();
    f.workers(2);
    let host = FakeHost::new();
    f.up(&host).await.unwrap();
    fs::write(f.temp.path().join("fail-destroy-control"), "").unwrap();
    let error = decommission_swarm_with(&host, &f.registry, &f.spec, Some(f.machines()))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("stopped at OpenTofu group control")
    );
    let record = f.registry.read_record("fleet").unwrap().unwrap();
    assert!(!record.hosts.contains_key("workers"));
    assert!(record.hosts.contains_key("control"));
    fs::remove_file(f.temp.path().join("fail-destroy-control")).unwrap();
    decommission_swarm_with(&host, &f.registry, &f.spec, Some(f.machines()))
        .await
        .unwrap();
    assert!(f.registry.read_record("fleet").unwrap().is_none());
}

#[tokio::test]
async fn changed_output_names_keep_previous_hosts_recorded() {
    let mut f = Fixture::new();
    f.spec.groups.get_mut("workers").unwrap().count = Some(2);
    f.registry.write_spec(&f.spec).unwrap();
    f.workers(2);
    let host = FakeHost::new();
    f.up(&host).await.unwrap();
    fs::write(f.temp.path().join("workers-renamed"), "").unwrap();
    let error = f.up(&host).await.unwrap_err();
    assert!(error.to_string().contains("fleet-workers-1"));
    let record = f.registry.read_record("fleet").unwrap().unwrap();
    assert!(
        record.hosts["workers"]
            .iter()
            .any(|h| h.name == "fleet-workers-1" && h.provisioned)
    );
    assert!(
        !record.hosts["workers"]
            .iter()
            .any(|h| h.name == "fleet-workers-renamed")
    );
}

#[tokio::test]
async fn private_ip_is_awaited_for_builtin_modules() {
    let mut f = Fixture::new();
    f.spec.groups.get_mut("workers").unwrap().count = Some(2);
    f.registry.write_spec(&f.spec).unwrap();
    f.workers(2);
    fs::write(f.temp.path().join("workers-no-private"), "").unwrap();
    let error = f.up(&FakeHost::new()).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("has no private address after apply")
    );
    assert_eq!(
        f.log()
            .lines()
            .filter(|l| *l == "call workers apply")
            .count(),
        2
    );
}

#[tokio::test]
async fn adding_a_path_module_size_after_provisioning_is_refused() {
    let mut f = Fixture::new();
    f.spec.groups.remove("workers");
    f.spec.placement.nodes.clear();
    let path = f.temp.path().join("custom-module");
    fs::create_dir(&path).unwrap();
    fs::write(path.join("main.tf"), "# main").unwrap();
    let group = f.spec.groups.get_mut("control").unwrap();
    group.module = Module::Path(path);
    group.size = None;
    group.location = None;
    let host = FakeHost::new();
    f.up(&host).await.unwrap();
    let prior = f.log().lines().filter(|s| s.contains(" apply")).count();
    f.spec.groups.get_mut("control").unwrap().size = Some("larger".into());
    let error = f.up(&host).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("size changed from <unset> to larger")
    );
    assert_eq!(
        f.log().lines().filter(|s| s.contains(" apply")).count(),
        prior
    );
}

#[cfg(feature = "remote")]
mod remote_bucket_precedence {
    use super::*;
    use crate::{BucketRemoval, Cloud, ObjectBucket, Ownership};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use swarmy_config::{
        ObjectPrefix,
        swarm::{StaticKeys, SwarmBucket},
    };

    struct FakeCloud {
        verified: AtomicUsize,
        ensured: AtomicUsize,
    }
    impl FakeCloud {
        fn new() -> Self {
            Self {
                verified: AtomicUsize::new(0),
                ensured: AtomicUsize::new(0),
            }
        }
    }
    impl Cloud for FakeCloud {
        fn verify_bucket_access(&self, _: &ObjectBucket) -> impl Future<Output = Result<()>> {
            self.verified.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Ok(()))
        }
        fn ensure_bucket(&self, _: &ObjectBucket) -> impl Future<Output = Result<()>> {
            self.ensured.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Ok(()))
        }
        fn bucket_ownership(&self, _: &ObjectBucket) -> impl Future<Output = Result<Ownership>> {
            std::future::ready(Ok(Ownership::Owned))
        }
        fn delete_bucket(&self, _: &ObjectBucket) -> impl Future<Output = Result<BucketRemoval>> {
            std::future::ready(Ok(BucketRemoval::Removed))
        }
    }
    fn explicit_bucket(f: &mut Fixture) {
        f.spec.bucket = Some(SwarmBucket {
            name: "explicit-bucket".into(),
            endpoint: "https://objects.example.com".into(),
            region: "eu-central".into(),
            prefix: ObjectPrefix::default(),
            conditional_create: true,
        });
    }
    fn keys() -> StaticKeys {
        StaticKeys {
            access_key: "static-access".into(),
            secret_key: "static-secret".into(),
        }
    }
    #[tokio::test]
    async fn spec_bucket_replaces_stale_tofu_bucket_before_provisioning() {
        let mut f = Fixture::new();
        fs::write(f.temp.path().join("fail-apply-workers"), "").unwrap();
        let host = FakeHost::new();
        assert!(f.up(&host).await.is_err());
        let mut record = f.registry.read_record("fleet").unwrap().unwrap();
        assert!(record.tofu_bucket.is_some());
        assert!(!record.hosts["control"][0].provisioned);
        explicit_bucket(&mut f);
        record.bucket_keys = Some(keys());
        f.registry.write_record(&record).unwrap();
        fs::remove_file(f.temp.path().join("fail-apply-workers")).unwrap();
        fs::write(f.temp.path().join("control-no-bucket-private"), "").unwrap();
        let cloud = FakeCloud::new();
        let mut settings = Settings::default();
        settings.selection.provider = "fake".into();
        crate::provision::provision_swarm_with_backend(
            &host,
            &cloud,
            f.request(&settings).unwrap(),
        )
        .await
        .unwrap();
        let record = f.registry.read_record("fleet").unwrap().unwrap();
        assert!(record.tofu_bucket.is_none());
        assert!(record.tofu_bucket_endpoint.is_none());
        assert!(record.tofu_bucket_region.is_none());
        assert_eq!(cloud.verified.load(Ordering::Relaxed), 1);
        assert_eq!(cloud.ensured.load(Ordering::Relaxed), 1);
        assert!(host.calls().iter().any(|call| call == "provision fleet-control-1 primary=none bucket=explicit-bucket"));
        assert!(host.calls().iter().any(|call| call
            == "provision fleet-workers-1 primary=fleet-control-1 bucket=explicit-bucket"));
        assert_eq!(
            crate::provision::effective_bucket(&f.registry, &f.spec, &record, None)
                .unwrap()
                .unwrap()
                .bucket,
            "explicit-bucket"
        );
        let applied: Value = serde_json::from_slice(
            &fs::read(f.temp.path().join("applied-control-2.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(applied["bucket_name"], "");
    }

    #[tokio::test]
    async fn planning_with_spec_bucket_clears_unprovisioned_tofu_bucket() {
        let mut f = Fixture::new();
        fs::write(f.temp.path().join("fail-apply-workers"), "").unwrap();
        assert!(f.up(&FakeHost::new()).await.is_err());
        assert!(
            f.registry
                .read_record("fleet")
                .unwrap()
                .unwrap()
                .tofu_bucket
                .is_some()
        );
        explicit_bucket(&mut f);
        let plans = plan_groups(&f.registry, &f.spec, f.machines())
            .await
            .unwrap();
        assert_eq!(plans.len(), 2);
        let record = f.registry.read_record("fleet").unwrap().unwrap();
        assert!(record.tofu_bucket.is_none());
        assert!(record.tofu_bucket_endpoint.is_none());
        assert!(record.tofu_bucket_region.is_none());
        assert_eq!(
            f.log()
                .lines()
                .filter(|line| *line == "call control apply")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn bad_group_is_refused_before_spec_bucket_is_prepared() {
        let mut f = Fixture::new();
        explicit_bucket(&mut f);
        let workers = f.spec.groups.remove("workers").unwrap();
        f.spec.groups.insert("Workers".into(), workers);
        f.spec.placement.nodes = vec!["Workers".into()];
        let mut settings = Settings::default();
        settings.selection.provider = "fake".into();
        let mut request = f.request(&settings).unwrap();
        request.bucket_keys = Some(keys());
        let cloud = FakeCloud::new();
        let error =
            crate::provision::provision_swarm_with_backend(&FakeHost::new(), &cloud, request)
                .await
                .unwrap_err();
        assert!(error.to_string().contains("group name"), "{error}");
        assert_eq!(cloud.verified.load(Ordering::Relaxed), 0);
        assert_eq!(cloud.ensured.load(Ordering::Relaxed), 0);
        assert!(f.registry.read_record("fleet").unwrap().is_none());
        assert!(f.log().is_empty());
    }
}

#[tokio::test]
async fn failed_server_apply_does_not_block_bucket_teardown() {
    let f = Fixture::new();
    fs::write(f.temp.path().join("fail-apply-control"), "").unwrap();
    let host = FakeHost::new();
    assert!(f.up(&host).await.is_err());
    assert!(
        f.registry
            .swarm_dir("fleet")
            .unwrap()
            .join("tofu/control/terraform.tfstate")
            .exists()
    );
    decommission_swarm_with(&host, &f.registry, &f.spec, Some(f.machines()))
        .await
        .unwrap();
    let log = f.log();
    assert!(
        log.lines().any(|line| line.contains("target control apply")
            && line.contains("-target=minio_s3_bucket.this")),
        "{log}"
    );
    assert!(
        log.lines().any(|line| line == "call control destroy"),
        "{log}"
    );
    assert!(f.registry.read_record("fleet").unwrap().is_none());
}

#[test]
fn only_a_not_empty_bucket_retries_destroy() {
    assert!(bucket_not_empty(
        "Error: [FATAL] unable to remove bucket (b): The bucket that you tried to delete is not empty."
    ));
    assert!(bucket_not_empty(
        "BucketNotEmpty: The bucket you tried to delete is not empty"
    ));
    assert!(!bucket_not_empty(
        "Error: Server Type \"cpx41\" is unavailable"
    ));
}

fn log_sections(log: &str) -> BTreeMap<String, String> {
    let mut sections = BTreeMap::<String, String>::new();
    let mut current = None;
    for line in log.lines() {
        if let Some(group) = line
            .strip_prefix("call ")
            .and_then(|rest| rest.split_whitespace().next())
        {
            current = Some(group.to_owned());
        }
        if let Some(group) = &current {
            let section = sections.entry(group.clone()).or_default();
            section.push_str(line);
            section.push('\n');
        }
    }
    sections
}

#[tokio::test]
async fn scaleway_node_group_gets_only_scaleway_keys_and_no_network() {
    let mut f = Fixture::new();
    let group = f.spec.groups.get_mut("workers").unwrap();
    group.module = Module::Scaleway;
    group.size = Some("L4-1-24G".into());
    group.location = Some("fr-par-2".into());
    fs::write(f.temp.path().join("outputs-workers.json"), serde_json::json!({
        "machines": {"value": [{"name":"fleet-workers-1", "public_ipv4":"203.0.113.11", "public_ipv6":null, "private_ip":null, "ssh_user":"root"}]},
        "network_id": {"value":null}
    }).to_string()).unwrap();
    let host = FakeHost::new();
    let error = f.up(&host).await.unwrap_err();
    assert!(error.to_string().contains("swarmy cloud login scaleway"));
    assert!(f.log().is_empty());
    f.credentials.set(
        CloudCredential::new(
            CloudProvider::Scaleway,
            vec![
                "SCWEXAMPLEACCESS0001".into(),
                "scw-secret-4f1a".into(),
                "11111111-2222-3333-4444-555555555555".into(),
            ],
            None,
        )
        .unwrap(),
    );
    f.up(&host).await.unwrap();
    let sections = log_sections(&f.log());
    let workers = &sections["workers"];
    assert!(workers.contains("SCW_ACCESS_KEY=SCWEXAMPLEACCESS0001"));
    assert!(workers.contains("SCW_SECRET_KEY=scw-secret-4f1a"));
    assert!(workers.contains("SCW_DEFAULT_PROJECT_ID=11111111-2222-3333-4444-555555555555"));
    assert!(workers.contains("SCW_CONFIG_PATH=/nonexistent/swarmy/scw-config.yaml"));
    for absent in ["HCLOUD_TOKEN", "MINIO_USER", "AWS_ACCESS_KEY_ID"] {
        assert!(!workers.contains(absent));
    }
    assert!(!sections["control"].contains("SCW_ACCESS_KEY"));
    assert_eq!(
        workers
            .lines()
            .filter(|line| line.starts_with("call "))
            .collect::<Vec<_>>(),
        [
            "call workers init",
            "call workers apply",
            "call workers output"
        ]
    );
    let vars: Value =
        serde_json::from_slice(&fs::read(f.temp.path().join("applied-workers-1.json")).unwrap())
            .unwrap();
    assert_eq!(vars["network_id"], "");
    assert_eq!(vars["bucket_name"], "");
    let record = f.registry.read_record("fleet").unwrap().unwrap();
    assert_eq!(record.hosts["workers"][0].private_address, None);
}

#[test]
fn elastic_group_is_not_a_laptop_opentofu_group() {
    let mut f = Fixture::new();
    let mut gpu = f.spec.groups["workers"].clone();
    gpu.count = None;
    gpu.elastic = true;
    gpu.max = Some(2);
    gpu.price_per_hour = Some("0.75".into());
    gpu.currency = Some("EUR".into());
    f.spec.groups.insert("gpu".into(), gpu);
    f.spec.placement.nodes.push("gpu".into());
    f.spec.validate().unwrap();
    assert_eq!(apply_order(&f.spec), ["control", "workers"]);
    f.spec.groups.get_mut("workers").unwrap().module = Module::Existing;
    assert!(has_module_groups(&f.spec)); // control still creates machines
    f.spec.groups.get_mut("control").unwrap().module = Module::Existing;
    assert!(!has_module_groups(&f.spec));
}

#[test]
fn scaleway_module_environment_has_only_its_three_provider_keys() {
    let credential = CloudCredential::from_env(CloudProvider::Scaleway, |name| {
        Some(match name {
            "SCW_DEFAULT_PROJECT_ID" => "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".into(),
            _ => "test-value".into(),
        })
    })
    .unwrap();
    let mut credentials = CloudCredentials::default();
    credentials.set(credential);
    let env = module_credential_env(&credentials, &Module::Scaleway, false).unwrap();
    assert_eq!(
        env.names()
            .filter(|name| name.starts_with("SCW_") && *name != "SCW_CONFIG_PATH")
            .collect::<Vec<_>>(),
        ["SCW_ACCESS_KEY", "SCW_SECRET_KEY", "SCW_DEFAULT_PROJECT_ID"]
    );
}

#[tokio::test]
async fn second_up_keeps_the_first_group_creating_its_network() {
    let f = Fixture::new();
    let host = FakeHost::new();
    f.up(&host).await.unwrap();
    f.up(&host).await.unwrap();
    let control: Value =
        serde_json::from_slice(&fs::read(f.temp.path().join("applied-control-2.json")).unwrap())
            .unwrap();
    let workers: Value =
        serde_json::from_slice(&fs::read(f.temp.path().join("applied-workers-2.json")).unwrap())
            .unwrap();
    assert_eq!(control["network_id"], "");
    assert_eq!(workers["network_id"], "4711");
}

#[test]
fn elastic_group_joins_the_first_applied_group_of_its_module() {
    let mut f = Fixture::new();
    let mut gpu = f.spec.groups["workers"].clone();
    gpu.count = None;
    gpu.elastic = true;
    gpu.max = Some(2);
    gpu.price_per_hour = Some("0.75".into());
    gpu.currency = Some("EUR".into());
    f.spec.groups.insert("gpu".into(), gpu);
    f.spec.placement.nodes.push("gpu".into());
    let mut record = f
        .registry
        .read_record("fleet")
        .unwrap()
        .unwrap_or_else(|| SwarmRecord {
            name: "fleet".into(),
            created_at: jiff::Timestamp::now(),
            api: SwarmApi::default(),
            bucket_keys: None,
            hosts: BTreeMap::new(),
            tofu_state: None,
            tofu_bucket: None,
            tofu_bucket_endpoint: None,
            tofu_bucket_region: None,
            tofu_networks: BTreeMap::new(),
        });
    record.tofu_networks.insert("control".into(), "4711".into());
    assert_eq!(joined_network(&f.spec, &record, "gpu"), "4711");
    assert_eq!(joined_network(&f.spec, &record, "control"), "");
    assert_eq!(joined_network(&f.spec, &record, "workers"), "4711");
    f.spec.groups.get_mut("gpu").unwrap().module = Module::Scaleway;
    assert_eq!(joined_network(&f.spec, &record, "gpu"), "");
}

#[tokio::test]
async fn elastic_group_in_another_network_zone_is_refused() {
    let mut f = Fixture::new();
    let mut gpu = f.spec.groups["workers"].clone();
    gpu.count = None;
    gpu.elastic = true;
    gpu.max = Some(2);
    gpu.price_per_hour = Some("0.75".into());
    gpu.currency = Some("EUR".into());
    gpu.location = Some("ash".into());
    f.spec.groups.insert("gpu".into(), gpu);
    f.spec.placement.nodes.push("gpu".into());
    let error = f.up(&FakeHost::new()).await.unwrap_err();
    let text = error.to_string();
    assert!(
        text.contains("control")
            && text.contains("gpu")
            && text.contains("fsn1")
            && text.contains("ash"),
        "{text}"
    );
    assert!(f.log().is_empty());
}

#[tokio::test]
async fn group_image_reaches_the_module_and_is_immutable() {
    let mut f = Fixture::new();
    f.spec.groups.get_mut("workers").unwrap().image = Some("ubuntu-22.04".into());
    let host = FakeHost::new();
    f.up(&host).await.unwrap();
    let workers: Value =
        serde_json::from_slice(&fs::read(f.temp.path().join("applied-workers-1.json")).unwrap())
            .unwrap();
    let control: Value =
        serde_json::from_slice(&fs::read(f.temp.path().join("applied-control-1.json")).unwrap())
            .unwrap();
    assert_eq!(workers["image"], "ubuntu-22.04");
    assert!(control.get("image").is_none());
    let prior = f
        .log()
        .lines()
        .filter(|line| line.contains(" apply"))
        .count();
    f.spec.groups.get_mut("workers").unwrap().image = Some("debian-12".into());
    let error = f.up(&host).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("image changed from ubuntu-22.04 to debian-12"),
        "{error}"
    );
    assert_eq!(
        f.log()
            .lines()
            .filter(|line| line.contains(" apply"))
            .count(),
        prior
    );
}
