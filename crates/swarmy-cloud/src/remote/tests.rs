#![deny(clippy::disallowed_methods)]
use super::{add_node, adopt, down, state::State};
use crate::{BucketRemoval, Cloud, Host, ObjectBucket, Ownership, Result};
use std::{
    cell::{Cell, RefCell},
    future::Future,
    path::{Path, PathBuf},
    time::Duration,
};
use swarmy_config::{Provider, RemoteNode, RemoteSettings};

#[derive(Default)]
struct FakeCloud {
    ensured: RefCell<Vec<String>>,
    removed: RefCell<Vec<String>>,
    unmanaged: Cell<bool>,
    denied: Cell<bool>,
}
impl Cloud for FakeCloud {
    fn ensure_bucket(&self, bucket: &ObjectBucket) -> impl Future<Output = Result<()>> {
        self.ensured.borrow_mut().push(bucket.spec.bucket.clone());
        std::future::ready(Ok(()))
    }
    fn verify_bucket_access(&self, _: &ObjectBucket) -> impl Future<Output = Result<()>> {
        std::future::ready(Ok(()))
    }
    fn bucket_ownership(&self, _: &ObjectBucket) -> impl Future<Output = Result<Ownership>> {
        std::future::ready(if self.denied.get() {
            Err(crate::Error::other("permission denied"))
        } else if self.unmanaged.get() {
            Ok(Ownership::Unmanaged)
        } else {
            Ok(Ownership::Owned)
        })
    }
    fn delete_bucket(&self, bucket: &ObjectBucket) -> impl Future<Output = Result<BucketRemoval>> {
        self.removed.borrow_mut().push(bucket.spec.bucket.clone());
        std::future::ready(Ok(BucketRemoval::Removed))
    }
}

#[derive(Default)]
struct FakeHost {
    fail_decommission: Cell<bool>,
    services: Cell<usize>,
    credentials: RefCell<Vec<PathBuf>>,
    keyrings: RefCell<Vec<PathBuf>>,
    provisioned: RefCell<Vec<String>>,
    decommissioned: RefCell<Vec<String>>,
    primary_addresses: RefCell<Vec<Option<String>>>,
}
impl Host for FakeHost {
    fn services(
        &self,
        _: &RemoteNode,
        _: &str,
        options: &super::services::Options<'_>,
    ) -> impl Future<Output = Result<()>> {
        self.services.set(self.services.get() + 1);
        if let Some(path) = &options.credential {
            self.credentials.borrow_mut().push(path.clone());
        }
        if let Some(path) = &options.keyring {
            self.keyrings.borrow_mut().push(path.clone());
        }
        std::future::ready(Ok(()))
    }
    fn build_image(&self, _: &RemoteNode, _: &str, _: &Path) -> impl Future<Output = Result<()>> {
        std::future::ready(Ok(()))
    }
    fn adopt_key(&self, node: &RemoteNode, source: &Path) -> impl Future<Output = Result<()>> {
        std::future::ready(
            std::fs::copy(source, &node.key_path)
                .and_then(|_| {
                    std::fs::write(node.key_path.with_extension("pub"), "ssh-ed25519 test")
                })
                .map_err(crate::Error::from),
        )
    }
    fn decommission(&self, node: &RemoteNode) -> impl Future<Output = Result<()>> {
        self.decommissioned.borrow_mut().push(node.name.clone());
        std::future::ready(if self.fail_decommission.get() {
            Err(crate::Error::other("simulated decommission failure"))
        } else {
            Ok(())
        })
    }
    fn provision(
        &self,
        node: &RemoteNode,
        primary: Option<&RemoteNode>,
    ) -> impl Future<Output = Result<String>> {
        self.provisioned.borrow_mut().push(node.name.clone());
        self.primary_addresses
            .borrow_mut()
            .push(primary.map(|node| node.private_ip.clone()));
        std::future::ready(Ok(node.public_ip.clone()))
    }
}

fn settings() -> RemoteSettings {
    RemoteSettings {
        provider: Provider::Existing,
        region: "eu-west-1".into(),
        service_user: "swarmy".into(),
        local_storage: "dir:/srv/swarmy-local".into(),
        ..Default::default()
    }
}
fn with_bucket() -> RemoteSettings {
    use swarmy_config::{BucketCredentials, BucketSpec};
    RemoteSettings {
        bucket: Some(BucketSpec {
            endpoint: "https://objects.example.invalid".into(),
            region: "eu-west-1".into(),
            bucket: "test-bucket".into(),
            prefix: "runs/team".parse().unwrap(),
            credentials: BucketCredentials::StaticKeys {
                access_key: "static-access".into(),
                secret_key: "static-secret".into(),
            },
            ..BucketSpec::default()
        }),
        ..settings()
    }
}
fn bootstrap_key(dir: &tempfile::TempDir) -> PathBuf {
    let path = dir.path().join("bootstrap-key");
    std::fs::write(&path, "private-bootstrap").unwrap();
    path
}
async fn adopt(
    state: &State,
    cloud: &FakeCloud,
    host: &FakeHost,
    settings: &RemoteSettings,
    key: &Path,
) {
    adopt::run(
        cloud,
        host,
        state,
        settings,
        adopt::AdoptNode {
            name: "demo",
            host: "203.0.113.10",
            ssh_user: "root",
            ssh_key: key,
            sandboxes: 4,
        },
        None.into(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn adopt_provisions_an_existing_host() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(&dir.path().join("remote")).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    adopt(&state, &cloud, &host, &settings(), &bootstrap_key(&dir)).await;
    let node = state.require("demo").unwrap();
    assert_eq!(node.public_ip, "203.0.113.10");
    assert_eq!(node.ssh_user, "root");
    assert!(node.instance_id.is_empty());
    assert_eq!(node.launch_settings.unwrap().provider, Provider::Existing);
    assert_eq!(*host.provisioned.borrow(), ["demo"]);
}

#[tokio::test]
async fn add_node_joins_an_existing_host() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(&dir.path().join("remote")).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    let key = bootstrap_key(&dir);
    adopt(&state, &cloud, &host, &settings(), &key).await;
    add_node::run(
        &host,
        &state,
        add_node::NewNode {
            name: "demo",
            sandboxes: 8,
            local_storage: "dir:/srv/join-local".into(),
            existing: add_node::ExistingJoin {
                host: "203.0.113.11",
                ssh_user: "admin",
                ssh_key: &key,
                primary_address: Some("192.0.2.10"),
            },
        },
        None,
    )
    .await
    .unwrap();
    let node = state.require("demo").unwrap();
    assert_eq!(node.nodes[0].name, "demo-2");
    assert_eq!(node.nodes[0].local_storage(), "dir:/srv/join-local");
    assert_eq!(
        host.primary_addresses.borrow()[1].as_deref(),
        Some("192.0.2.10")
    );
    assert_eq!(*host.provisioned.borrow(), ["demo", "demo-2"]);
}

#[tokio::test]
async fn down_deletes_owned_bucket_and_keeps_unmanaged_one() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(&dir.path().join("remote")).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    adopt(&state, &cloud, &host, &with_bucket(), &bootstrap_key(&dir)).await;
    let node = state.require("demo").unwrap();
    assert_eq!(*cloud.ensured.borrow(), ["test-bucket"]);
    assert_eq!(
        down::plan(&cloud, &state, &node)
            .await
            .unwrap()
            .unwrap()
            .bucket,
        "test-bucket"
    );
    down::run_existing(&cloud, &host, &state, &node, false)
        .await
        .unwrap();
    assert_eq!(*cloud.removed.borrow(), ["test-bucket"]);
    assert!(state.read("demo").unwrap().is_none());
    assert_eq!(*host.decommissioned.borrow(), ["demo"]);
}

#[tokio::test]
async fn down_leaves_unmanaged_bucket() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(&dir.path().join("remote")).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    adopt(&state, &cloud, &host, &with_bucket(), &bootstrap_key(&dir)).await;
    cloud.unmanaged.set(true);
    let node = state.require("demo").unwrap();
    assert!(down::plan(&cloud, &state, &node).await.unwrap().is_none());
    down::run_existing(&cloud, &host, &state, &node, false)
        .await
        .unwrap();
    assert!(cloud.removed.borrow().is_empty());
}

#[test]
fn join_requires_host_and_key() {
    let key = Path::new("key");
    let no_host = add_node::resolve_join("demo", None, None, Some(key), None)
        .err()
        .unwrap();
    assert!(swarmy_core::error_chain(&no_host).contains("pass --host ADDRESS"));
    let no_key = add_node::resolve_join("demo", Some("203.0.113.11"), None, None, None)
        .err()
        .unwrap();
    assert!(swarmy_core::error_chain(&no_key).contains("--ssh-key PATH"));
}

#[tokio::test]
async fn down_refuses_a_record_launched_on_ec2() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(&dir.path().join("remote")).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    adopt(&state, &cloud, &host, &settings(), &bootstrap_key(&dir)).await;
    let mut node = state.require("demo").unwrap();
    node.launch_settings.as_mut().unwrap().provider = Provider::Aws;
    state.save(&node).unwrap();
    let error = super::down_target(&state, "demo").err().unwrap();
    assert!(swarmy_core::error_chain(&error).contains("launched on EC2"));
    assert!(state.read("demo").unwrap().is_some());
    let error = super::existing_launch(&node).err().unwrap();
    assert!(swarmy_core::error_chain(&error).contains("launched on EC2"));
}

#[tokio::test]
async fn denied_ownership_keeps_state_for_retry() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(&dir.path().join("remote")).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    adopt(&state, &cloud, &host, &with_bucket(), &bootstrap_key(&dir)).await;
    cloud.denied.set(true);
    let node = state.require("demo").unwrap();
    let error = down::run_existing(&cloud, &host, &state, &node, false)
        .await
        .err()
        .unwrap();
    assert!(swarmy_core::error_chain(&error).contains("permission denied"));
    assert!(state.read("demo").unwrap().is_some());
    assert!(cloud.removed.borrow().is_empty());
}

#[tokio::test]
async fn failed_host_decommission_keeps_bucket_and_record() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(&dir.path().join("remote")).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    adopt(&state, &cloud, &host, &with_bucket(), &bootstrap_key(&dir)).await;
    host.fail_decommission.set(true);
    let node = state.require("demo").unwrap();
    let error = down::run_existing(&cloud, &host, &state, &node, false)
        .await
        .err()
        .unwrap();
    assert!(swarmy_core::error_chain(&error).contains("simulated decommission failure"));
    assert!(state.read("demo").unwrap().is_some());
    assert!(cloud.removed.borrow().is_empty());
}

#[tokio::test]
async fn keep_bucket_decommissions_host_without_deleting_bucket() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(&dir.path().join("remote")).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    adopt(&state, &cloud, &host, &with_bucket(), &bootstrap_key(&dir)).await;
    let node = state.require("demo").unwrap();
    down::run_existing(&cloud, &host, &state, &node, true)
        .await
        .unwrap();
    assert!(cloud.removed.borrow().is_empty());
    assert!(state.read("demo").unwrap().is_none());
}

#[derive(Default)]
struct FakeUpgradeHost {
    events: RefCell<Vec<String>>,
    versions: RefCell<Vec<String>>,
    upgrades: RefCell<Vec<(String, bool, bool, Duration)>>,
}

impl super::upgrade::UpgradeHost for FakeUpgradeHost {
    fn has_service_units(&self, node: &RemoteNode) -> impl Future<Output = Result<bool>> {
        std::future::ready(Ok(node.name != "child-b"))
    }

    fn version(&self, node: &RemoteNode) -> impl Future<Output = Result<String>> {
        self.versions.borrow_mut().push(node.name.clone());
        self.events
            .borrow_mut()
            .push(format!("version:{}", node.name));
        std::future::ready(Ok("swarmy-core 0.1.0 (test)".into()))
    }

    fn upgrade(
        &self,
        node: &RemoteNode,
        services_only: bool,
        drain_timeout: Duration,
        stack: bool,
    ) -> impl Future<Output = Result<super::upgrade::Summary>> {
        self.upgrades
            .borrow_mut()
            .push((node.name.clone(), services_only, stack, drain_timeout));
        self.events
            .borrow_mut()
            .push(format!("upgrade:{}", node.name));
        std::future::ready(Ok(super::upgrade::Summary {
            node: node.name.clone(),
            changed: vec!["swarmy-core".into()],
            units: vec!["swarmyd.service".into()],
            restarted: if services_only {
                vec!["gateway".into()]
            } else {
                vec!["gateway".into(), "node".into()]
            },
            elapsed_seconds: 1.5,
        }))
    }
}

fn upgrade_fixture() -> RemoteNode {
    serde_json::from_value(serde_json::json!({
        "name": "primary", "region": "us-east-1", "instance_id": "i-first",
        "public_ip": "203.0.113.1", "private_ip": "10.0.0.1", "key_path": "key",
        "launch_attempted":true,"created_at": "now", "nodes": [
            {"name":"child-a", "region":"us-east-1", "instance_id":"i-a", "public_ip":"203.0.113.2", "private_ip":"10.0.0.2", "key_path":"key-a", "launch_attempted":true,"created_at":"now"},
            {"name":"child-b", "region":"us-east-1", "instance_id":"i-b", "public_ip":"203.0.113.3", "private_ip":"10.0.0.3", "key_path":"key-b", "launch_attempted":true,"created_at":"now"}
        ]
    }))
    .unwrap()
}

#[tokio::test]
async fn upgrade_orders_nodes_and_respects_drain_and_dirty_checkout() {
    let root = upgrade_fixture();
    let host = FakeUpgradeHost::default();
    let error = super::upgrade::run(
        &host,
        &root,
        super::upgrade::Options {
            dirty_paths: vec!["M src/main.rs".into()],
            allow_dirty: false,
            scope: super::upgrade::Scope::All,
            drain_timeout: Duration::from_secs(600),
            format: super::upgrade::Format::Human,
        },
    )
    .await
    .unwrap_err();
    assert!(format!("{error:#}").contains("src/main.rs"));
    assert!(host.versions.borrow().is_empty());
    assert!(host.upgrades.borrow().is_empty());
    let clean = super::upgrade::Options::new(false, true, 17, true);
    assert_eq!(
        super::upgrade::run(&host, &root, clean)
            .await
            .unwrap()
            .len(),
        3
    );
    host.versions.borrow_mut().clear();
    host.upgrades.borrow_mut().clear();
    host.events.borrow_mut().clear();
    let summaries = super::upgrade::run(
        &host,
        &root,
        super::upgrade::Options {
            dirty_paths: vec!["M src/main.rs".into()],
            allow_dirty: true,
            scope: super::upgrade::Scope::ServicesOnly,
            drain_timeout: Duration::from_secs(17),
            format: super::upgrade::Format::Human,
        },
    )
    .await
    .unwrap();
    assert_eq!(&*host.versions.borrow(), &["child-a", "child-b", "primary"]);
    assert_eq!(
        &*host.events.borrow(),
        &[
            "version:child-a",
            "version:child-b",
            "version:primary",
            "upgrade:child-a",
            "upgrade:child-b",
            "upgrade:primary",
        ]
    );
    assert_eq!(
        host.upgrades
            .borrow()
            .iter()
            .map(|row| row.0.as_str())
            .collect::<Vec<_>>(),
        ["child-a", "child-b", "primary"]
    );
    assert!(
        host.upgrades
            .borrow()
            .iter()
            .all(|row| row.1 && row.3 == Duration::from_secs(17))
    );
    assert_eq!(
        host.upgrades
            .borrow()
            .iter()
            .map(|row| row.2)
            .collect::<Vec<_>>(),
        [true, false, true]
    );
    assert_eq!(summaries[0].changed, ["swarmy-core"]);
    assert!(
        summaries
            .iter()
            .all(|summary| !summary.restarted.contains(&"node".to_owned()))
    );
}

#[tokio::test]
async fn upgrade_json_has_one_complete_object_per_node() {
    let root = upgrade_fixture();
    // The same rendering used for JSON output emits one complete object per node.
    let json_summaries = super::upgrade::run(
        &FakeUpgradeHost::default(),
        &root,
        super::upgrade::Options::new(false, false, 17, true),
    )
    .await
    .unwrap();
    assert_eq!(json_summaries.len(), 3);
    for summary in &json_summaries {
        let line = super::upgrade::json_line(summary).unwrap();
        let value: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert!(value.is_object());
        assert!(value["changed"].is_array());
        assert!(value["units"].is_array());
        assert!(value["restarted"].is_array());
        assert!(value["elapsed_seconds"].is_number());
    }
}

#[tokio::test]
async fn adopt_node_services_copy_credentials_only_with_acknowledgement() {
    for copy in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let state = State::open(&dir.path().join("remote")).unwrap();
        let auth = dir.path().join("auth.json");
        let script = dir.path().join("fake.json");
        std::fs::write(&auth, "test credential").unwrap();
        std::fs::write(&script, "{}").unwrap();
        let settings = swarmy_config::Settings {
            remote: RemoteSettings {
                services: swarmy_config::RemoteServices::Node,
                ..settings()
            },
            selection: swarmy_config::SelectionSettings {
                credential_file: auth.clone(),
                ..Default::default()
            },
            fake: swarmy_config::Fake {
                script,
                ..Default::default()
            },
            ..Default::default()
        };
        let cloud = FakeCloud::default();
        let host = FakeHost::default();
        let keyring = dir.path().join("keyring");
        swarmy_config::Keyring::generate_at(&keyring).unwrap();
        let options = super::services::Options::with_keyring(
            &settings,
            copy,
            None,
            copy.then_some(keyring.clone()),
        )
        .unwrap();
        adopt::run(
            &cloud,
            &host,
            &state,
            &settings.remote,
            adopt::AdoptNode {
                name: "demo",
                host: "203.0.113.10",
                ssh_user: "root",
                ssh_key: &bootstrap_key(&dir),
                sandboxes: 4,
            },
            options,
        )
        .await
        .unwrap();
        assert_eq!(host.services.get(), 1);
        assert_eq!(
            *host.keyrings.borrow(),
            if copy { vec![keyring] } else { vec![] }
        );
        assert_eq!(
            *host.credentials.borrow(),
            if copy { vec![auth] } else { vec![] }
        );
        assert_eq!(
            state
                .require("demo")
                .unwrap()
                .launch_settings
                .unwrap()
                .services,
            swarmy_config::RemoteServices::Node
        );
        let mut chatgpt = settings;
        chatgpt.selection.provider = "chatgpt".into();
        assert!(super::services::Options::new(&chatgpt, false, None).is_err());
        chatgpt.remote.services = swarmy_config::RemoteServices::Laptop;
        assert!(super::services::Options::new(&chatgpt, true, None).is_err());
    }
}

#[tokio::test]
async fn add_node_copies_secrets_only_when_requested() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(&dir.path().join("remote")).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    let key = bootstrap_key(&dir);
    adopt(&state, &cloud, &host, &settings(), &key).await;
    add_node::run(
        &host,
        &state,
        add_node::NewNode {
            name: "demo",
            sandboxes: 4,
            local_storage: "dir:/srv/join-local".into(),
            existing: add_node::ExistingJoin {
                host: "203.0.113.11",
                ssh_user: "root",
                ssh_key: &key,
                primary_address: None,
            },
        },
        None,
    )
    .await
    .unwrap();
    assert_eq!(host.services.get(), 0);
    assert!(host.keyrings.borrow().is_empty());
    assert!(host.credentials.borrow().is_empty());
    let auth = dir.path().join("auth.json");
    let script = dir.path().join("fake.json");
    let keyring = dir.path().join("keyring");
    std::fs::write(&auth, "test credential").unwrap();
    std::fs::write(&script, "{}").unwrap();
    swarmy_config::Keyring::generate_at(&keyring).unwrap();
    let settings = swarmy_config::Settings {
        remote: RemoteSettings {
            services: swarmy_config::RemoteServices::Node,
            ..settings()
        },
        selection: swarmy_config::SelectionSettings {
            credential_file: auth.clone(),
            ..Default::default()
        },
        fake: swarmy_config::Fake {
            script,
            ..Default::default()
        },
        ..Default::default()
    };
    let options =
        super::services::Options::with_keyring(&settings, true, None, Some(keyring.clone()))
            .unwrap();
    add_node::run(
        &host,
        &state,
        add_node::NewNode {
            name: "demo",
            sandboxes: 4,
            local_storage: "dir:/srv/join-local".into(),
            existing: add_node::ExistingJoin {
                host: "203.0.113.12",
                ssh_user: "root",
                ssh_key: &key,
                primary_address: None,
            },
        },
        Some(&options),
    )
    .await
    .unwrap();
    assert_eq!(host.services.get(), 1);
    assert_eq!(*host.credentials.borrow(), vec![auth]);
    assert_eq!(*host.keyrings.borrow(), vec![keyring]);
}
