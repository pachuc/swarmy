#![deny(clippy::disallowed_methods)]
use super::{add_node, adopt, down, state::State};
use crate::{BucketRemoval, Cloud, Host, ObjectBucket, Ownership, Result};
use std::{
    cell::{Cell, RefCell},
    future::Future,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};
use swarmy_config::{Provider, RemoteNode, RemoteSettings};

#[derive(Default)]
struct FakeCloud {
    ensured: RefCell<Vec<String>>,
    removed: RefCell<Vec<String>>,
    unmanaged: Cell<bool>,
    denied: Cell<bool>,
    fail_verify: Cell<bool>,
    verify_calls: RefCell<Vec<String>>,
    retained: Cell<bool>,
    fail_delete: Cell<bool>,
}
impl Cloud for FakeCloud {
    fn ensure_bucket(&self, bucket: &ObjectBucket) -> impl Future<Output = Result<()>> {
        self.ensured.borrow_mut().push(bucket.spec.bucket.clone());
        std::future::ready(Ok(()))
    }
    fn verify_bucket_access(&self, bucket: &ObjectBucket) -> impl Future<Output = Result<()>> {
        self.verify_calls
            .borrow_mut()
            .push(bucket.spec.bucket.clone());
        std::future::ready(if self.fail_verify.get() {
            Err(crate::Error::other("invalid static credentials"))
        } else {
            Ok(())
        })
    }
    fn bucket_ownership(&self, _: &ObjectBucket) -> impl Future<Output = Result<Ownership>> {
        std::future::ready(if self.denied.get() {
            Err(crate::Error::MissingPermission {
                operation: "s3:GetBucketTagging".into(),
                source: Box::new(std::io::Error::other("denied")),
            })
        } else if self.unmanaged.get() {
            Ok(Ownership::Unmanaged)
        } else {
            Ok(Ownership::Owned)
        })
    }
    fn delete_bucket(&self, bucket: &ObjectBucket) -> impl Future<Output = Result<BucketRemoval>> {
        if self.fail_delete.get() {
            return std::future::ready(Err(crate::Error::MissingPermission {
                operation: "s3:ListBucketVersions".into(),
                source: Box::new(std::io::Error::other("denied")),
            }));
        }
        self.removed.borrow_mut().push(bucket.spec.bucket.clone());
        std::future::ready(Ok(if self.retained.get() {
            BucketRemoval::Retained
        } else {
            BucketRemoval::Removed
        }))
    }
}

#[derive(Default)]
struct FakeHost {
    fail_decommission: Cell<bool>,
    fail_names: RefCell<Vec<String>>,
    fail_image: Cell<bool>,
    images: RefCell<Vec<PathBuf>>,
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
    fn build_image(
        &self,
        _: &RemoteNode,
        _: &str,
        recipe: &Path,
    ) -> impl Future<Output = Result<()>> {
        self.images.borrow_mut().push(recipe.to_owned());
        std::future::ready(if self.fail_image.get() {
            Err(crate::Error::other("simulated image build failure"))
        } else {
            Ok(())
        })
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
        std::future::ready(
            if self.fail_decommission.get() || self.fail_names.borrow().contains(&node.name) {
                Err(crate::Error::other("simulated decommission failure"))
            } else {
                Ok(())
            },
        )
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
    let error = super::ensure_not_ec2(&node).err().unwrap();
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
    assert!(swarmy_core::error_chain(&error).contains("missing permission"));
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

#[test]
fn names_cannot_escape_state_and_commands_are_serialized() {
    for name in ["", "../escape", "a/b", "bad.name", "$(true)"] {
        assert!(swarmy_config::validate_remote_name(name).is_err());
    }
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let lock = state.lock().unwrap();
    assert!(state.lock().is_err());
    drop(lock);
    assert!(state.lock().is_ok());
}

fn node_settings(services: swarmy_config::RemoteServices, token: &str) -> swarmy_config::Settings {
    swarmy_config::Settings {
        remote: RemoteSettings {
            services,
            ..settings()
        },
        selection: swarmy_config::SelectionSettings {
            provider: "fake".into(),
            ..Default::default()
        },
        api: swarmy_config::ApiSettings {
            token: token.into(),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn node_config_token(config: &str) -> String {
    toml::Value::from_str(config)
        .unwrap()
        .get("api")
        .and_then(|api| api.get("token"))
        .and_then(|token| token.as_str())
        .unwrap_or_default()
        .to_owned()
}

#[test]
fn control_node_provisioning_generates_a_fresh_api_token_unless_configured() {
    let settings = node_settings(swarmy_config::RemoteServices::Node, "");
    let options = super::services::Options::with_keyring(&settings, false, None, None).unwrap();
    let first = node_config_token(options.config_toml());
    assert!(!first.is_empty());
    // Reprovisioning from the same laptop settings generates a fresh token for
    // the new node; the installed node keeps its own token because upgrades
    // only fill a missing one.
    let repeat = node_config_token(
        super::services::Options::with_keyring(&settings, false, None, None)
            .unwrap()
            .config_toml(),
    );
    assert!(!repeat.is_empty());
    assert_ne!(first, repeat);
    let configured = node_settings(swarmy_config::RemoteServices::Node, "existing-token");
    let kept = node_config_token(
        super::services::Options::with_keyring(&configured, false, None, None)
            .unwrap()
            .config_toml(),
    );
    assert_eq!(kept, "existing-token");
    assert!(toml::Value::from_str(&configured.to_toml().unwrap()).is_ok());
}

#[test]
fn laptop_services_provisioning_generates_no_api_token() {
    let settings = node_settings(swarmy_config::RemoteServices::Laptop, "");
    let options = super::services::Options::with_keyring(&settings, false, None, None).unwrap();
    assert!(node_config_token(options.config_toml()).is_empty());
}

#[test]
fn node_config_keeps_fake_call_log_relative_to_the_checkout() {
    // The laptop default login (`swarmy`) must never leak into node paths:
    // an existing host can have another login, so an absolute path
    // would point at the wrong checkout. The gateway resolves the relative
    // path against its unit working directory on the host.
    let settings = node_settings(swarmy_config::RemoteServices::Node, "");
    assert_eq!(settings.remote.service_user, "swarmy");
    let options = super::services::Options::with_keyring(&settings, false, None, None).unwrap();
    let config = toml::Value::from_str(options.config_toml()).unwrap();
    assert_eq!(
        config
            .get("fake")
            .and_then(|fake| fake.get("call_log"))
            .and_then(toml::Value::as_str),
        Some(".swarmy/calls.log")
    );
}

#[tokio::test]
async fn down_existing_skips_unprovisioned_hosts_but_removes_state() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(&dir.path().join("remote")).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    // An interrupted adoption saved its record before provisioning ran.
    let node = RemoteNode {
        name: "demo".into(),
        region: "us-east-1".into(),
        instance_id: String::new(),
        launch_attempted: false,
        public_ip: "203.0.113.10".into(),
        private_ip: "203.0.113.10".into(),
        key_path: state.directory.join("swarmy-test"),
        ssh_user: "root".into(),
        ports: swarmy_config::RemotePorts::default(),
        nodes: Vec::new(),
        sandboxes: 0,
        default_image: None,
        launch_settings: Some(settings()),
        created_at: "now".into(),
    };
    state.save(&node).unwrap();
    down::run_existing(&cloud, &host, &state, &node, true)
        .await
        .unwrap();
    assert!(host.decommissioned.borrow().is_empty());
    assert!(state.read("demo").unwrap().is_none());
}

#[tokio::test]
async fn adopt_rejects_ipv6_before_state_or_cloud_changes() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(&dir.path().join("remote")).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    let key = bootstrap_key(&dir);
    // Provisioning only accepts IPv4, so adopt must reject IPv6 up front
    // instead of creating state and a bucket first.
    let error = adopt::run(
        &cloud,
        &host,
        &state,
        &settings(),
        adopt::AdoptNode {
            name: "demo",
            host: "2001:db8::10",
            ssh_user: "root",
            ssh_key: &key,
            sandboxes: 0,
        },
        None.into(),
    )
    .await
    .unwrap_err();
    assert!(
        swarmy_core::error_chain(&error).contains("must be an IPv4 address"),
        "{error:?}"
    );
    assert!(state.read("demo").unwrap().is_none());
    assert!(host.provisioned.borrow().is_empty());
    assert!(cloud.ensured.borrow().is_empty());
}

#[tokio::test]
async fn adopt_rejects_aws_buckets_without_static_keys() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(&dir.path().join("remote")).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    let key = bootstrap_key(&dir);
    let settings = RemoteSettings {
        bucket: Some(swarmy_config::BucketSpec::aws("test-bucket")),
        ..settings()
    };
    // An IAM role would be created that the host could never assume.
    let error = adopt::run(
        &cloud,
        &host,
        &state,
        &settings,
        adopt::AdoptNode {
            name: "demo",
            host: "203.0.113.10",
            ssh_user: "root",
            ssh_key: &key,
            sandboxes: 0,
        },
        None.into(),
    )
    .await
    .unwrap_err();
    assert!(
        swarmy_core::error_chain(&error).contains("static keys"),
        "{error:?}"
    );
    assert!(state.read("demo").unwrap().is_none());
    assert!(cloud.ensured.borrow().is_empty());
}

#[test]
fn upgrade_summary_accepts_old_json() {
    let summary: super::upgrade::Summary =
        serde_json::from_str(r#"{"node":"","changed":[],"restarted":[],"elapsed_seconds":0}"#)
            .unwrap();
    assert!(summary.units.is_empty());
}

async fn adopt_existing(
    state: &State,
    cloud: &FakeCloud,
    host: &FakeHost,
    settings: &RemoteSettings,
    dir: &tempfile::TempDir,
    name: &str,
) {
    adopt::run(
        cloud,
        host,
        state,
        settings,
        adopt::AdoptNode {
            name,
            host: "203.0.113.10",
            ssh_user: "root",
            ssh_key: &bootstrap_key(dir),
            sandboxes: 4,
        },
        None.into(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn adopt_rejects_bad_input_before_state_or_cloud_changes() {
    for (address, ssh_user, sandboxes) in [
        ("not-an-address", "root", 0),
        ("203.0.113.10", "bad user", 0),
        ("203.0.113.10", "", 0),
        ("203.0.113.10", "root", 4),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let state = State::open(&dir.path().join("remote")).unwrap();
        let cloud = FakeCloud::default();
        let host = FakeHost::default();
        // Sandbox nodes without an explicit disk fail; the control-only
        // cases above fail on the address or login instead.
        let mut settings = settings();
        if sandboxes > 0 {
            settings.local_storage.clear();
        }
        let key = bootstrap_key(&dir);
        let result = adopt::run(
            &cloud,
            &host,
            &state,
            &settings,
            adopt::AdoptNode {
                name: "demo",
                host: address,
                ssh_user,
                ssh_key: &key,
                sandboxes,
            },
            None.into(),
        )
        .await;
        assert!(result.is_err(), "host={address} user={ssh_user}");
        assert!(state.read("demo").unwrap().is_none());
        assert!(host.provisioned.borrow().is_empty());
        assert!(cloud.ensured.borrow().is_empty());
    }
    // A missing key file and a duplicate name fail the same way.
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(&dir.path().join("remote")).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    let settings = settings();
    let missing = dir.path().join("absent-key");
    assert!(
        adopt::run(
            &cloud,
            &host,
            &state,
            &settings,
            adopt::AdoptNode {
                name: "demo",
                host: "203.0.113.10",
                ssh_user: "root",
                ssh_key: &missing,
                sandboxes: 0,
            },
            None.into(),
        )
        .await
        .is_err()
    );
    adopt_existing(&state, &cloud, &host, &settings, &dir, "demo").await;
    assert!(
        adopt::run(
            &cloud,
            &host,
            &state,
            &settings,
            adopt::AdoptNode {
                name: "demo",
                host: "203.0.113.11",
                ssh_user: "root",
                ssh_key: &bootstrap_key(&dir),
                sandboxes: 0,
            },
            None.into(),
        )
        .await
        .is_err()
    );
    assert_eq!(host.provisioned.borrow().len(), 1);
}

#[tokio::test]
async fn existing_join_rejects_bad_input_before_state_changes() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(&dir.path().join("remote")).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    adopt_existing(&state, &cloud, &host, &settings(), &dir, "demo").await;
    let key = bootstrap_key(&dir);
    let missing = dir.path().join("absent-key");
    for join in [
        add_node::ExistingJoin {
            host: "not-an-address",
            ssh_user: "admin",
            ssh_key: &key,
            primary_address: None,
        },
        add_node::ExistingJoin {
            host: "203.0.113.11",
            ssh_user: "bad user",
            ssh_key: &key,
            primary_address: None,
        },
        add_node::ExistingJoin {
            host: "203.0.113.11",
            ssh_user: "admin",
            ssh_key: &missing,
            primary_address: None,
        },
        add_node::ExistingJoin {
            host: "203.0.113.11",
            ssh_user: "admin",
            ssh_key: &key,
            primary_address: Some("not-an-address"),
        },
    ] {
        assert!(
            add_node::run(
                &host,
                &state,
                add_node::NewNode {
                    name: "demo",
                    sandboxes: 0,
                    local_storage: String::new(),
                    existing: join,
                },
                None,
            )
            .await
            .is_err()
        );
        assert!(state.require("demo").unwrap().nodes.is_empty());
    }
    assert!(cloud.ensured.borrow().is_empty());
    assert_eq!(host.provisioned.borrow().len(), 1);
}

#[tokio::test]
async fn down_existing_keeps_failed_hosts_and_reports_their_reasons() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(&dir.path().join("remote")).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    let settings = settings();
    adopt_existing(&state, &cloud, &host, &settings, &dir, "demo").await;
    let key = bootstrap_key(&dir);
    add_node::run(
        &host,
        &state,
        add_node::NewNode {
            name: "demo",
            sandboxes: 0,
            local_storage: String::new(),
            existing: add_node::ExistingJoin {
                host: "203.0.113.11",
                ssh_user: "admin",
                ssh_key: &key,
                primary_address: None,
            },
        },
        None,
    )
    .await
    .unwrap();
    // The child host is gone while the primary answers: teardown still
    // reaches the primary, then exits non-zero keeping only the failed
    // record, with the real failure reason (not an "unreachable" label).
    host.fail_names.borrow_mut().push("demo-2".into());
    let node = state.require("demo").unwrap();
    let primary_key = node.key_path.clone();
    let child_key = node.nodes[0].key_path.clone();
    let error = down::run_existing(&cloud, &host, &state, &node, true)
        .await
        .unwrap_err();
    let report = swarmy_core::error_chain(&error);
    assert!(report.contains("demo-2"), "{report}");
    assert!(
        report.contains("simulated decommission failure"),
        "{report}"
    );
    assert!(!report.contains("unreachable"), "{report}");
    // Both hosts were attempted; only the failed child's record is kept.
    assert_eq!(*host.decommissioned.borrow(), ["demo-2", "demo"]);
    let kept = state.require("demo").unwrap();
    assert_eq!(kept.nodes.len(), 1);
    assert_eq!(kept.nodes[0].name, "demo-2");
    assert!(primary_key.exists());
    assert!(child_key.exists());
    // The failed child is retried while the torn-down primary is a no-op:
    // clearing the failure completes the teardown and drops all state.
    host.fail_names.borrow_mut().clear();
    let kept = state.require("demo").unwrap();
    down::run_existing(&cloud, &host, &state, &kept, true)
        .await
        .unwrap();
    assert_eq!(
        *host.decommissioned.borrow(),
        ["demo-2", "demo", "demo-2", "demo"]
    );
    assert!(state.read("demo").unwrap().is_none());
    assert!(!primary_key.exists());
    assert!(!child_key.exists());
}

#[tokio::test]
async fn down_existing_prunes_torn_down_children_of_a_failed_primary() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(&dir.path().join("remote")).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    let settings = settings();
    adopt_existing(&state, &cloud, &host, &settings, &dir, "demo").await;
    let key = bootstrap_key(&dir);
    add_node::run(
        &host,
        &state,
        add_node::NewNode {
            name: "demo",
            sandboxes: 0,
            local_storage: String::new(),
            existing: add_node::ExistingJoin {
                host: "203.0.113.11",
                ssh_user: "admin",
                ssh_key: &key,
                primary_address: None,
            },
        },
        None,
    )
    .await
    .unwrap();
    // The primary fails while its child is torn down: the child is pruned
    // with its key file and only the primary record is kept for a retry.
    host.fail_names.borrow_mut().push("demo".into());
    let node = state.require("demo").unwrap();
    let primary_key = node.key_path.clone();
    let child_key = node.nodes[0].key_path.clone();
    let error = down::run_existing(&cloud, &host, &state, &node, true)
        .await
        .unwrap_err();
    assert!(
        swarmy_core::error_chain(&error).contains("demo"),
        "{error:?}"
    );
    let kept = state.require("demo").unwrap();
    assert!(kept.nodes.is_empty());
    assert!(primary_key.exists());
    assert!(!child_key.exists());
    assert!(!child_key.with_extension("pub").exists());
}

#[tokio::test]
async fn up_skip_custom_recipe_and_failed_image_preserve_correct_default() {
    for (recipe, fail_image, expected) in [
        (None, false, None),
        (
            Some(Path::new("images/custom-recipe")),
            false,
            Some("base-ubuntu:demo"),
        ),
        (Some(Path::new("images/base-ubuntu")), true, None),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let state = State::open(dir.path()).unwrap();
        let cloud = FakeCloud::default();
        let host = FakeHost::default();
        host.fail_image.set(fail_image);
        let result = adopt::run(
            &cloud,
            &host,
            &state,
            &settings(),
            adopt::AdoptNode {
                name: "demo",
                host: "203.0.113.10",
                ssh_user: "root",
                ssh_key: &bootstrap_key(&dir),
                sandboxes: 4,
            },
            recipe.into(),
        )
        .await;
        assert_eq!(result.is_err(), fail_image);
        let node = state.require("demo").unwrap();
        assert_eq!(node.default_image.as_deref(), expected);
        let profile = super::connect::new_profile(
            dir.path(),
            &node,
            node.ports,
            8742,
            dir.path().join("socket"),
        )
        .unwrap();
        let profile: swarmy_config::RemoteProfile =
            serde_json::from_slice(&serde_json::to_vec(&profile).unwrap()).unwrap();
        assert_eq!(profile.default_image.as_deref(), expected);
        let mut applied = swarmy_config::Settings::default();
        profile.apply(&mut applied);
        assert_eq!(applied.selection.default_image.as_deref(), expected);
        assert_eq!(host.images.borrow().first().map(PathBuf::as_path), recipe);
    }
}

#[tokio::test]
async fn static_bucket_up_skips_roles_and_redacts_logs() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    adopt_existing(&state, &cloud, &host, &with_bucket(), &dir, "static-test").await;
    assert_eq!(&*cloud.ensured.borrow(), &["test-bucket"]);
    assert_eq!(&*host.provisioned.borrow(), &["static-test"]);
    let node = state.require("static-test").unwrap();
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(state.directory.join("static-test.json"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "remote state holding keys must stay private");
    let spec = node.bucket_spec().unwrap();
    assert_eq!(spec.prefix.as_str(), "runs/team");
    for rendered in [
        format!("{node:?}"),
        format!("{:?}", node.cloud_settings().bucket),
        format!(
            "{:?}",
            ObjectBucket::from_spec("static-test", &spec, "eu-west-1")
        ),
    ] {
        assert!(!rendered.contains("static-access"), "{rendered}");
        assert!(!rendered.contains("static-secret"), "{rendered}");
    }
    let profile = super::connect::new_profile(
        dir.path(),
        &node,
        node.ports,
        8742,
        dir.path().join("socket"),
    )
    .unwrap();
    let mut applied = swarmy_config::Settings::default();
    profile.apply(&mut applied);
    assert_eq!(applied.s3.endpoint, "https://objects.example.invalid");
    assert_eq!(applied.s3.access_key, "static-access");
    assert_eq!(applied.s3.secret_key, "static-secret");
    assert_eq!(applied.s3.prefix.as_str(), "runs/team");
}

#[tokio::test]
async fn static_down_reports_a_retained_bucket_as_kept() {
    static DOWN_OUTPUT: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
    fn record(message: &str, _: bool) {
        DOWN_OUTPUT.lock().unwrap().push(message.to_owned());
    }
    crate::set_output_sink(record);
    DOWN_OUTPUT.lock().unwrap().clear();
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    let mut settings = with_bucket();
    settings.bucket.as_mut().unwrap().bucket = "retained-bucket".into();
    adopt_existing(&state, &cloud, &host, &settings, &dir, "retained-test").await;
    cloud.retained.set(true);
    let node = state.require("retained-test").unwrap();
    down::run_existing(&cloud, &host, &state, &node, false)
        .await
        .unwrap();
    let output = DOWN_OUTPUT.lock().unwrap().join("\n");
    assert!(
        output.contains(
            "Bucket retained-bucket: kept (bucket retains content outside the remote's prefix)"
        ),
        "{output}"
    );
    assert!(
        !output.contains("Bucket retained-bucket: removed"),
        "{output}"
    );
    assert!(state.read("retained-test").unwrap().is_none());
}

#[tokio::test]
async fn down_plan_reports_owned_resources_for_confirmation() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    adopt_existing(&state, &cloud, &host, &with_bucket(), &dir, "cleanup").await;
    let node = state.require("cleanup").unwrap();
    assert_eq!(
        down::plan(&cloud, &state, &node).await.unwrap(),
        Some(down::DeletionPlan {
            bucket: "test-bucket".into()
        })
    );
    assert!(
        cloud.removed.borrow().is_empty(),
        "planning must not delete"
    );
}

#[tokio::test]
async fn down_refuses_bucket_shared_by_another_remote() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    adopt_existing(&state, &cloud, &host, &with_bucket(), &dir, "cleanup").await;
    let node = state.require("cleanup").unwrap();
    let mut other = node.clone();
    other.name = "other".into();
    state.save(&other).unwrap();
    assert!(down::plan(&cloud, &state, &node).await.unwrap().is_none());
    down::run_existing(&cloud, &host, &state, &node, false)
        .await
        .unwrap();
    assert!(cloud.removed.borrow().is_empty());
    assert!(state.read("other").unwrap().is_some());
}

#[tokio::test]
async fn down_leaves_untagged_resources_and_removes_state() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    adopt_existing(&state, &cloud, &host, &with_bucket(), &dir, "cleanup").await;
    cloud.unmanaged.set(true);
    let node = state.require("cleanup").unwrap();
    down::run_existing(&cloud, &host, &state, &node, false)
        .await
        .unwrap();
    assert!(cloud.removed.borrow().is_empty());
    assert!(state.read("cleanup").unwrap().is_none());
}

#[tokio::test]
async fn denied_ownership_and_version_reads_retain_state_and_explain_permission() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    adopt_existing(&state, &cloud, &host, &with_bucket(), &dir, "cleanup").await;
    let node = state.require("cleanup").unwrap();
    cloud.denied.set(true);
    let error = down::plan(&cloud, &state, &node).await.unwrap_err();
    let report = swarmy_core::error_chain(&error);
    assert!(report.contains("s3:GetBucketTagging") && report.contains("missing permission"));
    assert!(state.read("cleanup").unwrap().is_some());
    cloud.denied.set(false);
    cloud.fail_delete.set(true);
    let error = down::run_existing(&cloud, &host, &state, &node, false)
        .await
        .unwrap_err();
    assert!(swarmy_core::error_chain(&error).contains("s3:ListBucketVersions"));
    assert!(state.read("cleanup").unwrap().is_some());
}

#[tokio::test]
async fn adopt_verifies_static_keys_before_saving_state() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    adopt_existing(&state, &cloud, &host, &with_bucket(), &dir, "static-test").await;
    assert_eq!(&*cloud.verify_calls.borrow(), &["test-bucket"]);
    assert!(state.require("static-test").is_ok());
    cloud.fail_verify.set(true);
    let error = adopt::run(
        &cloud,
        &host,
        &state,
        &with_bucket(),
        adopt::AdoptNode {
            name: "rejected-test",
            host: "203.0.113.10",
            ssh_user: "root",
            ssh_key: &bootstrap_key(&dir),
            sandboxes: 0,
        },
        None.into(),
    )
    .await
    .unwrap_err();
    assert!(swarmy_core::error_chain(&error).contains("were rejected"));
    assert!(state.read("rejected-test").unwrap().is_none());
    assert_eq!(cloud.ensured.borrow().len(), 1);
}

#[tokio::test]
async fn down_existing_keeps_the_bucket_until_every_host_is_torn_down() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    adopt_existing(&state, &cloud, &host, &with_bucket(), &dir, "static-test").await;
    host.fail_names.borrow_mut().push("static-test".into());
    let node = state.require("static-test").unwrap();
    down::run_existing(&cloud, &host, &state, &node, false)
        .await
        .unwrap_err();
    assert!(cloud.removed.borrow().is_empty());
    assert!(state.read("static-test").unwrap().is_some());
    host.fail_names.borrow_mut().clear();
    let node = state.require("static-test").unwrap();
    down::run_existing(&cloud, &host, &state, &node, false)
        .await
        .unwrap();
    assert_eq!(&*cloud.removed.borrow(), &["test-bucket"]);
    assert!(state.read("static-test").unwrap().is_none());
}
