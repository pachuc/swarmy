#![deny(clippy::disallowed_methods)]
use super::{add_node, adopt, down, state::State};
use crate::{BucketRemoval, Cloud, Host, ObjectBucket, Ownership, Result};
use std::{
    cell::{Cell, RefCell},
    future::Future,
    path::Path,
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
    async fn ensure_bucket(&self, bucket: &ObjectBucket) -> Result<()> {
        self.ensured.borrow_mut().push(bucket.spec.bucket.clone());
        Ok(())
    }
    async fn verify_bucket_access(&self, _: &ObjectBucket) -> Result<()> {
        Ok(())
    }
    async fn bucket_ownership(&self, _: &ObjectBucket) -> Result<Ownership> {
        if self.denied.get() {
            return Err(crate::Error::other("permission denied"));
        }
        Ok(if self.unmanaged.get() {
            Ownership::Unmanaged
        } else {
            Ownership::Owned
        })
    }
    async fn delete_bucket(&self, bucket: &ObjectBucket) -> Result<BucketRemoval> {
        self.removed.borrow_mut().push(bucket.spec.bucket.clone());
        Ok(BucketRemoval::Removed)
    }
}

#[derive(Default)]
struct FakeHost {
    provisioned: RefCell<Vec<String>>,
    decommissioned: RefCell<Vec<String>>,
    primary_addresses: RefCell<Vec<Option<String>>>,
}
impl Host for FakeHost {
    fn services(
        &self,
        _: &RemoteNode,
        _: &str,
        _: &super::services::Options<'_>,
    ) -> impl Future<Output = Result<()>> {
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
        std::future::ready(Ok(()))
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
fn bootstrap_key(dir: &tempfile::TempDir) -> std::path::PathBuf {
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
