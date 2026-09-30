use std::os::unix::fs::PermissionsExt;
use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    str::FromStr,
    time::Duration,
};

use crate::Result;
use swarmy_config::{RemoteNode, RemoteProfile, RemoteSettings};

use super::{
    Cloud, Host, Machine, MachineSpec, ObjectBucket, Ownership, down, retry_profile_propagation,
    state::State, up, wait_running,
};

/// The CLI's tag flow: list targets, confirm every exact name, then apply.
async fn tag_confirmed(
    cloud: &FakeCloud,
    state: &State,
    node: &RemoteNode,
    mut confirm: impl FnMut(&str, &str) -> Result<()>,
) -> Result<()> {
    for (kind, name) in down::adoption_targets(state, node)? {
        confirm(&kind, &name)?;
    }
    down::apply_tag(cloud, node).await
}

fn denied(operation: &str) -> crate::Error {
    crate::Error::MissingPermission {
        operation: operation.to_owned(),
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "AccessDenied",
        )),
    }
}

/// What `ensure_bucket` recorded: name, region, owner, endpoint, prefix, and
/// whether static keys were present (never the values).
type BucketEnsure = (String, String, String, Option<String>, String, bool);

#[derive(Default)]
struct FakeCloud {
    requests: RefCell<Vec<MachineSpec>>,
    teardown: RefCell<Vec<String>>,
    absent: Cell<bool>,
    deny_tag_read: Cell<bool>,
    deny_create_tags: Cell<bool>,
    deny_version_list: Cell<bool>,
    retain_bucket: Cell<bool>,
    absent_bucket: Cell<bool>,
    untagged: Cell<bool>,
    untagged_profile: Cell<bool>,
    untagged_role: Cell<bool>,
    tagged: RefCell<Vec<String>>,
    foreign_bucket: Cell<bool>,
    foreign_role: Cell<bool>,
    bucket_ensures: RefCell<Vec<BucketEnsure>>,
    bucket_creates: RefCell<Vec<String>>,
    role_creates: RefCell<Vec<String>>,
    policy_roles: RefCell<Vec<String>>,
    profile_creates: RefCell<Vec<String>>,
    profile_present: Cell<bool>,
    fail_profile_launch_once: Cell<bool>,
    launch_ids: RefCell<VecDeque<String>>,
    keys: RefCell<Vec<(String, Vec<u8>, String)>>,
    observations: RefCell<VecDeque<Option<Machine>>>,
    terminated: RefCell<Vec<String>>,
    deleted: RefCell<Vec<String>>,
    key_delete_attempts: RefCell<Vec<String>>,
    stock_reads: Cell<usize>,
    find_tokens: RefCell<Vec<String>>,
    fail_delete: Cell<bool>,
    fail_terminate: Cell<bool>,
}

impl Cloud for FakeCloud {
    fn ensure_bucket(&self, bucket: &ObjectBucket) -> impl Future<Output = Result<()>> {
        self.bucket_ensures.borrow_mut().push((
            bucket.spec.bucket.clone(),
            bucket.spec.region.clone(),
            bucket.owner.clone(),
            if bucket.spec.endpoint.is_empty() {
                None
            } else {
                Some(bucket.spec.endpoint.clone())
            },
            bucket.spec.prefix.as_str().to_owned(),
            bucket.spec.needs_static_keys(),
        ));
        if !self
            .bucket_creates
            .borrow()
            .iter()
            .any(|known| known == &bucket.spec.bucket)
        {
            self.bucket_creates
                .borrow_mut()
                .push(bucket.spec.bucket.clone());
        }
        let role = bucket
            .node_credentials
            .clone()
            .unwrap_or_else(|| format!("swarmy-{}", bucket.owner));
        // Creation succeeds even when the account denies ownership tags.
        if self.deny_create_tags.get() {
            self.untagged.set(true);
        }
        if bucket.spec.needs_static_keys() {
            // Static-key buckets have no IAM role or instance profile.
            return std::future::ready(Ok(()));
        }
        // The AWS provider writes the bucket policy to this role even on reuse.
        self.policy_roles.borrow_mut().push(role.clone());
        if !self.profile_present.replace(true) {
            self.role_creates.borrow_mut().push(role.clone());
            self.profile_creates.borrow_mut().push(role);
        }
        std::future::ready(Ok(()))
    }

    fn base_image(&self) -> impl Future<Output = Result<String>> {
        self.stock_reads.set(self.stock_reads.get() + 1);
        std::future::ready(Ok("ami-stock".into()))
    }
    fn import_ssh_key(
        &self,
        name: &str,
        public_key: Vec<u8>,
        owner: &str,
    ) -> impl Future<Output = Result<()>> {
        self.keys
            .borrow_mut()
            .push((name.into(), public_key, owner.into()));
        std::future::ready(Ok(()))
    }
    fn create(&self, spec: &MachineSpec) -> impl Future<Output = Result<String>> {
        self.requests.borrow_mut().push(spec.clone());
        if self.fail_profile_launch_once.replace(false) {
            return std::future::ready(Err(crate::Error::Aws {
                operation: "ec2:RunInstances".into(),
                code: "InvalidParameterValue".into(),
                message: "Invalid IAM Instance Profile name: fixture-profile".into(),
            }));
        }
        std::future::ready(Ok(self
            .launch_ids
            .borrow_mut()
            .pop_front()
            .unwrap_or_else(|| "i-test".into())))
    }
    fn get(&self, _: &str) -> impl Future<Output = Result<Option<Machine>>> {
        std::future::ready(Ok(self
            .observations
            .borrow_mut()
            .pop_front()
            .expect("unexpected describe call")))
    }
    fn find_by_tag(&self, token: &str) -> impl Future<Output = Result<Option<String>>> {
        self.find_tokens.borrow_mut().push(token.into());
        std::future::ready(Ok(Some("i-recovered".into())))
    }
    fn destroy(&self, id: &str) -> impl Future<Output = Result<()>> {
        if self.fail_terminate.get() {
            return std::future::ready(Err(denied("ec2:TerminateInstances")));
        }
        self.teardown.borrow_mut().push(format!("instance {id}"));
        self.terminated.borrow_mut().push(id.into());
        std::future::ready(Ok(()))
    }
    fn bucket_ownership(&self, _: &ObjectBucket) -> impl Future<Output = Result<Ownership>> {
        if self.deny_tag_read.get() {
            return std::future::ready(Err(denied("s3:GetBucketTagging")));
        }
        std::future::ready(Ok(if self.absent.get() || self.absent_bucket.get() {
            Ownership::Absent
        } else if self.untagged.get() {
            Ownership::Unmanaged
        } else {
            Ownership::Owned
        }))
    }
    fn role_ownership(
        &self,
        _: &str,
        _: &str,
    ) -> impl Future<Output = Result<(Ownership, Ownership)>> {
        let status = |untagged: bool| {
            if self.absent.get() {
                Ownership::Absent
            } else if self.untagged.get() || untagged {
                Ownership::Unmanaged
            } else {
                Ownership::Owned
            }
        };
        std::future::ready(Ok((
            status(self.untagged_profile.get()),
            status(self.untagged_role.get()),
        )))
    }
    fn tag_bucket(&self, bucket: &ObjectBucket) -> impl Future<Output = Result<()>> {
        if self.foreign_bucket.get() {
            return std::future::ready(Err(crate::Error::other(
                "bucket belongs to another remote",
            )));
        }
        self.tagged
            .borrow_mut()
            .push(format!("bucket {}", bucket.spec.bucket));
        std::future::ready(Ok(()))
    }
    fn tag_node_role(&self, name: &str, _: &str) -> impl Future<Output = Result<()>> {
        if self.foreign_role.get() {
            return std::future::ready(Err(crate::Error::other("role belongs to another remote")));
        }
        self.tagged
            .borrow_mut()
            .push(format!("role and profile {name}"));
        std::future::ready(Ok(()))
    }
    fn delete_bucket(
        &self,
        bucket: &ObjectBucket,
    ) -> impl Future<Output = Result<crate::BucketRemoval>> {
        if self.deny_version_list.get() {
            return std::future::ready(Err(denied("s3:ListBucketVersions")));
        }
        self.teardown
            .borrow_mut()
            .push(format!("bucket {}", bucket.spec.bucket));
        std::future::ready(Ok(if self.absent.get() {
            crate::BucketRemoval::Absent
        } else if self.retain_bucket.get() {
            crate::BucketRemoval::Retained
        } else {
            crate::BucketRemoval::Removed
        }))
    }
    fn delete_node_role(&self, name: &str, _: &str) -> impl Future<Output = Result<(bool, bool)>> {
        self.teardown.borrow_mut().push(format!("role {name}"));
        std::future::ready(Ok((!self.absent.get(), !self.absent.get())))
    }
    fn delete_ssh_key(&self, name: &str) -> impl Future<Output = Result<()>> {
        self.teardown.borrow_mut().push(format!("key {name}"));
        self.key_delete_attempts.borrow_mut().push(name.into());
        if self.fail_delete.get() {
            return std::future::ready(Err(denied("ec2:DeleteKeyPair")));
        }
        self.deleted.borrow_mut().push(name.into());
        std::future::ready(Ok(()))
    }
}

#[derive(Default)]
struct FakeHost {
    provisioned: RefCell<Vec<RemoteNode>>,
    fail: bool,
    fail_image: bool,
    nvme_failure: bool,
    block_devices: RefCell<String>,
    images: RefCell<Vec<(String, String, std::path::PathBuf)>>,
    services: Cell<usize>,
    credentials: RefCell<Vec<std::path::PathBuf>>,
    keyrings: RefCell<Vec<std::path::PathBuf>>,
    primaries: RefCell<Vec<Option<RemoteNode>>>,
}

/// Default `lsblk -dno PATH,MODEL` fixture: EBS root plus one instance-store
/// disk. Tests override `block_devices` for reorderings.
fn default_block_devices() -> String {
    "/dev/nvme0n1 Amazon Elastic Block Store\n/dev/nvme1n1 Amazon EC2 NVMe Instance Storage\n"
        .to_owned()
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
        node: &RemoteNode,
        address: &str,
        recipe: &std::path::Path,
    ) -> impl Future<Output = Result<()>> {
        assert_eq!(self.provisioned.borrow().last().unwrap().name, node.name);
        self.images
            .borrow_mut()
            .push((node.name.clone(), address.into(), recipe.to_owned()));
        std::future::ready(if self.fail_image {
            Err(crate::Error::other("image build failed"))
        } else {
            Ok(())
        })
    }

    async fn generate_key(&self, node: &RemoteNode) -> Result<Vec<u8>> {
        tokio::fs::write(&node.key_path, "private-test").await?;
        tokio::fs::write(node.key_path.with_extension("pub"), "ssh-ed25519 test").await?;
        Ok(b"ssh-ed25519 test".to_vec())
    }
    fn block_devices(&self, _: &RemoteNode) -> impl Future<Output = Result<String>> {
        let listing = self.block_devices.borrow().clone();
        std::future::ready(Ok(if listing.is_empty() {
            default_block_devices()
        } else {
            listing
        }))
    }
    fn provision(
        &self,
        node: &RemoteNode,
        primary: Option<&RemoteNode>,
    ) -> impl Future<Output = Result<String>> {
        self.provisioned.borrow_mut().push(node.clone());
        self.primaries.borrow_mut().push(primary.cloned());
        if self.fail {
            return std::future::ready(Err(crate::Error::other("SSH failed")));
        }
        if self.nvme_failure {
            return std::future::ready(Err(crate::Error::other(
                "An instance with local NVMe storage is required",
            )));
        }
        std::future::ready(Ok(node.public_ip.clone()))
    }
}

fn instance(state: &str) -> Machine {
    Machine {
        id: "i-test".into(),
        public_ip: "203.0.113.10".into(),
        private_ip: "10.0.0.10".into(),
        state: state.into(),
    }
}

fn observe_running(cloud: &FakeCloud) {
    cloud
        .observations
        .borrow_mut()
        .push_back(Some(instance("running")));
}

fn settings() -> RemoteSettings {
    RemoteSettings {
        managed_by_tag: "codex-launcher".into(),
        aws: swarmy_config::AwsSettings {
            subnet: Some("subnet-test".into()),
            security_group: Some("sg-test".into()),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn assert_demo_node(node: &RemoteNode, host: &FakeHost) {
    assert_eq!(node.name, "demo", "provisioned node record: name");
    assert_eq!(
        node.default_image.as_deref(),
        Some("base-ubuntu:demo"),
        "provisioned node record: default image"
    );
    assert_eq!(
        *host.images.borrow(),
        [(
            "demo".into(),
            node.public_ip.clone(),
            "images/base-ubuntu".into()
        )],
        "provisioned node record: image build"
    );
    assert_eq!(node.region, "us-east-1", "provisioned node record: region");
    assert_eq!(
        node.instance_id, "i-test",
        "provisioned node record: instance"
    );
    assert_eq!(
        node.public_ip, "203.0.113.10",
        "provisioned node record: public ip"
    );
    assert_eq!(
        node.private_ip, "10.0.0.10",
        "provisioned node record: private ip"
    );
    assert_eq!(node.ssh_user, "ubuntu", "provisioned node record: ssh user");
    assert_eq!(
        (node.ports.fdb, node.ports.nats, node.ports.s3),
        (4500, 4222, 8333),
        "provisioned node record: ports"
    );
}

fn assert_demo_service(node: &RemoteNode) {
    // AWS launches write the service login explicitly and resolve the
    // instance-store device over SSH, so later configuration defaults never
    // move existing fleet checkouts. The checkout path itself is resolved on
    // the host (`~ubuntu/swarmy`); see the provisioning command tests.
    assert_eq!(
        node.service_user(),
        "ubuntu",
        "service login and storage: user"
    );
    assert_eq!(
        node.local_storage(),
        "/dev/nvme1n1",
        "service login and storage: device"
    );
    assert!(
        node.nodes.is_empty(),
        "service login and storage: no join nodes"
    );
    assert!(
        node.created_at.parse::<jiff::Timestamp>().is_ok(),
        "service login and storage: timestamp"
    );
    assert!(
        node.key_path.is_file(),
        "service login and storage: key file"
    );
}

fn assert_demo_permissions(state: &State) {
    assert_eq!(
        std::fs::metadata(state.directory.join("demo.json"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600,
        "key file permissions: node record"
    );
    assert_eq!(
        std::fs::metadata(&state.directory)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700,
        "key file permissions: state directory"
    );
}

fn assert_demo_launch(cloud: &FakeCloud, node: &RemoteNode) {
    let request = cloud.requests.borrow()[0].clone();
    assert_eq!(request.image, "ami-stock", "launch request: image");
    assert_eq!(request.disk_gb, 100, "launch request: disk");
    assert_eq!(
        request.instance_type, "m6id.xlarge",
        "launch request: shape"
    );
    assert_eq!(request.name, "demo", "launch request: name");
    assert_eq!(
        request.subnet.as_deref(),
        Some("subnet-test"),
        "launch request: subnet"
    );
    assert_eq!(
        request.security_group.as_deref(),
        Some("sg-test"),
        "launch request: security group"
    );
    assert_eq!(
        request.managed_by, "codex-launcher",
        "launch request: owner tag"
    );
    assert_eq!(
        request.key_name,
        super::key_name(node).unwrap(),
        "launch request: key name"
    );
    assert!(
        request.key_name.len() <= 64,
        "launch request: key name fits AWS limits"
    );
}

fn assert_demo_keys(cloud: &FakeCloud, host: &FakeHost, node: &RemoteNode) {
    let request = cloud.requests.borrow()[0].clone();
    assert_eq!(
        cloud.keys.borrow()[0],
        (
            request.key_name,
            b"ssh-ed25519 test".to_vec(),
            "codex-launcher".into()
        ),
        "key and provision records: uploaded key"
    );
    assert_eq!(
        host.provisioned.borrow()[0].public_ip,
        node.public_ip,
        "key and provision records: provisioned host"
    );
    assert!(
        cloud.observations.borrow().is_empty(),
        "key and provision records: observations drained"
    );
}

#[tokio::test]
async fn up_provisions_node_and_persists_launch_record() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(&dir.path().join("remote")).unwrap();
    let cloud = FakeCloud::default();
    cloud.observations.borrow_mut().extend([
        None,
        Some(instance("pending")),
        Some(instance("running")),
    ]);
    let host = FakeHost::default();
    up::run(
        &cloud,
        &host,
        &state,
        &settings(),
        up::NewNode {
            name: "demo",
            sandboxes: 64,
        },
        Some(std::path::Path::new("images/base-ubuntu")).into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    let node = state.read("demo").unwrap().unwrap();
    assert_demo_node(&node, &host);
    assert_demo_service(&node);
    assert_demo_permissions(&state);
    assert_demo_launch(&cloud, &node);
    assert_demo_keys(&cloud, &host, &node);
}

#[tokio::test]
async fn up_resolves_instance_store_by_model_not_name() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(&dir.path().join("remote")).unwrap();
    let cloud = FakeCloud::default();
    observe_running(&cloud);
    let host = FakeHost {
        block_devices: RefCell::new(
            "/dev/nvme0n1 Amazon Elastic Block Store\n\
             /dev/nvme1n1 Amazon Elastic Block Store\n\
             /dev/nvme2n1 Amazon EC2 NVMe Instance Storage\n"
                .to_owned(),
        ),
        ..Default::default()
    };
    up::run(
        &cloud,
        &host,
        &state,
        &settings(),
        up::NewNode {
            name: "demo",
            sandboxes: 4,
        },
        None.into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    let node = state.read("demo").unwrap().unwrap();
    assert_eq!(node.local_storage(), "/dev/nvme2n1");
}

#[tokio::test]
async fn joining_node_resolves_its_own_device_not_the_primarys() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    cloud
        .launch_ids
        .borrow_mut()
        .extend(["i-test".into(), "i-second".into()]);
    cloud.observations.borrow_mut().extend([
        Some(instance("running")),
        Some(Machine {
            id: "i-second".into(),
            private_ip: "10.0.0.11".into(),
            ..instance("running")
        }),
    ]);
    let host = FakeHost::default();
    up::run(
        &cloud,
        &host,
        &state,
        &settings(),
        up::NewNode {
            name: "demo",
            sandboxes: 4,
        },
        None.into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    // Simulate a primary whose saved settings carry its resolved device.
    let mut primary = state.require("demo").unwrap();
    assert_eq!(primary.local_storage(), "/dev/nvme1n1");
    primary
        .launch_settings
        .as_mut()
        .expect("launch settings were saved")
        .local_storage = "/dev/primary-disk".into();
    state.save(&primary).unwrap();
    super::add_node::run(
        &cloud,
        &host,
        &state,
        super::add_node::NewNode {
            name: "demo",
            sandboxes: 4,
            shape: super::NodeShape::default(),
            // Empty explicit configuration: the join resolves its own device
            // over SSH instead of reusing the primary's path.
            local_storage: String::new(),
        },
        Duration::ZERO,
        None,
    )
    .await
    .unwrap();
    let node = state.require("demo").unwrap();
    assert_eq!(node.nodes.len(), 1);
    assert_eq!(node.nodes[0].local_storage(), "/dev/nvme1n1");
    assert_eq!(host.provisioned.borrow()[1].local_storage(), "/dev/nvme1n1");
}

#[tokio::test]
async fn joining_node_keeps_explicit_local_storage_without_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    cloud
        .launch_ids
        .borrow_mut()
        .extend(["i-test".into(), "i-second".into()]);
    cloud.observations.borrow_mut().extend([
        Some(instance("running")),
        Some(Machine {
            id: "i-second".into(),
            private_ip: "10.0.0.11".into(),
            ..instance("running")
        }),
    ]);
    let host = FakeHost::default();
    up::run(
        &cloud,
        &host,
        &state,
        &settings(),
        up::NewNode {
            name: "demo",
            sandboxes: 0,
        },
        None.into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    // An unparseable listing proves no lookup ran: the explicit directory
    // carries the join without touching SSH block devices.
    let host = FakeHost {
        block_devices: RefCell::new("not a device listing\n".to_owned()),
        ..Default::default()
    };
    super::add_node::run(
        &cloud,
        &host,
        &state,
        super::add_node::NewNode {
            name: "demo",
            sandboxes: 4,
            shape: super::NodeShape::default(),
            local_storage: "dir:/srv/local".into(),
        },
        Duration::ZERO,
        None,
    )
    .await
    .unwrap();
    let node = state.require("demo").unwrap();
    assert_eq!(node.nodes.len(), 1);
    assert_eq!(node.nodes[0].local_storage(), "dir:/srv/local");
}

#[tokio::test]
async fn up_rejects_duplicate_node() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(&dir.path().join("remote")).unwrap();
    let cloud = FakeCloud::default();
    observe_running(&cloud);
    let host = FakeHost::default();
    up::run(
        &cloud,
        &host,
        &state,
        &settings(),
        up::NewNode {
            name: "demo",
            sandboxes: 64,
        },
        Some(std::path::Path::new("images/base-ubuntu")).into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    assert!(
        up::run(
            &cloud,
            &host,
            &state,
            &settings(),
            up::NewNode {
                name: "demo",
                sandboxes: 64
            },
            Some(std::path::Path::new("images/base-ubuntu")).into(),
            Duration::ZERO
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn configured_image_and_failed_provision_leave_recoverable_state() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    cloud
        .observations
        .borrow_mut()
        .push_back(Some(instance("running")));
    let settings = RemoteSettings {
        aws: swarmy_config::AwsSettings {
            image: Some("ami-custom".into()),
            ..settings().aws
        },
        ..settings()
    };
    let host = FakeHost {
        fail: true,
        ..Default::default()
    };
    assert!(
        up::run(
            &cloud,
            &host,
            &state,
            &settings,
            up::NewNode {
                name: "demo",
                sandboxes: 64
            },
            Some(std::path::Path::new("images/base-ubuntu")).into(),
            Duration::ZERO
        )
        .await
        .is_err()
    );
    assert_eq!(cloud.stock_reads.get(), 0);
    assert_eq!(cloud.requests.borrow()[0].image, "ami-custom");
    let node = state.read("demo").unwrap().unwrap();
    cloud.observations.borrow_mut().extend([
        Some(instance("shutting-down")),
        Some(instance("terminated")),
    ]);
    down::run(&cloud, &state, &node, Duration::ZERO, true)
        .await
        .unwrap();
    assert_eq!(*cloud.terminated.borrow(), ["i-test"]);
    assert_eq!(*cloud.deleted.borrow(), [super::key_name(&node).unwrap()]);
    assert!(state.read("demo").unwrap().is_none());
    assert!(!node.key_path.exists());
    assert!(!node.key_path.with_extension("pub").exists());
}

#[tokio::test]
async fn down_removes_state_when_key_deletion_is_denied() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    cloud
        .observations
        .borrow_mut()
        .extend([Some(instance("running")), None]);
    up::run(
        &cloud,
        &FakeHost::default(),
        &state,
        &settings(),
        up::NewNode {
            name: "demo",
            sandboxes: 64,
        },
        Some(std::path::Path::new("images/base-ubuntu")).into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    let node = state.read("demo").unwrap().unwrap();
    cloud.fail_delete.set(true);
    down::run(&cloud, &state, &node, Duration::ZERO, true)
        .await
        .unwrap();
    assert_eq!(cloud.key_delete_attempts.borrow().len(), 1);
    assert!(cloud.deleted.borrow().is_empty());
    assert!(state.read("demo").unwrap().is_none());
}

#[tokio::test]
async fn down_recovers_launch_before_instance_id_was_saved() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    cloud
        .observations
        .borrow_mut()
        .extend([Some(instance("running")), None]);
    up::run(
        &cloud,
        &FakeHost::default(),
        &state,
        &settings(),
        up::NewNode {
            name: "demo",
            sandboxes: 64,
        },
        Some(std::path::Path::new("images/base-ubuntu")).into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    let mut node = state.read("demo").unwrap().unwrap();
    node.instance_id.clear();
    state.save(&node).unwrap();
    down::run(&cloud, &state, &node, Duration::ZERO, true)
        .await
        .unwrap();
    assert_eq!(
        *cloud.find_tokens.borrow(),
        [super::key_name(&node).unwrap()]
    );
    assert_eq!(*cloud.terminated.borrow(), ["i-recovered"]);
}

#[tokio::test]
async fn wait_rejects_terminal_state_and_times_out() {
    let cloud = FakeCloud::default();
    cloud
        .observations
        .borrow_mut()
        .push_back(Some(instance("terminated")));
    assert!(
        wait_running(&cloud, "i-test", Duration::ZERO)
            .await
            .is_err()
    );
    cloud
        .observations
        .borrow_mut()
        .extend(std::iter::repeat_n(Some(instance("pending")), 120));
    assert!(
        wait_running(&cloud, "i-test", Duration::ZERO)
            .await
            .is_err()
    );
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

#[tokio::test]
async fn termination_failure_keeps_key_and_record_for_retry() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    cloud
        .observations
        .borrow_mut()
        .push_back(Some(instance("running")));
    up::run(
        &cloud,
        &FakeHost::default(),
        &state,
        &settings(),
        up::NewNode {
            name: "demo",
            sandboxes: 64,
        },
        Some(std::path::Path::new("images/base-ubuntu")).into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    let node = state.read("demo").unwrap().unwrap();
    cloud.fail_terminate.set(true);
    assert!(
        down::run(&cloud, &state, &node, Duration::ZERO, true)
            .await
            .is_err()
    );
    assert!(node.key_path.is_file());
    assert!(state.read("demo").unwrap().is_some());
    assert_eq!(cloud.deleted.borrow().len(), 1);
}

fn assert_join_records_child(node: &RemoteNode, cloud: &FakeCloud, host: &FakeHost) {
    assert_eq!(node.nodes.len(), 1, "join records one child node");
    let child = &node.nodes[0];
    assert_eq!(
        serde_json::to_value(node).unwrap()["sandboxes"],
        0,
        "join keeps the primary sandbox count"
    );
    assert_eq!(
        serde_json::to_value(node).unwrap()["nodes"][0]["sandboxes"],
        4,
        "join records the child sandbox count"
    );
    assert_eq!(node.sandboxes, 0, "primary sandbox count");
    assert_eq!(child.sandboxes, 4, "child sandbox count");
    assert_eq!(
        host.provisioned.borrow()[0].sandboxes,
        0,
        "primary provisioned with zero sandboxes"
    );
    assert_eq!(
        host.provisioned.borrow()[1].sandboxes,
        4,
        "child provisioned with four sandboxes"
    );
    assert_eq!(child.name, "demo-2", "child node name");
    assert_ne!(child.key_path, node.key_path, "child has its own key");
    assert_eq!(
        cloud.stock_reads.get(),
        1,
        "join reuses the saved launch without re-reading stock"
    );
    {
        let requests = cloud.requests.borrow();
        let join = &requests[1];
        assert_eq!(
            join.image, requests[0].image,
            "join launch reuses the primary image"
        );
        assert_eq!(
            join.subnet,
            settings().aws.subnet,
            "join launch reuses the saved subnet"
        );
        assert_eq!(
            join.security_group,
            settings().aws.security_group,
            "join launch reuses the saved security group"
        );
        assert_eq!(
            join.instance_type,
            settings().aws.instance_type,
            "join launch reuses the saved shape"
        );
        assert_eq!(
            join.disk_gb,
            settings().disk_gb,
            "join launch reuses the saved disk"
        );
        assert_eq!(
            join.managed_by, "codex-launcher",
            "join launch keeps the owner tag"
        );
        assert_eq!(
            join.key_name,
            super::key_name(child).unwrap(),
            "join launch names the child key"
        );
        assert_eq!(join.name, child.name, "join launch names the child");
    }
    assert_eq!(
        host.images.borrow().len(),
        1,
        "image builds once for primary and child"
    );
    assert!(
        host.primaries.borrow()[0].is_none(),
        "primary provisions with no upstream"
    );
    assert_eq!(
        host.primaries.borrow()[1].as_ref().unwrap().private_ip,
        node.private_ip,
        "child provisions against the primary"
    );
}

fn assert_down_removes_both(state: &State, cloud: &FakeCloud, node: &RemoteNode) {
    let child = &node.nodes[0];
    assert_eq!(
        *cloud.terminated.borrow(),
        ["i-second", "i-test"],
        "down terminates child before primary"
    );
    assert_eq!(
        cloud.deleted.borrow().len(),
        2,
        "down deletes both instances"
    );
    assert!(!child.key_path.exists(), "down removes the child key");
    assert!(!node.key_path.exists(), "down removes the primary key");
    assert!(
        state.read("demo").unwrap().is_none(),
        "down removes the state record"
    );
}

#[tokio::test]
async fn add_node_uses_saved_launch_and_primary_services_and_down_removes_both() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    cloud
        .launch_ids
        .borrow_mut()
        .extend(["i-test".into(), "i-second".into()]);
    cloud.observations.borrow_mut().extend([
        Some(instance("running")),
        Some(Machine {
            id: "i-second".into(),
            private_ip: "10.0.0.11".into(),
            ..instance("running")
        }),
    ]);
    up::run(
        &cloud,
        &host,
        &state,
        &settings(),
        up::NewNode {
            name: "demo",
            sandboxes: 0,
        },
        Some(std::path::Path::new("images/base-ubuntu")).into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    super::add_node::run(
        &cloud,
        &host,
        &state,
        super::add_node::NewNode {
            name: "demo",
            sandboxes: 4,
            shape: super::NodeShape::default(),
            local_storage: String::new(),
        },
        Duration::ZERO,
        None,
    )
    .await
    .unwrap();
    let node = state.require("demo").unwrap();
    assert_join_records_child(&node, &cloud, &host);
    cloud.observations.borrow_mut().extend([None, None]);
    down::run(&cloud, &state, &node, Duration::ZERO, true)
        .await
        .unwrap();
    assert_down_removes_both(&state, &cloud, &node);
}

#[tokio::test]
async fn failed_join_retains_child_for_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    cloud
        .observations
        .borrow_mut()
        .extend([Some(instance("running")), Some(instance("running"))]);
    up::run(
        &cloud,
        &FakeHost::default(),
        &state,
        &settings(),
        up::NewNode {
            name: "demo",
            sandboxes: 64,
        },
        Some(std::path::Path::new("images/base-ubuntu")).into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    let host = FakeHost {
        fail: true,
        ..Default::default()
    };
    assert!(
        super::add_node::run(
            &cloud,
            &host,
            &state,
            super::add_node::NewNode {
                name: "demo",
                sandboxes: 64,
                shape: super::NodeShape::default(),
                local_storage: String::new(),
            },
            Duration::ZERO,
            None
        )
        .await
        .is_err()
    );
    let node = state.require("demo").unwrap();
    assert_eq!(node.nodes.len(), 1);
    assert!(!node.nodes[0].instance_id.is_empty());
    cloud.fail_terminate.set(true);
    assert!(
        down::run(&cloud, &state, &node, Duration::ZERO, true)
            .await
            .is_err()
    );
    assert!(node.nodes[0].key_path.exists());
    assert!(node.key_path.exists());
    cloud.fail_terminate.set(false);
    cloud.observations.borrow_mut().extend([None, None]);
    down::run(&cloud, &state, &node, Duration::ZERO, true)
        .await
        .unwrap();
}

#[tokio::test]
async fn up_skip_custom_recipe_and_failed_image_preserve_correct_default() {
    for (recipe, fail_image, expected) in [
        (None, false, None),
        (
            Some(std::path::Path::new("images/custom-recipe")),
            false,
            Some("base-ubuntu:demo"),
        ),
        (Some(std::path::Path::new("images/base-ubuntu")), true, None),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let state = State::open(dir.path()).unwrap();
        let cloud = FakeCloud::default();
        cloud
            .observations
            .borrow_mut()
            .push_back(Some(instance("running")));
        let host = FakeHost {
            fail_image,
            ..Default::default()
        };
        let result = up::run(
            &cloud,
            &host,
            &state,
            &settings(),
            up::NewNode {
                name: "demo",
                sandboxes: 64,
            },
            recipe.into(),
            Duration::ZERO,
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
        let profile: RemoteProfile =
            serde_json::from_slice(&serde_json::to_vec(&profile).unwrap()).unwrap();
        assert_eq!(profile.default_image.as_deref(), expected);
        let mut settings = swarmy_config::Settings::default();
        profile.apply(&mut settings);
        assert_eq!(settings.selection.default_image.as_deref(), expected);
        assert_eq!(node.instance_id, "i-test");
        assert_eq!(
            host.images
                .borrow()
                .first()
                .map(|(_, _, path)| path.as_path()),
            recipe
        );
    }
}

#[tokio::test]
async fn node_services_copy_credentials_only_with_explicit_acknowledgement() {
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
                script: script.clone(),
                ..Default::default()
            },
            ..Default::default()
        };
        let cloud = FakeCloud::default();
        cloud
            .observations
            .borrow_mut()
            .push_back(Some(instance("running")));
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
        assert_eq!(options.keyring, copy.then_some(keyring.clone()));
        up::run(
            &cloud,
            &host,
            &state,
            &settings.remote,
            up::NewNode {
                name: "demo",
                sandboxes: 64,
            },
            options,
            Duration::ZERO,
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
async fn add_node_copies_both_secrets_only_when_requested() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    cloud.observations.borrow_mut().extend([
        Some(instance("running")),
        Some(instance("running")),
        Some(instance("running")),
    ]);
    let host = FakeHost::default();
    up::run(
        &cloud,
        &host,
        &state,
        &settings(),
        up::NewNode {
            name: "demo",
            sandboxes: 64,
        },
        None.into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    super::add_node::run(
        &cloud,
        &host,
        &state,
        super::add_node::NewNode {
            name: "demo",
            sandboxes: 64,
            shape: super::NodeShape::default(),
            local_storage: String::new(),
        },
        Duration::ZERO,
        None,
    )
    .await
    .unwrap();
    assert_eq!(host.services.get(), 0);
    assert!(host.keyrings.borrow().is_empty());
    assert!(host.credentials.borrow().is_empty());
    let auth = dir.path().join("auth.json");
    let keyring = dir.path().join("keyring");
    std::fs::write(&auth, "fixture").unwrap();
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
        ..Default::default()
    };
    let options =
        super::services::Options::with_keyring(&settings, true, None, Some(keyring.clone()))
            .unwrap();
    super::add_node::run(
        &cloud,
        &host,
        &state,
        super::add_node::NewNode {
            name: "demo",
            sandboxes: 64,
            shape: super::NodeShape::default(),
            local_storage: String::new(),
        },
        Duration::ZERO,
        Some(&options),
    )
    .await
    .unwrap();
    assert_eq!(host.services.get(), 1);
    assert_eq!(*host.credentials.borrow(), vec![auth]);
    assert_eq!(*host.keyrings.borrow(), vec![keyring]);
}

fn assert_bucket_launch_profile(cloud: &FakeCloud) {
    assert_eq!(
        cloud.requests.borrow()[0].profile.as_deref(),
        Some("swarmy-bucket-test"),
        "up launches with the bucket instance profile"
    );
}

async fn assert_bucket_reused_without_relaunch(
    cloud: &FakeCloud,
    host: &FakeHost,
    state: &State,
    settings: &RemoteSettings,
    request: up::NewNode<'_>,
) {
    assert!(
        up::run(
            cloud,
            host,
            state,
            settings,
            request,
            None.into(),
            Duration::ZERO
        )
        .await
        .is_ok(),
        "second up reuses the retained bucket without relaunching"
    );
    assert_eq!(
        cloud.bucket_ensures.borrow().len(),
        1,
        "bucket ensured once across both ups"
    );
    assert_eq!(
        cloud.requests.borrow().len(),
        1,
        "second up launches no new instance"
    );
    assert_eq!(
        cloud.bucket_creates.borrow().len(),
        1,
        "bucket created once"
    );
    assert_eq!(cloud.role_creates.borrow().len(), 1, "role created once");
    assert_eq!(
        cloud.profile_creates.borrow().len(),
        1,
        "instance profile created once"
    );
}

fn assert_bucket_join_profile(cloud: &FakeCloud) {
    assert_eq!(
        cloud.requests.borrow()[1].profile.as_deref(),
        Some("swarmy-bucket-test"),
        "joined node launches with the bucket instance profile"
    );
}

fn assert_bucket_connect_profile(profile: &RemoteProfile) {
    assert_eq!(
        profile.bucket.as_ref().unwrap().bucket,
        "test-bucket",
        "connect profile carries the bucket"
    );
    assert_eq!(
        profile.bucket.as_ref().unwrap().region,
        "us-east-1",
        "connect profile carries the region"
    );
    assert!(
        profile.s3_endpoint.is_empty(),
        "AWS bucket needs no endpoint override"
    );
}

fn assert_bucket_retained(cloud: &FakeCloud) {
    assert_eq!(
        cloud.bucket_ensures.borrow().len(),
        1,
        "down retains the bucket ensure"
    );
}

fn assert_bucket_recreated_once(cloud: &FakeCloud) {
    assert_eq!(
        cloud.bucket_ensures.borrow().len(),
        2,
        "third up ensures the bucket again"
    );
    assert_eq!(
        cloud.bucket_creates.borrow().len(),
        1,
        "bucket created once across three ups"
    );
    assert_eq!(
        cloud.role_creates.borrow().len(),
        1,
        "role created once across three ups"
    );
    assert_eq!(
        cloud.profile_creates.borrow().len(),
        1,
        "instance profile created once across three ups"
    );
}

struct StaticSetup {
    dir: tempfile::TempDir,
    state: State,
    cloud: FakeCloud,
    host: FakeHost,
    settings: RemoteSettings,
}

fn static_setup() -> StaticSetup {
    static_setup_with("test-bucket")
}

fn static_setup_with(bucket: &str) -> StaticSetup {
    use swarmy_config::{BucketCredentials, BucketSpec};
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(&dir.path().join("remote")).unwrap();
    let settings = RemoteSettings {
        region: "eu-west-1".into(),
        bucket: Some(BucketSpec {
            endpoint: "https://objects.example.invalid".into(),
            region: "eu-west-1".into(),
            bucket: bucket.into(),
            prefix: "runs/team".parse().unwrap(),
            credentials: BucketCredentials::StaticKeys {
                access_key: "static-access".into(),
                secret_key: "static-secret".into(),
            },
            ..BucketSpec::default()
        }),
        ..settings()
    };
    StaticSetup {
        dir,
        state,
        cloud: FakeCloud::default(),
        host: FakeHost::default(),
        settings,
    }
}

async fn static_up(setup: &StaticSetup) {
    static_up_as(setup, "static-test").await;
}

async fn static_up_as(setup: &StaticSetup, name: &str) {
    observe_running(&setup.cloud);
    up::run(
        &setup.cloud,
        &setup.host,
        &setup.state,
        &setup.settings,
        up::NewNode { name, sandboxes: 0 },
        None.into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn bucket_remote_uses_profile_and_retains_bucket_on_down() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(&dir.path().join("remote")).unwrap();
    let cloud = FakeCloud::default();
    observe_running(&cloud);
    let host = FakeHost::default();
    let request = up::NewNode {
        name: "bucket-test",
        sandboxes: 0,
    };
    let settings = RemoteSettings {
        bucket: Some(swarmy_config::BucketSpec::aws("test-bucket")),
        ..settings()
    };
    up::run(
        &cloud,
        &host,
        &state,
        &settings,
        request,
        None.into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    assert_bucket_launch_profile(&cloud);
    assert_bucket_reused_without_relaunch(&cloud, &host, &state, &settings, request).await;
    observe_running(&cloud);
    super::add_node::run(
        &cloud,
        &host,
        &state,
        super::add_node::NewNode {
            name: "bucket-test",
            sandboxes: 4,
            shape: super::NodeShape::default(),
            local_storage: String::new(),
        },
        Duration::ZERO,
        None,
    )
    .await
    .unwrap();
    assert_bucket_join_profile(&cloud);
    let node = state.require("bucket-test").unwrap();
    let profile = super::connect::new_profile(
        dir.path(),
        &node,
        node.ports,
        8742,
        dir.path().join("socket"),
    )
    .unwrap();
    assert_bucket_connect_profile(&profile);
    cloud.observations.borrow_mut().extend([None, None]);
    down::run(&cloud, &state, &node, Duration::ZERO, true)
        .await
        .unwrap();
    // The role and profile stay with the bucket; a later up reuses them.
    assert_bucket_retained(&cloud);
    observe_running(&cloud);
    up::run(
        &cloud,
        &host,
        &state,
        &settings,
        request,
        None.into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    assert_bucket_recreated_once(&cloud);
}

#[tokio::test]
async fn static_bucket_up_skips_roles_and_redacts_logs() {
    let setup = static_setup();
    static_up(&setup).await;
    let cloud = &setup.cloud;
    // The endpoint, prefix, and key presence (never the values) reach the
    // provider; no IAM profile is attached to the machine.
    assert_eq!(
        cloud.bucket_ensures.borrow().as_slice(),
        [(
            String::from("test-bucket"),
            String::from("eu-west-1"),
            String::from("static-test"),
            Some(String::from("https://objects.example.invalid")),
            String::from("runs/team"),
            true
        )]
    );
    assert!(cloud.requests.borrow()[0].profile.is_none());
    assert!(cloud.role_creates.borrow().is_empty());
    assert!(cloud.profile_creates.borrow().is_empty());
    // Saved state carries the description; formatter output never does.
    let node = setup.state.require("static-test").unwrap();
    assert_eq!(
        PermissionsExt::mode(
            &std::fs::metadata(setup.state.directory.join("static-test.json"))
                .unwrap()
                .permissions()
        ) & 0o777,
        0o600,
        "remote state holding static keys must stay private"
    );
    let spec = node.bucket_spec().unwrap();
    assert_eq!(spec.prefix.as_str(), "runs/team");
    for rendered in [
        format!("{node:?}"),
        format!("{:?}", node.cloud_settings().bucket),
        format!(
            "{:?}",
            ObjectBucket::from_spec(
                "static-test",
                &spec,
                "eu-west-1",
                node.cloud_settings().instance_profile("static-test"),
            )
        ),
    ] {
        assert!(!rendered.contains("static-access"), "{rendered}");
        assert!(!rendered.contains("static-secret"), "{rendered}");
    }
    // The connect profile carries keys into service settings for the laptop.
    let profile = super::connect::new_profile(
        setup.dir.path(),
        &node,
        node.ports,
        8742,
        setup.dir.path().join("socket"),
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
async fn static_bucket_tag_and_down_touch_only_the_bucket() {
    let setup = static_setup();
    static_up(&setup).await;
    let node = setup.state.require("static-test").unwrap();
    // Tag adopts only the bucket; down deletes it without touching roles.
    assert_eq!(
        down::adoption_targets(&setup.state, &node).unwrap(),
        [("bucket".to_owned(), "test-bucket".to_owned())]
    );
    tag_confirmed(&setup.cloud, &setup.state, &node, |_, _| Ok(()))
        .await
        .unwrap();
    assert_eq!(
        setup.cloud.tagged.borrow().as_slice(),
        ["bucket test-bucket"]
    );
    setup.cloud.observations.borrow_mut().extend([None, None]);
    down::run(&setup.cloud, &setup.state, &node, Duration::ZERO, false)
        .await
        .unwrap();
    assert!(
        setup
            .cloud
            .teardown
            .borrow()
            .iter()
            .any(|entry| entry == "bucket test-bucket")
    );
    assert!(
        !setup
            .cloud
            .teardown
            .borrow()
            .iter()
            .any(|entry| entry.starts_with("role"))
    );
}

#[tokio::test]
async fn static_down_reports_a_retained_bucket_as_kept() {
    static DOWN_OUTPUT: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
    fn record(message: &str, _: bool) {
        DOWN_OUTPUT.lock().unwrap().push(message.to_owned());
    }
    // The only test using the process-level provisioning sink; it stays set
    // for later tests, which never read it.
    crate::set_output_sink(record);
    DOWN_OUTPUT.lock().unwrap().clear();
    // Its own bucket keeps its sink messages apart from the other static
    // tests running in parallel in this process.
    let setup = static_setup_with("retained-bucket");
    static_up_as(&setup, "retained-test").await;
    // The swarm's prefix scope is deleted but the bucket itself remains
    // because it retains content outside that scope.
    setup.cloud.retain_bucket.set(true);
    let node = setup.state.require("retained-test").unwrap();
    setup.cloud.observations.borrow_mut().extend([None, None]);
    down::run(&setup.cloud, &setup.state, &node, Duration::ZERO, false)
        .await
        .unwrap();
    assert!(
        setup
            .cloud
            .teardown
            .borrow()
            .iter()
            .any(|entry| entry == "bucket retained-bucket")
    );
    // A retained bucket is reported as kept, not removed, and teardown
    // still completes and drops the local state.
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
    assert!(setup.state.require("retained-test").is_err());
}

#[tokio::test]
async fn iam_role_override_reaches_bucket_and_machine() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(&dir.path().join("remote")).unwrap();
    let cloud = FakeCloud::default();
    observe_running(&cloud);
    let host = FakeHost::default();
    let mut settings = settings();
    settings.bucket = Some(swarmy_config::BucketSpec::aws("test-bucket"));
    settings.aws.iam_role = Some("custom-node-role".into());
    up::run(
        &cloud,
        &host,
        &state,
        &settings,
        up::NewNode {
            name: "role-test",
            sandboxes: 0,
        },
        None.into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    assert_eq!(
        cloud.requests.borrow()[0].profile.as_deref(),
        Some("custom-node-role")
    );
    assert_eq!(
        cloud.role_creates.borrow().as_slice(),
        &["custom-node-role"]
    );
    assert_eq!(
        cloud.profile_creates.borrow().as_slice(),
        &["custom-node-role"]
    );
}

#[tokio::test]
async fn for_settings_rejects_unknown_provider() {
    let mut settings = settings();
    settings.provider = "other-cloud".into();
    let error = super::for_settings(&settings)
        .await
        .err()
        .expect("unknown provider must fail");
    assert!(
        error
            .to_string()
            .contains("unknown cloud provider 'other-cloud'")
    );
}

#[tokio::test]
async fn profile_propagation_retries_one_failed_fake_launch() {
    let cloud = FakeCloud::default();
    cloud.fail_profile_launch_once.set(true);
    let request = MachineSpec::from_settings(
        "test",
        "ami-test",
        "test-key",
        b"ssh-ed25519 test".to_vec(),
        &settings(),
        Some("swarmy-test".into()),
    );
    let id = retry_profile_propagation(|| cloud.create(&request), true, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(id, "i-test");
    assert_eq!(cloud.requests.borrow().len(), 2);
}

#[tokio::test]
async fn node_shape_overrides_are_per_node_and_persist_before_provisioning() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    let mut first = settings();
    super::NodeShape {
        instance_type: Some("m6i.large".into()),
        disk_gb: Some(40),
    }
    .apply(&mut first)
    .unwrap();
    cloud
        .launch_ids
        .borrow_mut()
        .extend(["i-test".into(), "i-second".into()]);
    cloud.observations.borrow_mut().extend([
        Some(instance("running")),
        Some(Machine {
            id: "i-second".into(),
            ..instance("running")
        }),
    ]);
    up::run(
        &cloud,
        &host,
        &state,
        &first,
        up::NewNode {
            name: "demo",
            sandboxes: 0,
        },
        None.into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    assert_eq!(cloud.requests.borrow()[0].instance_type, "m6i.large");
    assert_eq!(cloud.requests.borrow()[0].disk_gb, 40);
    super::add_node::run(
        &cloud,
        &host,
        &state,
        super::add_node::NewNode {
            name: "demo",
            sandboxes: 4,
            shape: super::NodeShape {
                instance_type: Some("m6id.4xlarge".into()),
                disk_gb: Some(100),
            },
            local_storage: first.local_storage.clone(),
        },
        Duration::ZERO,
        None,
    )
    .await
    .unwrap();
    let requests = cloud.requests.borrow();
    let joining = &requests[1];
    assert_eq!(joining.instance_type, "m6id.4xlarge");
    assert_eq!(joining.disk_gb, 100);
    assert_eq!(joining.subnet, first.aws.subnet);
    assert_eq!(joining.security_group, first.aws.security_group);
    assert_eq!(joining.image.as_str(), requests[0].image.as_str());
    assert_eq!(joining.managed_by, first.managed_by_tag);
    drop(requests);
    let saved = state.require("demo").unwrap();
    let primary_settings = saved.launch_settings.unwrap();
    let child_settings = saved.nodes[0].launch_settings.as_ref().unwrap();
    assert_eq!(
        (
            primary_settings.aws.instance_type.as_str(),
            primary_settings.disk_gb
        ),
        ("m6i.large", 40)
    );
    assert_eq!(
        (
            child_settings.aws.instance_type.as_str(),
            child_settings.disk_gb
        ),
        ("m6id.4xlarge", 100)
    );
}

#[tokio::test]
async fn invalid_join_shape_fails_before_launch_or_state_change() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    let host = FakeHost::default();
    cloud
        .observations
        .borrow_mut()
        .push_back(Some(instance("running")));
    up::run(
        &cloud,
        &host,
        &state,
        &settings(),
        up::NewNode {
            name: "demo",
            sandboxes: 64,
        },
        None.into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    for shape in [
        super::NodeShape {
            instance_type: Some(String::new()),
            disk_gb: None,
        },
        super::NodeShape {
            instance_type: None,
            disk_gb: Some(0),
        },
    ] {
        assert!(
            super::add_node::run(
                &cloud,
                &host,
                &state,
                super::add_node::NewNode {
                    name: "demo",
                    sandboxes: 4,
                    shape,
                    local_storage: settings().local_storage.clone(),
                },
                Duration::ZERO,
                None
            )
            .await
            .is_err()
        );
        assert_eq!(cloud.requests.borrow().len(), 1);
        assert!(state.require("demo").unwrap().nodes.is_empty());
    }
}

#[tokio::test]
async fn nvme_provisioning_failure_keeps_join_for_down() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    cloud
        .launch_ids
        .borrow_mut()
        .extend(["i-test".into(), "i-second".into()]);
    cloud.observations.borrow_mut().extend([
        Some(instance("running")),
        Some(Machine {
            id: "i-second".into(),
            ..instance("running")
        }),
    ]);
    up::run(
        &cloud,
        &FakeHost::default(),
        &state,
        &settings(),
        up::NewNode {
            name: "demo",
            sandboxes: 64,
        },
        None.into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    let host = FakeHost {
        nvme_failure: true,
        ..Default::default()
    };
    let error = super::add_node::run(
        &cloud,
        &host,
        &state,
        super::add_node::NewNode {
            name: "demo",
            sandboxes: 4,
            shape: super::NodeShape {
                instance_type: Some("m6i.large".into()),
                disk_gb: Some(40),
            },
            local_storage: settings().local_storage.clone(),
        },
        Duration::ZERO,
        None,
    )
    .await
    .unwrap_err();
    assert!(format!("{error:#}").contains("local NVMe storage is required"));
    let saved = state.require("demo").unwrap();
    assert_eq!(saved.nodes.len(), 1);
    assert_eq!(saved.nodes[0].instance_id, "i-second");
    assert_eq!(
        saved.nodes[0]
            .launch_settings
            .as_ref()
            .unwrap()
            .aws
            .instance_type,
        "m6i.large"
    );
    cloud.observations.borrow_mut().extend([None, None]);
    down::run(&cloud, &state, &saved, Duration::ZERO, true)
        .await
        .unwrap();
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
        std::future::ready(Ok("swarmyd 0.1.0 (test)".into()))
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
            changed: vec!["swarmyd".into(), "swarmy-gateway".into()],
            restarted: if services_only {
                vec!["swarmy-gateway".into()]
            } else {
                vec!["swarmy-gateway".into(), "swarmyd".into()]
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
    assert_eq!(summaries[0].changed, ["swarmyd", "swarmy-gateway"]);
    assert!(
        summaries
            .iter()
            .all(|summary| !summary.restarted.contains(&"swarmyd".to_owned()))
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
        assert!(value["restarted"].is_array());
        assert!(value["elapsed_seconds"].is_number());
    }
}

#[tokio::test]
async fn bucket_profile_is_kept_when_key_deletion_is_denied() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    observe_running(&cloud);
    let settings = RemoteSettings {
        bucket: Some(swarmy_config::BucketSpec::aws("test-bucket")),
        ..settings()
    };
    up::run(
        &cloud,
        &FakeHost::default(),
        &state,
        &settings,
        up::NewNode {
            name: "demo",
            sandboxes: 0,
        },
        None.into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    let node = state.require("demo").unwrap();
    cloud.fail_delete.set(true);
    cloud.observations.borrow_mut().push_back(None);
    down::run(&cloud, &state, &node, Duration::ZERO, true)
        .await
        .unwrap();
    assert_eq!(&*cloud.terminated.borrow(), &["i-test"]);
    assert!(cloud.profile_present.get());
    assert_eq!(cloud.key_delete_attempts.borrow().len(), 1);
    assert!(state.read("demo").unwrap().is_none());
    assert!(!node.key_path.exists());
    assert_eq!(cloud.bucket_creates.borrow().len(), 1);
}

#[tokio::test]
async fn never_launched_record_is_removed_without_cloud_calls() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let node: RemoteNode = serde_json::from_value(serde_json::json!({
        "name": "demo", "region": "us-east-1", "instance_id": "", "launch_attempted": false,
        "public_ip": "", "private_ip": "", "key_path": state.directory.join("key"),
        "launch_settings": { "bucket": "test-bucket" }, "created_at": "now"
    }))
    .unwrap();
    state.save(&node).unwrap();
    let cloud = FakeCloud::default();
    down::run(&cloud, &state, &node, Duration::ZERO, true)
        .await
        .unwrap();
    assert!(state.read("demo").unwrap().is_none());
    assert!(cloud.find_tokens.borrow().is_empty());
    assert!(cloud.terminated.borrow().is_empty());
    assert!(cloud.deleted.borrow().is_empty());
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
    // an AWS node runs as `ubuntu`, so an absolute laptop-rendered path
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
async fn down_deletes_bucket_then_role_after_nodes_and_retries_absent_resources() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(&dir.path().join("remote")).unwrap();
    let cloud = FakeCloud::default();
    observe_running(&cloud);
    up::run(
        &cloud,
        &FakeHost::default(),
        &state,
        &RemoteSettings {
            bucket: Some(swarmy_config::BucketSpec::aws("test-bucket")),
            ..settings()
        },
        up::NewNode {
            name: "cleanup",
            sandboxes: 0,
        },
        None.into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    let node = state.require("cleanup").unwrap();
    cloud.observations.borrow_mut().push_back(None);
    down::run(&cloud, &state, &node, Duration::ZERO, false)
        .await
        .unwrap();
    assert_eq!(
        *cloud.teardown.borrow(),
        [
            "instance i-test",
            &format!("key {}", super::key_name(&node).unwrap()),
            "bucket test-bucket",
            "role swarmy-cleanup"
        ]
    );
    // A failed local state removal can leave the record after AWS cleanup.
    state.save(&node).unwrap();
    cloud.absent.set(true);
    cloud.observations.borrow_mut().push_back(None);
    down::run(&cloud, &state, &node, Duration::ZERO, false)
        .await
        .unwrap();
    assert!(state.read("cleanup").unwrap().is_none());
}

#[tokio::test]
async fn down_plan_reports_owned_resources_for_confirmation() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    observe_running(&cloud);
    up::run(
        &cloud,
        &FakeHost::default(),
        &state,
        &RemoteSettings {
            bucket: Some(swarmy_config::BucketSpec::aws("test-bucket")),
            ..settings()
        },
        up::NewNode {
            name: "cleanup",
            sandboxes: 0,
        },
        None.into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    let node = state.require("cleanup").unwrap();
    // The library reports what would be deleted; the CLI prints the wording
    // and prompts (including the --json and terminal refusals).
    let plan = down::plan(&cloud, &state, &node)
        .await
        .unwrap()
        .expect("owned bucket and role need confirmation");
    assert_eq!(plan.bucket.as_deref(), Some("test-bucket"));
    assert_eq!(plan.profile.as_deref(), Some("swarmy-cleanup"));
    assert_eq!(plan.role.as_deref(), Some("swarmy-cleanup"));
}

#[tokio::test]
async fn down_leaves_untagged_resources_and_removes_state() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    observe_running(&cloud);
    up::run(
        &cloud,
        &FakeHost::default(),
        &state,
        &RemoteSettings {
            bucket: Some(swarmy_config::BucketSpec::aws("test-bucket")),
            ..settings()
        },
        up::NewNode {
            name: "cleanup",
            sandboxes: 0,
        },
        None.into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    let node = state.require("cleanup").unwrap();
    cloud.untagged.set(true);
    cloud.observations.borrow_mut().push_back(None);
    down::run(&cloud, &state, &node, Duration::ZERO, false)
        .await
        .unwrap();
    assert!(state.read("cleanup").unwrap().is_none());
    assert!(
        !cloud
            .teardown
            .borrow()
            .iter()
            .any(|call| call.starts_with("bucket ") || call.starts_with("role "))
    );
}

#[tokio::test]
async fn down_keeps_mixed_ownership_iam_pairs() {
    for untagged_profile in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let state = State::open(dir.path()).unwrap();
        let cloud = FakeCloud::default();
        observe_running(&cloud);
        up::run(
            &cloud,
            &FakeHost::default(),
            &state,
            &RemoteSettings {
                bucket: Some(swarmy_config::BucketSpec::aws("test-bucket")),
                ..settings()
            },
            up::NewNode {
                name: "cleanup",
                sandboxes: 0,
            },
            None.into(),
            Duration::ZERO,
        )
        .await
        .unwrap();
        let node = state.require("cleanup").unwrap();
        cloud.untagged_profile.set(untagged_profile);
        cloud.untagged_role.set(!untagged_profile);
        // A missing bucket must not make a partially owned IAM pair deletable.
        cloud.absent_bucket.set(true);
        assert!(down::plan(&cloud, &state, &node).await.unwrap().is_none());
        cloud.observations.borrow_mut().push_back(None);
        down::run(&cloud, &state, &node, Duration::ZERO, false)
            .await
            .unwrap();
        assert!(
            !cloud
                .teardown
                .borrow()
                .iter()
                .any(|call| call.starts_with("role "))
        );
        assert!(state.read("cleanup").unwrap().is_none());
    }
}

#[tokio::test]
async fn down_refuses_bucket_shared_by_another_remote() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    observe_running(&cloud);
    up::run(
        &cloud,
        &FakeHost::default(),
        &state,
        &RemoteSettings {
            bucket: Some(swarmy_config::BucketSpec::aws("test-bucket")),
            ..settings()
        },
        up::NewNode {
            name: "cleanup",
            sandboxes: 0,
        },
        None.into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    let node = state.require("cleanup").unwrap();
    let mut other = node.clone();
    other.name = "other".into();
    state.save(&other).unwrap();
    cloud.observations.borrow_mut().push_back(None);
    down::run(&cloud, &state, &node, Duration::ZERO, false)
        .await
        .unwrap();
    assert!(
        !cloud
            .teardown
            .borrow()
            .iter()
            .any(|call| call == "bucket test-bucket")
    );
    assert!(state.read("other").unwrap().is_some());
}

#[tokio::test]
async fn tag_requires_exact_resource_names_and_calls_cloud_only_after_all_confirmations() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    observe_running(&cloud);
    up::run(
        &cloud,
        &FakeHost::default(),
        &state,
        &RemoteSettings {
            bucket: Some(swarmy_config::BucketSpec::aws("test-bucket")),
            ..settings()
        },
        up::NewNode {
            name: "cleanup",
            sandboxes: 0,
        },
        None.into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    let node = state.require("cleanup").unwrap();
    let mut prompted = Vec::new();
    tag_confirmed(&cloud, &state, &node, |kind, name| {
        prompted.push((kind.to_owned(), name.to_owned()));
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(
        prompted,
        [
            ("bucket".into(), "test-bucket".into()),
            ("role".into(), "swarmy-cleanup".into()),
            ("instance profile".into(), "swarmy-cleanup".into())
        ]
    );
    assert_eq!(
        *cloud.tagged.borrow(),
        ["bucket test-bucket", "role and profile swarmy-cleanup"]
    );
    assert!(
        tag_confirmed(&cloud, &state, &node, |_, _| {
            Err(crate::Error::other("no"))
        })
        .await
        .is_err()
    );
    assert_eq!(cloud.tagged.borrow().len(), 2);
}

#[tokio::test]
async fn down_ignores_tunnel_profile_and_keeps_shared_role() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    observe_running(&cloud);
    up::run(
        &cloud,
        &FakeHost::default(),
        &state,
        &RemoteSettings {
            bucket: Some(swarmy_config::BucketSpec::aws("test-bucket")),
            ..settings()
        },
        up::NewNode {
            name: "cleanup",
            sandboxes: 0,
        },
        None.into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    let node = state.require("cleanup").unwrap();
    std::fs::write(state.directory.join("cleanup.profile.json"), b"{}").unwrap();
    assert!(!state.bucket_shared("cleanup", "test-bucket").unwrap());
    let mut other = node.clone();
    other.name = "other".into();
    other.launch_settings.as_mut().unwrap().bucket =
        Some(swarmy_config::BucketSpec::aws("different-bucket"));
    // The other remote uses the same IAM override, but not the same bucket.
    other.launch_settings.as_mut().unwrap().aws.iam_role = Some("swarmy-cleanup".into());
    state.save(&other).unwrap();
    assert!(state.role_shared("cleanup", "swarmy-cleanup").unwrap());
    cloud.observations.borrow_mut().push_back(None);
    down::run(&cloud, &state, &node, Duration::ZERO, false)
        .await
        .unwrap();
    assert!(
        cloud
            .teardown
            .borrow()
            .iter()
            .any(|call| call == "bucket test-bucket")
    );
    assert!(
        !cloud
            .teardown
            .borrow()
            .iter()
            .any(|call| call == "role swarmy-cleanup")
    );
}

#[tokio::test]
async fn tag_refuses_cloud_resources_owned_by_another_remote() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    observe_running(&cloud);
    up::run(
        &cloud,
        &FakeHost::default(),
        &state,
        &RemoteSettings {
            bucket: Some(swarmy_config::BucketSpec::aws("test-bucket")),
            ..settings()
        },
        up::NewNode {
            name: "cleanup",
            sandboxes: 0,
        },
        None.into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    let node = state.require("cleanup").unwrap();
    cloud.foreign_bucket.set(true);
    assert!(
        tag_confirmed(&cloud, &state, &node, |_, _| Ok(()))
            .await
            .is_err()
    );
    assert!(cloud.tagged.borrow().is_empty());
    cloud.foreign_bucket.set(false);
    cloud.foreign_role.set(true);
    assert!(
        tag_confirmed(&cloud, &state, &node, |_, _| Ok(()))
            .await
            .is_err()
    );
    assert_eq!(*cloud.tagged.borrow(), ["bucket test-bucket"]);
}

#[tokio::test]
async fn denied_ownership_and_version_reads_retain_state_and_explain_permission() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    observe_running(&cloud);
    up::run(
        &cloud,
        &FakeHost::default(),
        &state,
        &RemoteSettings {
            bucket: Some(swarmy_config::BucketSpec::aws("test-bucket")),
            ..settings()
        },
        up::NewNode {
            name: "cleanup",
            sandboxes: 0,
        },
        None.into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    let node = state.require("cleanup").unwrap();
    cloud.deny_tag_read.set(true);
    let error = down::plan(&cloud, &state, &node).await.unwrap_err();
    assert!(swarmy_core::error_chain(&error).contains("s3:GetBucketTagging"));
    assert!(swarmy_core::error_chain(&error).contains("missing permission"));
    assert!(state.read("cleanup").unwrap().is_some());
    cloud.deny_tag_read.set(false);
    cloud.deny_version_list.set(true);
    cloud.observations.borrow_mut().push_back(None);
    let error = down::run(&cloud, &state, &node, Duration::ZERO, false)
        .await
        .unwrap_err();
    assert!(swarmy_core::error_chain(&error).contains("s3:ListBucketVersions"));
    assert!(state.read("cleanup").unwrap().is_some());
}

#[tokio::test]
async fn up_continues_when_creation_tags_are_denied_and_down_keeps_untagged_resources() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    cloud.deny_create_tags.set(true);
    observe_running(&cloud);
    up::run(
        &cloud,
        &FakeHost::default(),
        &state,
        &RemoteSettings {
            bucket: Some(swarmy_config::BucketSpec::aws("test-bucket")),
            ..settings()
        },
        up::NewNode {
            name: "cleanup",
            sandboxes: 0,
        },
        None.into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    let node = state.require("cleanup").unwrap();
    assert!(down::plan(&cloud, &state, &node).await.unwrap().is_none());
    cloud.observations.borrow_mut().push_back(None);
    down::run(&cloud, &state, &node, Duration::ZERO, false)
        .await
        .unwrap();
    assert!(state.read("cleanup").unwrap().is_none());
    assert!(
        !cloud
            .teardown
            .borrow()
            .iter()
            .any(|item| item.starts_with("bucket ") || item.starts_with("role "))
    );
}
