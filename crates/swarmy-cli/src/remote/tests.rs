use std::os::unix::fs::PermissionsExt;
use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    time::Duration,
};

use anyhow::Result;
use swarmy_config::{RemoteNode, RemoteSettings};

use super::{Cloud, Host, Instance, Launch, down, state::State, up, wait_running};

#[derive(Default)]
struct FakeCloud {
    requests: RefCell<Vec<Launch>>,
    launch_ids: RefCell<VecDeque<String>>,
    keys: RefCell<Vec<(String, Vec<u8>, String)>>,
    observations: RefCell<VecDeque<Option<Instance>>>,
    terminated: RefCell<Vec<String>>,
    deleted: RefCell<Vec<String>>,
    stock_reads: Cell<usize>,
    find_tokens: RefCell<Vec<String>>,
    fail_delete: Cell<bool>,
    fail_terminate: Cell<bool>,
}

impl Cloud for FakeCloud {
    fn stock_image(&self) -> impl Future<Output = Result<String>> {
        self.stock_reads.set(self.stock_reads.get() + 1);
        std::future::ready(Ok("ami-stock".into()))
    }
    fn import_key(
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
    fn launch(&self, request: &Launch) -> impl Future<Output = Result<String>> {
        self.requests.borrow_mut().push(request.clone());
        std::future::ready(Ok(self
            .launch_ids
            .borrow_mut()
            .pop_front()
            .unwrap_or_else(|| "i-test".into())))
    }
    fn instance(&self, _: &str) -> impl Future<Output = Result<Option<Instance>>> {
        std::future::ready(Ok(self
            .observations
            .borrow_mut()
            .pop_front()
            .expect("unexpected describe call")))
    }
    fn find_launch(&self, token: &str) -> impl Future<Output = Result<Option<String>>> {
        self.find_tokens.borrow_mut().push(token.into());
        std::future::ready(Ok(Some("i-recovered".into())))
    }
    fn terminate(&self, id: &str) -> impl Future<Output = Result<()>> {
        if self.fail_terminate.get() {
            return std::future::ready(Err(anyhow::anyhow!("access denied")));
        }
        self.terminated.borrow_mut().push(id.into());
        std::future::ready(Ok(()))
    }
    fn delete_key(&self, name: &str) -> impl Future<Output = Result<()>> {
        if self.fail_delete.get() {
            return std::future::ready(Err(anyhow::anyhow!("access denied")));
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
    images: RefCell<Vec<(String, String, std::path::PathBuf)>>,
    services: Cell<usize>,
    credentials: RefCell<Vec<std::path::PathBuf>>,
    keyrings: RefCell<Vec<std::path::PathBuf>>,
    primaries: RefCell<Vec<Option<RemoteNode>>>,
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
            Err(anyhow::anyhow!("image build failed"))
        } else {
            Ok(())
        })
    }

    async fn generate_key(&self, node: &RemoteNode) -> Result<Vec<u8>> {
        tokio::fs::write(&node.key_path, "private-test").await?;
        tokio::fs::write(node.key_path.with_extension("pub"), "ssh-ed25519 test").await?;
        Ok(b"ssh-ed25519 test".to_vec())
    }
    fn provision(
        &self,
        node: &RemoteNode,
        primary: Option<&RemoteNode>,
    ) -> impl Future<Output = Result<String>> {
        self.provisioned.borrow_mut().push(node.clone());
        self.primaries.borrow_mut().push(primary.cloned());
        if self.fail {
            return std::future::ready(Err(anyhow::anyhow!("SSH failed")));
        }
        std::future::ready(Ok(node.public_ip.clone()))
    }
}

fn instance(status: &str) -> Instance {
    Instance {
        id: "i-test".into(),
        status: status.into(),
        public_ip: "203.0.113.10".into(),
        private_ip: "10.0.0.10".into(),
    }
}

fn settings() -> RemoteSettings {
    RemoteSettings {
        subnet: Some("subnet-test".into()),
        security_group: Some("sg-test".into()),
        managed_by_tag: "codex-launcher".into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn up_waits_and_persists_connection_and_cleanup_contract() {
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
        "demo",
        Some(std::path::Path::new("images/base-ubuntu")).into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    let node = state.read("demo").unwrap().unwrap();
    assert_eq!(node.name, "demo");
    assert_eq!(node.default_image.as_deref(), Some("base-ubuntu:demo"));
    assert_eq!(
        *host.images.borrow(),
        [(
            "demo".into(),
            node.public_ip.clone(),
            "images/base-ubuntu".into()
        )]
    );
    assert_eq!(node.region, "us-east-1");
    assert_eq!(node.instance_id, "i-test");
    assert_eq!(node.public_ip, "203.0.113.10");
    assert_eq!(node.private_ip, "10.0.0.10");
    assert_eq!(node.ssh_user, "ubuntu");
    assert_eq!(
        (node.ports.fdb, node.ports.nats, node.ports.s3),
        (4500, 4222, 8333)
    );
    assert!(node.nodes.is_empty());
    assert!(node.created_at.parse::<jiff::Timestamp>().is_ok());
    assert!(node.key_path.is_file());
    assert_eq!(
        std::fs::metadata(state.directory.join("demo.json"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(
        std::fs::metadata(&state.directory)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    let request = cloud.requests.borrow()[0].clone();
    assert_eq!(request.image, "ami-stock");
    assert_eq!(request.settings.disk_gb, 100);
    assert_eq!(request.settings.instance_type, "m6id.xlarge");
    assert_eq!(request.name, "demo");
    assert_eq!(request.settings.subnet.as_deref(), Some("subnet-test"));
    assert_eq!(request.settings.security_group.as_deref(), Some("sg-test"));
    assert_eq!(request.settings.managed_by_tag, "codex-launcher");
    assert_eq!(request.key_name, super::key_name(&node).unwrap());
    assert!(request.key_name.len() <= 64);
    assert_eq!(
        cloud.keys.borrow()[0],
        (
            request.key_name.clone(),
            b"ssh-ed25519 test".to_vec(),
            "codex-launcher".into()
        )
    );
    assert_eq!(host.provisioned.borrow()[0].public_ip, node.public_ip);
    assert!(cloud.observations.borrow().is_empty());
    assert!(
        up::run(
            &cloud,
            &host,
            &state,
            &settings(),
            "demo",
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
        image: Some("ami-custom".into()),
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
            "demo",
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
    down::run(&cloud, &state, &node, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(*cloud.terminated.borrow(), ["i-test"]);
    assert_eq!(*cloud.deleted.borrow(), [super::key_name(&node).unwrap()]);
    assert!(state.read("demo").unwrap().is_none());
    assert!(!node.key_path.exists());
    assert!(!node.key_path.with_extension("pub").exists());
}

#[tokio::test]
async fn down_missing_instance_and_retry_after_key_failure() {
    let dir = tempfile::tempdir().unwrap();
    let state = State::open(dir.path()).unwrap();
    let cloud = FakeCloud::default();
    cloud
        .observations
        .borrow_mut()
        .extend([Some(instance("running")), None, None]);
    up::run(
        &cloud,
        &FakeHost::default(),
        &state,
        &settings(),
        "demo",
        Some(std::path::Path::new("images/base-ubuntu")).into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    let node = state.read("demo").unwrap().unwrap();
    cloud.fail_delete.set(true);
    assert!(
        down::run(&cloud, &state, &node, Duration::ZERO)
            .await
            .is_err()
    );
    assert!(state.read("demo").unwrap().is_some());
    cloud.fail_delete.set(false);
    down::run(&cloud, &state, &node, Duration::ZERO)
        .await
        .unwrap();
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
        "demo",
        Some(std::path::Path::new("images/base-ubuntu")).into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    let mut node = state.read("demo").unwrap().unwrap();
    node.instance_id.clear();
    state.save(&node).unwrap();
    down::run(&cloud, &state, &node, Duration::ZERO)
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
        "demo",
        Some(std::path::Path::new("images/base-ubuntu")).into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    let node = state.read("demo").unwrap().unwrap();
    cloud.fail_terminate.set(true);
    assert!(
        down::run(&cloud, &state, &node, Duration::ZERO)
            .await
            .is_err()
    );
    assert!(node.key_path.is_file());
    assert!(state.read("demo").unwrap().is_some());
    assert!(cloud.deleted.borrow().is_empty());
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
        Some(Instance {
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
        "demo",
        Some(std::path::Path::new("images/base-ubuntu")).into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    super::add_node::run(&cloud, &host, &state, "demo", Duration::ZERO, None)
        .await
        .unwrap();
    let node = state.require("demo").unwrap();
    assert_eq!(node.nodes.len(), 1);
    let child = &node.nodes[0];
    assert_eq!(child.name, "demo-2");
    assert_ne!(child.key_path, node.key_path);
    assert_eq!(cloud.stock_reads.get(), 1);
    {
        let requests = cloud.requests.borrow();
        let join = &requests[1];
        assert_eq!(join.image, requests[0].image);
        assert_eq!(join.settings.region, node.region);
        assert_eq!(join.settings.subnet, settings().subnet);
        assert_eq!(join.settings.security_group, settings().security_group);
        assert_eq!(join.settings.instance_type, settings().instance_type);
        assert_eq!(join.settings.disk_gb, settings().disk_gb);
        assert_eq!(join.settings.managed_by_tag, "codex-launcher");
        assert_eq!(join.key_name, super::key_name(child).unwrap());
        assert_eq!(join.name, child.name);
    }
    assert_eq!(host.images.borrow().len(), 1);
    assert!(host.primaries.borrow()[0].is_none());
    assert_eq!(
        host.primaries.borrow()[1].as_ref().unwrap().private_ip,
        node.private_ip
    );
    cloud.observations.borrow_mut().extend([None, None]);
    down::run(&cloud, &state, &node, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(*cloud.terminated.borrow(), ["i-second", "i-test"]);
    assert_eq!(cloud.deleted.borrow().len(), 2);
    assert!(!child.key_path.exists());
    assert!(!node.key_path.exists());
    assert!(state.read("demo").unwrap().is_none());
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
        "demo",
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
        super::add_node::run(&cloud, &host, &state, "demo", Duration::ZERO, None)
            .await
            .is_err()
    );
    let node = state.require("demo").unwrap();
    assert_eq!(node.nodes.len(), 1);
    assert!(!node.nodes[0].instance_id.is_empty());
    cloud.fail_terminate.set(true);
    assert!(
        down::run(&cloud, &state, &node, Duration::ZERO)
            .await
            .is_err()
    );
    assert!(node.nodes[0].key_path.exists());
    assert!(node.key_path.exists());
    cloud.fail_terminate.set(false);
    cloud.observations.borrow_mut().extend([None, None]);
    down::run(&cloud, &state, &node, Duration::ZERO)
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
            "demo",
            recipe.into(),
            Duration::ZERO,
        )
        .await;
        assert_eq!(result.is_err(), fail_image);
        let node = state.require("demo").unwrap();
        assert_eq!(node.default_image.as_deref(), expected);
        let profile =
            super::connect::new_profile(dir.path(), &node, node.ports, dir.path().join("socket"))
                .unwrap();
        let profile: swarmy_config::RemoteProfile =
            serde_json::from_slice(&serde_json::to_vec(&profile).unwrap()).unwrap();
        assert_eq!(profile.default_image.as_deref(), expected);
        let mut settings = swarmy_config::Settings::default();
        profile.apply(&mut settings);
        assert_eq!(settings.default_image.as_deref(), expected);
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
            credential_file: auth.to_string_lossy().into_owned(),
            fake: swarmy_config::Fake {
                script: script.to_string_lossy().into_owned(),
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
            "demo",
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
        chatgpt.provider = "chatgpt".into();
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
        "demo",
        None.into(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    super::add_node::run(&cloud, &host, &state, "demo", Duration::ZERO, None)
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
        remote: swarmy_config::RemoteSettings {
            services: swarmy_config::RemoteServices::Node,
            ..settings()
        },
        credential_file: auth.to_string_lossy().into_owned(),
        ..Default::default()
    };
    let options =
        super::services::Options::with_keyring(&settings, true, None, Some(keyring.clone()))
            .unwrap();
    super::add_node::run(
        &cloud,
        &host,
        &state,
        "demo",
        Duration::ZERO,
        Some(&options),
    )
    .await
    .unwrap();
    assert_eq!(host.services.get(), 1);
    assert_eq!(*host.credentials.borrow(), vec![auth]);
    assert_eq!(*host.keyrings.borrow(), vec![keyring]);
}
