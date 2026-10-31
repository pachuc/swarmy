#![deny(clippy::disallowed_methods)]
use super::*;
use std::future::Future;
use std::os::unix::fs::PermissionsExt as _;
use std::sync::Mutex;
use swarmy_config::swarm::Module;
pub(super) struct FakeHost {
    calls: Mutex<Vec<String>>,
    configs: Mutex<Vec<String>>,
    fail_provision: Mutex<Option<String>>,
    pub(super) fail_decommission: Mutex<Option<String>>,
    pub(super) fail_start: Mutex<Option<String>>,
    pub(super) drain_results:
        Mutex<std::collections::HashMap<String, std::result::Result<ssh::Drain, String>>>,
}
impl FakeHost {
    pub(super) fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            configs: Mutex::new(Vec::new()),
            fail_provision: Mutex::new(None),
            fail_decommission: Mutex::new(None),
            fail_start: Mutex::new(None),
            drain_results: Mutex::new(std::collections::HashMap::new()),
        }
    }
    fn configs(&self) -> Vec<String> {
        self.configs.lock().unwrap().clone()
    }
    pub(super) fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
    fn push(&self, message: String) {
        self.calls.lock().unwrap().push(message);
    }
}
impl SwarmHost for FakeHost {
    fn wait_ready(&self, node: &Machine) -> impl Future<Output = Result<()>> {
        std::future::ready({
            self.push(format!("ready {}", node.name));
            Ok(())
        })
    }
    fn adopt_key(&self, node: &Machine, source: &Path) -> impl Future<Output = Result<()>> {
        std::future::ready((|| {
            self.push(format!("adopt {}", node.name));
            swarmy_config::write_private(&node.key_path, &fs::read(source)?)?;
            Ok(())
        })())
    }
    fn provision(
        &self,
        node: &Machine,
        primary: Option<&Machine>,
    ) -> impl Future<Output = Result<String>> {
        std::future::ready((|| {
            self.push(format!(
                "provision {} primary={} bucket={}",
                node.name,
                primary.map_or("none", |p| &p.name),
                node.bucket.as_ref().map_or("none", |b| b.bucket.as_str())
            ));
            if self.fail_provision.lock().unwrap().as_deref() == Some(&node.name) {
                return Err(Error::other("provision failed"));
            }
            Ok(node.public_ip.clone())
        })())
    }
    fn services(
        &self,
        node: &Machine,
        _: &str,
        options: &services::Options,
    ) -> impl Future<Output = Result<()>> {
        std::future::ready({
            self.configs
                .lock()
                .unwrap()
                .push(options.config_toml().to_owned());
            self.push(format!("services {}", node.name));
            Ok(())
        })
    }
    fn api_token(&self, node: &Machine, _: &str) -> impl Future<Output = Result<String>> {
        std::future::ready({
            self.push(format!("token {}", node.name));
            Ok("node-token".into())
        })
    }
    fn build_image(
        &self,
        node: &Machine,
        _: &str,
        _: &Path,
        tag: &str,
    ) -> impl Future<Output = Result<()>> {
        std::future::ready({
            self.push(format!("image {} {tag}", node.name));
            Ok(())
        })
    }
    fn decommission(&self, node: &Machine) -> impl Future<Output = Result<()>> {
        std::future::ready((|| {
            self.push(format!("decommission {}", node.name));
            if self.fail_decommission.lock().unwrap().as_deref() == Some(&node.name) {
                return Err(Error::other("decommission failed"));
            }
            Ok(())
        })())
    }
    fn drain(
        &self,
        node: &Machine,
        timeout: Duration,
        force: bool,
    ) -> impl Future<Output = Result<ssh::Drain>> {
        std::future::ready({
            self.push(format!(
                "drain {} timeout={} force={force}",
                node.name,
                timeout.as_secs()
            ));
            self.drain_results
                .lock()
                .unwrap()
                .get(&node.name)
                .cloned()
                .unwrap_or(Ok(ssh::Drain {
                    stopped: true,
                    ..ssh::Drain::default()
                }))
                .map_err(Error::other)
        })
    }
    fn revoke_tunnel(&self, node: &Machine, primary: &Machine) -> impl Future<Output = Result<()>> {
        std::future::ready({
            self.push(format!("revoke {} primary={}", node.name, primary.name));
            Ok(())
        })
    }
    fn stop(&self, node: &Machine) -> impl Future<Output = Result<()>> {
        std::future::ready({
            self.push(format!("stop {}", node.name));
            Ok(())
        })
    }
    fn start(&self, node: &Machine) -> impl Future<Output = Result<()>> {
        std::future::ready({
            self.push(format!("start {}", node.name));
            if self.fail_start.lock().unwrap().as_deref() == Some(&node.name) {
                Err(Error::other("start failed"))
            } else {
                Ok(())
            }
        })
    }
    fn reachable(&self, _: &Machine) -> impl Future<Output = bool> {
        std::future::ready(true)
    }
}
fn fixture() -> (tempfile::TempDir, Registry, SwarmSpec) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let dirs = swarmy_config::paths::Dirs {
        config: root.join("config"),
        data: root.join("data"),
        state: root.join("state"),
        cache: root.join("cache"),
    };
    let registry = Registry::new(dirs);
    let key = root.join("bootstrap");
    fs::write(&key, "test-key").unwrap();
    let spec = SwarmSpec::parse(&format!(
        r#"
name = "fleet"
lifecycle = "persistent"
[groups.control]
module = "existing"
hosts = [{{ address = "203.0.113.10", ssh_user = "root", ssh_key = "{}" }}]
[groups.workers]
module = "existing"
local_storage = "dir:/srv/swarmy-local"
hosts = [
{{ address = "203.0.113.11", ssh_user = "root", ssh_key = "{}" }},
{{ address = "203.0.113.12", ssh_user = "root", ssh_key = "{}" }}]
[placement]
stack = "control"
services = "control"
nodes = ["workers"]
"#,
        key.display(),
        key.display(),
        key.display()
    ))
    .unwrap();
    registry.write_spec(&spec).unwrap();
    (temp, registry, spec)
}
fn source_list(registry: &Registry, spec: &SwarmSpec) -> Vec<HostSource> {
    existing_sources(spec, &registry.swarm_dir(&spec.name).unwrap()).unwrap()
}
fn request<'a>(
    registry: &'a Registry,
    spec: &'a SwarmSpec,
    settings: &'a Settings,
    recipe: Option<&'a Path>,
) -> UpRequest<'a> {
    UpRequest {
        registry,
        spec,
        sources: source_list(registry, spec),
        settings,
        copy_credential: false,
        recipe,
        cache: Path::new("/tmp/cache"),
        bucket_keys: None,
        machines: None,
    }
}
fn settings() -> Settings {
    let mut settings = Settings::default();
    settings.selection.provider = "fake".into();
    settings
}
#[test]
fn one_box_plan_runs_stack_services_and_sandboxes() {
    let (_tmp, registry, mut spec) = fixture();
    let workers = spec.groups.remove("workers").unwrap();
    let mut box_group = spec.groups.remove("control").unwrap();
    box_group.local_storage = workers.local_storage;
    spec.groups.insert("box".into(), box_group);
    spec.placement.stack = "box".into();
    spec.placement.services = "box".into();
    spec.placement.nodes = vec!["box".into()];
    let plan = plan(&spec, source_list(&registry, &spec)).unwrap();
    assert_eq!(plan.len(), 1);
    assert_eq!(plan[0].host.name, "fleet-box-1");
    assert_eq!(plan[0].role, Role::Stack { services: true });
    assert_eq!(plan[0].sandboxes, 64);
}
#[test]
fn split_plan_orders_primary_then_nodes() {
    let (_tmp, registry, spec) = fixture();
    let plan = plan(&spec, source_list(&registry, &spec)).unwrap();
    assert_eq!(
        plan.iter()
            .map(|p| p.host.name.as_str())
            .collect::<Vec<_>>(),
        ["fleet-control-1", "fleet-workers-1", "fleet-workers-2"]
    );
    assert_eq!(
        plan.iter().map(|p| p.sandboxes).collect::<Vec<_>>(),
        [0, 64, 64]
    );
}
#[test]
fn plan_refuses_unsupported_layouts_and_bad_hosts() {
    let (_tmp, registry, mut spec) = fixture();
    spec.placement.services = "workers".into();
    assert!(
        plan(&spec, source_list(&registry, &spec))
            .unwrap_err()
            .to_string()
            .contains("separate group")
    );
    spec.placement.services = "control".into();
    let existing = source_list(&registry, &spec);
    spec.groups.insert(
        "local".into(),
        swarmy_config::swarm::GroupSpec {
            module: Module::Local,
            ..spec.groups["control"].clone()
        },
    );
    spec.placement.services = "local".into();
    assert!(
        plan(&spec, existing)
            .unwrap_err()
            .to_string()
            .contains("services on this machine")
    );
    spec.placement.services = "control".into();
    spec.groups.remove("local");
    let mut sources = source_list(&registry, &spec);
    sources[0].host.public_address = "invalid".into();
    assert!(
        plan(&spec, sources)
            .unwrap_err()
            .to_string()
            .contains("IPv4 address")
    );
    let mut sources = source_list(&registry, &spec);
    sources[0].host.ssh_user = "bad user".into();
    assert!(
        plan(&spec, sources)
            .unwrap_err()
            .to_string()
            .contains("ssh_user")
    );
    let mut sources = source_list(&registry, &spec);
    sources[0].copy_key_from = Some(PathBuf::from("/definitely/missing/swarmy-key"));
    assert!(
        plan(&spec, sources)
            .unwrap_err()
            .to_string()
            .contains("does not exist")
    );
    let mut sources = source_list(&registry, &spec);
    sources[0].copy_key_from = Some(PathBuf::from("relative"));
    assert!(
        plan(&spec, sources)
            .unwrap_err()
            .to_string()
            .contains("absolute path")
    );
    spec.groups.get_mut("workers").unwrap().local_storage = None;
    assert!(
        plan(&spec, source_list(&registry, &spec))
            .unwrap_err()
            .to_string()
            .contains("need local_storage")
    );
}
#[test]
fn existing_sources_skips_module_groups() {
    let (_tmp, registry, mut spec) = fixture();
    spec.groups.get_mut("workers").unwrap().module = Module::HetznerCloud;
    assert!(
        existing_sources(&spec, &registry.swarm_dir("fleet").unwrap())
            .unwrap()
            .iter()
            .all(|source| source.group != "workers")
    );
}
#[tokio::test]
async fn up_provisions_in_order_and_records_the_token() {
    let (_tmp, registry, spec) = fixture();
    let fake = FakeHost::new();
    let recipe = Path::new("images/base-ubuntu");
    provision_swarm_with(&fake, request(&registry, &spec, &settings(), Some(recipe)))
        .await
        .unwrap();
    assert_eq!(
        fake.calls(),
        [
            "adopt fleet-control-1",
            "provision fleet-control-1 primary=none bucket=none",
            "services fleet-control-1",
            "token fleet-control-1",
            "image fleet-control-1 fleet",
            "adopt fleet-workers-1",
            "provision fleet-workers-1 primary=fleet-control-1 bucket=none",
            "adopt fleet-workers-2",
            "provision fleet-workers-2 primary=fleet-control-1 bucket=none"
        ]
    );
    let record = registry.read_record("fleet").unwrap().unwrap();
    assert!(record.hosts.values().flatten().all(|h| h.provisioned));
    assert_eq!(record.api.token.as_deref(), Some("node-token"));
    assert_eq!(record.api.url, None);
    for host in record.hosts.values().flatten() {
        assert_eq!(
            fs::metadata(&host.ssh_key).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    assert_eq!(registry.current().unwrap().as_deref(), Some("fleet"));
}
#[tokio::test]
async fn up_rerun_skips_provisioned_hosts_and_node_add_provisions_only_the_new_one() {
    let (_tmp, registry, mut spec) = fixture();
    let fake = FakeHost::new();
    provision_swarm_with(&fake, request(&registry, &spec, &settings(), None))
        .await
        .unwrap();
    let old = fake.calls().len();
    let reports = provision_swarm_with(&fake, request(&registry, &spec, &settings(), None))
        .await
        .unwrap();
    assert!(reports.iter().all(|r| r.provisioned && !r.changed));
    assert_eq!(fake.calls().len(), old);
    let new = spec.groups["workers"].hosts[0].clone();
    spec.groups
        .get_mut("workers")
        .unwrap()
        .hosts
        .push(swarmy_config::swarm::HostSpec {
            address: "203.0.113.13".into(),
            ..new
        });
    registry.write_spec(&spec).unwrap();
    provision_swarm_with(&fake, request(&registry, &spec, &settings(), None))
        .await
        .unwrap();
    assert_eq!(
        &fake.calls()[old..],
        [
            "adopt fleet-workers-3",
            "provision fleet-workers-3 primary=fleet-control-1 bucket=none"
        ]
    );
}
#[tokio::test]
async fn recorded_hosts_are_matched_by_name_after_reordering() {
    let (_tmp, registry, spec) = fixture();
    let fake = FakeHost::new();
    provision_swarm_with(&fake, request(&registry, &spec, &settings(), None))
        .await
        .unwrap();
    let mut record = registry.read_record("fleet").unwrap().unwrap();
    record.hosts.get_mut("workers").unwrap().reverse();
    registry.write_record(&record).unwrap();
    let old = fake.calls().len();
    let reports = provision_swarm_with(&fake, request(&registry, &spec, &settings(), None))
        .await
        .unwrap();
    assert_eq!(fake.calls().len(), old);
    assert!(reports.iter().all(|r| r.provisioned && !r.changed));
}
#[tokio::test]
async fn failed_host_stays_unprovisioned_and_resumes() {
    let (_tmp, registry, spec) = fixture();
    let fake = FakeHost::new();
    *fake.fail_provision.lock().unwrap() = Some("fleet-workers-2".into());
    assert!(
        provision_swarm_with(&fake, request(&registry, &spec, &settings(), None))
            .await
            .is_err()
    );
    assert!(!registry.read_record("fleet").unwrap().unwrap().hosts["workers"][1].provisioned);
    *fake.fail_provision.lock().unwrap() = None;
    let old = fake.calls().len();
    provision_swarm_with(&fake, request(&registry, &spec, &settings(), None))
        .await
        .unwrap();
    assert_eq!(
        &fake.calls()[old..],
        [
            "adopt fleet-workers-2",
            "provision fleet-workers-2 primary=fleet-control-1 bucket=none"
        ]
    );
}
#[tokio::test]
async fn decommission_goes_nodes_first_and_keeps_failed_hosts() {
    let (_tmp, registry, spec) = fixture();
    let fake = FakeHost::new();
    provision_swarm_with(&fake, request(&registry, &spec, &settings(), None))
        .await
        .unwrap();
    *fake.fail_decommission.lock().unwrap() = Some("fleet-workers-1".into());
    assert!(
        decommission_swarm_with(&fake, &registry, &spec, None)
            .await
            .is_err()
    );
    let calls = fake.calls();
    assert_eq!(
        &calls[calls.len() - 3..],
        [
            "decommission fleet-workers-2",
            "decommission fleet-workers-1",
            "decommission fleet-control-1"
        ]
    );
    let record = registry.read_record("fleet").unwrap().unwrap();
    assert_eq!(record.hosts["workers"].len(), 1);
    assert_eq!(record.hosts["workers"][0].name, "fleet-workers-1");
    *fake.fail_decommission.lock().unwrap() = None;
    decommission_swarm_with(&fake, &registry, &spec, None)
        .await
        .unwrap();
    assert!(!registry.swarm_dir("fleet").unwrap().exists());
    assert!(registry.spec_path("fleet").unwrap().exists());
}
#[tokio::test]
async fn stop_and_start_order() {
    let (_tmp, registry, spec) = fixture();
    let fake = FakeHost::new();
    provision_swarm_with(&fake, request(&registry, &spec, &settings(), None))
        .await
        .unwrap();
    let old = fake.calls().len();
    stop_swarm_with(&fake, &registry, &spec).await.unwrap();
    start_swarm_with(&fake, &registry, &spec).await.unwrap();
    assert_eq!(
        &fake.calls()[old..],
        [
            "stop fleet-workers-2",
            "stop fleet-workers-1",
            "stop fleet-control-1",
            "start fleet-control-1",
            "start fleet-workers-1",
            "start fleet-workers-2"
        ]
    );
}
#[tokio::test]
async fn down_removes_hosts_missing_from_spec() {
    let (_tmp, registry, mut spec) = fixture();
    let fake = FakeHost::new();
    provision_swarm_with(&fake, request(&registry, &spec, &settings(), None))
        .await
        .unwrap();
    spec.groups.get_mut("workers").unwrap().hosts.pop();
    registry.write_spec(&spec).unwrap();
    let before = fake.calls().len();
    decommission_swarm_with(&fake, &registry, &spec, None)
        .await
        .unwrap();
    assert_eq!(
        &fake.calls()[before..],
        [
            "decommission fleet-workers-2",
            "decommission fleet-workers-1",
            "decommission fleet-control-1"
        ]
    );
}
#[tokio::test]
async fn broken_spec_does_not_block_recorded_host_cleanup() {
    let (_tmp, registry, mut spec) = fixture();
    let fake = FakeHost::new();
    provision_swarm_with(&fake, request(&registry, &spec, &settings(), None))
        .await
        .unwrap();
    spec.groups.get_mut("workers").unwrap().hosts[0].address = "invalid".into();
    registry.write_spec(&spec).unwrap();
    assert!(plan(&spec, source_list(&registry, &spec)).is_err());
    let before = fake.calls().len();
    stop_swarm_with(&fake, &registry, &spec).await.unwrap();
    start_swarm_with(&fake, &registry, &spec).await.unwrap();
    let status = status_with(&fake, &registry, &spec).await.unwrap();
    assert_eq!(status.hosts.len(), 3);
    decommission_swarm_with(&fake, &registry, &spec, None)
        .await
        .unwrap();
    assert!(fake.calls()[before..].contains(&"decommission fleet-workers-2".to_owned()));
    assert!(!registry.swarm_dir("fleet").unwrap().exists());
}
#[test]
fn stack_primary_is_first_in_group_not_named_by_index() {
    let (_tmp, registry, spec) = fixture();
    let mut sources = source_list(&registry, &spec);
    sources[0].host.name = "arbitrary-machine".into();
    let planned = plan(&spec, sources).unwrap();
    assert_eq!(planned[0].role, Role::Stack { services: true });
}
#[test]
fn stack_group_extra_hosts_need_node_placement() {
    let (_tmp, registry, mut spec) = fixture();
    let second = spec.groups["control"].hosts[0].clone();
    spec.groups.get_mut("control").unwrap().hosts.push(second);
    assert!(
        plan(&spec, source_list(&registry, &spec))
            .unwrap_err()
            .to_string()
            .contains("other hosts must run nodes")
    );
}
#[test]
fn machine_maps_the_recorded_host() {
    let (_tmp, registry, spec) = fixture();
    let p = &plan(&spec, source_list(&registry, &spec)).unwrap()[0];
    let node = machine(&p.host, "swarmy", p.role, p.sandboxes, &p.local_storage);
    assert_eq!(node.private_ip, p.host.public_address);
    assert_eq!(node.key_path, p.host.ssh_key);
    assert!(node.services);
}
#[test]
fn lock_is_exclusive() {
    let (_tmp, registry, _spec) = fixture();
    let guard = lock(&registry, "fleet").unwrap();
    assert!(lock(&registry, "fleet").is_err());
    FileExt::unlock(&guard).unwrap();
    drop(guard);
    let _again = lock(&registry, "fleet").unwrap();
}
#[test]
fn reconcile_refuses_removed_or_moved_hosts() {
    let (_tmp, registry, spec) = fixture();
    let planned = plan(&spec, source_list(&registry, &spec)).unwrap();
    let mut record = SwarmRecord {
        bucket_keys: None,
        name: "fleet".into(),
        created_at: jiff::Timestamp::now(),
        api: SwarmApi::default(),
        hosts: BTreeMap::from([("workers".into(), vec![planned[2].host.clone()])]),
        tofu_state: None,
        tofu_bucket: None,
        tofu_bucket_endpoint: None,
        tofu_bucket_region: None,
        tofu_networks: BTreeMap::new(),
    };
    assert!(
        reconcile(&planned[..2], &record)
            .unwrap_err()
            .to_string()
            .contains("no longer in the spec")
    );
    record.hosts.get_mut("workers").unwrap()[0].public_address = "203.0.113.99".into();
    assert!(
        reconcile(&planned, &record)
            .unwrap_err()
            .to_string()
            .contains("changed in the spec")
    );
}
#[tokio::test]
async fn copy_credential_is_checked_before_any_host_change() {
    let (_tmp, registry, spec) = fixture();
    let fake = FakeHost::new();
    let mut settings = Settings::default();
    settings.selection.provider = "chatgpt".into();
    assert!(
        provision_swarm_with(&fake, request(&registry, &spec, &settings, None))
            .await
            .is_err()
    );
    assert!(fake.calls().is_empty());
    assert!(registry.read_record("fleet").unwrap().is_none());
}

#[cfg(not(feature = "remote"))]
#[tokio::test]
async fn bucket_swarm_needs_the_s3_client() {
    let (_tmp, registry, mut spec) = fixture();
    spec.bucket = Some(swarmy_config::swarm::SwarmBucket {
        name: "test-bucket".into(),
        endpoint: "https://objects.example.invalid".into(),
        region: "eu-west-1".into(),
        prefix: swarmy_config::ObjectPrefix::default(),
        conditional_create: true,
    });
    let host = FakeHost::new();
    let error = provision_swarm_with(&host, request(&registry, &spec, &settings(), None))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("built without the S3 client"));
    assert!(host.calls().is_empty());
    assert!(registry.read_record(&spec.name).unwrap().is_none());
    let error = decommission_swarm_with(&host, &registry, &spec, None)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("built without the S3 client"));
}

#[cfg(feature = "remote")]
mod bucket_lifecycle {
    use super::*;
    use crate::{BucketRemoval, Cloud, ObjectBucket, Ownership};
    use std::sync::atomic::{AtomicUsize, Ordering};

    static OUTPUT_LINES: Mutex<Vec<String>> = Mutex::new(Vec::new());
    fn capture_output(message: &str, _stderr: bool) {
        OUTPUT_LINES.lock().unwrap().push(message.to_owned());
    }

    struct FakeCloud {
        ensure: AtomicUsize,
        delete: AtomicUsize,
        ownership: Ownership,
        reject: bool,
    }
    impl Cloud for FakeCloud {
        fn verify_bucket_access(&self, _: &ObjectBucket) -> impl Future<Output = Result<()>> {
            std::future::ready(if self.reject {
                Err(Error::other("rejected"))
            } else {
                Ok(())
            })
        }
        fn ensure_bucket(&self, _: &ObjectBucket) -> impl Future<Output = Result<()>> {
            self.ensure.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Ok(()))
        }
        fn bucket_ownership(&self, _: &ObjectBucket) -> impl Future<Output = Result<Ownership>> {
            std::future::ready(Ok(self.ownership))
        }
        fn delete_bucket(&self, _: &ObjectBucket) -> impl Future<Output = Result<BucketRemoval>> {
            self.delete.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Ok(BucketRemoval::Removed))
        }
    }
    fn fixture_bucket() -> (tempfile::TempDir, Registry, SwarmSpec, SwarmRecord) {
        let (dir, registry, mut spec) = fixture();
        spec.bucket = Some(swarmy_config::swarm::SwarmBucket {
            name: "test-bucket".into(),
            endpoint: "https://objects.example.invalid".into(),
            region: "eu-west-1".into(),
            prefix: swarmy_config::ObjectPrefix::default(),
            conditional_create: true,
        });
        let record = SwarmRecord {
            name: spec.name.clone(),
            created_at: jiff::Timestamp::now(),
            api: SwarmApi::default(),
            hosts: BTreeMap::new(),
            tofu_state: None,
            tofu_bucket: None,
            tofu_bucket_endpoint: None,
            tofu_bucket_region: None,
            tofu_networks: BTreeMap::new(),
            bucket_keys: Some(StaticKeys {
                access_key: "access-secret".into(),
                secret_key: "value-secret".into(),
            }),
        };
        (dir, registry, spec, record)
    }
    #[tokio::test]
    async fn bucket_keys_are_verified_before_anything_is_recorded() {
        let (_dir, registry, spec, saved) = fixture_bucket();
        let cloud = FakeCloud {
            ensure: AtomicUsize::new(0),
            delete: AtomicUsize::new(0),
            ownership: Ownership::Owned,
            reject: true,
        };
        let host = FakeHost::new();
        let base = settings();
        let mut up = request(&registry, &spec, &base, None);
        up.bucket_keys = saved.bucket_keys.clone();
        let err = provision_swarm_with_backend(&host, &cloud, up)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("static bucket keys"));
        assert!(!format!("{err:?}").contains("value-secret"));
        assert_eq!(cloud.ensure.load(Ordering::Relaxed), 0);
        assert!(host.calls().is_empty());
        assert!(registry.read_record(&spec.name).unwrap().is_none());
    }
    #[tokio::test]
    async fn bucket_reaches_provisioning_with_static_keys() {
        let (_dir, registry, spec, record) = fixture_bucket();
        let cloud = FakeCloud {
            ensure: AtomicUsize::new(0),
            delete: AtomicUsize::new(0),
            ownership: Ownership::Owned,
            reject: false,
        };
        verify_bucket_with(&cloud, &spec, &record).await.unwrap();
        ensure_bucket_with(&cloud, &spec, &record).await.unwrap();
        assert_eq!(cloud.ensure.load(Ordering::Relaxed), 1);
        let bucket = bucket_spec(&spec, &record).unwrap();
        assert!(bucket.needs_static_keys());
        let planned = plan(&spec, source_list(&registry, &spec)).unwrap();
        let primary = node(
            &planned[0],
            user(&spec),
            bucket_spec(&spec, &record).as_ref(),
        );
        assert!(primary.bucket.unwrap().needs_static_keys());
    }
    #[tokio::test]
    async fn down_deletes_only_an_owned_bucket_scope() {
        let (_dir, _registry, spec, record) = fixture_bucket();
        crate::set_output_sink(capture_output);
        for (ownership, expected) in [(Ownership::Owned, 1), (Ownership::Unmanaged, 0)] {
            OUTPUT_LINES.lock().unwrap().clear();
            let cloud = FakeCloud {
                ensure: AtomicUsize::new(0),
                delete: AtomicUsize::new(0),
                ownership,
                reject: false,
            };
            delete_bucket_with(&cloud, &spec, &record).await.unwrap();
            assert_eq!(cloud.delete.load(Ordering::Relaxed), expected);
            let output = OUTPUT_LINES.lock().unwrap().join("\n");
            if ownership == Ownership::Unmanaged {
                assert!(output.contains("left in place"));
            } else {
                assert!(output.contains("Removed"));
            }
        }
    }
    #[tokio::test]
    async fn provisioned_hosts_reject_bucket_changes_and_key_rotation() {
        let (_dir, registry, spec, saved) = fixture_bucket();
        let cloud = FakeCloud {
            ensure: AtomicUsize::new(0),
            delete: AtomicUsize::new(0),
            ownership: Ownership::Owned,
            reject: false,
        };
        let host = FakeHost::new();
        let base = settings();
        let mut first = request(&registry, &spec, &base, None);
        first.bucket_keys = saved.bucket_keys.clone();
        provision_swarm_with_backend(&host, &cloud, first)
            .await
            .unwrap();
        let original = registry.read_record(&spec.name).unwrap().unwrap();
        let mut changed = request(&registry, &spec, &base, None);
        changed.bucket_keys = Some(StaticKeys {
            access_key: "new-key".into(),
            secret_key: "new-secret".into(),
        });
        let error = provision_swarm_with_backend(&host, &cloud, changed)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("cannot rotate"));
        let mut removed = spec.clone();
        removed.bucket = None;
        let error =
            provision_swarm_with_backend(&host, &cloud, request(&registry, &removed, &base, None))
                .await
                .unwrap_err();
        assert!(error.to_string().contains("[bucket] changed"));
        assert_eq!(registry.read_record(&spec.name).unwrap().unwrap(), original);
        assert_eq!(cloud.ensure.load(Ordering::Relaxed), 1);

        let (_dir, registry, mut no_bucket) = fixture();
        let host = FakeHost::new();
        provision_swarm_with(&host, request(&registry, &no_bucket, &base, None))
            .await
            .unwrap();
        no_bucket.bucket = spec.bucket.clone();
        let error = provision_swarm_with_backend(
            &host,
            &cloud,
            request(&registry, &no_bucket, &base, None),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("[bucket] changed"));
    }

    #[tokio::test]
    async fn bucket_swarm_never_logs_keys() {
        let (_dir, registry, spec, saved) = fixture_bucket();
        crate::set_output_sink(capture_output);
        OUTPUT_LINES.lock().unwrap().clear();
        let cloud = FakeCloud {
            ensure: AtomicUsize::new(0),
            delete: AtomicUsize::new(0),
            ownership: Ownership::Owned,
            reject: false,
        };
        let host = FakeHost::new();
        let base = settings();
        let mut up = request(&registry, &spec, &base, None);
        up.bucket_keys = saved.bucket_keys.clone();
        let reports = provision_swarm_with_backend(&host, &cloud, up)
            .await
            .unwrap();
        assert_eq!(cloud.ensure.load(Ordering::Relaxed), 1);
        assert!(
            host.calls()
                .iter()
                .any(|call| call.starts_with("provision fleet-control-1"))
        );
        let record = registry.read_record(&spec.name).unwrap().unwrap();
        assert_eq!(record.bucket_keys, saved.bucket_keys);
        let mode = fs::metadata(registry.swarm_dir(&spec.name).unwrap().join("record.toml"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            mode, 0o600,
            "record containing bucket keys must stay private"
        );
        let bucket = bucket_spec(&spec, &record);
        let primary = upgrade_machines(&record, &spec, bucket.as_ref())
            .pop()
            .unwrap();
        assert!(primary.bucket_spec().unwrap().needs_static_keys());
        delete_bucket_with(&cloud, &spec, &record).await.unwrap();
        let lines = OUTPUT_LINES.lock().unwrap().join("\n");
        assert!(lines.contains("bucket test-bucket"));
        for output in [
            format!("{primary:?}"),
            lines,
            format!("{record:?}"),
            format!("{reports:?}"),
            format!("{:?}", host.calls()),
            format!("{:?}", host.configs()),
        ] {
            assert!(!output.contains("access-secret"));
            assert!(!output.contains("value-secret"));
        }
    }

    #[tokio::test]
    async fn bucket_without_keys_fails_first_up() {
        let (_dir, registry, spec, _record) = fixture_bucket();
        let host = FakeHost::new();
        let err = provision_swarm_with(&host, request(&registry, &spec, &settings(), None))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("on its first swarmy swarm up"));
        assert!(host.calls().is_empty());
    }
}

#[tokio::test]
async fn control_node_provisioning_generates_a_fresh_api_token_unless_configured() {
    let mut generated = Vec::new();
    for _ in 0..2 {
        let (_dir, registry, spec) = fixture();
        let host = FakeHost::new();
        provision_swarm_with(&host, request(&registry, &spec, &settings(), None))
            .await
            .unwrap();
        let config: Settings = toml::from_str(&host.configs()[0]).unwrap();
        assert!(!config.api.token.is_empty());
        generated.push(config.api.token);
    }
    assert_ne!(generated[0], generated[1]);

    let (_dir, registry, spec) = fixture();
    let host = FakeHost::new();
    let mut configured = settings();
    configured.api.token = "operator-token".into();
    provision_swarm_with(&host, request(&registry, &spec, &configured, None))
        .await
        .unwrap();
    let config: Settings = toml::from_str(&host.configs()[0]).unwrap();
    assert_eq!(config.api.token, "operator-token");
    assert!(registry.read_record(&spec.name).unwrap().is_some());
}

#[tokio::test]
async fn node_config_keeps_fake_call_log_relative_to_the_checkout() {
    let (_dir, registry, mut spec) = fixture();
    spec.service_user = Some("deploy-user".into());
    let host = FakeHost::new();
    provision_swarm_with(&host, request(&registry, &spec, &settings(), None))
        .await
        .unwrap();
    let config: Settings = toml::from_str(&host.configs()[0]).unwrap();
    assert_eq!(config.selection.provider, "fake");
    assert_eq!(config.fake.call_log, PathBuf::from(".swarmy/calls.log"));
    assert!(!host.configs()[0].contains("[remote]"));
    assert!(
        host.calls()
            .contains(&"services fleet-control-1".to_owned())
    );
}

#[derive(Default)]
struct FakeUpgradeHost {
    events: Mutex<Vec<String>>,
    upgrades: Mutex<Vec<(String, bool, bool, Duration)>>,
}
impl upgrade::UpgradeHost for FakeUpgradeHost {
    fn version(&self, node: &Machine) -> impl Future<Output = Result<String>> {
        self.events
            .lock()
            .unwrap()
            .push(format!("version:{}", node.name));
        std::future::ready(Ok("swarmy-core 0.1.0 (test)".into()))
    }
    fn has_service_units(&self, node: &Machine) -> impl Future<Output = Result<bool>> {
        std::future::ready(Ok(node.name != "fleet-workers-1"))
    }
    fn upgrade(
        &self,
        node: &Machine,
        services_only: bool,
        drain_timeout: Duration,
        stack: bool,
    ) -> impl Future<Output = Result<Summary>> {
        self.events
            .lock()
            .unwrap()
            .push(format!("upgrade:{}", node.name));
        self.upgrades.lock().unwrap().push((
            node.name.clone(),
            services_only,
            stack,
            drain_timeout,
        ));
        std::future::ready(Ok(Summary {
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
async fn upgrade_fixture() -> (tempfile::TempDir, Registry, SwarmSpec, Vec<Machine>) {
    let (dir, registry, spec) = fixture();
    provision_swarm_with(
        &FakeHost::new(),
        request(&registry, &spec, &settings(), None),
    )
    .await
    .unwrap();
    let record = registry.read_record(&spec.name).unwrap().unwrap();
    let machines = upgrade_machines(&record, &spec, None);
    (dir, registry, spec, machines)
}
#[tokio::test]
async fn upgrade_orders_nodes_and_respects_drain_and_dirty_checkout() {
    let (_dir, _registry, _spec, machines) = upgrade_fixture().await;
    let host = FakeUpgradeHost::default();
    let mut policy = upgrade::Options::new(false, false, 600, false);
    policy.dirty_paths = vec!["M src/main.rs".into()];
    let error = upgrade::run(&host, &machines, policy).await.unwrap_err();
    assert!(error.to_string().contains("src/main.rs"));
    assert!(host.events.lock().unwrap().is_empty());
    let clean = upgrade::Options::new(false, true, 17, true);
    assert_eq!(
        upgrade::run(&host, &machines, clean).await.unwrap().len(),
        3
    );
    host.events.lock().unwrap().clear();
    host.upgrades.lock().unwrap().clear();
    let mut allowed = upgrade::Options::new(true, true, 17, false);
    allowed.dirty_paths = vec!["M src/main.rs".into()];
    let summaries = upgrade::run(&host, &machines, allowed).await.unwrap();
    assert_eq!(
        host.events.lock().unwrap().as_slice(),
        [
            "version:fleet-workers-2",
            "version:fleet-workers-1",
            "version:fleet-control-1",
            "upgrade:fleet-workers-2",
            "upgrade:fleet-workers-1",
            "upgrade:fleet-control-1",
        ]
    );
    let upgrades = host.upgrades.lock().unwrap();
    assert_eq!(
        upgrades
            .iter()
            .map(|row| row.0.as_str())
            .collect::<Vec<_>>(),
        ["fleet-workers-2", "fleet-workers-1", "fleet-control-1"]
    );
    assert!(
        upgrades
            .iter()
            .all(|row| row.1 && row.3 == Duration::from_secs(17))
    );
    assert_eq!(
        upgrades.iter().map(|row| row.2).collect::<Vec<_>>(),
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
    let (_dir, _registry, _spec, machines) = upgrade_fixture().await;
    let summaries = upgrade::run(
        &FakeUpgradeHost::default(),
        &machines,
        upgrade::Options::new(false, false, 17, true),
    )
    .await
    .unwrap();
    assert_eq!(summaries.len(), 3);
    // swarm upgrade --json serializes each Summary to exactly one line on stdout.
    for summary in &summaries {
        let line = serde_json::to_string(summary).unwrap();
        assert_eq!(line.lines().count(), 1);
        let value: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert!(value.is_object());
        for field in ["changed", "units", "restarted"] {
            assert!(value[field].is_array());
        }
        assert!(value["elapsed_seconds"].is_number());
    }
}
#[tokio::test]
async fn upgrade_callback_reports_completed_hosts_before_failure() {
    let (_dir, _registry, _spec, machines) = upgrade_fixture().await;
    let host = FakeUpgradeHost::default();
    let mut seen = Vec::new();
    let err = upgrade::run_with(
        &host,
        &machines,
        upgrade::Options::new(false, false, 17, true),
        |summary| {
            seen.push(summary.node.clone());
            Err(Error::other("stop after the first completed host"))
        },
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("first completed host"));
    assert_eq!(seen, ["fleet-workers-2"]);
    assert_eq!(host.upgrades.lock().unwrap().len(), 1);
}

#[test]
fn upgrade_summary_accepts_old_json() {
    let summary: Summary =
        serde_json::from_str(r#"{"node":"","changed":[],"restarted":[],"elapsed_seconds":0}"#)
            .unwrap();
    assert!(summary.units.is_empty());
}

#[test]
fn nodes_use_an_external_bucket_only_when_the_swarm_has_one() {
    let (_tmp, _registry, mut spec) = fixture();
    let mut record = SwarmRecord {
        name: spec.name.clone(),
        created_at: jiff::Timestamp::now(),
        api: SwarmApi::default(),
        hosts: BTreeMap::new(),
        bucket_keys: None,
        tofu_state: None,
        tofu_bucket: None,
        tofu_bucket_endpoint: None,
        tofu_bucket_region: None,
        tofu_networks: BTreeMap::new(),
    };
    assert!(!external_bucket(&spec, &record));
    record.tofu_bucket = Some("swarmy-fleet-000000000001".into());
    assert!(external_bucket(&spec, &record));
    record.tofu_bucket = None;
    spec.bucket = Some(swarmy_config::swarm::SwarmBucket {
        name: "fleet-bucket".into(),
        endpoint: "https://objects.example.com".into(),
        region: "eu-central".into(),
        prefix: swarmy_config::ObjectPrefix::default(),
        conditional_create: true,
    });
    assert!(external_bucket(&spec, &record));
}
