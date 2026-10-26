#![deny(clippy::disallowed_methods)]
use super::*;
use std::future::Future;
use std::os::unix::fs::PermissionsExt as _;
use std::sync::Mutex;
struct FakeHost {
    calls: Mutex<Vec<String>>,
    fail_provision: Mutex<Option<String>>,
    fail_decommission: Mutex<Option<String>>,
}
impl FakeHost {
    fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            fail_provision: Mutex::new(None),
            fail_decommission: Mutex::new(None),
        }
    }
    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
    fn push(&self, message: String) {
        self.calls.lock().unwrap().push(message);
    }
}
impl SwarmHost for FakeHost {
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
                "provision {} primary={}",
                node.name,
                primary.map_or("none", |p| &p.name)
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
        _: &services::Options<'_>,
    ) -> impl Future<Output = Result<()>> {
        std::future::ready({
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
    fn stop(&self, node: &Machine) -> impl Future<Output = Result<()>> {
        std::future::ready({
            self.push(format!("stop {}", node.name));
            Ok(())
        })
    }
    fn start(&self, node: &Machine) -> impl Future<Output = Result<()>> {
        std::future::ready({
            self.push(format!("start {}", node.name));
            Ok(())
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
fn existing_sources_rejects_module_groups() {
    let (_tmp, registry, mut spec) = fixture();
    spec.groups.get_mut("workers").unwrap().module = Module::HetznerCloud;
    assert!(
        existing_sources(&spec, &registry.swarm_dir("fleet").unwrap())
            .unwrap_err()
            .to_string()
            .contains("only existing-host groups")
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
            "provision fleet-control-1 primary=none",
            "services fleet-control-1",
            "token fleet-control-1",
            "image fleet-control-1 fleet",
            "adopt fleet-workers-1",
            "provision fleet-workers-1 primary=fleet-control-1",
            "adopt fleet-workers-2",
            "provision fleet-workers-2 primary=fleet-control-1"
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
            "provision fleet-workers-3 primary=fleet-control-1"
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
            "provision fleet-workers-2 primary=fleet-control-1"
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
        decommission_swarm_with(&fake, &registry, &spec)
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
    decommission_swarm_with(&fake, &registry, &spec)
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
    decommission_swarm_with(&fake, &registry, &spec)
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
    decommission_swarm_with(&fake, &registry, &spec)
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
    assert_eq!(node.services, true);
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
