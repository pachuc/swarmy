#![deny(clippy::disallowed_methods)]
use super::*;
use std::future::Future;
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
    fn adopt_key(&self, node: &RemoteNode, source: &Path) -> impl Future<Output = Result<()>> {
        std::future::ready((|| {
            self.push(format!("adopt {}", node.name));
            swarmy_config::write_private(&node.key_path, &fs::read(source)?)?;
            Ok(())
        })())
    }
    fn provision(
        &self,
        node: &RemoteNode,
        primary: Option<&RemoteNode>,
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
        node: &RemoteNode,
        _: &str,
        _: &services::Options<'_>,
    ) -> impl Future<Output = Result<()>> {
        std::future::ready((|| {
            self.push(format!("services {}", node.name));
            Ok(())
        })())
    }
    fn api_token(&self, node: &RemoteNode, _: &str) -> impl Future<Output = Result<String>> {
        std::future::ready((|| {
            self.push(format!("token {}", node.name));
            Ok("node-token".into())
        })())
    }
    fn build_image(
        &self,
        node: &RemoteNode,
        _: &str,
        _: &Path,
        tag: &str,
    ) -> impl Future<Output = Result<()>> {
        std::future::ready((|| {
            self.push(format!("image {} {tag}", node.name));
            Ok(())
        })())
    }
    fn decommission(&self, node: &RemoteNode) -> impl Future<Output = Result<()>> {
        std::future::ready((|| {
            self.push(format!("decommission {}", node.name));
            if self.fail_decommission.lock().unwrap().as_deref() == Some(&node.name) {
                return Err(Error::other("decommission failed"));
            }
            Ok(())
        })())
    }
    fn stop(&self, node: &RemoteNode) -> impl Future<Output = Result<()>> {
        std::future::ready((|| {
            self.push(format!("stop {}", node.name));
            Ok(())
        })())
    }
    fn start(&self, node: &RemoteNode) -> impl Future<Output = Result<()>> {
        std::future::ready((|| {
            self.push(format!("start {}", node.name));
            Ok(())
        })())
    }
    fn reachable(&self, _: &RemoteNode) -> impl Future<Output = bool> {
        std::future::ready((|| true)())
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
    provision_swarm_with(&fake, request(&registry, &spec, &settings(), None))
        .await
        .unwrap();
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
    let record = registry.read_record("fleet").unwrap().unwrap();
    assert_eq!(record.hosts["workers"].len(), 1);
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
#[test]
fn machine_maps_the_recorded_host() {
    let (_tmp, registry, spec) = fixture();
    let p = &plan(&spec, source_list(&registry, &spec)).unwrap()[0];
    let node = machine(&p.host, "swarmy", p.role, p.sandboxes, &p.local_storage);
    assert_eq!(node.private_ip, p.host.public_address);
    assert_eq!(node.key_path, p.host.ssh_key);
    assert_eq!(node.cloud_settings().services, RemoteServices::Node);
}
#[test]
fn lock_releases_on_drop() {
    let (_tmp, registry, _spec) = fixture();
    let guard = lock(&registry, "fleet").unwrap();
    assert!(lock(&registry, "fleet").is_err());
    drop(guard);
    let _again = lock(&registry, "fleet").unwrap();
}
