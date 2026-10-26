//! Provision SSH-reachable hosts as one swarm: stack first, services, then sandbox nodes.
//! Groups provide hosts (existing sources now, `OpenTofu` groups later), and everything after is shared.
//! Records precede host changes so a failed up resumes and down can clean up.
mod plan;
#[cfg(test)]
mod tests;
use crate::{Error, Result, services, ssh, tunnel::Tunnels};
use fs2::FileExt;
use std::{
    collections::BTreeMap,
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
};
use swarmy_config::{
    self, Provider, RemoteNode, RemotePorts, RemoteServices, RemoteSettings, Settings,
    create_private_dir,
    swarm::{Module, RecordedHost, Registry, SwarmApi, SwarmRecord, SwarmSpec},
};
const OBJECT_STORE_REGION: &str = "us-east-1";

/// One machine a group provides before provisioning.
#[derive(Clone, Debug)]
pub struct HostSource {
    pub group: String,
    /// As recorded, with a private SSH key path inside the swarm directory.
    pub host: RecordedHost,
    /// Operator key to copy, or none when already copied.
    pub copy_key_from: Option<PathBuf>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Stack { services: bool },
    Node,
}
impl Role {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Stack { services: true } => "stack+services",
            Self::Stack { services: false } => "stack",
            Self::Node => "node",
        }
    }
}
#[derive(Clone, Debug)]
pub struct Planned {
    pub group: String,
    pub index: usize,
    pub host: RecordedHost,
    pub copy_key_from: Option<PathBuf>,
    pub role: Role,
    pub sandboxes: u32,
    pub local_storage: String,
}
/// True when every group lists existing hosts.
#[must_use]
pub fn is_host_swarm(spec: &SwarmSpec) -> bool {
    spec.groups.values().all(|g| g.module == Module::Existing)
}
/// Build host sources from an existing-host spec.
/// # Errors
/// Rejects groups that do not provide existing hosts.
pub fn existing_sources(spec: &SwarmSpec, swarm_dir: &Path) -> Result<Vec<HostSource>> {
    plan::existing_sources(spec, swarm_dir)
}
/// Assign roles and validate all sources before any SSH work.
/// # Errors
/// Rejects unsupported placements and invalid hosts.
pub fn plan(spec: &SwarmSpec, sources: Vec<HostSource>) -> Result<Vec<Planned>> {
    plan::plan(spec, sources)
}

pub struct UpRequest<'a> {
    pub registry: &'a Registry,
    pub spec: &'a SwarmSpec,
    pub sources: Vec<HostSource>,
    pub settings: &'a Settings,
    pub copy_credential: bool,
    pub recipe: Option<&'a Path>,
    pub cache: &'a Path,
}
#[derive(Clone, Debug, serde::Serialize)]
pub struct HostReport {
    pub name: String,
    pub group: String,
    pub role: &'static str,
    pub address: String,
    pub provisioned: bool,
    pub changed: bool,
}
fn report(p: &Planned, changed: bool) -> HostReport {
    HostReport {
        name: p.host.name.clone(),
        group: p.group.clone(),
        role: p.role.label(),
        address: p.host.public_address.clone(),
        provisioned: p.host.provisioned || changed,
        changed,
    }
}

pub(crate) trait SwarmHost {
    async fn adopt_key(&self, node: &RemoteNode, source: &Path) -> Result<()>;
    async fn provision(&self, node: &RemoteNode, primary: Option<&RemoteNode>) -> Result<String>;
    async fn services(
        &self,
        node: &RemoteNode,
        address: &str,
        options: &services::Options<'_>,
    ) -> Result<()>;
    async fn api_token(&self, node: &RemoteNode, address: &str) -> Result<String>;
    async fn build_image(
        &self,
        node: &RemoteNode,
        address: &str,
        recipe: &Path,
        tag: &str,
    ) -> Result<()>;
    async fn decommission(&self, node: &RemoteNode) -> Result<()>;
    async fn stop(&self, node: &RemoteNode) -> Result<()>;
    async fn start(&self, node: &RemoteNode) -> Result<()>;
    async fn reachable(&self, node: &RemoteNode) -> bool;
}
impl SwarmHost for ssh::Ssh {
    async fn adopt_key(&self, node: &RemoteNode, source: &Path) -> Result<()> {
        ssh::adopt_key(node, source).await
    }
    async fn provision(&self, node: &RemoteNode, primary: Option<&RemoteNode>) -> Result<String> {
        self.provision(node, primary).await
    }
    async fn services(
        &self,
        node: &RemoteNode,
        address: &str,
        options: &services::Options<'_>,
    ) -> Result<()> {
        services::install(node, address, options).await
    }
    async fn api_token(&self, node: &RemoteNode, address: &str) -> Result<String> {
        ssh::api_token(node, address).await
    }
    async fn build_image(
        &self,
        node: &RemoteNode,
        address: &str,
        recipe: &Path,
        tag: &str,
    ) -> Result<()> {
        ssh::build_image_tagged(node, address, recipe, tag).await
    }
    async fn decommission(&self, node: &RemoteNode) -> Result<()> {
        self.decommission(node).await
    }
    async fn stop(&self, node: &RemoteNode) -> Result<()> {
        ssh::stop_units(node).await
    }
    async fn start(&self, node: &RemoteNode) -> Result<()> {
        ssh::start_units(node).await
    }
    async fn reachable(&self, node: &RemoteNode) -> bool {
        ssh::reachable_address(node).await.is_ok()
    }
}
fn machine(
    host: &RecordedHost,
    service_user: &str,
    role: Role,
    sandboxes: u32,
    local_storage: &str,
) -> RemoteNode {
    RemoteNode {
        name: host.name.clone(),
        region: OBJECT_STORE_REGION.into(),
        instance_id: String::new(),
        launch_attempted: true,
        public_ip: host.public_address.clone(),
        private_ip: host
            .private_address
            .clone()
            .unwrap_or_else(|| host.public_address.clone()),
        key_path: host.ssh_key.clone(),
        ssh_user: host.ssh_user.clone(),
        ports: RemotePorts::default(),
        nodes: Vec::new(),
        sandboxes,
        default_image: None,
        launch_settings: Some(RemoteSettings {
            provider: Provider::Existing,
            services: if matches!(role, Role::Stack { services: true }) {
                RemoteServices::Node
            } else {
                RemoteServices::Laptop
            },
            region: OBJECT_STORE_REGION.into(),
            service_user: service_user.into(),
            local_storage: local_storage.into(),
            bucket: None,
            ..RemoteSettings::default()
        }),
        created_at: String::new(),
    }
}
fn node(p: &Planned, user: &str) -> RemoteNode {
    machine(&p.host, user, p.role, p.sandboxes, &p.local_storage)
}
fn user(spec: &SwarmSpec) -> &str {
    spec.service_user.as_deref().unwrap_or("swarmy")
}
/// Exclusive nonblocking per-swarm lock, held until dropped.
/// # Errors
/// Reports filesystem failures or a concurrent swarm command.
pub fn lock(registry: &Registry, name: &str) -> Result<fs::File> {
    let dir = registry.swarm_dir(name)?;
    create_private_dir(&dir)?;
    let path = dir.join("swarm.lock");
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)?;
    if let Err(error) = file.try_lock_exclusive() {
        if error.kind() == ErrorKind::WouldBlock {
            return Err(Error::other(format!(
                "another swarmy command is changing swarm {name} (lock {})",
                path.display()
            )));
        }
        return Err(error.into());
    }
    Ok(file)
}
// Explicit unlock makes the transition between consecutive CLI commands
// deterministic even when a runtime retains completed futures briefly.
struct Locked(fs::File);
impl Drop for Locked {
    fn drop(&mut self) {
        swarmy_core::ignore_best_effort(self.0.unlock(), "release swarm command lock");
    }
}
fn reconcile(plan: &[Planned], record: &SwarmRecord) -> Result<()> {
    for old in record.hosts.values().flatten() {
        let Some(new) = plan.iter().find(|p| p.host.name == old.name) else {
            return Err(Error::other(format!(
                "host {} is recorded but no longer in the spec; restore it, or remove swarmy from the hosts with swarmy swarm down {} --destroy",
                old.name, record.name
            )));
        };
        for (field, before, after) in [
            ("address", &old.public_address, &new.host.public_address),
            (
                "private_address",
                old.private_address.as_ref().unwrap_or(&old.public_address),
                new.host
                    .private_address
                    .as_ref()
                    .unwrap_or(&new.host.public_address),
            ),
            ("ssh_user", &old.ssh_user, &new.host.ssh_user),
        ] {
            if before != after {
                return Err(Error::other(format!(
                    "host {} changed in the spec ({field} {before} -> {after}); swarmy cannot move a provisioned host; run swarmy swarm down {} --destroy first",
                    old.name, record.name
                )));
            }
        }
    }
    Ok(())
}
/// Provision hosts and resume any unfinished hosts.
/// # Errors
/// Returns planning, record, key, SSH, and service errors.
pub async fn provision_swarm(request: UpRequest<'_>) -> Result<Vec<HostReport>> {
    let host = ssh::Ssh::for_client(request.cache)?;
    provision_swarm_with(&host, request).await
}
async fn provision_swarm_with(
    host: &impl SwarmHost,
    mut request: UpRequest<'_>,
) -> Result<Vec<HostReport>> {
    let _lock = Locked(lock(request.registry, &request.spec.name)?);
    let spec = request.spec;
    let swarm_dir = request.registry.swarm_dir(&spec.name)?;
    let planned = plan(spec, std::mem::take(&mut request.sources))?;
    let mut record = request
        .registry
        .read_record(&spec.name)?
        .unwrap_or_else(|| SwarmRecord {
            name: spec.name.clone(),
            created_at: jiff::Timestamp::now(),
            api: SwarmApi::default(),
            hosts: BTreeMap::new(),
            tofu_state: None,
        });
    reconcile(&planned, &record)?;
    let primary = planned
        .first()
        .ok_or_else(|| Error::other("no primary host"))?;
    let primary_done = recorded(&record, primary).is_some_and(|h| h.provisioned);
    if !primary_done
        && planned
            .iter()
            .skip(1)
            .any(|p| recorded(&record, p).is_some_and(|h| h.provisioned))
    {
        return Err(Error::other(format!(
            "the stack host {} is not provisioned but nodes are; run swarmy swarm down {} --destroy and start again",
            primary.host.name, spec.name
        )));
    }
    let options = if primary_done {
        None
    } else {
        let token = record
            .api
            .token
            .clone()
            .unwrap_or_else(|| ulid::Ulid::generate().to_string());
        let mut settings = request.settings.clone();
        settings.api = swarmy_config::ApiSettings {
            token,
            ..Default::default()
        };
        settings.remote.services = RemoteServices::Node;
        settings.remote.service_user = user(spec).into();
        let options = services::Options::new(&settings, request.copy_credential, request.recipe)?;
        Some(if request.recipe.is_some() {
            options.with_default_image(&format!("base-ubuntu:{}", spec.name))?
        } else {
            options
        })
    };
    create_private_dir(&swarm_dir.join("keys"))?;
    let primary_node = node(primary, user(spec));
    let mut reports = Vec::new();
    for p in &planned {
        if recorded(&record, p).is_some_and(|h| h.provisioned) {
            cloud_out!("{}: already provisioned", p.host.name);
            reports.push(report(p, false));
            continue;
        }
        let result = provision_one(
            host,
            &request,
            &mut record,
            p,
            &primary_node,
            options.as_ref(),
        )
        .await;
        if let Err(error) = result {
            cloud_err!(
                "swarm up {} stopped at host {}; fix the cause and run swarmy swarm up {} again (provisioned hosts are skipped), or swarmy swarm down {} --destroy to remove swarmy from the hosts",
                spec.name,
                p.host.name,
                spec.name,
                spec.name
            );
            return Err(error);
        }
        reports.push(report(p, true));
    }
    if request.registry.current()?.is_none() {
        request.registry.set_current(&spec.name)?;
    }
    Ok(reports)
}
fn recorded<'a>(record: &'a SwarmRecord, p: &Planned) -> Option<&'a RecordedHost> {
    record.hosts.get(&p.group)?.get(p.index)
}
async fn provision_one(
    host: &impl SwarmHost,
    request: &UpRequest<'_>,
    record: &mut SwarmRecord,
    p: &Planned,
    primary: &RemoteNode,
    options: Option<&services::Options<'_>>,
) -> Result<()> {
    let registry = request.registry;
    let spec = request.spec;
    let hosts = record.hosts.entry(p.group.clone()).or_default();
    if p.index == hosts.len() {
        hosts.push(p.host.clone());
    } else {
        hosts[p.index] = p.host.clone();
    }
    registry.write_record(record)?;
    let machine = node(p, user(spec));
    if let Some(source) = &p.copy_key_from {
        host.adopt_key(&machine, source).await?;
    }
    if matches!(p.role, Role::Stack { .. }) {
        let address = host.provision(&machine, None).await?;
        host.services(
            &machine,
            &address,
            options.expect("primary options prepared"),
        )
        .await?;
        record.api.token = Some(host.api_token(&machine, &address).await?);
        registry.write_record(record)?;
        if let Some(recipe) = request.recipe {
            cloud_out!(
                "Building base-ubuntu:{} (this takes several minutes)",
                spec.name
            );
            host.build_image(&machine, &address, recipe, &spec.name)
                .await?;
        }
    } else {
        host.provision(&machine, Some(primary)).await?;
    }
    record.hosts.get_mut(&p.group).expect("group inserted")[p.index].provisioned = true;
    registry.write_record(record)?;
    Ok(())
}
fn recorded_plan(registry: &Registry, spec: &SwarmSpec) -> Result<(SwarmRecord, Vec<Planned>)> {
    let record = registry.read_record(&spec.name)?.ok_or_else(|| {
        Error::other(format!(
            "swarm {} has no hosts recorded; run swarmy swarm up {}",
            spec.name, spec.name
        ))
    })?;
    let sources = record
        .hosts
        .iter()
        .flat_map(|(group, hosts)| {
            hosts.iter().map(move |host| HostSource {
                group: group.clone(),
                host: host.clone(),
                copy_key_from: None,
            })
        })
        .collect();
    let planned = plan(spec, sources)?;
    Ok((record, planned))
}
/// Stop provisioned hosts in reverse dependency order.
/// # Errors
/// Reports missing records or host failures.
pub async fn stop_swarm(registry: &Registry, spec: &SwarmSpec) -> Result<Vec<HostReport>> {
    stop_swarm_with(&ssh::Ssh::teardown(), registry, spec).await
}
async fn stop_swarm_with(
    host: &impl SwarmHost,
    registry: &Registry,
    spec: &SwarmSpec,
) -> Result<Vec<HostReport>> {
    let _lock = Locked(lock(registry, &spec.name)?);
    let (record, planned) = recorded_plan(registry, spec)?;
    let mut reports = Vec::new();
    for p in planned.iter().rev() {
        if recorded(&record, p).is_some_and(|h| h.provisioned) {
            host.stop(&node(p, user(spec)))
                .await
                .map_err(|e| Error::context(e, format!("host {}", p.host.name)))?;
            reports.push(report(p, true));
        }
    }
    Ok(reports)
}
/// Start provisioned hosts in dependency order.
/// # Errors
/// Reports missing records or host failures.
pub async fn start_swarm(registry: &Registry, spec: &SwarmSpec) -> Result<Vec<HostReport>> {
    start_swarm_with(&ssh::Ssh::teardown(), registry, spec).await
}
async fn start_swarm_with(
    host: &impl SwarmHost,
    registry: &Registry,
    spec: &SwarmSpec,
) -> Result<Vec<HostReport>> {
    let _lock = Locked(lock(registry, &spec.name)?);
    let (record, planned) = recorded_plan(registry, spec)?;
    let mut reports = Vec::new();
    for p in &planned {
        if recorded(&record, p).is_some_and(|h| h.provisioned) {
            host.start(&node(p, user(spec)))
                .await
                .map_err(|e| Error::context(e, format!("host {}", p.host.name)))?;
            reports.push(report(p, true));
        }
    }
    Ok(reports)
}
/// Remove swarmy from hosts, leaving machines and spec in place.
/// # Errors
/// Reports host failures and retains them in the record.
pub async fn decommission_swarm(registry: &Registry, spec: &SwarmSpec) -> Result<Vec<HostReport>> {
    decommission_swarm_with(&ssh::Ssh::teardown(), registry, spec).await
}
async fn decommission_swarm_with(
    host: &impl SwarmHost,
    registry: &Registry,
    spec: &SwarmSpec,
) -> Result<Vec<HostReport>> {
    let _lock = Locked(lock(registry, &spec.name)?);
    let Some(mut record) = registry.read_record(&spec.name)? else {
        cloud_out!(
            "swarm {} has no hosts recorded; nothing to remove",
            spec.name
        );
        return Ok(Vec::new());
    };
    // Rebuild the full role order even after a partial down removed the primary.
    let sources = existing_sources(spec, &registry.swarm_dir(&spec.name)?)?
        .into_iter()
        .map(|mut source| {
            source.copy_key_from = None;
            if let Some(old) = record
                .hosts
                .values()
                .flatten()
                .find(|h| h.name == source.host.name)
            {
                source.host = old.clone();
            }
            source
        })
        .collect();
    let planned: Vec<_> = plan(spec, sources)?
        .into_iter()
        .filter(|p| {
            record
                .hosts
                .values()
                .flatten()
                .any(|h| h.name == p.host.name)
        })
        .collect();
    let mut reports = Vec::new();
    let mut failures = Vec::new();
    let mut failed = BTreeMap::<String, Vec<RecordedHost>>::new();
    for p in planned.iter().rev() {
        match host.decommission(&node(p, user(spec))).await {
            Ok(()) => reports.push(report(p, true)),
            Err(error) => {
                failures.push(format!(
                    "{}: {}",
                    p.host.name,
                    swarmy_core::error_chain(&error)
                ));
                failed
                    .entry(p.group.clone())
                    .or_default()
                    .push(p.host.clone());
            }
        }
    }
    if !failures.is_empty() {
        failures.sort();
        record.hosts = failed;
        registry.write_record(&record)?;
        return Err(Error::other(format!(
            "swarm down {} incomplete for {} host(s) ({}); fix them, then run swarmy swarm down {} --destroy again",
            spec.name,
            failures.len(),
            failures.join("; "),
            spec.name
        )));
    }
    let dir = registry.swarm_dir(&spec.name)?;
    swarmy_core::ignore_best_effort(
        Tunnels::ssh().disconnect(&dir.join("tunnel")).await,
        "close the swarm's SSH tunnel",
    );
    // The lock file is removed while the open descriptor still holds the lock on Linux.
    if let Err(error) = fs::remove_dir_all(&dir)
        && error.kind() != ErrorKind::NotFound
    {
        return Err(error.into());
    }
    cloud_out!(
        "removed swarmy from {} host(s) of swarm {}; the machines keep running. The spec stays at {}; swarmy swarm rm {} forgets it",
        reports.len(),
        spec.name,
        registry.spec_path(&spec.name)?.display(),
        spec.name
    );
    Ok(reports)
}
#[derive(Debug, serde::Serialize)]
pub struct Status {
    pub name: String,
    pub api_token: bool,
    pub tunnel: bool,
    pub hosts: Vec<HostStatus>,
}
#[derive(Debug, serde::Serialize)]
pub struct HostStatus {
    pub name: String,
    pub group: String,
    pub role: &'static str,
    pub address: String,
    pub provisioned: bool,
    pub ssh: bool,
}
/// Report recorded hosts, SSH reachability and tunnel status.
/// # Errors
/// Reports missing records or tunnel failures.
pub async fn status(registry: &Registry, spec: &SwarmSpec) -> Result<Status> {
    status_with(&ssh::Ssh::teardown(), registry, spec).await
}
async fn status_with(
    host: &impl SwarmHost,
    registry: &Registry,
    spec: &SwarmSpec,
) -> Result<Status> {
    let (record, planned) = recorded_plan(registry, spec)?;
    let mut hosts = Vec::new();
    for p in &planned {
        hosts.push(HostStatus {
            name: p.host.name.clone(),
            group: p.group.clone(),
            role: p.role.label(),
            address: p.host.public_address.clone(),
            provisioned: p.host.provisioned,
            ssh: host.reachable(&node(p, user(spec))).await,
        });
    }
    Ok(Status {
        name: spec.name.clone(),
        api_token: record.api.token.is_some(),
        tunnel: Tunnels::ssh()
            .status(&registry.swarm_dir(&spec.name)?.join("tunnel"))
            .await?
            .is_some(),
        hosts,
    })
}
