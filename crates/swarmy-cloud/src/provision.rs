//! Provision SSH-reachable hosts as one swarm: stack first, services, then sandbox nodes.
//! Existing sources and `OpenTofu` groups provide hosts; everything after is shared.
//! Records precede host changes so a failed up resumes and down can clean up.
pub mod groups;
mod machine;
mod plan;
pub mod scale;
mod upgrade;
pub use machine::Machine;
pub use upgrade::Summary;
#[cfg(test)]
#[path = "provision/tests/support.rs"]
mod test_support;
#[cfg(test)]
mod tests;
use crate::{Error, Result, services, ssh, tunnel::Tunnels};
use fs2::FileExt;
use std::{
    collections::BTreeMap,
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
    time::Duration,
};
use swarmy_config::{
    self, Settings, create_private_dir,
    swarm::{RecordedHost, Registry, StaticKeys, SwarmApi, SwarmRecord, SwarmSpec},
};

/// One machine a group provides before provisioning.
#[derive(Clone, Debug)]
pub struct HostSource {
    pub group: String,
    /// As recorded, with a private SSH key path inside the swarm directory.
    pub host: RecordedHost,
    /// Operator key to copy, or none when already copied.
    pub copy_key_from: Option<PathBuf>,
    pub created: bool,
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
    pub created: bool,
    pub role: Role,
    pub sandboxes: u32,
    pub local_storage: String,
}
/// Build host sources from an existing-host spec.
/// # Errors
/// Rejects groups that do not provide existing hosts.
pub fn existing_sources(spec: &SwarmSpec, swarm_dir: &Path) -> Result<Vec<HostSource>> {
    Ok(plan::existing_sources(spec, swarm_dir))
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
    pub bucket_keys: Option<StaticKeys>,
    pub machines: Option<groups::MachineGroups<'a>>,
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
    async fn adopt_key(&self, node: &Machine, source: &Path) -> Result<()>;
    async fn wait_ready(&self, node: &Machine) -> Result<()>;
    async fn provision(&self, node: &Machine, primary: Option<&Machine>) -> Result<String>;
    async fn services(
        &self,
        node: &Machine,
        address: &str,
        options: &services::Options,
    ) -> Result<()>;
    async fn api_token(&self, node: &Machine, address: &str) -> Result<String>;
    async fn build_image(
        &self,
        node: &Machine,
        address: &str,
        recipe: &Path,
        tag: &str,
    ) -> Result<()>;
    async fn decommission(&self, node: &Machine) -> Result<()>;
    async fn drain(&self, node: &Machine, timeout: Duration, force: bool) -> Result<ssh::Drain>;
    async fn revoke_tunnel(&self, node: &Machine, primary: &Machine) -> Result<()>;
    async fn stop(&self, node: &Machine) -> Result<()>;
    async fn start(&self, node: &Machine) -> Result<()>;
    async fn reachable(&self, node: &Machine) -> bool;
}
impl SwarmHost for ssh::Ssh {
    async fn wait_ready(&self, node: &Machine) -> Result<()> {
        ssh::wait_boot(node).await
    }
    async fn adopt_key(&self, node: &Machine, source: &Path) -> Result<()> {
        ssh::adopt_key(node, source).await
    }
    async fn provision(&self, node: &Machine, primary: Option<&Machine>) -> Result<String> {
        self.provision(node, primary.map(ssh::Primary::Remote))
            .await
    }
    async fn services(
        &self,
        node: &Machine,
        address: &str,
        options: &services::Options,
    ) -> Result<()> {
        services::install(node, address, options).await
    }
    async fn api_token(&self, node: &Machine, address: &str) -> Result<String> {
        ssh::api_token(node, address).await
    }
    async fn build_image(
        &self,
        node: &Machine,
        address: &str,
        recipe: &Path,
        tag: &str,
    ) -> Result<()> {
        ssh::build_image_tagged(node, address, recipe, tag).await
    }
    async fn decommission(&self, node: &Machine) -> Result<()> {
        self.decommission(node).await
    }
    async fn drain(&self, node: &Machine, timeout: Duration, force: bool) -> Result<ssh::Drain> {
        ssh::drain_node(node, timeout, force).await
    }
    async fn revoke_tunnel(&self, node: &Machine, primary: &Machine) -> Result<()> {
        ssh::revoke_tunnel(node, ssh::Primary::Remote(primary)).await
    }
    async fn stop(&self, node: &Machine) -> Result<()> {
        ssh::stop_units(node).await
    }
    async fn start(&self, node: &Machine) -> Result<()> {
        ssh::start_units(node).await
    }
    async fn reachable(&self, node: &Machine) -> bool {
        ssh::reachable_address(node).await.is_ok()
    }
}
fn machine(
    host: &RecordedHost,
    service_user: &str,
    role: Role,
    sandboxes: u32,
    local_storage: &str,
) -> Machine {
    Machine {
        name: host.name.clone(),
        public_ip: host.public_address.clone(),
        private_ip: host
            .private_address
            .clone()
            .unwrap_or_else(|| host.public_address.clone()),
        key_path: host.ssh_key.clone(),
        ssh_user: host.ssh_user.clone(),
        service_user: service_user.into(),
        local_storage: local_storage.into(),
        sandboxes,
        bucket: None,
        services: matches!(role, Role::Stack { services: true }),
        node_id: None,
    }
}
/// A machine an elastic group created, as the provisioner joins it: a sandbox node
/// with the group's sandboxes and storage, the swarm bucket, and its ledger node id.
#[must_use]
pub fn joining_node(
    host: &RecordedHost,
    service_user: &str,
    sandboxes: u32,
    local_storage: &str,
    bucket: Option<swarmy_config::BucketSpec>,
    node_id: swarmy_core::NodeId,
) -> Machine {
    let mut node = machine(host, service_user, Role::Node, sandboxes, local_storage);
    node.bucket = bucket;
    node.node_id = Some(node_id);
    node
}
fn node(p: &Planned, user: &str, bucket: Option<&swarmy_config::BucketSpec>) -> Machine {
    let mut machine = machine(&p.host, user, p.role, p.sandboxes, &p.local_storage);
    machine.bucket = bucket.cloned();
    machine
}
fn bucket_spec(spec: &SwarmSpec, record: &SwarmRecord) -> Option<swarmy_config::BucketSpec> {
    let bucket = spec.bucket.as_ref()?;
    let keys = record.bucket_keys.as_ref()?;
    Some(swarmy_config::BucketSpec {
        endpoint: bucket.endpoint.clone(),
        region: bucket.region.clone(),
        bucket: bucket.name.clone(),
        prefix: bucket.prefix.clone(),
        conditional_create: bucket.conditional_create,
        credentials: swarmy_config::BucketCredentials::StaticKeys {
            access_key: keys.access_key.clone(),
            secret_key: keys.secret_key.clone(),
        },
    })
}
/// The service login every host of the swarm uses.
#[must_use]
pub fn user(spec: &SwarmSpec) -> &str {
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
    request: UpRequest<'_>,
) -> Result<Vec<HostReport>> {
    let _lock = Locked(lock(request.registry, &request.spec.name)?);
    provision_locked(host, request).await
}

pub(crate) async fn provision_locked(
    host: &impl SwarmHost,
    request: UpRequest<'_>,
) -> Result<Vec<HostReport>> {
    #[cfg(feature = "remote")]
    {
        provision_swarm_with_backend(host, &crate::buckets::S3Buckets, request).await
    }
    #[cfg(not(feature = "remote"))]
    {
        provision_swarm_with_backend(host, request).await
    }
}
// Check machine groups before preparing a spec bucket can create remote state.
fn preflight_machine_groups(
    request: &UpRequest<'_>,
    record: &SwarmRecord,
    dir: &Path,
) -> Result<()> {
    let spec = request.spec;
    if groups::has_module_groups(spec) {
        let machines = request.machines.as_ref().ok_or_else(|| {
            Error::other(format!(
                "swarm {} has OpenTofu groups; this caller passed no OpenTofu runner",
                spec.name
            ))
        })?;
        groups::preflight(spec, record, machines, dir)?;
    }
    Ok(())
}
async fn provision_swarm_with_backend(
    host: &impl SwarmHost,
    #[cfg(feature = "remote")] cloud: &impl crate::Cloud,
    mut request: UpRequest<'_>,
) -> Result<Vec<HostReport>> {
    let spec = request.spec;
    check_bucket_support(spec)?;
    let swarm_dir = request.registry.swarm_dir(&spec.name)?;
    plan::check_layout(spec)?;
    plan::check_group_storage(spec)?;
    for source in &request.sources {
        plan::check_source(source)?;
    }
    let mut sources = std::mem::take(&mut request.sources);
    let mut record = request
        .registry
        .read_record(&spec.name)?
        .unwrap_or_else(|| SwarmRecord {
            bucket_keys: None,
            name: spec.name.clone(),
            created_at: jiff::Timestamp::now(),
            api: SwarmApi::default(),
            hosts: BTreeMap::new(),
            tofu_state: None,
            tofu_bucket: None,
            tofu_bucket_endpoint: None,
            tofu_bucket_region: None,
        });
    preflight_machine_groups(&request, &record, &swarm_dir)?;
    prepare_bucket(
        #[cfg(feature = "remote")]
        cloud,
        &mut request,
        &mut record,
    )
    .await?;
    // Validate service credentials before any OpenTofu apply spends money.
    let primary_done = record
        .hosts
        .get(&spec.placement.stack)
        .and_then(|hosts| hosts.first())
        .is_some_and(|h| h.provisioned);
    let _ = service_options(
        &request,
        &record,
        primary_done,
        bucket_spec(spec, &record).as_ref(),
    )?;
    let applied = if groups::has_module_groups(spec) {
        let machines = request.machines.as_ref().ok_or_else(|| {
            Error::other(format!(
                "swarm {} has OpenTofu groups; this caller passed no OpenTofu runner",
                spec.name
            ))
        })?;
        let applied = groups::apply(machines, request.registry, spec, &mut record).await?;
        sources.extend(applied.sources.clone());
        Some(applied)
    } else {
        None
    };
    let bucket = applied
        .and_then(|a| a.bucket)
        .or_else(|| bucket_spec(spec, &record));
    let planned = plan(spec, sources)?;
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
    let options = service_options(&request, &record, primary_done, bucket.as_ref())?;
    create_private_dir(&swarm_dir.join("keys"))?;
    let primary_node = node(primary, user(spec), bucket.as_ref());
    let reports = provision_planned(
        host,
        &request,
        &mut record,
        &planned,
        &primary_node,
        options.as_ref(),
        bucket.as_ref(),
    )
    .await?;
    if request.registry.current()?.is_none() {
        request.registry.set_current(&spec.name)?;
    }
    Ok(reports)
}
async fn provision_planned(
    host: &impl SwarmHost,
    request: &UpRequest<'_>,
    record: &mut SwarmRecord,
    planned: &[Planned],
    primary_node: &Machine,
    options: Option<&services::Options>,
    bucket: Option<&swarmy_config::BucketSpec>,
) -> Result<Vec<HostReport>> {
    let spec = request.spec;
    let mut reports = Vec::new();
    for p in planned {
        if recorded(record, p).is_some_and(|h| h.provisioned) {
            cloud_out!("{}: already provisioned", p.host.name);
            reports.push(HostReport {
                provisioned: true,
                ..report(p, false)
            });
            continue;
        }
        let result = provision_one(host, request, record, p, primary_node, options, bucket).await;
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
    Ok(reports)
}
#[cfg(feature = "remote")]
async fn prepare_bucket(
    cloud: &impl crate::Cloud,
    request: &mut UpRequest<'_>,
    record: &mut SwarmRecord,
) -> Result<()> {
    validate_bucket_keys(request, record)?;
    let spec = request.spec;
    if spec.bucket.is_some() {
        verify_bucket_with(cloud, spec, record).await?;
        request.registry.write_record(record)?;
        ensure_bucket_with(cloud, spec, record).await?;
    }
    Ok(())
}
#[cfg(not(feature = "remote"))]
fn prepare_bucket(
    request: &mut UpRequest<'_>,
    record: &mut SwarmRecord,
) -> impl Future<Output = Result<()>> {
    std::future::ready(validate_bucket_keys(request, record))
}
fn validate_bucket_keys(request: &mut UpRequest<'_>, record: &mut SwarmRecord) -> Result<()> {
    let spec = request.spec;
    let provisioned = record.hosts.values().flatten().any(|host| host.provisioned);
    if let Some(keys) = request.bucket_keys.take() {
        if spec.bucket.is_none() {
            return Err(Error::other(format!(
                "swarm {} has no [bucket]; S3 keys apply only to a swarm whose spec names a bucket",
                spec.name
            )));
        }
        if provisioned && record.bucket_keys.as_ref() != Some(&keys) {
            return Err(Error::other(format!(
                "swarm {} is provisioned; its hosts keep the bucket keys they were provisioned with, and swarmy cannot rotate them in place yet",
                spec.name
            )));
        }
        record.bucket_keys = Some(keys);
    }
    if provisioned && spec.bucket.is_some() != record.bucket_keys.is_some() {
        return Err(Error::other(format!(
            "swarm {}: [bucket] changed after its hosts were provisioned; restore it, or run swarmy swarm down {} --destroy first",
            spec.name, spec.name
        )));
    }
    if let Some(bucket) = &spec.bucket
        && record.bucket_keys.is_none()
    {
        return Err(Error::other(format!(
            "swarm {} uses bucket {}: pass --s3-access-key KEY with --s3-secret-stdin or --s3-secret-file FILE on its first swarmy swarm up",
            spec.name, bucket.name
        )));
    }
    Ok(())
}
fn service_options(
    request: &UpRequest<'_>,
    record: &SwarmRecord,
    primary_done: bool,
    bucket: Option<&swarmy_config::BucketSpec>,
) -> Result<Option<services::Options>> {
    let spec = request.spec;
    Ok(if primary_done {
        None
    } else {
        let token = record.api.token.clone().unwrap_or_else(|| {
            if request.settings.api.token.is_empty() {
                ulid::Ulid::generate().to_string()
            } else {
                request.settings.api.token.clone()
            }
        });
        let mut settings = request.settings.clone();
        if let Some(bucket) = bucket {
            bucket.apply_to_settings(&mut settings);
        }
        settings.api = swarmy_config::ApiSettings {
            token,
            ..Default::default()
        };
        let options = services::Options::new(&settings, true, user(spec), request.copy_credential)?;
        Some(if request.recipe.is_some() {
            options.with_default_image(&format!("base-ubuntu:{}", spec.name))?
        } else {
            options
        })
    })
}
fn recorded<'a>(record: &'a SwarmRecord, p: &Planned) -> Option<&'a RecordedHost> {
    record
        .hosts
        .values()
        .flatten()
        .find(|h| h.name == p.host.name)
}
async fn provision_one(
    host: &impl SwarmHost,
    request: &UpRequest<'_>,
    record: &mut SwarmRecord,
    p: &Planned,
    primary: &Machine,
    options: Option<&services::Options>,
    bucket: Option<&swarmy_config::BucketSpec>,
) -> Result<()> {
    let registry = request.registry;
    let spec = request.spec;
    let hosts = record.hosts.entry(p.group.clone()).or_default();
    if let Some(old) = hosts.iter_mut().find(|h| h.name == p.host.name) {
        *old = p.host.clone();
    } else {
        hosts.push(p.host.clone());
    }
    registry.write_record(record)?;
    let machine = node(p, user(spec), bucket);
    if p.created {
        host.wait_ready(&machine).await?;
    }
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
    record
        .hosts
        .get_mut(&p.group)
        .expect("group inserted")
        .iter_mut()
        .find(|h| h.name == p.host.name)
        .expect("host inserted")
        .provisioned = true;
    registry.write_record(record)?;
    Ok(())
}
// The record is the teardown inventory. A changed or broken spec must not hide a host.
// Keep the stack host first for start and last for stop/down, with all nodes in
// reverse record order for teardown.
fn record_plan(record: &SwarmRecord, stack: &str) -> Vec<Planned> {
    let mut planned = Vec::new();
    let mut add_group = |group: &str, hosts: &[RecordedHost]| {
        for (index, host) in hosts.iter().enumerate() {
            planned.push(Planned {
                group: group.to_owned(),
                index,
                host: host.clone(),
                copy_key_from: None,
                created: false,
                role: if group == stack && index == 0 {
                    Role::Stack { services: true }
                } else {
                    Role::Node
                },
                sandboxes: 0,
                local_storage: String::new(),
            });
        }
    };
    if let Some(hosts) = record.hosts.get(stack) {
        add_group(stack, hosts);
    }
    for (group, hosts) in &record.hosts {
        if group != stack {
            add_group(group, hosts);
        }
    }
    planned
}
fn effective_bucket(
    registry: &Registry,
    spec: &SwarmSpec,
    record: &SwarmRecord,
    credentials: Option<&swarmy_config::cloud::CloudCredentials>,
) -> Result<Option<swarmy_config::BucketSpec>> {
    if spec.bucket.is_none() && record.tofu_bucket.is_some() {
        let loaded;
        let credentials = if let Some(credentials) = credentials {
            credentials
        } else {
            loaded = swarmy_config::cloud::CloudCredentials::load(
                &swarmy_config::cloud::CloudCredentials::path(registry.dirs()),
            )?;
            &loaded
        };
        Ok(groups::bucket_from(record, credentials))
    } else {
        Ok(bucket_spec(spec, record))
    }
}
fn recorded_plan(registry: &Registry, spec: &SwarmSpec) -> Result<(SwarmRecord, Vec<Planned>)> {
    let record = registry.read_record(&spec.name)?.ok_or_else(|| {
        Error::other(format!(
            "swarm {} has no hosts recorded; run swarmy swarm up {}",
            spec.name, spec.name
        ))
    })?;
    let planned = record_plan(&record, &spec.placement.stack);
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
    let bucket = effective_bucket(registry, spec, &record, None)?;
    let mut reports = Vec::new();
    for p in planned.iter().rev() {
        if recorded(&record, p).is_some_and(|h| h.provisioned) {
            host.stop(&node(p, user(spec), bucket.as_ref()))
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
    let bucket = effective_bucket(registry, spec, &record, None)?;
    let mut reports = Vec::new();
    for p in &planned {
        if recorded(&record, p).is_some_and(|h| h.provisioned) {
            host.start(&node(p, user(spec), bucket.as_ref()))
                .await
                .map_err(|e| Error::context(e, format!("host {}", p.host.name)))?;
            reports.push(report(p, true));
        }
    }
    Ok(reports)
}
/// Remove swarmy from existing hosts and destroy created machine groups.
/// Returns existing-host reports and the number of groups with destroyed state.
/// The spec remains in place.
/// # Errors
/// Reports host failures and retains them in the record.
pub async fn decommission_swarm(
    registry: &Registry,
    spec: &SwarmSpec,
    machines: Option<groups::MachineGroups<'_>>,
) -> Result<(Vec<HostReport>, usize)> {
    decommission_swarm_with(&ssh::Ssh::teardown(), registry, spec, machines).await
}
async fn decommission_swarm_with(
    host: &impl SwarmHost,
    registry: &Registry,
    spec: &SwarmSpec,
    machines: Option<groups::MachineGroups<'_>>,
) -> Result<(Vec<HostReport>, usize)> {
    check_bucket_support(spec)?;
    let _lock = Locked(lock(registry, &spec.name)?);
    let Some(mut record) = registry.read_record(&spec.name)? else {
        cloud_out!(
            "swarm {} has no hosts recorded; nothing to remove",
            spec.name
        );
        return Ok((Vec::new(), 0));
    };
    let dir = registry.swarm_dir(&spec.name)?;
    let planned: Vec<_> = record_plan(&record, &spec.placement.stack)
        .into_iter()
        .filter(|p| {
            !dir.join("tofu")
                .join(&p.group)
                .join("terraform.tfstate")
                .is_file()
        })
        .collect();
    let bucket = effective_bucket(
        registry,
        spec,
        &record,
        machines.as_ref().map(|m| m.credentials),
    )?;
    let mut reports = Vec::new();
    let mut failures = Vec::new();
    let mut failed = BTreeMap::<String, Vec<RecordedHost>>::new();
    for p in planned.iter().rev() {
        match host
            .decommission(&node(p, user(spec), bucket.as_ref()))
            .await
        {
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
        record
            .hosts
            .retain(|group, _| dir.join("tofu").join(group).is_dir());
        record.hosts.extend(failed);
        registry.write_record(&record)?;
        return Err(Error::other(format!(
            "swarm down {} incomplete for {} host(s) ({}); fix them, then run swarmy swarm down {} --destroy again",
            spec.name,
            failures.len(),
            failures.join("; "),
            spec.name
        )));
    }
    delete_bucket(spec, &record).await?;
    let destroyed = groups::destroy(machines.as_ref(), registry, spec, &mut record).await?;
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
    if destroyed > 0 {
        cloud_out!("destroyed the machines of {destroyed} OpenTofu group(s)");
    }
    if !reports.is_empty() || destroyed == 0 {
        cloud_out!(
            "removed swarmy from {} existing host(s) of swarm {}; those machines keep running",
            reports.len(),
            spec.name
        );
    }
    cloud_out!(
        "the spec stays at {}; swarmy swarm rm {} forgets it",
        registry.spec_path(&spec.name)?.display(),
        spec.name
    );
    Ok((reports, destroyed))
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
    let bucket = effective_bucket(registry, spec, &record, None)?;
    let mut hosts = Vec::new();
    for p in &planned {
        hosts.push(HostStatus {
            name: p.host.name.clone(),
            group: p.group.clone(),
            role: p.role.label(),
            address: p.host.public_address.clone(),
            provisioned: p.host.provisioned,
            ssh: host.reachable(&node(p, user(spec), bucket.as_ref())).await,
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

/// Host upgrade policy and binary cache.
pub struct UpgradeOptions {
    pub allow_dirty: bool,
    pub services_only: bool,
    pub drain_timeout: Duration,
    pub json: bool,
    pub cache: PathBuf,
}
/// Upgrade provisioned nodes before the primary host.
/// # Errors
/// Reports an incomplete record, dirty checkout, or host failure.
pub async fn upgrade_swarm(
    registry: &Registry,
    spec: &SwarmSpec,
    options: UpgradeOptions,
) -> Result<Vec<Summary>> {
    upgrade_swarm_with(registry, spec, options, |_| Ok(())).await
}
/// Upgrade hosts and report each completed summary before starting the next host.
/// # Errors
/// Reports a host, checkout, or callback failure.
pub async fn upgrade_swarm_with(
    registry: &Registry,
    spec: &SwarmSpec,
    options: UpgradeOptions,
    on_summary: impl FnMut(&Summary) -> Result<()>,
) -> Result<Vec<Summary>> {
    let _lock = Locked(lock(registry, &spec.name)?);
    let (record, _) = recorded_plan(registry, spec)?;
    let host = ssh::Ssh::for_client(&options.cache)?;
    let mut policy = upgrade::Options::new(
        options.allow_dirty,
        options.services_only,
        options.drain_timeout.as_secs(),
        options.json,
    );
    policy.dirty_paths = host.checkout_changes()?;
    let bucket = effective_bucket(registry, spec, &record, None)?;
    let machines = upgrade_machines(&record, spec, bucket.as_ref());
    upgrade::run_with(&host, &machines, policy, on_summary).await
}

fn upgrade_machines(
    record: &SwarmRecord,
    spec: &SwarmSpec,
    bucket: Option<&swarmy_config::BucketSpec>,
) -> Vec<Machine> {
    record_plan(record, &spec.placement.stack)
        .iter()
        .rev()
        .filter(|p| recorded(record, p).is_some_and(|h| h.provisioned))
        .map(|p| node(p, user(spec), bucket))
        .collect()
}

/// Follow one service journal on a named host or the primary host.
/// # Errors
/// Rejects unknown units or hosts and propagates SSH failures.
pub async fn logs(
    registry: &Registry,
    spec: &SwarmSpec,
    host: Option<&str>,
    unit: &str,
) -> Result<()> {
    const UNITS: &[&str] = &[
        "swarmyd",
        "swarmy-stack",
        "swarmy-tunnel",
        "swarmy-api",
        "swarmy-scheduler",
        "swarmy-worker",
        "swarmy-gateway",
    ];
    Error::ensure(
        UNITS.contains(&unit),
        "--unit must be one of swarmyd, swarmy-stack, swarmy-tunnel, swarmy-api, swarmy-scheduler, swarmy-worker, swarmy-gateway",
    )?;
    let (record, planned) = recorded_plan(registry, spec)?;
    let selected = if let Some(host) = host {
        planned.iter().find(|p| p.host.name == host)
    } else {
        planned.first()
    }
    .ok_or_else(|| {
        Error::other(format!(
            "no host {} in swarm {}",
            host.unwrap_or("primary"),
            spec.name
        ))
    })?;
    Error::ensure(
        recorded(&record, selected).is_some_and(|h| h.provisioned),
        "host is not provisioned",
    )?;
    let bucket = effective_bucket(registry, spec, &record, None)?;
    let machine = node(selected, user(spec), bucket.as_ref());
    let address = ssh::reachable_address(&machine).await?;
    let command = "host journalctl".to_owned();
    let mut child = ssh::node_command(
        &machine,
        &address,
        &format!("journalctl --unit {unit} --follow --no-pager --lines 100"),
    )?
    .spawn()
    .map_err(Error::ssh(&command))?;
    tokio::select! {
        result = child.wait() => {
            let status = result.map_err(Error::ssh(&command))?;
            if !status.success() { return Err(Error::SshStatus { command, status }); }
        }
        result = tokio::signal::ctrl_c() => {
            result?;
            child.kill().await.map_err(Error::ssh(&command))?;
        }
    }
    Ok(())
}

#[cfg(not(feature = "remote"))]
const NO_S3: &str = "this swarmy was built without the S3 client; install it with make install-client (cargo --features remote) to use swarms with a bucket";
#[cfg(not(feature = "remote"))]
fn check_bucket_support(spec: &SwarmSpec) -> Result<()> {
    Error::ensure(spec.bucket.is_none(), NO_S3)
}
#[cfg(feature = "remote")]
fn check_bucket_support(spec: &SwarmSpec) -> Result<()> {
    if let Some(bucket) = &spec.bucket {
        bucket.validate()?;
    }
    Ok(())
}
#[cfg(feature = "remote")]
async fn verify_bucket_with(
    cloud: &impl crate::Cloud,
    spec: &SwarmSpec,
    record: &SwarmRecord,
) -> Result<()> {
    let bucket = bucket_spec(spec, record).expect("bucket and keys checked");
    cloud
        .verify_bucket_access(&crate::ObjectBucket::from_spec(
            &spec.name,
            &bucket,
            &bucket.region,
        ))
        .await
        .map_err(|error| {
            Error::context(
                error,
                format!(
                    "static bucket keys for {} were rejected; check --s3-access-key and the secret",
                    bucket.describe()
                ),
            )
        })
}
#[cfg(feature = "remote")]
async fn ensure_bucket_with(
    cloud: &impl crate::Cloud,
    spec: &SwarmSpec,
    record: &SwarmRecord,
) -> Result<()> {
    let bucket = bucket_spec(spec, record).expect("bucket and keys checked");
    cloud
        .ensure_bucket(&crate::ObjectBucket::from_spec(
            &spec.name,
            &bucket,
            &bucket.region,
        ))
        .await
}
#[cfg(feature = "remote")]
async fn delete_bucket(spec: &SwarmSpec, record: &SwarmRecord) -> Result<()> {
    delete_bucket_with(&crate::buckets::S3Buckets, spec, record).await
}
#[cfg(feature = "remote")]
async fn delete_bucket_with(
    cloud: &impl crate::Cloud,
    spec: &SwarmSpec,
    record: &SwarmRecord,
) -> Result<()> {
    use crate::Ownership;
    if spec.bucket.is_some() {
        let bucket = bucket_spec(spec, record)
            .ok_or_else(|| Error::other("bucket keys missing from swarm record"))?;
        let bucket = crate::ObjectBucket::from_spec(&spec.name, &bucket, &bucket.region);
        if cloud.bucket_ownership(&bucket).await? == Ownership::Owned {
            let outcome = cloud.delete_bucket(&bucket).await?;
            cloud_out!("bucket {}: {outcome:?}", bucket.spec.bucket);
        } else {
            cloud_out!(
                "bucket {}: left in place (swarmy did not create it for swarm {})",
                bucket.spec.bucket,
                spec.name
            );
        }
    }
    Ok(())
}
#[cfg(not(feature = "remote"))]
fn delete_bucket(spec: &SwarmSpec, _record: &SwarmRecord) -> impl Future<Output = Result<()>> {
    std::future::ready(check_bucket_support(spec))
}
