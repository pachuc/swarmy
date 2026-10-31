//! Machine groups created by `OpenTofu`: render each group's working directory and variables, apply it in placement order, and turn its outputs into hosts for the shared provisioning path.
//! Credentials reach `OpenTofu` only through its environment; down destroys the groups in reverse order.
#[cfg(test)]
mod tests;
use super::{HostSource, Locked, lock, plan};
use crate::{Error, Result, ssh};
use serde_json::{Map, Value, json};
use std::{
    collections::BTreeMap,
    fs,
    net::IpAddr,
    path::{Path, PathBuf},
};
use swarmy_config::{
    BucketCredentials, BucketSpec, ObjectPrefix,
    cloud::{CloudCredentials, CloudProvider},
    create_private_dir,
    swarm::{GroupSpec, Module, RecordedHost, Registry, SwarmApi, SwarmRecord, SwarmSpec},
};
use swarmy_infra::{
    CredentialEnv, ModuleFile, ModuleOutputs, PlanOutcome, Tofu, VARIABLES_FILE, Workspace,
};

/// Runner and credentials for machine groups.
#[derive(Clone, Copy)]
pub struct MachineGroups<'a> {
    pub tofu: &'a Tofu,
    pub credentials: &'a CloudCredentials,
}
/// True if any group creates machines.
#[must_use]
pub fn has_module_groups(spec: &SwarmSpec) -> bool {
    spec.groups.values().any(opentofu_group)
}
fn opentofu_group(group: &GroupSpec) -> bool {
    !matches!(group.module, Module::Existing | Module::Local) && !group.elastic
}
/// Module groups in placement order.
#[must_use]
pub fn apply_order(spec: &SwarmSpec) -> Vec<String> {
    let mut order = vec![
        spec.placement.stack.clone(),
        spec.placement.services.clone(),
    ];
    order.extend(spec.placement.nodes.iter().cloned());
    let mut result = Vec::new();
    for name in order {
        if !result.contains(&name) && spec.groups.get(&name).is_some_and(opentofu_group) {
            result.push(name);
        }
    }
    result
}
/// The network a group joins, from the first earlier applied group of its module.
/// Elastic groups are not in apply order, so they join the first applied group.
#[must_use]
pub fn joined_network<'a>(spec: &SwarmSpec, record: &'a SwarmRecord, group: &str) -> &'a str {
    let module = &spec.groups[group].module;
    apply_order(spec)
        .into_iter()
        .take_while(|name| name != group)
        .filter(|name| spec.groups[name].module == *module)
        .find_map(|name| record.tofu_networks.get(&name))
        .map_or("", String::as_str)
}
#[derive(Debug, serde::Serialize)]
pub struct GroupPlan {
    pub group: String,
    pub module: String,
    pub changes: bool,
    pub plan: String,
}
pub(crate) struct Applied {
    pub(crate) sources: Vec<HostSource>,
}
fn root(dir: &Path) -> PathBuf {
    dir.join("tofu")
}
/// Working directory for one machine group.
#[must_use]
pub fn group_dir(dir: &Path, group: &str) -> PathBuf {
    root(dir).join(group)
}
fn module_name(module: &Module) -> String {
    module.clone().into()
}
fn creates_bucket(spec: &SwarmSpec) -> bool {
    spec.bucket.is_none()
        && spec
            .groups
            .get(&spec.placement.stack)
            .is_some_and(|group| group.module == Module::HetznerCloud)
}
fn bucket_name(spec: &SwarmSpec, record: &mut SwarmRecord) -> Option<String> {
    creates_bucket(spec).then(|| {
        record
            .tofu_bucket
            .get_or_insert_with(|| {
                format!(
                    "swarmy-{}-{:012x}",
                    spec.name,
                    ulid::Ulid::generate().random() & 0xffff_ffff_ffff
                )
            })
            .clone()
    })
}
fn provider_env(
    credentials: &CloudCredentials,
    providers: &[CloudProvider],
    required: bool,
) -> Result<CredentialEnv> {
    let mut env = CredentialEnv::closed();
    for provider in providers {
        let credential = credentials.get(*provider);
        if required && credential.is_none() {
            return Err(swarmy_config::Error::CloudNotLoggedIn(provider.name()).into());
        }
        if let Some(credential) = credential {
            let vars = credential.env();
            env = vars
                .secrets()
                .fold(env, |env, (name, value)| env.with(name, value));
            env = vars
                .settings()
                .fold(env, |env, (name, value)| env.with_setting(name, value));
        }
    }
    Ok(env)
}
/// Environment variables required by a module, excluding secret values from files.
/// # Errors
/// Returns `CloudNotLoggedIn` when a required provider is absent.
pub fn module_credential_env(
    credentials: &CloudCredentials,
    module: &Module,
    bucket: bool,
) -> Result<CredentialEnv> {
    let providers: Vec<_> = match module {
        // Custom modules receive all stored providers: their provider needs are unknown.
        Module::Path(_) => CloudProvider::ALL.to_vec(),
        module => {
            let mut providers: Vec<_> = module.machine_provider().into_iter().collect();
            if *module == Module::HetznerCloud
                && (bucket || credentials.get(CloudProvider::HetznerS3).is_some())
            {
                providers.push(CloudProvider::HetznerS3);
            }
            providers
        }
    };
    provider_env(credentials, &providers, !matches!(module, Module::Path(_)))
}
fn network_zone(location: &str) -> Option<&'static str> {
    match location {
        "fsn1" | "nbg1" | "hel1" => Some("eu-central"),
        "ash" => Some("us-east"),
        "hil" => Some("us-west"),
        "sin" => Some("ap-southeast"),
        _ => None,
    }
}
fn compatible_network(a: &GroupSpec, b: &GroupSpec) -> bool {
    if a.location == b.location {
        return true;
    }
    if a.module != Module::HetznerCloud {
        return false;
    }
    match (
        a.location.as_deref().and_then(network_zone),
        b.location.as_deref().and_then(network_zone),
    ) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}
/// Check the credentials required by module groups before resolving `OpenTofu`.
/// # Errors
/// Reports missing provider logins without exposing credential values.
pub fn check_credentials(spec: &SwarmSpec, credentials: &CloudCredentials) -> Result<()> {
    for name in apply_order(spec) {
        let group = &spec.groups[&name];
        let bucket = name == spec.placement.stack && creates_bucket(spec);
        let _ = module_credential_env(credentials, &group.module, bucket)?;
    }
    Ok(())
}
pub(crate) fn preflight(
    spec: &SwarmSpec,
    record: &SwarmRecord,
    machines: &MachineGroups<'_>,
    dir: &Path,
) -> Result<()> {
    check_credentials(spec, machines.credentials)?;
    let elastic = spec
        .groups
        .iter()
        .filter(|(_, group)| group.elastic)
        .map(|(name, _)| name.clone());
    let mut seen: Vec<String> = Vec::new();
    for name in apply_order(spec).into_iter().chain(elastic) {
        let group = &spec.groups[&name];
        if let Some(first) = seen.iter().find(|first| {
            let other = &spec.groups[*first];
            other.module == group.module && !compatible_network(other, group)
        }) {
            return Err(Error::other(format!(
                "groups {first} and {name}: {} networks cannot span locations {} and {}; use groups in the same network zone or region",
                module_name(&group.module),
                spec.groups[first].location.as_deref().unwrap_or("<unset>"),
                group.location.as_deref().unwrap_or("<unset>"),
            )));
        }
        seen.push(name);
    }
    for name in apply_order(spec) {
        let group = &spec.groups[&name];
        if !swarmy_config::is_opentofu_name(&spec.name) {
            return Err(Error::other(format!(
                "swarm \"{}\": OpenTofu groups need a swarm name of 1-20 lowercase letters, digits and hyphens that starts with a letter and does not end with a hyphen",
                spec.name
            )));
        }
        if !swarmy_config::is_opentofu_name(&name) {
            return Err(Error::other(format!(
                "group \"{name}\": OpenTofu groups need a group name of 1-20 lowercase letters, digits and hyphens that starts with a letter and does not end with a hyphen"
            )));
        }
        if let Module::Path(path) = &group.module
            && !path.is_absolute()
        {
            return Err(Error::other(format!(
                "group \"{name}\": module path {} must be absolute",
                path.display()
            )));
        }
        let _ = module_files(&name, group)?;
        let previous = record.hosts.get(&name).map_or(0, Vec::len);
        let count = group.count.unwrap_or_default();
        if previous > count as usize {
            return Err(Error::other(format!(
                "group \"{name}\" has {previous} machines recorded but count = {count}; lowering count in the spec would destroy machines without draining them; restore count = {previous}, then run swarmy swarm scale {} {name} {count} (node groups only)",
                spec.name
            )));
        }
        if previous > 0 {
            check_immutable_fields(spec, &name, group, dir)?;
        }
    }
    if record.tofu_bucket.is_some()
        && record.hosts.values().flatten().any(|h| h.provisioned)
        && !creates_bucket(spec)
    {
        return Err(Error::other(format!(
            "swarm {}: OpenTofu bucket changed after its hosts were provisioned",
            spec.name
        )));
    }
    Ok(())
}
fn check_immutable_fields(
    spec: &SwarmSpec,
    name: &str,
    group: &GroupSpec,
    dir: &Path,
) -> Result<()> {
    let Some(last) = last_variables(&group_dir(dir, name)) else {
        return Ok(());
    };
    for (field, current) in [
        ("size", &group.size),
        ("location", &group.location),
        ("image", &group.image),
    ] {
        let old = last.get(field).and_then(Value::as_str);
        if old != current.as_deref() {
            return Err(Error::other(format!(
                "group \"{name}\": {field} changed from {} to {} since its machines were created; that would rebuild them. Restore it, or run swarmy swarm down {} first",
                old.unwrap_or("<unset>"),
                current.as_deref().unwrap_or("<unset>"),
                spec.name
            )));
        }
    }
    Ok(())
}
fn last_variables(dir: &Path) -> Option<Map<String, Value>> {
    fs::read(dir.join(VARIABLES_FILE))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
}
fn group_keys_dir(dir: &Path) -> PathBuf {
    dir.join("keys/groups")
}
/// Path of the group SSH identity on the primary.
#[must_use]
pub fn group_key_path(dir: &Path, group: &str) -> PathBuf {
    group_keys_dir(dir).join(group)
}
/// Return or generate the group SSH public key.
/// # Errors
/// Returns filesystem or key-generation failures.
pub async fn group_key(dir: &Path, swarm: &str, group: &str) -> Result<String> {
    create_private_dir(&group_keys_dir(dir))?;
    let key = group_key_path(dir, group);
    let public = if key.is_file() {
        fs::read(key.with_extension("pub"))?
    } else {
        ssh::generate_key(&key, &format!("swarmy-{swarm}-{group}")).await?
    };
    Ok(String::from_utf8(public)?.trim().to_owned())
}
pub struct GroupInputs<'a> {
    pub swarm: &'a str,
    pub group: &'a str,
    pub machine_count: u32,
    pub size: Option<&'a str>,
    pub location: Option<&'a str>,
    pub image: Option<&'a str>,
    pub ssh_public_key: &'a str,
    pub bucket: &'a str,
    pub network: &'a str,
}
/// Module variables shared by the client and the primary's provisioner.
#[must_use]
pub fn group_variables(inputs: &GroupInputs<'_>) -> Map<String, Value> {
    let mut vars = Map::new();
    for (k, v) in [
        ("swarm", json!(inputs.swarm)),
        ("group", json!(inputs.group)),
        ("machine_count", json!(inputs.machine_count)),
        ("ssh_public_key", json!(inputs.ssh_public_key)),
        ("ssh_allowed_cidrs", json!(["0.0.0.0/0", "::/0"])),
        ("bucket_name", json!(inputs.bucket)),
        ("bucket_force_destroy", json!(false)),
        ("network_id", json!(inputs.network)),
    ] {
        vars.insert(k.into(), v);
    }
    if let Some(size) = inputs.size {
        vars.insert("size".into(), json!(size));
    }
    if let Some(location) = inputs.location {
        vars.insert("location".into(), json!(location));
    }
    if let Some(image) = inputs.image {
        vars.insert("image".into(), json!(image));
    }
    vars
}

struct ModuleSet {
    files: Vec<(String, Vec<u8>)>,
}
impl ModuleSet {
    fn borrowed(&self) -> Vec<ModuleFile<'_>> {
        self.files
            .iter()
            .map(|(path, contents)| ModuleFile { path, contents })
            .collect()
    }
}
fn module_files(name: &str, group: &GroupSpec) -> Result<ModuleSet> {
    if let Module::Path(path) = &group.module {
        let mut files = Vec::new();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let filename = entry.file_name().to_string_lossy().into_owned();
            if entry.file_type()?.is_file()
                && (Path::new(&filename).extension().is_some_and(|e| e == "tf")
                    || filename.ends_with(".tf.json")
                    || filename == ".terraform.lock.hcl")
            {
                files.push((filename, fs::read(entry.path())?));
            }
        }
        files.sort_by(|a, b| a.0.cmp(&b.0));
        if !files.iter().any(|(name, _)| {
            Path::new(name).extension().is_some_and(|e| e == "tf") || name.ends_with(".tf.json")
        }) {
            return Err(Error::other(format!(
                "group \"{name}\": {} has no .tf files",
                path.display()
            )));
        }
        Ok(ModuleSet { files })
    } else {
        let module = swarmy_infra::modules::find(&module_name(&group.module))
            .ok_or_else(|| Error::other("unknown module"))?;
        Ok(ModuleSet {
            files: module
                .files
                .iter()
                .map(|f| (f.path.into(), f.contents.to_vec()))
                .collect(),
        })
    }
}
fn hosts_from(
    outputs: &ModuleOutputs,
    group: &str,
    swarm: &str,
    key: &Path,
    previous: &[RecordedHost],
) -> Result<Vec<RecordedHost>> {
    for old in previous.iter().filter(|host| host.provisioned) {
        if !outputs
            .machines
            .iter()
            .any(|machine| machine.name == old.name)
        {
            return Err(Error::other(format!(
                "group {group}: provisioned host {} disappeared from OpenTofu outputs; run swarmy swarm down {} before changing the machines",
                old.name, swarm
            )));
        }
    }
    outputs
        .machines
        .iter()
        .map(|m| {
            let mut host = recorded_host(group, m, key)?;
            host.provisioned = previous.iter().any(|h| h.name == m.name && h.provisioned);
            Ok(host)
        })
        .collect()
}
/// One `OpenTofu` machine as a host record, not yet provisioned. Swarmy provisions over IPv4.
/// # Errors
/// Missing public IPv4 or non-IPv4 private address.
pub fn recorded_host(group: &str, m: &swarmy_infra::Machine, key: &Path) -> Result<RecordedHost> {
    let public = m.public_ipv4.ok_or_else(|| {
        Error::other(format!(
            "group {group}: machine {} has no public IPv4 address; swarmy provisions over IPv4",
            m.name
        ))
    })?;
    let private_address = match m.private_ip {
        Some(IpAddr::V4(ip)) => Some(ip.to_string()),
        Some(address) => {
            return Err(Error::other(format!(
                "group {group}: machine {}: private address {address} must be IPv4",
                m.name
            )));
        }
        None => None,
    };
    Ok(RecordedHost {
        name: m.name.clone(),
        public_address: public.to_string(),
        private_address,
        ssh_user: m.ssh_user.clone(),
        ssh_key: key.into(),
        provisioned: false,
    })
}
pub(crate) fn bucket_from(
    record: &SwarmRecord,
    credentials: &CloudCredentials,
) -> Option<BucketSpec> {
    let keys = credentials.get(CloudProvider::HetznerS3)?;
    let swarmy_config::cloud::CloudCredential::HetznerS3(keys) = keys else {
        return None;
    };
    Some(BucketSpec {
        bucket: record.tofu_bucket.clone()?,
        endpoint: record.tofu_bucket_endpoint.clone()?,
        region: record.tofu_bucket_region.clone()?,
        prefix: ObjectPrefix::default(),
        conditional_create: true,
        credentials: BucketCredentials::StaticKeys {
            access_key: keys.access_key,
            secret_key: keys.secret_key,
        },
    })
}
async fn prepare_group<'a>(
    machines: MachineGroups<'a>,
    spec: &SwarmSpec,
    dir: &Path,
    name: &str,
    key: &str,
    bucket: Option<&str>,
    network: &str,
) -> Result<Workspace<'a>> {
    let group = &spec.groups[name];
    let group_bucket = if name == spec.placement.stack {
        bucket.unwrap_or("")
    } else {
        ""
    };
    let vars = group_variables(&GroupInputs {
        swarm: &spec.name,
        group: name,
        machine_count: group.count.unwrap_or_default(),
        size: group.size.as_deref(),
        location: group.location.as_deref(),
        image: group.image.as_deref(),
        ssh_public_key: key,
        bucket: group_bucket,
        network,
    });
    let env = module_credential_env(
        machines.credentials,
        &group.module,
        !group_bucket.is_empty(),
    )?;
    let workspace = machines.tofu.workspace(group_dir(dir, name), env);
    let module = module_files(name, group)?;
    workspace.prepare(&module.borrowed(), &vars).await?;
    workspace.init().await?;
    Ok(workspace)
}
/// Apply groups in placement order and record each output before provisioning hosts.
/// # Errors
/// Returns validation, `OpenTofu`, output and filesystem failures.
pub(crate) async fn apply(
    machines: &MachineGroups<'_>,
    registry: &Registry,
    spec: &SwarmSpec,
    record: &mut SwarmRecord,
) -> Result<Applied> {
    let dir = registry.swarm_dir(&spec.name)?;
    preflight(spec, record, machines, &dir)?;
    let order = apply_order(spec);
    // Generate every group's key before the first apply can create a machine.
    let mut keys = BTreeMap::new();
    for name in &order {
        keys.insert(name.clone(), group_key(&dir, &spec.name, name).await?);
    }
    let bucket = bucket_name(spec, record);
    if bucket.is_none() {
        record.tofu_bucket = None;
        record.tofu_bucket_endpoint = None;
        record.tofu_bucket_region = None;
    }
    let root = root(&dir);
    create_private_dir(&root)?;
    record.tofu_state = Some(root);
    registry.write_record(record)?;
    let mut sources = Vec::new();
    for name in order {
        let group = &spec.groups[&name];
        cloud_out!(
            "OpenTofu: applying group {name} ({})",
            module_name(&group.module)
        );
        let network = joined_network(spec, record, &name).to_owned();
        let workspace = prepare_group(
            *machines,
            spec,
            &dir,
            &name,
            &keys[&name],
            bucket.as_deref(),
            &network,
        )
        .await?;
        workspace.apply().await?;
        let mut outputs = workspace.outputs().await?;
        // Scaleway machines have no private network, so their private address is always null.
        if matches!(group.module, Module::HetznerCloud | Module::Aws)
            && outputs.machines.iter().any(|m| m.private_ip.is_none())
        {
            workspace.apply().await?;
            outputs = workspace.outputs().await?;
            if let Some(m) = outputs.machines.iter().find(|m| m.private_ip.is_none()) {
                return Err(Error::other(format!(
                    "group {name}: machine {} has no private address after apply",
                    m.name
                )));
            }
        }
        match outputs.network_id.clone() {
            Some(id) => record.tofu_networks.insert(name.clone(), id),
            None => record.tofu_networks.remove(&name),
        };
        if name == spec.placement.stack && bucket.is_some() && outputs.bucket.is_none() {
            return Err(Error::other(format!(
                "group {name}: expected a bucket output for the OpenTofu bucket"
            )));
        }
        if name == spec.placement.stack
            && let Some(out) = outputs.bucket.as_ref()
        {
            if let Some(expected) = bucket.as_ref() {
                if out.name != *expected {
                    return Err(Error::other(format!(
                        "group {name}: bucket output name does not match the recorded bucket"
                    )));
                }
                record.tofu_bucket_endpoint = Some(out.endpoint.clone());
                record.tofu_bucket_region = Some(out.region.clone());
            } else {
                cloud_err!(
                    "group {name}: ignoring the bucket output; swarmy creates buckets only for hetzner-cloud stack groups"
                );
            }
        }
        let key = dir.join("keys/groups").join(&name);
        let hosts = hosts_from(
            &outputs,
            &name,
            &spec.name,
            &key,
            record.hosts.get(&name).map_or(&[], Vec::as_slice),
        )?;
        record.hosts.insert(name.clone(), hosts.clone());
        registry.write_record(record)?;
        sources.extend(hosts.into_iter().map(|host| HostSource {
            group: name.clone(),
            host,
            copy_key_from: None,
            created: true,
        }));
    }
    Ok(Applied { sources })
}
/// Plan each group without applying or marking state as created.
/// # Errors
/// Returns validation, `OpenTofu`, output and filesystem failures.
pub async fn plan_groups(
    registry: &Registry,
    spec: &SwarmSpec,
    machines: MachineGroups<'_>,
) -> Result<Vec<GroupPlan>> {
    let _lock = Locked(lock(registry, &spec.name)?);
    plan::check_layout(spec)?;
    plan::check_group_storage(spec)?;
    let dir = registry.swarm_dir(&spec.name)?;
    let mut record = registry
        .read_record(&spec.name)?
        .unwrap_or_else(|| SwarmRecord {
            name: spec.name.clone(),
            created_at: jiff::Timestamp::now(),
            api: SwarmApi::default(),
            bucket_keys: None,
            hosts: BTreeMap::new(),
            tofu_state: None,
            tofu_bucket: None,
            tofu_bucket_endpoint: None,
            tofu_bucket_region: None,
            tofu_networks: BTreeMap::new(),
        });
    preflight(spec, &record, &machines, &dir)?;
    let bucket = bucket_name(spec, &mut record);
    if bucket.is_none() {
        record.tofu_bucket = None;
        record.tofu_bucket_endpoint = None;
        record.tofu_bucket_region = None;
    }
    create_private_dir(&root(&dir))?;
    registry.write_record(&record)?;
    let mut result = Vec::new();
    for name in apply_order(spec) {
        let group = &spec.groups[&name];
        let key = group_key(&dir, &spec.name, &name).await?;
        let workspace = prepare_group(
            machines,
            spec,
            &dir,
            &name,
            &key,
            bucket.as_deref(),
            joined_network(spec, &record, &name),
        )
        .await?;
        let (outcome, text) = workspace.plan_text().await?;
        result.push(GroupPlan {
            group: name.clone(),
            module: module_name(&group.module),
            changes: outcome == PlanOutcome::Changes,
            plan: text,
        });
    }
    Ok(result)
}
fn destroy_env(
    credentials: &CloudCredentials,
    group: Option<&GroupSpec>,
    work: &Path,
) -> Result<CredentialEnv> {
    if let Some(group) = group {
        let bucket = last_variables(work)
            .and_then(|v| {
                v.get("bucket_name")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .is_some_and(|s| !s.is_empty());
        return module_credential_env(credentials, &group.module, bucket);
    }
    provider_env(credentials, &CloudProvider::ALL, false)
}
/// Destroy recorded `OpenTofu` working directories, including groups removed from the spec.
/// # Errors
/// Returns credentials, filesystem or `OpenTofu` failures, preserving unfinished groups.
pub(crate) async fn destroy(
    machines: Option<&MachineGroups<'_>>,
    registry: &Registry,
    spec: &SwarmSpec,
    record: &mut SwarmRecord,
) -> Result<usize> {
    let dir = registry.swarm_dir(&spec.name)?;
    let root = root(&dir);
    if !root.exists() {
        return Ok(0);
    }
    let mut names: Vec<String> = fs::read_dir(&root)?
        .filter_map(std::result::Result::ok)
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    let order = apply_order(spec);
    names.sort_by(|a, b| {
        match (
            order.iter().position(|x| x == a),
            order.iter().position(|x| x == b),
        ) {
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (None, None) => a.cmp(b),
            (Some(a), Some(b)) => b.cmp(&a),
        }
    });
    if names.is_empty() {
        return Ok(0);
    }
    let Some(machines) = machines else {
        return Err(Error::other(format!(
            "swarm {} has OpenTofu groups ({}); swarm down needs OpenTofu and the cloud credentials",
            spec.name,
            names.join(", ")
        )));
    };
    let mut count = 0;
    for name in names {
        let work = group_dir(&dir, &name);
        let had_state = work.join("terraform.tfstate").is_file();
        let attempt: Result<()> = async {
            if had_state {
                let env = destroy_env(machines.credentials, spec.groups.get(&name), &work)?;
                let workspace = machines.tofu.workspace(&work, env);
                let mut vars = last_variables(&work).ok_or_else(|| {
                    Error::other(format!("group {name}: missing last applied variables"))
                })?;
                workspace.init().await?;
                if vars
                    .get("bucket_name")
                    .and_then(Value::as_str)
                    .is_some_and(|s| !s.is_empty())
                {
                    vars.insert("bucket_force_destroy".into(), json!(true));
                    workspace.set_variables(&vars).await?;
                    let bucket_target = match spec.groups.get(&name).map(|g| &g.module) {
                        Some(Module::Aws) => "aws_s3_bucket.this",
                        _ => "minio_s3_bucket.this",
                    };
                    workspace.apply_targets(&[bucket_target]).await?;
                }
                destroy_settling(&workspace).await?;
            }
            fs::remove_dir_all(&work)?;
            record.hosts.remove(&name);
            record.tofu_networks.remove(&name);
            registry.write_record(record)?;
            Ok(())
        }
        .await;
        if let Err(error) = attempt {
            return Err(Error::context(
                error,
                format!(
                    "swarm down {} stopped at OpenTofu group {name}; fix the cause and run swarmy swarm down {} again (destroyed groups are skipped)",
                    spec.name, spec.name
                ),
            ));
        }
        count += usize::from(had_state);
    }
    Ok(count)
}

/// Hetzner object storage can still report a bucket "not empty" for a short
/// while after the provider deleted its objects, so retry destroy on that
/// error only, a bounded number of times.
async fn destroy_settling(workspace: &Workspace<'_>) -> Result<()> {
    let mut attempt = 1;
    loop {
        match workspace.destroy().await {
            Err(error) if attempt < DESTROY_ATTEMPTS && bucket_not_empty(&error.to_string()) => {
                cloud_err!(
                    "OpenTofu destroy: the bucket is not empty yet; retrying ({attempt}/{DESTROY_ATTEMPTS})"
                );
                attempt += 1;
                tokio::time::sleep(std::time::Duration::from_secs(15)).await;
            }
            result => return result.map_err(Into::into),
        }
    }
}

const DESTROY_ATTEMPTS: u32 = 5;

fn bucket_not_empty(message: &str) -> bool {
    message.contains("BucketNotEmpty")
        || message.contains("bucket that you tried to delete is not empty")
}
