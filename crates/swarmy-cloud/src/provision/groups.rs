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
use swarmy_infra::{CredentialEnv, ModuleFile, ModuleOutputs, PlanOutcome, Tofu, VARIABLES_FILE};

/// Runner and credentials for machine groups.
#[derive(Clone, Copy)]
pub struct MachineGroups<'a> {
    pub tofu: &'a Tofu,
    pub credentials: &'a CloudCredentials,
}
/// True if any group creates machines.
#[must_use]
pub fn has_module_groups(spec: &SwarmSpec) -> bool {
    spec.groups.values().any(|g| is_module(&g.module))
}
fn is_module(module: &Module) -> bool {
    !matches!(module, Module::Existing | Module::Local)
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
        if !result.contains(&name) && spec.groups.get(&name).is_some_and(|g| is_module(&g.module)) {
            result.push(name);
        }
    }
    result
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
    pub(crate) bucket: Option<BucketSpec>,
}
fn root(dir: &Path) -> PathBuf {
    dir.join("tofu")
}
fn group_dir(dir: &Path, group: &str) -> PathBuf {
    root(dir).join(group)
}
fn valid_name(name: &str) -> bool {
    name.len() <= 20
        && name.starts_with(|c: char| c.is_ascii_lowercase())
        && name.ends_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
        && name
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
}
fn module_name(module: &Module) -> String {
    module.clone().into()
}
fn bucket_name(spec: &SwarmSpec, record: &mut SwarmRecord) -> Option<String> {
    if spec.bucket.is_none()
        && spec
            .groups
            .get(&spec.placement.stack)
            .is_some_and(|g| g.module == Module::HetznerCloud)
    {
        Some(
            record
                .tofu_bucket
                .get_or_insert_with(|| {
                    format!(
                        "swarmy-{}-{:012x}",
                        spec.name,
                        ulid::Ulid::generate().random() & 0xffff_ffff_ffff
                    )
                })
                .clone(),
        )
    } else {
        None
    }
}
fn credential_env(
    credentials: &CloudCredentials,
    group: &GroupSpec,
    bucket: bool,
) -> Result<CredentialEnv> {
    let providers: Vec<_> = match group.module {
        Module::HetznerCloud => {
            let mut p = vec![CloudProvider::HetznerCloud];
            if bucket || credentials.get(CloudProvider::HetznerS3).is_some() {
                p.push(CloudProvider::HetznerS3);
            }
            p
        }
        Module::Aws => vec![CloudProvider::Aws],
        // Custom modules receive all stored providers: their provider needs are unknown.
        Module::Path(_) => CloudProvider::ALL.to_vec(),
        _ => Vec::new(),
    };
    let mut result = CredentialEnv::new()
        .with("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
        .with("AWS_CONFIG_FILE", "/dev/null")
        .with("AWS_EC2_METADATA_DISABLED", "true");
    for provider in providers {
        let credential = if matches!(group.module, Module::Path(_)) {
            credentials.get(provider)
        } else {
            Some(
                credentials
                    .get(provider)
                    .ok_or(swarmy_config::Error::CloudNotLoggedIn(provider.name()))?,
            )
        };
        if let Some(c) = credential {
            result = c.env().iter().fold(result, |env, (n, v)| env.with(n, v));
        }
    }
    Ok(result)
}
fn preflight(
    spec: &SwarmSpec,
    record: &SwarmRecord,
    machines: &MachineGroups<'_>,
    dir: &Path,
) -> Result<()> {
    for name in apply_order(spec) {
        let group = &spec.groups[&name];
        if !valid_name(&spec.name) {
            return Err(Error::other(format!(
                "swarm \"{}\": OpenTofu groups need a swarm name of 1-20 lowercase letters, digits and hyphens that starts with a letter and does not end with a hyphen",
                spec.name
            )));
        }
        if !valid_name(&name) {
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
        let bucket = name == spec.placement.stack
            && spec.bucket.is_none()
            && group.module == Module::HetznerCloud;
        let _ = credential_env(machines.credentials, group, bucket)?;
        let previous = record.hosts.get(&name).map_or(0, Vec::len);
        let count = group.count.unwrap_or_default();
        if previous > count as usize {
            return Err(Error::other(format!(
                "group \"{name}\" has {previous} machines recorded but count = {count}; lowering count would destroy machines without draining them; restore count = {previous}"
            )));
        }
        if previous > 0 {
            check_immutable_fields(spec, &name, group, dir)?;
        }
    }
    if record.tofu_bucket.is_some()
        && record.hosts.values().flatten().any(|h| h.provisioned)
        && (spec.bucket.is_some()
            || spec
                .groups
                .get(&spec.placement.stack)
                .is_none_or(|g| g.module != Module::HetznerCloud))
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
    for (field, current) in [("size", &group.size), ("location", &group.location)] {
        if let Some(old) = last.get(field).and_then(Value::as_str)
            && current.as_deref() != Some(old)
        {
            return Err(Error::other(format!(
                "group \"{name}\": {field} changed from {old} to {} since its machines were created; that would rebuild them. Restore it, or run swarmy swarm down {} first",
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
async fn group_key(dir: &Path, swarm: &str, group: &str) -> Result<String> {
    let keys = dir.join("keys/groups");
    create_private_dir(&keys)?;
    let key = keys.join(group);
    let public = if key.is_file() {
        fs::read(key.with_extension("pub"))?
    } else {
        ssh::generate_key(&key, &format!("swarmy-{swarm}-{group}")).await?
    };
    Ok(String::from_utf8(public)?.trim().to_owned())
}
fn variables(
    spec: &SwarmSpec,
    name: &str,
    group: &GroupSpec,
    key: &str,
    bucket: &str,
    network: &str,
) -> Map<String, Value> {
    let mut vars = Map::new();
    for (k, v) in [
        ("swarm", json!(spec.name)),
        ("group", json!(name)),
        ("machine_count", json!(group.count.unwrap_or_default())),
        ("ssh_public_key", json!(key)),
        ("ssh_allowed_cidrs", json!(["0.0.0.0/0", "::/0"])),
        ("bucket_name", json!(bucket)),
        ("bucket_force_destroy", json!(false)),
        ("network_id", json!(network)),
    ] {
        vars.insert(k.into(), v);
    }
    if let Some(size) = &group.size {
        vars.insert("size".into(), json!(size));
    }
    if let Some(location) = &group.location {
        vars.insert("location".into(), json!(location));
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
    key: &Path,
    previous: &[RecordedHost],
) -> Result<Vec<RecordedHost>> {
    outputs.machines.iter().map(|m| {
        let public = m.public_ipv4.ok_or_else(|| Error::other(format!("group {group}: machine {} has no public IPv4 address; swarmy provisions over IPv4", m.name)))?;
        let private_address = match m.private_ip {
            Some(IpAddr::V4(ip)) => Some(ip.to_string()),
            Some(address) => return Err(Error::other(format!("group {group}: machine {}: private address {address} must be IPv4", m.name))),
            None => None,
        };
        Ok(RecordedHost { name: m.name.clone(), public_address: public.to_string(), private_address, ssh_user: m.ssh_user.clone(), ssh_key: key.into(), provisioned: previous.iter().any(|h| h.name == m.name && h.provisioned) })
    }).collect()
}
fn bucket_from(record: &SwarmRecord, credentials: &CloudCredentials) -> Option<BucketSpec> {
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
/// Effective bucket from the record, with credentials loaded only at call time.
#[must_use]
pub fn recorded_bucket(record: &SwarmRecord, credentials: &CloudCredentials) -> Option<BucketSpec> {
    bucket_from(record, credentials)
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
    let bucket = bucket_name(spec, record);
    let root = root(&dir);
    create_private_dir(&root)?;
    record.tofu_state = Some(root);
    registry.write_record(record)?;
    let mut sources = Vec::new();
    let mut networks: Vec<(Module, String)> = Vec::new();
    for name in apply_order(spec) {
        let group = &spec.groups[&name];
        let group_bucket = if name == spec.placement.stack {
            bucket.as_deref().unwrap_or("")
        } else {
            ""
        };
        let network = networks
            .iter()
            .find(|(kind, _)| kind == &group.module)
            .map_or("", |(_, id)| id.as_str());
        let vars = variables(
            spec,
            &name,
            group,
            &group_key(&dir, &spec.name, &name).await?,
            group_bucket,
            network,
        );
        let env = credential_env(machines.credentials, group, !group_bucket.is_empty())?;
        let workspace = machines.tofu.workspace(group_dir(&dir, &name), env);
        let module = module_files(&name, group)?;
        cloud_out!(
            "OpenTofu: applying group {name} ({})",
            module_name(&group.module)
        );
        workspace.prepare(&module.borrowed(), &vars).await?;
        workspace.init().await?;
        workspace.apply().await?;
        let mut outputs = workspace.outputs().await?;
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
        if let Some(id) = outputs.network_id.clone() {
            networks.push((group.module.clone(), id));
        }
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
    Ok(Applied {
        sources,
        bucket: bucket_from(record, machines.credentials),
    })
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
        });
    preflight(spec, &record, &machines, &dir)?;
    let bucket = bucket_name(spec, &mut record);
    registry.write_record(&record)?;
    let mut result = Vec::new();
    let mut networks: Vec<(Module, String)> = Vec::new();
    for name in apply_order(spec) {
        let group = &spec.groups[&name];
        let group_bucket = if name == spec.placement.stack {
            bucket.as_deref().unwrap_or("")
        } else {
            ""
        };
        let network = networks
            .iter()
            .find(|(kind, _)| kind == &group.module)
            .map_or("", |(_, id)| id.as_str());
        let vars = variables(
            spec,
            &name,
            group,
            &group_key(&dir, &spec.name, &name).await?,
            group_bucket,
            network,
        );
        let workspace = machines.tofu.workspace(
            group_dir(&dir, &name),
            credential_env(machines.credentials, group, !group_bucket.is_empty())?,
        );
        let module = module_files(&name, group)?;
        workspace.prepare(&module.borrowed(), &vars).await?;
        workspace.init().await?;
        let (outcome, text) = workspace.plan_text().await?;
        result.push(GroupPlan {
            group: name.clone(),
            module: module_name(&group.module),
            changes: outcome == PlanOutcome::Changes,
            plan: text,
        });
        if workspace.dir().join("terraform.tfstate").is_file()
            && let Ok(outputs) = workspace.outputs().await
            && let Some(id) = outputs.network_id
        {
            networks.push((group.module.clone(), id));
        }
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
        return credential_env(credentials, group, bucket);
    }
    let mut env = CredentialEnv::new()
        .with("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
        .with("AWS_CONFIG_FILE", "/dev/null")
        .with("AWS_EC2_METADATA_DISABLED", "true");
    for provider in CloudProvider::ALL {
        if let Some(c) = credentials.get(provider) {
            env = c.env().iter().fold(env, |env, (n, v)| env.with(n, v));
        }
    }
    Ok(env)
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
        let attempt: Result<()> = async {
            if work.join("terraform.tfstate").is_file() {
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
                    workspace.apply().await?;
                }
                workspace.destroy().await?;
            }
            fs::remove_dir_all(&work)?;
            record.hosts.remove(&name);
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
        count += 1;
    }
    Ok(count)
}
