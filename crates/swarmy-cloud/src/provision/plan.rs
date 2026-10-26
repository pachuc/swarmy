use super::{HostSource, Planned, Role};
use crate::{Error, Result};
use std::{collections::BTreeMap, net::Ipv4Addr, path::Path};
use swarmy_config::swarm::{Module, RecordedHost, SwarmSpec};

pub(super) fn existing_sources(spec: &SwarmSpec, swarm_dir: &Path) -> Result<Vec<HostSource>> {
    let mut sources = Vec::new();
    for (group, entry) in &spec.groups {
        if entry.module != Module::Existing {
            let module: String = entry.module.clone().into();
            return Err(Error::other(format!(
                "group \"{group}\" uses module {module}; only existing-host groups can be provisioned so far"
            )));
        }
        for (index, host) in entry.hosts.iter().enumerate() {
            let name = format!("{}-{group}-{}", spec.name, index + 1);
            sources.push(HostSource {
                group: group.clone(),
                host: RecordedHost {
                    name: name.clone(),
                    public_address: host.address.clone(),
                    private_address: host.private_address.clone(),
                    ssh_user: host.ssh_user.clone(),
                    ssh_key: swarm_dir.join("keys").join(name),
                    provisioned: false,
                },
                copy_key_from: Some(host.ssh_key.clone()),
            });
        }
    }
    Ok(sources)
}

pub(super) fn plan(spec: &SwarmSpec, sources: Vec<HostSource>) -> Result<Vec<Planned>> {
    let stack = &spec.placement.stack;
    if spec.placement.services != *stack {
        let services = &spec.groups[&spec.placement.services];
        if services.module == Module::Local {
            return Err(Error::other(format!(
                "swarm \"{}\": services on this machine with the stack on hosts is not supported yet; set placement.services = \"{stack}\"",
                spec.name
            )));
        }
        return Err(Error::other(format!(
            "swarm \"{}\": placement.services must name the stack group \"{stack}\"; running the services on a separate group is not supported yet",
            spec.name
        )));
    }
    for (name, group) in &spec.groups {
        if group.module != Module::Existing {
            let module: String = group.module.clone().into();
            return Err(Error::other(format!(
                "group \"{name}\" uses module {module}; only existing-host groups can be provisioned so far"
            )));
        }
    }
    let stack_count = sources.iter().filter(|s| &s.group == stack).count();
    let stack_nodes = spec.placement.nodes.contains(stack);
    if stack_count > 1 && !stack_nodes {
        return Err(Error::other(format!(
            "group \"{stack}\" runs the stack on its first host; its other hosts must run nodes: add \"{stack}\" to placement.nodes"
        )));
    }
    for source in &sources {
        for address in [
            Some(source.host.public_address.as_str()),
            source.host.private_address.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if address.parse::<Ipv4Addr>().is_err() {
                return Err(Error::other(format!(
                    "host {}: address {address} must be an IPv4 address",
                    source.host.name
                )));
            }
        }
        let user = &source.host.ssh_user;
        if user.is_empty()
            || !user
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(Error::other(format!(
                "host {}: ssh_user must contain letters, digits, hyphens, or underscores",
                source.host.name
            )));
        }
        if let Some(path) = &source.copy_key_from {
            if !path.is_absolute() {
                return Err(Error::other(format!(
                    "host {}: ssh_key {} must be an absolute path",
                    source.host.name,
                    path.display()
                )));
            }
            if !path.is_file() {
                return Err(Error::other(format!(
                    "host {}: ssh_key {} does not exist",
                    source.host.name,
                    path.display()
                )));
            }
        }
    }
    let mut groups: BTreeMap<String, Vec<(usize, HostSource)>> = BTreeMap::new();
    for source in sources {
        let hosts = groups.entry(source.group.clone()).or_default();
        hosts.push((hosts.len(), source));
    }
    let mut ordered = groups
        .remove(stack)
        .ok_or_else(|| Error::other(format!("group \"{stack}\" has no stack host")))?;
    for group in &spec.placement.nodes {
        if group != stack {
            ordered.extend(groups.remove(group).unwrap_or_default());
        }
    }
    ordered.into_iter().map(|(index, source)| {
        let group = &spec.groups[&source.group];
        let primary = source.host.name == ordered_primary_name(spec, stack);
        let sandboxes = if primary && !stack_nodes { 0 } else { group.sandboxes.unwrap_or(swarmy_config::default_sandboxes()) };
        let local_storage = group.local_storage.clone().unwrap_or_default();
        if sandboxes > 0 && local_storage.is_empty() {
            return Err(Error::other(format!("group \"{}\": hosts that run sandboxes need local_storage (a block device or dir:/path), or set sandboxes = 0", source.group)));
        }
        Ok(Planned { group: source.group, index, host: source.host, copy_key_from: source.copy_key_from,
            role: if primary { Role::Stack { services: true } } else { Role::Node }, sandboxes, local_storage })
    }).collect()
}
fn ordered_primary_name(spec: &SwarmSpec, stack: &str) -> String {
    format!("{}-{stack}-1", spec.name)
}
