//! Grow or shrink a node group. New machines go through the shared provisioning path;
//! removed machines are drained first, highest-numbered first, then destroyed by
//! `OpenTofu` or cleaned of swarmy (existing hosts).
#[cfg(test)]
mod tests;
use super::{
    Locked, Machine, Role, SwarmHost, UpRequest, existing_sources, groups, lock, machine,
    provision_locked, user,
};
use crate::{Error, Result, ssh};
use std::{fmt::Write as _, path::Path, time::Duration};
use swarmy_config::{
    Settings,
    swarm::{Module, Registry, SwarmRecord, SwarmSpec},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupKind {
    Module,
    Existing,
    Elastic,
}

/// # Errors
/// Rejects groups that do not exclusively run nodes.
pub fn check_scalable(spec: &SwarmSpec, group: &str) -> Result<GroupKind> {
    let Some(selected) = spec.groups.get(group) else {
        return Err(Error::other(format!(
            "swarm \"{}\" has no group \"{group}\"; its groups are {}",
            spec.name,
            spec.groups.keys().cloned().collect::<Vec<_>>().join(", ")
        )));
    };
    if group == spec.placement.stack {
        return Err(Error::other(format!(
            "group \"{group}\" runs the stack of swarm {}; scale changes only node groups for now",
            spec.name
        )));
    }
    if group == spec.placement.services {
        return Err(Error::other(format!(
            "group \"{group}\" runs the services of swarm {}; scale changes only node groups for now",
            spec.name
        )));
    }
    if !spec.placement.nodes.iter().any(|name| name == group) {
        return Err(Error::other(format!(
            "group \"{group}\" is not a node group of swarm {} (placement.nodes = [{}])",
            spec.name,
            spec.placement.nodes.join(", ")
        )));
    }
    if selected.elastic {
        return Ok(GroupKind::Elastic);
    }
    match selected.module {
        Module::Existing => Ok(GroupKind::Existing),
        Module::Local => Err(Error::other(format!(
            "swarm \"{}\" runs on this machine; scale applies to swarms on hosts or cloud machines",
            spec.name
        ))),
        _ => Ok(GroupKind::Module),
    }
}

pub struct ScaleRequest<'a> {
    pub registry: &'a Registry,
    pub spec: &'a SwarmSpec,
    pub group: &'a str,
    pub count: u32,
    pub drain_timeout: Duration,
    pub force: bool,
    pub machines: Option<groups::MachineGroups<'a>>,
    pub settings: &'a Settings,
    pub cache: &'a Path,
}
#[derive(Debug, serde::Serialize)]
pub struct ScaleReport {
    pub swarm: String,
    pub group: String,
    pub from: usize,
    pub to: usize,
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub interrupted: Vec<String>,
    pub unreleased: Vec<String>,
}
fn read_up_record(registry: &Registry, spec: &SwarmSpec) -> Result<SwarmRecord> {
    let record = registry.read_record(&spec.name)?.ok_or_else(|| {
        Error::other(format!(
            "swarm {} is not up; run swarmy swarm up {} first",
            spec.name, spec.name
        ))
    })?;
    if !record
        .hosts
        .get(&spec.placement.stack)
        .and_then(|h| h.first())
        .is_some_and(|h| h.provisioned)
    {
        return Err(Error::other(format!(
            "swarm {} is not up; run swarmy swarm up {} first",
            spec.name, spec.name
        )));
    }
    Ok(record)
}

/// Check refusals that need no lock or cloud credentials.
/// # Errors
/// Rejects non-node groups, swarms not up, and growing existing-host groups.
pub fn preflight(
    registry: &Registry,
    spec: &SwarmSpec,
    group: &str,
    count: u32,
) -> Result<(GroupKind, SwarmRecord)> {
    let kind = check_scalable(spec, group)?;
    if kind == GroupKind::Elastic {
        return Err(Error::other(format!(
            "group \"{group}\" is elastic; the CLI scales it through the swarm's API"
        )));
    }
    if count == 0 && kind == GroupKind::Existing {
        return Err(Error::other(format!(
            "group \"{group}\" lists existing hosts and cannot be scaled to zero; keep at least one host (only groups whose machines OpenTofu creates can have zero)"
        )));
    }
    let record = read_up_record(registry, spec)?;
    if kind == GroupKind::Existing && count as usize > record.hosts.get(group).map_or(0, Vec::len) {
        return Err(Error::other(format!(
            "group \"{group}\" lists existing hosts; add machines with swarmy swarm node add {} {group} --host ADDRESS --ssh-key PATH",
            spec.name
        )));
    }
    Ok((kind, record))
}

/// # Errors
/// Reports invalid groups, unavailable hosts and infrastructure failures.
pub async fn scale_swarm(request: ScaleRequest<'_>) -> Result<ScaleReport> {
    let (kind, record) = preflight(request.registry, request.spec, request.group, request.count)?;
    let recorded = record.hosts.get(request.group).map_or(0, Vec::len);
    // A same-count rerun can still have an unprovisioned host recorded by a
    // previous apply. Keep provisioning binaries available for that resume.
    let host = if kind == GroupKind::Module && request.count as usize >= recorded {
        ssh::Ssh::for_client(request.cache)?
    } else {
        ssh::Ssh::teardown()
    };
    scale_swarm_with(&host, request).await
}

pub(crate) async fn scale_swarm_with(
    host: &impl SwarmHost,
    request: ScaleRequest<'_>,
) -> Result<ScaleReport> {
    let spec = request.spec;
    let _lock = Locked(lock(request.registry, &spec.name)?);
    let (kind, mut record) = preflight(request.registry, spec, request.group, request.count)?;
    let before = record.hosts.get(request.group).cloned().unwrap_or_default();
    let from = before.len();
    let target = request.count as usize;
    let mut report = ScaleReport {
        swarm: spec.name.clone(),
        group: request.group.into(),
        from,
        to: target,
        added: Vec::new(),
        removed: Vec::new(),
        interrupted: Vec::new(),
        unreleased: Vec::new(),
    };
    if kind == GroupKind::Existing && target == from {
        cloud_out!(
            "group {} of swarm {} already has {} machines",
            request.group,
            spec.name,
            target
        );
        return Ok(report);
    }
    let mut spec = spec.clone();
    if target >= from {
        grow(host, &request, &mut spec, &record, &before, &mut report).await?;
    } else {
        shrink(
            host,
            &request,
            &mut spec,
            &mut record,
            &before,
            &mut report,
            kind,
        )
        .await?;
    }
    cloud_out!(
        "group {} of swarm {}: {} -> {} machines",
        request.group,
        spec.name,
        from,
        target
    );
    Ok(report)
}

fn write_spec(request: &ScaleRequest<'_>, spec: &SwarmSpec) -> Result<()> {
    spec.validate()?;
    request.registry.write_spec(spec)?;
    cloud_err!(
        "note: swarmy rewrote {}; comments in it are not kept",
        request.registry.spec_path(&spec.name)?.display()
    );
    Ok(())
}
fn runner<'a>(request: &'a ScaleRequest<'_>) -> Result<&'a groups::MachineGroups<'a>> {
    request.machines.as_ref().ok_or_else(|| {
        Error::other(format!(
            "swarm {} has OpenTofu groups; this caller passed no OpenTofu runner",
            request.spec.name
        ))
    })
}
async fn grow(
    host: &impl SwarmHost,
    request: &ScaleRequest<'_>,
    spec: &mut SwarmSpec,
    record: &SwarmRecord,
    before: &[swarmy_config::swarm::RecordedHost],
    report: &mut ScaleReport,
) -> Result<()> {
    let _ = runner(request)?;
    if spec.groups[request.group].count != Some(request.count) {
        spec.groups
            .get_mut(request.group)
            .expect("checked group")
            .count = Some(request.count);
        write_spec(request, spec)?;
    }
    let dir = request.registry.swarm_dir(&spec.name)?;
    let result = provision_locked(
        host,
        UpRequest {
            registry: request.registry,
            spec,
            sources: existing_sources(spec, &dir)?,
            settings: request.settings,
            copy_credential: false,
            recipe: None,
            cache: request.cache,
            bucket_keys: record.bucket_keys.clone(),
            machines: request.machines,
        },
    )
    .await;
    if let Err(error) = result {
        cloud_err!(
            "swarm scale {} {} {} stopped: the spec now says count = {}; run the same command again to resume, or swarmy swarm scale {} {} {} to go back",
            spec.name,
            request.group,
            request.count,
            request.count,
            spec.name,
            request.group,
            before.len()
        );
        return Err(error);
    }
    let after = request
        .registry
        .read_record(&spec.name)?
        .expect("provision recorded hosts");
    for host in after.hosts.get(request.group).into_iter().flatten() {
        if !before.iter().any(|old| old.name == host.name) {
            cloud_out!("{}: provisioned", host.name);
            report.added.push(host.name.clone());
        }
    }
    Ok(())
}
fn as_machine(spec: &SwarmSpec, host: &swarmy_config::swarm::RecordedHost) -> Machine {
    machine(host, user(spec), Role::Node, 0, "")
}
async fn restart(host: &impl SwarmHost, drained: &[Machine]) -> String {
    let mut started = Vec::new();
    let mut failed = Vec::new();
    for node in drained {
        match host.start(node).await {
            Ok(()) => started.push(node.name.as_str()),
            Err(error) => {
                cloud_err!(
                    "host {}: could not restart node agent ({})",
                    node.name,
                    swarmy_core::error_chain(&error)
                );
                failed.push(node.name.as_str());
            }
        }
    }
    let mut result = String::new();
    if !started.is_empty() {
        write!(
            result,
            ", and the node agents already stopped on {} were started again",
            started.join(", ")
        )
        .expect("writing to String cannot fail");
    }
    if !failed.is_empty() {
        write!(
            result,
            ", but the node agents on {} could not be restarted; start them by hand",
            failed.join(", ")
        )
        .expect("writing to String cannot fail");
    }
    result
}
fn warn_unreleased(node: &Machine, drain: &ssh::Drain) {
    if !drain.unreleased.is_empty() {
        cloud_err!(
            "host {}: the node agent stopped before releasing the computers of {}; they start on another node once their leases expire",
            node.name,
            drain.unreleased.join(", ")
        );
    }
}
async fn drain_all(
    host: &impl SwarmHost,
    request: &ScaleRequest<'_>,
    spec: &SwarmSpec,
    record: &SwarmRecord,
    before: &[swarmy_config::swarm::RecordedHost],
    report: &mut ScaleReport,
) -> Result<Vec<Machine>> {
    let mut drained = Vec::new();
    let primary_host = record.hosts[&spec.placement.stack]
        .first()
        .expect("checked primary");
    let primary = as_machine(spec, primary_host);
    for victim in before[request.count as usize..].iter().rev() {
        let node = as_machine(spec, victim);
        if victim.provisioned {
            match host
                .drain(&node, request.drain_timeout, request.force)
                .await
            {
                Ok(drain) if !drain.stopped => {
                    let restarted = restart(host, &drained).await;
                    return Err(Error::other(format!(
                        "host {} still has running sandbox commands after {}s (agents {}); no machine was removed{restarted}. Wait and run the command again, raise --drain-timeout, or pass --force to interrupt them",
                        node.name,
                        request.drain_timeout.as_secs(),
                        drain.busy.join(", ")
                    )));
                }
                Ok(drain) => {
                    warn_unreleased(&node, &drain);
                    cloud_out!(
                        "{}: drained{}",
                        node.name,
                        if drain.busy.is_empty() {
                            String::new()
                        } else {
                            format!(
                                ", interrupting {} running sandbox command(s)",
                                drain.busy.len()
                            )
                        }
                    );
                    report.interrupted.extend(drain.busy);
                    report.unreleased.extend(drain.unreleased);
                    drained.push(node.clone());
                }
                Err(error) if !request.force => {
                    let restarted = restart(host, &drained).await;
                    return Err(Error::other(format!(
                        "could not reach host {} to drain it ({}); pass --force to remove it without draining (its computers move to other nodes when their leases expire){restarted}",
                        node.name,
                        swarmy_core::error_chain(&error)
                    )));
                }
                Err(error) => {
                    cloud_err!(
                        "could not reach host {} to drain it ({}); removing without draining",
                        node.name,
                        swarmy_core::error_chain(&error)
                    );
                    cloud_out!("{}: removed without draining (--force)", node.name);
                }
            }
        }
    }
    for victim in before[request.count as usize..]
        .iter()
        .rev()
        .filter(|h| h.provisioned)
    {
        let node = as_machine(spec, victim);
        if let Err(error) = host.revoke_tunnel(&node, &primary).await {
            cloud_err!(
                "host {}: could not revoke its tunnel key on {} ({}); remove its line from ~{}/.ssh/authorized_keys there",
                node.name,
                primary.name,
                swarmy_core::error_chain(&error),
                user(spec)
            );
        }
    }
    Ok(drained)
}
async fn shrink(
    host: &impl SwarmHost,
    request: &ScaleRequest<'_>,
    spec: &mut SwarmSpec,
    record: &mut SwarmRecord,
    before: &[swarmy_config::swarm::RecordedHost],
    report: &mut ScaleReport,
    kind: GroupKind,
) -> Result<()> {
    let _ = drain_all(host, request, spec, record, before, report).await?;
    match kind {
        GroupKind::Module => apply_lower(request, spec, record, before, report).await?,
        GroupKind::Existing => remove_existing(host, request, spec, record, before, report).await?,
        GroupKind::Elastic => {
            return Err(Error::other("elastic groups are scaled through the API"));
        }
    }
    Ok(())
}
async fn apply_lower(
    request: &ScaleRequest<'_>,
    spec: &mut SwarmSpec,
    record: &mut SwarmRecord,
    before: &[swarmy_config::swarm::RecordedHost],
    report: &mut ScaleReport,
) -> Result<()> {
    let machines = runner(request)?;
    let victims = before[request.count as usize..]
        .iter()
        .rev()
        .map(|h| h.name.clone())
        .collect::<Vec<_>>();
    record
        .hosts
        .get_mut(request.group)
        .expect("recorded group")
        .truncate(request.count as usize);
    request.registry.write_record(record)?;
    spec.groups
        .get_mut(request.group)
        .expect("checked group")
        .count = Some(request.count);
    write_spec(request, spec)?;
    if let Err(error) = groups::apply(machines, request.registry, spec, record).await {
        cloud_err!(
            "swarm scale {} {} {} stopped: {} are drained and no longer recorded, but OpenTofu has not removed them yet; run the same command again to finish",
            spec.name,
            request.group,
            request.count,
            victims.join(", ")
        );
        return Err(error);
    }
    let kept = before[..request.count as usize]
        .iter()
        .map(|h| h.name.as_str())
        .collect::<Vec<_>>();
    let actual = record
        .hosts
        .get(request.group)
        .into_iter()
        .flatten()
        .map(|h| h.name.as_str())
        .collect::<Vec<_>>();
    if actual != kept {
        return Err(Error::other(format!(
            "group {}: OpenTofu kept {} but swarmy drained {}; the record now follows OpenTofu's outputs; check swarmy swarm status {}",
            request.group,
            actual.join(", "),
            victims.join(", "),
            spec.name
        )));
    }
    for victim in &before[request.count as usize..] {
        swarmy_core::ignore_best_effort(
            ssh::forget_host_key(&victim.public_address, &victim.ssh_key),
            "remove destroyed node from SSH known hosts",
        );
    }
    for name in victims {
        cloud_out!("{name}: removed");
        report.removed.push(name);
    }
    Ok(())
}
async fn remove_existing(
    host: &impl SwarmHost,
    request: &ScaleRequest<'_>,
    spec: &mut SwarmSpec,
    record: &mut SwarmRecord,
    before: &[swarmy_config::swarm::RecordedHost],
    report: &mut ScaleReport,
) -> Result<()> {
    for victim in before[request.count as usize..].iter().rev() {
        let node = as_machine(spec, victim);
        let failed = host.decommission(&node).await.err();
        if failed.is_none() {
            report.removed.push(victim.name.clone());
            cloud_out!("{}: removed", victim.name);
        }
        let remaining = before.len() - report.removed.len();
        spec.groups
            .get_mut(request.group)
            .expect("checked group")
            .hosts
            .truncate(remaining);
        record
            .hosts
            .get_mut(request.group)
            .expect("recorded group")
            .truncate(remaining);
        write_spec(request, spec)?;
        request.registry.write_record(record)?;
        if failed.is_none() {
            for path in [
                &victim.ssh_key,
                &victim.ssh_key.with_extension("known_hosts"),
            ] {
                swarmy_core::ignore_best_effort(
                    std::fs::remove_file(path),
                    "remove decommissioned host key",
                );
            }
        }
        if let Some(error) = failed {
            return Err(Error::other(format!(
                "swarm scale {} {} {} stopped at host {} ({}); it is drained but still listed; fix it and run the same command again",
                spec.name,
                request.group,
                request.count,
                node.name,
                swarmy_core::error_chain(&error)
            )));
        }
    }
    Ok(())
}
