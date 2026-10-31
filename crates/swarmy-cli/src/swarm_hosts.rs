//! CLI presentation for swarms on hosts.
use anyhow::{Context, Result, bail};
use serde_json::json;
use std::path::PathBuf;
use swarmy_cloud::provision::groups::{self, MachineGroups};
use swarmy_cloud::provision::{self, UpRequest};
use swarmy_config::cloud::{CloudCredentials, CloudProvider};
use swarmy_config::swarm::{
    GroupSpec, HostSpec, Lifecycle, Module, Placement, RecordedHost, Registry, SwarmRecord,
    SwarmSpec,
};
use swarmy_infra::Tofu;

pub(crate) struct UpArgs {
    pub copy_credential: bool,
    pub bucket_keys: Option<swarmy_config::swarm::StaticKeys>,
    pub no_image: bool,
    pub image_recipe: PathBuf,
}
pub(crate) struct CreateHost {
    pub address: String,
    pub ssh_user: String,
    pub ssh_key: PathBuf,
    pub private_address: Option<String>,
    pub sandboxes: Option<u32>,
    pub local_storage: Option<String>,
    pub service_user: Option<String>,
    pub lifecycle: Option<Lifecycle>,
}
pub(crate) struct CreateCloud {
    pub module: String,
    pub size: Option<String>,
    pub location: Option<String>,
    pub nodes: u32,
    pub node_size: Option<String>,
    pub sandboxes: Option<u32>,
    pub local_storage: Option<String>,
    pub lifecycle: Option<Lifecycle>,
}

// SWARMY_TOFU_BIN is a development and test override of the pinned download.
async fn tofu(registry: &Registry) -> Result<Tofu> {
    Ok(Tofu::resolve(&registry.dirs().cache).await?)
}
async fn machines_for(
    registry: &Registry,
    spec: &SwarmSpec,
    need: bool,
) -> Result<Option<(Tofu, CloudCredentials)>> {
    if !need && !groups::has_module_groups(spec) {
        return Ok(None);
    }
    let creds = CloudCredentials::load(&CloudCredentials::path(registry.dirs()))?;
    groups::check_credentials(spec, &creds)?;
    let runner = tofu(registry).await?;
    Ok(Some((runner, creds)))
}
fn machine_refs(pair: Option<&(Tofu, CloudCredentials)>) -> Option<MachineGroups<'_>> {
    pair.map(|(tofu, credentials)| MachineGroups { tofu, credentials })
}

const ON_MODULES: &str = "--on takes hetzner-cloud, aws, scaleway, or an absolute path to your own module; use --host for machines you own and --local for this machine";

pub(crate) fn cloud_spec(name: &str, args: &CreateCloud) -> Result<SwarmSpec> {
    let module = Module::try_from(args.module.clone()).map_err(|_| anyhow::anyhow!(ON_MODULES))?;
    if matches!(module, Module::Existing | Module::Local)
        || matches!(&module, Module::Path(p) if !p.is_absolute())
    {
        bail!(ON_MODULES);
    }
    if args.nodes == 0 && args.node_size.is_some() {
        bail!("--node-size requires --nodes greater than 0");
    }
    let size = args
        .size
        .clone()
        .ok_or_else(|| anyhow::anyhow!("--size is required with --on"))?;
    let location = args
        .location
        .clone()
        .or_else(|| (module == Module::HetznerCloud).then(|| "fsn1".into()))
        .ok_or_else(|| anyhow::anyhow!("--location is required with --on {}", args.module))?;
    let group = |size: String, count, nodes: bool| GroupSpec {
        module: module.clone(),
        size: Some(size),
        location: Some(location.clone()),
        image: None,
        count: Some(count),
        stack_dir: None,
        hosts: Vec::new(),
        sandboxes: if nodes { args.sandboxes } else { None },
        local_storage: if nodes {
            args.local_storage.clone()
        } else {
            None
        },

        elastic: false,
        max: None,
        price_per_hour: None,
        currency: None,
        autoscale: false,
    };
    let mut groups = std::collections::BTreeMap::new();
    groups.insert("control".into(), group(size.clone(), 1, args.nodes == 0));
    if args.nodes > 0 {
        groups.insert(
            "workers".into(),
            group(args.node_size.clone().unwrap_or(size), args.nodes, true),
        );
    }
    let spec = SwarmSpec {
        name: name.into(),
        lifecycle: args.lifecycle.unwrap_or(Lifecycle::Persistent),
        service_user: None,
        bucket: None,
        groups,
        placement: Placement {
            stack: "control".into(),
            services: "control".into(),
            nodes: vec![
                if args.nodes == 0 {
                    "control"
                } else {
                    "workers"
                }
                .into(),
            ],
        },
    };
    spec.validate()?;
    Ok(spec)
}

pub(crate) fn cloud_next(registry: &Registry, spec: &SwarmSpec) -> Result<Vec<String>> {
    let mut next = Vec::new();
    let creds = CloudCredentials::load(&CloudCredentials::path(registry.dirs()))?;
    for provider in CloudProvider::ALL {
        let needed = spec.groups.values().any(|g| {
            g.module.machine_provider() == Some(provider)
                || (provider == CloudProvider::HetznerS3 && g.module == Module::HetznerCloud)
        });
        if needed && creds.get(provider).is_none() {
            next.push(format!("next: swarmy cloud login {provider}"));
        }
    }
    next.push(format!("next: swarmy swarm plan {}", spec.name));
    Ok(next)
}

pub(crate) async fn plan(registry: &Registry, spec: &SwarmSpec, json_output: bool) -> Result<()> {
    if !groups::has_module_groups(spec) {
        bail!(
            "swarm \"{}\" has no OpenTofu groups; nothing to plan",
            spec.name
        );
    }
    let pair = machines_for(registry, spec, true).await?;
    let (tofu, credentials) = pair.expect("plan requested a machine runner");
    let plans = groups::plan_groups(
        registry,
        spec,
        MachineGroups {
            tofu: &tofu,
            credentials: &credentials,
        },
    )
    .await?;
    if json_output {
        println!("{}", json!({"swarm":spec.name,"groups":plans}));
    } else {
        for p in plans {
            println!(
                "== group {} ({}): {} ==",
                p.group,
                p.module,
                if p.changes { "changes" } else { "no changes" }
            );
            println!("{}", p.plan);
        }
    }
    Ok(())
}

pub(crate) struct NodeAdd {
    pub group: String,
    pub address: String,
    pub ssh_user: String,
    pub ssh_key: PathBuf,
    pub private_address: Option<String>,
}
pub(crate) fn host_spec(registry: &Registry, name: &str, args: &CreateHost) -> Result<SwarmSpec> {
    let spec = SwarmSpec {
        bucket: None,
        name: name.into(),
        lifecycle: args.lifecycle.unwrap_or(Lifecycle::Persistent),
        service_user: args.service_user.clone(),
        groups: [(
            "box".into(),
            GroupSpec {
                module: Module::Existing,
                size: None,
                location: None,
                image: None,
                count: None,
                stack_dir: None,
                sandboxes: args.sandboxes,
                local_storage: args.local_storage.clone(),
                hosts: vec![HostSpec {
                    address: args.address.clone(),
                    ssh_user: args.ssh_user.clone(),
                    ssh_key: std::fs::canonicalize(&args.ssh_key)?,
                    private_address: args.private_address.clone(),
                }],

                elastic: false,
                max: None,
                price_per_hour: None,
                currency: None,
                autoscale: false,
            },
        )]
        .into(),
        placement: Placement {
            stack: "box".into(),
            services: "box".into(),
            nodes: vec!["box".into()],
        },
    };
    spec.validate()?;
    provision::plan(
        &spec,
        provision::existing_sources(&spec, &registry.swarm_dir(name)?)?,
    )?;
    Ok(spec)
}
pub(crate) async fn up(
    registry: &Registry,
    spec: &SwarmSpec,
    args: &UpArgs,
    json_output: bool,
) -> Result<()> {
    let recipe = (!args.no_image)
        .then(|| swarmy_cloud::node_files::recipe(&args.image_recipe))
        .transpose()?;
    let settings = swarmy_config::Settings::load_base()?.settings;
    let sources = provision::existing_sources(spec, &registry.swarm_dir(&spec.name)?)?;
    let pair = machines_for(registry, spec, false).await?;
    let applied = groups::apply_order(spec);
    let reports = provision::provision_swarm(UpRequest {
        registry,
        spec,
        sources,
        settings: &settings,
        copy_credential: args.copy_credential,
        recipe: recipe.as_deref(),
        cache: &registry.dirs().cache.join("core"),
        machines: machine_refs(pair.as_ref()),
        bucket_keys: args.bucket_keys.clone(),
    })
    .await?;
    if json_output {
        println!(
            "{}",
            json!({"swarm":spec.name,"hosts":reports,"groups":applied})
        );
    } else {
        for r in reports {
            println!(
                "{} {} {} {} {}",
                r.name,
                r.group,
                r.role,
                r.address,
                if r.changed { "provisioned" } else { "skipped" }
            );
        }
        println!(
            "swarm {} is up; client commands take --swarm {}",
            spec.name, spec.name
        );
    }
    Ok(())
}
pub(crate) async fn down(
    registry: &Registry,
    spec: &SwarmSpec,
    destroy: bool,
    json_output: bool,
) -> Result<()> {
    if spec.lifecycle == Lifecycle::Persistent && !destroy {
        let what = if groups::has_module_groups(spec) {
            "down destroys its machines, database and bucket"
        } else if spec.groups.values().any(|group| group.elastic) {
            "down destroys its elastic machines and deletes its database and swarmy's files on every host"
        } else {
            "down deletes its database and swarmy's files on every host (the machines keep running)"
        };
        bail!(
            "swarm \"{0}\" is persistent: {what}. Stop it with swarmy swarm stop {0}, or pass --destroy",
            spec.name
        );
    }
    let mut elastic = Vec::new();
    let swarm = swarm_record(registry, spec)?;
    if spec.groups.values().any(|group| group.elastic) {
        let (client, endpoint) = crate::target::connect_swarm(&spec.name).await.context(
            "down needs the swarm's API to stop its elastic groups; start the swarm first",
        )?;
        for (name, group) in &spec.groups {
            if !group.elastic {
                continue;
            }
            let mut record = match client.elastic_group(name).await {
                Ok(record) => record,
                Err(swarmy_client::Error::Api { status, .. }) if status.as_u16() == 404 => {
                    continue;
                }
                Err(error) => return Err(error).context(
                    "down needs the swarm's API to stop its elastic groups; start the swarm first",
                ),
            };
            if record.autoscale {
                let definition = autoscale_off_request(&record, stack_primary(spec, &swarm)?, spec);
                record = swarmy_client::api_client::call(
                    &endpoint,
                    client.put_elastic_group(name, &definition),
                )
                .await?;
            }
            if record.desired > 0 {
                record = swarmy_client::api_client::call(
                    &endpoint,
                    client.set_elastic_desired(
                        name,
                        &swarmy_api_types::SetElasticDesired {
                            idempotency_key: ulid::Ulid::generate().to_string(),
                            desired: 0,
                            reason: Some("swarmy swarm down".into()),
                        },
                    ),
                )
                .await?;
            }
            elastic.push(record);
        }
        let blockers = elastic_down_blockers(&elastic);
        if !blockers.is_empty() {
            bail!(
                "swarm {} still has elastic machines or unfinished changes ({}); their desired count is now 0 and the provisioner on the primary destroys them; run swarmy swarm down {} again in a few minutes",
                spec.name,
                blockers.join(", "),
                spec.name
            );
        }
        if !json_output {
            for line in elastic_down_report(&elastic) {
                println!("{line}");
            }
        }
    }
    let tofu_dir = registry.swarm_dir(&spec.name)?.join("tofu");
    let pair = machines_for(registry, spec, tofu_dir.exists()).await?;
    let (removed, destroyed_groups) =
        provision::decommission_swarm(registry, spec, machine_refs(pair.as_ref())).await?;
    if json_output {
        println!(
            "{}",
            json!({"swarm":spec.name,"removed":removed.iter().map(|r| &r.name).collect::<Vec<_>>(),"destroyed_groups":destroyed_groups,"elastic_groups":elastic.iter().map(|r| &r.group).collect::<Vec<_>>()})
        );
    }
    Ok(())
}
pub(crate) async fn stop(registry: &Registry, spec: &SwarmSpec, json_output: bool) -> Result<()> {
    let reports = provision::stop_swarm(registry, spec).await?;
    if json_output {
        println!(
            "{}",
            json!({"swarm":spec.name,"stopped":reports.iter().map(|r| &r.name).collect::<Vec<_>>()})
        );
    } else {
        for r in reports {
            println!("{}: stopped", r.name);
        }
    }
    Ok(())
}
pub(crate) async fn start(registry: &Registry, spec: &SwarmSpec, json_output: bool) -> Result<()> {
    let reports = provision::start_swarm(registry, spec).await?;
    if json_output {
        println!(
            "{}",
            json!({"swarm":spec.name,"started":reports.iter().map(|r| &r.name).collect::<Vec<_>>()})
        );
    } else {
        for r in reports {
            println!("{}: started", r.name);
        }
    }
    Ok(())
}
pub(crate) async fn status(registry: &Registry, spec: &SwarmSpec, json_output: bool) -> Result<()> {
    let status = provision::status(registry, spec).await?;
    if json_output {
        println!("{}", serde_json::to_string(&status)?);
    } else {
        for h in status.hosts {
            println!(
                "{} {} {} {} ssh={} {}",
                h.name,
                h.group,
                h.role,
                h.address,
                if h.ssh { "reachable" } else { "unreachable" },
                if h.provisioned {
                    "provisioned"
                } else {
                    "not provisioned"
                }
            );
        }
        println!(
            "api token: {}",
            if status.api_token {
                "recorded"
            } else {
                "missing"
            }
        );
        println!("tunnel: {}", if status.tunnel { "open" } else { "closed" });
    }
    Ok(())
}
pub(crate) async fn node_add(
    registry: &Registry,
    name: &str,
    args: NodeAdd,
    json_output: bool,
) -> Result<()> {
    let lock = provision::lock(registry, name)?;
    let mut spec = crate::swarm_command::managed_spec(registry, name)?;
    let group = spec.groups.get(&args.group);
    if !group.is_some_and(|g| g.module == Module::Existing)
        || !spec.placement.nodes.contains(&args.group)
    {
        bail!(
            "group \"{}\" of swarm {name} is not an existing-host node group",
            args.group
        );
    }
    if spec
        .groups
        .values()
        .flat_map(|g| &g.hosts)
        .any(|h| h.address == args.address)
    {
        bail!("{} is already a host of swarm {name}", args.address);
    }
    if !registry.read_record(name)?.is_some_and(|r| {
        r.hosts
            .get(&spec.placement.stack)
            .and_then(|hosts| hosts.first())
            .is_some_and(|host| host.provisioned)
    }) {
        bail!("swarm {name} is not up; run swarmy swarm up {name} first");
    }
    spec.groups
        .get_mut(&args.group)
        .expect("checked group")
        .hosts
        .push(HostSpec {
            address: args.address,
            ssh_user: args.ssh_user,
            ssh_key: std::fs::canonicalize(args.ssh_key)?,
            private_address: args.private_address,
        });
    spec.validate()?;
    provision::plan(
        &spec,
        provision::existing_sources(&spec, &registry.swarm_dir(name)?)?,
    )?;
    registry.write_spec(&spec)?;
    eprintln!(
        "note: swarmy rewrote {}; comments in it are not kept",
        registry.spec_path(name)?.display()
    );
    // flock is not reentrant: up takes this lock again. A concurrent down can
    // run in this gap; up will reconcile the resulting record before SSH work.
    fs2::FileExt::unlock(&lock)?;
    drop(lock);
    up(
        registry,
        &spec,
        &UpArgs {
            bucket_keys: None,
            no_image: true,
            copy_credential: false,
            image_recipe: "images/base-ubuntu".into(),
        },
        json_output,
    )
    .await
}

/// Build the registration and desired-count requests without network access.
pub(crate) fn elastic_scale_requests(
    spec: &SwarmSpec,
    record: &SwarmRecord,
    group: &str,
    count: u32,
) -> Result<(
    swarmy_api_types::PutElasticGroup,
    swarmy_api_types::SetElasticDesired,
)> {
    use swarmy_cloud::provision::scale::{GroupKind, check_scalable};
    if check_scalable(spec, group)? != GroupKind::Elastic {
        bail!("group \"{group}\" is not elastic");
    }
    let primary = stack_primary(spec, record)?;
    let selected = &spec.groups[group];
    let max = selected.max.context("elastic group has no max")?;
    if count > max {
        bail!("group \"{group}\" is elastic with max = {max}; scale it between 0 and {max}");
    }
    let request = swarmy_api_types::PutElasticGroup {
        idempotency_key: ulid::Ulid::generate().to_string(),
        swarm: spec.name.clone(),
        module: selected.module.clone().into(),
        size: selected.size.clone().context("elastic group has no size")?,
        location: selected
            .location
            .clone()
            .context("elastic group has no location")?,
        sandboxes: selected
            .sandboxes
            .unwrap_or(swarmy_config::default_sandboxes()),
        local_storage: selected
            .local_storage
            .clone()
            .unwrap_or_else(|| swarmy_config::DEFAULT_CREATED_LOCAL_STORAGE.into()),
        max,
        price_per_hour: selected
            .price_per_hour
            .clone()
            .context("elastic group has no price")?,
        currency: selected
            .currency
            .clone()
            .context("elastic group has no currency")?,
        primary_address: primary.public_address.clone(),
        service_user: provision::user(spec).into(),
        autoscale: selected.autoscale,
        external_bucket: provision::external_bucket(spec, record),
        network_id: groups::joined_network(spec, record, group).to_owned(),
        image: selected.image.clone(),
    };
    let desired = swarmy_api_types::SetElasticDesired {
        idempotency_key: ulid::Ulid::generate().to_string(),
        desired: count,
        reason: Some("swarmy swarm scale".into()),
    };
    Ok((request, desired))
}
fn autoscale_off_request(
    record: &swarmy_api_types::ElasticGroup,
    primary: &RecordedHost,
    spec: &SwarmSpec,
) -> swarmy_api_types::PutElasticGroup {
    swarmy_api_types::PutElasticGroup {
        idempotency_key: ulid::Ulid::generate().to_string(),
        swarm: record.swarm.clone(),
        module: record.module.clone(),
        size: record.size.clone(),
        location: record.location.clone(),
        sandboxes: record.sandboxes,
        local_storage: record.local_storage.clone(),
        max: record.max,
        price_per_hour: record.price_per_hour.clone(),
        currency: record.currency.clone(),
        primary_address: primary.public_address.clone(),
        service_user: provision::user(spec).into(),
        autoscale: false,
        external_bucket: record.external_bucket,
        network_id: record.network_id.clone(),
        image: record.image.clone(),
    }
}
fn not_up(spec: &SwarmSpec) -> anyhow::Error {
    anyhow::anyhow!(
        "swarm {} is not up; run swarmy swarm up {} first",
        spec.name,
        spec.name
    )
}
fn swarm_record(registry: &Registry, spec: &SwarmSpec) -> Result<SwarmRecord> {
    registry
        .read_record(&spec.name)?
        .ok_or_else(|| not_up(spec))
}
fn stack_primary<'a>(spec: &SwarmSpec, record: &'a SwarmRecord) -> Result<&'a RecordedHost> {
    record
        .hosts
        .get(&spec.placement.stack)
        .and_then(|hosts| hosts.first())
        .ok_or_else(|| not_up(spec))
}
/// Apply an elastic scale through the swarm's control plane.
pub(crate) async fn scale_elastic(
    registry: &Registry,
    spec: &SwarmSpec,
    group: &str,
    count: u32,
    json_output: bool,
) -> Result<()> {
    let record = swarm_record(registry, spec)?;
    let (definition, desired) = elastic_scale_requests(spec, &record, group, count)?;
    let (client, endpoint) = crate::target::connect_swarm(&spec.name).await?;
    swarmy_client::api_client::call(&endpoint, client.put_elastic_group(group, &definition))
        .await?;
    let record =
        swarmy_client::api_client::call(&endpoint, client.set_elastic_desired(group, &desired))
            .await?;
    if json_output {
        println!("{}", serde_json::to_string(&record)?);
    } else if record.autoscale {
        println!(
            "group {group}: desired {} of max {}, {} machines now; autoscale is on: the provisioner adds machines for sessions waiting for a GPU and removes machines idle for 30 minutes",
            record.desired, record.max, record.applied
        );
    } else {
        println!(
            "group {group}: desired {} of max {}, {} machines now; the provisioner on the primary creates or destroys the rest",
            record.desired, record.max, record.applied
        );
    }
    Ok(())
}
/// One line for each elastic group down brought to zero machines.
pub(crate) fn elastic_down_report(groups: &[swarmy_api_types::ElasticGroup]) -> Vec<String> {
    groups
        .iter()
        .map(|group| {
            format!(
                "elastic group {}: torn down; the provisioner on the primary destroyed its machines",
                group.group
            )
        })
        .collect()
}

/// Groups that still hold machines or have an unreconciled desired count.
pub(crate) fn elastic_down_blockers(groups: &[swarmy_api_types::ElasticGroup]) -> Vec<String> {
    groups
        .iter()
        .filter_map(|group| {
            if group.applied > 0 {
                Some(format!("{}: {} machines", group.group, group.applied))
            } else if !group.settled {
                Some(format!("{}: not yet reconciled", group.group))
            } else {
                None
            }
        })
        .collect()
}

/// Change a node group's size after draining removed machines.
pub(crate) async fn scale(
    registry: &Registry,
    spec: &SwarmSpec,
    group: &str,
    count: u32,
    drain_timeout: u64,
    force: bool,
    json_output: bool,
) -> Result<()> {
    use swarmy_cloud::provision::scale::{ScaleRequest, preflight, scale_swarm};
    let _ = preflight(registry, spec, group, count)?;
    let pair = machines_for(registry, spec, groups::has_module_groups(spec)).await?;
    let settings = swarmy_config::Settings::load_base()?.settings;
    let report = scale_swarm(ScaleRequest {
        registry,
        spec,
        group,
        count,
        drain_timeout: std::time::Duration::from_secs(drain_timeout),
        force,
        machines: machine_refs(pair.as_ref()),
        settings: &settings,
        cache: &registry.dirs().cache.join("core"),
    })
    .await?;
    if json_output {
        println!("{}", serde_json::to_string(&report)?);
    }
    Ok(())
}

#[cfg(test)]
mod elastic_tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    fn scale_fixture() -> (SwarmSpec, RecordedHost, SwarmRecord) {
        let text = r#"
name = "fleet"
lifecycle = "persistent"
[groups.control]
module = "hetzner-cloud"
size = "cpx41"
location = "fsn1"
count = 1
[groups.gpu]
module = "hetzner-cloud"
size = "cpx41"
location = "fsn1"
elastic = true
max = 2
price_per_hour = "0.75"
currency = "EUR"
autoscale = true
[placement]
stack = "control"
services = "control"
nodes = ["gpu"]
"#;
        let spec = SwarmSpec::parse(text).unwrap();
        let primary = RecordedHost {
            name: "control".into(),
            public_address: "198.51.100.7".into(),
            private_address: None,
            ssh_user: "root".into(),
            ssh_key: "key".into(),
            provisioned: true,
        };
        let record = SwarmRecord {
            name: "fleet".into(),
            created_at: jiff::Timestamp::now(),
            api: swarmy_config::swarm::SwarmApi::default(),
            hosts: std::collections::BTreeMap::from([("control".into(), vec![primary.clone()])]),
            bucket_keys: None,
            tofu_state: None,
            tofu_bucket: None,
            tofu_bucket_endpoint: None,
            tofu_bucket_region: None,
            tofu_networks: std::collections::BTreeMap::new(),
        };
        (spec, primary, record)
    }
    #[test]
    fn scale_requests_and_down_blockers() {
        let (mut spec, primary, mut record) = scale_fixture();
        let (definition, desired) = elastic_scale_requests(&spec, &record, "gpu", 1).unwrap();
        assert!(!definition.external_bucket);
        assert_eq!(definition.network_id, "");
        assert_eq!(definition.image, None);
        record.tofu_networks.insert("control".into(), "4711".into());
        spec.groups.get_mut("gpu").unwrap().image = Some("ubuntu-24.04".into());
        let updated = elastic_scale_requests(&spec, &record, "gpu", 1).unwrap().0;
        assert_eq!(updated.network_id, "4711");
        assert_eq!(updated.image.as_deref(), Some("ubuntu-24.04"));
        record.tofu_bucket = Some("swarmy-fleet-000000000001".into());
        assert!(
            elastic_scale_requests(&spec, &record, "gpu", 1)
                .unwrap()
                .0
                .external_bucket
        );
        record.hosts.clear();
        assert!(
            elastic_scale_requests(&spec, &record, "gpu", 1)
                .unwrap_err()
                .to_string()
                .contains("is not up")
        );
        record.hosts.insert("control".into(), vec![primary]);
        assert_eq!(definition.primary_address, "198.51.100.7");
        assert_eq!(definition.service_user, "swarmy");
        assert!(definition.autoscale);
        assert_eq!(definition.swarm, "fleet");
        assert_eq!(definition.sandboxes, 64);
        assert_eq!(definition.local_storage, "dir:/srv/swarmy-local");
        assert_eq!(desired.desired, 1);
        assert_eq!(desired.reason.as_deref(), Some("swarmy swarm scale"));
        assert!(
            elastic_scale_requests(&spec, &record, "gpu", 3)
                .unwrap_err()
                .to_string()
                .contains("max = 2")
        );
        assert!(elastic_scale_requests(&spec, &record, "control", 1).is_err());
    }
    #[test]
    fn autoscale_off_preserves_group_definition_and_down_reports_blockers() {
        let (spec, primary, _record) = scale_fixture();
        let mut group = swarmy_api_types::ElasticGroup {
            swarm: "fleet".into(),
            group: "gpu".into(),
            module: "hetzner-cloud".into(),
            size: "cpx41".into(),
            location: "fsn1".into(),
            sandboxes: 64,
            local_storage: swarmy_config::DEFAULT_CREATED_LOCAL_STORAGE.into(),
            max: 2,
            autoscale: false,
            price_per_hour: "0.75".into(),
            currency: "EUR".into(),
            desired: 0,
            desired_reason: "registered".into(),
            desired_agent: None,
            desired_session: None,
            applied: 0,
            generation: 0,
            reconciled_generation: 0,
            settled: true,
            updated_at: String::new(),
            external_bucket: true,
            network_id: "4711".into(),
            image: Some("ubuntu-24.04".into()),
        };
        group.autoscale = true;
        group.desired = 1;
        group.size = "other-size".into();
        group.location = "other-location".into();
        group.module = "scaleway".into();
        group.max = 1;
        let off = autoscale_off_request(&group, &primary, &spec);
        assert!(!off.autoscale);
        assert!(off.external_bucket);
        assert_eq!(off.network_id, "4711");
        assert_eq!(off.image.as_deref(), Some("ubuntu-24.04"));
        assert_eq!(off.size, group.size);
        assert_eq!(off.location, group.location);
        assert_eq!(off.module, group.module);
        assert_eq!(off.max, group.max);
        assert_eq!(off.primary_address, primary.public_address);
        assert!(elastic_down_blockers(&[group.clone()]).is_empty());
        assert_eq!(
            elastic_down_report(std::slice::from_ref(&group)),
            ["elastic group gpu: torn down; the provisioner on the primary destroyed its machines"]
        );
        assert!(elastic_down_report(&[]).is_empty());
        group.applied = 1;
        assert_eq!(elastic_down_blockers(&[group.clone()]), ["gpu: 1 machines"]);
        group.applied = 0;
        group.settled = false;
        assert_eq!(elastic_down_blockers(&[group]), ["gpu: not yet reconciled"]);
    }
}
