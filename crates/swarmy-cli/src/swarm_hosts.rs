//! CLI presentation for swarms on hosts.
use anyhow::{Result, bail};
use serde_json::json;
use std::path::PathBuf;
use swarmy_cloud::provision::groups::{self, MachineGroups};
use swarmy_cloud::provision::{self, UpRequest};
use swarmy_config::cloud::{CloudCredentials, CloudProvider};
use swarmy_config::swarm::{
    GroupSpec, HostSpec, Lifecycle, Module, Placement, Registry, SwarmSpec,
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
    if let Some(path) = std::env::var_os("SWARMY_TOFU_BIN").filter(|p| !p.is_empty()) {
        if !std::path::Path::new(&path).is_file() {
            bail!("SWARMY_TOFU_BIN={} does not exist", path.to_string_lossy());
        }
        return Ok(Tofu::from_binary(path, &registry.dirs().cache));
    }
    Ok(Tofu::install(
        &registry.dirs().cache,
        &swarmy_infra::opentofu::Source::pinned(),
    )
    .await?)
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

pub(crate) fn cloud_spec(name: &str, args: &CreateCloud) -> Result<SwarmSpec> {
    let module = Module::try_from(args.module.clone()).map_err(|_| anyhow::anyhow!(
        "--on takes hetzner-cloud, aws, or an absolute path to your own module; use --host for machines you own and --local for this machine"
    ))?;
    if matches!(module, Module::Existing | Module::Local)
        || matches!(&module, Module::Path(p) if !p.is_absolute())
    {
        bail!(
            "--on takes hetzner-cloud, aws, or an absolute path to your own module; use --host for machines you own and --local for this machine"
        );
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
        count: Some(count),
        stack_dir: None,
        hosts: Vec::new(),
        sandboxes: if nodes { args.sandboxes } else { None },
        local_storage: if nodes {
            args.local_storage.clone()
        } else {
            None
        },
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
    for provider in [
        CloudProvider::HetznerCloud,
        CloudProvider::HetznerS3,
        CloudProvider::Aws,
    ] {
        let needed = match provider {
            CloudProvider::HetznerCloud | CloudProvider::HetznerS3 => spec
                .groups
                .values()
                .any(|g| g.module == Module::HetznerCloud),
            CloudProvider::Aws => spec.groups.values().any(|g| g.module == Module::Aws),
        };
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
        } else {
            "down deletes its database and swarmy's files on every host (the machines keep running)"
        };
        bail!(
            "swarm \"{0}\" is persistent: {what}. Stop it with swarmy swarm stop {0}, or pass --destroy",
            spec.name
        );
    }
    let tofu_dir = registry.swarm_dir(&spec.name)?.join("tofu");
    let pair = machines_for(registry, spec, tofu_dir.exists()).await?;
    let (removed, destroyed_groups) =
        provision::decommission_swarm(registry, spec, machine_refs(pair.as_ref())).await?;
    if json_output {
        println!(
            "{}",
            json!({"swarm":spec.name,"removed":removed.iter().map(|r| &r.name).collect::<Vec<_>>(),"destroyed_groups":destroyed_groups})
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
    use swarmy_cloud::provision::scale::{ScaleRequest, check_scalable, scale_swarm};
    let _ = check_scalable(spec, group)?;
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
