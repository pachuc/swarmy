//! CLI presentation for hosts provided by an existing-host group.
use anyhow::{Result, bail};
use serde_json::json;
use std::path::PathBuf;
use swarmy_cloud::provision::{self, UpRequest};
use swarmy_config::swarm::{
    GroupSpec, HostSpec, Lifecycle, Module, Placement, Registry, SwarmSpec,
};

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
    let reports = provision::provision_swarm(UpRequest {
        registry,
        spec,
        sources,
        settings: &settings,
        copy_credential: args.copy_credential,
        recipe: recipe.as_deref(),
        cache: &registry.dirs().cache.join("core"),
        machines: None,
        bucket_keys: args.bucket_keys.clone(),
    })
    .await?;
    if json_output {
        println!("{}", json!({"swarm":spec.name,"hosts":reports}));
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
        bail!(
            "swarm \"{}\" is persistent: down deletes its database and swarmy's files on every host (the machines keep running). Stop it with swarmy swarm stop {}, or pass --destroy",
            spec.name,
            spec.name
        );
    }
    let removed = provision::decommission_swarm(registry, spec, None).await?;
    if json_output {
        println!(
            "{}",
            json!({"swarm":spec.name,"removed":removed.iter().map(|r| &r.name).collect::<Vec<_>>()})
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
