//! Registry commands for the selected swarm.
use anyhow::{Context, Result, bail};
use clap::Subcommand;
use serde_json::json;
use std::path::PathBuf;
use swarmy_cloud::tunnel::Tunnels;
use swarmy_config::{
    paths::Dirs,
    swarm::{self, Lifecycle, Registry, Swarm, Target},
};

pub(crate) fn parse_name(value: &str) -> Result<String, swarmy_config::Error> {
    swarm::validate_swarm_name(value)?;
    Ok(value.to_owned())
}
#[derive(Subcommand)]
pub(crate) enum Command {
    /// Write a new swarm spec; --local runs the stack and services on this machine
    #[command(group(clap::ArgGroup::new("kind").required(true).args(["local", "host", "on"])))]
    Create {
        #[arg(value_parser = parse_name)]
        name: String,
        /// Run the backing stack and services on this machine
        #[arg(long, conflicts_with = "host")]
        local: bool,
        /// Adopt a host at this IPv4 address
        #[arg(long, requires = "ssh_key")]
        host: Option<String>,
        /// Create machines with an `OpenTofu` module
        #[arg(long, requires = "size")]
        on: Option<String>,
        /// Machine size (required with --on)
        #[arg(long, requires = "on")]
        size: Option<String>,
        /// Machine location (defaults to fsn1 for hetzner-cloud)
        #[arg(long, requires = "on", required_if_eq("on", "aws"))]
        location: Option<String>,
        /// Number of worker machines (default: 0)
        #[arg(long, requires = "on", default_value_t = 0)]
        nodes: u32,
        /// Worker machine size (defaults to --size)
        #[arg(long, requires = "on")]
        node_size: Option<String>,
        /// SSH login on the host (default: root)
        #[arg(long, default_value = "root", requires = "host", conflicts_with = "on")]
        ssh_user: String,
        /// Private SSH key for the host
        #[arg(long, requires = "host", conflicts_with = "local")]
        ssh_key: Option<PathBuf>,
        /// IPv4 address for traffic between hosts (default: --host)
        #[arg(long, requires = "host")]
        private_address: Option<String>,
        /// Sandbox capacity on node-running hosts (default: 64)
        #[arg(long, conflicts_with = "local")]
        sandboxes: Option<u32>,
        /// Node storage device or `dir:/path`
        #[arg(long, conflicts_with = "local")]
        local_storage: Option<String>,
        /// Account that owns the installed services (default: swarmy)
        #[arg(long, requires = "host")]
        service_user: Option<String>,
        /// Ephemeral or persistent lifecycle (default: persistent)
        #[arg(long, value_enum, conflicts_with = "local")]
        lifecycle: Option<LifecycleArg>,
        /// Keep data on down unless --destroy is given (local swarms default to ephemeral)
        #[arg(long, conflicts_with_all = ["host", "on"])]
        persistent: bool,
        /// Backing stack directory (default: the checkout's .dev inside a checkout, else the swarm's data directory)
        #[arg(long, value_name = "PATH", conflicts_with_all = ["host", "on"])]
        stack_dir: Option<PathBuf>,
    },
    /// Bring a swarm up, restarting running services
    Up {
        #[arg(value_parser = parse_name)]
        name: String,
        /// Run this swarmy-core instead of the matching release
        #[arg(long, env = "SWARMY_CORE_BIN", value_name = "PATH")]
        core_bin: Option<PathBuf>,
        /// Do not download and register the release base image when no default image is configured
        #[arg(long, conflicts_with = "image_recipe")]
        no_image: bool,
        /// Acknowledge copying the credential and keyring onto host services
        #[arg(long)]
        copy_credential: bool,
        /// Image recipe carried by this client
        #[arg(long)]
        image_recipe: Option<PathBuf>,
        /// Static S3 access key for this swarm's bucket
        #[arg(long)]
        s3_access_key: Option<String>,
        /// Read the static S3 secret from a 0600 file
        #[arg(long, conflicts_with = "s3_secret_stdin")]
        s3_secret_file: Option<PathBuf>,
        /// Read the static S3 secret from standard input
        #[arg(long)]
        s3_secret_stdin: bool,
    },
    /// Show what `OpenTofu` would create or change for a swarm's machine groups, without applying it
    Plan {
        #[arg(value_parser = parse_name)]
        name: String,
    },
    /// Change the number of machines in a node group; removed machines are drained first, highest-numbered first
    Scale {
        #[arg(value_parser = parse_name)]
        name: String,
        /// A group listed in placement.nodes that runs neither the stack nor the services
        group: String,
        /// New number of machines (0-50; 0 only for node groups whose machines `OpenTofu` creates)
        #[arg(value_parser = clap::value_parser!(u32).range(0..=50))]
        count: u32,
        /// Seconds to wait on each removed node for running sandbox commands to finish
        #[arg(long, default_value_t = 600)]
        drain_timeout: u64,
        /// Stop removed nodes even if sandbox commands are still running (or the node is unreachable) at the deadline
        #[arg(long)]
        force: bool,
    },
    /// Start a stopped swarm again
    Start {
        #[arg(value_parser = parse_name)]
        name: String,
        /// Run this swarmy-core instead of the matching release
        #[arg(long, env = "SWARMY_CORE_BIN", value_name = "PATH")]
        core_bin: Option<PathBuf>,
    },
    /// Stop a swarm, keeping its data
    Stop {
        #[arg(value_parser = parse_name)]
        name: String,
    },
    /// Delete swarm data; persistent swarms require --destroy
    Down {
        #[arg(value_parser = parse_name)]
        name: String,
        /// Delete a persistent swarm's data
        #[arg(long)]
        destroy: bool,
    },
    /// Show a swarm's processes and uptime
    Status {
        #[arg(value_parser = parse_name)]
        name: String,
    },
    /// Add hosts to an existing-host node group
    Node {
        #[command(subcommand)]
        command: NodeCommand,
    },
    /// Upgrade a host swarm's binaries, units, and services in place
    Upgrade {
        #[arg(value_parser = parse_name)]
        name: String,
        /// Restart only the control services
        #[arg(long)]
        services_only: bool,
        /// Allow a development checkout with uncommitted changes
        #[arg(long)]
        allow_dirty: bool,
        /// Seconds to wait for sandboxes to drain
        #[arg(long, default_value_t = 600)]
        drain_timeout: u64,
    },
    /// Follow service logs
    Logs {
        #[arg(value_parser = parse_name)]
        name: String,
        #[arg(value_enum)]
        service: Option<crate::local::Service>,
        /// Follow logs on this host of a host swarm
        #[arg(long)]
        host: Option<String>,
        /// Systemd unit on a host swarm (default: swarmyd)
        #[arg(long)]
        unit: Option<String>,
    },
    #[command(hide = true)]
    Supervise { state: PathBuf, core: PathBuf },
    /// List registered swarms; `*` marks the current one
    Ls,
    /// List elastic machines with state, age, node, cost and cause
    Machines {
        /// Include destroyed and failed-before-creation machines
        #[arg(long)]
        all: bool,
    },
    /// Show a swarm's API, record, and spec (default: the swarm this command would use)
    Show {
        #[arg(value_parser = parse_name)]
        name: Option<String>,
    },
    /// Make NAME the current swarm
    Use {
        #[arg(value_parser = parse_name)]
        name: String,
    },
    /// Forget a swarm without touching its machines
    Rm {
        #[arg(value_parser = parse_name)]
        name: String,
        /// Forget even if machines or infrastructure state are recorded
        #[arg(long)]
        force: bool,
    },
    /// Register a running API that swarmy does not manage
    Attach {
        #[arg(value_parser = parse_name)]
        name: String,
        #[arg(long)]
        url: String,
        /// Read the API token from standard input
        #[arg(long, required = true)]
        token_stdin: bool,
    },
}
#[derive(Clone, Copy, clap::ValueEnum)]
pub(crate) enum LifecycleArg {
    Persistent,
    Ephemeral,
}
impl From<LifecycleArg> for Lifecycle {
    fn from(value: LifecycleArg) -> Self {
        match value {
            LifecycleArg::Persistent => Self::Persistent,
            LifecycleArg::Ephemeral => Self::Ephemeral,
        }
    }
}
#[derive(Subcommand)]
pub(crate) enum NodeCommand {
    /// List HOST in GROUP of swarm NAME, then provision it
    Add {
        #[arg(value_parser = parse_name)]
        name: String,
        group: String,
        #[arg(long)]
        host: String,
        #[arg(long, default_value = "root")]
        ssh_user: String,
        #[arg(long)]
        ssh_key: PathBuf,
        #[arg(long)]
        private_address: Option<String>,
    },
}
pub(crate) fn managed_spec(registry: &Registry, name: &str) -> Result<swarm::SwarmSpec> {
    registry.read(name)?.spec.ok_or_else(|| {
        anyhow::anyhow!(
            "swarm \"{name}\" has no spec; it is an attached API, which swarmy does not provision"
        )
    })
}
fn host_spec(registry: &Registry, name: &str) -> Result<Option<swarm::SwarmSpec>> {
    let spec = managed_spec(registry, name)?;
    Ok(spec.local_group().is_none().then_some(spec))
}
// Lifecycle commands follow the recorded inventory even if the spec was edited
// after up. Up still checks that every group is a supported source.
fn lifecycle_host_spec(registry: &Registry, name: &str) -> Result<Option<swarm::SwarmSpec>> {
    let spec = managed_spec(registry, name)?;
    if registry
        .read_record(name)?
        .is_some_and(|r| !r.hosts.is_empty())
    {
        return Ok(Some(spec));
    }
    host_spec(registry, name)
}
fn kind(s: &Swarm) -> &'static str {
    if s.spec.is_some() {
        "managed"
    } else if s
        .record
        .as_ref()
        .and_then(|r| r.api.url.as_ref().filter(|url| !url.is_empty()))
        .is_some()
    {
        "attached"
    } else {
        "incomplete"
    }
}
fn api(s: &Swarm) -> String {
    if let Some(url) = s
        .record
        .as_ref()
        .and_then(|r| r.api.url.as_ref().filter(|url| !url.is_empty()))
    {
        return url.clone();
    }
    if s.spec.is_some()
        && s.record
            .as_ref()
            .and_then(|r| r.api.token.as_ref().filter(|token| !token.is_empty()))
            .is_some()
    {
        return "ssh tunnel".into();
    }
    "not up".into()
}
fn print_value(value: &serde_json::Value, human: impl std::fmt::Display, json_output: bool) {
    if json_output {
        println!("{value}");
    } else {
        println!("{human}");
    }
}
fn show(registry: &Registry, swarm: &Swarm, json_output: bool) -> Result<()> {
    let current = registry.current()?.as_deref() == Some(&swarm.name);
    let spec_path = swarm
        .spec
        .as_ref()
        .map(|_| registry.spec_path(&swarm.name))
        .transpose()?;
    let hosts = swarm.record.as_ref().map(|r| &r.hosts);
    let summary = hosts
        .map(|groups| {
            groups
                .iter()
                .map(|(group, hosts)| format!("{group}={}", hosts.len()))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default();
    let url = api(swarm);
    let token_recorded = swarm
        .record
        .as_ref()
        .and_then(|r| r.api.token.as_ref().filter(|token| !token.is_empty()))
        .is_some();
    print_value(
        &json!({"name": swarm.name, "current": current, "kind": kind(swarm), "api_url": url,
        "token_recorded": token_recorded, "spec_path": spec_path, "spec": swarm.spec,
        "hosts": hosts, "tofu_state": swarm.record.as_ref().and_then(|r| r.tofu_state.as_ref()) }),
        format!(
            "swarm: {}{}\nkind: {}\napi: {}\ntoken: {}\nspec: {}\nhosts: {}",
            swarm.name,
            if current { " (current)" } else { "" },
            kind(swarm),
            url,
            if token_recorded {
                "recorded"
            } else {
                "missing"
            },
            spec_path
                .as_ref()
                .map_or_else(|| "none".into(), |p| p.display().to_string()),
            summary
        ),
        json_output,
    );
    Ok(())
}
fn list(registry: &Registry, json_output: bool) -> Result<()> {
    let current = registry.current()?;
    let names = registry.list()?;
    if current.as_ref().is_some_and(|c| !names.contains(c)) {
        eprintln!(
            "current swarm \"{}\" no longer exists; run swarmy swarm use NAME",
            current.as_deref().unwrap_or_default()
        );
    }
    let mut entries = Vec::new();
    for name in names {
        match registry.read(&name) {
            Ok(swarm) => entries.push(json!({"name":name,"current":current.as_deref()==Some(&name),"kind":kind(&swarm),"api_url":api(&swarm)})),
            Err(error) => {
                eprintln!("warning: swarm {name}: {error:#}");
                entries.push(json!({"name":name,"current":current.as_deref()==Some(&name),"kind":"unreadable","api_url":"not up"}));
            }
        }
    }
    if json_output {
        println!("{}", json!(entries));
    } else if entries.is_empty() {
        println!(
            "no swarms registered; register a running API with swarmy swarm attach NAME --url URL --token-stdin"
        );
    } else {
        for entry in entries {
            println!(
                "{} {} {} {}",
                if entry["current"] == true { "*" } else { " " },
                entry["name"].as_str().unwrap_or_default(),
                entry["kind"].as_str().unwrap_or_default(),
                entry["api_url"].as_str().unwrap_or_default()
            );
        }
    }
    Ok(())
}
async fn remove(registry: &Registry, name: &str, force: bool, json_output: bool) -> Result<()> {
    let swarm = registry.read(name)?;
    if let Some(spec) = &swarm.spec {
        crate::local::refuse_running(registry, spec)?;
    }
    let hosts: usize = swarm
        .record
        .as_ref()
        .map_or(0, |r| r.hosts.values().map(Vec::len).sum());
    let detail = if hosts == 0 {
        "infrastructure state recorded".to_owned()
    } else {
        format!("{hosts} hosts")
    };
    if !force
        && swarm
            .record
            .as_ref()
            .is_some_and(|r| !r.hosts.is_empty() || r.tofu_state.is_some())
    {
        bail!(
            "swarm \"{name}\" has machines or infrastructure state recorded ({detail}); swarmy swarm rm only forgets registrations. Tear it down first, or pass --force to forget it and leave the machines running"
        );
    }
    swarmy_core::ignore_best_effort(
        Tunnels::ssh()
            .disconnect(&registry.swarm_dir(name)?.join("tunnel"))
            .await,
        "close the swarm's SSH tunnel",
    );
    let was_current = registry.current()?.as_deref() == Some(name);
    registry.remove(name)?;
    print_value(
        &json!({"removed":name,"was_current":was_current}),
        format!(
            "removed swarm {name}{}",
            if was_current {
                "; no swarm is current now"
            } else {
                ""
            }
        ),
        json_output,
    );
    Ok(())
}
fn attach(registry: &Registry, name: &str, url: &str, json_output: bool) -> Result<()> {
    let url = url::Url::parse(url).context("--url must be an http or https URL")?;
    if !matches!(url.scheme(), "http" | "https") || url.host().is_none() {
        bail!("--url must be an http or https URL");
    }
    let token = std::io::read_to_string(std::io::stdin())?.trim().to_owned();
    if token.is_empty() {
        bail!(
            "no token on stdin; pipe it in, for example: printf '%s' \"$TOKEN\" | swarmy swarm attach NAME --url URL --token-stdin"
        );
    }
    let url = url.as_str().trim_end_matches('/');
    registry.attach(name, url, &token)?;
    print_value(
        &json!({"attached":name,"api_url":url}),
        format!("attached swarm {name} at {url}; make it current with swarmy swarm use {name}"),
        json_output,
    );
    Ok(())
}
fn run_create(registry: &Registry, command: Command, json_output: bool) -> Result<()> {
    match command {
        Command::Create {
            name,
            local,
            host,
            on,
            size,
            location,
            nodes,
            node_size,
            ssh_user,
            ssh_key,
            private_address,
            sandboxes,
            local_storage,
            service_user,
            lifecycle,
            persistent,
            stack_dir,
        } => {
            if local {
                return crate::local::create(registry, &name, persistent, stack_dir, json_output);
            }
            let path = registry.spec_path(&name)?;
            if path.exists() {
                bail!("swarm \"{name}\" already has a spec at {}", path.display());
            }
            let (spec, next) = if let Some(address) = host {
                (
                    crate::swarm_hosts::host_spec(
                        registry,
                        &name,
                        &crate::swarm_hosts::CreateHost {
                            address,
                            ssh_user,
                            ssh_key: ssh_key.expect("--host requires --ssh-key"),
                            private_address,
                            sandboxes,
                            local_storage,
                            service_user,
                            lifecycle: lifecycle.map(Into::into),
                        },
                    )?,
                    None,
                )
            } else if let Some(module) = on {
                let spec = crate::swarm_hosts::cloud_spec(
                    &name,
                    &crate::swarm_hosts::CreateCloud {
                        module,
                        size,
                        location,
                        nodes,
                        node_size,
                        sandboxes,
                        local_storage,
                        lifecycle: lifecycle.map(Into::into),
                    },
                )?;
                let next = (!json_output)
                    .then(|| crate::swarm_hosts::cloud_next(registry, &spec))
                    .transpose()?;
                (spec, next)
            } else {
                bail!("choose --local, --host ADDRESS or --on MODULE");
            };
            registry.write_spec(&spec)?;
            print_value(
                &json!({"created":name}),
                format!("created swarm {name}"),
                json_output,
            );
            if let Some(lines) = next {
                for line in lines {
                    println!("{line}");
                }
            }
        }
        _ => unreachable!("create dispatch"),
    }
    Ok(())
}
async fn run_up_start(
    registry: &Registry,
    command: Command,
    json_output: bool,
    allow_version_mismatch: bool,
) -> Result<()> {
    match command {
        Command::Up {
            name,
            core_bin,
            no_image,
            copy_credential,
            image_recipe,
            s3_access_key,
            s3_secret_file,
            s3_secret_stdin,
        } => {
            let has_keys = s3_access_key.is_some() || s3_secret_file.is_some() || s3_secret_stdin;
            let bucket_keys = if let Some(access_key) = s3_access_key {
                let secret_key = if let Some(path) = s3_secret_file.as_deref() {
                    swarmy_cloud::secrets::read_secret_file(path)?
                } else if s3_secret_stdin {
                    swarmy_cloud::secrets::read_secret_stdin()?
                } else {
                    bail!("--s3-access-key requires --s3-secret-file or --s3-secret-stdin");
                };
                Some(swarm::StaticKeys {
                    access_key,
                    secret_key,
                })
            } else {
                if s3_secret_file.is_some() || s3_secret_stdin {
                    bail!("--s3-secret-file and --s3-secret-stdin require --s3-access-key");
                }
                None
            };
            if let Some(spec) = host_spec(registry, &name)? {
                crate::swarm_hosts::up(
                    registry,
                    &spec,
                    &crate::swarm_hosts::UpArgs {
                        copy_credential,
                        bucket_keys,
                        no_image,
                        image_recipe: image_recipe.unwrap_or_else(|| "images/base-ubuntu".into()),
                    },
                    json_output,
                )
                .await?;
            } else {
                if json_output {
                    bail!("--json is not supported for local swarm up");
                }
                if has_keys {
                    bail!("S3 keys apply only to swarms on hosts");
                }
                if copy_credential {
                    bail!("--copy-credential applies only to swarms on hosts");
                }
                if image_recipe.is_some() {
                    bail!("--image-recipe applies only to swarms on hosts");
                }
                crate::local::up(
                    registry,
                    &name,
                    crate::local::Start {
                        core_bin,
                        register_image: !no_image,
                        require_record: false,
                        allow_version_mismatch,
                    },
                )
                .await?;
            }
        }
        Command::Start { name, core_bin } => {
            if let Some(spec) = lifecycle_host_spec(registry, &name)? {
                crate::swarm_hosts::start(registry, &spec, json_output).await?;
            } else {
                if json_output {
                    bail!("--json is not supported for local swarm start");
                }
                crate::local::up(
                    registry,
                    &name,
                    crate::local::Start {
                        core_bin,
                        register_image: false,
                        require_record: true,
                        allow_version_mismatch,
                    },
                )
                .await?;
            }
        }
        _ => unreachable!("up/start dispatch"),
    }
    Ok(())
}
async fn run_host_lifecycle(
    registry: &Registry,
    command: Command,
    json_output: bool,
) -> Result<()> {
    match command {
        Command::Stop { name } => {
            if let Some(spec) = lifecycle_host_spec(registry, &name)? {
                crate::swarm_hosts::stop(registry, &spec, json_output).await?;
            } else {
                if json_output {
                    bail!("--json is not supported for local swarm stop");
                }
                crate::local::stop(registry, &name).await?;
            }
        }
        Command::Down { name, destroy } => {
            if let Some(spec) = lifecycle_host_spec(registry, &name)? {
                crate::swarm_hosts::down(registry, &spec, destroy, json_output).await?;
            } else {
                if json_output {
                    bail!("--json is not supported for local swarm down");
                }
                crate::local::down(registry, &name, destroy).await?;
            }
        }
        Command::Status { name } => {
            if let Some(spec) = lifecycle_host_spec(registry, &name)? {
                crate::swarm_hosts::status(registry, &spec, json_output).await?;
            } else {
                if json_output {
                    bail!("--json is not supported for local swarm status");
                }
                crate::local::show_status(registry, &name).await?;
            }
        }
        Command::Node {
            command:
                NodeCommand::Add {
                    name,
                    group,
                    host,
                    ssh_user,
                    ssh_key,
                    private_address,
                },
        } => {
            Box::pin(crate::swarm_hosts::node_add(
                registry,
                &name,
                crate::swarm_hosts::NodeAdd {
                    group,
                    address: host,
                    ssh_user,
                    ssh_key,
                    private_address,
                },
                json_output,
            ))
            .await?;
        }
        _ => unreachable!("lifecycle dispatch"),
    }
    Ok(())
}
async fn run_host_observation(
    registry: &Registry,
    command: Command,
    json_output: bool,
) -> Result<()> {
    match command {
        Command::Logs {
            name,
            service,
            host,
            unit,
        } => {
            if let Some(spec) = lifecycle_host_spec(registry, &name)? {
                if service.is_some() {
                    bail!("a positional service applies only to local swarms");
                }
                swarmy_cloud::provision::logs(
                    registry,
                    &spec,
                    host.as_deref(),
                    unit.as_deref().unwrap_or("swarmyd"),
                )
                .await?;
            } else {
                if host.is_some() || unit.is_some() {
                    bail!("--host and --unit apply only to swarms on hosts");
                }
                crate::local::follow_logs(registry, &name, service).await?;
            }
        }
        Command::Upgrade {
            name,
            services_only,
            allow_dirty,
            drain_timeout,
        } => {
            let Some(spec) = lifecycle_host_spec(registry, &name)? else {
                bail!(
                    "upgrade applies to swarms on hosts; a local swarm runs this client's binaries"
                );
            };
            swarmy_cloud::provision::upgrade_swarm_with(
                registry,
                &spec,
                swarmy_cloud::provision::UpgradeOptions {
                    allow_dirty,
                    services_only,
                    drain_timeout: std::time::Duration::from_secs(drain_timeout),
                    json: json_output,
                    cache: registry.dirs().cache.join("core"),
                },
                |summary| {
                    if json_output {
                        println!("{}", serde_json::to_string(summary)?);
                        std::io::Write::flush(&mut std::io::stdout())?;
                    }
                    Ok(())
                },
            )
            .await?;
        }
        _ => unreachable!("host observation dispatch"),
    }
    Ok(())
}
pub(crate) async fn run(
    command: Command,
    json_output: bool,
    allow_version_mismatch: bool,
) -> Result<()> {
    if let Command::Supervise { state, core } = command {
        return crate::local::supervise(&state, &core).await;
    }
    if json_output && matches!(command, Command::Logs { .. }) {
        bail!("--json is not supported for swarm logs; run without it for human-readable output");
    }
    // Preserve the local command's early JSON rejection when there is no swarm
    // to dispatch, while host swarms may return structured status.
    let registry = Registry::new(Dirs::from_process()?);
    if json_output
        && let Command::Status { name } = &command
        && registry.read_spec(name)?.is_none()
        && registry.read_record(name)?.is_none()
    {
        bail!("--json is not supported for swarm status without a managed spec");
    }
    match command {
        Command::Supervise { .. } => unreachable!("supervisor dispatched above"),
        command @ Command::Create { .. } => run_create(&registry, command, json_output)?,
        command @ (Command::Up { .. } | Command::Start { .. }) => {
            Box::pin(run_up_start(
                &registry,
                command,
                json_output,
                allow_version_mismatch,
            ))
            .await?;
        }
        command @ (Command::Stop { .. }
        | Command::Down { .. }
        | Command::Status { .. }
        | Command::Node { .. }) => {
            Box::pin(run_host_lifecycle(&registry, command, json_output)).await?;
        }
        command @ (Command::Logs { .. } | Command::Upgrade { .. }) => {
            run_host_observation(&registry, command, json_output).await?;
        }
        Command::Scale {
            name,
            group,
            count,
            drain_timeout,
            force,
        } => {
            let spec = managed_spec(&registry, &name)?;
            if spec.local_group().is_some() {
                bail!(
                    "swarm \"{name}\" runs on this machine; scale applies to swarms on hosts or cloud machines"
                );
            }
            Box::pin(crate::swarm_hosts::scale(
                &registry,
                &spec,
                &group,
                count,
                drain_timeout,
                force,
                json_output,
            ))
            .await?;
        }
        Command::Plan { name } => {
            let spec = managed_spec(&registry, &name)?;
            crate::swarm_hosts::plan(&registry, &spec, json_output).await?;
        }
        Command::Ls => list(&registry, json_output)?,
        Command::Machines { all } => crate::machines::list(all, json_output).await?,
        Command::Show { name } => {
            if let Some(name) = name {
                show(&registry, &registry.read(&name)?, json_output)?;
            } else {
                match swarm::resolve_process(crate::target::flag())?.1 {
                    Target::Swarm(s) => show(&registry, &s, json_output)?,
                    Target::Endpoint { url, .. } => print_value(
                        &json!({"kind":"endpoint","api_url":url}),
                        format!("SWARMY_API_URL {url} (not registered)"),
                        json_output,
                    ),
                }
            }
        }
        Command::Use { name } => {
            registry.set_current(&name)?;
            print_value(
                &json!({"current":name}),
                format!("current swarm: {name}"),
                json_output,
            );
        }
        Command::Rm { name, force } => remove(&registry, &name, force, json_output).await?,
        Command::Attach {
            name,
            url,
            token_stdin: _,
        } => attach(&registry, &name, &url, json_output)?,
    }
    Ok(())
}
