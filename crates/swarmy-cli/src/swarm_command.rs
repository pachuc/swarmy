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
    Create {
        #[arg(value_parser = parse_name)]
        name: String,
        /// Run the backing stack and the services on this machine (the only kind so far)
        #[arg(long, conflicts_with = "host")]
        local: bool,
        /// Adopt a host at this IPv4 address
        #[arg(long, requires = "ssh_key")]
        host: Option<String>,
        #[arg(long, default_value = "root")]
        ssh_user: String,
        #[arg(long)]
        ssh_key: Option<PathBuf>,
        #[arg(long)]
        private_address: Option<String>,
        #[arg(long)]
        sandboxes: Option<u32>,
        #[arg(long)]
        local_storage: Option<String>,
        #[arg(long)]
        service_user: Option<String>,
        #[arg(long, value_enum)]
        lifecycle: Option<LifecycleArg>,
        /// Keep data on down unless --destroy is given (local swarms default to ephemeral)
        #[arg(long)]
        persistent: bool,
        /// Backing stack directory (default: the checkout's .dev inside a checkout, else the swarm's data directory)
        #[arg(long, value_name = "PATH")]
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
        #[arg(long)]
        no_image: bool,
        /// Acknowledge copying the credential and keyring onto host services
        #[arg(long)]
        copy_credential: bool,
        /// Image recipe carried by this client
        #[arg(long, default_value = "images/base-ubuntu")]
        image_recipe: PathBuf,
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
    /// Follow service logs
    Logs {
        #[arg(value_parser = parse_name)]
        name: String,
        #[arg(value_enum)]
        service: Option<crate::local::Service>,
    },
    #[command(hide = true)]
    Supervise { state: PathBuf, core: PathBuf },
    /// List registered swarms; `*` marks the current one
    Ls,
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
fn managed_spec(registry: &Registry, name: &str) -> Result<swarm::SwarmSpec> {
    registry.read_spec(name)?.ok_or_else(|| {
        anyhow::anyhow!(
            "swarm \"{name}\" has no spec; it is an attached API, which swarmy does not provision"
        )
    })
}
fn host_spec(registry: &Registry, name: &str) -> Result<Option<swarm::SwarmSpec>> {
    let spec = managed_spec(registry, name)?;
    if swarmy_cloud::provision::is_host_swarm(&spec) {
        return Ok(Some(spec));
    }
    if spec.local_group().is_some() {
        return Ok(None);
    }
    let modules = spec
        .groups
        .values()
        .map(|g| String::from(g.module.clone()))
        .collect::<Vec<_>>()
        .join(", ");
    bail!("swarm \"{name}\" uses module groups ({modules}); swarmy cannot create machines yet")
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
pub(crate) async fn run(
    command: Command,
    json_output: bool,
    allow_version_mismatch: bool,
) -> Result<()> {
    if let Command::Supervise { state, core } = command {
        return crate::local::supervise(&state, &core).await;
    }
    if false
        && json_output
        && matches!(
            command,
            Command::Up { .. }
                | Command::Start { .. }
                | Command::Stop { .. }
                | Command::Down { .. }
                | Command::Status { .. }
                | Command::Logs { .. }
        )
    {
        bail!(
            "--json is not supported for swarm up, start, stop, down, status, or logs; run without it for human-readable output"
        );
    }
    let registry = Registry::new(Dirs::from_process()?);
    match command {
        Command::Supervise { .. } => unreachable!("supervisor dispatched above"),
        Command::Create {
            name,
            local,
            host,
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
            if let Some(address) = host {
                let path = registry.spec_path(&name)?;
                if path.exists() {
                    bail!("swarm \"{name}\" already has a spec at {}", path.display());
                }
                let spec = crate::swarm_hosts::host_spec(
                    &name,
                    &crate::swarm_hosts::CreateHost {
                        address,
                        ssh_user,
                        ssh_key: ssh_key.context("--host requires --ssh-key")?,
                        private_address,
                        sandboxes,
                        local_storage,
                        service_user,
                        lifecycle: lifecycle.map(Into::into),
                    },
                )?;
                registry.write_spec(&spec)?;
                print_value(
                    &json!({"created":name}),
                    format!("created swarm {name}"),
                    json_output,
                );
            } else if local {
                crate::local::create(&registry, &name, persistent, stack_dir, json_output)?;
            } else {
                bail!("choose --local or --host ADDRESS");
            }
        }
        Command::Up {
            name,
            core_bin,
            no_image,
            copy_credential,
            image_recipe,
        } => {
            if let Some(spec) = host_spec(&registry, &name)? {
                crate::swarm_hosts::up(
                    &registry,
                    &spec,
                    &crate::swarm_hosts::UpArgs {
                        copy_credential,
                        no_image,
                        image_recipe,
                    },
                    json_output,
                )
                .await?;
            } else {
                if json_output {
                    bail!("--json is not supported for local swarm up");
                }
                if copy_credential {
                    bail!("--copy-credential applies only to swarms on hosts");
                }
                crate::local::up(
                    &registry,
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
            if let Some(spec) = host_spec(&registry, &name)? {
                crate::swarm_hosts::start(&registry, &spec, json_output).await?;
            } else {
                if json_output {
                    bail!("--json is not supported for local swarm start");
                }
                crate::local::up(
                    &registry,
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
        Command::Stop { name } => {
            if let Some(spec) = host_spec(&registry, &name)? {
                crate::swarm_hosts::stop(&registry, &spec, json_output).await?;
            } else {
                if json_output {
                    bail!("--json is not supported for local swarm stop");
                }
                crate::local::stop(&registry, &name).await?;
            }
        }
        Command::Down { name, destroy } => {
            if let Some(spec) = host_spec(&registry, &name)? {
                crate::swarm_hosts::down(&registry, &spec, destroy, json_output).await?;
            } else {
                if json_output {
                    bail!("--json is not supported for local swarm down");
                }
                crate::local::down(&registry, &name, destroy).await?;
            }
        }
        Command::Status { name } => {
            if let Some(spec) = host_spec(&registry, &name)? {
                crate::swarm_hosts::status(&registry, &spec, json_output).await?;
            } else {
                if json_output {
                    bail!("--json is not supported for local swarm status");
                }
                crate::local::show_status(&registry, &name).await?;
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
            crate::swarm_hosts::node_add(
                &registry,
                &name,
                crate::swarm_hosts::NodeAdd {
                    group,
                    address: host,
                    ssh_user,
                    ssh_key,
                    private_address,
                },
                json_output,
            )
            .await?;
        }
        Command::Logs { name, service } => {
            crate::local::follow_logs(&registry, &name, service).await?;
        }
        Command::Ls => list(&registry, json_output)?,
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
/// Transitional registration of the local dev API.
pub(crate) fn register_dev(dirs: Dirs, url: &str, token: &str) -> Result<()> {
    let registry = Registry::new(dirs);
    if registry.read_spec("dev")?.is_some() {
        eprintln!("warning: a swarm spec named dev exists; not registering this API as swarm dev");
        return Ok(());
    }
    registry.attach("dev", url, token)?;
    if registry.current()?.is_none() {
        registry.set_current("dev")?;
        println!("swarm dev: {url} (current)");
    } else {
        println!("swarm dev: {url} (select it with swarmy swarm use dev or --swarm dev)");
    }
    Ok(())
}
