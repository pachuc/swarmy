//! Registry commands for the selected swarm.
use anyhow::{Context, Result, bail};
use clap::Subcommand;
use serde_json::json;
use swarmy_cloud::tunnel::Tunnels;
use swarmy_config::{
    paths::Dirs,
    swarm::{self, Registry, Swarm, Target},
};

pub(crate) fn parse_name(value: &str) -> Result<String, swarmy_config::Error> {
    swarm::validate_swarm_name(value)?;
    Ok(value.to_owned())
}
#[derive(Subcommand)]
pub(crate) enum Command {
    /// List registered swarms; `*` marks the current one
    Ls,
    /// Show a swarm's API, record, and spec
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
fn kind(s: &Swarm) -> &'static str {
    if s.spec.is_some() {
        "managed"
    } else if s.record.as_ref().and_then(|r| r.api.url.as_ref()).is_some() {
        "attached"
    } else {
        "incomplete"
    }
}
fn api(s: &Swarm) -> String {
    if let Some(url) = s.record.as_ref().and_then(|r| r.api.url.as_ref()) {
        return url.clone();
    }
    if s.spec.is_some()
        && s.record
            .as_ref()
            .and_then(|r| r.api.token.as_ref())
            .is_some()
    {
        return "ssh tunnel".into();
    }
    "not up".into()
}
fn print_value(value: serde_json::Value, human: impl std::fmt::Display, json_output: bool) {
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
        .and_then(|r| r.api.token.as_ref())
        .is_some();
    print_value(
        json!({"name": swarm.name, "current": current, "kind": kind(swarm), "api_url": url,
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
pub(crate) async fn run(command: Command, json_output: bool) -> Result<()> {
    let registry = Registry::new(Dirs::from_process()?);
    match command {
        Command::Ls => {
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
                    Ok(s) => entries.push(json!({"name":name,"current":current.as_deref()==Some(&name),"kind":kind(&s),"api_url":api(&s)})),
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
        }
        Command::Show { name } => {
            if let Some(name) = name {
                show(&registry, &registry.read(&name)?, json_output)?;
            } else {
                match swarm::resolve_process(crate::target::flag())?.1 {
                    Target::Swarm(s) => show(&registry, &s, json_output)?,
                    Target::Endpoint { url, .. } => print_value(
                        json!({"kind":"endpoint","api_url":url}),
                        format!("SWARMY_API_URL {url} (not registered)"),
                        json_output,
                    ),
                }
            }
        }
        Command::Use { name } => {
            registry.set_current(&name)?;
            print_value(
                json!({"current":name}),
                format!("current swarm: {name}"),
                json_output,
            );
        }
        Command::Rm { name, force } => {
            let swarm = registry.read(&name)?;
            let hosts: usize = swarm
                .record
                .as_ref()
                .map_or(0, |r| r.hosts.values().map(Vec::len).sum());
            if !force
                && swarm
                    .record
                    .as_ref()
                    .is_some_and(|r| !r.hosts.is_empty() || r.tofu_state.is_some())
            {
                bail!(
                    "swarm \"{name}\" has machines recorded ({hosts} hosts); swarmy swarm rm only forgets registrations. Tear it down first, or pass --force to forget it and leave the machines running"
                );
            }
            swarmy_core::ignore_best_effort(
                Tunnels::ssh()
                    .disconnect(&registry.swarm_dir(&name)?.join("tunnel"))
                    .await,
                "close the swarm's SSH tunnel",
            );
            let was_current = registry.current()?.as_deref() == Some(&name);
            registry.remove(&name)?;
            print_value(
                json!({"removed":name,"was_current":was_current}),
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
        }
        Command::Attach {
            name,
            url,
            token_stdin: _,
        } => {
            let url = url::Url::parse(&url).context("--url must be an http or https URL")?;
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
            registry.attach(&name, url, &token)?;
            print_value(
                json!({"attached":name,"api_url":url}),
                format!(
                    "attached swarm {name} at {url}; make it current with swarmy swarm use NAME"
                ),
                json_output,
            );
        }
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
