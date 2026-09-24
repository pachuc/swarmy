//! In-place upgrades keep the remote state and all instance identities intact.
use std::time::{Duration, Instant};

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use swarmy_config::RemoteNode;

use super::ssh;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    All,
    ServicesOnly,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Human,
    Json,
}

pub struct Options {
    pub clean: bool,
    pub allow_dirty: bool,
    pub scope: Scope,
    pub drain_timeout: Duration,
    pub format: Format,
}

impl Options {
    pub fn new(allow_dirty: bool, services_only: bool, drain_timeout: u64, json: bool) -> Self {
        Self {
            clean: false,
            allow_dirty,
            scope: if services_only {
                Scope::ServicesOnly
            } else {
                Scope::All
            },
            drain_timeout: Duration::from_secs(drain_timeout),
            format: if json { Format::Json } else { Format::Human },
        }
    }
}

pub async fn command(state: &super::state::State, name: &str, mut options: Options) -> Result<()> {
    let _lock = state.lock()?;
    let node = state.require(name)?;
    let host = ssh::Ssh::discover()?;
    options.clean = host.is_clean()?;
    run(&host, &node, options).await.map(|_| ())
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Summary {
    pub node: String,
    pub changed: Vec<String>,
    pub restarted: Vec<String>,
    pub elapsed_seconds: f64,
}

pub trait UpgradeHost {
    async fn version(&self, node: &RemoteNode) -> Result<String>;
    async fn upgrade(
        &self,
        node: &RemoteNode,
        services_only: bool,
        drain_timeout: Duration,
        stack: bool,
    ) -> Result<Summary>;
}

impl UpgradeHost for ssh::Ssh {
    async fn version(&self, node: &RemoteNode) -> Result<String> {
        let address = ssh::reachable_address(node).await?;
        self.node_version(node, &address).await
    }

    async fn upgrade(
        &self,
        node: &RemoteNode,
        services_only: bool,
        drain_timeout: Duration,
        stack: bool,
    ) -> Result<Summary> {
        let address = ssh::reachable_address(node).await?;
        let started = Instant::now();
        let value = self
            .upgrade(node, &address, services_only, drain_timeout, stack)
            .await?;
        let mut summary: Summary = serde_json::from_value(value)?;
        summary.node.clone_from(&node.name);
        summary.elapsed_seconds = started.elapsed().as_secs_f64();
        Ok(summary)
    }
}

fn visit<'a>(node: &'a RemoteNode, nodes: &mut Vec<&'a RemoteNode>) {
    for child in &node.nodes {
        visit(child, nodes);
    }
    nodes.push(node);
}

fn print_version(message: &str, json: bool) {
    if json {
        eprintln!("{message}");
    } else {
        println!("{message}");
    }
}

pub async fn run(
    host: &impl UpgradeHost,
    primary: &RemoteNode,
    options: Options,
) -> Result<Vec<Summary>> {
    ensure!(
        options.clean || options.allow_dirty,
        "checkout has uncommitted changes; commit them or pass --allow-dirty"
    );
    let mut nodes = Vec::new();
    visit(primary, &mut nodes);
    print_version(
        &format!("Local CLI: swarmy {}", swarmy_version::IDENTITY),
        options.format == Format::Json,
    );
    for node in &nodes {
        print_version(
            &format!("{}: {}", node.name, host.version(node).await?),
            options.format == Format::Json,
        );
    }
    let mut summaries = Vec::new();
    for node in nodes {
        print_version(
            &format!("Upgrading {}", node.name),
            options.format == Format::Json,
        );
        let summary = host
            .upgrade(
                node,
                options.scope == Scope::ServicesOnly,
                options.drain_timeout,
                node.name == primary.name,
            )
            .await?;
        if options.format == Format::Json {
            println!("{}", serde_json::to_string(&summary)?);
        } else {
            println!(
                "{}: changed={} restarted={} elapsed={:.1}s",
                summary.node,
                summary.changed.join(","),
                summary.restarted.join(","),
                summary.elapsed_seconds
            );
        }
        summaries.push(summary);
    }
    Ok(summaries)
}
