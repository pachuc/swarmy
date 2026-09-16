mod aws;
mod down;
mod ssh;
mod state;
#[cfg(test)]
mod tests;
mod up;

use std::time::Duration;

use anyhow::{Result, bail};
use clap::Subcommand;
use swarmy_config::{RemoteNode, RemoteSettings, Settings};

#[derive(Subcommand)]
pub enum Command {
    /// Launch, copy this checkout, and provision a remote node
    Up { name: String },
    /// Terminate a node and remove its key pair and local state
    Down { name: String },
}

pub async fn run(command: Command) -> Result<()> {
    let loaded = Settings::load()?;
    let directory = loaded
        .path
        .as_ref()
        .and_then(|p| p.parent())
        .map_or_else(|| loaded.root.join(".swarmy"), std::path::Path::to_owned)
        .join("remote");
    let state = state::State::open(&directory)?;
    let _lock = state.lock()?;
    match command {
        Command::Up { name } => {
            state::validate_name(&name)?;
            let host = ssh::Ssh::discover()?;
            let cloud = aws::Aws::new(&loaded.settings.remote.region).await;
            tokio::select! {
                result = up::run(&cloud, &host, &state, &loaded.settings.remote, &name, Duration::from_secs(5)) => result,
                result = tokio::signal::ctrl_c() => {
                    result?;
                    bail!("interrupted; run swarmy remote down {name} to clean up")
                }
            }
        }
        Command::Down { name } => {
            let Some(node) = state.read(&name)? else {
                println!("No remote node named {name}");
                return Ok(());
            };
            let cloud = aws::Aws::new(&node.region).await;
            down::run(&cloud, &state, &node, Duration::from_secs(5)).await
        }
    }
}

#[derive(Clone, Debug)]
struct Launch {
    settings: RemoteSettings,
    image: String,
    name: String,
    key_name: String,
}

#[derive(Clone, Debug)]
struct Instance {
    id: String,
    status: String,
    public_ip: String,
    private_ip: String,
}

/// Only this boundary knows about AWS. Missing resources are represented by None.
trait Cloud {
    async fn stock_image(&self) -> Result<String>;
    async fn import_key(&self, name: &str, public_key: Vec<u8>, owner: &str) -> Result<()>;
    async fn launch(&self, request: &Launch) -> Result<String>;
    async fn instance(&self, id: &str) -> Result<Option<Instance>>;
    async fn find_launch(&self, token: &str) -> Result<Option<String>>;
    async fn terminate(&self, id: &str) -> Result<()>;
    async fn delete_key(&self, name: &str) -> Result<()>;
}

trait Host {
    async fn generate_key(&self, node: &RemoteNode) -> Result<Vec<u8>>;
    async fn provision(&self, node: &RemoteNode) -> Result<String>;
}

async fn wait_running(cloud: &impl Cloud, id: &str, delay: Duration) -> Result<Instance> {
    for _ in 0..120 {
        if let Some(instance) = cloud.instance(id).await? {
            anyhow::ensure!(instance.id == id, "EC2 returned a different instance");
            match instance.status.as_str() {
                "running" if !instance.public_ip.is_empty() && !instance.private_ip.is_empty() => {
                    return Ok(instance);
                }
                "pending" | "running" => {}
                status => bail!("instance {id} entered {status} while waiting for running"),
            }
        }
        tokio::time::sleep(delay).await;
    }
    bail!("timed out waiting for instance {id} to run with an IP address")
}

fn key_name(node: &RemoteNode) -> Result<&str> {
    node.key_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow::anyhow!("invalid key path in remote state"))
}
