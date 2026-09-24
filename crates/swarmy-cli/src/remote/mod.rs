mod add_node;
mod aws;
mod connect;
mod disconnect;
mod down;
mod logs;
mod services;
pub(crate) mod ssh;
mod state;
#[cfg(test)]
mod tests;
mod up;

use std::{path::PathBuf, time::Duration};

use anyhow::{Result, bail};
use swarmy_config::{RemoteNode, RemoteSettings, Settings};

use crate::remote_command::Command;
use state::State;

pub async fn run(command: Command, json: bool) -> Result<()> {
    // The base settings are enough here: provisioning does not use the selected tunnel profile.
    let loaded = Settings::load_base()?;
    let state_dir = PathBuf::from(&loaded.settings.state_dir);
    let state = State::open(&state_dir.join("remote"))?;
    match command {
        Command::Up {
            name,
            bucket,
            sandboxes,
            no_image,
            image_recipe,
            services,
            copy_credential,
        } => {
            let _lock = state.lock()?;
            swarmy_config::validate_remote_name(&name)?;
            let host = ssh::Ssh::discover()?;
            let recipe = if no_image {
                None
            } else {
                Some(host.image_recipe(&image_recipe)?)
            };
            let mut settings = loaded.settings;
            if let Some(services) = services {
                settings.remote.services = services;
            }
            if let Some(bucket) = bucket {
                settings.remote.bucket = Some(bucket);
            }
            let options = services::Options::new(&settings, copy_credential, recipe.as_deref())?;
            let cloud = aws::Aws::new(&settings.remote.region).await;
            tokio::select! {
                result = Box::pin(up::run(&cloud, &host, &state, &settings.remote, up::NewNode { name: &name, sandboxes: sandboxes.unwrap_or_else(swarmy_config::default_sandboxes) }, options, Duration::from_secs(5))) => result,
                result = tokio::signal::ctrl_c() => {
                    result?;
                    bail!("interrupted; run swarmy remote down {name} to clean up")
                }
            }
        }
        Command::AddNode {
            name,
            sandboxes,
            copy_credential,
        } => {
            let mut settings = loaded.settings;
            settings.remote.services = swarmy_config::RemoteServices::Node;
            let options = if copy_credential {
                Some(services::Options::new(&settings, true, None)?)
            } else {
                None
            };
            let _lock = state.lock()?;
            let node = state.require(&name)?;
            let host = ssh::Ssh::discover()?;
            let cloud = aws::Aws::new(&node.region).await;
            tokio::select! {
                result = Box::pin(add_node::run(&cloud, &host, &state, &name, sandboxes.unwrap_or_else(swarmy_config::default_sandboxes), Duration::from_secs(5), options.as_ref())) => result,
                result = tokio::signal::ctrl_c() => {
                    result?;
                    bail!("interrupted; run swarmy remote down {name} to clean up")
                }
            }
        }
        Command::Down { name } => {
            let _lock = state.lock()?;
            let Some(node) = state.read(&name)? else {
                println!("No remote node named {name}");
                return Ok(());
            };
            let cloud = aws::Aws::new(&node.region).await;
            down::run(&cloud, &state, &node, Duration::from_secs(5)).await
        }
        Command::Connect { name } => connect::run(&state_dir, &state, &name, json).await,
        Command::Disconnect { name } => disconnect::run(&state_dir, &state, &name).await,
        Command::Logs { name } => logs::run(&state, &name).await,
        Command::Status => unreachable!("status runs in swarmy-session"),
    }
}

#[derive(Clone, Debug)]
struct Launch {
    settings: RemoteSettings,
    image: String,
    name: String,
    key_name: String,
    profile: Option<String>,
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
    async fn prepare_bucket(&self, bucket: &str, region: &str, name: &str) -> Result<()>;
    async fn delete_profile(&self, name: &str) -> Result<()>;
    async fn stock_image(&self) -> Result<String>;
    async fn import_key(&self, name: &str, public_key: Vec<u8>, owner: &str) -> Result<()>;
    async fn launch(&self, request: &Launch) -> Result<String>;
    async fn instance(&self, id: &str) -> Result<Option<Instance>>;
    async fn find_launch(&self, token: &str) -> Result<Option<String>>;
    async fn terminate(&self, id: &str) -> Result<()>;
    async fn delete_key(&self, name: &str) -> Result<()>;
}

/// Key generation and provisioning over SSH, replaceable by a fake in tests.
trait Host {
    async fn services(
        &self,
        node: &RemoteNode,
        address: &str,
        options: &services::Options<'_>,
    ) -> Result<()>;
    async fn generate_key(&self, node: &RemoteNode) -> Result<Vec<u8>>;
    async fn build_image(
        &self,
        node: &RemoteNode,
        address: &str,
        recipe: &std::path::Path,
    ) -> Result<()>;
    async fn provision(&self, node: &RemoteNode, primary: Option<&RemoteNode>) -> Result<String>;
}

impl Host for ssh::Ssh {
    async fn services(
        &self,
        node: &RemoteNode,
        address: &str,
        options: &services::Options<'_>,
    ) -> Result<()> {
        services::install(node, address, options).await
    }

    async fn build_image(
        &self,
        node: &RemoteNode,
        address: &str,
        recipe: &std::path::Path,
    ) -> Result<()> {
        ssh::build_image(node, address, recipe).await
    }

    async fn generate_key(&self, node: &RemoteNode) -> Result<Vec<u8>> {
        ssh::generate_key(node).await
    }

    async fn provision(&self, node: &RemoteNode, primary: Option<&RemoteNode>) -> Result<String> {
        ssh::Ssh::provision(self, node, primary).await
    }
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
