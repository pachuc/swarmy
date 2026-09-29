//! Remote provisioning, compiled only with the remote feature.
use super::{Cloud, Command, Host, Machine, RemoteNode, RemoteSettings, Result, services, ssh};
use anyhow::bail;
use std::{future::Future, path::PathBuf, time::Duration};
use swarmy_config::Settings;

#[path = "add_node.rs"]
mod add_node;
#[path = "aws.rs"]
mod aws;
#[path = "connect.rs"]
mod connect;
#[path = "disconnect.rs"]
mod disconnect;
#[path = "down.rs"]
mod down;
#[path = "logs.rs"]
mod logs;
#[path = "state.rs"]
mod state;
#[path = "status.rs"]
mod status;
#[cfg(test)]
#[path = "tests.rs"]
mod tests;
#[path = "up.rs"]
mod up;
#[path = "upgrade.rs"]
mod upgrade;

pub use super::services::Options as ServiceOptions;
pub use aws::Aws;

/// Build the provider selected by the remote settings.
///
/// # Errors
///
/// Rejects unknown providers. Only `aws` exists today.
pub async fn for_settings(settings: &RemoteSettings) -> Result<Aws> {
    anyhow::ensure!(
        settings.provider == "aws",
        "unknown cloud provider '{}': only 'aws' is supported",
        settings.provider
    );
    Ok(Aws::new(&settings.region).await)
}

/// Run the `swarmy remote` subcommand.
///
/// # Errors
///
/// Returns errors for invalid configuration, state, provisioning, and tunnel
/// failures.
pub async fn run(command: Command, json: bool) -> Result<()> {
    // The base settings are enough here: provisioning does not use the selected tunnel profile.
    let loaded = Settings::load_base()?;
    let state_dir = PathBuf::from(&loaded.settings.state_dir);
    let state = State::open(&state_dir.join("remote"))?;
    match command {
        Command::Up { .. } => Box::pin(run_up(&state, loaded.settings, command)).await,
        Command::AddNode { .. } => Box::pin(run_add_node(&state, loaded.settings, command)).await,
        Command::Upgrade {
            name,
            services_only,
            allow_dirty,
            drain_timeout,
        } => {
            upgrade::command(
                &state,
                &name,
                upgrade::Options::new(allow_dirty, services_only, drain_timeout, json),
            )
            .await
        }
        Command::Down {
            name,
            keep_bucket,
            yes,
        } => {
            let _lock = state.lock()?;
            let Some(node) = state.read(&name)? else {
                println!("No remote node named {name}");
                return Ok(());
            };
            let mut cloud_settings = node.cloud_settings();
            cloud_settings.region.clone_from(&node.region);
            let cloud = for_settings(&cloud_settings).await?;
            down::confirm(&cloud, &state, &node, keep_bucket, yes, json)
                .await
                .map_err(down::actionable_error)?;
            down::run(&cloud, &state, &node, Duration::from_secs(5), keep_bucket)
                .await
                .map_err(down::actionable_error)
        }
        Command::Tag { name } => {
            let _lock = state.lock()?;
            let node = state.require(&name)?;
            let mut settings = node.cloud_settings();
            settings.region.clone_from(&node.region);
            down::tag(&for_settings(&settings).await?, &state, &node)
                .await
                .map_err(down::actionable_error)
        }
        Command::Connect { name } => connect::run(&state_dir, &state, &name, json).await,
        Command::Disconnect { name } => disconnect::run(&state_dir, &state, &name).await,
        Command::Logs { name } => logs::run(&state, &name).await,
        Command::Status => status::run(json).await,
    }
}

/// Launch and provision the first node of a remote.
async fn run_up(state: &State, mut settings: Settings, command: Command) -> Result<()> {
    let Command::Up {
        name,
        bucket,
        sandboxes,
        instance_type,
        disk_gb,
        no_image,
        image_recipe,
        services,
        copy_credential,
    } = command
    else {
        unreachable!("run_up handles remote up");
    };
    let _lock = state.lock()?;
    swarmy_config::validate_remote_name(&name)?;
    let host = ssh::Ssh::discover()?;
    let recipe = if no_image {
        None
    } else {
        Some(host.image_recipe(&image_recipe)?)
    };
    if let Some(services) = services {
        settings.remote.services = services;
    }
    if let Some(bucket) = bucket {
        settings.remote.bucket = Some(bucket);
    }
    NodeShape {
        instance_type,
        disk_gb,
    }
    .apply(&mut settings.remote)?;
    let options = services::Options::new(&settings, copy_credential, recipe.as_deref())?;
    let cloud = for_settings(&settings.remote).await?;
    guard(
        &name,
        up::run(
            &cloud,
            &host,
            state,
            &settings.remote,
            up::NewNode {
                name: &name,
                sandboxes: sandboxes.unwrap_or_else(swarmy_config::default_sandboxes),
            },
            options,
            Duration::from_secs(5),
        ),
    )
    .await
}

/// Join another node to an existing remote over its private network.
async fn run_add_node(state: &State, mut settings: Settings, command: Command) -> Result<()> {
    let Command::AddNode {
        name,
        sandboxes,
        instance_type,
        disk_gb,
        copy_credential,
    } = command
    else {
        unreachable!("run_add_node handles remote add-node");
    };
    settings.remote.services = swarmy_config::RemoteServices::Node;
    let options = if copy_credential {
        Some(services::Options::new(&settings, true, None)?)
    } else {
        None
    };
    let _lock = state.lock()?;
    let node = state.require(&name)?;
    let host = ssh::Ssh::discover()?;
    let launch = node.launch_settings.clone().ok_or_else(|| {
        anyhow::anyhow!(
            "remote has no saved launch configuration; recreate it with remote up before adding nodes"
        )
    })?;
    let cloud = for_settings(&launch).await?;
    guard(
        &name,
        add_node::run(
            &cloud,
            &host,
            state,
            add_node::NewNode {
                name: &name,
                sandboxes: sandboxes.unwrap_or_else(swarmy_config::default_sandboxes),
                shape: NodeShape {
                    instance_type,
                    disk_gb,
                },
            },
            Duration::from_secs(5),
            options.as_ref(),
        ),
    )
    .await
}

/// Run provisioning to completion unless interrupted, keeping state for `down`.
async fn guard(name: &str, task: impl Future<Output = Result<()>>) -> Result<()> {
    tokio::select! {
        result = Box::pin(task) => result,
        result = tokio::signal::ctrl_c() => {
            result?;
            bail!("interrupted; run swarmy remote down {name} to clean up")
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct NodeShape {
    instance_type: Option<String>,
    disk_gb: Option<u32>,
}

impl NodeShape {
    pub(crate) fn apply(self, settings: &mut RemoteSettings) -> Result<()> {
        if let Some(instance_type) = self.instance_type {
            settings.aws.instance_type = instance_type;
        }
        if let Some(disk_gb) = self.disk_gb {
            settings.disk_gb = disk_gb;
        }
        anyhow::ensure!(
            !settings.aws.instance_type.is_empty(),
            "instance type must not be empty"
        );
        anyhow::ensure!(settings.disk_gb > 0, "root disk size must be positive");
        Ok(())
    }
}

/// Retry an attempt while it fails with a not-yet-propagated identity error.
///
/// A role or instance profile is visible to the compute API only after the
/// identity system has propagated it. Launching sooner can bind the machine
/// to stale identity data whose credentials are then rejected.
pub(crate) async fn retry_profile_propagation<T, F, Fut>(
    mut attempt: F,
    profile: bool,
    pause: Duration,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        match attempt().await {
            Ok(value) => return Ok(value),
            Err(error)
                if profile
                    && profile_not_propagated(&error)
                    && tokio::time::Instant::now() < deadline =>
            {
                tracing::info!("waiting for IAM instance profile to propagate to EC2");
                tokio::time::sleep(
                    pause.min(deadline.saturating_duration_since(tokio::time::Instant::now())),
                )
                .await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn profile_not_propagated(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}");
    message.contains("InvalidParameterValue") && message.contains("Invalid IAM Instance Profile")
}

pub(crate) async fn wait_running(cloud: &impl Cloud, id: &str, delay: Duration) -> Result<Machine> {
    for _ in 0..120 {
        if let Some(machine) = cloud.get(id).await? {
            anyhow::ensure!(machine.id == id, "provider returned a different machine");
            match machine.state.as_str() {
                "running" if !machine.public_ip.is_empty() && !machine.private_ip.is_empty() => {
                    return Ok(machine);
                }
                "pending" | "running" => {}
                state => bail!("machine {id} entered {state} while waiting for running"),
            }
        }
        tokio::time::sleep(delay).await;
    }
    bail!("timed out waiting for machine {id} to run with an IP address")
}

pub(crate) fn key_name(node: &RemoteNode) -> Result<&str> {
    node.key_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow::anyhow!("invalid key path in remote state"))
}
