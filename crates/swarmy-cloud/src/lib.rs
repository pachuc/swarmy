//! Cloud substrate for remote development stacks.
//!
//! Everything that knows about a cloud provider lives here behind one public
//! interface, so a second provider is an implementation, not a rewrite. The
//! [`Cloud`] trait speaks in provider-neutral types ([`MachineSpec`],
//! [`Machine`], [`ObjectBucket`]); [`Aws`] implements it with EC2, SSM, S3,
//! and IAM. [`Host`] covers the SSH half of provisioning and stays
//! provider-independent. See `docs/cloud-substrate.md` for the contract a
//! second provider must implement.
mod command;
pub mod ssh;
pub use command::{Command, select};
#[cfg(feature = "remote")]
mod add_node;
#[cfg(feature = "remote")]
mod aws;
#[cfg(feature = "remote")]
mod connect;
#[cfg(feature = "remote")]
mod disconnect;
#[cfg(feature = "remote")]
mod down;
#[cfg(feature = "remote")]
mod logs;
mod services;
#[cfg(feature = "remote")]
mod state;
#[cfg(feature = "remote")]
mod status;
#[cfg(all(test, feature = "remote"))]
mod tests;
#[cfg(feature = "remote")]
mod up;
#[cfg(feature = "remote")]
mod upgrade;

#[cfg(feature = "remote")]
pub use aws::Aws;
#[cfg(feature = "remote")]
pub use services::Options as ServiceOptions;

use std::future::Future;
#[cfg(feature = "remote")]
use std::time::Duration;

use anyhow::Result;
#[cfg(feature = "remote")]
use anyhow::bail;
#[cfg(feature = "remote")]
use std::path::PathBuf;
#[cfg(feature = "remote")]
use swarmy_config::Settings;
use swarmy_config::{RemoteNode, RemoteSettings};

#[cfg(feature = "remote")]
use state::State;

/// Provider-neutral description of one machine to create.
///
/// Generic shape (`cpus`, `memory_mib`, `disk_gb`) is authoritative for
/// providers that size machines directly. AWS sizes from `instance_type`
/// instead and ignores `cpus` and `memory_mib`; the lookup in
/// [`MachineSpec::from_settings`] fills them when the type is known and
/// leaves them zero otherwise.
#[derive(Clone, Debug)]
pub struct MachineSpec {
    /// Human name; AWS also uses it for the `Name` tag and the client token.
    pub name: String,
    /// Image reference; an AMI id on AWS.
    pub image: String,
    /// Key pair name; AWS imports it with [`Cloud::import_ssh_key`] before
    /// launch and passes the name to `RunInstances`.
    pub key_name: String,
    /// Raw public key bytes for providers that install keys inline at
    /// creation. AWS ignores this field because keys are imported separately.
    pub ssh_public_key: Vec<u8>,
    /// vCPUs for providers that size machines directly.
    pub cpus: u32,
    /// Memory in MiB for providers that size machines directly.
    pub memory_mib: u32,
    /// Root disk size in GiB.
    pub disk_gb: u32,
    /// Provider-specific shape; the EC2 instance type on AWS.
    pub instance_type: String,
    /// Placement or network selection; the subnet id on AWS.
    pub subnet: Option<String>,
    /// Network membership; the security group id on AWS.
    pub security_group: Option<String>,
    /// Value of the `managed-by` tag.
    pub managed_by: String,
    /// Node credentials to attach; the IAM instance profile on AWS.
    pub profile: Option<String>,
    /// First-boot script. AWS leaves this unset because provisioning runs
    /// over SSH after launch instead.
    pub bootstrap: Option<Vec<u8>>,
}

impl MachineSpec {
    /// Build a launch request from saved remote settings.
    ///
    /// `profile` carries the node credentials (the `swarmy-{remote}` instance
    /// profile when the remote uses an object bucket). `ssh_public_key` is
    /// the key about to be imported with [`Cloud::import_ssh_key`].
    #[must_use]
    pub fn from_settings(
        name: &str,
        image: &str,
        key_name: &str,
        ssh_public_key: Vec<u8>,
        settings: &RemoteSettings,
        profile: Option<String>,
    ) -> Self {
        let (cpus, memory_mib) = instance_shape(&settings.aws.instance_type);
        Self {
            name: name.into(),
            image: image.into(),
            key_name: key_name.into(),
            ssh_public_key,
            cpus,
            memory_mib,
            disk_gb: settings.disk_gb,
            instance_type: settings.aws.instance_type.clone(),
            subnet: settings.aws.subnet.clone(),
            security_group: settings.aws.security_group.clone(),
            managed_by: settings.managed_by_tag.clone(),
            profile,
            bootstrap: None,
        }
    }
}

/// Provider-neutral view of one machine. Missing machines are `None`.
#[derive(Clone, Debug)]
pub struct Machine {
    /// Provider's machine id (the EC2 instance id on AWS).
    pub id: String,
    pub public_ip: String,
    pub private_ip: String,
    /// Lifecycle state as reported by the provider (`pending`, `running`,
    /// `terminated`, and similar).
    pub state: String,
}

/// Provider-neutral description of the object bucket backing a remote.
///
/// The node credentials guard this bucket until teardown.
#[derive(Clone, Debug)]
pub struct ObjectBucket {
    /// Bucket name.
    pub name: String,
    /// Bucket region.
    pub region: String,
    /// Owning remote; AWS derives the default IAM role from this.
    pub owner: String,
    /// Storage endpoint override for S3-compatible providers. AWS leaves
    /// this unset and uses its regional endpoints.
    pub endpoint: Option<String>,
    /// Credentials attached to nodes; the IAM instance profile on AWS.
    pub node_credentials: Option<String>,
}

/// The cloud boundary. Only the provider implementation knows about provider
/// APIs; callers use machines, keys, images, and buckets. Missing resources
/// are represented by `None`.
pub trait Cloud {
    /// Create the bucket and the node credentials guarding it, idempotently.
    fn ensure_bucket(&self, bucket: &ObjectBucket) -> impl Future<Output = Result<()>>;
    /// Resolve the stock machine image for the configured region.
    fn base_image(&self) -> impl Future<Output = Result<String>>;
    /// Import an SSH public key under `name` and tag it with `owner`.
    fn import_ssh_key(
        &self,
        name: &str,
        public_key: Vec<u8>,
        owner: &str,
    ) -> impl Future<Output = Result<()>>;
    /// Create a machine and return its id. The launch token is the key name,
    /// so a lost response is recoverable with [`Cloud::find_by_tag`].
    fn create(&self, spec: &MachineSpec) -> impl Future<Output = Result<String>>;
    /// Describe a machine by id, or `None` when it does not exist.
    fn get(&self, id: &str) -> impl Future<Output = Result<Option<Machine>>>;
    /// Find a machine created under a launch token (the key name).
    fn find_by_tag(&self, token: &str) -> impl Future<Output = Result<Option<String>>>;
    /// Terminate a machine; already-terminated or missing machines are
    /// success.
    fn destroy(&self, id: &str) -> impl Future<Output = Result<()>>;
    /// Empty and delete a bucket; return false if it was already absent.
    fn delete_bucket(&self, name: &str) -> impl Future<Output = Result<bool>>;
    /// Delete the instance profile and its role; return false if both were absent.
    fn delete_node_role(&self, name: &str) -> impl Future<Output = Result<bool>>;
    /// Delete an SSH key; missing keys are success.
    fn delete_ssh_key(&self, name: &str) -> impl Future<Output = Result<()>>;
}

/// Key generation and provisioning over SSH, replaceable by a fake in tests.
pub trait Host {
    fn services(
        &self,
        node: &RemoteNode,
        address: &str,
        options: &services::Options<'_>,
    ) -> impl Future<Output = Result<()>>;
    fn generate_key(&self, node: &RemoteNode) -> impl Future<Output = Result<Vec<u8>>>;
    fn build_image(
        &self,
        node: &RemoteNode,
        address: &str,
        recipe: &std::path::Path,
    ) -> impl Future<Output = Result<()>>;
    fn provision(
        &self,
        node: &RemoteNode,
        primary: Option<&RemoteNode>,
    ) -> impl Future<Output = Result<String>>;
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

/// Build the provider selected by the remote settings.
///
/// # Errors
///
/// Rejects unknown providers. Only `aws` exists today.
#[cfg(feature = "remote")]
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
#[cfg(feature = "remote")]
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
            down::confirm(keep_bucket, yes, json)?;
            let _lock = state.lock()?;
            let Some(node) = state.read(&name)? else {
                println!("No remote node named {name}");
                return Ok(());
            };
            let mut cloud_settings = node.cloud_settings();
            cloud_settings.region.clone_from(&node.region);
            let cloud = for_settings(&cloud_settings).await?;
            down::run(&cloud, &state, &node, Duration::from_secs(5), keep_bucket).await
        }
        Command::Connect { name } => connect::run(&state_dir, &state, &name, json).await,
        Command::Disconnect { name } => disconnect::run(&state_dir, &state, &name).await,
        Command::Logs { name } => logs::run(&state, &name).await,
        Command::Status => status::run(json).await,
    }
}

/// Launch and provision the first node of a remote.
#[cfg(feature = "remote")]
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
#[cfg(feature = "remote")]
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
#[cfg(feature = "remote")]
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
#[cfg(feature = "remote")]
pub(crate) struct NodeShape {
    instance_type: Option<String>,
    disk_gb: Option<u32>,
}

#[cfg(feature = "remote")]
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

/// Generic shape for known EC2 instance types. Unknown types leave the
/// generic shape unset; the provider-specific `instance_type` stays
/// authoritative on AWS.
fn instance_shape(instance_type: &str) -> (u32, u32) {
    match instance_type {
        "m6i.large" => (2, 8 * 1024),
        "m6id.xlarge" => (4, 16 * 1024),
        "m6id.4xlarge" => (16, 64 * 1024),
        _ => (0, 0),
    }
}

/// Retry an attempt while it fails with a not-yet-propagated identity error.
///
/// A role or instance profile is visible to the compute API only after the
/// identity system has propagated it. Launching sooner can bind the machine
/// to stale identity data whose credentials are then rejected.
#[cfg(feature = "remote")]
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

#[cfg(feature = "remote")]
fn profile_not_propagated(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}");
    message.contains("InvalidParameterValue") && message.contains("Invalid IAM Instance Profile")
}

#[cfg(feature = "remote")]
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

#[cfg(feature = "remote")]
pub(crate) fn key_name(node: &RemoteNode) -> Result<&str> {
    node.key_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow::anyhow!("invalid key path in remote state"))
}
