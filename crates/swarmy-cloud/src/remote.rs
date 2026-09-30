//! Remote provisioning, compiled only with the remote feature.
use super::{
    Cloud, Command, Host, Machine, MachineSpec, ObjectBucket, Ownership, RemoteNode,
    RemoteSettings, Result, services, ssh,
};

use state::State;
use std::{
    future::Future,
    path::{Path, PathBuf},
    time::Duration,
};
use swarmy_config::Settings;

use crate::BucketRemoval;

mod add_node;
mod adopt;
mod aws;
mod bucket;
mod buckets;
mod connect;
mod disconnect;
mod down;
mod logs;
mod state;
mod status;
#[cfg(test)]
mod tests;
mod up;
mod upgrade;

pub use super::services::Options as ServiceOptions;
pub use aws::Aws;
pub use down::DeletionPlan;

/// The cloud substrate for a remote. Bucket lifecycle always goes through
/// object-storage APIs (the same calls `remote up` uses, including
/// S3-compatible endpoints with static keys). Machine operations delegate
/// to AWS, or fail for existing-host remotes, which have no cloud machines:
/// the provisioning paths never call them, so a failure is a programming
/// mistake. One `Cloud` implementation; the provider enum is read where
/// behavior must differ: [`for_settings`] records it in the substrate
/// (whose `machines` gate refuses machine calls for existing hosts), `run`
/// picks host decommissioning over instance termination, and
/// `add_node::resolve_existing` validates join flags per provider.
/// Nothing else branches on it.
struct Substrate {
    aws: Aws,
    provider: swarmy_config::Provider,
}

fn no_machines() -> crate::Error {
    crate::Error::other(
        "existing-host remotes have no cloud machines; decommission the server itself",
    )
}

impl Substrate {
    /// The AWS implementation for machine operations, or an error for
    /// existing-host remotes, which have no cloud machines. The one place
    /// machine calls are gated; bucket calls delegate unconditionally.
    fn machines(&self) -> Result<&Aws> {
        if self.provider == swarmy_config::Provider::Existing {
            Err(no_machines())
        } else {
            Ok(&self.aws)
        }
    }
}

impl Cloud for Substrate {
    async fn ensure_bucket(&self, bucket: &ObjectBucket) -> Result<()> {
        self.aws.ensure_bucket(bucket).await
    }
    async fn verify_bucket_access(&self, bucket: &ObjectBucket) -> Result<()> {
        self.aws.verify_bucket_access(bucket).await
    }
    async fn base_image(&self) -> Result<String> {
        self.machines()?.base_image().await
    }
    async fn import_ssh_key(&self, name: &str, public_key: Vec<u8>, owner: &str) -> Result<()> {
        self.machines()?
            .import_ssh_key(name, public_key, owner)
            .await
    }
    async fn create(&self, spec: &MachineSpec) -> Result<String> {
        self.machines()?.create(spec).await
    }
    async fn get(&self, id: &str) -> Result<Option<Machine>> {
        self.machines()?.get(id).await
    }
    async fn find_by_tag(&self, token: &str) -> Result<Option<String>> {
        self.machines()?.find_by_tag(token).await
    }
    async fn destroy(&self, id: &str) -> Result<()> {
        self.machines()?.destroy(id).await
    }
    async fn bucket_ownership(&self, bucket: &ObjectBucket) -> Result<Ownership> {
        self.aws.bucket_ownership(bucket).await
    }
    async fn role_ownership(&self, name: &str, owner: &str) -> Result<(Ownership, Ownership)> {
        self.aws.role_ownership(name, owner).await
    }
    async fn tag_bucket(&self, bucket: &ObjectBucket) -> Result<()> {
        self.aws.tag_bucket(bucket).await
    }
    async fn tag_node_role(&self, name: &str, owner: &str) -> Result<()> {
        self.aws.tag_node_role(name, owner).await
    }
    async fn delete_bucket(&self, bucket: &ObjectBucket) -> Result<BucketRemoval> {
        self.aws.delete_bucket(bucket).await
    }
    async fn delete_node_role(&self, name: &str, owner: &str) -> Result<(bool, bool)> {
        self.aws.delete_node_role(name, owner).await
    }
    async fn delete_ssh_key(&self, name: &str) -> Result<()> {
        self.machines()?.delete_ssh_key(name).await
    }
}

/// Build the substrate for the remote settings: AWS machines, or the
/// existing-host substrate that only manages buckets. The concrete type
/// stays private; callers use the [`Cloud`] interface.
pub async fn for_settings(settings: &RemoteSettings) -> impl Cloud {
    Substrate {
        aws: Aws::new(&settings.region).await,
        provider: settings.provider,
    }
}

/// Require a configured region before any state or host changes. Shared by
/// `up` and `adopt` so the message never drifts.
pub(super) fn validate_region(settings: &RemoteSettings, command: &str) -> Result<()> {
    crate::Error::ensure(
        !settings.region.is_empty(),
        format!("configure remote.region in config.toml before running swarmy remote {command}"),
    )
}

/// Parse an IPv4 address for provisioning, with the flag in the message.
/// The provisioning script only accepts IPv4, so every operator-given
/// address is rejected up front instead of failing after state exists.
pub(super) fn parse_ipv4(field: &str, value: &str) -> Result<std::net::Ipv4Addr> {
    value
        .parse()
        .map_err(|source| crate::Error::context(source, format!("{field} must be an IPv4 address")))
}

/// Check the bucket binding before any state or host changes. Shared by
/// `up` and `adopt`: the IAM role name bounds the remote name and the
/// bucket name must be valid.
pub(super) fn validate_bucket_binding(settings: &RemoteSettings, name: &str) -> Result<()> {
    if let Some(spec) = &settings.bucket {
        crate::Error::ensure(
            name.len() <= 57,
            "bucket-backed remote name must be at most 57 characters to fit the IAM role name",
        )?;
        spec.validate_name()?;
    }
    Ok(())
}

/// Require explicit local storage for sandbox nodes. Shared by `adopt` and
/// existing-host joins: the machine already exists, so there is no
/// instance-store lookup to fall back on.
pub(super) fn validate_sandbox_storage(
    sandboxes: u32,
    local_storage: &str,
    command: &str,
) -> Result<()> {
    crate::Error::ensure(
        sandboxes == 0 || !local_storage.is_empty(),
        format!(
            "sandbox nodes need local storage: pass --local-storage with a block device or dir:/path to {command}"
        ),
    )
}

/// Set the bucket up through object-storage APIs. Shared by `up` (create a
/// machine, then provision it) and `adopt` (provision an existing machine)
/// so the bucket options never drift apart.
pub(super) async fn ensure_remote_bucket(
    cloud: &impl Cloud,
    settings: &RemoteSettings,
    name: &str,
) -> Result<()> {
    if let Some(spec) = &settings.bucket {
        cloud
            .ensure_bucket(&ObjectBucket::from_spec(
                name,
                spec,
                &settings.region,
                settings.instance_profile(name),
            ))
            .await?;
    }
    Ok(())
}

/// Instance type only exists for AWS machines; existing hosts report none
/// so status never prints a cloud shape that was never selected. One
/// helper so only this module branches on the provider for presentation.
pub(crate) fn display_instance_type(node: &RemoteNode) -> Option<String> {
    let settings = node.launch_settings.as_ref()?;
    (settings.provider == swarmy_config::Provider::Aws).then(|| settings.aws.instance_type.clone())
}

/// What a `swarmy remote` invocation did. Confirmation variants carry what
/// the CLI must prompt for; the rerun passes the original command with
/// `confirmed: bool` instead of rebuilding state from a saved command.
pub enum RunOutcome {
    Completed,
    NeedsConfirmation {
        plan: DeletionPlan,
    },
    NeedsTagConfirmation {
        node: String,
        targets: Vec<(String, String)>,
    },
}

/// Run the `swarmy remote` subcommand.
///
/// # Errors
///
/// Returns errors for invalid configuration, state, provisioning, and tunnel
/// failures.
pub async fn run(command: Command, json: bool, confirmed: bool) -> Result<RunOutcome> {
    // The base settings are enough here: provisioning does not use the selected tunnel profile.
    let loaded = Settings::load_base()?;
    let state_dir = PathBuf::from(&loaded.settings.state_dir);
    let state = State::open(&state_dir.join("remote"))?;
    match command {
        Command::Up { .. } => {
            Box::pin(run_up(&state, loaded.settings, command)).await?;
            Ok(RunOutcome::Completed)
        }
        Command::Adopt { .. } => {
            Box::pin(run_adopt(&state, loaded.settings, command)).await?;
            Ok(RunOutcome::Completed)
        }
        Command::AddNode { .. } => {
            Box::pin(run_add_node(&state, loaded.settings, command)).await?;
            Ok(RunOutcome::Completed)
        }
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
            .await?;
            Ok(RunOutcome::Completed)
        }
        Command::Down {
            name,
            keep_bucket,
            yes,
        } => {
            let _lock = state.lock()?;
            let Some(node) = state.read(&name)? else {
                cloud_out!("No remote node named {name}");
                return Ok(RunOutcome::Completed);
            };
            let mut cloud_settings = node.cloud_settings();
            cloud_settings.region.clone_from(&node.region);
            let cloud = for_settings(&cloud_settings).await;
            if !keep_bucket
                && !yes
                && !confirmed
                && let Some(plan) = down::plan(&cloud, &state, &node).await?
            {
                return Ok(RunOutcome::NeedsConfirmation { plan });
            }
            // The one provider branch for teardown: existing-host remotes
            // need SSH for host decommissioning, while AWS teardown needs no
            // checkout, so discovery stays lazy. Cloud calls never branch:
            // the substrate refuses machine operations for existing hosts.
            if node.cloud_settings().provider == swarmy_config::Provider::Existing {
                let host = ssh::Ssh::discover()?;
                down::run_existing(&cloud, &host, &state, &node, keep_bucket).await?;
            } else {
                down::run(&cloud, &state, &node, Duration::from_secs(5), keep_bucket).await?;
            }
            Ok(RunOutcome::Completed)
        }
        Command::Tag { name } => {
            let _lock = state.lock()?;
            let node = state.require(&name)?;
            let targets = down::adoption_targets(&state, &node)?;
            if !confirmed {
                return Ok(RunOutcome::NeedsTagConfirmation {
                    node: name,
                    targets,
                });
            }
            let mut settings = node.cloud_settings();
            settings.region.clone_from(&node.region);
            down::apply_tag(&for_settings(&settings).await, &node).await?;
            Ok(RunOutcome::Completed)
        }
        Command::Connect { name } => {
            connect::run(&state_dir, &state, &name, json).await?;
            Ok(RunOutcome::Completed)
        }
        Command::Disconnect { name } => {
            disconnect::run(&state_dir, &state, &name).await?;
            Ok(RunOutcome::Completed)
        }
        Command::Logs { name } => {
            logs::run(&state, &name).await?;
            Ok(RunOutcome::Completed)
        }
        Command::Ls => {
            status::run(json).await?;
            Ok(RunOutcome::Completed)
        }
    }
}

/// Resolve the remote's bucket description from flags, environment, and
/// config. Shared by `up` (create a machine, then provision it) and `adopt`
/// (provision an existing machine) so the bucket options never drift apart.
fn resolve_bucket(settings: &mut RemoteSettings, args: &crate::BucketArgs) -> Result<()> {
    let stdin_secret = args
        .s3_secret_stdin
        .then(bucket::read_secret_stdin)
        .transpose()?;
    let resolved = bucket::resolve(
        settings.bucket.clone(),
        &bucket::BucketOptions {
            bucket: args.bucket.clone(),
            endpoint: args.s3_endpoint.clone(),
            region: args.s3_region.clone(),
            prefix: args.s3_prefix.clone(),
            access_key: args.s3_access_key.clone(),
            secret_file: args.s3_secret_file.clone(),
            secret_stdin: args.s3_secret_stdin,
            stdin_secret,
            env_access_key: std::env::var("AWS_ACCESS_KEY_ID").ok(),
            env_secret_key: std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
        },
    )?;
    settings.bucket = resolved;
    if let Some(spec) = settings.bucket.as_mut() {
        let region = settings.region.clone();
        spec.resolve_region(&region);
    }
    Ok(())
}

/// Resolve the image recipe unless `--no-image` skips the registered build.
/// Shared by `up` and `adopt`: a missing recipe fails before any host changes.
fn resolve_recipe(host: &ssh::Ssh, no_image: bool, image_recipe: &Path) -> Result<Option<PathBuf>> {
    if no_image {
        Ok(None)
    } else {
        Ok(Some(host.image_recipe(image_recipe)?))
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
    let recipe = resolve_recipe(&host, no_image, &image_recipe)?;
    if let Some(services) = services {
        settings.remote.services = services;
    }
    resolve_bucket(&mut settings.remote, &bucket)?;
    NodeShape {
        instance_type,
        disk_gb,
    }
    .apply(&mut settings.remote)?;
    let options = services::Options::new(&settings, copy_credential, recipe.as_deref())?;
    let cloud = for_settings(&settings.remote).await;
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

/// Provision an existing SSH-reachable machine as the first node of a
/// remote. The machine is recorded with the existing-host provider, then the
/// shared provisioning `up` runs after its machine is ready (services,
/// image build) runs against its address. No machine is created and no
/// cloud machine call happens.
async fn run_adopt(state: &State, mut settings: Settings, command: Command) -> Result<()> {
    let Command::Adopt {
        name,
        host: address,
        ssh_user,
        ssh_key,
        service_user,
        local_storage,
        bucket,
        sandboxes,
        no_image,
        image_recipe,
        services,
        copy_credential,
    } = command
    else {
        unreachable!("run_adopt handles remote adopt");
    };
    let _lock = state.lock()?;
    swarmy_config::validate_remote_name(&name)?;
    let host = ssh::Ssh::discover()?;
    let recipe = resolve_recipe(&host, no_image, &image_recipe)?;
    if let Some(services) = services {
        settings.remote.services = services;
    }
    if let Some(service_user) = service_user {
        settings.remote.service_user = service_user;
    }
    if let Some(local_storage) = local_storage {
        settings.remote.local_storage = local_storage;
    }
    // The adopted machine is operator-owned whatever the configured default
    // substrate is; record it as an existing host throughout.
    settings.remote.provider = swarmy_config::Provider::Existing;
    resolve_bucket(&mut settings.remote, &bucket)?;
    let options = services::Options::new(&settings, copy_credential, recipe.as_deref())?;
    let cloud = for_settings(&settings.remote).await;
    guard(
        &name,
        adopt::run(
            &cloud,
            &host,
            state,
            &settings.remote,
            adopt::AdoptNode {
                name: &name,
                host: &address,
                ssh_user: &ssh_user,
                ssh_key: &ssh_key,
                sandboxes: sandboxes.unwrap_or_else(swarmy_config::default_sandboxes),
            },
            options,
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
        host: join_host,
        ssh_user,
        ssh_key,
        local_storage,
        primary_address,
        copy_credential,
    } = command
    else {
        unreachable!("run_add_node handles remote add-node");
    };
    settings.remote.services = swarmy_config::RemoteServices::Node;
    if let Some(local_storage) = local_storage {
        settings.remote.local_storage = local_storage;
    }
    let options = if copy_credential {
        Some(services::Options::new(&settings, true, None)?)
    } else {
        None
    };
    let _lock = state.lock()?;
    let node = state.require(&name)?;
    let host = ssh::Ssh::discover()?;
    let launch = node.launch_settings.clone().ok_or_else(|| {
        crate::Error::other(
            "remote has no saved launch configuration; recreate it with remote up before adding nodes",
        )
    })?;
    let cloud = for_settings(&launch).await;
    // The provider dispatch for joins lives in the add-node module, so this
    // call site never branches on the provider itself.
    let existing = add_node::resolve_existing(
        &name,
        launch.provider,
        join_host.as_deref(),
        ssh_user.as_deref(),
        ssh_key.as_deref(),
        primary_address.as_deref(),
        instance_type.as_deref(),
        disk_gb,
    )?;
    // Box the join future: it holds saved launch state across awaits and
    // would otherwise exceed the large-future budget.
    guard(
        &name,
        // Pin the join so service-user paths and disk state do not inflate
        // the guarded future past the pedantic size limit.
        Box::pin(add_node::run(
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
                // The saved primary settings may carry its resolved device;
                // the join starts from the explicit configuration instead.
                local_storage: settings.remote.local_storage.clone(),
                existing,
            },
            Duration::from_secs(5),
            options.as_ref(),
        )),
    )
    .await
}

/// Run provisioning to completion unless interrupted, keeping state for `down`.
async fn guard(name: &str, task: impl Future<Output = Result<()>>) -> Result<()> {
    tokio::select! {
        result = Box::pin(task) => result,
        result = tokio::signal::ctrl_c() => {
            result?;
            Err(crate::Error::other(format!(
                "interrupted; run swarmy remote down {name} to clean up"
            )))
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
        crate::Error::ensure(
            !settings.aws.instance_type.is_empty(),
            "instance type must not be empty",
        )?;
        crate::Error::ensure(settings.disk_gb != 0, "root disk size must be positive")?;
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

fn profile_not_propagated(error: &crate::Error) -> bool {
    // Only the not-yet-propagated identity case retries: the compute API
    // rejects the launch with InvalidParameterValue naming the instance
    // profile until the identity system has propagated it.
    matches!(
        error,
        crate::Error::Aws {
            operation,
            code,
            message,
        } if operation == "ec2:RunInstances"
            && code == "InvalidParameterValue"
            && message.contains("Invalid IAM Instance Profile")
    )
}

pub(crate) async fn wait_running(cloud: &impl Cloud, id: &str, delay: Duration) -> Result<Machine> {
    for _ in 0..120 {
        if let Some(machine) = cloud.get(id).await? {
            crate::Error::ensure(machine.id == id, "provider returned a different machine")?;
            let state = machine.state.as_str();
            if state == "running" && !machine.public_ip.is_empty() && !machine.private_ip.is_empty()
            {
                return Ok(machine);
            }
            crate::Error::ensure(
                state == "pending" || state == "running",
                format!("machine {id} entered {state} while waiting for running"),
            )?;
        }
        tokio::time::sleep(delay).await;
    }
    Err(crate::Error::other(format!(
        "timed out waiting for machine {id} to run with an IP address"
    )))
}

pub(crate) fn key_name(node: &RemoteNode) -> Result<&str> {
    node.key_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| crate::Error::other("invalid key path in remote state"))
}
