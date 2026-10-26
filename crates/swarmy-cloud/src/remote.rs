//! Remote provisioning, compiled only with the remote feature.
use super::{Cloud, Command, Host, ObjectBucket, Ownership, RemoteNode, Result, services, ssh};

use state::State;
use std::{
    future::Future,
    path::{Path, PathBuf},
};
use swarmy_config::{RemoteSettings, Settings};

mod add_node;
mod adopt;
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
mod upgrade;

pub use super::services::Options as ServiceOptions;
pub use down::DeletionPlan;

/// Records launched on EC2 predate removal of that provider.
fn launched_on_ec2(name: &str) -> crate::Error {
    crate::Error::other(format!(
        "remote {name} was launched on EC2, which swarmy no longer manages; remove its instances, key pairs, bucket, IAM role, and instance profile in the AWS console, then delete its record under <state directory>/remote"
    ))
}

/// Require a configured region before any state or host changes. Shared by
/// `adopt` before state changes.
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

/// Validate an adopted bucket name before any state or host changes.
pub(super) fn validate_bucket_binding(settings: &RemoteSettings, _name: &str) -> Result<()> {
    if let Some(spec) = &settings.bucket {
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

/// Set the bucket up through object-storage APIs for adoption.
pub(super) async fn ensure_remote_bucket(
    cloud: &impl Cloud,
    settings: &RemoteSettings,
    name: &str,
) -> Result<()> {
    if let Some(spec) = &settings.bucket {
        cloud
            .ensure_bucket(&ObjectBucket::from_spec(name, spec, &settings.region))
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
    NeedsConfirmation { plan: DeletionPlan },
}

fn down_target(state: &State, name: &str) -> Result<RemoteNode> {
    let node = state.require(name)?;
    if node.cloud_settings().provider != swarmy_config::Provider::Existing {
        return Err(launched_on_ec2(name));
    }
    Ok(node)
}

fn existing_launch(node: &RemoteNode) -> Result<&RemoteSettings> {
    let launch = node.launch_settings.as_ref().ok_or_else(|| {
        crate::Error::other(
            "remote has no saved launch configuration; adopt it again before adding nodes",
        )
    })?;
    if launch.provider != swarmy_config::Provider::Existing {
        return Err(launched_on_ec2(&node.name));
    }
    Ok(launch)
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
    let cache = state_dir.join("core");
    let state = State::open(&state_dir.join("remote"))?;
    match command {
        Command::Adopt { .. } => {
            Box::pin(run_adopt(&state, loaded.settings, command, &cache)).await?;
            Ok(RunOutcome::Completed)
        }
        Command::AddNode { .. } => {
            Box::pin(run_add_node(&state, loaded.settings, command, &cache)).await?;
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
                &cache,
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
            down_target(&state, &name)?;
            let cloud = buckets::S3Buckets;
            if !keep_bucket
                && !yes
                && !confirmed
                && let Some(plan) = down::plan(&cloud, &state, &node).await?
            {
                return Ok(RunOutcome::NeedsConfirmation { plan });
            }
            down::run_existing(&cloud, &ssh::Ssh::teardown(), &state, &node, keep_bucket).await?;
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
fn resolve_recipe(no_image: bool, image_recipe: &Path) -> Result<Option<PathBuf>> {
    if no_image {
        Ok(None)
    } else {
        Ok(Some(crate::node_files::recipe(image_recipe)?))
    }
}

/// The SSH host for provisioning, carrying the matching release when this client has one.
fn provisioning_host(cache: &Path) -> Result<ssh::Ssh> {
    let binaries = if let Some(release) = crate::release::Release::for_client()? {
        cloud_out!(
            "Using swarmy {} release binaries; nothing is built on the node",
            release.tag()
        );
        ssh::Binaries::Release {
            release,
            cache: cache.to_owned(),
        }
    } else {
        let checkout = ssh::find_checkout(&std::env::current_dir()?)?;
        cloud_out!(
            "Development client {}: the node builds swarmy-core from {} (copied to <node dir>/source)",
            swarmy_version::IDENTITY,
            checkout.display()
        );
        ssh::Binaries::Build { checkout }
    };
    Ok(ssh::Ssh::new(binaries))
}

/// Provision an existing SSH-reachable machine as the first node of a
/// remote. The machine is recorded with the existing-host provider, then the
/// shared provisioning `up` runs after its machine is ready (services,
/// image build) runs against its address. No machine is created and no
/// cloud machine call happens.
async fn run_adopt(
    state: &State,
    mut settings: Settings,
    command: Command,
    cache: &Path,
) -> Result<()> {
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
    let recipe = resolve_recipe(no_image, &image_recipe)?;
    let host = provisioning_host(cache)?;
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
    let cloud = buckets::S3Buckets;
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

/// Join another existing host to a remote.
async fn run_add_node(
    state: &State,
    mut settings: Settings,
    command: Command,
    cache: &Path,
) -> Result<()> {
    let Command::AddNode {
        name,
        sandboxes,
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
    existing_launch(&node)?;
    let host = provisioning_host(cache)?;
    let existing = add_node::resolve_join(
        &name,
        join_host.as_deref(),
        ssh_user.as_deref(),
        ssh_key.as_deref(),
        primary_address.as_deref(),
    )?;
    // Box the join future: it holds saved launch state across awaits and
    // would otherwise exceed the large-future budget.
    guard(
        &name,
        // Pin the join so service-user paths and disk state do not inflate
        // the guarded future past the pedantic size limit.
        Box::pin(add_node::run(
            &host,
            state,
            add_node::NewNode {
                name: &name,
                sandboxes: sandboxes.unwrap_or_else(swarmy_config::default_sandboxes),
                local_storage: settings.remote.local_storage.clone(),
                existing,
            },
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
