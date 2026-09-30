//! Provision an existing SSH-reachable machine as a remote's first node.
//!
//! `adopt` records the machine with the existing-host provider, then runs
//! the shared provisioning tail from [`super::up`] (services, image build)
//! against its address. No machine is created and no cloud machine call
//! happens: only the bucket setup touches object-storage APIs.
use std::{path::Path, time::Instant};

use crate::Result;
use swarmy_config::{RemoteNode, RemotePorts, RemoteSettings};

use super::{Cloud, Host, ObjectBucket, state::State};

pub(super) struct AdoptNode<'a> {
    pub name: &'a str,
    pub host: &'a str,
    pub ssh_user: &'a str,
    pub ssh_key: &'a Path,
    pub sandboxes: u32,
}

pub(super) async fn run(
    cloud: &impl Cloud,
    host: &impl Host,
    state: &State,
    settings: &RemoteSettings,
    request: AdoptNode<'_>,
    options: super::services::Options<'_>,
) -> Result<()> {
    let name = request.name;
    if state.read(name)?.is_some() {
        return Err(crate::Error::AlreadyExists(name.to_owned()));
    }
    validate(settings, &request)?;
    // Static keys are checked with a cheap read-only call before any state
    // exists, so wrong keys fail here instead of after the record is saved.
    if let Some(spec) = &settings.bucket
        && spec.needs_static_keys()
    {
        cloud
            .verify_bucket_access(&ObjectBucket::from_spec(
                name,
                spec,
                &settings.region,
                settings.instance_profile(name),
            ))
            .await
            .map_err(|source| {
                crate::Error::context(
                    source,
                    format!(
                        "static bucket keys for {} were rejected; check --s3-access-key and the secret",
                        spec.describe(),
                    ),
                )
            })?;
    }
    let started = Instant::now();
    let mut node = initial_node(state, settings, &request);
    // Write the record before any bucket or host mutation so down can
    // recover an interrupted adoption.
    state.save(&node)?;
    let result = async {
        super::ensure_remote_bucket(cloud, settings, name).await?;
        node.launch_attempted = true;
        state.save(&node)?;
        host.adopt_key(&node, request.ssh_key).await?;
        let address = host.provision(&node, None).await?;
        super::up::provision_stack(host, state, settings, &mut node, &address, options, started)
            .await?;
        Ok::<_, crate::Error>(())
    }
    .await;
    match result {
        Ok(()) => Ok(()),
        Err(error) => {
            cloud_err!("remote adopt failed; cleanup with swarmy remote down {name}");
            Err(error)
        }
    }
}

/// Build the first node record for an adopted machine. Both addresses are
/// the operator-given host: the laptop provisions over it and later nodes
/// reach the primary through it unless `add-node --primary-address`
/// overrides that path. There is no instance type or image: the machine
/// already exists. The key path is generated; the bootstrap key is copied
/// there before provisioning.
fn initial_node(state: &State, settings: &RemoteSettings, request: &AdoptNode<'_>) -> RemoteNode {
    RemoteNode {
        name: request.name.into(),
        region: settings.region.clone(),
        instance_id: String::new(),
        launch_attempted: false,
        public_ip: request.host.into(),
        private_ip: request.host.into(),
        key_path: state
            .directory
            .join(format!("swarmy-{}", ulid::Ulid::generate())),
        ssh_user: request.ssh_user.into(),
        ports: RemotePorts::default(),
        nodes: Vec::new(),
        sandboxes: request.sandboxes,
        default_image: None,
        launch_settings: Some(RemoteSettings {
            provider: swarmy_config::Provider::Existing,
            service_user: settings.service_user.clone(),
            local_storage: settings.local_storage.clone(),
            aws: swarmy_config::AwsSettings {
                image: None,
                instance_type: String::new(),
                ..settings.aws.clone()
            },
            ..settings.clone()
        }),
        created_at: jiff::Timestamp::now().to_string(),
    }
}

fn validate(settings: &RemoteSettings, request: &AdoptNode<'_>) -> Result<()> {
    super::validate_region(settings, "adopt")?;
    validate_bootstrap(request.host, request.ssh_user, request.ssh_key, "adopt")?;
    swarmy_config::validate_service_user(&settings.service_user)?;
    super::validate_sandbox_storage(request.sandboxes, &settings.local_storage, "adopt")?;
    super::validate_bucket_binding(settings, request.name)?;
    // An existing host can never use an IAM instance role: the role would be
    // created but the machine could never assume it. Require static keys.
    if let Some(spec) = &settings.bucket {
        crate::Error::ensure(
            !spec.is_aws(),
            "existing-host remotes cannot use an AWS instance-role bucket; pass static keys with --s3-endpoint, --s3-access-key, and --s3-secret-file",
        )?;
    }
    Ok(())
}

/// Check an operator-given bootstrap address, login, and key before any
/// state or host changes. `command` names the calling subcommand so messages
/// point at its flags. The address must be IPv4 here: provisioning rejects
/// anything else, so accepting IPv6 would create state and a bucket before
/// failing.
pub(super) fn validate_bootstrap(
    host: &str,
    ssh_user: &str,
    ssh_key: &Path,
    command: &str,
) -> Result<()> {
    super::parse_ipv4(&format!("{command} --host"), host)?;
    crate::Error::ensure(
        !ssh_user.is_empty()
            && ssh_user
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
        format!("{command} --ssh-user must contain letters, digits, hyphens, or underscores"),
    )?;
    crate::Error::ensure(
        ssh_key.is_file(),
        format!("{command} --ssh-key {} does not exist", ssh_key.display()),
    )?;
    Ok(())
}
