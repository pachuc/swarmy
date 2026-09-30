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
    let started = Instant::now();
    let mut node = initial_node(state, settings, &request);
    // Write the record before any bucket or host mutation so down can
    // recover an interrupted adoption.
    state.save(&node)?;
    let result = async {
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
    crate::Error::ensure(
        !settings.region.is_empty(),
        "configure remote.region in config.toml before running swarmy remote adopt",
    )?;
    validate_bootstrap(request.host, request.ssh_user, request.ssh_key, "adopt")?;
    swarmy_config::validate_service_user(&settings.service_user)?;
    crate::Error::ensure(
        request.sandboxes == 0 || !settings.local_storage.is_empty(),
        "sandbox nodes need local storage: pass --local-storage with a block device or dir:/path",
    )?;
    if let Some(spec) = &settings.bucket {
        crate::Error::ensure(
            request.name.len() <= 57,
            "bucket-backed remote name must be at most 57 characters to fit the IAM role name",
        )?;
        spec.validate_name()?;
    }
    Ok(())
}

/// Check an operator-given bootstrap address, login, and key before any
/// state or host changes. `command` names the calling subcommand so messages
/// point at its flags.
pub(super) fn validate_bootstrap(
    host: &str,
    ssh_user: &str,
    ssh_key: &Path,
    command: &str,
) -> Result<()> {
    let _: std::net::IpAddr = host.parse().map_err(|source| {
        crate::Error::context(source, format!("{command} --host must be an IP address"))
    })?;
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
