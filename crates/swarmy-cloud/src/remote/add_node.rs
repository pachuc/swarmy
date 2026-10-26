use std::path::Path;

use crate::Result;
use swarmy_config::{RemoteNode, RemoteSettings};

use super::{Host, state::State};

/// Join an existing SSH-reachable machine. The bootstrap login and key reach the new machine; the service user stays the
/// primary's so the tunnel login matches. `primary_address` overrides the
/// address the joining node uses for the primary's backing services (a
/// vSwitch address, for example); empty keeps the recorded private address.
pub(super) struct ExistingJoin<'a> {
    pub host: &'a str,
    pub ssh_user: &'a str,
    pub ssh_key: &'a Path,
    pub primary_address: Option<&'a str>,
}

pub(super) struct NewNode<'a> {
    pub name: &'a str,
    pub sandboxes: u32,
    /// Explicit storage, not the primary's resolved disk.
    pub local_storage: String,
    pub existing: ExistingJoin<'a>,
}

pub(super) async fn run(
    host: &impl Host,
    state: &State,
    request: NewNode<'_>,
    options: Option<&super::services::Options<'_>>,
) -> Result<()> {
    let NewNode {
        name,
        sandboxes,
        local_storage,
        existing,
    } = request;
    let mut primary = state.require(name)?;
    let Some(mut settings) = primary.launch_settings.clone() else {
        return Err(crate::Error::other(
            "remote has no saved launch configuration; adopt it again before adding nodes",
        ));
    };
    settings.local_storage = local_storage;
    let effective = plan_join(&primary, &mut settings, sandboxes, &existing)?;
    crate::Error::ensure(
        settings.region == primary.region,
        "saved launch region differs from remote region",
    )?;
    let node = RemoteNode {
        name: format!("{name}-{}", primary.nodes.len() + 2),
        region: primary.region.clone(),
        instance_id: String::new(),
        launch_attempted: false,
        public_ip: existing.host.into(),
        private_ip: existing.host.into(),
        key_path: state
            .directory
            .join(format!("swarmy-{}", ulid::Ulid::generate())),
        ssh_user: existing.ssh_user.into(),
        ports: primary.ports,
        nodes: Vec::new(),
        sandboxes,
        default_image: None,
        launch_settings: Some(settings),
        created_at: jiff::Timestamp::now().to_string(),
    };
    primary.nodes.push(node.clone());
    state.save(&primary)?;
    let result = async {
        let mut node = node;
        node.launch_attempted = true;
        *primary.nodes.last_mut().expect("joining node was inserted") = node.clone();
        state.save(&primary)?;
        host.adopt_key(&node, existing.ssh_key).await?;
        let address = host.provision(&node, Some(&effective)).await?;
        finish(host, name, &node, &address, options).await
    }
    .await;
    if result.is_err() {
        cloud_err!("remote add-node failed; cleanup with swarmy remote down {name}");
    }
    result
}

/// Install joining-node services and print the join summary once an address
/// is known.
async fn finish(
    host: &impl Host,
    primary: &str,
    node: &RemoteNode,
    address: &str,
    options: Option<&super::services::Options<'_>>,
) -> Result<()> {
    if let Some(options) = options {
        host.services(node, address, options).await?;
    }
    cloud_out!("Remote node {} joined {primary}", node.name);
    cloud_out!("{}", super::ssh::command_line(node, address)?);
    Ok(())
}

/// Require both flags before changing state.
pub(super) fn resolve_join<'a>(
    remote: &str,
    join_host: Option<&'a str>,
    ssh_user: Option<&'a str>,
    ssh_key: Option<&'a Path>,
    primary_address: Option<&'a str>,
) -> Result<ExistingJoin<'a>> {
    let Some(host) = join_host else {
        return Err(crate::Error::other(format!(
            "remote {remote} uses existing hosts; pass --host ADDRESS to join one"
        )));
    };
    let Some(ssh_key) = ssh_key else {
        return Err(crate::Error::other(
            "joining an existing host needs --ssh-key PATH",
        ));
    };
    Ok(ExistingJoin {
        host,
        ssh_user: ssh_user.unwrap_or("root"),
        ssh_key,
        primary_address,
    })
}

/// Resolve the primary's effective address without reusing its disk.
fn plan_join(
    primary: &RemoteNode,
    settings: &mut RemoteSettings,
    sandboxes: u32,
    join: &ExistingJoin<'_>,
) -> Result<RemoteNode> {
    validate_existing(join)?;
    settings.aws.image = None;
    settings.aws.instance_type = String::new();
    super::validate_sandbox_storage(sandboxes, &settings.local_storage, "add-node")?;
    let mut effective = primary.clone();
    if let Some(address) = join.primary_address {
        effective.private_ip = address.into();
    }
    super::parse_ipv4("primary's private address", &effective.private_ip)?;
    Ok(effective)
}

/// Check an existing-host join before any state or host changes: the
/// bootstrap address, login, and key, plus the primary override (if any),
/// which must be an IPv4 address for the provisioning script.
fn validate_existing(join: &ExistingJoin<'_>) -> Result<()> {
    super::adopt::validate_bootstrap(join.host, join.ssh_user, join.ssh_key, "add-node")?;
    if let Some(address) = join.primary_address {
        super::parse_ipv4("add-node --primary-address", address)?;
    }
    Ok(())
}
