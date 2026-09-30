use std::{path::Path, time::Duration};

use crate::Result;
use swarmy_config::{RemoteNode, RemoteSettings};

use super::{Cloud, Host, MachineSpec, NodeShape, key_name, state::State, wait_running};

/// Join an existing SSH-reachable machine instead of launching one. The
/// bootstrap login and key reach the new machine; the service user stays the
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
    pub shape: NodeShape,
    /// Explicit `local_storage` from the current configuration. The primary's
    /// saved settings may carry a device resolved on the primary, which a
    /// joining node must never reuse without a lookup, so the saved value is
    /// replaced with this before resolving. Empty means each node resolves
    /// its own instance-store device.
    pub local_storage: String,
    pub existing: Option<ExistingJoin<'a>>,
}

pub(super) async fn run(
    cloud: &impl Cloud,
    host: &impl Host,
    state: &State,
    request: NewNode<'_>,
    delay: Duration,
    options: Option<&super::services::Options<'_>>,
) -> Result<()> {
    let NewNode {
        name,
        sandboxes,
        shape,
        local_storage,
        existing,
    } = request;
    let mut primary = state.require(name)?;
    let Some(mut settings) = primary.launch_settings.clone() else {
        return Err(crate::Error::other(
            "remote has no saved launch configuration; recreate it with remote up before adding nodes",
        ));
    };
    // The saved settings may carry the primary's resolved device; a joining
    // node has its own disks, so it starts from the explicit configuration
    // and resolves its own device below. Only the disk setting is refreshed:
    // the service user must stay the primary's so the tunnel login matches.
    settings.local_storage = local_storage;
    // An existing-host join provisions an operator-owned machine; a launch
    // creates an EC2 machine from the saved settings. The plan resolves
    // before any state or host changes.
    let plan = plan_join(&primary, &mut settings, sandboxes, shape, existing)?;
    crate::Error::ensure(
        settings.region == primary.region,
        "saved launch region differs from remote region",
    )?;
    // An adopted join is reached at its operator-given address on both
    // interfaces; a launched join learns its addresses from the cloud. The
    // bootstrap login reaches a new machine; the service user stays the
    // primary's so the tunnel login matches.
    let (address, ssh_user) = match &plan {
        JoinPlan::Launch { .. } => (String::new(), primary.ssh_user.clone()),
        JoinPlan::Existing { join, .. } => (join.host.into(), join.ssh_user.into()),
    };
    let node = RemoteNode {
        name: format!("{name}-{}", primary.nodes.len() + 2),
        region: primary.region.clone(),
        instance_id: String::new(),
        launch_attempted: false,
        public_ip: address.clone(),
        private_ip: address,
        key_path: state
            .directory
            .join(format!("swarmy-{}", ulid::Ulid::generate())),
        ssh_user,
        ports: primary.ports,
        nodes: Vec::new(),
        sandboxes,
        default_image: None,
        launch_settings: Some(settings.clone()),
        created_at: jiff::Timestamp::now().to_string(),
    };
    // Persist the unique launch token before creating resources, including on interruption.
    primary.nodes.push(node.clone());
    state.save(&primary)?;
    let result = async {
        let mut node = node;
        let address = match plan {
            JoinPlan::Existing { join, effective } => {
                node.launch_attempted = true;
                *primary.nodes.last_mut().expect("joining node was inserted") = node.clone();
                state.save(&primary)?;
                host.adopt_key(&node, join.ssh_key).await?;
                host.provision(&node, Some(&effective)).await?
            }
            JoinPlan::Launch { image } => {
                let key = key_name(&node)?.to_owned();
                let public_key = host.generate_key(&node).await?;
                cloud
                    .import_ssh_key(&key, public_key.clone(), &settings.managed_by_tag)
                    .await?;
                node.launch_attempted = true;
                *primary.nodes.last_mut().expect("joining node was inserted") = node.clone();
                state.save(&primary)?;
                node.instance_id = cloud
                    .create(&MachineSpec::from_settings(
                        &node.name,
                        &image,
                        &key,
                        public_key,
                        &settings,
                        settings.instance_profile(name),
                    ))
                    .await?;
                *primary.nodes.last_mut().expect("joining node was inserted") = node.clone();
                state.save(&primary)?;
                let machine = wait_running(cloud, &node.instance_id, delay).await?;
                node.public_ip = machine.public_ip;
                node.private_ip = machine.private_ip;
                *primary.nodes.last_mut().expect("joining node was inserted") = node.clone();
                state.save(&primary)?;
                if super::aws::resolve_instance_store(host, &mut node).await? {
                    *primary.nodes.last_mut().expect("joining node was inserted") = node.clone();
                    state.save(&primary)?;
                }
                host.provision(&node, Some(&primary)).await?
            }
        };
        finish(host, name, &node, &address, options).await
    }
    .await;
    match result {
        Ok(()) => Ok(()),
        Err(error) => {
            cloud_err!("remote add-node failed; cleanup with swarmy remote down {name}");
            Err(error)
        }
    }
}

/// Install joining-node services and print the join summary once an address
/// is known. Shared by launched and adopted joins.
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

/// How a node joins: launch an EC2 machine from the saved image, or
/// provision an existing host. The existing variant carries the primary as
/// the joining node reaches it, boxed because it dwarfs the image name.
enum JoinPlan<'a> {
    Launch {
        image: String,
    },
    Existing {
        join: ExistingJoin<'a>,
        effective: Box<RemoteNode>,
    },
}

/// Resolve how a node joins before any state or host changes. An
/// existing-host join validates the operator-given addresses and key and
/// clears the EC2 shape it never uses; a launch applies the shape override
/// and requires the saved image. The joining node reaches the primary's
/// backing services through its recorded private address unless the
/// operator overrides it (a vSwitch address, for example); never assume an
/// AWS VPC.
fn plan_join<'a>(
    primary: &RemoteNode,
    settings: &mut RemoteSettings,
    sandboxes: u32,
    shape: NodeShape,
    existing: Option<ExistingJoin<'a>>,
) -> Result<JoinPlan<'a>> {
    let Some(join) = existing else {
        shape.apply(settings)?;
        crate::Error::ensure(
            !primary.instance_id.is_empty(),
            "first node has not launched",
        )?;
        let _: std::net::Ipv4Addr = primary.private_ip.parse().map_err(|source| {
            crate::Error::context(source, "primary's private address must be an IPv4 address")
        })?;
        let Some(image) = settings.aws.image.clone() else {
            return Err(crate::Error::other("saved launch image is missing"));
        };
        return Ok(JoinPlan::Launch { image });
    };
    validate_existing(&join)?;
    settings.aws.image = None;
    settings.aws.instance_type = String::new();
    crate::Error::ensure(
        sandboxes == 0 || !settings.local_storage.is_empty(),
        "sandbox nodes need local storage: pass --local-storage with a block device or dir:/path",
    )?;
    let mut effective = primary.clone();
    if let Some(address) = join.primary_address {
        effective.private_ip = address.into();
    }
    let _: std::net::Ipv4Addr = effective.private_ip.parse().map_err(|source| {
        crate::Error::context(source, "primary's private address must be an IPv4 address")
    })?;
    Ok(JoinPlan::Existing {
        join,
        effective: Box::new(effective),
    })
}

/// Check an existing-host join before any state or host changes: the
/// bootstrap address, login, and key, plus the primary override (if any),
/// which must be an IPv4 address for the provisioning script.
fn validate_existing(join: &ExistingJoin<'_>) -> Result<()> {
    super::adopt::validate_bootstrap(join.host, join.ssh_user, join.ssh_key, "add-node")?;
    if let Some(address) = join.primary_address {
        let _: std::net::Ipv4Addr = address.parse().map_err(|source| {
            crate::Error::context(source, "add-node --primary-address must be an IPv4 address")
        })?;
    }
    Ok(())
}
