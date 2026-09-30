use std::{path::Path, time::Duration};

use crate::Result;
use swarmy_config::RemoteNode;

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
    // An existing-host primary joins operator-owned machines: no EC2 shape
    // and no instance-store lookup. An AWS primary launches EC2 machines and
    // rejects the existing-host flags (checked by the caller).
    if let Some(join) = existing.as_ref() {
        validate_existing(join)?;
    }
    let join = existing.as_ref();
    if join.is_none() {
        shape.apply(&mut settings)?;
    }
    // The saved settings may carry the primary's resolved device; a joining
    // node has its own disks, so it starts from the explicit configuration
    // and resolves its own device below. Only the disk setting is refreshed:
    // the service user must stay the primary's so the tunnel login matches.
    settings.local_storage = local_storage;
    if join.is_some() {
        settings.aws.image = None;
        settings.aws.instance_type = String::new();
        crate::Error::ensure(
            sandboxes == 0 || !settings.local_storage.is_empty(),
            "sandbox nodes need local storage: pass --local-storage with a block device or dir:/path",
        )?;
    } else {
        crate::Error::ensure(
            !primary.instance_id.is_empty(),
            "first node has not launched",
        )?;
    }
    crate::Error::ensure(
        settings.region == primary.region,
        "saved launch region differs from remote region",
    )?;
    // The joining node reaches the primary's backing services through the
    // recorded private address unless the operator overrides it (a vSwitch
    // address, for example); never assume an AWS VPC.
    let mut effective = primary.clone();
    if let Some(join) = join {
        if let Some(address) = join.primary_address {
            effective.private_ip = address.into();
        }
    }
    let _: std::net::Ipv4Addr = effective.private_ip.parse().map_err(|source| {
        crate::Error::context(source, "primary's private address must be an IPv4 address")
    })?;
    let image = if join.is_none() {
        let Some(image) = settings.aws.image.clone() else {
            return Err(crate::Error::other("saved launch image is missing"));
        };
        Some(image)
    } else {
        None
    };
    let node = RemoteNode {
        name: format!("{name}-{}", primary.nodes.len() + 2),
        region: primary.region.clone(),
        instance_id: String::new(),
        launch_attempted: false,
        // An adopted join is reached at its operator-given address on both
        // interfaces; a launched join learns its addresses from the cloud.
        public_ip: join.map(|join| join.host.to_owned()).unwrap_or_default(),
        private_ip: join.map(|join| join.host.to_owned()).unwrap_or_default(),
        key_path: state
            .directory
            .join(format!("swarmy-{}", ulid::Ulid::generate())),
        // The bootstrap login reaches a new machine; the service user stays
        // the primary's so the tunnel login matches.
        ssh_user: join
            .map(|join| join.ssh_user.to_owned())
            .unwrap_or_else(|| primary.ssh_user.clone()),
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
        if let Some(join) = join {
            node.launch_attempted = true;
            *primary.nodes.last_mut().expect("joining node was inserted") = node.clone();
            state.save(&primary)?;
            host.adopt_key(&node, join.ssh_key).await?;
            let address = host.provision(&node, Some(&effective)).await?;
            if let Some(options) = options {
                host.services(&node, &address, options).await?;
            }
            cloud_out!("Remote node {} joined {name}", node.name);
            cloud_out!("{}", super::ssh::command_line(&node, &address)?);
            return Ok::<_, crate::Error>(());
        }
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
                image
                    .as_deref()
                    .expect("AWS joins resolve an image before launch"),
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
        let address = host.provision(&node, Some(&effective)).await?;
        if let Some(options) = options {
            host.services(&node, &address, options).await?;
        }
        cloud_out!("Remote node {} joined {name}", node.name);
        cloud_out!("{}", super::ssh::command_line(&node, &address)?);
        Ok::<_, crate::Error>(())
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

/// Check an existing-host join before any state or host changes: the join
/// address must be reachable over IP, the bootstrap login must be SSH-safe,
/// the key file must exist, and the primary override (if any) must be an
/// IPv4 address for the provisioning script.
fn validate_existing(join: &ExistingJoin<'_>) -> Result<()> {
    let _: std::net::IpAddr = join.host.parse().map_err(|source| {
        crate::Error::context(source, "add-node --host must be an IP address")
    })?;
    crate::Error::ensure(
        !join.ssh_user.is_empty()
            && join
                .ssh_user
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
        "add-node --ssh-user must contain letters, digits, hyphens, or underscores",
    )?;
    crate::Error::ensure(
        join.ssh_key.is_file(),
        format!(
            "add-node --ssh-key {} does not exist",
            join.ssh_key.display()
        ),
    )?;
    if let Some(address) = join.primary_address {
        let _: std::net::Ipv4Addr = address.parse().map_err(|source| {
            crate::Error::context(source, "add-node --primary-address must be an IPv4 address")
        })?;
    }
    Ok(())
}
