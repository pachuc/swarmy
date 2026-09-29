use std::time::Duration;

use crate::Result;
use swarmy_config::RemoteNode;

use super::{Cloud, Host, MachineSpec, NodeShape, key_name, state::State, wait_running};

pub struct NewNode<'a> {
    pub name: &'a str,
    pub sandboxes: u32,
    pub shape: NodeShape,
}

pub async fn run(
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
    } = request;
    let mut primary = state.require(name)?;
    let Some(mut settings) = primary.launch_settings.clone() else {
        return Err(crate::Error::other(
            "remote has no saved launch configuration; recreate it with remote up before adding nodes",
        ));
    };
    shape.apply(&mut settings)?;
    crate::Error::ensure(
        !primary.instance_id.is_empty(),
        "first node has not launched",
    )?;
    let _: std::net::Ipv4Addr = primary.private_ip.parse()?;
    crate::Error::ensure(
        settings.region == primary.region,
        "saved launch region differs from remote region",
    )?;
    let Some(image) = settings.aws.image.clone() else {
        return Err(crate::Error::other("saved launch image is missing"));
    };
    let node = RemoteNode {
        name: format!("{name}-{}", primary.nodes.len() + 2),
        region: primary.region.clone(),
        instance_id: String::new(),
        launch_attempted: false,
        public_ip: String::new(),
        private_ip: String::new(),
        key_path: state
            .directory
            .join(format!("swarmy-{}", ulid::Ulid::generate())),
        ssh_user: primary.ssh_user.clone(),
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
        let address = host.provision(&node, Some(&primary)).await?;
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
