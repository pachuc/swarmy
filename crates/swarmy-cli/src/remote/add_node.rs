use std::time::Duration;

use anyhow::{Context, Result, ensure};
use swarmy_config::RemoteNode;

use super::{Cloud, Host, Launch, key_name, state::State, wait_running};

pub async fn run(
    cloud: &impl Cloud,
    host: &impl Host,
    state: &State,
    name: &str,
    delay: Duration,
) -> Result<()> {
    let mut primary = state.require(name)?;
    let settings = primary.launch_settings.clone().context(
        "remote has no saved launch configuration; recreate it with remote up before adding nodes",
    )?;
    ensure!(
        !primary.instance_id.is_empty(),
        "first node has not launched"
    );
    let _: std::net::Ipv4Addr = primary
        .private_ip
        .parse()
        .context("invalid private address")?;
    ensure!(
        settings.region == primary.region,
        "saved launch region differs from remote region"
    );
    let image = settings
        .image
        .clone()
        .context("saved launch image is missing")?;
    let node = RemoteNode {
        name: format!("{name}-{}", primary.nodes.len() + 2),
        region: primary.region.clone(),
        instance_id: String::new(),
        public_ip: String::new(),
        private_ip: String::new(),
        key_path: state
            .directory
            .join(format!("swarmy-{}", ulid::Ulid::generate())),
        ssh_user: primary.ssh_user.clone(),
        ports: primary.ports,
        nodes: Vec::new(),
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
        cloud
            .import_key(
                &key,
                host.generate_key(&node).await?,
                &settings.managed_by_tag,
            )
            .await?;
        node.instance_id = cloud
            .launch(&Launch {
                settings,
                image,
                name: node.name.clone(),
                key_name: key,
            })
            .await?;
        *primary.nodes.last_mut().expect("joining node was inserted") = node.clone();
        state.save(&primary)?;
        let instance = wait_running(cloud, &node.instance_id, delay).await?;
        node.public_ip = instance.public_ip;
        node.private_ip = instance.private_ip;
        *primary.nodes.last_mut().expect("joining node was inserted") = node.clone();
        state.save(&primary)?;
        let address = host.provision(&node, Some(&primary)).await?;
        println!("Remote node {} joined {name}", node.name);
        println!("{}", super::ssh::command_line(&node, &address)?);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    result
        .with_context(|| format!("remote add-node failed; cleanup with swarmy remote down {name}"))
}
