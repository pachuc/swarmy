use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use swarmy_config::{RemoteNode, RemotePorts, RemoteSettings};

use super::{Cloud, Host, Launch, key_name, state::State, wait_running};

pub async fn run(
    cloud: &impl Cloud,
    host: &impl Host,
    state: &State,
    settings: &RemoteSettings,
    name: &str,
    delay: Duration,
) -> Result<()> {
    ensure!(
        state.read(name)?.is_none(),
        "remote node {name} already exists; run swarmy remote down {name} first"
    );
    ensure!(
        !settings.region.is_empty()
            && !settings.subnet.is_empty()
            && !settings.security_group.is_empty(),
        "configure remote.region, remote.subnet, and remote.security_group"
    );
    ensure!(
        settings.disk_gb > 0
            && !settings.managed_by_tag.is_empty()
            && !settings.instance_type.is_empty(),
        "remote disk_gb must be positive and instance_type and managed_by_tag must not be empty"
    );
    let started = Instant::now();
    println!("Resolving Ubuntu image in {}", settings.region);
    let image = match &settings.image {
        Some(image) => image.clone(),
        None => cloud.stock_image().await?,
    };
    let mut node = RemoteNode {
        name: name.into(),
        region: settings.region.clone(),
        instance_id: String::new(),
        public_ip: String::new(),
        private_ip: String::new(),
        key_path: state
            .directory
            .join(format!("swarmy-{}", ulid::Ulid::generate())),
        ssh_user: "ubuntu".into(),
        ports: RemotePorts::default(),
        nodes: Vec::new(),
        created_at: jiff::Timestamp::now().to_string(),
    };
    // Write the key name before any AWS mutation so down can recover an interrupted launch.
    state.save(&node)?;
    let result = provision(cloud, host, state, settings, image, &mut node, delay).await;
    let address = result
        .with_context(|| format!("remote up failed; cleanup with swarmy remote down {name}"))?;
    println!(
        "Remote node {name} ready in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    println!("{}", super::ssh::command_line(&node, &address));
    Ok(())
}

async fn provision(
    cloud: &impl Cloud,
    host: &impl Host,
    state: &State,
    settings: &RemoteSettings,
    image: String,
    node: &mut RemoteNode,
    delay: Duration,
) -> Result<String> {
    println!("Generating and importing ed25519 SSH key");
    let public_key = host.generate_key(node).await?;
    let key_name = key_name(node)?.to_owned();
    cloud
        .import_key(&key_name, public_key, &settings.managed_by_tag)
        .await?;
    println!("Launching {} from {image}", settings.instance_type);
    node.instance_id = cloud
        .launch(&Launch {
            settings: settings.clone(),
            image,
            name: node.name.clone(),
            key_name,
        })
        .await?;
    state.save(node)?;
    println!("Waiting for {} to run", node.instance_id);
    let instance = wait_running(cloud, &node.instance_id, delay).await?;
    node.public_ip = instance.public_ip;
    node.private_ip = instance.private_ip;
    state.save(node)?;
    host.provision(node).await
}
