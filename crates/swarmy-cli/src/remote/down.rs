use std::time::Duration;

use anyhow::{Result, bail, ensure};
use swarmy_config::RemoteNode;

use super::{Cloud, key_name, state::State};

pub async fn run(
    cloud: &impl Cloud,
    state: &State,
    node: &RemoteNode,
    delay: Duration,
) -> Result<()> {
    ensure!(
        node.nodes.is_empty(),
        "this version cannot remove a multi-node remote cluster"
    );
    let key = key_name(node)?;
    let id = if node.instance_id.is_empty() {
        cloud.find_launch(key).await?
    } else {
        Some(node.instance_id.clone())
    };
    if let Some(id) = id {
        println!("Terminating {id}");
        cloud.terminate(&id).await?;
        wait_terminated(cloud, &id, delay).await?;
        println!("Confirmed {id} is terminated or absent");
    }
    println!("Deleting key pair {key}");
    cloud.delete_key(key).await?;
    state.remove(node)?;
    println!("Removed remote node {}", node.name);
    Ok(())
}

async fn wait_terminated(cloud: &impl Cloud, id: &str, delay: Duration) -> Result<()> {
    for _ in 0..120 {
        match cloud.instance(id).await? {
            None => return Ok(()),
            Some(instance) if instance.status == "terminated" => return Ok(()),
            Some(_) => tokio::time::sleep(delay).await,
        }
    }
    bail!("timed out waiting for {id} to terminate; state retained for retry")
}
