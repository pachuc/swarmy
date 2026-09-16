use std::time::Duration;

use anyhow::{Result, bail};
use swarmy_config::RemoteNode;

use super::{Cloud, key_name, state::State};

pub async fn run(
    cloud: &impl Cloud,
    state: &State,
    node: &RemoteNode,
    delay: Duration,
) -> Result<()> {
    let mut pending = vec![node];
    let mut nodes = Vec::new();
    while let Some(current) = pending.pop() {
        pending.extend(&current.nodes);
        nodes.push(current);
    }
    // Keep all local records and keys until every termination succeeds so down is retryable.
    for current in nodes.iter().rev() {
        terminate(cloud, current, delay).await?;
    }
    for current in nodes {
        state.remove_key(current)?;
    }
    state.remove(node)?;
    println!("Removed remote {}", node.name);
    Ok(())
}

async fn terminate(cloud: &impl Cloud, node: &RemoteNode, delay: Duration) -> Result<()> {
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
