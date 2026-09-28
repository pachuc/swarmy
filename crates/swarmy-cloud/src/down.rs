use std::{
    io::{self, IsTerminal, Write},
    time::Duration,
};

use anyhow::{Result, bail};
use swarmy_config::RemoteNode;

use super::{Cloud, key_name, state::State};

#[derive(Default)]
struct Report {
    failures: Vec<String>,
    live: Vec<String>,
}

impl Report {
    fn failed(&mut self, resource: &str, permission: &str, error: &anyhow::Error) {
        self.failures.push(format!(
            "Skipped {resource}: requires {permission}; {error:#}"
        ));
    }

    fn print(&self) {
        for failure in &self.failures {
            eprintln!("{failure}");
        }
    }
}

fn permission<'a>(error: &anyhow::Error, default: &'a str, alternate: &'a str) -> &'a str {
    if format!("{error:#}").contains(alternate) {
        alternate
    } else {
        default
    }
}

pub fn confirm(keep_bucket: bool, yes: bool, json: bool) -> Result<()> {
    if keep_bucket || yes {
        return Ok(());
    }
    if json {
        bail!("remote down --json requires --yes unless --keep-bucket is set");
    }
    if !io::stdin().is_terminal() {
        bail!("remote down requires --yes without a terminal");
    }
    print!("Permanently delete remote bucket and all its objects? [y/N] ");
    io::stdout().flush()?;
    let mut answer = String::new();
    io::stdin().read_line(&mut answer)?;
    if answer.trim() != "y" && answer.trim() != "yes" {
        bail!("remote down cancelled");
    }
    Ok(())
}

pub async fn run(
    cloud: &impl Cloud,
    state: &State,
    node: &RemoteNode,
    delay: Duration,
    keep_bucket: bool,
) -> Result<()> {
    let mut pending = vec![node];
    let mut nodes = Vec::new();
    while let Some(current) = pending.pop() {
        pending.extend(&current.nodes);
        nodes.push(current);
    }
    let mut report = Report::default();
    // Every operation is attempted even if another AWS permission is denied.
    for current in nodes.iter().rev() {
        if !current.launch_attempted && current.instance_id.is_empty() {
            println!("Nothing was launched for {}", current.name);
            continue;
        }
        let key = key_name(current)?;
        stop_instance(cloud, current, key, delay, &mut report).await;
        if let Err(error) = cloud.delete_ssh_key(key).await {
            report.failed(
                &format!("key pair {key}"),
                permission(&error, "ec2:DescribeKeyPairs", "ec2:DeleteKeyPair"),
                &error,
            );
        } else {
            println!("Key pair {key}: removed or absent");
        }
    }
    report.print();
    if !report.live.is_empty() {
        bail!(
            "instances {} may still exist; local state retained for retry",
            report.live.join(", ")
        );
    }
    if let Some(bucket) = node.bucket() {
        let role = node
            .cloud_settings()
            .instance_profile(&node.name)
            .ok_or_else(|| anyhow::anyhow!("bucket has no node role"))?;
        if keep_bucket {
            println!("Kept bucket {bucket} and guarding role and instance profile {role}");
        } else {
            let removed = cloud.delete_bucket(bucket).await?;
            println!(
                "Bucket {bucket}: {}",
                if removed { "removed" } else { "absent" }
            );
            let (profile, role_removed) = cloud.delete_node_role(&role).await?;
            println!(
                "Instance profile {role}: {}",
                if profile { "removed" } else { "absent" }
            );
            println!(
                "Role {role}: {}",
                if role_removed { "removed" } else { "absent" }
            );
        }
    }
    for current in nodes {
        state.remove_key(current)?;
    }
    state.remove(node)?;
    println!("Removed remote {}", node.name);
    Ok(())
}

async fn stop_instance(
    cloud: &impl Cloud,
    node: &RemoteNode,
    key: &str,
    delay: Duration,
    report: &mut Report,
) {
    let id = if node.instance_id.is_empty() {
        match cloud.find_by_tag(key).await {
            Ok(id) => id,
            Err(error) => {
                report.failed(
                    &format!("launch for {}", node.name),
                    "ec2:DescribeInstances",
                    &error,
                );
                report.live.push(node.name.clone());
                return;
            }
        }
    } else {
        Some(node.instance_id.clone())
    };
    let Some(id) = id else {
        println!("No launched instance found for {}", node.name);
        return;
    };
    println!("Terminating {id}");
    if let Err(error) = cloud.destroy(&id).await {
        report.failed(
            &format!("instance {id}"),
            permission(&error, "ec2:TerminateInstances", "ec2:DescribeInstances"),
            &error,
        );
        report.live.push(id);
        return;
    }
    match wait_terminated(cloud, &id, delay).await {
        Ok(()) => println!("Confirmed {id} is terminated or absent"),
        Err(error) => {
            report.failed(&format!("instance {id}"), "ec2:DescribeInstances", &error);
            report.live.push(id);
        }
    }
}

async fn wait_terminated(cloud: &impl Cloud, id: &str, delay: Duration) -> Result<()> {
    for _ in 0..120 {
        match cloud.get(id).await? {
            None => return Ok(()),
            Some(machine) if machine.state == "terminated" => return Ok(()),
            Some(_) => tokio::time::sleep(delay).await,
        }
    }
    bail!("timed out waiting for {id} to terminate")
}
