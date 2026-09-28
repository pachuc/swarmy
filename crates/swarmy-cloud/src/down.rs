use std::{
    io::{self, IsTerminal, Write},
    time::Duration,
};

use anyhow::{Result, bail};
use swarmy_config::RemoteNode;

use super::{Cloud, Ownership, key_name, state::State};

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

pub async fn confirm(
    cloud: &impl Cloud,
    state: &State,
    node: &RemoteNode,
    keep_bucket: bool,
    yes: bool,
    json: bool,
) -> Result<()> {
    if keep_bucket || yes {
        return Ok(());
    }
    let Some(bucket) = node.bucket() else {
        return Ok(());
    };
    let role = node
        .cloud_settings()
        .instance_profile(&node.name)
        .ok_or_else(|| anyhow::anyhow!("bucket has no node role"))?;
    let bucket_status = cloud.bucket_ownership(bucket, &node.name).await?;
    let bucket_owned =
        !state.bucket_shared(&node.name, bucket)? && bucket_status == Ownership::Owned;
    let role_shared = state.role_shared(&node.name, &role)?;
    let (profile, role_status) = cloud.role_ownership(&role, &node.name).await?;
    let iam_safe = !role_shared && (bucket_owned || bucket_status == Ownership::Absent);
    let iam_owned =
        iam_safe && profile != Ownership::Unmanaged && role_status != Ownership::Unmanaged;
    let profile_owned = iam_owned && profile == Ownership::Owned;
    let role_owned = iam_owned && role_status == Ownership::Owned;
    if !bucket_owned && !profile_owned && !role_owned {
        return Ok(());
    }
    if json {
        bail!("remote down --json requires --yes to delete owned resources");
    }
    if !io::stdin().is_terminal() {
        bail!("remote down requires --yes without a terminal");
    }
    println!("Permanently delete these owned resources and their data:");
    if bucket_owned {
        println!("  bucket {bucket} (all objects and versions)");
    }
    if profile_owned {
        println!("  instance profile {role}");
    }
    if role_owned {
        println!("  role {role}");
    }
    print!("Continue? [y/N] ");
    io::stdout().flush()?;
    let mut answer = String::new();
    io::stdin().read_line(&mut answer)?;
    if answer.trim() != "y" && answer.trim() != "yes" {
        bail!("remote down cancelled");
    }
    Ok(())
}

/// Adoption is deliberately interactive and requires typing every exact resource name.
pub async fn tag(cloud: &impl Cloud, state: &State, node: &RemoteNode) -> Result<()> {
    tag_with_confirmation(cloud, state, node, |kind, name| {
        if !io::stdin().is_terminal() {
            bail!("remote tag requires a terminal");
        }
        print!(
            "Type the exact {kind} name {name} to adopt it for remote {}: ",
            node.name
        );
        io::stdout().flush()?;
        let mut answer = String::new();
        io::stdin().read_line(&mut answer)?;
        if answer.trim() != name {
            bail!("remote tag cancelled");
        }
        Ok(())
    })
    .await
}

pub(crate) async fn tag_with_confirmation(
    cloud: &impl Cloud,
    state: &State,
    node: &RemoteNode,
    mut confirm_name: impl FnMut(&str, &str) -> Result<()>,
) -> Result<()> {
    let bucket = node
        .bucket()
        .ok_or_else(|| anyhow::anyhow!("remote has no bucket"))?;
    let role = node
        .cloud_settings()
        .instance_profile(&node.name)
        .ok_or_else(|| anyhow::anyhow!("bucket has no node role"))?;
    anyhow::ensure!(
        !state.bucket_shared(&node.name, bucket)?,
        "bucket is also recorded by another remote"
    );
    anyhow::ensure!(
        !state.role_shared(&node.name, &role)?,
        "role is also recorded by another remote"
    );
    for (kind, name) in [
        ("bucket", bucket),
        ("role", role.as_str()),
        ("instance profile", role.as_str()),
    ] {
        confirm_name(kind, name)?;
    }
    cloud.tag_bucket(bucket, &node.name).await?;
    cloud.tag_node_role(&role, &node.name).await?;
    println!(
        "Tagged bucket {bucket}, role {role}, and instance profile {role} for remote {}",
        node.name
    );
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
    cleanup_bucket_and_role(cloud, state, node, keep_bucket).await?;
    for current in nodes {
        state.remove_key(current)?;
    }
    state.remove(node)?;
    println!("Removed remote {}", node.name);
    Ok(())
}

async fn cleanup_bucket_and_role(
    cloud: &impl Cloud,
    state: &State,
    node: &RemoteNode,
    keep_bucket: bool,
) -> Result<()> {
    if let Some(bucket) = node.bucket() {
        let role = node
            .cloud_settings()
            .instance_profile(&node.name)
            .ok_or_else(|| anyhow::anyhow!("bucket has no node role"))?;
        if keep_bucket {
            println!("Kept bucket {bucket} and guarding role and instance profile {role}");
        } else {
            let shared = state.bucket_shared(&node.name, bucket)?;
            let bucket_status = if shared {
                Ownership::Unmanaged
            } else {
                cloud.bucket_ownership(bucket, &node.name).await?
            };
            if shared {
                println!("Bucket {bucket}: kept (another remote state records it)");
            } else {
                match bucket_status {
                    Ownership::Owned => {
                        let removed = cloud.delete_bucket(bucket, &node.name).await?;
                        println!(
                            "Bucket {bucket}: {}",
                            if removed { "removed" } else { "absent" }
                        );
                    }
                    Ownership::Absent => println!("Bucket {bucket}: absent"),
                    Ownership::Unmanaged => {
                        println!("Bucket {bucket}: kept (ownership tags do not match)");
                    }
                }
            }
            let role_shared = state.role_shared(&node.name, &role)?;
            if bucket_status == Ownership::Unmanaged || role_shared {
                let reason = if role_shared {
                    "another remote state records the role"
                } else {
                    "bucket is retained"
                };
                println!("Instance profile {role}: kept ({reason})");
                println!("Role {role}: kept ({reason})");
            } else {
                let (profile, role_status) = cloud.role_ownership(&role, &node.name).await?;
                if profile == Ownership::Unmanaged {
                    println!("Instance profile {role}: kept (ownership tags do not match)");
                }
                if role_status == Ownership::Unmanaged && profile != Ownership::Unmanaged {
                    println!("Instance profile {role}: kept (role is unowned)");
                }
                if role_status == Ownership::Unmanaged || profile == Ownership::Unmanaged {
                    println!(
                        "Role {role}: kept (ownership tags do not match or profile is unowned)"
                    );
                }
                if profile != Ownership::Unmanaged && role_status != Ownership::Unmanaged {
                    let (profile_removed, role_removed) =
                        cloud.delete_node_role(&role, &node.name).await?;
                    println!(
                        "Instance profile {role}: {}",
                        if profile_removed { "removed" } else { "absent" }
                    );
                    println!(
                        "Role {role}: {}",
                        if role_removed { "removed" } else { "absent" }
                    );
                }
            }
        }
    }
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
