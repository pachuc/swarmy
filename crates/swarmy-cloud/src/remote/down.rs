use std::{io::IsTerminal, time::Duration};

use crate::Result;
use swarmy_config::RemoteNode;

use super::{Cloud, Ownership, key_name, state::State};

/// Explain a denied teardown permission without wrapping the typed error.
/// The CLI prints the same advice when it reports the failure.
pub(crate) fn actionable_error(error: crate::Error) -> crate::Error {
    if error.permission().is_some() {
        cloud_err!(
            "AWS denied the named permission; nothing was deleted by this operation. Grant it and retry; local remote state is retained"
        );
    }
    error
}

#[derive(Default)]
struct Report {
    failures: Vec<String>,
    live: Vec<String>,
}

impl Report {
    fn failed(&mut self, resource: &str, permission: &str, error: &crate::Error) {
        self.failures.push(format!(
            "Skipped {resource}: requires {permission}; {error:#}"
        ));
    }

    fn print(&self) {
        for failure in &self.failures {
            cloud_err!("{failure}");
        }
    }
}

fn permission<'a>(error: &crate::Error, default: &'a str, alternate: &'a str) -> &'a str {
    if error.permission() == Some(alternate) {
        alternate
    } else {
        default
    }
}

/// Owned resources `remote down` would delete. The CLI prints the
/// confirmation wording and prompts; the library only reports the plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletionPlan {
    pub bucket: Option<String>,
    pub profile: Option<String>,
    pub role: Option<String>,
}

/// What `remote down` would delete, or `None` when nothing is owned.
/// Query only: prints nothing and prompts for nothing.
pub async fn plan(
    cloud: &impl Cloud,
    state: &State,
    node: &RemoteNode,
) -> Result<Option<DeletionPlan>> {
    let Some(bucket) = node.bucket() else {
        return Ok(None);
    };
    let role = node
        .cloud_settings()
        .instance_profile(&node.name)
        .ok_or_else(|| crate::Error::other("bucket has no node role"))?;
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
        return Ok(None);
    }
    Ok(Some(DeletionPlan {
        bucket: bucket_owned.then(|| bucket.to_owned()),
        profile: profile_owned.then(|| role.clone()),
        role: role_owned.then(|| role.clone()),
    }))
}

/// Adoption is deliberately interactive and requires typing every exact resource name.
pub async fn tag(cloud: &impl Cloud, state: &State, node: &RemoteNode) -> Result<()> {
    tag_with_confirmation(cloud, state, node, |kind, name| {
        if !std::io::stdin().is_terminal() {
            return Err(crate::Error::other("remote tag requires a terminal"));
        }
        let answer = crate::prompt(&format!(
            "Type the exact {kind} name {name} to adopt it for remote {}: ",
            node.name
        ))?;
        if answer.trim() != name {
            return Err(crate::Error::other("remote tag cancelled"));
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
        .ok_or_else(|| crate::Error::other("remote has no bucket"))?;
    let role = node
        .cloud_settings()
        .instance_profile(&node.name)
        .ok_or_else(|| crate::Error::other("bucket has no node role"))?;
    if state.bucket_shared(&node.name, bucket)? {
        return Err(crate::Error::other(
            "bucket is also recorded by another remote",
        ));
    }
    if state.role_shared(&node.name, &role)? {
        return Err(crate::Error::other(
            "role is also recorded by another remote",
        ));
    }
    for (kind, name) in [
        ("bucket", bucket),
        ("role", role.as_str()),
        ("instance profile", role.as_str()),
    ] {
        confirm_name(kind, name)?;
    }
    cloud.tag_bucket(bucket, &node.name).await?;
    cloud.tag_node_role(&role, &node.name).await?;
    cloud_out!(
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
            cloud_out!("Nothing was launched for {}", current.name);
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
            cloud_out!("Key pair {key}: removed or absent");
        }
    }
    report.print();
    if !report.live.is_empty() {
        return Err(crate::Error::other(format!(
            "instances {} may still exist; local state retained for retry",
            report.live.join(", ")
        )));
    }
    cleanup_bucket_and_role(cloud, state, node, keep_bucket).await?;
    for current in nodes {
        state.remove_key(current)?;
    }
    state.remove(node)?;
    cloud_out!("Removed remote {}", node.name);
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
            .ok_or_else(|| crate::Error::other("bucket has no node role"))?;
        if keep_bucket {
            cloud_out!("Kept bucket {bucket} and guarding role and instance profile {role}");
        } else {
            let shared = state.bucket_shared(&node.name, bucket)?;
            let bucket_status = if shared {
                Ownership::Unmanaged
            } else {
                cloud.bucket_ownership(bucket, &node.name).await?
            };
            if shared {
                cloud_out!("Bucket {bucket}: kept (another remote state records it)");
            } else {
                match bucket_status {
                    Ownership::Owned => {
                        let removed = cloud.delete_bucket(bucket, &node.name).await?;
                        cloud_out!(
                            "Bucket {bucket}: {}",
                            if removed { "removed" } else { "absent" }
                        );
                    }
                    Ownership::Absent => cloud_out!("Bucket {bucket}: absent"),
                    Ownership::Unmanaged => {
                        cloud_out!("Bucket {bucket}: kept (ownership tags do not match)");
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
                cloud_out!("Instance profile {role}: kept ({reason})");
                cloud_out!("Role {role}: kept ({reason})");
            } else {
                let (profile, role_status) = cloud.role_ownership(&role, &node.name).await?;
                if profile == Ownership::Unmanaged {
                    cloud_out!("Instance profile {role}: kept (ownership tags do not match)");
                }
                if role_status == Ownership::Unmanaged && profile != Ownership::Unmanaged {
                    cloud_out!("Instance profile {role}: kept (role is unowned)");
                }
                if role_status == Ownership::Unmanaged || profile == Ownership::Unmanaged {
                    cloud_out!(
                        "Role {role}: kept (ownership tags do not match or profile is unowned)"
                    );
                }
                if profile != Ownership::Unmanaged && role_status != Ownership::Unmanaged {
                    let (profile_removed, role_removed) =
                        cloud.delete_node_role(&role, &node.name).await?;
                    cloud_out!(
                        "Instance profile {role}: {}",
                        if profile_removed { "removed" } else { "absent" }
                    );
                    cloud_out!(
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
        cloud_out!("No launched instance found for {}", node.name);
        return;
    };
    cloud_out!("Terminating {id}");
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
        Ok(()) => cloud_out!("Confirmed {id} is terminated or absent"),
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
    return Err(crate::Error::other(format!("timed out waiting for {id} to terminate")))
}
