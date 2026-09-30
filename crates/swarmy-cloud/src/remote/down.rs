use std::time::Duration;

use crate::Result;
use swarmy_config::RemoteNode;

use super::{Cloud, Host, ObjectBucket, Ownership, key_name, state::State};
use crate::BucketRemoval;

#[derive(Default)]
struct Report {
    failures: Vec<String>,
    live: Vec<String>,
}

impl Report {
    fn failed(&mut self, resource: &str, permission: &str, error: &crate::Error) {
        let detail = match error {
            crate::Error::MissingPermission { operation, source } => {
                format!("{operation}: {}", swarmy_core::error_chain(source.as_ref()))
            }
            _ => swarmy_core::error_chain(error),
        };
        self.failures.push(format!(
            "Skipped {resource}: requires {permission}; {detail}"
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
pub(super) async fn plan(
    cloud: &impl Cloud,
    state: &State,
    node: &RemoteNode,
) -> Result<Option<DeletionPlan>> {
    let Some(bucket) = object_bucket(node) else {
        return Ok(None);
    };
    let bucket_status = cloud.bucket_ownership(&bucket).await?;
    let bucket_owned =
        !state.bucket_shared(&node.name, &bucket.spec.bucket)? && bucket_status == Ownership::Owned;
    // Static-key buckets have no IAM role or instance profile to delete.
    let Some(role) = node.cloud_settings().instance_profile(&node.name) else {
        return Ok(bucket_owned.then(|| DeletionPlan {
            bucket: Some(bucket.spec.bucket.clone()),
            profile: None,
            role: None,
        }));
    };
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
        bucket: bucket_owned.then(|| bucket.spec.bucket.clone()),
        profile: profile_owned.then(|| role.clone()),
        role: role_owned.then(|| role.clone()),
    }))
}

/// Provider-neutral bucket for teardown, or `None` when the remote uses the
/// local `SeaweedFS` object store instead of an object bucket.
fn object_bucket(node: &RemoteNode) -> Option<ObjectBucket> {
    let spec = node.bucket_spec()?;
    let settings = node.cloud_settings();
    Some(ObjectBucket::from_spec(
        &node.name,
        &spec,
        &settings.region,
        settings.instance_profile(&node.name),
    ))
}

/// Adoption targets `remote tag` would adopt. The CLI prints the wording
/// and prompts for each exact name; the library only reports the plan.
/// Static-key buckets adopt only the bucket; there is no role or profile.
pub(super) fn adoption_targets(state: &State, node: &RemoteNode) -> Result<Vec<(String, String)>> {
    let bucket = node
        .bucket()
        .ok_or_else(|| crate::Error::other("remote has no bucket"))?;
    crate::Error::ensure(
        !state.bucket_shared(&node.name, bucket)?,
        "bucket is also recorded by another remote",
    )?;
    let Some(role) = node.cloud_settings().instance_profile(&node.name) else {
        return Ok([("bucket", bucket)]
            .into_iter()
            .map(|(kind, name)| (kind.to_owned(), name.to_owned()))
            .collect());
    };
    crate::Error::ensure(
        !state.role_shared(&node.name, &role)?,
        "role is also recorded by another remote",
    )?;
    Ok([
        ("bucket", bucket),
        ("role", role.as_str()),
        ("instance profile", role.as_str()),
    ]
    .into_iter()
    .map(|(kind, name)| (kind.to_owned(), name.to_owned()))
    .collect())
}

/// Adopt the bucket, role, and instance profile after the CLI confirmed
/// every exact resource name. Confirmation lives in the CLI; this applies.
/// Static-key buckets adopt only the bucket.
pub(super) async fn apply_tag(cloud: &impl Cloud, node: &RemoteNode) -> Result<()> {
    let Some(bucket) = object_bucket(node) else {
        return Err(crate::Error::other("remote has no bucket"));
    };
    cloud.tag_bucket(&bucket).await?;
    let Some(role) = node.cloud_settings().instance_profile(&node.name) else {
        cloud_out!(
            "Tagged bucket {} for remote {}",
            bucket.spec.bucket,
            node.name
        );
        return Ok(());
    };
    cloud.tag_node_role(&role, &node.name).await?;
    cloud_out!(
        "Tagged bucket {}, role {role}, and instance profile {role} for remote {}",
        bucket.spec.bucket,
        node.name
    );
    Ok(())
}

pub(super) async fn run(
    cloud: &impl Cloud,
    state: &State,
    node: &RemoteNode,
    delay: Duration,
    keep_bucket: bool,
) -> Result<()> {
    let nodes = collect(node);
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
    let live = report.live.join(", ");
    crate::Error::ensure(
        report.live.is_empty(),
        format!("instances {live} may still exist; local state retained for retry"),
    )?;
    finish(cloud, state, node, nodes, keep_bucket).await
}

/// Tear down an existing-host remote: stop swarmy services and remove
/// swarmy files and state on every provisioned host, delete the owned
/// bucket scope, and drop local state. The machines are operator-owned:
/// they stay running and no cloud machine call happens. A failed host
/// never blocks the others, and the teardown SSH wait is short (see
/// [`Host`]) so a cancelled server cannot hold teardown hostage. When any
/// host fails, only its records are kept and the bucket is left alone:
/// hosts that were torn down are pruned with their key files, and the
/// command exits non-zero so re-running `down` retries exactly the failed
/// hosts and deletes the bucket once every host is torn down.
pub(super) async fn run_existing(
    cloud: &impl Cloud,
    host: &impl Host,
    state: &State,
    node: &RemoteNode,
    keep_bucket: bool,
) -> Result<()> {
    let nodes = collect(node);
    cloud_out!(
        "Remote {} uses existing hosts: the machines stay running; swarmy services, files, and state are removed from the hosts",
        node.name
    );
    let mut failed: Vec<(String, String)> = Vec::new();
    for current in nodes.iter().rev() {
        if !current.launch_attempted {
            cloud_out!("Nothing was provisioned for {}", current.name);
            continue;
        }
        // The reason travels with the name: a reachable host with a missing
        // checkout fails differently from a host that never answers SSH,
        // and the report must say which.
        if let Err(error) = host.decommission(current).await {
            failed.push((current.name.clone(), swarmy_core::error_chain(&error)));
        }
    }
    if failed.is_empty() {
        cleanup_bucket_and_role(cloud, state, node, keep_bucket).await?;
        for current in nodes {
            state.remove_key(current)?;
        }
        state.remove(node)?;
        cloud_out!("Removed remote {}", node.name);
        return Ok(());
    }
    let failed_names: std::collections::HashSet<&str> =
        failed.iter().map(|(name, _)| name.as_str()).collect();
    let mut kept = node.clone();
    retain_failed(&mut kept, &failed_names, state)?;
    state.save(&kept)?;
    let mut detail: Vec<String> = failed
        .iter()
        .map(|(name, reason)| format!("host {name}: {reason}"))
        .collect();
    detail.sort();
    Err(crate::Error::other(format!(
        "remote down {} incomplete for {} host(s) ({}); fix or remove them, then re-run swarmy remote down {}",
        node.name,
        detail.len(),
        detail.join("; "),
        node.name,
    )))
}

/// Drop torn-down hosts from the tree and delete their key files, keeping
/// exactly the failed records for a retry. A node stays when it failed
/// itself or still contains a failed descendant; everything else is pruned.
fn retain_failed(
    node: &mut RemoteNode,
    failed: &std::collections::HashSet<&str>,
    state: &State,
) -> Result<()> {
    let mut kept = Vec::new();
    for mut child in std::mem::take(&mut node.nodes) {
        retain_failed(&mut child, failed, state)?;
        if failed.contains(child.name.as_str()) || !child.nodes.is_empty() {
            kept.push(child);
        } else {
            state.remove_key(&child)?;
        }
    }
    node.nodes = kept;
    Ok(())
}

/// Every node in the remote, primary first.
fn collect(node: &RemoteNode) -> Vec<&RemoteNode> {
    let mut pending = vec![node];
    let mut nodes = Vec::new();
    while let Some(current) = pending.pop() {
        pending.extend(&current.nodes);
        nodes.push(current);
    }
    nodes
}

/// Delete the owned bucket scope, then drop every key file and the record.
async fn finish(
    cloud: &impl Cloud,
    state: &State,
    node: &RemoteNode,
    nodes: Vec<&RemoteNode>,
    keep_bucket: bool,
) -> Result<()> {
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
    if let Some(bucket) = object_bucket(node) {
        let name = bucket.spec.bucket.clone();
        if keep_bucket {
            match node.cloud_settings().instance_profile(&node.name) {
                Some(role) => {
                    cloud_out!("Kept bucket {name} and guarding role and instance profile {role}");
                }
                None => cloud_out!("Kept bucket {name}"),
            }
        } else {
            let shared = state.bucket_shared(&node.name, &name)?;
            let bucket_status = if shared {
                Ownership::Unmanaged
            } else {
                cloud.bucket_ownership(&bucket).await?
            };
            if shared {
                cloud_out!("Bucket {name}: kept (another remote state records it)");
            } else {
                match bucket_status {
                    Ownership::Owned => match cloud.delete_bucket(&bucket).await? {
                        BucketRemoval::Removed => cloud_out!("Bucket {name}: removed"),
                        BucketRemoval::Absent => cloud_out!("Bucket {name}: absent"),
                        BucketRemoval::Retained => cloud_out!(
                            "Bucket {name}: kept (bucket retains content outside the remote's prefix)"
                        ),
                    },
                    Ownership::Absent => cloud_out!("Bucket {name}: absent"),
                    Ownership::Unmanaged => {
                        cloud_out!("Bucket {name}: kept (ownership tags do not match)");
                    }
                }
            }
            // Static-key buckets have no IAM role or instance profile.
            let Some(role) = node.cloud_settings().instance_profile(&node.name) else {
                return Ok(());
            };
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
    Err(crate::Error::other(format!(
        "timed out waiting for {id} to terminate"
    )))
}
