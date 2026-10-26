use crate::Result;
use swarmy_config::RemoteNode;

use super::{Cloud, Host, ObjectBucket, Ownership, state::State};
use crate::BucketRemoval;

/// Owned resources `remote down` would delete. The CLI prints the
/// confirmation wording and prompts; the library only reports the plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletionPlan {
    pub bucket: String,
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
    if state.bucket_shared(&node.name, &bucket.spec.bucket)? {
        return Ok(None);
    }
    Ok(
        (cloud.bucket_ownership(&bucket).await? == Ownership::Owned).then_some(DeletionPlan {
            bucket: bucket.spec.bucket,
        }),
    )
}

fn object_bucket(node: &RemoteNode) -> Option<ObjectBucket> {
    let spec = node.bucket_spec()?;
    if !spec.needs_static_keys() {
        return None;
    }
    Some(ObjectBucket::from_spec(
        &node.name,
        &spec,
        &node.cloud_settings().region,
    ))
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
        cleanup_bucket(cloud, state, node, keep_bucket).await?;
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

async fn cleanup_bucket(
    cloud: &impl Cloud,
    state: &State,
    node: &RemoteNode,
    keep_bucket: bool,
) -> Result<()> {
    if let Some(spec) = node.bucket_spec()
        && !spec.needs_static_keys()
    {
        cloud_err!(
            "Bucket {}: left in place; it uses an AWS instance role, which swarmy no longer manages",
            spec.bucket
        );
    }
    if let Some(bucket) = object_bucket(node) {
        let name = bucket.spec.bucket.clone();
        if keep_bucket {
            cloud_out!("Kept bucket {name}");
        } else if state.bucket_shared(&node.name, &name)? {
            cloud_out!("Bucket {name}: kept (another remote state records it)");
        } else {
            match cloud.bucket_ownership(&bucket).await? {
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
    }
    Ok(())
}
