use crate::remote_ssh as ssh;
use anyhow::Result;
use serde::Serialize;
use std::{future::Future, path::Path, time::Duration};
use swarmy_config::{RemoteNode, RemoteProfile, Settings};
use swarmy_core::NodeRecord;
use tokio::time::timeout;

#[derive(Debug, Serialize)]
struct Registration {
    node_id: swarmy_core::NodeId,
    heartbeat_age_seconds: i64,
    heartbeating: bool,
}

#[derive(Debug, Serialize)]
struct Status {
    name: String,
    instance_id: String,
    instance_state: String,
    tunnel: bool,
    registrations: Vec<Registration>,
    registration_error: Option<String>,
    nodes: Vec<NodeStatus>,
}

#[derive(Debug, Serialize)]
struct NodeStatus {
    name: String,
    instance_id: String,
    instance_state: String,
    private_ip: String,
}

async fn reachable(node: &RemoteNode) -> bool {
    ssh::reachable_address(node).await.is_ok()
}

fn instance_state(reachable: bool) -> String {
    // SSH cannot distinguish a stopped instance from a network failure.
    if reachable {
        "running (SSH reachable)"
    } else {
        "unknown (SSH unreachable)"
    }
    .into()
}

pub async fn run(json: bool) -> Result<()> {
    let base = Settings::load_base()?.settings;
    let directory = Path::new(&base.state_dir).join("remote");
    let mut nodes = Vec::new();
    if directory.exists() {
        for entry in std::fs::read_dir(directory)? {
            let path = entry?.path();
            if path.extension().is_some_and(|ext| ext == "json")
                && !path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .ends_with(".profile.json")
            {
                let node: RemoteNode = serde_json::from_slice(&std::fs::read(path)?)?;
                nodes.push(node);
            }
        }
    }
    nodes.sort_by(|a, b| a.name.cmp(&b.name));
    let mut statuses = Vec::new();
    for node in nodes {
        let tunnel = match RemoteProfile::read(Path::new(&base.state_dir), &node.name) {
            Ok(profile) => ssh::healthy(&profile).await,
            Err(_) => false,
        };
        let mut status = inspect(&node, tunnel, reachable(&node).await, || {
            registrations(&base, &node.name)
        })
        .await;
        let mut pending: Vec<_> = node.nodes.iter().collect();
        while let Some(child) = pending.pop() {
            status.nodes.push(NodeStatus {
                name: child.name.clone(),
                instance_id: child.instance_id.clone(),
                private_ip: child.private_ip.clone(),
                instance_state: instance_state(reachable(child).await),
            });
            pending.extend(&child.nodes);
        }
        statuses.push(status);
    }
    if json {
        println!("{}", serde_json::to_string(&statuses)?);
    } else {
        for status in statuses {
            println!(
                "{} instance={} state={} tunnel={}",
                status.name,
                status.instance_id,
                status.instance_state,
                if status.tunnel { "up" } else { "down" }
            );
            for node in status.nodes {
                println!(
                    "  {} instance={} state={} private_ip={}",
                    node.name, node.instance_id, node.instance_state, node.private_ip
                );
            }
            for record in status.registrations {
                println!(
                    "  swarmyd {} heartbeat={}s {}",
                    record.node_id,
                    record.heartbeat_age_seconds,
                    if record.heartbeating { "live" } else { "stale" }
                );
            }
            if let Some(error) = status.registration_error {
                println!("  registration: {error}");
            }
        }
    }
    Ok(())
}

async fn inspect<F, Fut>(node: &RemoteNode, tunnel: bool, reachable: bool, scan: F) -> Status
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<Vec<NodeRecord>>>,
{
    let mut status = Status {
        name: node.name.clone(),
        instance_id: node.instance_id.clone(),
        instance_state: instance_state(reachable),
        nodes: Vec::new(),
        tunnel,
        registrations: Vec::new(),
        registration_error: None,
    };
    if !tunnel {
        status.registration_error = Some("unknown: tunnel disconnected".into());
        return status;
    }
    match timeout(Duration::from_secs(5), scan()).await {
        Ok(Ok(records)) => {
            let now = jiff::Timestamp::now().as_second();
            status.registrations = records
                .into_iter()
                .map(|record| {
                    let age = now.saturating_sub(record.last_heartbeat.as_second()).max(0);
                    Registration {
                        node_id: record.node_id,
                        heartbeat_age_seconds: age,
                        heartbeating: age <= 30,
                    }
                })
                .collect();
            if status.registrations.is_empty() {
                status.registration_error = Some("no swarmyd registered in this stack".into());
            }
        }
        Ok(Err(error)) => status.registration_error = Some(format!("store unavailable: {error}")),
        Err(_) => status.registration_error = Some("store check timed out".into()),
    }
    status
}

async fn registrations(base: &Settings, name: &str) -> Result<Vec<NodeRecord>> {
    use std::sync::Arc;
    use swarmy_store::{MAX_SCAN_LIMIT, Store, blob::ObjectBlobStore};
    let mut settings = base.clone();
    let profile = RemoteProfile::read(Path::new(&base.state_dir), name)?;
    profile.validate_fdb_port()?;
    profile.apply(&mut settings);
    let blobs = Arc::new(ObjectBlobStore::new(settings.object_store()?));
    let directory: Vec<_> = settings
        .store_directory
        .split('/')
        .map(str::to_owned)
        .collect();
    let store = Store::open(Some(&settings.fdb_cluster_file), Some(&directory), blobs).await?;
    let mut records = Vec::new();
    let mut cursor = None;
    loop {
        // Include stale registrations so a stopped daemon is distinguishable from an absent one.
        let (page, next) = store
            .scan_live_nodes(cursor, jiff::Timestamp::MIN, MAX_SCAN_LIMIT)
            .await?;
        records.extend(page);
        if next.is_none() {
            return Ok(records);
        }
        cursor = next;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn fake_state_and_store_report_live_stale_absent_and_unavailable() {
        let node: RemoteNode = serde_json::from_str(r#"{"name":"test","region":"local","instance_id":"i-test","public_ip":"127.0.0.1","private_ip":"127.0.0.1","key_path":"key","created_at":"now"}"#).unwrap();
        let record = |seconds| NodeRecord {
            node_id: swarmy_core::NodeId::from_ulid(ulid::Ulid::generate()),
            roles: vec![],
            capacity: Settings::default().node_capacity,
            last_heartbeat: jiff::Timestamp::from_second(
                jiff::Timestamp::now().as_second() - seconds,
            )
            .unwrap(),
            cached_images: vec![],
        };
        let status = inspect(&node, true, true, || async {
            Ok(vec![record(1), record(90)])
        })
        .await;
        assert!(status.registrations[0].heartbeating);
        assert!(!status.registrations[1].heartbeating);
        let absent = inspect(&node, true, true, || async { Ok(vec![]) }).await;
        assert!(absent.registration_error.unwrap().contains("no swarmyd"));
        let down = inspect(&node, false, false, || async {
            panic!("disconnected status must not scan")
        })
        .await;
        assert!(down.instance_state.starts_with("unknown"));
        let failed = inspect(&node, true, true, || async {
            anyhow::bail!("fake failure")
        })
        .await;
        assert!(failed.registration_error.unwrap().contains("fake failure"));
    }
}
