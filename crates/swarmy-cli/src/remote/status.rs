use crate::remote_ssh as ssh;
use anyhow::Result;
use serde::Serialize;
use std::{future::Future, path::Path, time::Duration};
use swarmy_config::{RemoteNode, RemoteProfile, RemoteServices, Settings};
use swarmy_core::{ImageRecord, NodeRecord};
use tokio::time::timeout;

#[derive(Debug, Serialize)]
struct Registration {
    node_id: swarmy_core::NodeId,
    heartbeat_age_seconds: i64,
    heartbeating: bool,
    committed_memory_mib: u64,
    free_memory_mib: u64,
}

#[derive(Debug, Serialize)]
struct Status {
    name: String,
    instance_id: String,
    instance_state: String,
    sandboxes: u32,
    instance_type: Option<String>,
    tunnel: bool,
    api_token: String,
    registrations: Vec<Registration>,
    registration_error: Option<String>,
    nodes: Vec<NodeStatus>,
    images: Vec<ImageRecord>,
    services: Vec<swarmy_store::ServiceHealth>,
    image_error: Option<String>,
}

#[derive(Debug, Serialize)]
struct NodeStatus {
    name: String,
    instance_id: String,
    instance_state: String,
    instance_type: Option<String>,
    private_ip: String,
    sandboxes: u32,
}

async fn reachable(node: &RemoteNode) -> bool {
    ssh::reachable_address(node).await.is_ok()
}

/// Token status for `remote status`. Only control nodes serve the API, so only
/// they report whether the connected profile carries its token. Sandbox-only
/// nodes and laptop-service remotes never use a node API token. After
/// `remote upgrade` fills a missing node token, reconnect to refresh it here.
fn api_token_status(node: &RemoteNode, profile: Option<&RemoteProfile>) -> &'static str {
    let control = node
        .launch_settings
        .as_ref()
        .is_some_and(|settings| settings.services == RemoteServices::Node);
    if !control {
        return "not-applicable";
    }
    match profile.and_then(|profile| profile.api_token.as_deref()) {
        Some(token) if !token.is_empty() => "set",
        _ => "missing",
    }
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
        let profile = RemoteProfile::read(Path::new(&base.state_dir), &node.name).ok();
        let tunnel = match &profile {
            Some(profile) => ssh::healthy(profile).await,
            None => false,
        };
        let mut status = inspect(&node, tunnel, reachable(&node).await, || {
            inventory(&base, &node.name)
        })
        .await;
        status.api_token = api_token_status(&node, profile.as_ref()).into();
        let mut pending: Vec<_> = node.nodes.iter().collect();
        while let Some(child) = pending.pop() {
            status.nodes.push(NodeStatus {
                name: child.name.clone(),
                instance_id: child.instance_id.clone(),
                instance_type: child
                    .launch_settings
                    .as_ref()
                    .map(|settings| settings.instance_type.clone()),
                private_ip: child.private_ip.clone(),
                sandboxes: child.sandboxes,
                instance_state: instance_state(reachable(child).await),
            });
            pending.extend(&child.nodes);
        }
        statuses.push(status);
    }
    if json {
        println!("{}", serde_json::to_string(&statuses)?);
    } else {
        print_human(statuses);
    }
    Ok(())
}

fn print_human(statuses: Vec<Status>) {
    for status in statuses {
        println!(
            "{} instance={} type={} state={} sandboxes={} tunnel={} api_token={}",
            status.name,
            status.instance_id,
            status.instance_type.as_deref().unwrap_or("unknown"),
            status.instance_state,
            status.sandboxes,
            if status.tunnel { "up" } else { "down" },
            status.api_token,
        );
        for node in status.nodes {
            println!(
                "  {} instance={} type={} state={} private_ip={} sandboxes={}",
                node.name,
                node.instance_id,
                node.instance_type.as_deref().unwrap_or("unknown"),
                node.instance_state,
                node.private_ip,
                node.sandboxes
            );
        }
        for service in status.services {
            println!(
                "  service {:?} {} {}",
                service.heartbeat.role,
                service.heartbeat.instance_id,
                if service.alive { "live" } else { "stale" }
            );
        }
        for image in status.images {
            println!(
                "  image {}:{} {}",
                image.name, image.tag.0, image.manifest_id
            );
        }
        if let Some(error) = status.image_error {
            println!("  images: {error}");
        }
        for record in status.registrations {
            println!(
                "  swarmyd {} heartbeat={}s {} memory={}MiB committed={}MiB free",
                record.node_id,
                record.heartbeat_age_seconds,
                if record.heartbeating { "live" } else { "stale" },
                record.committed_memory_mib,
                record.free_memory_mib
            );
        }
        if let Some(error) = status.registration_error {
            println!("  registration: {error}");
        }
    }
}

async fn inspect<F, Fut>(node: &RemoteNode, tunnel: bool, reachable: bool, scan: F) -> Status
where
    F: FnOnce() -> Fut,
    Fut: Future<
        Output = Result<(
            Vec<NodeRecord>,
            Vec<ImageRecord>,
            Vec<u64>,
            Vec<swarmy_store::ServiceHealth>,
        )>,
    >,
{
    let mut status = Status {
        name: node.name.clone(),
        instance_id: node.instance_id.clone(),
        instance_state: instance_state(reachable),
        sandboxes: node.sandboxes,
        instance_type: node
            .launch_settings
            .as_ref()
            .map(|settings| settings.instance_type.clone()),
        nodes: Vec::new(),
        images: Vec::new(),
        services: Vec::new(),
        image_error: None,
        tunnel,
        // `run` fills the connected profile's token status after inspection.
        api_token: String::new(),
        registrations: Vec::new(),
        registration_error: None,
    };
    if !tunnel {
        status.registration_error = Some("unknown: tunnel disconnected".into());
        status.image_error.clone_from(&status.registration_error);
        return status;
    }
    match timeout(Duration::from_secs(5), scan()).await {
        Ok(Ok((records, images, committed, services))) => {
            status.images = images;
            status.services = services;
            let now = jiff::Timestamp::now().as_second();
            status.registrations = records
                .into_iter()
                .zip(committed)
                .map(|(record, committed)| {
                    let age = now.saturating_sub(record.last_heartbeat.as_second()).max(0);
                    Registration {
                        node_id: record.node_id,
                        heartbeat_age_seconds: age,
                        heartbeating: age <= 30,
                        committed_memory_mib: committed / (1024 * 1024),
                        free_memory_mib: record.capacity.memory_bytes.saturating_sub(committed)
                            / (1024 * 1024),
                    }
                })
                .collect();
            if status.registrations.is_empty() {
                status.registration_error = Some("no swarmyd registered in this stack".into());
            }
        }
        Ok(Err(error)) => {
            let error = format!("store unavailable: {error}");
            status.registration_error = Some(error.clone());
            status.image_error = Some(error);
        }
        Err(_) => {
            status.registration_error = Some("store check timed out".into());
            status.image_error.clone_from(&status.registration_error);
        }
    }
    status
}

async fn inventory(
    base: &Settings,
    name: &str,
) -> Result<(
    Vec<NodeRecord>,
    Vec<ImageRecord>,
    Vec<u64>,
    Vec<swarmy_store::ServiceHealth>,
)> {
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
            break;
        }
        cursor = next;
    }
    let mut committed = Vec::with_capacity(records.len());
    for record in &records {
        committed.push(store.committed_memory(record.node_id).await?);
    }
    let mut images: Vec<ImageRecord> = Vec::new();
    loop {
        let after = images.last().map(|image| (image.name.as_str(), &image.tag));
        let page = store.list_images(after, MAX_SCAN_LIMIT).await?;
        if page.is_empty() {
            return Ok((records, images, committed, store.list_services().await?));
        }
        images.extend(page);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn control_nodes_report_profile_token_presence_and_others_opt_out() {
        let mut node: RemoteNode = serde_json::from_str(r#"{"name":"test","region":"local","instance_id":"i-test","public_ip":"127.0.0.1","private_ip":"127.0.0.1","key_path":"key","created_at":"now"}"#).unwrap();
        // Without saved launch settings the node predates service roles.
        assert_eq!(api_token_status(&node, None), "not-applicable");
        node.launch_settings = Some(swarmy_config::RemoteSettings {
            services: swarmy_config::RemoteServices::Laptop,
            ..Default::default()
        });
        assert_eq!(api_token_status(&node, None), "not-applicable");
        node.launch_settings = Some(swarmy_config::RemoteSettings {
            services: RemoteServices::Node,
            ..Default::default()
        });
        assert_eq!(api_token_status(&node, None), "missing");
        let mut profile = RemoteProfile {
            name: "test".into(),
            socket_path: std::path::PathBuf::from("socket"),
            pid: 0,
            ports: swarmy_config::RemotePorts::default(),
            remote_ports: swarmy_config::RemotePorts::default(),
            fdb_cluster_file: std::path::PathBuf::from("cluster"),
            nats_url: String::new(),
            s3_endpoint: String::new(),
            api_url: None,
            api_token: None,
            s3_bucket: None,
            s3_region: None,
            default_image: None,
        };
        assert_eq!(api_token_status(&node, Some(&profile)), "missing");
        profile.api_token = Some(String::new());
        assert_eq!(api_token_status(&node, Some(&profile)), "missing");
        profile.api_token = Some("provisioned-token".into());
        assert_eq!(api_token_status(&node, Some(&profile)), "set");
    }
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
        let mut node = node;
        node.launch_settings = Some(swarmy_config::RemoteSettings {
            instance_type: "m6i.large".into(),
            disk_gb: 40,
            ..Default::default()
        });
        let status = inspect(&node, true, true, || async {
            Ok((
                vec![record(1), record(90)],
                vec![ImageRecord {
                    name: "base-ubuntu".into(),
                    tag: swarmy_core::ImageTag("test".into()),
                    manifest_id: swarmy_core::ManifestId::from_ulid(ulid::Ulid::generate()),
                }],
                vec![0, 0],
                vec![],
            ))
        })
        .await;
        assert_eq!(status.sandboxes, 64);
        assert_eq!(serde_json::to_value(&status).unwrap()["sandboxes"], 64);
        let child = NodeStatus {
            name: "test-2".into(),
            instance_id: "i-child".into(),
            instance_state: "running".into(),
            instance_type: Some("m6id.4xlarge".into()),
            private_ip: "10.0.0.2".into(),
            sandboxes: 4,
        };
        assert_eq!(serde_json::to_value(&child).unwrap()["sandboxes"], 4);
        assert_eq!(status.images[0].name, "base-ubuntu");
        assert_eq!(status.images[0].tag.0, "test");
        assert!(status.image_error.is_none());
        assert_eq!(status.instance_type.as_deref(), Some("m6i.large"));
        assert!(status.registrations[0].heartbeating);
        assert!(!status.registrations[1].heartbeating);
        let absent = inspect(&node, true, true, || async {
            Ok((vec![], vec![], vec![], vec![]))
        })
        .await;
        assert!(absent.images.is_empty());
        assert!(absent.image_error.is_none());
        assert!(absent.registration_error.unwrap().contains("no swarmyd"));
        let down = inspect(&node, false, false, || async {
            panic!("disconnected status must not scan")
        })
        .await;
        assert!(down.image_error.unwrap().contains("disconnected"));
        assert!(down.instance_state.starts_with("unknown"));
        let failed = inspect(&node, true, true, || async {
            anyhow::bail!("fake failure")
        })
        .await;
        assert!(failed.image_error.unwrap().contains("fake failure"));
        assert!(failed.registration_error.unwrap().contains("fake failure"));
    }
}
