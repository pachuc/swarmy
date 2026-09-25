use super::ssh;
use anyhow::Result;
use serde::Serialize;
use std::{future::Future, path::Path, time::Duration};
use swarmy_config::{RemoteNode, RemoteProfile, Settings};
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
    registrations: Vec<Registration>,
    registration_error: Option<String>,
    nodes: Vec<NodeStatus>,
    images: Vec<ImageRecord>,
    services: Vec<ServiceSummary>,
    image_error: Option<String>,
}

#[derive(Debug, Serialize)]
struct ServiceSummary {
    role: String,
    instance_id: String,
    alive: bool,
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
            inventory(&base, &node.name)
        })
        .await;
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
            "{} instance={} type={} state={} sandboxes={} tunnel={}",
            status.name,
            status.instance_id,
            status.instance_type.as_deref().unwrap_or("unknown"),
            status.instance_state,
            status.sandboxes,
            if status.tunnel { "up" } else { "down" }
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
                "  service {} {} {}",
                service.role,
                service.instance_id,
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
            Vec<(NodeRecord, u64)>,
            Vec<ImageRecord>,
            Vec<ServiceSummary>,
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
        registrations: Vec::new(),
        registration_error: None,
    };
    if !tunnel {
        status.registration_error = Some("unknown: tunnel disconnected".into());
        status.image_error.clone_from(&status.registration_error);
        return status;
    }
    match timeout(Duration::from_secs(10), scan()).await {
        Ok(Ok((records, images, services))) => {
            status.images = images;
            status.services = services;
            let now = jiff::Timestamp::now().as_second();
            status.registrations = records
                .into_iter()
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
            let error = format!("API unavailable: {error:#}");
            status.registration_error = Some(error.clone());
            status.image_error = Some(error);
        }
        Err(_) => {
            status.registration_error = Some("API check timed out".into());
            status.image_error.clone_from(&status.registration_error);
        }
    }
    status
}

/// Read node registrations, images, and services through the control-plane
/// API reached over the remote tunnel. The client opens no database.
async fn inventory(
    base: &Settings,
    name: &str,
) -> Result<(
    Vec<(NodeRecord, u64)>,
    Vec<ImageRecord>,
    Vec<ServiceSummary>,
)> {
    let mut settings = base.clone();
    let profile = RemoteProfile::read(Path::new(&base.state_dir), name)?;
    profile.apply(&mut settings);
    let endpoint = settings
        .api
        .url
        .clone()
        .unwrap_or_else(|| format!("http://{}", settings.api.listen));
    anyhow::ensure!(
        !settings.api.token.is_empty(),
        "no [api] token configured for remote {name}"
    );
    let client = swarmy_client::Client::new(&endpoint, settings.api.token.clone())?;
    let snapshot = tokio::time::timeout(Duration::from_secs(10), client.doctor())
        .await
        .map_err(|_| anyhow::anyhow!("API at {endpoint}: request timed out"))?
        .map_err(|error| anyhow::anyhow!("API at {endpoint}: {error}"))?;
    let mut images = Vec::new();
    let mut after = None;
    loop {
        let page: Vec<swarmy_api_types::Image> = tokio::time::timeout(
            Duration::from_secs(10),
            client.images(after.as_deref(), 256),
        )
        .await
        .map_err(|_| anyhow::anyhow!("API at {endpoint}: request timed out"))?
        .map_err(|error| anyhow::anyhow!("API at {endpoint}: {error}"))?;
        if page.is_empty() {
            break;
        }
        for image in page {
            let manifest = image
                .id
                .parse::<ulid::Ulid>()
                .map(swarmy_core::ManifestId::from_ulid)
                .map_err(|_| anyhow::anyhow!("invalid manifest id"))?;
            after = Some(format!("{}:{}", image.name, image.tag));
            images.push(ImageRecord {
                name: image.name,
                tag: swarmy_core::ImageTag(image.tag),
                manifest_id: manifest,
            });
        }
    }
    let mut records = Vec::new();
    for node in snapshot.nodes {
        let node_id = node
            .node_id
            .parse::<ulid::Ulid>()
            .map(swarmy_core::NodeId::from_ulid)
            .map_err(|_| anyhow::anyhow!("invalid node id"))?;
        let last_heartbeat: jiff::Timestamp = node
            .last_heartbeat
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid heartbeat"))?;
        records.push((
            NodeRecord {
                node_id,
                roles: node
                    .roles
                    .into_iter()
                    .map(|role| match role {
                        swarmy_api_types::NodeRole::Sandbox => swarmy_core::NodeRole::Sandbox,
                        swarmy_api_types::NodeRole::Volume => swarmy_core::NodeRole::Volume,
                    })
                    .collect(),
                capacity: swarmy_core::NodeCapacity {
                    cpu_millis: node.capacity.cpu_millis,
                    memory_bytes: node.capacity.memory_bytes,
                    disk_bytes: node.capacity.disk_bytes,
                    sandboxes: node.capacity.sandboxes,
                },
                last_heartbeat,
                cached_images: Vec::new(),
            },
            node.committed_memory_bytes,
        ));
    }
    // Include stale registrations so a stopped daemon is distinguishable from an absent one.
    let services = snapshot
        .services
        .into_iter()
        .map(|service| ServiceSummary {
            role: service.role,
            instance_id: service.instance_id,
            alive: service.alive,
        })
        .collect();
    Ok((records, images, services))
}
