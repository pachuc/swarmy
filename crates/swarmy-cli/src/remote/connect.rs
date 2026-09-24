use super::{
    ssh,
    state::{self, State},
};
use anyhow::{Context, Result, bail, ensure};
use std::{
    net::TcpListener,
    path::Path,
    process::Stdio,
    time::{Duration, Instant},
};
use swarmy_config::{RemotePorts, RemoteProfile, remote_path};
use tokio::{
    process::Child,
    time::{sleep, timeout},
};

struct StartingTunnel {
    child: Child,
    published: bool,
}

impl Drop for StartingTunnel {
    fn drop(&mut self) {
        if !self.published {
            let _ = self.child.start_kill();
        }
    }
}

pub async fn run(state_dir: &Path, state: &State, name: &str, json: bool) -> Result<()> {
    let started = Instant::now();
    let _lock = state.lock()?;
    let node = state.require(name)?;
    let path = remote_path(state_dir, name, "profile.json")?;
    if path.exists() {
        let profile = RemoteProfile::read(state_dir, name)?;
        if ssh::control(&profile, "check").await?.status.success() {
            print(
                &profile,
                json,
                &Timing::new(started, Duration::ZERO, Duration::ZERO, true),
            )?;
            return Ok(());
        }
        cleanup(&profile)?;
        std::fs::remove_file(&path)?;
    }
    let probing = Instant::now();
    let address = ssh::reachable_address(&node).await?;
    let probe_elapsed = probing.elapsed();
    let startup = Instant::now();
    let (reservations, ports) = reserve_ports(&node)?;
    // A private, short directory avoids the Unix socket path length limit.
    let socket_dir = tempfile::Builder::new()
        .prefix("swarmy-ssh-")
        .tempdir_in("/tmp")?;
    let profile = new_profile(state_dir, &node, ports, socket_dir.path().join("control"))?;
    let (mut command, log_path) = tunnel_command(&node, &profile, state_dir, &address)?;
    command.kill_on_drop(false);
    drop(reservations);
    let mut tunnel = StartingTunnel {
        child: command.spawn().context("start SSH tunnel")?,
        published: false,
    };
    let mut profile = profile;
    profile.pid = tunnel
        .child
        .id()
        .context("SSH exited before recording pid")?;
    let result = timeout(Duration::from_secs(15), async {
        loop {
            if let Some(exit) = tunnel.child.try_wait()? {
                bail!("SSH exited ({exit}); inspect {}", log_path.display());
            }
            if ssh::healthy(&profile).await {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
        // Use the forwarding connection for this session too. OpenSSH enables
        // TCP_NODELAY on its server transport when a session is opened; a bare
        // -N connection otherwise adds delayed-ACK stalls to small store replies.
        let output = ssh::command(&node)?
            .arg("-S")
            .arg(&profile.socket_path)
            .arg(&address)
            .arg(if node.launch_settings.is_some() {
                "cat swarmy/.dev/fdb.cluster"
            } else {
                "true"
            })
            .output()
            .await?;
        ensure!(
            output.status.success(),
            "initialize SSH forwarding session failed"
        );
        let cluster = if node.launch_settings.is_some() {
            rewrite_address(&String::from_utf8(output.stdout)?, ports.fdb)?
        } else {
            // Old single-node remotes used this fixed cluster identity and loopback listener.
            format!("dev:dev@127.0.0.1:{}\n", ports.fdb)
        };
        state::write(&profile.fdb_cluster_file, cluster.as_bytes())?;
        state::write(&path, &serde_json::to_vec_pretty(&profile)?)?;
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("SSH tunnel startup timed out")
    .and_then(std::convert::identity);
    if let Err(error) = result {
        let _ = tunnel.child.kill().await;
        let _ = std::fs::remove_file(&profile.fdb_cluster_file);
        return Err(error);
    }
    // The recorded control socket is the authority for stopping this process.
    tunnel.published = true;
    let _ = socket_dir.keep();
    print(
        &profile,
        json,
        &Timing::new(started, probe_elapsed, startup.elapsed(), false),
    )
}

pub(super) fn new_profile(
    state_dir: &Path,
    node: &swarmy_config::RemoteNode,
    ports: RemotePorts,
    socket_path: std::path::PathBuf,
) -> Result<RemoteProfile> {
    Ok(RemoteProfile {
        name: node.name.clone(),
        socket_path,
        pid: 0,
        ports,
        remote_ports: node.ports,
        fdb_cluster_file: remote_path(state_dir, &node.name, "cluster")?,
        nats_url: format!("nats://127.0.0.1:{}", ports.nats),
        s3_endpoint: if node
            .launch_settings
            .as_ref()
            .and_then(|s| s.bucket.as_ref())
            .is_some()
        {
            String::new()
        } else {
            format!("http://127.0.0.1:{}", ports.s3)
        },
        s3_bucket: node.launch_settings.as_ref().and_then(|s| s.bucket.clone()),
        s3_region: node
            .launch_settings
            .as_ref()
            .and_then(|s| s.bucket.as_ref().map(|_| node.region.clone())),
        default_image: node.default_image.clone(),
    })
}

fn reserve_ports(node: &swarmy_config::RemoteNode) -> Result<([TcpListener; 3], RemotePorts)> {
    // Reserve all three ports together so an ephemeral choice cannot be reused.
    let reservations = [
        reserve(node.ports.fdb)?,
        reserve(node.ports.nats)?,
        reserve(
            if node
                .launch_settings
                .as_ref()
                .and_then(|s| s.bucket.as_ref())
                .is_some()
            {
                0
            } else {
                node.ports.s3
            },
        )?,
    ];
    let ports = RemotePorts {
        fdb: reservations[0].local_addr()?.port(),
        nats: reservations[1].local_addr()?.port(),
        s3: reservations[2].local_addr()?.port(),
    };
    Ok((reservations, ports))
}

fn tunnel_command(
    node: &swarmy_config::RemoteNode,
    profile: &RemoteProfile,
    state_dir: &Path,
    address: &str,
) -> Result<(tokio::process::Command, std::path::PathBuf)> {
    let ports = profile.ports;
    let mut command = ssh::command(node)?;
    command
        .args(["-N", "-M", "-S"])
        .arg(&profile.socket_path)
        .args(["-o", "ControlPersist=no", "-o", "ExitOnForwardFailure=yes"]);
    for (local, remote) in [(ports.fdb, node.ports.fdb), (ports.nats, node.ports.nats)]
        .into_iter()
        .chain((profile.s3_bucket.is_none()).then_some((ports.s3, node.ports.s3)))
    {
        ensure!(remote != 0, "remote ports must be nonzero");
        command
            .arg("-L")
            .arg(format!("127.0.0.1:{local}:127.0.0.1:{remote}"));
    }
    let log_path = remote_path(state_dir, &profile.name, "ssh.log")?;
    state::write(&log_path, b"")?;
    command
        .arg(address)
        .stdout(Stdio::null())
        .stderr(std::fs::OpenOptions::new().append(true).open(&log_path)?)
        .process_group(0);
    Ok((command, log_path))
}

// Keep the cluster identity from the server while connecting through the local tunnel.
fn rewrite_address(cluster: &str, port: u16) -> Result<String> {
    let line = cluster
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with('#'))
        .context("empty cluster file")?;
    let (identity, address) = line.split_once('@').context("invalid cluster file")?;
    ensure!(
        identity.contains(':') && address.parse::<std::net::SocketAddr>().is_ok(),
        "expected one coordinator address in remote cluster file"
    );
    Ok(format!("{identity}@127.0.0.1:{port}\n"))
}

fn reserve(preferred: u16) -> Result<TcpListener> {
    Ok(
        TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, preferred))
            .or_else(|_| TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)))?,
    )
}

#[derive(serde::Serialize)]
struct Timing {
    elapsed_seconds: f64,
    address_probe_seconds: f64,
    tunnel_startup_seconds: f64,
    reused: bool,
}

impl Timing {
    fn new(started: Instant, probing: Duration, startup: Duration, reused: bool) -> Self {
        Self {
            elapsed_seconds: started.elapsed().as_secs_f64(),
            address_probe_seconds: probing.as_secs_f64(),
            tunnel_startup_seconds: startup.as_secs_f64(),
            reused,
        }
    }
}

fn print(profile: &RemoteProfile, json: bool, timing: &Timing) -> Result<()> {
    if json {
        let mut output = serde_json::to_value(profile)?;
        output["timing"] = serde_json::to_value(timing)?;
        println!("{output}");
    } else {
        println!(
            "export SWARMY_REMOTE={}\n# FoundationDB: {}\n# NATS: {}\n# S3: {}",
            profile.name,
            profile.fdb_cluster_file.display(),
            profile.nats_url,
            profile.s3_bucket.as_deref().unwrap_or(&profile.s3_endpoint)
        );
        if let Some(image) = &profile.default_image {
            println!("# Default image: {image}");
        }
        println!("# swarmy dev up --remote {}", profile.name);
        println!(
            "# Connected in {:.3}s (address probing: {:.3}s; tunnel startup: {:.3}s; reused: {})",
            timing.elapsed_seconds,
            timing.address_probe_seconds,
            timing.tunnel_startup_seconds,
            timing.reused
        );
    }
    if profile.validate_fdb_port().is_err() {
        eprintln!(
            "FoundationDB coordinator forwarded on {}, but the server advertises {}. FoundationDB rejects port remapping. Free the advertised port and reconnect before starting services; doctor will report this profile as unusable.",
            profile.ports.fdb, profile.remote_ports.fdb
        );
    }
    Ok(())
}

pub(super) fn cleanup(profile: &RemoteProfile) -> Result<()> {
    for path in [&profile.socket_path, &profile.fdb_cluster_file] {
        if path.exists() {
            std::fs::remove_file(path)?;
        }
    }
    if let Some(parent) = profile.socket_path.parent().filter(|p| *p != Path::new("")) {
        // Never recursively remove a directory supplied by a profile.
        let _ = std::fs::remove_dir(parent);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn advertised_address_rewrite_preserves_cluster_identity() {
        assert_eq!(
            rewrite_address("# comment\nstack:secret@10.2.3.4:4500\n", 4500).unwrap(),
            "stack:secret@127.0.0.1:4500\n"
        );
        assert_eq!(
            rewrite_address("dev:dev@127.0.0.1:4500", 4500).unwrap(),
            "dev:dev@127.0.0.1:4500\n"
        );
        for bad in [
            "",
            "dev",
            "dev:dev@invalid",
            "dev:dev@10.0.0.1:4500,10.0.0.2:4500",
        ] {
            assert!(rewrite_address(bad, 4500).is_err());
        }
    }

    #[test]
    fn all_tunnels_forward_to_loopback() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("remote")).unwrap();
        let mut node: swarmy_config::RemoteNode = serde_json::from_value(serde_json::json!({
            "name": "test", "region": "test", "instance_id": "i-test",
            "public_ip": "203.0.113.1", "private_ip": "10.0.0.1",
            "key_path": "key", "created_at": "now"
        }))
        .unwrap();
        let profile = RemoteProfile {
            name: "test".into(),
            socket_path: dir.path().join("socket"),
            pid: 0,
            ports: RemotePorts::default(),
            remote_ports: RemotePorts::default(),
            fdb_cluster_file: dir.path().join("cluster"),
            nats_url: String::new(),
            s3_endpoint: String::new(),
            s3_bucket: None,
            s3_region: None,
            default_image: None,
        };
        for (settings, destination) in [
            (None, "127.0.0.1"),
            (Some(swarmy_config::RemoteSettings::default()), "127.0.0.1"),
        ] {
            node.launch_settings = settings;
            let (command, _) =
                tunnel_command(&node, &profile, dir.path(), &node.public_ip).unwrap();
            let args: Vec<_> = command
                .as_std()
                .get_args()
                .map(|arg| arg.to_string_lossy().into_owned())
                .collect();
            for port in [4500, 4222, 8333] {
                assert!(args.contains(&format!("127.0.0.1:{port}:{destination}:{port}")));
            }
        }
        let mut bucket_profile = profile;
        bucket_profile.s3_bucket = Some("bucket-test".into());
        bucket_profile.s3_region = Some("us-east-1".into());
        let (command, _) =
            tunnel_command(&node, &bucket_profile, dir.path(), &node.public_ip).unwrap();
        let args: Vec<_> = command
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        assert!(!args.iter().any(|arg| arg.contains(":8333:")));
    }

    #[test]
    fn collision_chooses_another_port() {
        let occupied = reserve(0).unwrap();
        let port = occupied.local_addr().unwrap().port();
        let alternative = reserve(port).unwrap();
        assert_ne!(alternative.local_addr().unwrap().port(), port);
    }
}
