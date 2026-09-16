use super::{
    ssh,
    state::{self, State},
};
use anyhow::{Context, Result, bail, ensure};
use std::{net::TcpListener, path::Path, process::Stdio, time::Duration};
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
    let _lock = state.lock()?;
    let node = state.require(name)?;
    let path = remote_path(state_dir, name, "profile.json")?;
    if path.exists() {
        let profile = RemoteProfile::read(state_dir, name)?;
        if ssh::control(&profile, "check").await?.status.success() {
            print(&profile, json)?;
            return Ok(());
        }
        cleanup(&profile)?;
        std::fs::remove_file(&path)?;
    }
    let address = ssh::reachable_address(&node).await?;
    // Reserve all three ports together so an ephemeral choice cannot be reused.
    let reservations = [
        reserve(node.ports.fdb)?,
        reserve(node.ports.nats)?,
        reserve(node.ports.s3)?,
    ];
    let ports = RemotePorts {
        fdb: reservations[0].local_addr()?.port(),
        nats: reservations[1].local_addr()?.port(),
        s3: reservations[2].local_addr()?.port(),
    };
    // A private, short directory avoids the Unix socket path length limit.
    let socket_dir = tempfile::Builder::new()
        .prefix("swarmy-ssh-")
        .tempdir_in("/tmp")?;
    let profile = RemoteProfile {
        name: name.into(),
        socket_path: socket_dir.path().join("control"),
        pid: 0,
        ports,
        remote_ports: node.ports,
        fdb_cluster_file: remote_path(state_dir, name, "cluster")?,
        nats_url: format!("nats://127.0.0.1:{}", ports.nats),
        s3_endpoint: format!("http://127.0.0.1:{}", ports.s3),
    };
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
        let cluster = if node.launch_settings.is_some() {
            let output = ssh::command(&node)?
                .arg(&address)
                .arg("cat swarmy/.dev/fdb.cluster")
                .output()
                .await?;
            ensure!(output.status.success(), "read remote cluster file failed");
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
    print(&profile, json)
}

fn tunnel_command(
    node: &swarmy_config::RemoteNode,
    profile: &RemoteProfile,
    state_dir: &Path,
    address: &str,
) -> Result<(tokio::process::Command, std::path::PathBuf)> {
    let ports = profile.ports;
    let destination: std::net::IpAddr = if node.launch_settings.is_some() {
        node.private_ip.parse().context("invalid private IP")?
    } else {
        std::net::Ipv4Addr::LOCALHOST.into()
    };
    let mut command = ssh::command(node)?;
    command
        .args(["-N", "-M", "-S"])
        .arg(&profile.socket_path)
        .args(["-o", "ControlPersist=no", "-o", "ExitOnForwardFailure=yes"]);
    for (local, remote) in [
        (ports.fdb, node.ports.fdb),
        (ports.nats, node.ports.nats),
        (ports.s3, node.ports.s3),
    ] {
        ensure!(remote != 0, "remote ports must be nonzero");
        command
            .arg("-L")
            .arg(format!("127.0.0.1:{local}:{destination}:{remote}"));
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

fn print(profile: &RemoteProfile, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string(profile)?);
    } else {
        println!(
            "export SWARMY_REMOTE={}\n# FoundationDB: {}\n# NATS: {}\n# S3: {}",
            profile.name,
            profile.fdb_cluster_file.display(),
            profile.nats_url,
            profile.s3_endpoint
        );
        println!("# swarmy dev up --remote {}", profile.name);
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
    fn new_tunnel_forwards_to_private_address_and_legacy_tunnel_to_loopback() {
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
        };
        for (settings, destination) in [
            (None, "127.0.0.1"),
            (Some(swarmy_config::RemoteSettings::default()), "10.0.0.1"),
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
    }

    #[test]
    fn collision_chooses_another_port() {
        let occupied = reserve(0).unwrap();
        let port = occupied.local_addr().unwrap().port();
        let alternative = reserve(port).unwrap();
        assert_ne!(alternative.local_addr().unwrap().port(), port);
    }
}
