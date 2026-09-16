use anyhow::{Context, Result, ensure};
use std::{process::Stdio, time::Duration};
use swarmy_config::{RemoteNode, RemoteProfile};
use tokio::{process::Command, time::timeout};

pub fn command(node: &RemoteNode) -> Result<Command> {
    // SSH passes remote commands to a shell; the destination must be data only.
    let _: std::net::IpAddr = node
        .public_ip
        .parse()
        .context("public_ip must be an IP address")?;
    ensure!(
        !node.ssh_user.is_empty()
            && node
                .ssh_user
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
        "invalid ssh_user"
    );
    let mut command = Command::new("ssh");
    command
        .args([
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=5",
            "-o",
            "ServerAliveInterval=15",
            "-o",
            "ServerAliveCountMax=3",
            "-o",
            "StrictHostKeyChecking=accept-new",
            "-o",
            "IdentitiesOnly=yes",
        ])
        .arg("-i")
        .arg(&node.key_path)
        .arg("-l")
        .arg(&node.ssh_user)
        .stdin(Stdio::null())
        .kill_on_drop(true);
    Ok(command)
}

pub async fn control(profile: &RemoteProfile, action: &str) -> Result<std::process::Output> {
    Ok(timeout(
        Duration::from_secs(5),
        Command::new("ssh")
            .arg("-S")
            .arg(&profile.socket_path)
            .args(["-O", action, "localhost"])
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .context("SSH control request timed out")??)
}

pub async fn healthy(profile: &RemoteProfile) -> bool {
    control(profile, "check")
        .await
        .is_ok_and(|output| output.status.success())
}
