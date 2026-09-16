//! SSH helpers shared by provisioning, tunnel, log, and status commands.
//! `swarmy-session` includes this file directly, so it must not reach into the
//! parent module.
use std::{path::PathBuf, process::Stdio, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use swarmy_config::{RemoteNode, RemoteProfile};
use tokio::{process::Command, time::timeout};

/// Options shared by every SSH and rsync invocation for a node. The host key
/// learned while provisioning lives next to the key so later commands verify it.
fn arguments(node: &RemoteNode) -> Result<Vec<String>> {
    // SSH passes remote commands to a shell; the user and destination must be data only.
    ensure!(
        !node.ssh_user.is_empty()
            && node
                .ssh_user
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
        "invalid ssh_user"
    );
    Ok(vec![
        "-o".into(),
        "BatchMode=yes".into(),
        "-o".into(),
        "ConnectTimeout=10".into(),
        "-o".into(),
        "ServerAliveInterval=15".into(),
        "-o".into(),
        "ServerAliveCountMax=4".into(),
        "-o".into(),
        "StrictHostKeyChecking=accept-new".into(),
        "-o".into(),
        "IdentitiesOnly=yes".into(),
        "-o".into(),
        format!(
            "UserKnownHostsFile={}",
            node.key_path.with_extension("known_hosts").display()
        ),
        "-i".into(),
        node.key_path.to_string_lossy().into_owned(),
        "-l".into(),
        node.ssh_user.clone(),
    ])
}

fn base(node: &RemoteNode) -> Result<Command> {
    let mut command = Command::new("ssh");
    command
        .args(arguments(node)?)
        .stdin(Stdio::null())
        .kill_on_drop(true);
    Ok(command)
}

/// A configured `ssh` command; callers append the public address and remote command.
pub fn command(node: &RemoteNode) -> Result<Command> {
    let _: std::net::IpAddr = node
        .public_ip
        .parse()
        .context("public_ip must be an IP address")?;
    base(node)
}

/// The interactive login command to print after provisioning.
pub fn command_line(node: &RemoteNode, address: &str) -> Result<String> {
    let mut args = vec!["ssh".to_owned()];
    args.extend(arguments(node)?);
    args.push(address.to_owned());
    Ok(shell_words::join(args))
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

async fn checked(command: &mut Command, action: &str) -> Result<()> {
    let status = command
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .status()
        .await
        .with_context(|| action.to_owned())?;
    ensure!(status.success(), "{action} failed with {status}");
    Ok(())
}

/// Create the node's private key file and return its public half.
pub async fn generate_key(node: &RemoteNode) -> Result<Vec<u8>> {
    checked(
        Command::new("ssh-keygen")
            .args(["-q", "-t", "ed25519", "-N", "", "-f"])
            .arg(&node.key_path),
        "generate SSH key",
    )
    .await?;
    Ok(std::fs::read(node.key_path.with_extension("pub"))?)
}

/// Provisions a fresh node from this repository checkout.
pub struct Ssh {
    repo: PathBuf,
}

impl Ssh {
    pub fn discover() -> Result<Self> {
        let cwd = std::env::current_dir()?;
        let repo = cwd
            .ancestors()
            .find(|p| p.join("scripts/remote-provision.sh").is_file())
            .context("run remote up from a swarmy repository checkout")?
            .to_owned();
        Ok(Self { repo })
    }

    /// Copy the checkout and run the provisioning script; returns the reachable address.
    pub async fn provision(&self, node: &RemoteNode) -> Result<String> {
        let address = wait_ssh(node).await?;
        checked(base(node)?.arg(&address)
            .arg("command -v rsync >/dev/null || (sudo cloud-init status --wait && sudo apt-get update && sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y rsync)"), "prepare remote rsync").await?;
        println!("Copying repository checkout");
        let mut transport = vec!["ssh".to_owned()];
        transport.extend(arguments(node)?);
        checked(
            Command::new("rsync")
                .args([
                    "-az",
                    "--exclude=target/",
                    "--exclude=.dev/",
                    "--exclude=.swarmy/",
                    "--exclude=.git/",
                    "--exclude=.env",
                    "--exclude=.env.*",
                    "-e",
                ])
                .arg(shell_words::join(transport))
                .arg(format!("{}/", self.repo.display()))
                .arg(format!("{address}:swarmy/")),
            "copy checkout with rsync",
        )
        .await?;
        println!("Provisioning node and building release binaries (this takes several minutes)");
        checked(
            base(node)?
                .arg(&address)
                .arg("cd swarmy && bash scripts/remote-provision.sh"),
            "provision remote node",
        )
        .await?;
        Ok(address)
    }
}

async fn wait_ssh(node: &RemoteNode) -> Result<String> {
    for address in [&node.public_ip, &node.private_ip] {
        let _: std::net::IpAddr = address.parse().context("invalid instance IP")?;
    }
    println!(
        "Waiting for SSH at {} or {}",
        node.public_ip, node.private_ip
    );
    for _ in 0..90 {
        // Same-VPC launchers may reach only the private IP under group-based rules.
        for address in [&node.public_ip, &node.private_ip] {
            let status = base(node)?
                .arg(address)
                .arg("true")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await?;
            if status.success() {
                return Ok(address.clone());
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    bail!("timed out waiting for SSH on both instance addresses")
}
