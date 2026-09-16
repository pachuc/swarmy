use std::{path::PathBuf, process::Stdio, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use swarmy_config::RemoteNode;
use tokio::process::Command;

use super::Host;

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
}

fn arguments(node: &RemoteNode) -> Vec<String> {
    vec![
        "-i".into(),
        node.key_path.to_string_lossy().into_owned(),
        "-o".into(),
        "IdentitiesOnly=yes".into(),
        "-o".into(),
        "BatchMode=yes".into(),
        "-o".into(),
        "ConnectTimeout=10".into(),
        "-o".into(),
        "StrictHostKeyChecking=accept-new".into(),
        "-o".into(),
        format!(
            "UserKnownHostsFile={}",
            node.key_path.with_extension("known_hosts").display()
        ),
        "-o".into(),
        "ServerAliveInterval=15".into(),
        "-o".into(),
        "ServerAliveCountMax=4".into(),
    ]
}

pub fn command_line(node: &RemoteNode, address: &str) -> String {
    let mut args = vec!["ssh".into()];
    args.extend(arguments(node));
    args.push(format!("{}@{address}", node.ssh_user));
    shell_words::join(args)
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

impl Host for Ssh {
    async fn generate_key(&self, node: &RemoteNode) -> Result<Vec<u8>> {
        checked(
            Command::new("ssh-keygen")
                .args(["-q", "-t", "ed25519", "-N", "", "-f"])
                .arg(&node.key_path),
            "generate SSH key",
        )
        .await?;
        Ok(std::fs::read(node.key_path.with_extension("pub"))?)
    }

    async fn provision(&self, node: &RemoteNode) -> Result<String> {
        let address = wait_ssh(node).await?;
        let destination = format!("{}@{address}", node.ssh_user);
        checked(Command::new("ssh").args(arguments(node)).arg(&destination)
            .arg("command -v rsync >/dev/null || (sudo cloud-init status --wait && sudo apt-get update && sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y rsync)"), "prepare remote rsync").await?;
        println!("Copying repository checkout");
        let mut transport = vec!["ssh".into()];
        transport.extend(arguments(node));
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
                .arg(format!("{destination}:swarmy/")),
            "copy checkout with rsync",
        )
        .await?;
        println!("Provisioning node and building release binaries (this takes several minutes)");
        checked(
            Command::new("ssh")
                .args(arguments(node))
                .arg(destination)
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
            let status = Command::new("ssh")
                .args(arguments(node))
                .arg(format!("{}@{address}", node.ssh_user))
                .arg("true")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true)
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
