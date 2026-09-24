//! SSH helpers shared by provisioning, tunnel, log, and status commands.
//! `swarmy-session` includes this file directly, so it must not reach into the
//! parent module.
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

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

/// Same-VPC launchers may be admitted only through the private interface.
pub async fn reachable_address(node: &RemoteNode) -> Result<String> {
    for address in [&node.public_ip, &node.private_ip] {
        let _: std::net::IpAddr = address.parse().context("invalid instance IP")?;
        let result = timeout(
            Duration::from_secs(7),
            base(node)?.arg(address).arg("true").output(),
        )
        .await;
        if result.is_ok_and(|output| output.is_ok_and(|output| output.status.success())) {
            return Ok(address.clone());
        }
    }
    bail!("SSH is unreachable at both instance addresses")
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
            .context("run remote provisioning from a swarmy repository checkout")?
            .to_owned();
        Ok(Self { repo })
    }

    /// Resolve before launching so missing recipes cannot leave cloud resources behind.
    pub fn image_recipe(&self, path: &Path) -> Result<PathBuf> {
        let path = self
            .repo
            .join(path)
            .canonicalize()
            .context("resolve image recipe directory")?;
        ensure!(
            path.is_dir() && path.join("recipe.toml").is_file(),
            "image recipe must be a directory containing recipe.toml"
        );
        path.to_str().context("image recipe path must be UTF-8")?;
        let relative = path
            .strip_prefix(&self.repo)
            .context("image recipe must be inside the copied checkout")?;
        ensure!(
            !relative.components().any(|part| matches!(
                part.as_os_str().to_str(),
                Some("target" | ".dev" | ".swarmy" | ".git")
            )),
            "image recipe is excluded from the copied checkout"
        );
        Ok(relative.to_owned())
    }

    /// Copy the checkout and run the provisioning script; returns the reachable address.
    pub async fn provision(
        &self,
        node: &RemoteNode,
        primary: Option<&RemoteNode>,
    ) -> Result<String> {
        let address = wait_ssh(node).await?;
        checked(base(node)?.arg(&address)
            .arg("command -v rsync >/dev/null || (sudo cloud-init status --wait && sudo apt-get update && sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y rsync)"), "prepare remote rsync").await?;
        println!("Copying repository checkout");
        let mut transport = vec!["ssh".to_owned()];
        transport.extend(arguments(node)?);
        let settings = swarmy_config::Settings::load_base()?.settings;
        let credential = Path::new(&settings.credential_file);
        let mut copy = Command::new("rsync");
        for relative in credential_excludes(&self.repo, credential)
            .into_iter()
            .chain(credential_excludes(
                &self.repo,
                &swarmy_config::Keyring::path()?,
            ))
        {
            copy.arg(format!("--exclude=/{}", relative.display()));
        }
        checked(
            copy.args([
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
        let service_ip: std::net::Ipv4Addr = primary
            .unwrap_or(node)
            .private_ip
            .parse()
            .context("private_ip must be an IPv4 address")?;
        let mode = if let Some(primary) = primary {
            let source = wait_ssh(primary).await?;
            let cluster = base(primary)?
                .arg(&source)
                .arg("cat swarmy/.dev/fdb.cluster")
                .output()
                .await?;
            ensure!(cluster.status.success(), "read primary cluster file failed");
            let cluster = String::from_utf8(cluster.stdout)?;
            ensure!(
                cluster.trim().ends_with("@127.0.0.1:4500"),
                "primary cluster file does not advertise loopback port 4500; recreate this remote"
            );
            checked(
                base(node)?.arg(&address).arg(format!(
                    "mkdir -p swarmy/.dev && printf %s {} > swarmy/.dev/fdb.cluster",
                    shell_words::quote(&cluster)
                )),
                "copy primary cluster file",
            )
            .await?;
            install_tunnel(node, &address, primary, &source).await?;
            "node"
        } else {
            "stack"
        };
        println!("Provisioning node and building release binaries (this takes several minutes)");
        checked(
            base(node)?.arg(&address).arg(format!(
                "cd swarmy && bash scripts/remote-provision.sh {mode} {service_ip} {} {}",
                shell_words::quote(
                    node.launch_settings
                        .as_ref()
                        .and_then(|s| s.bucket.as_deref())
                        .unwrap_or("")
                ),
                shell_words::quote(&node.region)
            )),
            "provision remote node",
        )
        .await?;
        Ok(address)
    }
}

/// Use the node's service environment, including its native `FoundationDB` library.
pub async fn build_image(node: &RemoteNode, address: &str, recipe: &Path) -> Result<()> {
    checked(
        base(node)?
            .arg(address)
            .arg(image_build_command(recipe, &node.name)?),
        "build and register remote image",
    )
    .await
}

fn image_build_command(recipe: &Path, tag: &str) -> Result<String> {
    let recipe = recipe.to_str().context("image recipe path must be UTF-8")?;
    let build = shell_words::join([
        "/usr/local/bin/swarmy",
        "image",
        "build",
        recipe,
        "--name",
        "base-ubuntu",
        "--tag",
        tag,
    ]);
    Ok(format!(
        "cd swarmy && sudo -n bash -c {}",
        shell_words::quote(&format!(
            "set -e; set -a; . /etc/swarmy/node.env; set +a; exec {build}"
        ))
    ))
}

// Generate the forwarding key on its owner; the primary login key never leaves the client.
async fn install_tunnel(
    node: &RemoteNode,
    address: &str,
    primary: &RemoteNode,
    source: &str,
) -> Result<()> {
    let output = base(node)?.arg(address).arg(
        "umask 077; mkdir -p swarmy/.swarmy; test -f swarmy/.swarmy/tunnel-key || ssh-keygen -q -t ed25519 -N '' -f swarmy/.swarmy/tunnel-key; cat swarmy/.swarmy/tunnel-key.pub",
    ).output().await?;
    ensure!(
        output.status.success(),
        "generate joining node tunnel key failed"
    );
    let key = String::from_utf8(output.stdout)?;
    let authorization = tunnel_authorization(&key)?;
    checked(base(primary)?.arg(source).arg(format!(
        "umask 077; mkdir -p ~/.ssh && touch ~/.ssh/authorized_keys && (grep -qxF -- {0} ~/.ssh/authorized_keys || printf '%s\\n' {0} >> ~/.ssh/authorized_keys)",
        shell_words::quote(&authorization)
    )), "authorize joining node tunnel").await?;
    // Obtain the host key over the already authenticated provisioning connection.
    let host_key = base(primary)?
        .arg(source)
        .arg("cat /etc/ssh/ssh_host_ed25519_key.pub")
        .output()
        .await?;
    ensure!(
        host_key.status.success(),
        "read primary SSH host key failed"
    );
    let host_key = String::from_utf8(host_key.stdout)?;
    let known_host = format!("{} {}", primary.private_ip, host_key.trim());
    checked(
        base(node)?.arg(address).arg(format!(
            "umask 077; printf '%s\\n' {} > swarmy/.swarmy/tunnel-known-hosts",
            shell_words::quote(&known_host)
        )),
        "pin primary SSH host key",
    )
    .await
}

fn tunnel_authorization(key: &str) -> Result<String> {
    let fields: Vec<_> = key.split_whitespace().collect();
    ensure!(
        fields.len() >= 2
            && fields[0] == "ssh-ed25519"
            && fields[1]
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"+/=".contains(&byte)),
        "invalid joining node public key"
    );
    // Limit destination access, and disable shell, agent, X11, and PTY sessions.
    Ok(format!(
        "restrict,port-forwarding,command=\"/bin/false\",permitopen=\"127.0.0.1:4500\",permitopen=\"127.0.0.1:4222\",permitopen=\"127.0.0.1:8333\" ssh-ed25519 {}",
        fields[1]
    ))
}

// Exclude both the configured name and its target. A path containing `..` or
// a symlink must not let the ordinary checkout copy export credential contents.
fn credential_excludes(repo: &Path, credential: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Ok(relative) = credential.strip_prefix(repo) {
        paths.push(relative.to_owned());
    }
    if let (Ok(root), Ok(target)) = (repo.canonicalize(), credential.canonicalize())
        && let Ok(relative) = target.strip_prefix(root)
    {
        paths.push(relative.to_owned());
    }
    paths
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

#[cfg(test)]
mod tests {
    use super::{Ssh, image_build_command, tunnel_authorization};

    #[test]
    fn checkout_excludes_credential_targets_with_parent_components_and_symlinks() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir(root.join("nested")).unwrap();
        std::fs::write(root.join("auth.json"), "private fixture").unwrap();
        std::os::unix::fs::symlink(root.join("auth.json"), root.join("alias")).unwrap();
        for path in [root.join("nested/../auth.json"), root.join("alias")] {
            assert!(super::credential_excludes(root, &path).contains(&"auth.json".into()));
        }
    }

    #[test]
    fn image_command_sources_node_environment_and_quotes_recipe() {
        let path = std::path::Path::new("images/custom ' $(touch unwanted)");
        let command = image_build_command(path, "demo").unwrap();
        let outer = shell_words::split(&command).unwrap();
        assert_eq!(
            &outer[..7],
            ["cd", "swarmy", "&&", "sudo", "-n", "bash", "-c"]
        );
        let script = &outer[7];
        assert!(script.starts_with("set -e; set -a; . /etc/swarmy/node.env; set +a; exec "));
        let args = shell_words::split(script.split("exec ").nth(1).unwrap()).unwrap();
        assert_eq!(
            args,
            [
                "/usr/local/bin/swarmy",
                "image",
                "build",
                path.to_str().unwrap(),
                "--name",
                "base-ubuntu",
                "--tag",
                "demo"
            ]
        );
    }

    #[test]
    fn recipes_must_be_copied_with_the_checkout() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        for recipe in ["images/custom", ".swarmy/excluded"] {
            std::fs::create_dir_all(repo.join(recipe)).unwrap();
            std::fs::write(repo.join(recipe).join("recipe.toml"), "").unwrap();
        }
        let host = Ssh { repo: repo.clone() };
        assert_eq!(
            host.image_recipe(std::path::Path::new("images/custom"))
                .unwrap(),
            std::path::Path::new("images/custom")
        );
        for recipe in [
            repo.join("missing"),
            repo.join(".swarmy/excluded"),
            dir.path().to_owned(),
        ] {
            assert!(host.image_recipe(&recipe).is_err());
        }
    }

    #[test]
    fn tunnel_key_authorization_restricts_sessions_and_destinations() {
        let entry = tunnel_authorization("ssh-ed25519 AAAA+/= arbitrary comment\n").unwrap();
        assert!(entry.starts_with("restrict,port-forwarding,command=\"/bin/false\","));
        for port in [4500, 4222, 8333] {
            assert!(entry.contains(&format!("permitopen=\"127.0.0.1:{port}\"")));
        }
        assert!(entry.ends_with(" ssh-ed25519 AAAA+/="));
        for key in [
            "",
            "ssh-rsa AAAA",
            "ssh-ed25519 AAAA;command",
            "ssh-ed25519 'AAAA'",
        ] {
            assert!(tunnel_authorization(key).is_err());
        }
    }
}
