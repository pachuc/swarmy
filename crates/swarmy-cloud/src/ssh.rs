//! SSH helpers shared by provisioning, tunnel, log, and status commands.
use std::{
    fmt::Write as _,
    io::Write as _,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use crate::Result;
use swarmy_config::{RemoteNode, RemoteProfile};
use tokio::io::AsyncWriteExt;
use tokio::{process::Command, time::timeout};

/// Options shared by every SSH and rsync invocation for a node. The host key
/// learned while provisioning lives next to the key so later commands verify it.
fn arguments(node: &RemoteNode) -> Result<Vec<String>> {
    // SSH passes remote commands to a shell; the user and destination must be data only.
    crate::Error::ensure(
        !node.ssh_user.is_empty()
            && node
                .ssh_user
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
        "invalid ssh_user",
    )?;
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

/// Service login for a node, validated so it is data, never shell.
/// Bash on the host resolves its home (`~user`); Rust never assumes
/// `/home/<user>`.
fn service_user(node: &RemoteNode) -> Result<String> {
    let user = node.service_user().to_owned();
    swarmy_config::validate_service_user(&user)?;
    Ok(user)
}

/// Home directory resolved on the host, keeping one path implementation in
/// bash (`~user` expands through the passwd entry).
fn tilde_repo(user: &str) -> String {
    format!("~{user}/swarmy")
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
/// The `command` in [`crate::Error::Ssh`] names the attempted operation,
/// not the remote shell fragment.
///
/// # Errors
///
/// Rejects node records whose public address is not an IP address.
pub fn command(node: &RemoteNode) -> Result<Command> {
    let _: std::net::IpAddr = node
        .public_ip
        .parse()
        .map_err(|source| crate::Error::context(source, "public_ip must be an IP address"))?;
    base(node)
}

/// Same-VPC launchers may be admitted only through the private interface.
///
/// # Errors
///
/// Rejects malformed instance addresses and reports when neither address answers over SSH.
pub async fn reachable_address(node: &RemoteNode) -> Result<String> {
    for address in [&node.public_ip, &node.private_ip] {
        let _: std::net::IpAddr = address
            .parse()
            .map_err(|source| crate::Error::context(source, "invalid instance IP"))?;
        let result = timeout(
            Duration::from_secs(7),
            base(node)?.arg(address).arg("true").output(),
        )
        .await;
        if result.is_ok_and(|output| output.is_ok_and(|output| output.status.success())) {
            return Ok(address.clone());
        }
    }
    Err(crate::Error::SshUnavailable)
}

/// The interactive login command to print after provisioning.
///
/// # Errors
///
/// Rejects node records with an invalid SSH user.
pub fn command_line(node: &RemoteNode, address: &str) -> Result<String> {
    let mut args = vec!["ssh".to_owned()];
    args.extend(arguments(node)?);
    args.push(address.to_owned());
    Ok(shell_words::join(args))
}

/// Send one control action (`check` or `exit`) to a running SSH control master.
///
/// # Errors
///
/// Reports a missing control socket and commands that time out after five seconds.
pub async fn control(profile: &RemoteProfile, action: &str) -> Result<std::process::Output> {
    let output = timeout(
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
    .map_err(|source| crate::Error::context(source, "SSH control request timed out"))?
    .map_err(crate::Error::ssh(&format!("control {action}")))?;
    Ok(output)
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
        .map_err(crate::Error::ssh(action))?;
    if !status.success() {
        return Err(crate::Error::SshStatus {
            command: action.to_owned(),
            status,
        });
    }
    Ok(())
}

async fn run_output(command: &mut Command, action: &str) -> Result<std::process::Output> {
    let output = command.output().await.map_err(crate::Error::ssh(action))?;
    if !output.status.success() {
        return Err(crate::Error::SshStatus {
            command: action.to_owned(),
            status: output.status,
        });
    }
    Ok(output)
}

/// Run a helper script on the host over SSH stdin (`bash -s`), for helpers
/// that must work with no checkout on the host. The `action` names the
/// attempted operation in [`crate::Error::Ssh`].
async fn pipe_script(
    node: &RemoteNode,
    address: &str,
    action: &str,
    script: &str,
) -> Result<std::process::Output> {
    let mut child = base(node)?
        .arg(address)
        .arg("bash -s")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(crate::Error::ssh(action))?;
    let Some(mut stdin) = child.stdin.take() else {
        return Err(crate::Error::other("SSH stdin missing"));
    };
    stdin
        .write_all(script.as_bytes())
        .await
        .map_err(crate::Error::ssh(action))?;
    drop(stdin);
    let output = child
        .wait_with_output()
        .await
        .map_err(crate::Error::ssh(action))?;
    if !output.status.success() {
        return Err(crate::Error::SshStatus {
            command: action.to_owned(),
            status: output.status,
        });
    }
    Ok(output)
}

/// Create the node's private key file and return its public half.
///
/// # Errors
///
/// Reports `ssh-keygen` failures.
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

/// Copy an operator-owned bootstrap private key into remote state for
/// adoption and derive its public half for inspection. The state directory
/// is 0700; the copy is created with mode 0600 whatever the source's mode
/// or umask is, and the source file is left untouched.
///
/// # Errors
///
/// Reports unreadable sources and keys `ssh-keygen` cannot parse.
pub async fn adopt_key(node: &RemoteNode, source: &Path) -> Result<()> {
    let bytes = std::fs::read(source)?;
    let mut key = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&node.key_path)?;
    key.write_all(&bytes)?;
    drop(key);
    let output = run_output(
        Command::new("ssh-keygen")
            .args(["-y", "-f"])
            .arg(&node.key_path),
        "read bootstrap public key",
    )
    .await?;
    std::fs::write(node.key_path.with_extension("pub"), output.stdout)?;
    Ok(())
}

/// Provisions a fresh node from this repository checkout.
pub struct Ssh {
    repo: PathBuf,
}

impl Ssh {
    /// Find the repository checkout holding the provisioning scripts.
    ///
    /// # Errors
    ///
    /// Reports when the current directory is not inside a swarmy checkout.
    pub fn discover() -> Result<Self> {
        let cwd = std::env::current_dir()?;
        let repo = cwd
            .ancestors()
            .find(|p| p.join("scripts/remote-provision.sh").is_file())
            .ok_or_else(|| {
                crate::Error::other("run remote provisioning from a swarmy repository checkout")
            })?
            .to_owned();
        Ok(Self { repo })
    }

    /// Resolve before launching so missing recipes cannot leave cloud resources behind.
    ///
    /// # Errors
    ///
    /// Reports recipes outside the checkout and directories without `recipe.toml`.
    pub fn image_recipe(&self, path: &Path) -> Result<PathBuf> {
        let path =
            self.repo.join(path).canonicalize().map_err(|source| {
                crate::Error::context(source, "resolve image recipe directory")
            })?;
        crate::Error::ensure(
            path.is_dir() && path.join("recipe.toml").is_file(),
            "image recipe must be a directory containing recipe.toml",
        )?;
        path.to_str()
            .ok_or_else(|| crate::Error::other("image recipe path must be UTF-8"))?;
        let relative = path.strip_prefix(&self.repo).map_err(|source| {
            crate::Error::context(source, "image recipe must be inside the copied checkout")
        })?;
        crate::Error::ensure(
            !relative.components().any(|part| {
                matches!(
                    part.as_os_str().to_str(),
                    Some("target" | ".dev" | ".swarmy" | ".git")
                )
            }),
            "image recipe is excluded from the copied checkout",
        )?;
        Ok(relative.to_owned())
    }

    /// List block devices with their models (`lsblk -dno PATH,MODEL`).
    /// The caller matches the model it needs; this stays provider-independent.
    ///
    /// # Errors
    ///
    /// Reports unreachable hosts and SSH failures.
    pub(crate) async fn block_devices(&self, node: &RemoteNode) -> Result<String> {
        let _ = self;
        let address = wait_ssh(node).await?;
        let output = run_output(
            base(node)?.arg(&address).arg("lsblk -dno PATH,MODEL"),
            "list block devices",
        )
        .await?;
        Ok(String::from_utf8(output.stdout)?)
    }

    /// Read the provisioning env helpers for piping over stdin when the
    /// checkout is not on the host (yet or anymore). The piped script has
    /// no directory for `BASH_SOURCE`, so its S3 helper cannot be sourced;
    /// only functions without that need run here.
    fn piped_env(&self) -> Result<String> {
        let env = String::from_utf8(std::fs::read(
            self.repo.join("scripts/remote-provision-env.sh"),
        )?)?;
        let mut script = String::new();
        for line in env.split_inclusive('\n') {
            if line.trim_start().starts_with("source") && line.contains("remote-s3-env.sh") {
                continue;
            }
            script.push_str(line);
        }
        Ok(script)
    }

    /// Run the checkout's `ensure_service_user` on the host before the first
    /// copy, so a root login gains a destination owned by its owner. The helper
    /// script is piped over stdin because the checkout is not on the host yet;
    /// user creation, sudoers, and path resolution stay in the one bash
    /// implementation instead of being repeated in Rust. Returns the
    /// host-resolved home for the rsync destination; every later command
    /// resolves the same home through `~user` on the host instead of assuming
    /// `/home/<user>`.
    async fn ensure_service_user(
        &self,
        node: &RemoteNode,
        address: &str,
        user: &str,
    ) -> Result<String> {
        if user == "root" {
            return Ok("/root".to_owned());
        }
        let mut script = self.piped_env()?;
        // `user` is validated (letters, digits, `_`, `-`), so embedding it in
        // single quotes is data, never shell. `visudo -cf` reports to stdout, so
        // the home is the last line.
        // Writing to a `String` cannot fail.
        write!(
            script,
            "ensure_service_user '{user}'\nservice_home_for '{user}'\n"
        )
        .expect("writing to String cannot fail");
        let output = pipe_script(node, address, "create service user", &script).await?;
        let home = String::from_utf8(output.stdout)?
            .lines()
            .rfind(|line| !line.trim().is_empty())
            .unwrap_or_default()
            .trim()
            .to_owned();
        crate::Error::ensure(
            home.starts_with('/')
                && !home.contains([
                    ' ', '\t', '\n', '\'', '"', '$', '`', ';', '&', '|', '(', ')', '<', '>',
                ])
                && !home.contains(".."),
            "invalid service home from host",
        )?;
        Ok(home)
    }

    /// Copy the same filtered checkout used by initial provisioning.
    ///
    /// # Errors
    ///
    /// Reports SSH, rsync, and remote package installation failures.
    pub async fn copy_checkout(&self, node: &RemoteNode, address: &str) -> Result<()> {
        // Dedicated servers boot without cloud-init; only wait where it exists.
        checked(base(node)?.arg(address)
            .arg("command -v rsync >/dev/null || (if command -v cloud-init >/dev/null 2>&1; then sudo cloud-init status --wait; fi; sudo apt-get update && sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y rsync)"), "prepare remote rsync").await?;
        let user = service_user(node)?;
        // A root login has no service home yet; run the checkout's user
        // setup before the copy so rsync has a destination owned by its owner.
        let home = self.ensure_service_user(node, address, &user).await?;
        let repo = format!("{home}/swarmy");
        let mut transport = vec!["ssh".to_owned()];
        transport.extend(arguments(node)?);
        let settings = swarmy_config::Settings::load_base()?.settings;
        let credential = &settings.selection.credential_file;
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
        copy.arg("-az");
        copy.arg("--mkpath");
        for filter in checkout_filters() {
            copy.arg(filter);
        }
        checked(
            copy.args(["-e"])
                .arg(shell_words::join(transport))
                .arg(format!("{}/", self.repo.display()))
                .arg(format!("{address}:{repo}/")),
            "copy checkout with rsync",
        )
        .await?;
        // A root bootstrap login owns the copied tree; hand it to the service user.
        // The user already exists from the bootstrap, so the chown cannot fail
        // on a missing login.
        checked(
            base(node)?.arg(address).arg(format!(
                "sudo chown -R {user}:{user} {}",
                shell_words::quote(&repo)
            )),
            "hand copied checkout to service user",
        )
        .await?;
        Ok(())
    }

    /// Report uncommitted paths while ignoring Python bytecode caches.
    ///
    /// # Errors
    ///
    /// Reports git failures and non-UTF-8 free output is lossy-decoded, never an error.
    pub fn checkout_changes(&self) -> Result<Vec<String>> {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(&self.repo)
            .args(["status", "--porcelain", "--untracked-files=all"])
            .output()?;
        crate::Error::ensure(
            output.status.success(),
            "cannot inspect checkout git status",
        )?;
        Ok(String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter(|line| !is_python_cache(line))
            .map(str::to_owned)
            .collect())
    }

    /// Report whether the node has swarmy service units installed. Asks the
    /// host script (piped over stdin, so no checkout is needed), which reads
    /// the shared unit list instead of matching unit names in Rust.
    ///
    /// # Errors
    ///
    /// Reports SSH failures and non-zero remote exits.
    pub async fn has_service_units(&self, node: &RemoteNode, address: &str) -> Result<bool> {
        let mut script = self.piped_env()?;
        // Fail loudly inside the piped script, like decommission does: the
        // query must error when systemctl fails, never look like no units.
        // Writing to a `String` cannot fail.
        write!(script, "set -euo pipefail\nlist_installed_control_units\n")
            .expect("writing to String cannot fail");
        let output = pipe_script(node, address, "inspect installed service units", &script).await?;
        Ok(!String::from_utf8(output.stdout)?.trim().is_empty())
    }

    /// Read the installed `swarmyd` version from a node.
    ///
    /// # Errors
    ///
    /// Reports SSH failures and non-zero remote exits.
    pub async fn node_version(&self, node: &RemoteNode, address: &str) -> Result<String> {
        let output = run_output(
            base(node)?
                .arg(address)
                .arg("/usr/local/bin/swarmyd --version"),
            "read node version",
        )
        .await?;
        Ok(String::from_utf8(output.stdout)?.trim().to_owned())
    }

    /// Rebuild the checkout on a node, install changed binaries, and restart
    /// service units whose running executables differ.
    ///
    /// # Errors
    ///
    /// Reports SSH failures, dirty checkouts without `--allow-dirty`, and
    /// remote build or restart failures.
    pub async fn upgrade(
        &self,
        node: &RemoteNode,
        address: &str,
        services_only: bool,
        drain_timeout: Duration,
        stack: bool,
    ) -> Result<serde_json::Value> {
        self.copy_checkout(node, address).await?;
        let mode = if stack { "stack" } else { "node" };
        let services = if services_only {
            "services-only"
        } else {
            "all"
        };
        let user = service_user(node)?;
        // Bash resolves the checkout through `~user`; Rust passes only the login.
        let script = format!(
            "cd {} && bash scripts/remote-upgrade.sh {mode} {services} {}",
            tilde_repo(&user),
            drain_timeout.as_secs()
        );
        let mut remote = base(node)?;
        remote.arg(address).arg(script).stderr(Stdio::inherit());
        let output = run_output(&mut remote, "remote upgrade").await?;
        serde_json::from_slice(&output.stdout)
            .map_err(|source| crate::Error::context(source, "parse remote upgrade summary"))
    }

    /// Copy the checkout and run the provisioning script; returns the reachable address.
    ///
    /// # Errors
    ///
    /// Reports unreachable hosts and provisioning script failures.
    pub async fn provision(
        &self,
        node: &RemoteNode,
        primary: Option<&RemoteNode>,
    ) -> Result<String> {
        let address = wait_ssh(node).await?;
        cloud_out!("Copying repository checkout");
        self.copy_checkout(node, &address).await?;
        let service_ip: std::net::Ipv4Addr =
            primary
                .unwrap_or(node)
                .private_ip
                .parse()
                .map_err(|source| {
                    crate::Error::context(source, "private_ip must be an IPv4 address")
                })?;
        let mode = if let Some(primary) = primary {
            let source = wait_ssh(primary).await?;
            let cluster = run_output(
                base(primary)?
                    .arg(&source)
                    .arg(read_cluster_command(primary)?),
                "read primary cluster file",
            )
            .await?;
            let cluster = String::from_utf8(cluster.stdout)?;
            crate::Error::ensure(
                cluster.trim().ends_with("@127.0.0.1:4500"),
                "primary cluster file does not advertise loopback port 4500; recreate this remote",
            )?;
            checked(
                base(node)?
                    .arg(&address)
                    .arg(copy_cluster_command(node, &cluster)?),
                "copy primary cluster file",
            )
            .await?;
            install_tunnel(node, &address, primary, &source).await?;
            "node"
        } else {
            "stack"
        };
        cloud_out!("Provisioning node and building release binaries (this takes several minutes)");
        // Static S3 keys travel on SSH stdin into a root-owned file, never on
        // a command line. The provisioning script merges them into the 0600
        // node environment without printing them.
        crate::services::upload_bucket_keys(node, &address).await?;
        checked(
            base(node)?.arg(&address).arg({
                let spec = node.bucket_spec();
                provisioning_command(
                    mode,
                    service_ip,
                    spec.as_ref(),
                    &node.region,
                    node.sandboxes,
                    node,
                )?
            }),
            "provision remote node",
        )
        .await?;
        Ok(address)
    }

    /// Stop swarmy services on an adopted host and remove its units,
    /// binaries, node environment, and checkout. Pipes the checkout's
    /// `decommission_probe` over stdin (so no checkout is needed on the
    /// host), which runs the shared teardown when the checkout is present,
    /// fails loudly when units remain without one, and no-ops when nothing
    /// remains. `/etc/swarmy` goes because it holds static bucket keys and
    /// copied credentials. Local sandbox disk data, the service user, the
    /// fstab line and its mount, and tunnel keys authorized on the primary
    /// stay: the machine is operator-owned.
    ///
    /// # Errors
    ///
    /// Reports unreachable hosts and SSH failures. The wait is short
    /// (about half a minute): `down` must continue past a cancelled
    /// server instead of blocking for a full provisioning wait per host.
    pub async fn decommission(&self, node: &RemoteNode) -> Result<()> {
        let _ = self;
        let address = wait_ssh_for(node, 6).await?;
        let user = service_user(node)?;
        let mut script = self.piped_env()?;
        // Fail loudly inside the piped script: without this, a failed
        // removal would still exit zero and look like a clean teardown.
        // `user` is validated, so single quotes are data, never shell.
        // Writing to a `String` cannot fail.
        write!(script, "set -euo pipefail\ndecommission_probe '{user}'\n")
            .expect("writing to String cannot fail");
        pipe_script(node, &address, "remove swarmy services", &script).await?;
        Ok(())
    }
}

fn is_python_cache(status_line: &str) -> bool {
    status_line
        .get(3..)
        .unwrap_or(status_line)
        .split('/')
        .any(|part| part.trim_matches('"') == "__pycache__")
}

fn provisioning_command(
    mode: &str,
    service_ip: std::net::Ipv4Addr,
    spec: Option<&swarmy_config::BucketSpec>,
    fallback_region: &str,
    sandboxes: u32,
    node: &RemoteNode,
) -> Result<String> {
    let (bucket, region, endpoint, prefix, conditional_create, static_keys) = match spec {
        Some(spec) => (
            spec.bucket.as_str(),
            if spec.region.is_empty() {
                fallback_region
            } else {
                spec.region.as_str()
            },
            spec.endpoint.as_str(),
            spec.prefix.as_str(),
            spec.conditional_create,
            spec.needs_static_keys(),
        ),
        None => ("", fallback_region, "", "", true, false),
    };
    let user = service_user(node)?;
    // Bash resolves `~user` through the passwd entry; Rust passes only the login.
    Ok(format!(
        "cd {} && bash scripts/remote-provision.sh {mode} {service_ip} {} {} {} {} {conditional_create} {static_keys} {sandboxes} {} {}",
        tilde_repo(&user),
        shell_words::quote(bucket),
        shell_words::quote(region),
        shell_words::quote(endpoint),
        shell_words::quote(prefix),
        shell_words::quote(&user),
        shell_words::quote(node.local_storage()),
    ))
}

/// Use the node's service environment, including its native `FoundationDB` library.
///
/// # Errors
///
/// Reports SSH failures and image build failures on the node.
pub async fn build_image(node: &RemoteNode, address: &str, recipe: &Path) -> Result<()> {
    checked(
        base(node)?.arg(address).arg(image_build_command(
            &service_user(node)?,
            recipe,
            &node.name,
        )?),
        "build and register remote image",
    )
    .await
}

fn image_build_command(user: &str, recipe: &Path, tag: &str) -> Result<String> {
    let recipe = recipe
        .to_str()
        .ok_or_else(|| crate::Error::other("image recipe path must be UTF-8"))?;
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
        "cd {} && sudo -n bash -c {}",
        tilde_repo(user),
        shell_words::quote(&format!(
            "set -e; set -a; . /etc/swarmy/node.env; set +a; exec {build}"
        ))
    ))
}

/// Remote commands built synchronously so their temporaries never inflate
/// the provisioning futures. Each validates the service user first and
/// resolves its home through `~user` on the host, keeping one path
/// implementation in bash.
fn read_cluster_command(primary: &RemoteNode) -> Result<String> {
    let user = service_user(primary)?;
    Ok(format!("cat {}/.dev/fdb.cluster", tilde_repo(&user)))
}

fn copy_cluster_command(node: &RemoteNode, cluster: &str) -> Result<String> {
    let user = service_user(node)?;
    let repo = tilde_repo(&user);
    Ok(format!(
        "mkdir -p {repo}/.dev && printf %s {} > {repo}/.dev/fdb.cluster && chown -R {user}:{user} {repo}/.dev",
        shell_words::quote(cluster),
    ))
}

fn tunnel_key_command(node: &RemoteNode) -> Result<String> {
    let user = service_user(node)?;
    let repo = tilde_repo(&user);
    Ok(format!(
        "umask 077; mkdir -p {repo}/.swarmy; chown -R {user}:{user} {repo}/.swarmy; test -f {repo}/.swarmy/tunnel-key || sudo -u {user} ssh-keygen -q -t ed25519 -N '' -f {repo}/.swarmy/tunnel-key; chown {user}:{user} {repo}/.swarmy/tunnel-key {repo}/.swarmy/tunnel-key.pub; cat {repo}/.swarmy/tunnel-key.pub",
    ))
}

fn pin_primary_key_command(node: &RemoteNode, known_host: &str) -> Result<String> {
    let user = service_user(node)?;
    let repo = tilde_repo(&user);
    Ok(format!(
        "umask 077; printf '%s\\n' {} > {repo}/.swarmy/tunnel-known-hosts && chown {user}:{user} {repo}/.swarmy/tunnel-known-hosts",
        shell_words::quote(known_host),
    ))
}

// Generate the forwarding key on its owner; the primary login key never leaves the client.
async fn install_tunnel(
    node: &RemoteNode,
    address: &str,
    primary: &RemoteNode,
    source: &str,
) -> Result<()> {
    let output = run_output(
        base(node)?.arg(address).arg(tunnel_key_command(node)?),
        "generate joining node tunnel key",
    )
    .await?;
    let key = String::from_utf8(output.stdout)?;
    let authorization = tunnel_authorization(&key)?;
    // The tunnel unit connects as the service user, so its key belongs in the
    // service user's authorized_keys, not the bootstrap login's.
    let primary_user = service_user(primary)?;
    checked(base(primary)?.arg(source).arg(format!(
        "service_user={primary_user}; service_home=~{primary_user}; umask 077; sudo -u $service_user mkdir -p $service_home/.ssh && sudo -u $service_user touch $service_home/.ssh/authorized_keys && (sudo -u $service_user grep -qxF -- {0} $service_home/.ssh/authorized_keys || printf '%s\\n' {0} | sudo -u $service_user tee -a $service_home/.ssh/authorized_keys >/dev/null)",
        shell_words::quote(&authorization)
    )), "authorize joining node tunnel").await?;
    // Obtain the host key over the already authenticated provisioning connection.
    let host_key = run_output(
        base(primary)?
            .arg(source)
            .arg("cat /etc/ssh/ssh_host_ed25519_key.pub"),
        "read primary SSH host key",
    )
    .await?;
    let host_key = String::from_utf8(host_key.stdout)?;
    let known_host = format!("{} {}", primary.private_ip, host_key.trim());
    checked(
        base(node)?
            .arg(address)
            .arg(pin_primary_key_command(node, &known_host)?),
        "pin primary SSH host key",
    )
    .await
}

fn tunnel_authorization(key: &str) -> Result<String> {
    let fields: Vec<_> = key.split_whitespace().collect();
    crate::Error::ensure(
        fields.len() >= 2
            && fields[0] == "ssh-ed25519"
            && fields[1]
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"+/=".contains(&byte)),
        "invalid joining node public key",
    )?;
    // Limit destination access, and disable shell, agent, X11, and PTY sessions.
    Ok(format!(
        "restrict,port-forwarding,command=\"/bin/false\",permitopen=\"127.0.0.1:4500\",permitopen=\"127.0.0.1:4222\",permitopen=\"127.0.0.1:8333\" ssh-ed25519 {}",
        fields[1]
    ))
}

// Rsync filters for the copied checkout, in first-match-wins order. The fleet
// example stays usable on the node while every other fleet TOML never leaves
// the laptop: `fleet.toml` holds the fleet's GitHub token and is gitignored
// precisely because it is a secret.
fn checkout_filters() -> Vec<String> {
    [
        "--include=scripts/fleet/fleet.example.toml",
        "--exclude=scripts/fleet/fleet.toml",
        "--exclude=scripts/fleet/*.toml",
        "--exclude=target/",
        "--exclude=.dev/",
        "--exclude=.swarmy/",
        "--exclude=.git/",
        "--exclude=.env",
        "--exclude=.env.*",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
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
    wait_ssh_for(node, 90).await
}

/// Wait for SSH with an explicit attempt budget (five seconds between
/// passes over both addresses). Provisioning waits the full budget while a
/// new machine boots; `down` passes a short budget so a cancelled server
/// cannot block teardown for minutes per host.
async fn wait_ssh_for(node: &RemoteNode, attempts: u32) -> Result<String> {
    for address in [&node.public_ip, &node.private_ip] {
        let _: std::net::IpAddr = address
            .parse()
            .map_err(|source| crate::Error::context(source, "invalid instance IP"))?;
    }
    cloud_out!(
        "Waiting for SSH at {} or {}",
        node.public_ip,
        node.private_ip
    );
    for _ in 0..attempts {
        // Same-VPC launchers may reach only the private IP under group-based rules.
        for address in [&node.public_ip, &node.private_ip] {
            let status = base(node)?
                .arg(address)
                .arg("true")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await
                .map_err(crate::Error::ssh("wait for SSH"))?;
            if status.success() {
                return Ok(address.clone());
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    Err(crate::Error::other(
        "timed out waiting for SSH on both instance addresses",
    ))
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
    fn python_caches_are_ignored_but_other_untracked_paths_are_reported() {
        let dir = tempfile::tempdir().unwrap();
        assert!(
            std::process::Command::new("git")
                .arg("init")
                .arg("-q")
                .arg(dir.path())
                .status()
                .unwrap()
                .success()
        );
        std::fs::create_dir(dir.path().join("__pycache__")).unwrap();
        std::fs::write(dir.path().join("__pycache__/module.pyc"), "cache").unwrap();
        std::fs::write(dir.path().join(".gitignore"), "ignored\n").unwrap();
        std::fs::write(dir.path().join("ignored"), "ignored").unwrap();
        std::fs::write(dir.path().join("untracked.rs"), "code").unwrap();
        let changes = Ssh {
            repo: dir.path().to_owned(),
        }
        .checkout_changes()
        .unwrap();
        assert!(changes.iter().any(|path| path.contains("untracked.rs")));
        assert!(
            !changes
                .iter()
                .any(|path| path.contains("__pycache__") || path.contains("ignored"))
        );
    }

    #[test]
    fn image_command_sources_node_environment_and_quotes_recipe() {
        let path = std::path::Path::new("images/custom ' $(touch unwanted)");
        let command = image_build_command("swarmy", path, "demo").unwrap();
        let outer = shell_words::split(&command).unwrap();
        assert_eq!(
            &outer[..7],
            ["cd", "~swarmy/swarmy", "&&", "sudo", "-n", "bash", "-c"]
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
    fn fleet_secrets_are_excluded_from_the_copied_checkout() {
        let filters = super::checkout_filters();
        let position = |flag: &str| {
            filters
                .iter()
                .position(|filter| filter == flag)
                .unwrap_or_else(|| panic!("missing rsync filter: {flag}"))
        };
        // Rsync uses first-match-wins order, so the example must precede the
        // wildcard that keeps every other fleet TOML off the node.
        assert!(
            position("--include=scripts/fleet/fleet.example.toml")
                < position("--exclude=scripts/fleet/*.toml")
        );
        assert!(filters.contains(&"--exclude=scripts/fleet/fleet.toml".to_owned()));
        if std::process::Command::new("rsync")
            .arg("--version")
            .output()
            .is_err()
        {
            eprintln!("skipping fleet rsync copy: rsync is not installed");
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("src");
        for path in [
            "scripts/fleet/fleet",
            "scripts/fleet/fleet.toml",
            "scripts/fleet/other.toml",
            "scripts/fleet/fleet.example.toml",
        ] {
            let full = source.join(path);
            std::fs::create_dir_all(full.parent().unwrap()).unwrap();
            std::fs::write(&full, "fixture").unwrap();
        }
        let destination = dir.path().join("dest");
        std::fs::create_dir_all(&destination).unwrap();
        let mut copy = std::process::Command::new("rsync");
        copy.arg("-a");
        for filter in &filters {
            copy.arg(filter);
        }
        copy.arg(format!("{}/", source.display()));
        copy.arg(&destination);
        assert!(copy.status().unwrap().success());
        assert!(destination.join("scripts/fleet/fleet").is_file());
        assert!(
            destination
                .join("scripts/fleet/fleet.example.toml")
                .is_file()
        );
        assert!(!destination.join("scripts/fleet/fleet.toml").exists());
        assert!(!destination.join("scripts/fleet/other.toml").exists());
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

#[cfg(test)]
mod provisioning_command_tests {
    use super::provisioning_command;

    fn node() -> swarmy_config::RemoteNode {
        serde_json::from_value(serde_json::json!({
            "name": "demo", "region": "us-east-1", "instance_id": "i-test",
            "public_ip": "203.0.113.1", "private_ip": "10.0.0.1",
            "key_path": "key", "launch_attempted": true, "created_at": "now",
            "launch_settings": {
                "service_user": "swarmy", "local_storage": "/dev/nvme1n1",
            },
        }))
        .unwrap()
    }

    #[test]
    fn sandbox_limit_is_passed_to_both_node_modes() {
        let ip = "10.0.0.1".parse().unwrap();
        assert_eq!(
            provisioning_command("stack", ip, None, "us-east-1", 0, &node()).unwrap(),
            "cd ~swarmy/swarmy && bash scripts/remote-provision.sh stack 10.0.0.1 '' us-east-1 '' '' true false 0 swarmy /dev/nvme1n1"
        );
        assert_eq!(
            provisioning_command("node", ip, None, "us-east-1", 4, &node()).unwrap(),
            "cd ~swarmy/swarmy && bash scripts/remote-provision.sh node 10.0.0.1 '' us-east-1 '' '' true false 4 swarmy /dev/nvme1n1"
        );
    }

    #[test]
    fn legacy_nodes_keep_the_ssh_login_and_checkout() {
        let ip = "10.0.0.1".parse().unwrap();
        let legacy: swarmy_config::RemoteNode = serde_json::from_value(serde_json::json!({
            "name": "legacy", "region": "us-east-1", "instance_id": "i-test",
            "public_ip": "203.0.113.1", "private_ip": "10.0.0.1",
            "key_path": "key", "launch_attempted": true, "created_at": "now",
        }))
        .unwrap();
        assert_eq!(legacy.service_user(), "ubuntu");
        assert_eq!(
            provisioning_command("stack", ip, None, "us-east-1", 0, &legacy).unwrap(),
            "cd ~ubuntu/swarmy && bash scripts/remote-provision.sh stack 10.0.0.1 '' us-east-1 '' '' true false 0 ubuntu ''"
        );
    }

    #[test]
    fn existing_launch_settings_without_user_keep_the_login() {
        let ip = "10.0.0.1".parse().unwrap();
        let existing: swarmy_config::RemoteNode = serde_json::from_value(serde_json::json!({
            "name": "existing", "region": "us-east-1", "instance_id": "i-test",
            "public_ip": "203.0.113.1", "private_ip": "10.0.0.1",
            "key_path": "key", "launch_attempted": true, "created_at": "now",
            "launch_settings": {
                "aws": {"instance_type": "m6id.xlarge"}, "disk_gb": 100,
            },
        }))
        .unwrap();
        assert_eq!(existing.service_user(), "ubuntu");
        assert_eq!(
            provisioning_command("stack", ip, None, "us-east-1", 0, &existing).unwrap(),
            "cd ~ubuntu/swarmy && bash scripts/remote-provision.sh stack 10.0.0.1 '' us-east-1 '' '' true false 0 ubuntu ''"
        );
    }

    #[test]
    fn static_coordinates_are_quoted_positionally() {
        use swarmy_config::{BucketCredentials, BucketSpec};
        let ip = "10.0.0.1".parse().unwrap();
        let spec = BucketSpec {
            endpoint: "https://objects.example.invalid".into(),
            region: String::new(),
            bucket: "test-bucket".into(),
            prefix: "runs/team".parse().unwrap(),
            credentials: BucketCredentials::StaticKeys {
                access_key: "test-access".into(),
                secret_key: "test-secret".into(),
            },
            conditional_create: false,
        };
        // The bucket region falls back to the remote region; the create-only
        // switch and the static signal travel positionally. Keys never do.
        let command =
            provisioning_command("stack", ip, Some(&spec), "eu-west-1", 4, &node()).unwrap();
        assert_eq!(
            command,
            "cd ~swarmy/swarmy && bash scripts/remote-provision.sh stack 10.0.0.1 test-bucket eu-west-1 https://objects.example.invalid runs/team false true 4 swarmy /dev/nvme1n1"
        );
        assert!(!command.contains("test-access"));
        assert!(!command.contains("test-secret"));
    }

    // The piped host query behind `has_service_units`, run through a local
    // `bash -s` with a stubbed systemctl: the piped env text plus the query
    // must compose and report installed control units (empty means none).
    async fn piped_query(repo: &std::path::Path, bin: &std::path::Path) -> String {
        use tokio::io::AsyncWriteExt;
        let host = super::Ssh {
            repo: repo.to_owned(),
        };
        let mut script = host.piped_env().unwrap();
        assert!(!script.contains("remote-s3-env.sh"));
        // Mirror `has_service_units`: fail loudly inside the pipe.
        script.push_str("set -euo pipefail\nlist_installed_control_units\n");
        let path = format!(
            "{}:{}",
            bin.display(),
            std::env::var("PATH").unwrap_or_default()
        );
        let mut child = tokio::process::Command::new("bash")
            .arg("-s")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .env("PATH", path)
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(script.as_bytes())
            .await
            .unwrap();
        let output = child.wait_with_output().await.unwrap();
        assert!(output.status.success());
        String::from_utf8(output.stdout).unwrap()
    }

    #[tokio::test]
    async fn piped_host_query_lists_control_units() {
        use std::os::unix::fs::PermissionsExt;
        if std::process::Command::new("bash")
            .arg("--version")
            .output()
            .is_err()
        {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        let systemctl = bin.join("systemctl");
        std::fs::write(
            &systemctl,
            "#!/usr/bin/env bash\nprintf 'swarmy-tunnel.service enabled\\nswarmyd.service enabled\\nswarmy-gateway.service enabled\\n'",
        )
        .unwrap();
        std::fs::set_permissions(&systemctl, std::fs::Permissions::from_mode(0o755)).unwrap();
        let repo = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../");
        assert_eq!(
            piped_query(&repo, &bin).await.trim(),
            "swarmy-gateway.service"
        );
        std::fs::write(
            &systemctl,
            "#!/usr/bin/env bash\nprintf 'swarmy-tunnel.service enabled\\nswarmyd.service enabled\\n'",
        )
        .unwrap();
        assert!(piped_query(&repo, &bin).await.trim().is_empty());
    }

    #[tokio::test]
    async fn adopt_key_copies_the_operator_key_with_private_permissions() {
        use std::os::unix::fs::PermissionsExt;
        if std::process::Command::new("ssh-keygen")
            .arg("-h")
            .output()
            .is_err()
        {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("bootstrap");
        assert!(
            std::process::Command::new("ssh-keygen")
                .args(["-q", "-t", "ed25519", "-N", "", "-f"])
                .arg(&source)
                .status()
                .unwrap()
                .success()
        );
        let node: swarmy_config::RemoteNode = serde_json::from_value(serde_json::json!({
            "name": "demo", "region": "us-east-1", "instance_id": "",
            "launch_attempted": false,
            "public_ip": "203.0.113.10", "private_ip": "203.0.113.10",
            "key_path": dir.path().join("adopted-key"), "ssh_user": "root",
            "launch_settings": { "provider": "existing", "service_user": "swarmy" },
            "created_at": "now"
        }))
        .unwrap();
        super::adopt_key(&node, &source).await.unwrap();
        assert_eq!(
            std::fs::metadata(&node.key_path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::read(&node.key_path).unwrap(),
            std::fs::read(&source).unwrap()
        );
        let expected = std::process::Command::new("ssh-keygen")
            .args(["-y", "-f"])
            .arg(&source)
            .output()
            .unwrap();
        assert!(expected.status.success());
        assert_eq!(
            std::fs::read(node.key_path.with_extension("pub")).unwrap(),
            expected.stdout
        );
    }
}
