//! SSH helpers shared by provisioning, tunnel, log, and status commands.
use std::{
    ffi::OsStr,
    fmt::Write as _,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use crate::Result;
use crate::provision::Machine;
use tokio::io::AsyncWriteExt;
use tokio::{process::Command, time::timeout};

/// Options shared by every SSH and rsync invocation for a node. The host key
/// learned while provisioning lives next to the key so later commands verify it.
fn arguments(node: &Machine) -> Result<Vec<String>> {
    options(
        &node.ssh_user,
        &node.key_path,
        &node.key_path.with_extension("known_hosts"),
    )
}

pub(crate) fn options(user: &str, key: &Path, known_hosts: &Path) -> Result<Vec<String>> {
    // SSH passes remote commands to a shell; the user and destination must be data only.
    crate::Error::ensure(
        !user.is_empty()
            && user
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
        format!("UserKnownHostsFile={}", known_hosts.display()),
        "-i".into(),
        key.to_string_lossy().into_owned(),
        "-l".into(),
        user.to_owned(),
    ])
}

/// Service login for a node, validated so it is data, never shell.
/// Bash on the host resolves its home (`~user`); Rust never assumes
/// `/home/<user>`.
fn service_user(node: &Machine) -> Result<String> {
    let user = node.service_user().to_owned();
    swarmy_config::validate_service_user(&user)?;
    Ok(user)
}

/// Home directory resolved on the host, keeping one path implementation in
/// bash (`~user` expands through the passwd entry).
fn tilde_repo(user: &str) -> String {
    format!("~{user}/swarmy")
}

fn ssh_with(program: &OsStr, options: &[String]) -> Command {
    let mut command = Command::new(program);
    command
        .args(options)
        .stdin(Stdio::null())
        .kill_on_drop(true);
    command
}

fn base(node: &Machine) -> Result<Command> {
    Ok(ssh_with(OsStr::new("ssh"), &arguments(node)?))
}

/// A configured `ssh` command; callers append the public address and remote command.
/// The `command` in [`crate::Error::Ssh`] names the attempted operation,
/// not the remote shell fragment.
///
/// # Errors
///
/// Rejects node records whose public address is not an IP address.
pub fn command(node: &Machine) -> Result<Command> {
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
pub async fn reachable_address(node: &Machine) -> Result<String> {
    first_reachable(
        OsStr::new("ssh"),
        &arguments(node)?,
        &[node.public_ip.clone(), node.private_ip.clone()],
    )
    .await
}

pub(crate) async fn first_reachable(
    program: &OsStr,
    options: &[String],
    addresses: &[String],
) -> Result<String> {
    for address in addresses {
        let _: std::net::IpAddr = address
            .parse()
            .map_err(|source| crate::Error::context(source, "invalid instance IP"))?;
        let result = timeout(
            Duration::from_secs(7),
            ssh_with(program, options).arg(address).arg("true").output(),
        )
        .await;
        if result.is_ok_and(|output| output.is_ok_and(|output| output.status.success())) {
            return Ok(address.clone());
        }
    }
    Err(crate::Error::SshUnavailable)
}

pub(crate) async fn control_socket(
    program: &OsStr,
    socket: &Path,
    action: &str,
) -> Result<std::process::Output> {
    let output = timeout(
        Duration::from_secs(5),
        Command::new(program)
            .arg("-S")
            .arg(socket)
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

pub(crate) async fn alive(program: &OsStr, socket: &Path) -> bool {
    control_socket(program, socket, "check")
        .await
        .is_ok_and(|output| output.status.success())
}

async fn checked(command: &mut Command, action: &str) -> Result<()> {
    let status = command
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .stdout(std::io::stderr())
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
    node: &Machine,
    address: &str,
    action: &str,
    script: &str,
) -> Result<std::process::Output> {
    let child = base(node)?
        .arg(address)
        .arg("bash -s")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(crate::Error::ssh(action))?;
    feed_and_wait(child, script.as_bytes(), action).await
}

/// Write `bytes` to a spawned command's stdin, close stdin, and wait for the
/// command. Closing stdin is what lets a remote reader such as `cat` or
/// `bash -s` see end of input; a handle left open keeps both sides waiting
/// forever. The `action` names the attempted operation in [`crate::Error::Ssh`].
///
/// # Errors
///
/// Reports a missing stdin, a failed write or wait, and a non-zero exit.
pub(crate) async fn feed_and_wait(
    mut child: tokio::process::Child,
    bytes: &[u8],
    action: &str,
) -> Result<std::process::Output> {
    let Some(mut stdin) = child.stdin.take() else {
        return Err(crate::Error::other("SSH stdin missing"));
    };
    stdin
        .write_all(bytes)
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
pub async fn generate_key(key_path: &Path, comment: &str) -> Result<Vec<u8>> {
    checked(
        Command::new("ssh-keygen")
            .args(["-q", "-t", "ed25519", "-N", "", "-f"])
            .arg(key_path)
            .args(["-C", comment]),
        "generate SSH key",
    )
    .await?;
    Ok(std::fs::read(key_path.with_extension("pub"))?)
}

/// Copy an operator-owned bootstrap private key into remote state for
/// adoption and derive its public half for inspection. The state directory
/// is 0700; the copy is created with mode 0600 whatever the source's mode
/// or umask is, and the source file is left untouched.
///
/// # Errors
///
/// Reports unreadable sources and keys `ssh-keygen` cannot parse.
pub async fn adopt_key(node: &Machine, source: &Path) -> Result<()> {
    let bytes = std::fs::read(source)?;
    swarmy_config::write_private(&node.key_path, &bytes)?;
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

/// Where a joining node's primary-side steps run: over SSH from the client, or on the
/// primary itself by the provisioner. Both run the same shell commands.
#[derive(Clone, Copy)]
pub enum Primary<'a> {
    Remote(&'a Machine),
    Local(&'a LocalPrimary),
}
/// The primary as seen by a provisioner running on it.
#[derive(Clone, Debug)]
pub struct LocalPrimary {
    pub address: std::net::Ipv4Addr,
    pub service_user: String,
}
struct PrimaryShell<'a> {
    primary: Primary<'a>,
    address: Option<String>,
}
impl<'a> Primary<'a> {
    fn tunnel_address(self) -> Result<std::net::Ipv4Addr> {
        match self {
            Self::Remote(machine) => machine.private_ip.parse().map_err(|source| {
                crate::Error::context(source, "private_ip must be an IPv4 address")
            }),
            Self::Local(primary) => Ok(primary.address),
        }
    }
    fn user(self) -> Result<String> {
        match self {
            Self::Remote(machine) => service_user(machine),
            Self::Local(primary) => {
                swarmy_config::validate_service_user(&primary.service_user)?;
                Ok(primary.service_user.clone())
            }
        }
    }
    async fn shell(self, attempts: u32) -> Result<PrimaryShell<'a>> {
        let address = match self {
            Self::Remote(machine) => Some(wait_ssh_for(machine, attempts).await?),
            Self::Local(_) => None,
        };
        Ok(PrimaryShell {
            primary: self,
            address,
        })
    }
}
impl PrimaryShell<'_> {
    fn command(&self, remote: &str) -> Result<Command> {
        match self.primary {
            Primary::Remote(machine) => {
                let mut command = base(machine)?;
                command
                    .arg(
                        self.address
                            .as_deref()
                            .expect("remote primary has SSH address"),
                    )
                    .arg(remote);
                Ok(command)
            }
            Primary::Local(_) => {
                let mut command = Command::new("sh");
                command
                    .args(["-c", remote])
                    .stdin(Stdio::null())
                    .kill_on_drop(true);
                Ok(command)
            }
        }
    }
    async fn output(&self, remote: &str, action: &str) -> Result<std::process::Output> {
        run_output(&mut self.command(remote)?, action).await
    }
    async fn checked(&self, remote: &str, action: &str) -> Result<()> {
        checked(&mut self.command(remote)?, action).await
    }
}

/// Where a node's swarmy binaries come from.
pub enum Binaries {
    /// Verified release binaries cached locally.
    Release {
        release: crate::release::Release,
        cache: PathBuf,
    },
    /// The build this process runs: swarmy-core beside the current executable, and the
    /// libfdb_c.so this process loaded. The provisioner uploads it to nodes it joins.
    Installed { core: PathBuf, library: PathBuf },
    /// Development checkout copied into the node's source directory.
    Build { checkout: PathBuf },
}

impl Binaries {
    /// Discover the running primary's core and loaded FoundationDB library.
    /// # Errors
    /// Missing swarmy-core beside the executable, or no loaded libfdb_c.so.
    pub fn installed() -> Result<Self> {
        Self::installed_from(
            &std::env::current_exe()?,
            &std::fs::read_to_string("/proc/self/maps")?,
        )
    }
    fn installed_from(exe: &Path, maps: &str) -> Result<Self> {
        let core = exe
            .parent()
            .ok_or_else(|| crate::Error::other("executable has no parent"))?
            .join("swarmy-core");
        crate::Error::ensure(
            core.is_file(),
            format!("missing swarmy-core at {}", core.display()),
        )?;
        let library = maps
            .lines()
            .filter_map(|line| line.split_whitespace().last())
            .find(|path| Path::new(path).file_name() == Some(OsStr::new("libfdb_c.so")))
            .ok_or_else(|| crate::Error::other("this process has no libfdb_c.so loaded"))?;
        Ok(Self::Installed {
            core,
            library: library.into(),
        })
    }
}

/// Provisions nodes from bundled files and optional binaries.
pub struct Ssh {
    binaries: Option<Binaries>,
}

/// Find the nearest checkout containing the provisioning script.
///
/// # Errors
/// Explains how to supply release binaries when outside a checkout.
pub fn find_checkout(cwd: &Path) -> Result<PathBuf> {
    cwd.ancestors()
        .find(|p| p.join(crate::node_files::PROVISION).is_file())
        .map(Path::to_owned)
        .ok_or_else(|| {
            crate::Error::other("this development build of swarmy has no release binaries for the node: run it inside a swarmy checkout so the node can build from it, or set SWARMY_RELEASE_BASE_URL (see docs/REMOTE.md)")
        })
}

impl Ssh {
    /// Select matching release binaries, or build from a checkout.
    /// # Errors
    /// Returns release discovery and checkout failures.
    pub fn for_client(cache: &Path) -> Result<Self> {
        let binaries = if let Some(release) = crate::release::Release::for_client()? {
            cloud_out!(
                "Using swarmy {} release binaries; nothing is built on the node",
                release.tag()
            );
            Binaries::Release {
                release,
                cache: cache.to_owned(),
            }
        } else {
            let checkout = find_checkout(&std::env::current_dir()?)?;
            cloud_out!(
                "Development client {}: the node builds swarmy-core from {} (copied to <node dir>/source)",
                swarmy_version::IDENTITY,
                checkout.display()
            );
            Binaries::Build { checkout }
        };
        Ok(Self::new(binaries))
    }
    /// Open a provisioning handle.
    #[must_use]
    pub const fn new(binaries: Binaries) -> Self {
        Self {
            binaries: Some(binaries),
        }
    }
    /// Open a handle that cannot provision.
    #[must_use]
    pub const fn teardown() -> Self {
        Self { binaries: None }
    }

    async fn node_target(node: &Machine, address: &str) -> Result<crate::release::Target> {
        let output = run_output(
            base(node)?.arg(address).arg("uname -m"),
            "read node architecture",
        )
        .await?;
        crate::release::Target::from_uname(String::from_utf8(output.stdout)?.trim())
    }

    async fn upload_release(
        node: &Machine,
        address: &str,
        node_dir: &str,
        release: &crate::release::Release,
        cache: &Path,
        stack: bool,
    ) -> Result<()> {
        let target = Self::node_target(node, address).await?;
        cloud_out!(
            "Uploading swarmy {} release binaries for {}",
            release.tag(),
            target.triple()
        );
        let core = release
            .ensure(cache, crate::release::Component::Core, target)
            .await?;
        let mut sources = vec![core.join("swarmy-core"), core.join("libfdb_c.so")];
        if stack {
            sources.push(core.join("swarmy-provisioner"));
            let client = release
                .ensure(cache, crate::release::Component::Client, target)
                .await?;
            sources.push(client.join("swarmy"));
        }
        Self::upload_bundle(node, address, node_dir, &sources).await
    }

    async fn upload_installed(
        node: &Machine,
        address: &str,
        node_dir: &str,
        core: &Path,
        library: &Path,
    ) -> Result<()> {
        let target = Self::node_target(node, address).await?;
        let host = crate::release::Target::host()?;
        crate::Error::ensure(
            target == host,
            format!(
                "node {} is {}, but this primary runs {}; the provisioner uploads its own build",
                node.name,
                target.triple(),
                host.triple()
            ),
        )?;
        cloud_out!("Uploading this primary's swarmy-core build");
        Self::upload_bundle(
            node,
            address,
            node_dir,
            &[core.to_owned(), library.to_owned()],
        )
        .await
    }

    async fn upload_bundle(
        node: &Machine,
        address: &str,
        node_dir: &str,
        sources: &[PathBuf],
    ) -> Result<()> {
        let mut transport = vec!["ssh".to_owned()];
        transport.extend(arguments(node)?);
        checked(
            Command::new("rsync")
                .args(["-a", "--mkpath", "-e"])
                .arg(shell_words::join(transport))
                .args(sources)
                .arg(format!("{address}:{node_dir}/.swarmy/release/")),
            "upload release binaries",
        )
        .await?;
        let user = service_user(node)?;
        checked(
            base(node)?.arg(address).arg(format!(
                "sudo chown {user}:{user} {} && sudo chown -R {user}:{user} {}",
                shell_words::quote(&format!("{node_dir}/.swarmy")),
                shell_words::quote(&format!("{node_dir}/.swarmy/release")),
            )),
            "hand release binaries to service user",
        )
        .await?;
        Ok(())
    }

    /// Run the embedded `ensure_service_user` on the host before the first
    /// copy, so a root login gains a destination owned by its owner. The helper
    /// script is piped over stdin because the node directory is not on the host yet;
    /// user creation, sudoers, and path resolution stay in the one bash
    /// implementation instead of being repeated in Rust. Returns the
    /// host-resolved home for the rsync destination; every later command
    /// resolves the same home through `~user` on the host instead of assuming
    /// `/home/<user>`.
    async fn ensure_service_user(
        &self,
        node: &Machine,
        address: &str,
        user: &str,
    ) -> Result<String> {
        if user == "root" {
            return Ok("/root".to_owned());
        }
        let mut script = piped_env();
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

    async fn prepare_host(&self, node: &Machine, address: &str) -> Result<String> {
        // Dedicated servers boot without cloud-init, so wait only when it is present.
        checked(base(node)?.arg(address)
            .arg("command -v rsync >/dev/null || (if command -v cloud-init >/dev/null 2>&1; then sudo cloud-init status --wait; fi; sudo apt-get update && sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y rsync)"), "prepare remote rsync").await?;
        let home = self
            .ensure_service_user(node, address, &service_user(node)?)
            .await?;
        Ok(format!("{home}/swarmy"))
    }

    async fn stage(&self, node: &Machine, address: &str, stack: bool) -> Result<bool> {
        let Some(binaries) = &self.binaries else {
            return Err(crate::Error::other(
                "this SSH host was opened for teardown and cannot provision",
            ));
        };
        let node_dir = self.prepare_host(node, address).await?;
        upload_node_files(node, address, &node_dir).await?;
        match binaries {
            Binaries::Release { release, cache } => {
                Self::upload_release(node, address, &node_dir, release, cache, stack).await?;
                Ok(true)
            }
            Binaries::Installed { core, library } => {
                crate::Error::ensure(!stack, "installed binaries join sandbox nodes only")?;
                Self::upload_installed(node, address, &node_dir, core, library).await?;
                Ok(true)
            }
            Binaries::Build { checkout } => {
                copy_source(node, address, &node_dir, checkout).await?;
                Ok(false)
            }
        }
    }

    /// Report uncommitted paths while ignoring Python bytecode caches.
    ///
    /// # Errors
    ///
    /// Reports git failures and non-UTF-8 free output is lossy-decoded, never an error.
    pub fn checkout_changes(&self) -> Result<Vec<String>> {
        let Some(Binaries::Build { checkout }) = &self.binaries else {
            return Ok(Vec::new());
        };
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(checkout)
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
    pub async fn has_service_units(&self, node: &Machine, address: &str) -> Result<bool> {
        let mut script = piped_env();
        // Fail loudly inside the piped script, like decommission does: the
        // query must error when systemctl fails, never look like no units.
        script.push_str("set -euo pipefail\nlist_installed_control_units\n");
        let output = pipe_script(node, address, "inspect installed service units", &script).await?;
        Ok(!String::from_utf8(output.stdout)?.trim().is_empty())
    }

    /// Read the installed `swarmy-core` version from a node.
    ///
    /// # Errors
    ///
    /// Reports SSH failures and non-zero remote exits.
    pub async fn node_version(&self, node: &Machine, address: &str) -> Result<String> {
        let output = run_output(
            base(node)?
                .arg(address)
                // A node provisioned before swarmy-core still has the per-role
                // binaries; report their version so upgrade can replace them.
                .arg(
                    "/usr/local/bin/swarmy-core --version 2>/dev/null \
                     || /usr/local/bin/swarmyd --version 2>/dev/null \
                     || echo 'swarmy-core not installed'",
                ),
            "read node version",
        )
        .await?;
        Ok(String::from_utf8(output.stdout)?.trim().to_owned())
    }

    /// Upgrade a node: install changed binaries, rewrite unit files that differ
    /// from the provisioning text, and restart service units whose unit file
    /// or running executable changed.
    ///
    /// # Errors
    ///
    /// Reports SSH failures, dirty checkouts without `--allow-dirty`, and
    /// remote build or restart failures.
    pub async fn upgrade(
        &self,
        node: &Machine,
        address: &str,
        services_only: bool,
        drain_timeout: Duration,
        stack: bool,
    ) -> Result<serde_json::Value> {
        let prebuilt = self.stage(node, address, stack).await?;
        let mode = if stack { "stack" } else { "node" };
        let services = if services_only {
            "services-only"
        } else {
            "all"
        };
        let user = service_user(node)?;
        // Bash resolves the node directory through `~user`; Rust passes only the login.
        let script = format!(
            "cd {} && bash {} {mode} {services} {} {}",
            tilde_repo(&user),
            crate::node_files::UPGRADE,
            drain_timeout.as_secs(),
            if prebuilt { "prebuilt" } else { "build" }
        );
        let mut remote = base(node)?;
        remote.arg(address).arg(script).stderr(Stdio::inherit());
        let output = run_output(&mut remote, "upgrade host").await?;
        serde_json::from_slice(&output.stdout)
            .map_err(|source| crate::Error::context(source, "parse the host upgrade summary"))
    }

    /// Upload node files and run the provisioning script; returns the reachable address.
    ///
    /// # Errors
    ///
    /// Reports unreachable hosts and provisioning script failures.
    pub async fn provision(&self, node: &Machine, primary: Option<Primary<'_>>) -> Result<String> {
        let address = wait_ssh(node).await?;
        let prebuilt = self.stage(node, &address, primary.is_none()).await?;
        if let Some(id) = node.node_id {
            checked(
                base(node)?.arg(&address).arg(node_id_command(node, id)?),
                "write node id",
            )
            .await?;
        }
        let service_ip: std::net::Ipv4Addr = match primary {
            Some(primary) => primary.tunnel_address()?,
            None => node.private_ip.parse().map_err(|source| {
                crate::Error::context(source, "private_ip must be an IPv4 address")
            })?,
        };
        let mode = if let Some(primary) = primary {
            let shell = primary.shell(90).await?;
            let cluster = shell
                .output(
                    &read_cluster_command(&primary.user()?),
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
            install_tunnel(node, &address, primary, &shell).await?;
            "node"
        } else {
            "stack"
        };
        if prebuilt {
            cloud_out!("Provisioning node from release binaries");
        } else {
            cloud_out!(
                "Provisioning node and building release binaries (this takes several minutes)"
            );
        }
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
                    node.region(),
                    node.sandboxes,
                    node,
                    prebuilt,
                )?
            }),
            "provision remote node",
        )
        .await?;
        Ok(address)
    }

    /// Stop swarmy services on an adopted host and remove its units,
    /// binaries, node environment, and node directory. Pipes the embedded
    /// `decommission_probe` over stdin (so no checkout is needed on the
    /// host), which runs the shared teardown when the node directory is present,
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
    pub async fn decommission(&self, node: &Machine) -> Result<()> {
        let address = wait_ssh_for(node, 6).await?;
        let user = service_user(node)?;
        let mut script = piped_env();
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

fn piped_env() -> String {
    crate::node_files::REMOTE_PROVISION_ENV
        .lines()
        .filter(|line| {
            !(line.trim_start().starts_with("source") && line.contains("remote-s3-env.sh"))
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n"
}

fn node_files_rsync_args(source: &Path, destination: &str) -> Vec<String> {
    [
        "-rlpt",
        "--delete",
        "--mkpath",
        "--include=/scripts/***",
        "--include=/images/***",
        "--exclude=*",
    ]
    .into_iter()
    .map(str::to_owned)
    .chain([format!("{}/", source.display()), format!("{destination}/")])
    .collect()
}

async fn upload_node_files(node: &Machine, address: &str, node_dir: &str) -> Result<()> {
    let temp = tempfile::tempdir()?;
    crate::node_files::write_to(temp.path())?;
    cloud_out!(
        "Uploading {} node files (scripts and image recipes)",
        crate::node_files::FILES.len()
    );
    let mut transport = vec!["ssh".to_owned()];
    transport.extend(arguments(node)?);
    let args = node_files_rsync_args(temp.path(), &format!("{address}:{node_dir}"));
    let split = args.len() - 2;
    checked(
        Command::new("rsync")
            .args(&args[..split])
            .arg("-e")
            .arg(shell_words::join(transport))
            .args(&args[split..]),
        "upload node files",
    )
    .await?;
    let user = service_user(node)?;
    checked(
        base(node)?.arg(address).arg(format!(
            "sudo chown {user}:{user} {} && sudo chown -R {user}:{user} {} {}",
            shell_words::quote(node_dir),
            shell_words::quote(&format!("{node_dir}/scripts")),
            shell_words::quote(&format!("{node_dir}/images"))
        )),
        "hand node files to service user",
    )
    .await
}

async fn copy_source(node: &Machine, address: &str, node_dir: &str, checkout: &Path) -> Result<()> {
    cloud_out!(
        "Copying {} to {node_dir}/source for the on-node build",
        checkout.display()
    );
    let user = service_user(node)?;
    let mut transport = vec!["ssh".to_owned()];
    transport.extend(arguments(node)?);
    let settings = swarmy_config::Settings::load_base()?.settings;
    let credential = &settings.selection.credential_file;
    let mut copy = Command::new("rsync");
    for relative in
        credential_excludes(checkout, credential)
            .into_iter()
            .chain(credential_excludes(
                checkout,
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
            .arg(format!("{}/", checkout.display()))
            .arg(format!("{address}:{node_dir}/source/")),
        "copy checkout with rsync",
    )
    .await?;
    // The copy excludes .git, so record its commit for the build's identity;
    // otherwise the node reports an unknown commit and the version guard
    // refuses every client.
    let commit = source_commit(checkout).await?;
    let marker = format!("{node_dir}/source/.swarmy-source-commit");
    checked(
        base(node)?.arg(address).arg(format!(
            "printf '%s\\n' {} > {}",
            shell_words::quote(&commit),
            shell_words::quote(&marker)
        )),
        "record copied checkout commit",
    )
    .await?;
    // A root bootstrap login owns the copied tree; hand it to the service user.
    // The user already exists from the bootstrap, so the chown cannot fail
    // on a missing login.
    checked(
        base(node)?.arg(address).arg(format!(
            "sudo chown -R {user}:{user} {}",
            shell_words::quote(&format!("{node_dir}/source"))
        )),
        "hand copied checkout to service user",
    )
    .await?;
    Ok(())
}

/// The short commit of the checkout a development client copies to a node.
async fn source_commit(checkout: &Path) -> Result<String> {
    let output = run_output(
        Command::new("git")
            .arg("-C")
            .arg(checkout)
            .args(["rev-parse", "--short=12", "HEAD"]),
        "read checkout commit",
    )
    .await?;
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
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
    node: &Machine,
    prebuilt: bool,
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
        "cd {} && bash {} {mode} {service_ip} {} {} {} {} {conditional_create} {static_keys} {sandboxes} {} {} {prebuilt}",
        tilde_repo(&user),
        crate::node_files::PROVISION,
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
pub async fn build_image_tagged(
    node: &Machine,
    address: &str,
    recipe: &Path,
    tag: &str,
) -> Result<()> {
    checked(
        base(node)?
            .arg(address)
            .arg(image_build_command(&service_user(node)?, recipe, tag)?),
        "build and register remote image",
    )
    .await
}

/// Read the installed services token rather than assuming the uploaded value.
/// # Errors
/// Reports SSH and configuration failures.
pub async fn api_token(node: &Machine, address: &str) -> Result<String> {
    let user = service_user(node)?;
    let output = run_output(
        base(node)?
            .arg(address)
            .arg(format!("cat ~{user}/swarmy/.swarmy/config.toml")),
        "read the swarm API token",
    )
    .await?;
    let config: toml::Value = toml::from_str(&String::from_utf8(output.stdout)?)?;
    config
        .get("api")
        .and_then(|api| api.get("token"))
        .and_then(toml::Value::as_str)
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| {
            crate::Error::other("the services host has no API token in its configuration")
        })
}

async fn run_piped(node: &Machine, action: &str, call: &str) -> Result<()> {
    let address = wait_ssh_for(node, 6).await?;
    let mut script = piped_env();
    script.push_str("set -euo pipefail\n");
    script.push_str(call);
    script.push('\n');
    pipe_script(node, &address, action, &script).await?;
    Ok(())
}
/// Stop and disable installed units.
/// # Errors
/// Reports SSH and unit failures.
pub async fn stop_units(node: &Machine) -> Result<()> {
    run_piped(node, "stop swarmy units", "stop_installed_units").await
}
/// Enable and start installed units.
/// # Errors
/// Reports SSH and unit failures.
pub async fn start_units(node: &Machine) -> Result<()> {
    run_piped(node, "start swarmy units", "start_installed_units").await
}

/// Remove a destroyed machine's address from the group's shared known-hosts file.
/// `ssh-keygen -R` also removes hashed entries, which a text filter cannot find.
/// # Errors
/// Reports a failed `ssh-keygen` invocation; callers may handle it as best effort.
pub fn forget_host_key(address: &str, key: &Path) -> Result<()> {
    let known_hosts = key.with_extension("known_hosts");
    if !known_hosts.is_file() {
        return Ok(());
    }
    let status = std::process::Command::new("ssh-keygen")
        .args(["-R", address, "-f"])
        .arg(&known_hosts)
        .status()?;
    crate::Error::ensure(
        status.success(),
        format!("ssh-keygen -R failed with {status}"),
    )
}

/// Result of waiting for a node and stopping its agent.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize)]
pub struct Drain {
    pub stopped: bool,
    pub busy: Vec<String>,
    pub unreleased: Vec<String>,
}

/// Drain a node, then cleanly stop its agent.
/// # Errors
/// SSH failures, a non-zero helper exit, or a missing or invalid drain line.
pub async fn drain_node(node: &Machine, timeout: Duration, force: bool) -> Result<Drain> {
    let address = wait_ssh_for(node, 6).await?;
    let script = piped_env()
        + &format!(
            "set -euo pipefail\ndrain_node_agent {} {} {}\n",
            shell_words::quote(&service_user(node)?),
            timeout.as_secs(),
            if force { "force" } else { "wait" }
        );
    let output = pipe_script(node, &address, "drain node", &script).await?;
    let text = String::from_utf8(output.stdout)?;
    let line = text
        .lines()
        .rev()
        .find_map(|line| line.strip_prefix("drain "))
        .ok_or_else(|| crate::Error::other("drain node returned no drain line"))?;
    Ok(serde_json::from_str(line)?)
}

/// Remove a node's tunnel key from the primary's service-user `authorized_keys`.
/// # Errors
/// SSH failures, or a node without a tunnel key.
pub async fn revoke_tunnel(node: &Machine, primary: Primary<'_>) -> Result<()> {
    let user = service_user(node)?;
    let key = run_output(
        base(node)?
            .arg(wait_ssh_for(node, 6).await?)
            .arg(format!("cat {}/.swarmy/tunnel-key.pub", tilde_repo(&user))),
        "read joining node tunnel key",
    )
    .await?;
    let authorization = tunnel_authorization(&String::from_utf8(key.stdout)?)?;
    let shell = primary.shell(6).await?;
    shell
        .checked(
            &authorized_keys_command(&primary.user()?, &authorization, true)?,
            "revoke joining node tunnel key",
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
    // The client chooses its API from the environment, not from config files,
    // so export the node's own API address and token. They are read on the
    // host, so the token never appears in an SSH command line.
    let api = shell_words::quote(NODE_API_ENV);
    Ok(format!(
        "cd {} && sudo -n bash -c {}",
        tilde_repo(user),
        shell_words::quote(&format!(
            "set -e; set -a; . /etc/swarmy/node.env; set +a; eval \"$(python3 -c {api})\"; exec {build}"
        ))
    ))
}

/// Prints shell exports of the node's API URL and token from `.swarmy/config.toml`.
const NODE_API_ENV: &str = "import shlex, tomllib\n\
api = tomllib.load(open('.swarmy/config.toml', 'rb')).get('api', {})\n\
host, port = api.get('listen', '127.0.0.1:8742').rsplit(':', 1)\n\
host = '127.0.0.1' if host in ('', '0.0.0.0', '::', '[::]') else host\n\
print('export SWARMY_API_URL=' + shlex.quote(api.get('url') or 'http://' + host + ':' + port))\n\
print('export SWARMY_API_TOKEN=' + shlex.quote(api.get('token', '')))";

/// Remote commands built synchronously so their temporaries never inflate
/// the provisioning futures. Each validates the service user first and
/// resolves its home through `~user` on the host, keeping one path
/// implementation in bash.
fn read_cluster_command(user: &str) -> String {
    format!("cat {}/.dev/fdb.cluster", tilde_repo(user))
}

fn copy_cluster_command(node: &Machine, cluster: &str) -> Result<String> {
    let user = service_user(node)?;
    let repo = tilde_repo(&user);
    Ok(format!(
        "mkdir -p {repo}/.dev && printf %s {} > {repo}/.dev/fdb.cluster && chown -R {user}:{user} {repo}/.dev",
        shell_words::quote(cluster),
    ))
}

fn node_id_command(node: &Machine, id: swarmy_core::NodeId) -> Result<String> {
    let user = service_user(node)?;
    let repo = tilde_repo(&user);
    Ok(format!(
        "umask 077; mkdir -p {repo}/.swarmy && printf %s {id} > {repo}/.swarmy/node-id && chown {user}:{user} {repo}/.swarmy {repo}/.swarmy/node-id"
    ))
}

fn tunnel_key_command(node: &Machine) -> Result<String> {
    let user = service_user(node)?;
    let repo = tilde_repo(&user);
    Ok(format!(
        "umask 077; mkdir -p {repo}/.swarmy; chown -R {user}:{user} {repo}/.swarmy; test -f {repo}/.swarmy/tunnel-key || sudo -u {user} ssh-keygen -q -t ed25519 -N '' -f {repo}/.swarmy/tunnel-key; chown {user}:{user} {repo}/.swarmy/tunnel-key {repo}/.swarmy/tunnel-key.pub; cat {repo}/.swarmy/tunnel-key.pub",
    ))
}

fn pin_primary_key_command(node: &Machine, known_host: &str) -> Result<String> {
    let user = service_user(node)?;
    let repo = tilde_repo(&user);
    Ok(format!(
        "umask 077; printf '%s\\n' {} > {repo}/.swarmy/tunnel-known-hosts && chown {user}:{user} {repo}/.swarmy/tunnel-known-hosts",
        shell_words::quote(known_host),
    ))
}

// The service user's authorized_keys lives at its account home, not at the
// bootstrap login's home. Both installation and revocation use this path.
fn authorized_keys_command(user: &str, authorization: &str, revoke: bool) -> Result<String> {
    let home = format!("~{user}");
    let file = format!("{home}/.ssh/authorized_keys");
    let line = shell_words::quote(authorization);
    if revoke {
        // Grep exit 1 means no matching line; exit 2 must never replace the file.
        let inner = format!(
            "umask 077; grep -vxF -- {line} {file} > {file}.tmp; status=$?; if [ \"$status\" -le 1 ]; then mv {file}.tmp {file}; else rm -f {file}.tmp; exit \"$status\"; fi"
        );
        Ok(format!(
            "sudo -u {user} sh -c {}",
            shell_words::quote(&inner)
        ))
    } else {
        Ok(format!(
            "service_user={user}; service_home={home}; umask 077; sudo -u $service_user mkdir -p $service_home/.ssh && sudo -u $service_user touch $service_home/.ssh/authorized_keys && (sudo -u $service_user grep -qxF -- {line} $service_home/.ssh/authorized_keys || printf '%s\\n' {line} | sudo -u $service_user tee -a $service_home/.ssh/authorized_keys >/dev/null)"
        ))
    }
}

// Generate the forwarding key on its owner; the primary login key never leaves the client.
async fn install_tunnel(
    node: &Machine,
    address: &str,
    primary: Primary<'_>,
    shell: &PrimaryShell<'_>,
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
    shell
        .checked(
            &authorized_keys_command(&primary.user()?, &authorization, false)?,
            "authorize joining node tunnel",
        )
        .await?;
    // Obtain the host key over the already authenticated provisioning connection.
    let host_key = shell
        .output(
            "cat /etc/ssh/ssh_host_ed25519_key.pub",
            "read primary SSH host key",
        )
        .await?;
    let host_key = String::from_utf8(host_key.stdout)?;
    let known_host = format!("{} {}", primary.tunnel_address()?, host_key.trim());
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

async fn wait_ssh(node: &Machine) -> Result<String> {
    wait_ssh_for(node, 90).await
}

/// Wait for SSH with an explicit attempt budget (five seconds between
/// passes over both addresses). Provisioning waits the full budget while a
/// new machine boots; `down` passes a short budget so a cancelled server
/// cannot block teardown for minutes per host.
async fn wait_ssh_for(node: &Machine, attempts: u32) -> Result<String> {
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
    #![deny(clippy::disallowed_methods)]
    use super::{Ssh, feed_and_wait, image_build_command, tunnel_authorization};

    /// The fed input must reach end of file: a reader such as the remote
    /// `cat` that stores bucket keys returns only once stdin is closed.
    #[tokio::test]
    async fn fed_input_reaches_end_of_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("keys.env");
        let child = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(format!("cat > {}", path.display()))
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            feed_and_wait(child, b"KEY=value\n", "write keys"),
        )
        .await
        .expect("the reader saw end of input and exited")
        .unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"KEY=value\n");
    }

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
        let changes = Ssh::new(super::Binaries::Build {
            checkout: dir.path().to_owned(),
        })
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
    fn private_address_command_configures_only_unaddressed_interfaces() {
        let command = super::private_address_command("10.0.0.2").unwrap();
        let outer = shell_words::split(&command).unwrap();
        assert_eq!(&outer[..3], ["sudo", "-n", "sh"]);
        let script = &outer[4];
        assert!(script.contains(r#"grep -q " inet 10.0.0.2/""#));
        assert!(script.contains("grep -q inet && continue"));
        assert!(script.contains("netplan apply"));
        assert!(super::private_address_command("10.0.0.2; reboot").is_err());
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
        assert!(script.starts_with("set -e; set -a; . /etc/swarmy/node.env; set +a; eval "));
        assert!(script.contains("SWARMY_API_TOKEN"));
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
    fn find_checkout_walks_up_and_explains_the_release_override() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path().join("repo");
        std::fs::create_dir_all(repo.join("scripts")).unwrap();
        std::fs::create_dir_all(repo.join("a/b")).unwrap();
        std::fs::write(repo.join(crate::node_files::PROVISION), "").unwrap();
        assert_eq!(super::find_checkout(&repo.join("a/b")).unwrap(), repo);
        assert!(
            super::find_checkout(tmp.path())
                .unwrap_err()
                .to_string()
                .contains("SWARMY_RELEASE_BASE_URL")
        );
    }

    #[test]
    fn node_files_upload_replaces_scripts_and_images_only() {
        if std::process::Command::new("rsync")
            .arg("--version")
            .output()
            .is_err()
        {
            eprintln!("skipping node files rsync test: rsync is not installed");
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        let dest = tmp.path().join("dest");
        crate::node_files::write_to(&src).unwrap();
        for path in [
            "scripts/stale.sh",
            "images/old/recipe.toml",
            ".dev/fdb.cluster",
            ".swarmy/config.toml",
            "source/Cargo.toml",
            "crates/legacy.rs",
        ] {
            let file = dest.join(path);
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(file, "fixture").unwrap();
        }
        assert!(
            std::process::Command::new("rsync")
                .args(super::node_files_rsync_args(&src, dest.to_str().unwrap()))
                .status()
                .unwrap()
                .success()
        );
        for path in ["scripts/stale.sh", "images/old/recipe.toml"] {
            assert!(!dest.join(path).exists());
        }
        for path in [
            ".dev/fdb.cluster",
            ".swarmy/config.toml",
            "source/Cargo.toml",
            "crates/legacy.rs",
        ] {
            assert!(dest.join(path).exists());
        }
        for &(path, text) in crate::node_files::FILES {
            assert_eq!(std::fs::read_to_string(dest.join(path)).unwrap(), text);
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
    #![deny(clippy::disallowed_methods)]
    use super::provisioning_command;

    fn node() -> super::Machine {
        super::Machine {
            name: "demo".into(),
            public_ip: "203.0.113.1".into(),
            private_ip: "10.0.0.1".into(),
            key_path: "key".into(),
            ssh_user: "root".into(),
            service_user: "swarmy".into(),
            local_storage: "/dev/nvme1n1".into(),
            sandboxes: 4,
            bucket: None,
            services: true,
            node_id: None,
        }
    }

    #[test]
    fn primary_steps_run_over_ssh_or_on_this_host() {
        let remote = node();
        let remote_shell = super::PrimaryShell {
            primary: super::Primary::Remote(&remote),
            address: Some("10.0.0.1".into()),
        };
        let command = remote_shell.command("echo ok").unwrap();
        let args = command
            .as_std()
            .get_args()
            .map(|arg| arg.to_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(command.as_std().get_program(), "ssh");
        assert_eq!(&args[args.len() - 2..], &["10.0.0.1", "echo ok"]);
        let local = super::LocalPrimary {
            address: "192.0.2.1".parse().unwrap(),
            service_user: "swarmy".into(),
        };
        let shell = super::PrimaryShell {
            primary: super::Primary::Local(&local),
            address: None,
        };
        let command = shell.command("echo ok").unwrap();
        assert_eq!(command.as_std().get_program(), "sh");
        assert_eq!(
            command.as_std().get_args().collect::<Vec<_>>(),
            ["-c", "echo ok"]
        );
    }

    #[test]
    fn local_primary_tunnels_to_its_given_address() {
        let local = super::LocalPrimary {
            address: "192.0.2.1".parse().unwrap(),
            service_user: "swarmy".into(),
        };
        assert_eq!(
            super::Primary::Local(&local).tunnel_address().unwrap(),
            local.address
        );
    }

    #[test]
    fn node_id_command_writes_the_persisted_id_file() {
        let id = swarmy_core::NodeId::from_ulid(ulid::Ulid::generate());
        let text = super::node_id_command(&node(), id).unwrap();
        let words = shell_words::split(&text).unwrap();
        assert!(
            words
                .iter()
                .any(|word| word == "~swarmy/swarmy/.swarmy/node-id")
        );
        assert!(words.iter().any(|word| word == &id.to_string()));
    }

    #[test]
    fn installed_binaries_come_from_the_executable_dir_and_loaded_library() {
        let dir = tempfile::tempdir().unwrap();
        let exe = dir.path().join("swarmy-provisioner");
        assert!(
            super::Binaries::installed_from(&exe, "")
                .err()
                .unwrap()
                .to_string()
                .contains("swarmy-core")
        );
        std::fs::write(dir.path().join("swarmy-core"), "binary").unwrap();
        assert!(
            super::Binaries::installed_from(&exe, "")
                .err()
                .unwrap()
                .to_string()
                .contains("no libfdb_c.so loaded")
        );
        let maps = "00400000-00452000 r-xp 00000000 00:00 0 /opt/x/libfdb_c.so";
        let super::Binaries::Installed { core, library } =
            super::Binaries::installed_from(&exe, maps).unwrap()
        else {
            panic!("expected installed binaries")
        };
        assert_eq!(core, dir.path().join("swarmy-core"));
        assert_eq!(library, std::path::Path::new("/opt/x/libfdb_c.so"));
    }

    #[test]
    fn revoke_only_moves_authorized_keys_after_a_successful_grep() {
        let command =
            super::authorized_keys_command("swarmy", "ssh-ed25519 AAAA test", true).unwrap();
        let words = shell_words::split(&command).unwrap();
        assert_eq!(&words[..4], &["sudo", "-u", "swarmy", "sh"]);
        let inner = words.last().unwrap();
        assert!(inner.starts_with("umask 077; grep -vxF"));
        assert!(inner.contains("status=$?; if [ \"$status\" -le 1 ]; then mv"));
        assert!(inner.contains("else rm -f"));
        assert!(inner.contains("~swarmy/.ssh/authorized_keys"));
    }

    #[test]
    fn sandbox_limit_is_passed_to_both_node_modes() {
        let ip = "10.0.0.1".parse().unwrap();
        assert_eq!(
            provisioning_command("stack", ip, None, "us-east-1", 0, &node(), false).unwrap(),
            "cd ~swarmy/swarmy && bash scripts/remote-provision.sh stack 10.0.0.1 '' us-east-1 '' '' true false 0 swarmy /dev/nvme1n1 false"
        );
        assert_eq!(
            provisioning_command("node", ip, None, "us-east-1", 4, &node(), false).unwrap(),
            "cd ~swarmy/swarmy && bash scripts/remote-provision.sh node 10.0.0.1 '' us-east-1 '' '' true false 4 swarmy /dev/nvme1n1 false"
        );
    }

    #[test]
    fn prebuilt_is_passed_as_twelfth_argument() {
        let ip = "10.0.0.1".parse().unwrap();
        assert!(
            provisioning_command("node", ip, None, "us-east-1", 4, &node(), true)
                .unwrap()
                .ends_with(" /dev/nvme1n1 true")
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
            provisioning_command("stack", ip, Some(&spec), "eu-west-1", 4, &node(), false).unwrap();
        assert_eq!(
            command,
            "cd ~swarmy/swarmy && bash scripts/remote-provision.sh stack 10.0.0.1 test-bucket eu-west-1 https://objects.example.invalid runs/team false true 4 swarmy /dev/nvme1n1 false"
        );
        assert!(!command.contains("test-access"));
        assert!(!command.contains("test-secret"));
    }

    // The piped host query behind `has_service_units`, run through a local
    // `bash -s` with a stubbed systemctl: the piped env text plus the query
    // must compose and report installed control units (empty means none).
    async fn piped_query(bin: &std::path::Path) -> String {
        use tokio::io::AsyncWriteExt;
        let mut script = super::piped_env();
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
        assert_eq!(piped_query(&bin).await.trim(), "swarmy-gateway.service");
        std::fs::write(
            &systemctl,
            "#!/usr/bin/env bash\nprintf 'swarmy-tunnel.service enabled\\nswarmyd.service enabled\\n'",
        )
        .unwrap();
        assert!(piped_query(&bin).await.trim().is_empty());
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
        let mut node = node();
        node.key_path = dir.path().join("adopted-key");
        node.public_ip = "203.0.113.10".into();
        node.private_ip = node.public_ip.clone();
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

/// Wait for SSH and cloud-init to finish on a fresh machine.
/// # Errors
/// Returns SSH and cloud-init failures.
pub async fn wait_boot(node: &Machine) -> Result<()> {
    let address = wait_ssh(node).await?;
    run_output(base(node)?.arg(&address).arg("command -v cloud-init >/dev/null 2>&1 || exit 0; cloud-init status --wait >/dev/null 2>&1; rc=$?; [ $rc -eq 0 ] || [ $rc -eq 2 ]"), "wait for cloud-init").await?;
    if node.private_ip != node.public_ip {
        run_output(
            base(node)?
                .arg(&address)
                .arg(private_address_command(&node.private_ip)?),
            "bring up the private network address",
        )
        .await?;
    }
    Ok(())
}

/// Make sure a new machine holds its private address. A cloud network that
/// attaches while the machine boots can miss both the boot-time network
/// configuration and cloud-init's hotplug listener, leaving its interface
/// down; give every unconfigured interface DHCP through netplan and wait.
fn private_address_command(private_ip: &str) -> Result<String> {
    let address: std::net::Ipv4Addr = private_ip
        .parse()
        .map_err(|_| crate::Error::other(format!("private address {private_ip} must be IPv4")))?;
    let script = format!(
        r#"set -eu
have() {{ ip -4 -o addr show | grep -q " inet {address}/"; }}
have && exit 0
for dev in $(ls /sys/class/net); do
    [ "$dev" = lo ] && continue
    ip -4 -o addr show dev "$dev" | grep -q inet && continue
    mac=$(cat "/sys/class/net/$dev/address")
    file="/etc/netplan/60-swarmy-$dev.yaml"
    printf 'network:
  version: 2
  ethernets:
    swarmy-%s:
      match:
        macaddress: "%s"
      dhcp4: true
' "$dev" "$mac" > "$file"
    chmod 600 "$file"
done
netplan apply
for _ in $(seq 1 30); do have && exit 0; sleep 2; done
echo "private address {address} did not come up" >&2
exit 1"#
    );
    Ok(format!("sudo -n sh -c {}", shell_words::quote(&script)))
}
