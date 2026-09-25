use crate::{
    BlockDevice, Error, ExecOutput, ExecRequest, ExecResult, PauseHandle, Result, RuntimeCaps,
    Sandbox, SandboxRuntime, SandboxSpec,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::File,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use swarmy_core::AgentId;
use swarmy_store::ScratchRecord;
use swarmy_volume::server::{self, ServerConfig};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::{Mutex, mpsc, oneshot},
    task::JoinHandle,
};

#[derive(Serialize, Deserialize)]
struct Journal {
    spec: SandboxSpec,
    disk: BlockDevice,
}

struct Running {
    journal: Journal,
    server: Option<ServerTask>,
    cancellations: Arc<std::sync::Mutex<Vec<std::thread::JoinHandle<()>>>>,
    credentials: Option<crate::credentials::Credentials>,
    network: Option<tokio::process::Child>,
}

struct ServerTask(JoinHandle<server::Result<()>>);
impl Drop for ServerTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

// The file is image data, not a shell script. Reject malformed entries instead
// of passing surprising strings to runc or expanding them in the host.
fn parse_image_environment(content: &str) -> Result<Vec<String>> {
    let mut variables = Vec::new();
    for (index, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (key, value) = line.split_once('=').ok_or_else(|| {
            Error::Operation(format!("invalid image environment line {}", index + 1))
        })?;
        if key.is_empty()
            || !key.bytes().enumerate().all(|(i, byte)| {
                byte == b'_' || byte.is_ascii_alphabetic() || (i > 0 && byte.is_ascii_digit())
            })
            || value.contains('\0')
        {
            return Err(Error::Operation(format!(
                "invalid image environment line {}",
                index + 1
            )));
        }
        variables.push(format!("{key}={value}"));
    }
    Ok(variables)
}

#[cfg(test)]
mod image_environment_tests {
    use super::parse_image_environment;

    #[test]
    fn parses_literal_values_and_rejects_malformed_lines() {
        assert_eq!(
            parse_image_environment(
                "# display settings\n\nDISPLAY=:99\n  GALLIUM_DRIVER=llvmpipe\nVALUE=$HOME=a\n"
            )
            .unwrap(),
            ["DISPLAY=:99", "GALLIUM_DRIVER=llvmpipe", "VALUE=$HOME=a"]
        );
        assert!(parse_image_environment("DISPLAY=:99\nnot-an-assignment\n").is_err());
        assert!(parse_image_environment("9BAD=value\n").is_err());
    }
}

/// One runtime per node state directory. An exclusive lock fences local daemons.
pub struct RuncRuntime {
    root: PathBuf,
    scratch_root: PathBuf,
    scratch_policy: ScratchPolicy,
    config: ServerConfig,
    sandboxes: Mutex<BTreeMap<AgentId, Arc<Mutex<Running>>>>,
    lifecycle: Mutex<()>,
    _lock: File,
}

/// Bounds for disposable host-local scratch. Percentages refer to the entire
/// local filesystem, which also holds the volume cache.
#[derive(Clone, Copy)]
pub struct ScratchPolicy {
    pub idle_days: u64,
    pub high_water: u8,
    pub low_water: u8,
}

impl Default for ScratchPolicy {
    fn default() -> Self {
        Self {
            idle_days: 7,
            high_water: 80,
            low_water: 70,
        }
    }
}

impl RuncRuntime {
    /// Address reused in each sandbox's independent network namespace.
    pub const NETWORK_ADDRESS: std::net::Ipv4Addr = std::net::Ipv4Addr::new(10, 0, 2, 2);
    /// Clean up this node's containers and mounts from an earlier incarnation.
    /// Old writer leases are left to expire; recovery never steals a live lease.
    /// # Errors
    /// Returns locking, filesystem, or stale-container cleanup errors.
    pub async fn open(root: PathBuf, config: ServerConfig) -> Result<Self> {
        Self::open_with_scratch(root, config, ScratchPolicy::default()).await
    }

    /// Open with the node's scratch eviction thresholds.
    /// # Errors
    /// Returns invalid settings, locking, filesystem, or stale-container cleanup errors.
    pub async fn open_with_scratch(
        root: PathBuf,
        config: ServerConfig,
        scratch_policy: ScratchPolicy,
    ) -> Result<Self> {
        if scratch_policy.low_water >= scratch_policy.high_water || scratch_policy.high_water > 100
        {
            return Err(Error::Operation(
                "scratch water marks must satisfy low < high <= 100".into(),
            ));
        }
        std::fs::create_dir_all(&config.directory)?;
        let local = config.directory.canonicalize()?;
        let scratch_root = local.parent().ok_or(Error::State)?.join("scratch");
        std::fs::create_dir_all(&scratch_root)?;
        std::fs::set_permissions(&scratch_root, std::fs::Permissions::from_mode(0o700))?;
        std::fs::create_dir_all(&root)?;
        let root = std::fs::canonicalize(root)?;
        let lock = File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("lock"))?;
        fs2::FileExt::try_lock_exclusive(&lock)?;
        std::fs::create_dir_all(root.join("bundles"))?;
        let runtime = Self {
            root,
            scratch_root,
            scratch_policy,
            config,
            sandboxes: Mutex::new(BTreeMap::new()),
            lifecycle: Mutex::new(()),
            _lock: lock,
        };
        for entry in std::fs::read_dir(runtime.root.join("bundles"))? {
            let path = entry?.path();
            if path.join("journal.json").exists() {
                let journal: Journal =
                    serde_json::from_slice(&std::fs::read(path.join("journal.json"))?)?;
                runtime.stop(journal.spec.agent_id).await?;
                unmount(&path.join("rootfs")).await?;
                if path.join("device").exists() {
                    let device = std::fs::read_to_string(path.join("device"))?;
                    swarmy_volume::kernel::cleanup_stale(Path::new(&device)).map_err(|error| {
                        Error::Operation(format!("clear stale attachment {device}: {error}"))
                    })?;
                }
            }
            // A crash between mkdir and journal persistence leaves no attachment.
            std::fs::remove_dir_all(path)?;
        }
        Ok(runtime)
    }

    /// Sweep deleted, moved, idle, and pressure-evicted scratch. Active sandboxes
    /// are excluded, so no bind mount loses its source while in use.
    /// # Errors
    /// Returns metadata, database, or filesystem failures.
    pub async fn sweep_scratch(&self) -> Result<()> {
        let active: std::collections::BTreeSet<_> =
            self.sandboxes.lock().await.keys().copied().collect();
        let mut candidates = Vec::new();
        for entry in std::fs::read_dir(&self.scratch_root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let Ok(ulid) = ulid::Ulid::from_string(&entry.file_name().to_string_lossy()) else {
                continue;
            };
            let id = AgentId::from_ulid(ulid);
            if active.contains(&id) {
                std::fs::write(entry.path().join(".hosted"), b"")?;
                if !self.config.store.is_computer_deleted(id).await? {
                    let bytes = directory_bytes(&entry.path())?;
                    if let Err(error) = self
                        .config
                        .store
                        .report_scratch(
                            id,
                            &ScratchRecord {
                                node_id: self.config.node,
                                bytes,
                            },
                        )
                        .await
                    {
                        tracing::warn!(%id, %error, "scratch size report failed");
                    }
                }
                continue;
            }
            let marker = entry.path().join(".hosted");
            let modified = std::fs::metadata(&marker)
                .or_else(|_| entry.metadata())?
                .modified()?;
            let bytes = directory_bytes(&entry.path())?;
            let deleted = self.config.store.is_computer_deleted(id).await?;
            let moved = self
                .config
                .store
                .get_by_agent(id)
                .await?
                .is_some_and(|placement| placement.node_id != self.config.node);
            let idle = modified.elapsed().unwrap_or_default()
                > Duration::from_secs(self.scratch_policy.idle_days.saturating_mul(86_400));
            if deleted || moved || idle {
                self.evict_scratch(id, bytes)?;
                self.config
                    .store
                    .clear_scratch(id, self.config.node)
                    .await?;
            } else {
                self.config
                    .store
                    .report_scratch(
                        id,
                        &ScratchRecord {
                            node_id: self.config.node,
                            bytes,
                        },
                    )
                    .await?;
                candidates.push((modified, id, bytes));
            }
        }
        let total = fs2::total_space(&self.scratch_root)?;
        if total > 0
            && fs2::available_space(&self.scratch_root)?
                <= total.saturating_mul(u64::from(100 - self.scratch_policy.high_water)) / 100
        {
            candidates.sort_by_key(|item| item.0);
            for (_, id, bytes) in candidates {
                if fs2::available_space(&self.scratch_root)?
                    > total.saturating_mul(u64::from(100 - self.scratch_policy.low_water)) / 100
                {
                    break;
                }
                self.evict_scratch(id, bytes)?;
                self.config
                    .store
                    .clear_scratch(id, self.config.node)
                    .await?;
            }
        }
        Ok(())
    }

    fn evict_scratch(&self, id: AgentId, bytes: u64) -> Result<()> {
        std::fs::remove_dir_all(self.scratch_root.join(id.to_string()))?;
        tracing::info!(computer = %id, bytes, "scratch evicted");
        Ok(())
    }

    fn prepare_scratch(&self, id: AgentId, paths: &[String]) -> Result<()> {
        if paths.is_empty() {
            return Ok(());
        }
        let rootfs = self.bundle(id).join("rootfs");
        let agent = std::fs::metadata(rootfs.join("home/agent"))?;
        let computer = self.scratch_root.join(id.to_string());
        std::fs::create_dir_all(&computer)?;
        for (index, path) in paths.iter().enumerate() {
            let relative = Path::new(path)
                .strip_prefix("/")
                .map_err(|_| Error::State)?;
            if relative.as_os_str().is_empty()
                || relative
                    .components()
                    .any(|part| !matches!(part, std::path::Component::Normal(_)))
            {
                return Err(Error::Operation("invalid scratch path".into()));
            }
            let mut target = rootfs.clone();
            for part in relative.components() {
                target.push(part);
                if target
                    .symlink_metadata()
                    .is_ok_and(|meta| meta.file_type().is_symlink())
                {
                    return Err(Error::Operation("scratch path traverses symlink".into()));
                }
            }
            std::fs::create_dir_all(&target)?;
            let source = computer.join(index.to_string());
            std::fs::create_dir_all(&source)?;
            std::os::unix::fs::chown(&source, Some(agent.uid()), Some(agent.gid()))?;
            if path == "/tmp" {
                std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o1777))?;
            }
        }
        std::fs::write(computer.join(".hosted"), b"")?;
        Ok(())
    }

    fn bundle(&self, id: AgentId) -> PathBuf {
        self.root.join("bundles").join(id.to_string())
    }

    fn network_name(&self, id: AgentId) -> String {
        // A recovering agent can briefly occupy two nodes on one host.
        format!("swarmy-{}-{id}", self.config.node)
    }

    /// Fingerprint regular memory files without a sandbox exec. The published
    /// manifest alone cannot detect writes still buffered in the mounted disk.
    /// # Errors
    /// Rejects paths outside the computer and returns metadata failures.
    pub async fn memory_fingerprint(&self, agent: AgentId, directory: &str) -> Result<u64> {
        use std::{
            hash::{Hash, Hasher},
            os::unix::fs::MetadataExt,
        };
        let _running = self.running(agent).await?;
        let relative = std::path::Path::new(directory)
            .strip_prefix("/")
            .map_err(|_| Error::State)?;
        if relative
            .components()
            .any(|part| !matches!(part, std::path::Component::Normal(_)))
        {
            return Err(Error::State);
        }
        let mut path = self.bundle(agent).join("rootfs").canonicalize()?;
        for part in relative.components() {
            path.push(part);
            match path.symlink_metadata() {
                Ok(metadata) if metadata.file_type().is_symlink() => return Err(Error::State),
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
                Err(error) => return Err(error.into()),
            }
        }
        let mut entries = std::fs::read_dir(path)?.collect::<std::io::Result<Vec<_>>>()?;
        entries.sort_by_key(std::fs::DirEntry::file_name);
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        for entry in entries {
            let meta = entry.path().symlink_metadata()?;
            if meta.is_file() {
                entry.file_name().hash(&mut hash);
                (
                    meta.ino(),
                    meta.len(),
                    meta.mtime(),
                    meta.mtime_nsec(),
                    meta.ctime(),
                    meta.ctime_nsec(),
                )
                    .hash(&mut hash);
            }
        }
        Ok(hash.finish())
    }

    /// Whether this node currently has the agent's computer open.
    /// Prompt context reads must not create a computer or block its next tool.
    pub async fn is_resident(&self, agent: AgentId) -> bool {
        self.sandboxes.lock().await.contains_key(&agent)
    }

    fn command(&self) -> Command {
        let mut command = Command::new("runc");
        command.arg("--root").arg(self.root.join("runc"));
        command.stdin(Stdio::null()).kill_on_drop(true);
        command
    }

    async fn stop(&self, id: AgentId) -> Result<()> {
        // Checking the state directory distinguishes absence from a runc failure.
        if self.root.join("runc").join(id.to_string()).exists() {
            checked(self.command().args(["delete", "--force", &id.to_string()])).await?;
        }
        // The named handle keeps the namespace alive after runc exits, so
        // stop pasta explicitly even when an earlier daemon was killed.
        let pid_file = pasta_pid_file(id);
        if let Ok(pid) = std::fs::read_to_string(&pid_file) {
            if let Ok(pid) = pid.trim().parse::<u32>() {
                let command = std::fs::read(format!("/proc/{pid}/cmdline")).unwrap_or_default();
                if command.split(|byte| *byte == 0).any(|arg| arg == b"pasta")
                    && command
                        .windows(pid_file.as_os_str().as_encoded_bytes().len())
                        .any(|window| window == pid_file.as_os_str().as_encoded_bytes())
                {
                    let _ = Command::new("kill").arg(pid.to_string()).status().await;
                }
            }
            let _ = std::fs::remove_file(pid_file);
        }
        let name = self.network_name(id);
        if Path::new("/run/netns").join(&name).exists() {
            checked(Command::new("ip").args(["netns", "delete", &name])).await?;
        }
        Ok(())
    }

    async fn network(&self, id: AgentId) -> Result<tokio::process::Child> {
        let state = output(self.command().args(["state", &id.to_string()])).await?;
        let state: serde_json::Value = serde_json::from_slice(&state)?;
        let pid = state["pid"].as_u64().ok_or(Error::State)?;
        let name = self.network_name(id);
        checked(Command::new("ip").args(["netns", "attach", &name, &pid.to_string()])).await?;
        // A runc namespace belongs to the host user namespace. pasta needs
        // root to enter it; its default nobody account cannot call setns here.
        // The PID file lives under /run: Ubuntu's AppArmor profile for pasta
        // refuses to write one under the sandbox bundle.
        let pid_file = pasta_pid_file(id);
        if let Some(directory) = pid_file.parent() {
            std::fs::create_dir_all(directory)?;
            std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))?;
        }
        let mut process = Command::new("pasta")
            .args(pasta_arguments(&name, &pid_file))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()?;
        let target = pid.to_string();
        let mut ready = false;
        for _ in 0..100 {
            if let Some(status) = process.try_wait()? {
                return Err(Error::Operation(format!(
                    "pasta exited during setup: {status}"
                )));
            }
            let address = Command::new("nsenter")
                .args(["-t", &target, "-n", "ip", "-4", "addr", "show"])
                .output()
                .await;
            ready = address.is_ok_and(|result| {
                result.status.success()
                    && result.stdout.windows(8).any(|bytes| bytes == b"10.0.2.2")
            });
            if ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if !ready {
            return Err(Error::Operation(
                "pasta did not configure the namespace".into(),
            ));
        }
        checked(
            Command::new("nsenter").args(["-t", &target, "-n", "ip", "link", "set", "lo", "up"]),
        )
        .await?;
        // The connected 10.0.2.0/24 route keeps pasta's gateway and DNS
        // reachable. Other private and link-local destinations are outside
        // the sandbox's outbound internet access.
        for range in [
            "10.0.0.0/8",
            "172.16.0.0/12",
            "192.168.0.0/16",
            "169.254.0.0/16",
        ] {
            checked(Command::new("nsenter").args([
                "-t", &target, "-n", "ip", "route", "replace", "prohibit", range,
            ]))
            .await?;
        }
        let addresses = output(Command::new("ip").args(["-j", "-4", "addr", "show"])).await?;
        let addresses: serde_json::Value = serde_json::from_slice(&addresses)?;
        for interface in addresses.as_array().ok_or(Error::State)? {
            for address in interface["addr_info"].as_array().ok_or(Error::State)? {
                if address["scope"] == "global" {
                    let ip = address["local"].as_str().ok_or(Error::State)?;
                    checked(
                        Command::new("nsenter")
                            .args(["-t", &target, "-n", "ip", "route", "replace", "prohibit"])
                            .arg(format!("{ip}/32")),
                    )
                    .await?;
                }
            }
        }
        // The sandbox resolves names by talking to the host's upstream
        // resolvers directly, which are usually inside a prohibited private
        // range (a VPC resolver, for example), so each gets a narrow route
        // through pasta's gateway. pasta's own DNS forwarder is not used: it
        // ignores a loopback stub such as systemd-resolved's and needs the
        // host-side target set in ways that differ between versions.
        for server in upstream_resolvers() {
            checked(
                Command::new("nsenter")
                    .args(["-t", &target, "-n", "ip", "route", "replace"])
                    .arg(format!("{server}/32"))
                    .args(["via", "10.0.2.1"]),
            )
            .await?;
        }
        Ok(process)
    }

    async fn start(&self, id: AgentId, scratch: &[String], memory_mib: u64) -> Result<()> {
        let bundle = self.bundle(id);
        checked(self.command().args(["spec", "--bundle"]).arg(&bundle)).await?;
        let path = bundle.join("config.json");
        let mut config: serde_json::Value = serde_json::from_slice(&std::fs::read(&path)?)?;
        let bytes = memory_mib
            .checked_mul(1024 * 1024)
            .and_then(|bytes| i64::try_from(bytes).ok())
            .filter(|bytes| *bytes > 0)
            .ok_or_else(|| Error::Operation("invalid sandbox memory limit".into()))?;
        config["linux"]["resources"]["memory"]["limit"] = serde_json::json!(bytes);
        config["root"]["path"] = serde_json::json!(bundle.join("rootfs"));
        config["root"]["readonly"] = false.into();
        config["process"]["terminal"] = false.into();
        // An image may provide its own PID 1 for background services. Older
        // images keep the inert init used before this hook was introduced.
        let init = bundle.join("rootfs/usr/local/libexec/swarmy-init");
        config["process"]["args"] = if init.is_file() {
            serde_json::json!(["/bin/sh", "/usr/local/libexec/swarmy-init"])
        } else {
            serde_json::json!(["/bin/sleep", "infinity"])
        };
        // Package managers need ordinary root filesystem capabilities, but no
        // host administration capabilities such as SYS_ADMIN or NET_ADMIN.
        let caps = serde_json::json!([
            "CAP_CHOWN",
            "CAP_DAC_OVERRIDE",
            "CAP_FOWNER",
            "CAP_FSETID",
            "CAP_SETGID",
            "CAP_SETUID",
            "CAP_SETFCAP",
            "CAP_KILL",
            "CAP_NET_BIND_SERVICE"
        ]);
        for set in ["bounding", "effective", "permitted"] {
            config["process"]["capabilities"][set] = caps.clone();
        }
        let mut env: Vec<String> = [
            "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "HOME=/home/agent",
            "GH_CONFIG_DIR=/run/swarmy-gh",
            "TERM=xterm",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        let environment = bundle.join("rootfs/etc/swarmy/environment");
        if environment.is_file() {
            for variable in parse_image_environment(&std::fs::read_to_string(environment)?)? {
                let key = variable.split_once('=').ok_or(Error::State)?.0;
                env.retain(|existing| !existing.starts_with(&format!("{key}=")));
                env.push(variable);
            }
        }
        config["process"]["env"] = serde_json::json!(env);
        config["process"]["rlimits"]
            .as_array_mut()
            .ok_or(Error::State)?
            .push(serde_json::json!({"type": "RLIMIT_CORE", "hard": 0, "soft": 0}));
        config["process"]["cwd"] = "/home/agent/work".into();
        config["hostname"] = "swarmy".into();
        // runc creates a private network namespace; pasta supplies outbound
        // TCP, UDP, and DNS without exposing host listeners or guest ports.
        let mounts = config["mounts"].as_array_mut().ok_or(Error::State)?;
        mounts.push(serde_json::json!({"destination": "/run/swarmy", "type": "bind", "source": bundle.join("guest"), "options": ["bind", "nosuid", "nodev", "noexec"]}));
        mounts.push(serde_json::json!({"destination": "/run/swarmy-gh", "type": "tmpfs", "source": "tmpfs", "options": ["nosuid", "nodev", "noexec", "mode=1777", "size=16m"]}));
        std::fs::write(bundle.join("resolv.conf"), sandbox_resolv_conf())?;
        mounts.push(serde_json::json!({"destination": "/etc/resolv.conf", "type": "bind", "source": bundle.join("resolv.conf"), "options": ["bind", "ro", "nosuid", "nodev", "noexec"]}));
        for (index, destination) in scratch.iter().enumerate() {
            mounts.push(serde_json::json!({"destination": destination, "type": "bind", "source": self.scratch_root.join(id.to_string()).join(index.to_string()), "options": ["bind", "rw", "nosuid", "nodev"]}));
        }
        std::fs::write(path, serde_json::to_vec_pretty(&config)?)?;
        // Init outlives runc create, so it must not inherit pipes that the
        // host command helper waits to drain before starting the container.
        let status = self
            .command()
            .arg("--log")
            .arg(bundle.join("runc.log"))
            .args(["create", "--bundle"])
            .arg(&bundle)
            .arg(id.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await?;
        if !status.success() {
            return Err(Error::Operation(std::fs::read_to_string(
                bundle.join("runc.log"),
            )?));
        }
        let network = self.network(id).await?;
        self.running(id).await?.lock().await.network = Some(network);
        checked(self.command().args(["start", &id.to_string()])).await
    }

    async fn running(&self, id: AgentId) -> Result<Arc<Mutex<Running>>> {
        self.sandboxes
            .lock()
            .await
            .get(&id)
            .cloned()
            .ok_or(Error::State)
    }

    async fn remove(&self, id: AgentId, publish: bool) -> Result<PauseHandle> {
        self.stop(id).await?;
        let _lifecycle = self.lifecycle.lock().await;
        let entry = self.running(id).await?;
        let mut running = entry.lock().await;
        if let Some(mut network) = running.network.take() {
            let _ = network.kill().await;
        }
        // A delayed cancellation signal must finish before this id can be reused.
        while running
            .cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .any(|task| !task.is_finished())
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        for task in std::mem::take(
            &mut *running
                .cancellations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        ) {
            let _ = task.join();
        }
        unmount(&self.bundle(id).join("rootfs")).await?;
        let mut forced = false;
        if running
            .server
            .as_ref()
            .is_some_and(|server| !server.0.is_finished())
        {
            if publish {
                server::control(&self.config, running.journal.disk.volume_id, None, true).await?;
            } else if let Err(error) =
                server::discard(&self.config, running.journal.disk.volume_id).await
            {
                // A detach error may arrive after the device was disconnected.
                // Finish local cleanup even if the control reply reports failure.
                tracing::warn!(%error, "forcing attachment shutdown after discard error");
                if let Some(server) = &running.server {
                    server.0.abort();
                }
                forced = true;
            }
        }
        let outcome = if let Some(mut server) = running.server.take() {
            (&mut server.0)
                .await
                .map_err(|error| Error::Operation(error.to_string()))
                .and_then(|result| result.map_err(Error::from))
        } else {
            Ok(())
        };
        if publish {
            outcome?;
        } else if let Err(error) = outcome {
            tracing::warn!(%error, "discarded failed attachment");
        }
        if forced {
            let device = std::fs::read_to_string(self.bundle(id).join("device"))?;
            swarmy_volume::kernel::cleanup_stale(Path::new(&device)).map_err(|error| {
                Error::Operation(format!("clear discarded attachment {device}: {error}"))
            })?;
        }
        let handle = PauseHandle {
            spec: running.journal.spec.clone(),
            disk: running.journal.disk,
        };
        let marker = self.scratch_root.join(id.to_string()).join(".hosted");
        if marker.exists() {
            std::fs::write(marker, b"")?;
        }
        std::fs::remove_dir_all(self.bundle(id))
            .map_err(|error| Error::Operation(format!("remove sandbox bundle {id}: {error}")))?;
        self.sandboxes.lock().await.remove(&id);
        Ok(handle)
    }

    /// Stop local processes and background disk activity without publication.
    /// # Errors
    /// Returns stop, unmount, or detach errors.
    pub async fn discard_if_present(&self, id: AgentId) -> Result<()> {
        if self.sandboxes.lock().await.contains_key(&id) {
            self.remove(id, false).await?;
        } else if self.bundle(id).join("journal.json").exists() {
            let journal: Journal =
                serde_json::from_slice(&std::fs::read(self.bundle(id).join("journal.json"))?)?;
            self.stop(id).await?;
            unmount(&self.bundle(id).join("rootfs")).await?;
            if self
                .config
                .directory
                .join(format!("{}.sock", journal.disk.volume_id))
                .exists()
            {
                server::discard(&self.config, journal.disk.volume_id).await?;
            }
            std::fs::remove_dir_all(self.bundle(id))?;
        }
        Ok(())
    }

    /// Publish the attached disk while keeping its container alive.
    /// # Errors
    /// Returns missing sandbox, sync, or fenced publication errors.
    pub async fn checkpoint(&self, sandbox: &Sandbox) -> Result<swarmy_core::ManifestId> {
        let entry = self.running(sandbox.agent_id).await?;
        let running = entry.lock().await;
        Ok(server::checkpoint(
            &self.config,
            running.journal.disk.volume_id,
            Some(self.bundle(sandbox.agent_id).join("rootfs")),
        )
        .await?)
    }

    /// List local containers owned by this runtime.
    pub async fn list(&self) -> Vec<Sandbox> {
        self.sandboxes
            .lock()
            .await
            .keys()
            .map(|id| Sandbox { agent_id: *id })
            .collect()
    }

    /// Stop and flush every local sandbox before a graceful daemon exit.
    /// # Errors
    /// Returns the first cleanup error after attempting every sandbox.
    pub async fn shutdown(&self) -> Result<()> {
        let ids: Vec<_> = self.sandboxes.lock().await.keys().copied().collect();
        let mut result = Ok(());
        for id in ids {
            if let Err(error) = self.remove(id, true).await {
                result = Err(error);
            }
        }
        result
    }
}

#[async_trait]
impl SandboxRuntime for RuncRuntime {
    // Creation keeps attachment cleanup and journal publication in one fenced path.
    #[allow(clippy::too_many_lines)]
    async fn create(&self, spec: SandboxSpec, disk: BlockDevice) -> Result<Sandbox> {
        let total = fs2::total_space(&self.scratch_root)?;
        if total > 0
            && fs2::available_space(&self.scratch_root)?
                <= total.saturating_mul(u64::from(100 - self.scratch_policy.high_water)) / 100
        {
            self.sweep_scratch().await?;
        }
        let _lifecycle = self.lifecycle.lock().await;
        let (id, scratch, memory_mib) = (
            spec.agent_id,
            spec.scratch.clone(),
            spec.requirements.memory_mib,
        );
        if self.sandboxes.lock().await.contains_key(&id) {
            return Err(Error::State);
        }
        let bundle = self.bundle(id);
        std::fs::create_dir(&bundle)?;
        let journal = Journal { spec, disk };
        // Persist ownership before attaching so SIGKILL at any later step leaves
        // enough information to clean up on the next daemon start.
        let journal_path = bundle.join("journal.json");
        std::fs::write(&journal_path, serde_json::to_vec(&journal)?)?;
        File::open(journal_path)?.sync_all()?;
        File::open(&bundle)?.sync_all()?;
        std::fs::create_dir(bundle.join("rootfs"))?;
        std::fs::create_dir(bundle.join("guest"))?;
        let (ready_tx, ready_rx) = oneshot::channel();
        let config = self.config.clone();
        let device_journal = bundle.join("device");
        let mut server = ServerTask(tokio::spawn(server::attach(
            config,
            disk.volume_id,
            None,
            true,
            move |path| {
                std::fs::write(&device_journal, path.as_os_str().as_encoded_bytes())?;
                File::open(&device_journal)?.sync_all()?;
                ready_tx
                    .send(path.to_path_buf())
                    .map_err(|_| server::Error::Message("sandbox creation cancelled".into()))
            },
            std::future::pending(),
        )));
        let Ok(path) = ready_rx.await else {
            let outcome = (&mut server.0)
                .await
                .map_err(|error| Error::Operation(error.to_string()))?;
            std::fs::remove_dir_all(bundle)?;
            outcome?;
            return Err(Error::State);
        };
        self.sandboxes.lock().await.insert(
            id,
            Arc::new(Mutex::new(Running {
                journal,
                server: Some(server),
                cancellations: Arc::default(),
                credentials: None,
                network: None,
            })),
        );
        let setup = async {
            checked(
                Command::new("mount")
                    .args(["-t", "ext4", "-o", "noatime", "--"])
                    .arg(path)
                    .arg(bundle.join("rootfs")),
            )
            .await?;
            checked(Command::new("chroot").arg(bundle.join("rootfs")).args([
                "/bin/sh",
                "-ec",
                include_str!("../../../images/common/agent-setup.sh"),
            ]))
            .await?;
            let credentials = crate::credentials::Credentials::start(
                &bundle.join("guest/github.sock"),
                self.config.store.clone(),
                id,
            )?;
            self.running(id).await?.lock().await.credentials = Some(credentials);
            self.prepare_scratch(id, &scratch)?;
            self.start(id, &scratch, memory_mib).await?;
            if !scratch.is_empty() {
                self.config
                    .store
                    .report_scratch(
                        id,
                        &ScratchRecord {
                            node_id: self.config.node,
                            bytes: directory_bytes(&self.scratch_root.join(id.to_string()))?,
                        },
                    )
                    .await?;
            }
            Ok(())
        }
        .await;
        if let Err(error) = setup {
            // Release the lifecycle gate before the common teardown takes it.
            drop(_lifecycle);
            self.remove(id, false).await?;
            return Err(error);
        }
        Ok(Sandbox { agent_id: id })
    }

    async fn exec(
        &self,
        sb: &Sandbox,
        request: ExecRequest,
        output: mpsc::Sender<ExecOutput>,
    ) -> Result<ExecResult> {
        if request.args.is_empty() || request.timeout_ms == 0 {
            return Err(Error::Operation(
                "exec requires arguments and a positive timeout".into(),
            ));
        }
        let _activity = swarmy_volume::priority::ToolActivity::begin();
        let entry = self.running(sb.agent_id).await?;
        let running = entry.lock().await;
        let token = ulid::Ulid::generate().to_string();
        let guest = self.bundle(sb.agent_id).join("guest");
        let mut guard = ExecGuard {
            root: self.root.join("runc"),
            id: sb.agent_id,
            token: token.clone(),
            guest,
            armed: true,
            cancellations: running.cancellations.clone(),
        };
        let mut child = self
            .command()
            .arg("exec")
            .arg(sb.agent_id.to_string())
            .args(["/usr/bin/setsid", "--wait", "/bin/bash", "-c",
                "echo $$ > /run/swarmy/$1.pid; test ! -e /run/swarmy/$1.cancel || exit 137; shift; exec \"$@\"",
                "swarmy", &token])
            .args(request.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let mut stdin = child.stdin.take().ok_or(Error::State)?;
        let stdout = child.stdout.take().ok_or(Error::State)?;
        let stderr = child.stderr.take().ok_or(Error::State)?;
        let run = async {
            let ((), (), (), status) = tokio::try_join!(
                async {
                    stdin.write_all(&request.stdin).await?;
                    drop(stdin);
                    Ok::<_, Error>(())
                },
                pump(stdout, output.clone(), true),
                pump(stderr, output, false),
                async { child.wait().await.map_err(Error::from) }
            )?;
            Ok::<_, Error>(ExecResult {
                exit_code: status.code().unwrap_or(137),
                timed_out: false,
            })
        };
        match tokio::time::timeout(Duration::from_millis(request.timeout_ms), run).await {
            Ok(Ok(result)) => {
                guard.armed = false;
                Ok(result)
            }
            Ok(Err(error)) => Err(error),
            Err(_) => {
                std::fs::write(guard.guest.join(format!("{token}.cancel")), b"")?;
                checked(&mut guard.kill_command()).await?;
                let _ = child.kill().await;
                let _ = child.wait().await;
                guard.armed = false;
                Ok(ExecResult {
                    exit_code: 137,
                    timed_out: true,
                })
            }
        }
    }

    async fn pause(&self, sb: &Sandbox) -> Result<PauseHandle> {
        self.remove(sb.agent_id, true).await
    }
    async fn resume(&self, handle: PauseHandle) -> Result<Sandbox> {
        self.create(handle.spec, handle.disk).await
    }
    async fn destroy(&self, sb: Sandbox) -> Result<()> {
        self.remove(sb.agent_id, true).await.map(|_| ())
    }
    fn capabilities(&self) -> RuntimeCaps {
        RuntimeCaps {
            memory_pause: false,
            kvm: false,
        }
    }
}

fn directory_bytes(path: &Path) -> Result<u64> {
    let mut bytes = 0_u64;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = match entry.path().symlink_metadata() {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        if metadata.is_dir() {
            match directory_bytes(&entry.path()) {
                Ok(size) => bytes = bytes.saturating_add(size),
                Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        } else {
            bytes = bytes.saturating_add(metadata.len());
        }
    }
    Ok(bytes)
}

struct ExecGuard {
    root: PathBuf,
    id: AgentId,
    armed: bool,
    token: String,
    guest: PathBuf,
    cancellations: Arc<std::sync::Mutex<Vec<std::thread::JoinHandle<()>>>>,
}
impl ExecGuard {
    fn kill_command(&self) -> Command {
        let mut command = Command::new("runc");
        command.arg("--root").arg(&self.root)
            .args(["exec", &self.id.to_string(), "/bin/bash", "-c",
                "if test -s /run/swarmy/$1.pid; then read -r pid < /run/swarmy/$1.pid; kill -KILL -- -$pid 2>/dev/null || true; fi",
                "swarmy", &self.token]);
        command
    }
}
impl Drop for ExecGuard {
    fn drop(&mut self) {
        if self.armed {
            // Mark cancellation before signalling so an exec still starting
            // cannot escape cleanup by publishing its pid after the signal.
            let _ = std::fs::write(self.guest.join(format!("{}.cancel", self.token)), b"");
            let mut command: std::process::Command = self.kill_command().into_std();
            if let Ok(mut child) = command.stdout(Stdio::null()).stderr(Stdio::null()).spawn() {
                let task = std::thread::spawn(move || {
                    let _ = child.wait();
                });
                self.cancellations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(task);
            }
        } else {
            let _ = std::fs::remove_file(self.guest.join(format!("{}.pid", self.token)));
            let _ = std::fs::remove_file(self.guest.join(format!("{}.cancel", self.token)));
        }
    }
}

async fn pump(
    mut reader: impl AsyncRead + Unpin,
    output: mpsc::Sender<ExecOutput>,
    stdout: bool,
) -> Result<()> {
    let mut bytes = vec![0; 8192];
    loop {
        let n = reader.read(&mut bytes).await?;
        if n == 0 {
            return Ok(());
        }
        let bytes = bytes[..n].to_vec();
        output
            .send(if stdout {
                ExecOutput::Stdout(bytes)
            } else {
                ExecOutput::Stderr(bytes)
            })
            .await
            .map_err(|_| Error::Operation("exec output receiver closed".into()))?;
    }
}

fn pasta_arguments(netns: &str, pid_file: &Path) -> Vec<String> {
    let mut arguments = Vec::new();
    arguments.extend(
        [
            "--foreground",
            "--runas",
            "0",
            "--netns",
            netns,
            "--config-net",
            "--no-map-gw",
            "--ipv4-only",
            "--address",
            "10.0.2.2",
            "--netmask",
            "24",
            "--gateway",
            "10.0.2.1",
            "-t",
            "none",
            "-u",
            "none",
            "-T",
            "none",
            "-U",
            "none",
            "--pid",
        ]
        .map(String::from),
    );
    arguments.push(pid_file.to_string_lossy().into_owned());
    arguments
}

/// Location of the pasta helper's PID file for one computer.
///
/// The file lives under `/run` because the sandbox bundle path is rejected by
/// the host `AppArmor` profile for pasta. Tests must read the PID here rather
/// than from the bundle directory.
#[must_use]
pub fn pasta_pid_file(id: AgentId) -> PathBuf {
    Path::new("/run/swarmy/pasta").join(format!("{id}.pid"))
}

/// The sandbox's resolver file: the host's upstream resolvers, reached
/// through per-server routes, with the host's search list.
fn sandbox_resolv_conf() -> String {
    let mut text = String::new();
    for server in upstream_resolvers() {
        text.push_str("nameserver ");
        text.push_str(&server);
        text.push('\n');
    }
    for path in ["/run/systemd/resolve/resolv.conf", "/etc/resolv.conf"] {
        if let Ok(host) = std::fs::read_to_string(path)
            && let Some(search) = host.lines().find(|line| line.starts_with("search "))
        {
            text.push_str(search.trim());
            text.push('\n');
            break;
        }
    }
    text
}

/// The real upstream resolvers: those behind a local stub when
/// systemd-resolved runs, otherwise any non-loopback entries in
/// /etc/resolv.conf.
fn upstream_resolvers() -> Vec<String> {
    for path in ["/run/systemd/resolve/resolv.conf", "/etc/resolv.conf"] {
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        let servers: Vec<String> = text
            .lines()
            .filter_map(|line| line.trim().strip_prefix("nameserver "))
            .map(|server| server.trim().to_owned())
            .filter(|server| {
                server
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|address| !address.is_loopback())
            })
            .collect();
        if !servers.is_empty() {
            return servers;
        }
    }
    Vec::new()
}

async fn checked(command: &mut Command) -> Result<()> {
    output(command).await.map(|_| ())
}

async fn output(command: &mut Command) -> Result<Vec<u8>> {
    let output = command
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .output()
        .await?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(Error::Operation(format!(
            "{command:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        )))
    }
}

async fn unmount(path: &Path) -> Result<()> {
    let status = Command::new("mountpoint")
        .arg("-q")
        .arg(path)
        .status()
        .await?;
    if status.success() {
        checked(Command::new("umount").arg("--").arg(path)).await?;
    } else if status.code() != Some(32) && path.exists() {
        return Err(Error::Operation("cannot inspect sandbox mount".into()));
    }
    Ok(())
}
