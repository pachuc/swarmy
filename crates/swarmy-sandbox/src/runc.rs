use crate::{
    BlockDevice, Error, ExecOutput, ExecRequest, ExecResult, PauseHandle, Result, RuntimeCaps,
    Sandbox, SandboxRuntime, SandboxSpec,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::File,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use swarmy_core::AgentId;
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

/// One runtime per node state directory. An exclusive lock fences local daemons.
pub struct RuncRuntime {
    root: PathBuf,
    config: ServerConfig,
    sandboxes: Mutex<BTreeMap<AgentId, Arc<Mutex<Running>>>>,
    lifecycle: Mutex<()>,
    _lock: File,
}

impl RuncRuntime {
    /// Address reused in each sandbox's independent network namespace.
    pub const NETWORK_ADDRESS: std::net::Ipv4Addr = std::net::Ipv4Addr::new(10, 0, 2, 2);
    /// Clean up this node's containers and mounts from an earlier incarnation.
    /// Old writer leases are left to expire; recovery never steals a live lease.
    /// # Errors
    /// Returns locking, filesystem, or stale-container cleanup errors.
    pub async fn open(root: PathBuf, config: ServerConfig) -> Result<Self> {
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
        let pid_file = self.bundle(id).join("pasta.pid");
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
        let bundle = self.bundle(id);
        let name = self.network_name(id);
        checked(Command::new("ip").args(["netns", "attach", &name, &pid.to_string()])).await?;
        // A runc namespace belongs to the host user namespace. pasta needs
        // root to enter it; its default nobody account cannot call setns here.
        let mut process = Command::new("pasta")
            .args([
                "--foreground",
                "--runas",
                "0",
                "--netns",
                &name,
                "--config-net",
                "--no-map-gw",
                "--ipv4-only",
                "--address",
                "10.0.2.2",
                "--netmask",
                "24",
                "--gateway",
                "10.0.2.1",
                "--dns-forward",
                "10.0.2.3",
                "-t",
                "none",
                "-u",
                "none",
                "-T",
                "none",
                "-U",
                "none",
                "--pid",
            ])
            .arg(bundle.join("pasta.pid"))
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
        Ok(process)
    }

    async fn start(&self, id: AgentId) -> Result<()> {
        let bundle = self.bundle(id);
        checked(self.command().args(["spec", "--bundle"]).arg(&bundle)).await?;
        let path = bundle.join("config.json");
        let mut config: serde_json::Value = serde_json::from_slice(&std::fs::read(&path)?)?;
        config["root"]["path"] = serde_json::json!(bundle.join("rootfs"));
        config["root"]["readonly"] = false.into();
        config["process"]["terminal"] = false.into();
        config["process"]["args"] = serde_json::json!(["/bin/sleep", "infinity"]);
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
        config["process"]["env"] = serde_json::json!([
            "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "HOME=/home/agent",
            "GH_CONFIG_DIR=/run/swarmy-gh",
            "TERM=xterm"
        ]);
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
        std::fs::write(bundle.join("resolv.conf"), "nameserver 10.0.2.3\n")?;
        mounts.push(serde_json::json!({"destination": "/etc/resolv.conf", "type": "bind", "source": bundle.join("resolv.conf"), "options": ["bind", "ro", "nosuid", "nodev", "noexec"]}));
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
    async fn create(&self, spec: SandboxSpec, disk: BlockDevice) -> Result<Sandbox> {
        let _lifecycle = self.lifecycle.lock().await;
        let id = spec.agent_id;
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
                include_str!("../../../images/base-ubuntu/setup.sh"),
            ]))
            .await?;
            let credentials = crate::credentials::Credentials::start(
                &bundle.join("guest/github.sock"),
                self.config.store.clone(),
                id,
            )?;
            self.running(id).await?.lock().await.credentials = Some(credentials);
            self.start(id).await
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
