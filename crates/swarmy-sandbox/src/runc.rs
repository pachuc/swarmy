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
    io::{AsyncRead, AsyncReadExt},
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
        Ok(())
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
        config["hostname"] = "swarmy".into();
        // This slice uses the host network for outbound package downloads.
        config["linux"]["namespaces"]
            .as_array_mut()
            .ok_or(Error::State)?
            .retain(|ns| ns["type"] != "network");
        let mounts = config["mounts"].as_array_mut().ok_or(Error::State)?;
        mounts.push(serde_json::json!({"destination": "/run/swarmy", "type": "bind", "source": bundle.join("guest"), "options": ["bind", "nosuid", "nodev", "noexec"]}));
        mounts.push(serde_json::json!({"destination": "/etc/resolv.conf", "type": "bind", "source": "/etc/resolv.conf", "options": ["bind", "ro", "nosuid", "nodev", "noexec"]}));
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
        let entry = self.running(sb.agent_id).await?;
        let running = entry.lock().await;
        let mut child = self
            .command()
            .arg("exec")
            .arg(sb.agent_id.to_string())
            .args(request.args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let mut guard = ExecGuard {
            root: self.root.join("runc"),
            id: sb.agent_id,
            armed: true,
            cancellations: running.cancellations.clone(),
        };
        let stdout = child.stdout.take().ok_or(Error::State)?;
        let stderr = child.stderr.take().ok_or(Error::State)?;
        let run = async {
            let ((), (), status) = tokio::try_join!(
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
                checked(
                    self.command()
                        .args(["kill", "--all", &sb.agent_id.to_string(), "KILL"]),
                )
                .await?;
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
    cancellations: Arc<std::sync::Mutex<Vec<std::thread::JoinHandle<()>>>>,
}
impl Drop for ExecGuard {
    fn drop(&mut self) {
        if self.armed {
            // Signalling can wait for tasks in kernel I/O. Do not block the
            // executor that must keep serving their NBD requests during teardown.
            if let Ok(mut child) = std::process::Command::new("runc")
                .arg("--root")
                .arg(&self.root)
                .args(["kill", "--all", &self.id.to_string(), "KILL"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
            {
                let task = std::thread::spawn(move || {
                    let _ = child.wait();
                });
                self.cancellations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(task);
            }
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
    let output = command
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .output()
        .await?;
    if output.status.success() {
        Ok(())
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
