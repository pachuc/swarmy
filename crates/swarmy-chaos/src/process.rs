use std::{ffi::OsString, fs::OpenOptions, path::Path, process::Stdio};

use anyhow::{Context, Result, ensure};
use tokio::process::{Child, Command};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Scheduler,
    Worker,
    Gateway,
    Api,
    Node,
}

impl Kind {
    pub const fn binary(self) -> &'static str {
        match self {
            Self::Scheduler => "swarmy-scheduler",
            Self::Worker => "swarmy-worker",
            Self::Gateway => "swarmy-gateway",
            Self::Api => "swarmy-api",
            Self::Node => "swarmyd",
        }
    }
}

/// A restart reuses the complete command and environment for this service slot.
/// Add a Kind and register its slots to extend the set of killable services.
pub struct Process {
    pub kind: Kind,
    pub name: String,
    command: Command,
    child: Child,
}

impl Process {
    pub fn start(
        kind: Kind,
        index: usize,
        binaries: &Path,
        files: &Path,
        environment: &[(OsString, OsString)],
    ) -> Result<Self> {
        let name = format!("{}-{index}", kind.binary());
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(files.join(format!("{name}.log")))?;
        let mut command = Command::new(binaries.join(kind.binary()));
        command
            .current_dir(files)
            .envs(environment.iter().cloned())
            .env_remove("SWARMY_WORKER_KILL_POINT")
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .kill_on_drop(true);
        let child = command.spawn().with_context(|| format!("start {name}"))?;
        Ok(Self {
            kind,
            name,
            command,
            child,
        })
    }

    pub fn check(&mut self) -> Result<()> {
        ensure!(
            self.child.try_wait()?.is_none(),
            "{} exited unexpectedly; see its service log",
            self.name
        );
        Ok(())
    }

    pub async fn restart(&mut self) -> Result<()> {
        self.check()?;
        self.child
            .kill()
            .await
            .with_context(|| format!("SIGKILL {}", self.name))?;
        self.child = self
            .command
            .spawn()
            .with_context(|| format!("restart {}", self.name))?;
        Ok(())
    }

    pub fn start_stopped(&mut self) -> Result<()> {
        ensure!(
            self.child.try_wait()?.is_some(),
            "{} is still running",
            self.name
        );
        self.child = self
            .command
            .spawn()
            .with_context(|| format!("start {} after full stop", self.name))?;
        Ok(())
    }

    pub fn kill_now(&mut self) {
        let _ = self.child.start_kill();
        for _ in 0..500 {
            if self.child.try_wait().ok().flatten().is_some() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    pub async fn stop(&mut self) -> Result<()> {
        if self.child.try_wait()?.is_none() {
            if self.kind == Kind::Node {
                let status = std::process::Command::new("kill")
                    .args([
                        "-TERM",
                        &self.child.id().context("node pid missing")?.to_string(),
                    ])
                    .status()?;
                ensure!(status.success(), "node termination failed");
                tokio::time::timeout(std::time::Duration::from_secs(45), self.child.wait())
                    .await??;
            } else {
                self.child.kill().await?;
            }
        }
        Ok(())
    }
}
