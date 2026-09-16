use std::{path::PathBuf, process::Command};

use anyhow::{Context, Result, ensure};
use clap::Parser;

#[derive(Debug, Parser)]
#[command(about = "Verify durable sessions while killing and restarting services")]
pub struct Config {
    #[arg(long, default_value_t = 20)]
    pub sessions: usize,
    /// Run bash disk checks and include swarmyd in the kill schedule (requires root).
    #[arg(long)]
    pub image: Option<String>,
    /// Kill the node after the command writes its file, then require exactly one retry.
    #[arg(long, requires = "image")]
    pub kill_node_mid_command: bool,
    #[arg(long, default_value_t = 5)]
    pub steps: usize,
    #[arg(long, default_value_t = 15)]
    pub kills: usize,
    #[arg(long, default_value_t = 2)]
    pub schedulers: usize,
    #[arg(long, default_value_t = 2)]
    pub workers: usize,
    #[arg(long, default_value_t = 2)]
    pub gateways: usize,
    /// Replay victim selection and intervals (OS scheduling still varies).
    #[arg(long)]
    pub seed: Option<u64>,
    #[arg(long, default_value_t = 100)]
    pub min_interval_ms: u64,
    #[arg(long, default_value_t = 350)]
    pub max_interval_ms: u64,
    /// Delay before each fake delta; each response emits two deltas.
    #[arg(long, default_value_t = 100)]
    pub latency_ms: u64,
    #[arg(long, default_value_t = 120)]
    pub session_timeout_secs: u64,
    /// Use connection settings from the environment without starting the stack.
    #[arg(long)]
    pub no_start_stack: bool,
    /// Use prebuilt service binaries from this directory instead of building them.
    #[arg(long)]
    pub bin_dir: Option<PathBuf>,
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.sessions > 0 && self.steps > 0,
            "sessions and steps must be positive"
        );
        ensure!(
            self.schedulers > 0 && self.workers > 0 && self.gateways > 0,
            "each service count must be positive"
        );
        ensure!(
            self.min_interval_ms > 0 && self.min_interval_ms <= self.max_interval_ms,
            "kill intervals must be positive and min <= max"
        );
        ensure!(
            self.session_timeout_secs > 0,
            "session timeout must be positive"
        );
        if self.image.is_some() {
            ensure!(
                std::process::Command::new("id").arg("-u").output()?.stdout == b"0\n",
                "--image requires root; run the prebuilt binary with sudo"
            );
            ensure!(self.steps >= 2, "bash checks require at least two steps");
        }
        if self.kill_node_mid_command {
            ensure!(
                self.sessions == 1 && self.steps == 3 && self.kills == 0,
                "deterministic node kill requires --sessions 1 --steps 3 --kills 0"
            );
        }
        self.sessions
            .checked_mul(self.steps)
            .and_then(|steps| steps.checked_add(self.kills))
            .context("call count overflow")?;
        Ok(())
    }

    pub fn binaries(&self) -> Result<PathBuf> {
        if let Some(path) = &self.bin_dir {
            return path.canonicalize().context("binary directory missing");
        }
        let executable = std::env::current_exe()?;
        let directory = executable
            .parent()
            .context("executable directory missing")?;
        let mut build = Command::new("cargo");
        build.current_dir(repo()).args([
            "build",
            "--locked",
            "-p",
            "swarmy-scheduler",
            "-p",
            "swarmy-worker",
            "-p",
            "swarmy-gateway",
            "-p",
            "swarmyd",
        ]);
        if directory.file_name().is_some_and(|name| name == "release") {
            build.arg("--release");
        }
        ensure!(build.status()?.success(), "service build failed");
        Ok(directory.to_owned())
    }
}

pub fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Source Bash's escaped connection settings in a child, before FDB or Tokio starts.
pub fn with_stack() -> Result<()> {
    let status = Command::new("bash")
        .arg("-c")
        .arg("cd -- \"$1\" && scripts/dev-stack.sh start && source .dev/env && shift && exec \"$@\" --no-start-stack")
        .arg("swarmy-chaos")
        .arg(repo())
        .arg(std::env::current_exe()?)
        .args(std::env::args_os().skip(1))
        .status()?;
    ensure!(status.success(), "chaos child exited with {status}");
    Ok(())
}
