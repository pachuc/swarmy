use std::{path::PathBuf, process::Command};

use anyhow::{Context, Result, ensure};
use clap::Parser;

#[derive(Debug, Parser)]
#[command(about = "Verify durable sessions while killing and restarting services")]
pub(crate) struct Config {
    /// Select a named-agent acceptance scenario instead of the kill schedule.
    #[arg(long, value_enum)]
    pub scenario: Option<Scenario>,
    /// Executable controlling a remote first node: start, kill, and stop.
    /// Receives the node settings as environment variables. Used by cloud benchmarks.
    #[arg(long)]
    pub node_driver: Option<PathBuf>,
    /// Directory for the desktop proof screenshots, app record, and summary.
    #[arg(long)]
    pub desktop_output: Option<PathBuf>,
    /// Run the root volume measurement script twice after persistent scenarios.
    #[arg(long)]
    pub measurements: Option<PathBuf>,
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
    /// Use a prebuilt swarmy-core binary from this directory instead of building it.
    #[arg(long)]
    pub bin_dir: Option<PathBuf>,
    /// Run the node inside this network namespace (`ip netns exec NAME`).
    #[arg(long, requires = "image")]
    pub node_netns: Option<String>,
    /// Fail when a node store transaction takes longer than this many milliseconds.
    #[arg(long, requires = "image")]
    pub max_node_transaction_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Scenario {
    /// Exercise shared computers, background-process loss, and idle eviction.
    Persistent,
    /// Prove named-agent memory, tools, and timers survive summarization and a full restart.
    Continuity,
    /// Clone, repair a failing test, and push through one node kill using a local remote.
    Coding,
    /// Log into a web app in the sandbox, change a setting, and take browser and screen screenshots with a scripted agent. Needs root and a display image.
    Desktop,
    /// Saturate two rate-limited API keys of one HTTP fake behind one route with named agents. Needs no root.
    Saturation,
}

impl Config {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            (self.node_netns.is_none() && self.max_node_transaction_ms.is_none())
                || self.scenario.is_none(),
            "--node-netns and --max-node-transaction-ms need the default scenario"
        );
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
        ensure!(
            (self.node_driver.is_none() && self.measurements.is_none())
                || self.scenario == Some(Scenario::Persistent),
            "--node-driver and --measurements require --scenario persistent"
        );
        ensure!(
            self.desktop_output.is_none() || self.scenario == Some(Scenario::Desktop),
            "--desktop-output requires --scenario desktop"
        );
        ensure!(
            !matches!(
                self.scenario,
                Some(
                    Scenario::Persistent
                        | Scenario::Continuity
                        | Scenario::Coding
                        | Scenario::Desktop
                )
            ) || self.image.is_some(),
            "--scenario persistent, continuity, coding, or desktop requires --image"
        );
        ensure!(
            self.scenario != Some(Scenario::Saturation) || self.image.is_none(),
            "--scenario saturation does not use --image"
        );
        if self.scenario == Some(Scenario::Saturation) {
            ensure!(self.kills == 0, "--scenario saturation requires --kills 0");
            ensure!(
                self.sessions >= 4,
                "--scenario saturation needs at least four agents to saturate two keys"
            );
        }
        if self.image.is_some() {
            ensure!(
                Command::new("id").arg("-u").output()?.stdout == b"0\n",
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
        if matches!(
            self.scenario,
            Some(Scenario::Continuity | Scenario::Coding | Scenario::Desktop)
        ) {
            ensure!(
                self.gateways == 1 && self.kills == 0,
                "--scenario continuity, coding, and desktop require --gateways 1 --kills 0"
            );
        }
        if self.scenario == Some(Scenario::Desktop) {
            ensure!(
                self.sessions == 1,
                "--scenario desktop requires --sessions 1"
            );
        }
        if self.scenario == Some(Scenario::Persistent) {
            ensure!(
                self.sessions == 2 && self.gateways == 1 && self.kills == 0,
                "--scenario persistent requires --sessions 2 --gateways 1 --kills 0"
            );
        }
        self.sessions
            .checked_mul(self.steps)
            .and_then(|steps| steps.checked_add(self.kills))
            .context("call count overflow")?;
        Ok(())
    }

    pub(crate) fn binaries(&self) -> Result<PathBuf> {
        if let Some(path) = &self.bin_dir {
            return path.canonicalize().context("binary directory missing");
        }
        let executable = std::env::current_exe()?;
        let directory = executable
            .parent()
            .context("executable directory missing")?;
        let mut build = Command::new("cargo");
        build
            .current_dir(repo())
            .args(["build", "--locked", "-p", "swarmy-core-bin"]);
        if directory.file_name().is_some_and(|name| name == "release") {
            build.arg("--release");
        }
        ensure!(build.status()?.success(), "service build failed");
        Ok(directory.to_owned())
    }
}

pub(crate) fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Source Bash's escaped connection settings in a child, before FDB or Tokio starts.
pub(crate) fn with_stack() -> Result<()> {
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

/// Documented `swarmy-chaos ...` flags resolve against the real clap tree.
#[cfg(test)]
mod docs_command_tests {
    #![deny(clippy::disallowed_methods)]
    #[test]
    fn markdown_chaos_commands_match_the_clap_tree() {
        use clap::CommandFactory;
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../");
        let command = super::Config::command();
        swarmy_testkit::check_docs_commands(&root, &[("swarmy-chaos", &command)])
            .expect("documented swarmy-chaos flags match the chaos clap tree");
    }
}
