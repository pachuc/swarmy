use clap::Subcommand;

#[derive(Subcommand)]
pub enum Command {
    /// Launch, copy this checkout, and provision a remote node
    Up {
        name: String,
        /// Override the first node's EC2 instance type
        #[arg(long)]
        instance_type: Option<String>,
        /// Override the first node's EBS root disk size in GiB
        #[arg(long)]
        disk_gb: Option<u32>,
        #[arg(long)]
        bucket: Option<String>,
        /// Maximum sandboxes on this node (zero for a control-only node)
        #[arg(long)]
        sandboxes: Option<u32>,
        /// Run control-plane services on the laptop (default) or the node
        #[arg(long)]
        services: Option<swarmy_config::RemoteServices>,
        /// Acknowledge that the `ChatGPT` credential file and cluster keyring leave this laptop over SSH
        #[arg(long)]
        copy_credential: bool,
        /// Skip building and registering the stack's default image
        #[arg(long, conflicts_with = "image_recipe")]
        no_image: bool,
        /// Recipe directory within the checkout, relative to its root
        #[arg(long, default_value = "images/base-ubuntu")]
        image_recipe: std::path::PathBuf,
    },
    /// Join another node to a remote over its private network
    AddNode {
        name: String,
        /// Maximum sandboxes on the joining node
        #[arg(long)]
        sandboxes: Option<u32>,
        /// Override this node's EC2 instance type
        #[arg(long)]
        instance_type: Option<String>,
        /// Override this node's EBS root disk size in GiB
        #[arg(long)]
        disk_gb: Option<u32>,
        /// Copy the `ChatGPT` credential and keyring and run a gateway on this node
        #[arg(long)]
        copy_credential: bool,
    },
    /// Update binaries and services on every node without replacing instances
    Upgrade {
        name: String,
        /// Leave node daemons running even when their binaries changed
        #[arg(long)]
        services_only: bool,
        /// Accept uncommitted local checkout changes
        #[arg(long)]
        allow_dirty: bool,
        /// Maximum seconds to wait for running sandbox commands
        #[arg(long, default_value_t = 600)]
        drain_timeout: u64,
    },
    /// Terminate nodes and remove their cloud resources and local state
    Down {
        name: String,
        /// Retain the bucket and its guarding role and profile
        #[arg(long)]
        keep_bucket: bool,
        /// Confirm permanent deletion without an interactive prompt
        #[arg(long)]
        yes: bool,
    },
    /// Explicitly adopt an older remote's bucket, role, and profile for teardown
    Tag { name: String },
    /// Forward remote `FoundationDB`, NATS, and S3 to local ports
    Connect { name: String },
    /// Stop the recorded SSH tunnel
    Disconnect { name: String },
    /// List saved instances, tunnels, store heartbeats, and registered images
    Status,
    /// Follow the remote swarmyd journal
    Logs { name: String },
}

/// Re-exec before starting threads instead of mutating the process environment.
///
/// # Errors
///
/// Rejects invalid remote names and reports failures to re-exec the process.
pub fn select(name: Option<&str>) -> anyhow::Result<()> {
    if let Some(name) = name {
        swarmy_config::validate_remote_name(name)?;
        if std::env::var("SWARMY_REMOTE").as_deref() != Ok(name) {
            use std::os::unix::process::CommandExt;
            return Err(std::process::Command::new(std::env::current_exe()?)
                .args(std::env::args_os().skip(1))
                .env("SWARMY_REMOTE", name)
                .exec()
                .into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Command;
    use clap::Parser;

    #[derive(Parser)]
    struct Cli {
        #[command(subcommand)]
        command: Command,
    }
}

#[cfg(test)]
mod sandbox_limit_tests {
    use super::Command;
    use clap::Parser;

    #[derive(Parser)]
    struct Cli {
        #[command(subcommand)]
        command: Command,
    }

    #[test]
    fn accepts_zero_and_rejects_invalid_limits_before_launch() {
        for action in ["up", "add-node"] {
            for invalid in ["-1", "wrong", "4294967296"] {
                assert!(
                    Cli::try_parse_from(["remote", action, "demo", "--sandboxes", invalid])
                        .is_err()
                );
            }
            let command = Cli::try_parse_from(["remote", action, "demo", "--sandboxes", "0"])
                .unwrap()
                .command;
            match command {
                Command::Up { sandboxes, .. } | Command::AddNode { sandboxes, .. } => {
                    assert_eq!(sandboxes, Some(0));
                }
                _ => panic!("expected launch command"),
            }
        }
    }
}
