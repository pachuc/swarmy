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
    /// Terminate all nodes and remove their key pairs and local state
    Down { name: String },
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

    #[test]
    fn bucket_flag_parses() {
        let Command::Up { bucket, .. } =
            Cli::try_parse_from(["remote", "up", "demo", "--bucket", "example-bucket"])
                .unwrap()
                .command
        else {
            panic!("expected up");
        };
        assert_eq!(bucket.as_deref(), Some("example-bucket"));
    }

    #[test]
    fn node_shape_flags_parse_for_both_commands() {
        let Command::Up {
            instance_type,
            disk_gb,
            ..
        } = Cli::try_parse_from([
            "remote",
            "up",
            "demo",
            "--instance-type",
            "m6i.large",
            "--disk-gb",
            "40",
        ])
        .unwrap()
        .command
        else {
            panic!("expected up")
        };
        assert_eq!(instance_type.as_deref(), Some("m6i.large"));
        assert_eq!(disk_gb, Some(40));
        let Command::AddNode {
            instance_type,
            disk_gb,
            ..
        } = Cli::try_parse_from([
            "remote",
            "add-node",
            "demo",
            "--instance-type",
            "m6id.4xlarge",
            "--disk-gb",
            "100",
        ])
        .unwrap()
        .command
        else {
            panic!("expected add-node")
        };
        assert_eq!(instance_type.as_deref(), Some("m6id.4xlarge"));
        assert_eq!(disk_gb, Some(100));
    }

    #[test]
    fn upgrade_flags_parse_with_any_drain_timeout() {
        let Command::Upgrade {
            name,
            services_only,
            allow_dirty,
            drain_timeout,
        } = Cli::try_parse_from(["remote", "upgrade", "demo"])
            .unwrap()
            .command
        else {
            panic!("expected upgrade")
        };
        assert_eq!(name, "demo");
        assert!(!services_only && !allow_dirty);
        assert_eq!(drain_timeout, 600);
        let Command::Upgrade {
            services_only,
            allow_dirty,
            drain_timeout,
            ..
        } = Cli::try_parse_from([
            "remote",
            "upgrade",
            "demo",
            "--services-only",
            "--allow-dirty",
            "--drain-timeout",
            "17",
        ])
        .unwrap()
        .command
        else {
            panic!("expected upgrade")
        };
        assert!(services_only && allow_dirty);
        assert_eq!(drain_timeout, 17);
        // Zero means restart the daemon without waiting for sandbox commands.
        let Command::Upgrade { drain_timeout, .. } =
            Cli::try_parse_from(["remote", "upgrade", "demo", "--drain-timeout", "0"])
                .unwrap()
                .command
        else {
            panic!("expected upgrade")
        };
        assert_eq!(drain_timeout, 0);
        assert!(
            Cli::try_parse_from(["remote", "upgrade", "demo", "--drain-timeout", "-1"]).is_err()
        );
    }

    #[test]
    fn image_build_options_parse() {
        for (args, skipped, recipe) in [
            (vec!["remote", "up", "demo"], false, "images/base-ubuntu"),
            (
                vec!["remote", "up", "demo", "--no-image"],
                true,
                "images/base-ubuntu",
            ),
            (
                vec!["remote", "up", "demo", "--image-recipe", "images/custom"],
                false,
                "images/custom",
            ),
        ] {
            let Command::Up {
                name,
                no_image,
                image_recipe,
                ..
            } = Cli::try_parse_from(args).unwrap().command
            else {
                panic!("expected up")
            };
            assert_eq!(name, "demo");
            assert_eq!(no_image, skipped);
            assert_eq!(image_recipe, std::path::Path::new(recipe));
        }
        assert!(
            Cli::try_parse_from([
                "remote",
                "up",
                "demo",
                "--no-image",
                "--image-recipe",
                "images/custom"
            ])
            .is_err()
        );
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
