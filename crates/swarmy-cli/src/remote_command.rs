use clap::Subcommand;

#[derive(Subcommand)]
pub enum Command {
    /// Launch, copy this checkout, and provision a remote node
    Up {
        name: String,
        #[arg(long)]
        bucket: Option<String>,
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
        /// Copy the `ChatGPT` credential and keyring and run a gateway on this node
        #[arg(long)]
        copy_credential: bool,
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
