use clap::Subcommand;

#[derive(Subcommand)]
pub enum Command {
    /// Launch, copy this checkout, and provision a remote node
    Up { name: String },
    /// Join another node to a remote over its private network
    AddNode { name: String },
    /// Terminate all nodes and remove their key pairs and local state
    Down { name: String },
    /// Forward remote `FoundationDB`, NATS, and S3 to local ports
    Connect { name: String },
    /// Stop the recorded SSH tunnel
    Disconnect { name: String },
    /// List saved instances, tunnels, and store heartbeats
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
