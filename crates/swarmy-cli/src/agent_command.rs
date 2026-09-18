use clap::Subcommand;

#[derive(Subcommand)]
pub enum Command {
    /// Create a named agent and pin its computer's image
    Create {
        #[arg(value_parser = agent_name)]
        name: String,
        #[arg(long)]
        image: Option<String>,
        #[arg(long, default_value = "")]
        description: String,
    },
    /// List named agents
    #[command(alias = "list")]
    Ls,
    /// Inspect an agent and its sessions
    Show { name: String },
    /// Delete an agent and its computer, retaining transcripts
    Delete {
        name: String,
        #[arg(long)]
        yes: bool,
    },
}

fn agent_name(name: &str) -> Result<String, String> {
    swarmy_config::validate_remote_name(name).map_err(|_| {
        "agent names must contain 1-64 letters, digits, hyphens, or underscores".to_owned()
    })?;
    Ok(name.to_owned())
}
