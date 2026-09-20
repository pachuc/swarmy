mod agent_command;
mod auth_command;
mod bench_command;
mod dev;
mod doctor;
mod image_command;
mod models;
mod remote;
mod remote_command;
mod selection_command;
mod session_command;
mod tools;
mod vol_command;

use clap::{Parser, Subcommand};
use std::{io::Write, path::PathBuf};
use swarmy_llm::auth::{FileCredentialStore, OAuthClient};

#[derive(Parser)]
#[command(
    name = "swarmy",
    version,
    about = "Operate a Swarmy cluster: agents, sessions, volumes, images, channels"
)]
struct Cli {
    /// Emit compact machine-readable JSON
    #[arg(long, global = true)]
    json: bool,
    /// Use a saved remote tunnel profile
    #[arg(long, global = true)]
    remote: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Browse the configured provider and model catalog
    Models {
        #[command(subcommand)]
        command: models::Command,
    },
    /// Create and manage named agents
    Agent {
        #[command(subcommand)]
        command: agent_command::Command,
    },
    /// Measure conversation latency
    Bench {
        #[command(subcommand)]
        command: bench_command::Command,
    },
    /// Launch, connect to, and inspect remote development stacks
    Remote {
        #[command(subcommand)]
        command: remote_command::Command,
    },
    /// Collect unreferenced chunks older than the configured grace window
    Gc {
        #[arg(long)]
        dry_run: bool,
    },
    /// Create, attach, and snapshot durable volumes
    Vol {
        #[command(subcommand)]
        command: vol_command::Command,
    },
    /// Build and inspect base filesystem images
    Image {
        #[command(subcommand)]
        command: image_command::Command,
    },
    /// Start and operate the local development system
    Dev {
        #[command(subcommand)]
        command: dev::Command,
    },
    /// Check installation, configuration, credentials, and local connectivity
    Doctor,
    /// Print the version of this CLI
    Version,
    /// Start a conversation and stream its output until idle
    Run {
        prompt: String,
        #[arg(long)]
        image: Option<String>,
        /// Resume the main session on a named agent (name or agent id)
        #[arg(long, conflicts_with = "image")]
        agent: Option<String>,
        /// Create a side conversation on the named agent
        #[arg(long, requires = "agent")]
        new: bool,
        #[command(flatten)]
        selection: selection_command::SelectionArgs,
    },
    /// Open a terminal conversation, or resume a session
    Chat {
        #[arg(conflicts_with_all = ["provider", "model", "effort"])]
        session_id: Option<ulid::Ulid>,
        /// Base image in NAME:TAG form; otherwise use `default_image`.
        #[arg(long, conflicts_with = "session_id")]
        image: Option<String>,
        /// Resume the main session on a named agent (name or agent id)
        #[arg(long, conflicts_with_all = ["image", "session_id"])]
        agent: Option<String>,
        /// Create a side conversation on the named agent
        #[arg(long, requires = "agent")]
        new: bool,
        #[command(flatten)]
        selection: selection_command::SelectionArgs,
    },
    /// Inspect stored sessions
    Session {
        #[command(subcommand)]
        command: session_command::Command,
    },
    /// Manage encrypted provider credentials
    Auth {
        /// Swarmy's credential file (never defaults to Codex's auth.json)
        #[arg(long, env = "SWARMY_CHATGPT_AUTH", global = true)]
        auth_file: Option<PathBuf>,
        #[command(subcommand)]
        command: auth_command::Command,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = swarmy_version::parse::<Cli>("swarmy")?;
    remote_command::select(cli.remote.as_deref())?;
    // Background service logs must not overwrite the full-screen transcript.
    let writer = if matches!(cli.command, Command::Chat { .. }) {
        tracing_subscriber::fmt::writer::BoxMakeWriter::new(std::io::sink)
    } else {
        tracing_subscriber::fmt::writer::BoxMakeWriter::new(std::io::stderr)
    };
    tracing_subscriber::fmt()
        .with_writer(writer)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();
    if let Command::Dev { command } = cli.command {
        return tokio::runtime::Runtime::new()?.block_on(dev::run(command));
    }
    if matches!(
        cli.command,
        Command::Auth {
            command: auth_command::Command::Set(_)
                | auth_command::Command::Ls
                | auth_command::Command::Rm { .. }
                | auth_command::Command::Check { .. }
                | auth_command::Command::Import { .. },
            ..
        } | Command::Remote {
            command: remote_command::Command::Status
        } | Command::Bench { .. }
            | Command::Run { .. }
            | Command::Agent { .. }
            | Command::Session { .. }
            | Command::Chat { .. }
            | Command::Vol { .. }
            | Command::Image { .. }
            | Command::Gc { .. }
    ) {
        use std::os::unix::process::CommandExt;
        let runtime = std::env::current_exe()?.with_file_name("swarmy-session");
        let error = std::process::Command::new(runtime)
            .args(std::env::args_os().skip(1))
            .exec();
        return Err(anyhow::anyhow!(
            "cannot start database commands: {error}; reinstall swarmy-cli"
        ));
    }
    tokio::runtime::Runtime::new()?.block_on(run(cli))
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    match cli.command {
        Command::Models { command } => models::run(command, cli.json)?,
        Command::Remote { command } => remote::run(command, cli.json).await?,
        Command::Dev { .. } => unreachable!("dev commands run without the database network"),
        Command::Bench { .. }
        | Command::Run { .. }
        | Command::Agent { .. }
        | Command::Session { .. }
        | Command::Chat { .. }
        | Command::Vol { .. }
        | Command::Image { .. }
        | Command::Gc { .. } => unreachable!(),
        Command::Doctor => {
            if !doctor::run(cli.json).await? {
                std::process::exit(1);
            }
        }
        Command::Auth { auth_file, command } => {
            let path = auth_file.map_or_else(
                || {
                    Ok::<_, anyhow::Error>(PathBuf::from(
                        swarmy_config::Settings::load()?.settings.credential_file,
                    ))
                },
                Ok,
            )?;
            let store = FileCredentialStore::new(&path);
            match command {
                auth_command::Command::Login { provider } => {
                    anyhow::ensure!(provider == "chatgpt", "only chatgpt login is available");
                    let oauth = OAuthClient::new()?;
                    let code = oauth.device_code().await?;
                    if cli.json {
                        println!(
                            "{}",
                            serde_json::json!({"event": "device_code", "url": code.verification_url, "code": code.user_code})
                        );
                    } else {
                        println!(
                            "Open {} and enter code {}",
                            code.verification_url, code.user_code
                        );
                    }
                    std::io::stdout().flush()?;
                    oauth.complete_login(code, &store).await?;
                }
                _ => unreachable!("database auth commands run in swarmy-session"),
            }
            if cli.json {
                println!(
                    "{}",
                    serde_json::json!({"event": "credentials_saved", "path": path, "note": "Login will save to the cluster after the logins change; use swarmy auth import now."})
                );
            } else {
                println!(
                    "Saved ChatGPT credentials to {}. Login will save to the cluster after the logins change; use swarmy auth import now.",
                    path.display()
                );
            }
        }
        Command::Version => swarmy_version::print("swarmy", cli.json)?,
    }
    Ok(())
}
