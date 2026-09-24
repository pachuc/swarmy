mod agent_command;
mod api_client;
mod api_commands;
mod auth;
mod auth_command;
mod bench_command;
mod client_bench;
mod client_chat;
mod client_commands;
mod client_conversation;
mod dev;
mod doctor;
mod image_command;
mod models;
mod models_probe_command;
mod provider_report;
#[cfg(feature = "remote")]
mod remote;
mod remote_command;
#[cfg(not(feature = "remote"))]
#[allow(dead_code)] // The node CLI only uses tunnel health checks from this shared module.
#[path = "remote/ssh.rs"]
mod remote_ssh;
mod selection_command;
mod session_command;
mod tools;
mod vol_command;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

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
    #[cfg(not(feature = "remote"))]
    if matches!(cli.command, Command::Remote { .. }) {
        anyhow::bail!("swarmy was built without remote support");
    }
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
        &cli.command,
        Command::Auth {
            command: auth_command::Command::Login { .. } | auth_command::Command::Import { .. },
            ..
        }
    ) {
        let Command::Auth { command, auth_file } = cli.command else {
            unreachable!()
        };
        return tokio::runtime::Runtime::new()?.block_on(auth::run(command, auth_file, cli.json));
    }
    if matches!(
        &cli.command,
        Command::Session {
            command: session_command::Command::List | session_command::Command::Show { .. }
        } | Command::Agent { .. }
            | Command::Image {
                command: image_command::Command::Ls | image_command::Command::Show { .. }
            }
            | Command::Auth {
                command: auth_command::Command::Set(_)
                    | auth_command::Command::Ls
                    | auth_command::Command::Rm { .. }
                    | auth_command::Command::Check { .. },
                ..
            }
    ) {
        return tokio::runtime::Runtime::new()?.block_on(api_commands::run(cli.command, cli.json));
    }
    if matches!(
        cli.command,
        Command::Auth { .. }
            | Command::Models {
                command: models::Command::Probe(_)
            }
            | Command::Remote {
                command: remote_command::Command::Status
            }
            | Command::Agent { .. }
            | Command::Session { .. }
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
        Command::Models { command } => models::run(command, cli.json).await?,
        Command::Bench { command } => {
            let (client, endpoint) = api_client::connect()?;
            api_client::call(&endpoint, client.health()).await?;
            client_bench::run(client, command, cli.json).await?;
        }
        Command::Run {
            prompt,
            image,
            agent,
            new,
            selection,
        } => {
            let (client, endpoint) = api_client::connect()?;
            api_client::call(&endpoint, client.health()).await?;
            client_commands::run(client, prompt, image, agent, new, selection, cli.json).await?;
        }
        Command::Chat {
            session_id,
            image,
            agent,
            new,
            selection,
        } => {
            let (client, endpoint) = api_client::connect()?;
            api_client::call(&endpoint, client.health()).await?;
            client_conversation::wait_healthy(&client, selection.provider.as_deref()).await?;
            if cli.json {
                client_commands::chat(client, session_id, image, agent, new, selection, true)
                    .await?;
            } else {
                client_chat::run(client, session_id, image, agent, new, selection).await?;
            }
        }
        #[cfg(feature = "remote")]
        Command::Remote { command } => Box::pin(remote::run(command, cli.json)).await?,
        #[cfg(not(feature = "remote"))]
        Command::Remote { .. } => unreachable!("remote commands are rejected before dispatch"),
        Command::Dev { .. } => unreachable!("dev commands run without the database network"),
        Command::Agent { .. }
        | Command::Session { .. }
        | Command::Vol { .. }
        | Command::Image { .. }
        | Command::Gc { .. } => unreachable!(),
        Command::Doctor => {
            if !doctor::run(cli.json).await? {
                std::process::exit(1);
            }
        }
        Command::Auth { .. } => unreachable!("auth commands run in swarmy-session"),
        Command::Version => swarmy_version::print("swarmy", cli.json)?,
    }
    Ok(())
}
