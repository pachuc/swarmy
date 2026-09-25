//! Database commands run separately so the public CLI can diagnose a missing client library.
mod agent;
mod agent_command;
mod api_client;
mod auth;
mod auth_command;
mod bench;
mod bench_command;
mod chat;
mod conversation;
mod gc;
mod image;
mod image_command;
mod models_probe;
mod models_probe_command;
mod provider_runtime;
mod remote_command;
// Status only needs the health and command helpers; provisioning helpers stay unused here.
#[allow(dead_code)]
#[path = "remote/ssh.rs"]
mod remote_ssh;
#[path = "remote/status.rs"]
mod remote_status;
mod selection;
mod selection_command;
mod session;
mod session_command;
mod vol;
mod vol_command;
mod vol_server;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "swarmy")]
struct Cli {
    #[arg(long, global = true)]
    json: bool,
    #[arg(long, global = true)]
    remote: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum ProbeCommand {
    Probe(models_probe_command::Args),
}

#[derive(Subcommand)]
enum Command {
    Models {
        #[command(subcommand)]
        command: ProbeCommand,
    },
    Auth {
        #[arg(long, env = "SWARMY_CHATGPT_AUTH", global = true)]
        auth_file: Option<std::path::PathBuf>,
        #[command(subcommand)]
        command: auth_command::Command,
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
    Remote {
        #[command(subcommand)]
        command: remote_command::Command,
    },
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
    Run {
        prompt: String,
        /// Base image in NAME:TAG form for this session's disk.
        #[arg(long)]
        image: Option<String>,
        /// Resume the main session on a named agent (name or agent id)
        #[arg(long, conflicts_with = "image")]
        agent: Option<String>,
        /// Create a side conversation on the named agent
        #[arg(long, requires = "agent")]
        new: bool,
        /// Continue an existing session by id instead of an agent's main one
        #[arg(long, conflicts_with_all = ["agent", "image", "new"])]
        session: Option<ulid::Ulid>,
        #[command(flatten)]
        selection: selection_command::SelectionArgs,
    },
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
    Session {
        #[command(subcommand)]
        command: session_command::Command,
    },
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();
    let cli = swarmy_version::parse::<Cli>("swarmy-session")?;
    remote_command::select(cli.remote.as_deref())?;
    // The network guard must outlive the runtime and all database operations.
    let _network = swarmy_store::boot();
    tokio::runtime::Runtime::new()?.block_on(async {
        match cli.command {
            Command::Models {
                command: ProbeCommand::Probe(args),
            } => models_probe::run(args, cli.json).await,
            Command::Auth { command, auth_file } => auth::run(command, auth_file, cli.json).await,
            Command::Bench { command } => bench::run(command, cli.json).await,
            Command::Remote {
                command: remote_command::Command::Status,
            } => remote_status::run(cli.json).await,
            Command::Remote { .. } => {
                anyhow::bail!("use swarmy for node, tunnel, and log commands")
            }
            Command::Gc { dry_run } => gc::run(dry_run, cli.json).await,
            Command::Vol { command } => vol::run(command, cli.json).await,
            Command::Image { command } => image::run(command, cli.json).await,
            Command::Agent { .. } => unreachable!("agent management uses the API"),
            Command::Run {
                prompt,
                image,
                agent,
                new,
                session,
                selection,
            } => {
                session::run(
                    prompt,
                    image,
                    agent,
                    new,
                    session,
                    crate::selection::normalize(selection.into())?,
                    cli.json,
                )
                .await
            }
            Command::Chat {
                session_id,
                image,
                agent,
                new,
                selection,
            } => {
                let id = session_id.map(swarmy_core::SessionId::from_ulid);
                if cli.json {
                    session::chat_json(
                        id,
                        image,
                        agent,
                        new,
                        crate::selection::normalize(selection.into())?,
                    )
                    .await
                } else {
                    chat::run(
                        id,
                        image,
                        agent,
                        new,
                        crate::selection::normalize(selection.into())?,
                    )
                    .await
                }
            }
            Command::Session { command } => session::inspect(command, cli.json).await,
        }
    })
}
