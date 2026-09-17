//! Database commands run separately so the public CLI can diagnose a missing client library.
mod chat;
mod conversation;
mod gc;
mod image;
mod image_command;
mod remote_command;
// Status only needs the health and command helpers; provisioning helpers stay unused here.
#[allow(dead_code)]
#[path = "remote/ssh.rs"]
mod remote_ssh;
#[path = "remote/status.rs"]
mod remote_status;
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
enum Command {
    /// Bounded database probe used by doctor without linking its front end to `libfdb_c`.
    #[command(hide = true)]
    DoctorFdb,
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
    },
    Chat {
        session_id: Option<ulid::Ulid>,
        /// Base image in NAME:TAG form; otherwise use `default_image`.
        #[arg(long, conflicts_with = "session_id")]
        image: Option<String>,
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
    let cli = Cli::parse();
    remote_command::select(cli.remote.as_deref())?;
    // The network guard must outlive the runtime and all database operations.
    let _network = swarmy_store::boot();
    tokio::runtime::Runtime::new()?.block_on(async {
        match cli.command {
            Command::DoctorFdb => {
                conversation::store().await?.list_sessions(None, 1).await?;
                Ok(())
            }
            Command::Remote {
                command: remote_command::Command::Status,
            } => remote_status::run(cli.json).await,
            Command::Remote { .. } => {
                anyhow::bail!("use swarmy for node, tunnel, and log commands")
            }
            Command::Gc { dry_run } => gc::run(dry_run, cli.json).await,
            Command::Vol { command } => vol::run(command, cli.json).await,
            Command::Image { command } => image::run(command, cli.json).await,
            Command::Run { prompt, image } => session::run(prompt, image, cli.json).await,
            Command::Chat { session_id, image } => {
                anyhow::ensure!(
                    !cli.json,
                    "chat is a terminal interface and does not support --json"
                );
                chat::run(session_id.map(swarmy_core::SessionId::from_ulid), image).await
            }
            Command::Session { command } => session::inspect(command, cli.json).await,
        }
    })
}
