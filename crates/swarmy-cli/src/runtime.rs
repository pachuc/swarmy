//! Database commands run separately so the public CLI can diagnose a missing client library.
mod chat;
mod conversation;
mod image;
mod image_command;
mod session;
mod session_command;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "swarmy")]
struct Cli {
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Build and inspect base filesystem images
    Image {
        #[command(subcommand)]
        command: image_command::Command,
    },
    Run {
        prompt: String,
    },
    Chat {
        session_id: Option<ulid::Ulid>,
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
    // The network guard must outlive the runtime and all database operations.
    let _network = swarmy_store::boot();
    tokio::runtime::Runtime::new()?.block_on(async {
        match cli.command {
            Command::Image { command } => image::run(command, cli.json).await,
            Command::Run { prompt } => session::run(prompt, cli.json).await,
            Command::Chat { session_id } => {
                anyhow::ensure!(
                    !cli.json,
                    "chat is a terminal interface and does not support --json"
                );
                chat::run(session_id.map(swarmy_core::SessionId::from_ulid)).await
            }
            Command::Session { command } => session::inspect(command, cli.json).await,
        }
    })
}
