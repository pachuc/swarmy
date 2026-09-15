//! Database commands run separately so the public CLI can diagnose a missing client library.
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
    Run {
        prompt: String,
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
            Command::Run { prompt } => session::run(prompt, cli.json).await,
            Command::Session { command } => session::inspect(command, cli.json).await,
        }
    })
}
