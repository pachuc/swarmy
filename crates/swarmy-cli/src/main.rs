mod chat;
mod conversation;
mod dev;
mod session;

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
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start and operate the local development system
    Dev {
        #[command(subcommand)]
        command: dev::Command,
    },
    /// Print the version of this CLI
    Version,
    /// Start a conversation and stream its output until idle
    Run { prompt: String },
    /// Open a terminal conversation, or resume a session
    Chat { session_id: Option<ulid::Ulid> },
    /// Inspect stored sessions
    Session {
        #[command(subcommand)]
        command: session::Command,
    },
    /// Manage `ChatGPT` subscription credentials
    Auth {
        /// Swarmy's credential file (never defaults to Codex's auth.json)
        #[arg(long, env = "SWARMY_CHATGPT_AUTH", global = true)]
        auth_file: Option<PathBuf>,
        #[command(subcommand)]
        command: AuthCommand,
    },
}

#[derive(Subcommand)]
enum AuthCommand {
    /// Sign in with a dedicated `ChatGPT` device-code login
    Login,
    /// Import a Codex auth.json; stop its original refresh owner first
    Import { source: PathBuf },
}

// The network guard must outlive the runtime and all database operations.
fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
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
    let _network = swarmy_store::boot();
    tokio::runtime::Runtime::new()?.block_on(run(cli))
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    match cli.command {
        Command::Dev { .. } => unreachable!("dev commands run without the database network"),
        Command::Run { prompt } => session::run(prompt, cli.json).await?,
        Command::Chat { session_id } => {
            anyhow::ensure!(
                !cli.json,
                "chat is a terminal interface and does not support --json"
            );
            chat::run(session_id.map(swarmy_core::SessionId::from_ulid)).await?;
        }
        Command::Session { command } => session::inspect(command, cli.json).await?,
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
                AuthCommand::Login => {
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
                AuthCommand::Import { source } => store.import(&source).await?,
            }
            if cli.json {
                println!(
                    "{}",
                    serde_json::json!({"event": "credentials_saved", "path": path})
                );
            } else {
                println!("Saved ChatGPT credentials to {}", path.display());
            }
        }
        Command::Version => {
            if cli.json {
                println!("{{\"version\":\"{}\"}}", env!("CARGO_PKG_VERSION"));
            } else {
                println!("swarmy {}", env!("CARGO_PKG_VERSION"));
            }
        }
    }
    Ok(())
}
