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
    /// Print the version of this CLI
    Version,
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Auth { auth_file, command } => {
            let path = auth_file.map_or_else(
                || {
                    std::env::var_os("HOME")
                        .map(|home| PathBuf::from(home).join(".swarmy/auth.json"))
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "set --auth-file or SWARMY_CHATGPT_AUTH when HOME is unset"
                            )
                        })
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
