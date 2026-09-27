mod api_client;
mod auth;
mod auth_command;
mod models_probe;
mod models_probe_command;
mod provider_runtime;
use clap::Parser;
#[derive(Parser)]
struct Cli {
    #[arg(long)]
    json: bool,
    #[arg(long)]
    auth_file: Option<std::path::PathBuf>,
    #[command(subcommand)]
    command: Command,
}
#[derive(clap::Subcommand)]
enum Command {
    Login {
        #[arg(default_value = "chatgpt")]
        provider: String,
        #[arg(long)]
        label: Option<String>,
        #[arg(long)]
        resource: Option<String>,
        #[arg(long)]
        scope: Option<String>,
    },
    Import {
        #[arg(long)]
        file: Option<std::path::PathBuf>,
        #[arg(long)]
        label: Option<String>,
    },
    Probe(models_probe_command::Args),
}
fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    tokio::runtime::Runtime::new()?.block_on(async {
        match cli.command {
            Command::Login {
                provider,
                label,
                resource,
                scope,
            } => {
                auth::run(
                    auth_command::Command::Login {
                        provider,
                        label,
                        resource,
                        scope,
                    },
                    cli.auth_file,
                    cli.json,
                )
                .await
            }
            Command::Import { file, label } => {
                auth::run(
                    auth_command::Command::Import { file, label },
                    cli.auth_file,
                    cli.json,
                )
                .await
            }
            Command::Probe(args) => models_probe::run(args, cli.json).await,
        }
    })
}
