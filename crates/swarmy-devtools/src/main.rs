mod api_client;
mod auth;
mod auth_command;
use clap::Parser;
#[derive(Parser)]
struct Cli {
    #[arg(long)]
    json: bool,
    #[arg(long)]
    auth_file: Option<std::path::PathBuf>,
    #[command(subcommand)]
    command: auth_command::Command,
}
fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    tokio::runtime::Runtime::new()?.block_on(auth::run(cli.command, cli.auth_file, cli.json))
}
