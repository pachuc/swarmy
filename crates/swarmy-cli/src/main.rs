use clap::{Parser, Subcommand};

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
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::Version => {
            if cli.json {
                println!("{{\"version\":\"{}\"}}", env!("CARGO_PKG_VERSION"));
            } else {
                println!("swarmy {}", env!("CARGO_PKG_VERSION"));
            }
        }
    }
}
