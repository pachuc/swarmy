use clap::Subcommand;
use ulid::Ulid;

#[derive(Subcommand)]
pub enum Command {
    /// Print the session's events in sequence order
    Show { session_id: Ulid },
    /// List stored sessions in id order
    #[command(name = "ls", alias = "list")]
    List,
    /// Close an ephemeral session and delete its computer
    Close { session_id: Ulid },
}
