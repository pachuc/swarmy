use clap::Subcommand;
use ulid::Ulid;

#[derive(Subcommand)]
pub enum Command {
    /// Print the session's events in sequence order
    Show { session_id: Ulid },
    /// List stored sessions in id order
    #[command(name = "ls", alias = "list")]
    List,
    /// List durable per-turn metrics for a session
    Metrics { session_id: Ulid },
    /// Close a side session, or an ephemeral session and its computer
    Close { session_id: Ulid },
    /// End the current turn, including a parked inference wait
    Interrupt { session_id: Ulid },
}
