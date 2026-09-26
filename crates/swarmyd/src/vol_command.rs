use clap::Subcommand;
use std::path::PathBuf;

#[derive(Subcommand)]
pub enum Command {
    /// Create a volume from an image NAME:TAG
    Create { image: String },
    /// Serve a volume in the foreground (requires root)
    Attach {
        volume: ulid::Ulid,
        /// Choose a kernel device; otherwise select an unused /dev/nbdX
        #[arg(long)]
        device: Option<PathBuf>,
        /// Upload dirty chunks continuously between flushes
        #[arg(long)]
        background: bool,
    },
    /// Publish a durable manifest from the attached writer
    Flush {
        volume: ulid::Ulid,
        /// Freeze the filesystem for a clean image; defaults to a block boundary
        #[arg(long)]
        freeze: bool,
        /// Validate this mount belongs to the volume
        #[arg(long)]
        mount: Option<PathBuf>,
    },
    /// Immediately publish the attached volume and print its new manifest id
    Checkpoint {
        volume: ulid::Ulid,
        /// Validate this mount belongs to the volume
        #[arg(long)]
        mount: Option<PathBuf>,
    },
    /// Flush an attached volume and print its immutable manifest id
    Snapshot { volume: ulid::Ulid },
    /// Create an independent writer from the last committed manifest
    Clone { volume: ulid::Ulid },
    /// Unmount, perform a final flush, and disconnect the foreground server
    Detach { volume: ulid::Ulid },
    /// List every volume
    Ls,
    /// Print the volume and its manifest history, newest first
    Show { volume: ulid::Ulid },
}
