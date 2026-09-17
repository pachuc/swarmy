use std::path::PathBuf;

#[derive(clap::Subcommand)]
pub enum Command {
    /// Measure both scripted fake-provider turn shapes on the configured stack
    Turn {
        /// Measured turns per shape, after one excluded warmup each
        #[arg(long, default_value_t = 20, value_parser = clap::value_parser!(u32).range(1..))]
        turns: u32,
        /// Registered image for the trivial bash call
        #[arg(long)]
        image: String,
        /// Save complete timelines, including warmups, as JSON
        #[arg(long)]
        output: Option<PathBuf>,
        /// Deadline per turn, including receipt of all observations
        #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..))]
        timeout_secs: u64,
    },
}
