use std::path::PathBuf;

#[derive(clap::Subcommand)]
pub enum Command {
    /// Build a recipe directory (or recipe.toml) and register its directory name
    Build {
        recipe: PathBuf,
        #[arg(long)]
        tag: String,
        /// Override the registered name (defaults to the recipe directory name)
        #[arg(long)]
        name: Option<String>,
        /// Keep a copy of the raw ext4 file for offline inspection
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// List registered image names, tags, and manifest ids
    Ls,
    /// Print a registered image's manifest header
    Show { image: String },
}
