use std::path::PathBuf;

#[derive(clap::Subcommand)]
pub(crate) enum Command {
    /// Build a recipe directory (or recipe.toml) and register its directory name
    Build {
        recipe: PathBuf,
        #[arg(long, default_value = "dev")]
        tag: String,
        /// Override the registered name (defaults to the recipe directory name)
        #[arg(long)]
        name: Option<String>,
        /// Keep a copy of the raw ext4 file for offline inspection
        #[arg(long)]
        output: Option<PathBuf>,
        /// Build the ext4 file only, without connecting or registering (requires --output; release packaging)
        #[arg(long, requires = "output")]
        no_register: bool,
    },
    /// Download the base image published with this client's release, verify it against SHA256SUMS, and register it
    Pull {
        #[arg(default_value = "base-ubuntu", value_parser = ["base-ubuntu"])]
        name: String,
        /// Tag to register (defaults to this client's release tag)
        #[arg(long)]
        tag: Option<String>,
        /// Linux target triple of the image; defaults to this machine
        #[arg(long)]
        target: Option<String>,
        /// Also write `selection.default_image` to the configuration file (restart with swarmy dev up to apply)
        #[arg(long)]
        set_default: bool,
    },
    /// List registered image names, tags, and manifest ids
    Ls,
    /// Print a registered image's manifest header
    Show { image: String },
}
