use clap::Subcommand;
use std::path::PathBuf;
use swarmy_cloud::release::Target;

#[derive(Subcommand)]
pub(crate) enum Command {
    /// Download and verify the matching swarmy-core release (or check --core-bin) and print its path
    Ensure {
        #[arg(long, env = "SWARMY_CORE_BIN", value_name = "PATH")]
        core_bin: Option<PathBuf>,
        /// Linux target triple to fetch; defaults to this machine
        #[arg(long)]
        target: Option<String>,
    },
}
pub(crate) async fn run(
    command: Command,
    json: bool,
    allow_version_mismatch: bool,
) -> anyhow::Result<()> {
    let Command::Ensure { core_bin, target } = command;
    let target = target
        .as_deref()
        .map(Target::parse)
        .transpose()?
        .map_or_else(Target::host, Ok)?;
    let core = crate::dev::resolve_core(core_bin, Some(target)).await?;
    if Some(target) == Target::host().ok() {
        crate::dev::check_versions(&core, allow_version_mismatch).await?;
    }
    if json {
        println!(
            "{}",
            serde_json::json!({"path": core.path, "source": core.source, "tag": swarmy_version::RELEASE_TAG, "target": target.triple(), "identity": swarmy_version::IDENTITY})
        );
    } else {
        println!(
            "swarmy-core {} ({}, {}): {}",
            swarmy_version::IDENTITY,
            core.source,
            target.triple(),
            core.path.display()
        );
    }
    Ok(())
}
