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
    if Some(target) != Target::host().ok()
        && !matches!(
            crate::core_command::CoreSource::select(core_bin.clone())?,
            crate::core_command::CoreSource::Release(_)
        )
    {
        anyhow::bail!(
            "--target only selects a release download; --core-bin and development builds use this machine's binary"
        );
    }
    let core = crate::core_command::resolve_core(core_bin, Some(target)).await?;
    if Some(target) == Target::host().ok() {
        crate::core_command::check_versions(&core, allow_version_mismatch).await?;
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

use anyhow::{Context, Result, bail, ensure};
use std::{path::Path, process::Stdio, time::Duration};
use tokio::{process::Command as Process, time::timeout};
pub(crate) enum CoreSource {
    Explicit(PathBuf),
    Release(swarmy_cloud::release::Release),
    Development,
}
impl CoreSource {
    pub(crate) fn select(core_bin: Option<PathBuf>) -> Result<Self> {
        if let Some(path) = core_bin {
            return Ok(Self::Explicit(path));
        }
        Ok(swarmy_cloud::release::Release::for_client()?.map_or(Self::Development, Self::Release))
    }
    pub(crate) fn fix(&self) -> Result<String> {
        Ok(match self {
            Self::Explicit(path) => format!(
                "Rebuild {} at commit {} (cargo build -p swarmy-core-bin) or unset --core-bin/SWARMY_CORE_BIN",
                path.display(),
                swarmy_version::GIT_COMMIT
            ),
            Self::Release(release) => format!(
                "Remove {}/{} and run the command again to download {}",
                core_cache()?.display(),
                release.tag(),
                release.tag()
            ),
            Self::Development => format!("Reinstall from the CLI checkout: {REINSTALL}"),
        })
    }
    pub(crate) const fn label(&self) -> &'static str {
        match self {
            Self::Explicit(_) => "explicit",
            Self::Release(_) => "release",
            Self::Development => "development",
        }
    }
}
pub(crate) struct ResolvedCore {
    pub(crate) path: PathBuf,
    pub(crate) source: &'static str,
    pub(crate) fix: String,
}
/// `core/` in swarmy's XDG cache directory: downloaded swarmy-core releases.
pub(crate) fn core_cache() -> Result<PathBuf> {
    Ok(swarmy_config::paths::Dirs::from_process()?
        .cache
        .join("core"))
}
/// `images/` in swarmy's XDG cache directory: release base-image downloads.
pub(crate) fn image_cache() -> Result<PathBuf> {
    Ok(swarmy_config::paths::Dirs::from_process()?
        .cache
        .join("images"))
}
/// Set `selection.default_image` in CONFIG, keeping the rest of the file.
pub(crate) fn set_default_image(config: &Path, image: &str) -> Result<()> {
    let mut settings = Settings::read(config)?;
    settings.selection.default_image = Some(image.to_owned());
    swarmy_config::write_private(config, settings.to_toml()?.as_bytes())?;
    Ok(())
}
pub(crate) async fn resolve_core(
    core_bin: Option<PathBuf>,
    target: Option<swarmy_cloud::release::Target>,
) -> Result<ResolvedCore> {
    use swarmy_cloud::release::{Component, Target};
    let source = CoreSource::select(core_bin)?;
    let label = source.label();
    let fix = source.fix()?;
    let path = match source {
        CoreSource::Explicit(path) => path.canonicalize().with_context(|| {
            format!(
                "--core-bin/SWARMY_CORE_BIN {} does not exist",
                path.display()
            )
        })?,
        CoreSource::Release(release) => {
            let cache = core_cache()?;
            release
                .ensure(
                    &cache,
                    Component::Core,
                    target.map_or_else(Target::host, Ok)?,
                )
                .await?
                .join("swarmy-core")
        }
        CoreSource::Development => service_binary("swarmy-core")?,
    };
    Ok(ResolvedCore {
        path,
        source: label,
        fix,
    })
}
pub(crate) async fn check_versions(
    core: &ResolvedCore,
    allow_version_mismatch: bool,
) -> Result<()> {
    if let Err(error) = version_check(&core.path, "swarmy-core").await {
        let message = format!("{error}. {}", core.fix);
        if !allow_version_mismatch {
            bail!("{message}\nUse --allow-version-mismatch to override.");
        }
        eprintln!("warning: {message} (--allow-version-mismatch)");
    }
    Ok(())
}

// Keep this command aligned with docs/DEV.md's no-root installation workflow.
pub(crate) const REINSTALL: &str = "for crate in swarmy-cli swarmy-core-bin; do SWARMY_FDB_LIB_DIR=\"$HOME/.local/lib\" cargo install --locked --path \"crates/$crate\"; done";

pub(crate) fn service_binary(name: &str) -> Result<PathBuf> {
    let executable =
        crate::tools::find(name).unwrap_or(std::env::current_exe()?.with_file_name(name));
    executable.canonicalize().with_context(|| {
        format!(
            "missing {name} at {}; reinstall: {REINSTALL}",
            executable.display()
        )
    })
}

pub(crate) async fn version_check(executable: &Path, name: &str) -> Result<String> {
    let output = timeout(
        Duration::from_secs(5),
        Process::new(executable)
            .arg("--version")
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .with_context(|| format!("{name} version check timed out"))?
    .with_context(|| format!("cannot run {name} --version"))?;
    ensure!(
        output.status.success(),
        "{name} version check failed ({})",
        output.status
    );
    let reported = String::from_utf8(output.stdout)
        .with_context(|| format!("{name} reported a non-UTF-8 version"))?;
    let reported = reported.trim();
    let identity = reported
        .strip_prefix(name)
        .and_then(|rest| rest.strip_prefix(' '))
        .unwrap_or(reported);
    ensure!(
        identity == swarmy_version::IDENTITY,
        "{name} version mismatch: found {identity:?}, CLI expects {}",
        swarmy_version::IDENTITY
    );
    Ok(format!("{}: {identity}", executable.display()))
}
