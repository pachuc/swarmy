//! Files a remote node runs, compiled into the client so provisioning needs no checkout.
use crate::Result;
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

/// Provisioning entrypoint.
pub const PROVISION: &str = "scripts/remote-provision.sh";
/// Upgrade entrypoint.
pub const UPGRADE: &str = "scripts/remote-upgrade.sh";
/// Services entrypoint.
pub const SERVICES: &str = "scripts/remote-services.sh";
/// Decommission entrypoint.
pub const DECOMMISSION: &str = "scripts/remote-decommission.sh";
/// Shared environment for remote scripts and SSH stdin.
pub const REMOTE_PROVISION_ENV: &str = include_str!("../../../scripts/remote-provision-env.sh");
/// Installer shared with `swarmy dev install-tools`.
pub const INSTALL_DEV_TOOLS: &str = include_str!("../../../scripts/install-dev-tools.sh");
/// Backing stack shared with `swarmy dev`.
pub const DEV_STACK: &str = include_str!("../../../scripts/dev-stack.sh");

/// Files uploaded to the node directory, sorted by path.
pub const FILES: &[(&str, &str)] = &[
    (
        "images/base-desktop/recipe.toml",
        include_str!("../../../images/base-desktop/recipe.toml"),
    ),
    (
        "images/base-desktop/setup.sh",
        include_str!("../../../images/base-desktop/setup.sh"),
    ),
    (
        "images/base-ubuntu/recipe.toml",
        include_str!("../../../images/base-ubuntu/recipe.toml"),
    ),
    (
        "images/common/agent-setup.sh",
        include_str!("../../../images/common/agent-setup.sh"),
    ),
    (
        "images/swarmy-dev/recipe.toml",
        include_str!("../../../images/swarmy-dev/recipe.toml"),
    ),
    (
        "images/swarmy-dev/setup.sh",
        include_str!("../../../images/swarmy-dev/setup.sh"),
    ),
    ("scripts/dev-stack.sh", DEV_STACK),
    ("scripts/install-dev-tools.sh", INSTALL_DEV_TOOLS),
    (
        DECOMMISSION,
        include_str!("../../../scripts/remote-decommission.sh"),
    ),
    ("scripts/remote-provision-env.sh", REMOTE_PROVISION_ENV),
    (
        PROVISION,
        include_str!("../../../scripts/remote-provision.sh"),
    ),
    (
        "scripts/remote-s3-env.sh",
        include_str!("../../../scripts/remote-s3-env.sh"),
    ),
    (
        SERVICES,
        include_str!("../../../scripts/remote-services.sh"),
    ),
    (
        "scripts/remote-upgrade-lib.sh",
        include_str!("../../../scripts/remote-upgrade-lib.sh"),
    ),
    (UPGRADE, include_str!("../../../scripts/remote-upgrade.sh")),
];

/// Write the bundled files under `dir`, making scripts executable.
///
/// # Errors
/// Reports filesystem failures.
pub fn write_to(dir: &Path) -> Result<()> {
    fs::create_dir_all(dir)?;
    fs::set_permissions(dir, fs::Permissions::from_mode(0o755))?;
    for &(path, contents) in FILES {
        let file = dir.join(path);
        if let Some(parent) = file.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&file, contents)?;
        fs::set_permissions(
            &file,
            fs::Permissions::from_mode(if path.ends_with(".sh") { 0o755 } else { 0o644 }),
        )?;
    }
    Ok(())
}

/// Recipe directories carried by the client, sorted.
#[must_use]
pub fn recipes() -> Vec<&'static str> {
    FILES
        .iter()
        .filter_map(|(path, _)| path.strip_suffix("/recipe.toml"))
        .collect()
}

/// Validate a recipe against the carried recipes.
///
/// # Errors
/// Rejects recipes that cannot be uploaded by this client.
pub fn recipe(path: &Path) -> Result<PathBuf> {
    let raw = path.to_str().unwrap_or_default();
    let normalized = raw.trim_start_matches("./").trim_end_matches('/');
    if recipes().contains(&normalized) {
        Ok(PathBuf::from(normalized))
    } else {
        Err(crate::Error::other(format!(
            "image recipe {raw} is not carried by this client; choose one of {}, or build a custom recipe with swarmy image build --remote NAME DIR",
            recipes().join(", ")
        )))
    }
}
