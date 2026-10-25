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
            fs::Permissions::from_mode(
                if Path::new(path)
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("sh"))
                {
                    0o755
                } else {
                    0o644
                },
            ),
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

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use std::collections::BTreeSet;

    fn root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
    }

    #[test]
    fn table_matches_repository_files() {
        let paths: Vec<_> = FILES.iter().map(|(path, _)| *path).collect();
        assert!(paths.windows(2).all(|pair| pair[0] < pair[1]));
        for &(path, text) in FILES {
            assert_eq!(fs::read_to_string(root().join(path)).unwrap(), text);
        }
        for (path, text) in [
            ("scripts/remote-provision-env.sh", REMOTE_PROVISION_ENV),
            ("scripts/install-dev-tools.sh", INSTALL_DEV_TOOLS),
            ("scripts/dev-stack.sh", DEV_STACK),
        ] {
            assert_eq!(
                FILES.iter().find(|(entry, _)| *entry == path).unwrap().1,
                text
            );
        }
    }

    fn normalize(path: &Path) -> String {
        let mut parts = Vec::new();
        for part in path.components() {
            match part {
                std::path::Component::ParentDir => {
                    parts.pop();
                }
                std::path::Component::Normal(name) => parts.push(name.to_str().unwrap()),
                _ => {}
            }
        }
        parts.join("/")
    }

    fn script_references(text: &str, directory: &Path) -> Vec<String> {
        let mut refs = Vec::new();
        for line in text.lines() {
            let mut rest = line;
            while let Some(start) = rest.find("$(dirname -- \"${BASH_SOURCE[0]}\")/") {
                rest = &rest[start + "$(dirname -- \"${BASH_SOURCE[0]}\")/".len()..];
                let name: String = rest
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || "_.-".contains(*c))
                    .collect();
                if name.ends_with(".sh") {
                    refs.push(normalize(&directory.join(name)));
                }
            }
            let mut rest = line;
            while let Some(start) = rest.find("scripts/") {
                rest = &rest[start + "scripts/".len()..];
                let name: String = rest
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || "_.-".contains(*c))
                    .collect();
                if name.ends_with(".sh") {
                    refs.push(format!("scripts/{name}"));
                }
                rest = &rest[name.len()..];
            }
        }
        refs
    }

    #[test]
    fn table_is_the_node_closure() {
        let root = root();
        let mut pending = vec![
            PROVISION.to_owned(),
            UPGRADE.to_owned(),
            SERVICES.to_owned(),
            DECOMMISSION.to_owned(),
        ];
        for entry in fs::read_dir(root.join("images")).unwrap() {
            let recipe = entry.unwrap().path().join("recipe.toml");
            if recipe.is_file() {
                pending.push(normalize(recipe.strip_prefix(&root).unwrap()));
            }
        }
        let mut closure = BTreeSet::new();
        while let Some(path) = pending.pop() {
            if !closure.insert(path.clone()) {
                continue;
            }
            let text = fs::read_to_string(root.join(&path)).unwrap();
            let dir = Path::new(&path).parent().unwrap();
            if Path::new(path)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("sh"))
            {
                pending.extend(script_references(&text, dir));
            }
            if path.ends_with("recipe.toml") {
                let recipe: toml::Value = text.parse().unwrap();
                if let Some(source) = recipe.get("source") {
                    if let Some(script) = source.get("script").and_then(toml::Value::as_str) {
                        pending.push(normalize(&dir.join(script)));
                    }
                    if let Some(scripts) = source.get("scripts").and_then(toml::Value::as_array) {
                        for script in scripts {
                            pending.push(normalize(&dir.join(script.as_str().unwrap())));
                        }
                    }
                }
            }
        }
        let bundled: BTreeSet<_> = FILES.iter().map(|(path, _)| (*path).to_owned()).collect();
        assert_eq!(
            closure,
            bundled,
            "missing: {:?}; excess: {:?}",
            closure.difference(&bundled).collect::<Vec<_>>(),
            bundled.difference(&closure).collect::<Vec<_>>()
        );
    }

    #[test]
    fn recipe_accepts_only_carried_recipes() {
        for path in [
            "images/base-ubuntu",
            "./images/swarmy-dev/",
            "images/base-desktop",
        ] {
            assert!(recipe(Path::new(path)).is_ok());
        }
        for path in [
            "images/custom",
            "/abs/images/base-ubuntu",
            "images/../scripts",
            "images/common",
        ] {
            let error = recipe(Path::new(path)).unwrap_err().to_string();
            assert!(
                error.contains("images/base-ubuntu")
                    && error.contains("images/base-desktop")
                    && error.contains("images/swarmy-dev")
                    && error.contains("swarmy image build --remote")
            );
        }
    }

    #[test]
    fn written_files_are_self_contained() {
        if std::process::Command::new("bash")
            .arg("--version")
            .output()
            .is_err()
        {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        write_to(dir.path()).unwrap();
        assert_eq!(
            fs::metadata(dir.path()).unwrap().permissions().mode() & 0o777,
            0o755
        );
        for &(path, _) in FILES {
            if Path::new(path)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("sh"))
            {
                assert_eq!(
                    fs::metadata(dir.path().join(path))
                        .unwrap()
                        .permissions()
                        .mode()
                        & 0o777,
                    0o755
                );
                assert!(
                    std::process::Command::new("bash")
                        .arg("-n")
                        .arg(dir.path().join(path))
                        .status()
                        .unwrap()
                        .success()
                );
            }
        }
        for command in [
            "source scripts/remote-provision-env.sh && parse_provision_args && [[ $(release_bundle_dir /x) == /x/.swarmy/release && $(node_source_dir /x) == /x/source ]]",
            "source scripts/remote-provision-env.sh && [[ $(swarmy_remote_s3_env \"\" \"\" \"\" \"\" false) == *SWARMY_S3_BUCKET=swarmy* ]]",
        ] {
            assert!(
                std::process::Command::new("bash")
                    .current_dir(dir.path())
                    .args(["-c", command])
                    .status()
                    .unwrap()
                    .success()
            );
        }
        for (script, code) in [(UPGRADE, 2), (DECOMMISSION, 1), (SERVICES, 1)] {
            let output = std::process::Command::new("bash")
                .current_dir(dir.path())
                .arg(script)
                .output()
                .unwrap();
            assert_eq!(output.status.code(), Some(code));
            assert!(String::from_utf8_lossy(&output.stderr).contains("Usage:"));
        }
    }
}
