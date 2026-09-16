//! Benchmarks share the isolated chaos metadata and object namespace.
use crate::Fixture;
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use std::path::Path;
use swarmy_core::{ManifestId, VolumeId};

pub async fn run(f: &Fixture, binaries: &Path, script: &Path) -> Result<()> {
    let root = f.files.path().join("measurements");
    std::fs::create_dir_all(root.join(".swarmy"))?;
    std::fs::write(root.join(".swarmy/config.toml"), "")?;
    for sample in 0..2 {
        let result = invoke(f, binaries, script, &root, &["prepare", "chaos:test"]).await?;
        tracing::info!(sample, measurements = %result, "persistent volume raw sample");
        for retained in result["retained"]
            .as_array()
            .context("retained snapshots missing")?
        {
            let manifest: ManifestId = serde_json::from_value(retained["manifest_id"].clone())?;
            let volume = VolumeId::from_ulid(ulid::Ulid::generate());
            // A clone at a retained manifest has its own writer and boots without
            // the original attachment's cache or dirty overlay.
            f.store.create_volume(volume, manifest).await?;
            let boot = invoke(
                f,
                binaries,
                script,
                &root,
                &[
                    "boot",
                    &volume.to_string(),
                    &retained["generation"].to_string(),
                    retained["sha256"]
                        .as_str()
                        .context("snapshot hash missing")?,
                ],
            )
            .await?;
            tracing::info!(sample, %manifest, measurement = %boot, "retained snapshot booted after collection");
        }
    }
    Ok(())
}

async fn invoke(
    f: &Fixture,
    binaries: &Path,
    script: &Path,
    root: &Path,
    args: &[&str],
) -> Result<Value> {
    let path = std::env::join_paths(std::iter::once(binaries.to_owned()).chain(
        std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()),
    ))?;
    let output = tokio::process::Command::new("python3")
        .arg(script)
        .args(args)
        .current_dir(root)
        .envs(f.environment.iter().cloned())
        .env("PATH", path)
        .output()
        .await?;
    ensure!(
        output.status.success(),
        "volume benchmark failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(serde_json::from_slice(&output.stdout)?)
}
