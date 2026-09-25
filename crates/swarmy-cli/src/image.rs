use anyhow::{Context, Result};
use swarmy_core::{ImageTag, ManifestId};
use swarmy_volume::image::{Recipe, build_ext4, upload_image_protected, validate_label};

use crate::{image_command::Command, session::store};

pub async fn run(command: Command, json: bool) -> Result<()> {
    match command {
        Command::Build {
            recipe,
            tag,
            name,
            output,
        } => build(recipe, tag, name, output, json).await,
        Command::Show { image } => {
            let (name, tag) = image.split_once(':').context("expected NAME:TAG")?;
            validate_label(name)?;
            validate_label(tag)?;
            anyhow::bail!("image show is handled by the API client")
        }
        Command::Ls => unreachable!("image reads use the API"),
    }
}

async fn build(
    path: std::path::PathBuf,
    tag: String,
    name: Option<String>,
    output: Option<std::path::PathBuf>,
    json: bool,
) -> Result<()> {
    validate_label(&tag)?;
    let (recipe, directory, directory_name) = Recipe::load(&path)?;
    let scratch = recipe.sandbox.scratch.clone();
    let memory_mib = recipe.sandbox.memory_mib;
    let recipe_display = recipe.sandbox.display;
    anyhow::ensure!(
        memory_mib.is_none_or(|m| m > 0),
        "sandbox memory_mib must be positive"
    );
    let name = name.unwrap_or(directory_name);
    validate_label(&name)?;
    let store = store().await?;
    let settings = swarmy_config::Settings::load()?.settings;
    let objects = settings.object_store()?;
    let image = tokio::task::spawn_blocking(move || build_ext4(&recipe, &directory)).await??;
    let built = upload_image_protected(image.path(), objects, store.clone()).await?;
    if let Some(output) = output {
        // Refuse to overwrite an existing image, including through a symlink.
        let destination = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&output)?;
        drop(destination);
        // A same-filesystem output can reuse the completed image without
        // allocating a second copy of a large development cache.
        if std::fs::rename(image.path(), &output).is_err() {
            let status = std::process::Command::new("cp")
                .args(["--sparse=always", "--"])
                .arg(image.path())
                .arg(&output)
                .status()?;
            anyhow::ensure!(status.success(), "copying ext4 image failed: {status}");
        }
    }
    let manifest_id = ManifestId::from_ulid(ulid::Ulid::generate());
    store.put_manifest(manifest_id, &built.header).await?;
    store
        .put_image_with_requirements(
            &name,
            &ImageTag(tag.clone()),
            manifest_id,
            &scratch,
            memory_mib,
            recipe_display,
        )
        .await?;
    if json {
        println!(
            "{}",
            serde_json::json!({"event": "image_built", "name": name, "tag": tag, "manifest_id": manifest_id, "header": built.header, "size": built.header.size, "chunks_total": built.chunks_total, "chunks_stored": built.chunks_stored, "chunks_uploaded": built.chunks_uploaded})
        );
    } else {
        println!(
            "{name}:{tag} {manifest_id}\nsize={} bytes chunks_stored={} chunks_uploaded={} chunks_total={}",
            built.header.size, built.chunks_stored, built.chunks_uploaded, built.chunks_total
        );
    }
    Ok(())
}
