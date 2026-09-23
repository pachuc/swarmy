use anyhow::{Context, Result};
use swarmy_core::{ImageTag, ManifestId};
use swarmy_store::MAX_SCAN_LIMIT;
use swarmy_volume::image::{Recipe, build_ext4, upload_image_protected, validate_label};

use crate::{conversation::store, image_command::Command};

pub async fn run(command: Command, json: bool) -> Result<()> {
    match command {
        Command::Build {
            recipe,
            tag,
            name,
            output,
        } => build(recipe, tag, name, output, json).await?,
        Command::Ls => {
            let store = store().await?;
            let mut after: Option<swarmy_core::ImageRecord> = None;
            loop {
                let images = store
                    .list_images(
                        after
                            .as_ref()
                            .map(|image| (image.name.as_str(), &image.tag)),
                        MAX_SCAN_LIMIT,
                    )
                    .await?;
                if images.is_empty() {
                    break;
                }
                for image in images {
                    if json {
                        println!("{}", serde_json::to_string(&image)?);
                    } else {
                        println!("{}:{} {}", image.name, image.tag.0, image.manifest_id);
                    }
                    after = Some(image);
                }
            }
        }
        Command::Show { image } => {
            let (name, tag) = image.split_once(':').context("expected NAME:TAG")?;
            validate_label(name)?;
            validate_label(tag)?;
            let store = store().await?;
            let manifest_id = store
                .get_image(name, &ImageTag(tag.into()))
                .await?
                .context("image not found")?;
            let scratch = store
                .image_scratch(&swarmy_core::ImageRecord {
                    name: name.into(),
                    tag: ImageTag(tag.into()),
                    manifest_id,
                })
                .await?;
            let header = store
                .get_manifest(manifest_id)
                .await?
                .context("image manifest is missing")?;
            if json {
                println!(
                    "{}",
                    serde_json::json!({"name": name, "tag": tag, "manifest_id": manifest_id, "header": header, "scratch": scratch})
                );
            } else {
                println!(
                    "{image} {manifest_id}\nsize={} chunk_size={} root_hash={} scratch={}",
                    header.size,
                    header.chunk_size,
                    header.root_hash,
                    scratch.join(",")
                );
            }
        }
    }
    Ok(())
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
        .put_image_with_scratch(&name, &ImageTag(tag.clone()), manifest_id, &scratch)
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
