use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::aws::AmazonS3Builder;
use swarmy_core::{ImageTag, ManifestId};
use swarmy_store::MAX_SCAN_LIMIT;
use swarmy_volume::image::{Recipe, build_ext4, upload_image, validate_label};

use crate::{conversation::store, image_command::Command};

pub async fn run(command: Command, json: bool) -> Result<()> {
    match command {
        Command::Build {
            recipe,
            tag,
            output,
        } => build(recipe, tag, output, json).await?,
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
            let header = store
                .get_manifest(manifest_id)
                .await?
                .context("image manifest is missing")?;
            if json {
                println!(
                    "{}",
                    serde_json::json!({"name": name, "tag": tag, "manifest_id": manifest_id, "header": header})
                );
            } else {
                println!(
                    "{image} {manifest_id}\nsize={} chunk_size={} root_hash={}",
                    header.size, header.chunk_size, header.root_hash
                );
            }
        }
    }
    Ok(())
}

async fn build(
    path: std::path::PathBuf,
    tag: String,
    output: Option<std::path::PathBuf>,
    json: bool,
) -> Result<()> {
    validate_label(&tag)?;
    let (recipe, directory, name) = Recipe::load(&path)?;
    let store = store().await?;
    let settings = swarmy_config::Settings::load()?.settings;
    let objects = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(settings.s3_endpoint)
            .with_access_key_id(settings.s3_access_key)
            .with_secret_access_key(settings.s3_secret_key)
            .with_bucket_name(settings.s3_bucket)
            .with_region(settings.s3_region)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .build()?,
    );
    let image = tokio::task::spawn_blocking(move || build_ext4(&recipe, &directory)).await??;
    let built = upload_image(image.path(), objects).await?;
    if let Some(output) = output {
        // Refuse to overwrite an existing image, including through a symlink.
        let destination = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&output)?;
        let status = std::process::Command::new("cp")
            .args(["--sparse=always", "--"])
            .arg(image.path())
            .arg(&output)
            .status()?;
        drop(destination);
        anyhow::ensure!(status.success(), "copying ext4 image failed: {status}");
    }
    let manifest_id = ManifestId::from_ulid(ulid::Ulid::generate());
    store.put_manifest(manifest_id, &built.header).await?;
    store
        .put_image(&name, &ImageTag(tag.clone()), manifest_id)
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
