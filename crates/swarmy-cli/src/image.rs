use anyhow::{Context, Result};
use swarmy_image::{Recipe, validate_label};

use crate::image_command::Command;

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
    // The ext4 file is built locally; chunk publication and registration run
    // on the control plane so the client never needs object store credentials.
    let image = tokio::task::spawn_blocking(move || swarmy_image::build_ext4(&recipe, &directory))
        .await??;
    let (client, endpoint) = crate::api_client::connect()?;
    let uploaded = crate::api_client::call(
        &endpoint,
        client.upload_image(&swarmy_client::UploadImage {
            name: &name,
            tag: &tag,
            idempotency_key: &ulid::Ulid::generate().to_string(),
            scratch: &scratch,
            memory_mib,
            display: recipe_display,
            file: image.path(),
        }),
    )
    .await?;
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
    if json {
        println!(
            "{}",
            serde_json::json!({"event": "image_built", "name": uploaded.name, "tag": uploaded.tag, "manifest_id": uploaded.manifest_id, "header": uploaded.header, "size": uploaded.size, "chunks_total": uploaded.chunks_total, "chunks_stored": uploaded.chunks_stored, "chunks_uploaded": uploaded.chunks_uploaded})
        );
    } else {
        println!(
            "{}:{} {}\nsize={} bytes chunks_stored={} chunks_uploaded={} chunks_total={}",
            uploaded.name,
            uploaded.tag,
            uploaded.manifest_id,
            uploaded.size,
            uploaded.chunks_stored,
            uploaded.chunks_uploaded,
            uploaded.chunks_total
        );
    }
    Ok(())
}
