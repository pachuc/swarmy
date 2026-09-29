use anyhow::{Context, Result};
use swarmy_image::{Recipe, validate_label};

/// Run one image command. One match owns every variant: builds run locally
/// without a prior connection, while reads connect on their own path.
pub(crate) async fn run(command: crate::image_command::Command, json: bool) -> Result<()> {
    use crate::image_command::Command;
    match command {
        Command::Build {
            recipe,
            tag,
            name,
            output,
        } => build(recipe, tag, name, output, json).await,
        Command::Ls => {
            let (client, endpoint) = swarmy_client::api_client::connect()?;
            let mut after = None;
            loop {
                let page = swarmy_client::api_client::call(
                    &endpoint,
                    client.images(after.as_deref(), 256),
                )
                .await?;
                if page.is_empty() {
                    break;
                }
                for image in page {
                    if json {
                        println!("{}", serde_json::to_string(&image)?);
                    } else {
                        println!("{}:{} {}", image.name, image.tag, image.manifest_id);
                    }
                    after = Some(format!("{}:{}", image.name, image.tag));
                }
            }
            Ok(())
        }
        Command::Show { image } => {
            let (name, tag) = image.split_once(':').context("expected NAME:TAG")?;
            validate_label(name)?;
            validate_label(tag)?;
            let (client, endpoint) = swarmy_client::api_client::connect()?;
            let value = client.image(name, tag).await.map_err(|error| {
                if matches!(&error, swarmy_client::Error::Api { body, .. } if body.code == "image_not_found") {
                    anyhow::anyhow!("image not found")
                } else {
                    swarmy_client::api_client::api_error(error, &endpoint)
                }
            })?;
            let header = value.header.as_ref().context("image header missing")?;
            if json {
                println!("{}", serde_json::to_string(&value)?);
            } else {
                println!(
                    "{image} {}\nsize={} chunk_size={} root_hash={} scratch={}",
                    value.manifest_id,
                    header.size,
                    header.chunk_size,
                    header.root_hash,
                    value
                        .scratch
                        .as_ref()
                        .context("image scratch missing")?
                        .join(",")
                );
            }
            Ok(())
        }
    }
}

/// Build a recipe locally and register it through the control plane.
pub(crate) async fn build(
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
    let (client, endpoint) = swarmy_client::api_client::connect()?;
    let uploaded = swarmy_client::api_client::call_upload(
        &endpoint,
        image.path(),
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
        crate::client_commands::print_event(&crate::client_commands::Event::image_built(&uploaded));
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
