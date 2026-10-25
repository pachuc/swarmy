use anyhow::{Context, Result, bail};
use std::path::{Path, PathBuf};
use swarmy_cloud::release::{Component, Release, Target};
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
            no_register,
        } => build(recipe, tag, name, output, no_register, json).await,
        Command::Pull {
            name: _,
            tag,
            target,
            set_default,
        } => pull(tag, target, set_default, json).await,
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
                    swarmy_client::api_client::api_error(error, &endpoint).into()
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

/// Pull a release image and optionally persist it as the default.
async fn pull(
    tag: Option<String>,
    target: Option<String>,
    set_default: bool,
    json: bool,
) -> Result<()> {
    let tag = tag.as_deref().unwrap_or(swarmy_version::RELEASE_TAG);
    let target = target.as_deref().map_or_else(Target::host, Target::parse)?;
    let image = release_image(tag);
    if pull_release_image(&crate::dev::image_cache()?, tag, target, json)
        .await?
        .is_none()
    {
        bail!(
            "this is a development build of swarmy with no release to pull from; build the image from a checkout with swarmy image build images/base-ubuntu, or set SWARMY_RELEASE_BASE_URL"
        );
    }
    if set_default {
        let loaded = swarmy_config::Settings::load_base()?;
        let path = loaded.path.context(
            "no configuration file to update; run swarmy dev up first or set SWARMY_DEFAULT_IMAGE",
        )?;
        crate::dev::set_default_image(&path, &image)?;
        if json {
            println!(
                "{}",
                serde_json::json!({"default_image": image, "config": path})
            );
        } else {
            println!(
                "default_image = {image} in {}; run swarmy dev up to restart the API with it",
                path.display()
            );
            if std::env::var_os("SWARMY_DEFAULT_IMAGE").is_some() {
                println!("note: SWARMY_DEFAULT_IMAGE overrides it");
            }
        }
    }
    Ok(())
}

/// Build a recipe locally and register it through the control plane.
pub(crate) async fn build(
    path: PathBuf,
    tag: String,
    name: Option<String>,
    output: Option<PathBuf>,
    no_register: bool,
    json: bool,
) -> Result<()> {
    validate_label(&tag)?;
    let (recipe, directory, directory_name) = Recipe::load(&path)?;
    let name = name.unwrap_or(directory_name);
    validate_label(&name)?;
    validate_sandbox(&recipe.sandbox)?;
    // The ext4 file is built locally; chunk publication and registration run
    // on the control plane so the client never needs object store credentials.
    let (image, sandbox) = tokio::task::spawn_blocking(move || {
        let image = swarmy_image::build_ext4(&recipe, &directory)?;
        Ok::<_, swarmy_image::ImageError>((image, recipe.sandbox))
    })
    .await??;
    if !no_register {
        register(&name, &tag, image.path(), &sandbox, json).await?;
    }
    if let Some(ref output) = output {
        // Refuse to overwrite an existing image, including through a symlink.
        let destination = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output)?;
        drop(destination);
        // A same-filesystem output can reuse the completed image without
        // allocating a second copy of a large development cache.
        if std::fs::rename(image.path(), output).is_err() {
            let status = std::process::Command::new("cp")
                .args(["--sparse=always", "--"])
                .arg(image.path())
                .arg(output)
                .status()?;
            anyhow::ensure!(status.success(), "copying ext4 image failed: {status}");
        }
    }
    if no_register {
        let output = output.context("--no-register requires --output")?;
        let size = std::fs::metadata(&output)?.len();
        if json {
            println!("{}", serde_json::json!({"path": output, "size": size}));
        } else {
            println!("{} {size} bytes", output.display());
        }
    }
    Ok(())
}

fn validate_sandbox(sandbox: &swarmy_image::SandboxRecipe) -> Result<()> {
    anyhow::ensure!(
        sandbox.memory_mib.is_none_or(|m| m > 0),
        "sandbox memory_mib must be positive"
    );
    Ok(())
}

/// Publish a local sparse ext4 through the control plane and print the result.
async fn register(
    name: &str,
    tag: &str,
    file: &Path,
    sandbox: &swarmy_image::SandboxRecipe,
    json: bool,
) -> Result<swarmy_api_types::ImageUpload> {
    validate_sandbox(sandbox)?;
    let (client, endpoint) = swarmy_client::api_client::connect()?;
    let uploaded = swarmy_client::api_client::call_upload(
        &endpoint,
        file,
        client.upload_image(&swarmy_client::UploadImage {
            name,
            tag,
            idempotency_key: &ulid::Ulid::generate().to_string(),
            scratch: &sandbox.scratch,
            memory_mib: sandbox.memory_mib,
            display: sandbox.display,
            file,
        }),
    )
    .await?;
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
    Ok(uploaded)
}

/// The registered reference for a release image.
pub(crate) fn release_image(tag: &str) -> String {
    format!("base-ubuntu:{tag}")
}
/// True when DEFAULT names a release base image other than this client's.
pub(crate) fn other_release_image(default: &str) -> bool {
    default.starts_with("base-ubuntu:v") && default != release_image(swarmy_version::RELEASE_TAG)
}
/// Register the release's base image; None for development clients.
pub(crate) async fn pull_release_image(
    cache: &Path,
    tag: &str,
    target: Target,
    json: bool,
) -> Result<Option<bool>> {
    validate_label(tag)?;
    let Some(release) = Release::for_client()? else {
        return Ok(None);
    };
    let (client, endpoint) = swarmy_client::api_client::connect()?;
    match client.image("base-ubuntu", tag).await {
        Ok(_) => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({"image": release_image(tag), "registered": false})
                );
            } else {
                println!("{} is already registered", release_image(tag));
            }
            return Ok(Some(false));
        }
        Err(swarmy_client::Error::Api { body, .. }) if body.code == "image_not_found" => {}
        Err(error) => return Err(swarmy_client::api_client::api_error(error, &endpoint).into()),
    }
    let dir = release.ensure(cache, Component::BaseImage, target).await?;
    let (recipe, _, _) = Recipe::load(&dir.join("recipe.toml"))?;
    let disk = dir.join("disk.ext4");
    let len = std::fs::metadata(&disk)?.len();
    anyhow::ensure!(
        len == recipe.disk_size,
        "disk.ext4 is {len} bytes but its recipe declares {}",
        recipe.disk_size
    );
    register("base-ubuntu", tag, &disk, &recipe.sandbox, json).await?;
    std::fs::remove_dir_all(&dir)?;
    Ok(Some(true))
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    #[test]
    fn release_names() {
        assert_eq!(release_image("v1.2.3"), "base-ubuntu:v1.2.3");
        assert!(other_release_image("base-ubuntu:v0.0.1"));
        for name in [
            release_image(swarmy_version::RELEASE_TAG),
            "base-ubuntu:noble".into(),
            "fixture:test".into(),
        ] {
            assert!(!other_release_image(&name));
        }
    }
}
