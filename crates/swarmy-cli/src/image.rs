use anyhow::Result;
use swarmy_image::{Recipe, validate_label};

/// Build a recipe locally and register it through the control plane.
pub async fn build(
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
    if json {
        println!(
            "{}",
            serde_json::to_string(&crate::client_commands::ImageBuilt::from_upload(&uploaded))?
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
