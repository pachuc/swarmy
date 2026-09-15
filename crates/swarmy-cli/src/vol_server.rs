use anyhow::Result;
use std::{path::PathBuf, sync::Arc};
use swarmy_core::VolumeId;
use swarmy_volume::server::{self, ServerConfig};

async fn config() -> Result<ServerConfig> {
    let loaded = swarmy_config::Settings::load()?;
    let settings = &loaded.settings;
    Ok(ServerConfig {
        directory: loaded.root.join(".swarmy/volumes"),
        node: loaded.node_id()?,
        store: crate::conversation::store().await?,
        objects: Arc::new(
            object_store::aws::AmazonS3Builder::new()
                .with_endpoint(&settings.s3_endpoint)
                .with_access_key_id(&settings.s3_access_key)
                .with_secret_access_key(&settings.s3_secret_key)
                .with_bucket_name(&settings.s3_bucket)
                .with_region(&settings.s3_region)
                .with_allow_http(true)
                .with_virtual_hosted_style_request(false)
                .build()?,
        ),
    })
}

pub async fn control(id: VolumeId, mount: Option<PathBuf>, detach: bool, json: bool) -> Result<()> {
    let flushed = server::control_flush(&config().await?, id, mount, detach).await?;
    let mut value = serde_json::to_value(&flushed)?;
    value["volume_id"] = serde_json::to_value(id)?;
    value["detached"] = serde_json::json!(detach);
    crate::vol::output(&value, &flushed.manifest_id.to_string(), json)
}

pub async fn attach(
    id: VolumeId,
    path: Option<PathBuf>,
    background: bool,
    json: bool,
) -> Result<()> {
    anyhow::ensure!(
        rustix::process::geteuid().is_root(),
        "vol attach requires root; run it with sudo -E"
    );
    let config = config().await?;
    let node = config.node;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    server::attach(
        config,
        id,
        path,
        background,
        move |path| {
            crate::vol::output(
                &serde_json::json!({"volume_id": id, "node_id": node, "device": path}),
                &path.display().to_string(),
                json,
            )
            .map_err(|error| server::Error::Message(error.to_string()))
        },
        async move {
            tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
        },
    )
    .await?;
    Ok(())
}
