use anyhow::Result;
use std::path::PathBuf;
use swarmy_core::VolumeId;
use swarmy_volume::server::{self, ServerConfig};

async fn config() -> Result<(ServerConfig, swarmy_store::Store)> {
    let loaded = swarmy_config::Settings::load()?;
    let settings = &loaded.settings;
    let store = crate::vol::store().await?;
    Ok((ServerConfig {
        directory: loaded.root.join(".swarmy/volumes"),
        node: loaded.node_id()?,
        metadata: std::sync::Arc::new(store.clone()),
        objects: swarmy_volume::objects::from_settings(settings)?,
    }, store))
}

pub(crate) async fn control(
    id: VolumeId,
    mount: Option<PathBuf>,
    detach: bool,
    json: bool,
) -> Result<()> {
    let flushed = server::control_flush(&config().await?.0, id, mount, detach).await?;
    let mut value = serde_json::to_value(&flushed)?;
    value["volume_id"] = serde_json::to_value(id)?;
    value["detached"] = serde_json::json!(detach);
    crate::vol::output(&value, &flushed.manifest_id.to_string(), json)?;
    Ok(())
}

pub(crate) async fn attach(
    id: VolumeId,
    path: Option<PathBuf>,
    background: bool,
    json: bool,
) -> Result<()> {
    anyhow::ensure!(
        rustix::process::geteuid().is_root(),
        "vol attach requires root; run it with sudo -E"
    );
    let (config, store) = config().await?;
    let node = config.node;
    let grant = store.open_volume_writer(id, swarmy_core::LeaseOwnerId::from_ulid(node.as_ulid())).await?;
    let stop = swarmy_config::shutdown_signal();
    server::attach(
        config,
        grant,
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
        stop,
    )
    .await?;
    Ok(())
}

pub(crate) async fn flush(
    id: VolumeId,
    mount: Option<PathBuf>,
    freeze: bool,
    json: bool,
) -> Result<()> {
    let flushed =
        server::control_flush_with_freeze(&config().await?.0, id, mount, false, freeze).await?;
    let mut value = serde_json::to_value(&flushed)?;
    value["volume_id"] = serde_json::to_value(id)?;
    crate::vol::output(&value, &flushed.manifest_id.to_string(), json)?;
    Ok(())
}
