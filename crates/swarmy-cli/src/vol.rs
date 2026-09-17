use crate::{conversation::store, vol_command::Command};
use anyhow::{Context, Result};
use std::fmt::Write;
use swarmy_core::{ImageTag, VolumeId};
use swarmy_store::{MAX_SCAN_LIMIT, Store};

pub async fn run(command: Command, json: bool) -> Result<()> {
    match command {
        Command::Attach {
            volume,
            device,
            background,
        } => {
            crate::vol_server::attach(VolumeId::from_ulid(volume), device, background, json)
                .await?;
        }
        Command::Flush {
            volume,
            mount,
            freeze,
        } => {
            crate::vol_server::flush(VolumeId::from_ulid(volume), mount, freeze, json).await?;
        }
        Command::Checkpoint { volume, mount } => {
            crate::vol_server::control(VolumeId::from_ulid(volume), mount, false, json).await?;
        }
        Command::Snapshot { volume } => {
            let id = VolumeId::from_ulid(volume);
            let store = store().await?;
            let record = store.get_volume(id).await?.context("volume not found")?;
            if record
                .writer_lease
                .as_ref()
                .is_some_and(|lease| lease.expires_at > jiff::Timestamp::now())
            {
                crate::vol_server::control(id, None, false, json).await?;
            } else {
                output(
                    &serde_json::json!({"volume_id": id, "manifest_id": record.head_manifest}),
                    &record.head_manifest.to_string(),
                    json,
                )?;
            }
        }
        Command::Detach { volume } => {
            crate::vol_server::control(VolumeId::from_ulid(volume), None, true, json).await?;
        }
        other => inspect(other, &store().await?, json).await?,
    }
    Ok(())
}

async fn inspect(command: Command, store: &Store, json: bool) -> Result<()> {
    match command {
        Command::Create { image } => {
            let (name, tag) = image.split_once(':').context("expected NAME:TAG")?;
            swarmy_volume::image::validate_label(name)?;
            swarmy_volume::image::validate_label(tag)?;
            let manifest = store
                .get_image(name, &ImageTag(tag.into()))
                .await?
                .context("image not found")?;
            let id = VolumeId::from_ulid(ulid::Ulid::generate());
            store.create_volume(id, manifest).await?;
            output(
                &serde_json::json!({"volume_id": id, "manifest_id": manifest}),
                &id.to_string(),
                json,
            )?;
        }
        Command::Clone { volume } => {
            let id = VolumeId::from_ulid(ulid::Ulid::generate());
            store.clone_volume(VolumeId::from_ulid(volume), id).await?;
            output(
                &serde_json::json!({"volume_id": id, "parent": volume}),
                &id.to_string(),
                json,
            )?;
        }
        Command::Ls => {
            let mut after = None;
            loop {
                let page = store.list_volumes(after, MAX_SCAN_LIMIT).await?;
                if page.is_empty() {
                    break;
                }
                for (id, record) in page {
                    output(
                        &serde_json::json!({"volume_id": id, "record": record}),
                        &format!("{id} {}", record.head_manifest),
                        json,
                    )?;
                    after = Some(id);
                }
            }
        }
        Command::Show { volume } => {
            let id = VolumeId::from_ulid(volume);
            let record = store.get_volume(id).await?.context("volume not found")?;

            let mut chain = Vec::new();
            let mut text = format!(
                "{id} parent={}",
                record
                    .parent
                    .map_or_else(|| "-".into(), |parent| parent.to_string())
            );
            for manifest in store.volume_snapshots(id).await? {
                let header = store
                    .get_manifest(manifest)
                    .await?
                    .context("manifest missing")?;
                write!(
                    text,
                    "\n{manifest} size={} root_hash={}",
                    header.size, header.root_hash
                )?;
                chain.push(serde_json::json!({"manifest_id": manifest, "header": header}));
            }
            output(
                &serde_json::json!({"volume_id": id, "record": record, "manifests": chain}),
                &text,
                json,
            )?;
        }
        _ => unreachable!("attachment commands handled by run"),
    }
    Ok(())
}

pub fn output(value: &serde_json::Value, text: &str, json: bool) -> Result<()> {
    use std::io::Write;
    if json {
        println!("{value}");
    } else {
        println!("{text}");
    }
    std::io::stdout().flush()?;
    Ok(())
}
