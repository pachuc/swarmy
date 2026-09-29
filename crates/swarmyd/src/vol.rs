use crate::vol_command::Command;
use anyhow::{Context, Result};
use std::{fmt::Write, sync::Arc};
use swarmy_core::{ImageTag, VolumeId};
use swarmy_store::{MAX_SCAN_LIMIT, Store};

/// Developer volume tools, served from the node daemon: attach and snapshot
/// need local devices and the store, neither of which the client links.
#[derive(clap::Parser)]
#[command(name = "vol", about = "Create, attach, and snapshot durable volumes")]
pub struct VolCli {
    /// Emit compact machine-readable JSON
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

pub async fn run_cli() -> Result<()> {
    let cli = match <VolCli as clap::Parser>::try_parse_from(
        std::iter::once("vol".to_owned()).chain(std::env::args().skip(2)),
    ) {
        Ok(cli) => cli,
        // Help and version exit from here with their own status codes.
        Err(error) => error.exit(),
    };
    run(cli.command, cli.json).await
}

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

/// Open the cluster store for volume commands.
pub(crate) async fn store() -> Result<swarmy_store::Store> {
    let settings = swarmy_config::Settings::load()?.settings;
    let cluster = settings.fdb_cluster_file;
    let directory: Vec<_> = settings
        .store_directory
        .split('/')
        .map(str::to_owned)
        .collect();
    Ok(swarmy_store::Store::open(
        Some(&cluster),
        Some(&directory),
        Arc::new(swarmy_store::blob::ObjectBlobStore::from_env()?),
    )
    .await?)
}
