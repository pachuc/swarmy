use std::sync::Arc;

use swarmy_store::{Store, blob::ObjectBlobStore};

pub async fn run(dry_run: bool, json: bool) -> anyhow::Result<()> {
    let settings = swarmy_config::Settings::load()?.settings;
    let blobs = Arc::new(ObjectBlobStore::new(settings.object_store()?));
    let objects = blobs.object_store();
    let directory: Vec<_> = settings
        .store_directory
        .split('/')
        .map(str::to_owned)
        .collect();
    anyhow::ensure!(
        directory.iter().all(|part| !part.is_empty()),
        "empty store directory component"
    );
    let store = Store::open(Some(&settings.fdb_cluster_file), Some(&directory), blobs).await?;
    let run = swarmy_volume::gc::collect(&store, objects, settings.gc, dry_run).await?;
    if !dry_run {
        let days = settings.metering.raw_retention_days.get();
        if let Some(cutoff) = jiff::Timestamp::now()
            .as_second()
            .checked_sub(
                i64::try_from(days)
                    .unwrap_or(i64::MAX)
                    .saturating_mul(86_400),
            )
            .and_then(|second| jiff::Timestamp::from_second(second).ok())
        {
            let pruned = store
                .prune_metering_raw(cutoff, swarmy_store::MAX_SCAN_LIMIT)
                .await?;
            if pruned > 0 {
                tracing::info!(pruned, "metering raw records pruned");
            }
        }
    }
    if json {
        println!("{}", serde_json::to_string(&run)?);
    } else {
        println!(
            "Run {}: {} manifests, {} chunks scanned; {} candidates ({} bytes); {} deleted ({} bytes freed); {} ms{}",
            run.owner,
            run.manifests,
            run.scanned,
            run.candidates,
            run.candidate_bytes,
            run.deleted,
            run.bytes_freed,
            run.duration_ms,
            if dry_run { " (dry run)" } else { "" }
        );
    }
    Ok(())
}
