use std::sync::Arc;

use swarmy_store::{Store, blob::ObjectBlobStore};

pub async fn run(dry_run: bool, json: bool) -> anyhow::Result<()> {
    let settings = swarmy_config::Settings::load()?.settings;
    let blobs = Arc::new(ObjectBlobStore::from_env()?);
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
