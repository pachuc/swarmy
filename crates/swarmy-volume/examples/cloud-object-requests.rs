//! Measure serial HEAD and 256 KiB PUT calls through the same S3 client as volumes.
//! Run against a disposable benchmark bucket or run prefix, then delete its objects.
use std::{error::Error, time::Instant};

use object_store::{PutMode, path::Path};
use swarmy_core::CHUNK_SIZE;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let store = swarmy_store::objects::from_settings(&swarmy_config::Settings::load()?.settings)?;
    for sample in 0..20 {
        let id = ulid::Ulid::generate();
        let path = Path::from(format!("request-probe/{id}"));
        let mut bytes = vec![0; CHUNK_SIZE as usize];
        blake3::Hasher::new()
            .update(&id.to_bytes())
            .finalize_xof()
            .fill(&mut bytes);
        let start = Instant::now();
        match store.head(&path).await {
            Err(object_store::Error::NotFound { .. }) => {}
            result => return Err(format!("expected missing probe object: {result:?}").into()),
        }
        report("head_missing", sample, start);
        let start = Instant::now();
        store
            .put_opts(&path, bytes.into(), PutMode::Create.into())
            .await?;
        report("put_256k", sample, start);
        let start = Instant::now();
        store.head(&path).await?;
        report("head_existing", sample, start);
    }
    Ok(())
}

fn report(operation: &str, sample: usize, start: Instant) {
    println!(
        "{}",
        serde_json::json!({
            "operation": operation, "sample": sample, "seconds": start.elapsed().as_secs_f64(),
        })
    );
}
