//! Measure serial HEAD and 256 KiB PUT calls through the same S3 client as volumes.
//! Run against a disposable benchmark bucket or run prefix, then delete its objects.
use std::{env, error::Error, time::Instant};

use object_store::{ObjectStore, PutMode, aws::AmazonS3Builder, path::Path};
use swarmy_core::CHUNK_SIZE;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let store = AmazonS3Builder::new()
        .with_endpoint(env::var("SWARMY_S3_ENDPOINT")?)
        .with_access_key_id(env::var("SWARMY_S3_ACCESS_KEY")?)
        .with_secret_access_key(env::var("SWARMY_S3_SECRET_KEY")?)
        .with_bucket_name(env::var("SWARMY_S3_BUCKET")?)
        .with_region(env::var("SWARMY_S3_REGION")?)
        .with_virtual_hosted_style_request(false)
        .build()?;
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
