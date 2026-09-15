//! Measure uncached 256 KiB reads through the production chunk store.
use std::{env, error::Error, sync::Arc, time::Instant};

use object_store::aws::AmazonS3Builder;
use swarmy_core::CHUNK_SIZE;
use swarmy_volume::ChunkStore;

fn store() -> Result<ChunkStore, Box<dyn Error>> {
    Ok(ChunkStore::new(Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(env::var("SWARMY_S3_ENDPOINT")?)
            .with_access_key_id(env::var("SWARMY_S3_ACCESS_KEY")?)
            .with_secret_access_key(env::var("SWARMY_S3_SECRET_KEY")?)
            .with_bucket_name(env::var("SWARMY_S3_BUCKET")?)
            .with_region(env::var("SWARMY_S3_REGION")?)
            .with_virtual_hosted_style_request(false)
            .build()?,
    )))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let uploader = store()?;
    let mut hashes = Vec::new();
    for index in 0..10_u8 {
        let mut bytes = vec![0; CHUNK_SIZE as usize];
        blake3::Hasher::new()
            .update(&[index])
            .finalize_xof()
            .fill(&mut bytes);
        hashes.push(uploader.put_chunk(&bytes).await?.hash);
    }
    // A separate client includes DNS/TCP/TLS setup in the first sample. Later
    // samples reuse its connection, but every sample issues a remote GET.
    let reader = store()?;
    for (index, hash) in hashes.into_iter().enumerate() {
        let start = Instant::now();
        let bytes = reader.get_chunk(hash).await?;
        println!(
            "{}",
            serde_json::json!({
                "sample": index,
                "bytes": bytes.len(),
                "seconds": start.elapsed().as_secs_f64(),
            })
        );
    }
    Ok(())
}
