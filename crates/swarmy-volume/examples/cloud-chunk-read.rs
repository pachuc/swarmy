//! Measure uncached 256 KiB reads through the production chunk store.
use std::{error::Error, time::Instant};

use swarmy_core::CHUNK_SIZE;
use swarmy_volume::ChunkStore;

fn store() -> Result<ChunkStore, Box<dyn Error>> {
    let settings = swarmy_config::Settings::load()?.settings;
    Ok(ChunkStore::new(swarmy_store::objects::from_settings(
        &settings,
    )?))
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
