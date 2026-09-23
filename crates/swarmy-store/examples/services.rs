use std::sync::Arc;
use swarmy_store::{Store, blob::MemoryBlobStore};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _network = swarmy_store::boot();
    let cluster = std::env::var("SWARMY_FDB_CLUSTER_FILE")?;
    let store = Store::open(Some(&cluster), None, Arc::new(MemoryBlobStore::default())).await?;
    for service in store.list_services().await? {
        println!(
            "{:?} {} {} alive={}",
            service.heartbeat.role,
            service.heartbeat.instance_id,
            service.heartbeat.version,
            service.alive
        );
    }
    Ok(())
}
