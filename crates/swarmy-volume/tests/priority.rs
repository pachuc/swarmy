use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use swarmy_core::CHUNK_SIZE;
use swarmy_volume::{ChunkStore, Manifest, VolumeDevice, priority::ToolActivity};

#[tokio::test]
async fn tool_activity_caps_node_uploads_and_restores_idle_priority() {
    let dir = tempfile::tempdir().unwrap();
    let device = VolumeDevice::open(
        ChunkStore::new(Arc::new(object_store::memory::InMemory::new())),
        Manifest::empty(64 * u64::from(CHUNK_SIZE)).unwrap(),
        dir.path().join("cache"),
        dir.path().join("dirty"),
        0,
    )
    .await
    .unwrap();
    for number in 0..64_u8 {
        device
            .write(
                u64::from(number) * u64::from(CHUNK_SIZE),
                &[number + 1; 4096],
            )
            .await
            .unwrap();
    }
    let tool = ToolActivity::begin();
    let nested = ToolActivity::begin();
    drop(nested);
    assert_eq!(device.stats().upload_concurrency_limit, 4);
    assert_eq!(device.stats().upload_bytes_per_second, 16 * 1024 * 1024);
    let start = Instant::now();
    let uploading = device.clone();
    let task = tokio::spawn(async move { uploading.upload_dirty().await });
    while !task.is_finished() {
        assert!(device.stats().uploads_in_flight <= 4);
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    task.await.unwrap().unwrap();
    assert!(start.elapsed() >= Duration::from_millis(980));
    assert_eq!(device.stats().tool_priority_uploads, 64);
    drop(tool);
    assert_eq!(device.stats().upload_concurrency_limit, 32);
    assert_eq!(device.stats().upload_bytes_per_second, 0);
}
