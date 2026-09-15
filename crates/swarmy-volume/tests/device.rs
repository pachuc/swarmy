use object_store::{ObjectStore, memory::InMemory};
use std::sync::Arc;
use swarmy_core::CHUNK_SIZE;
use swarmy_volume::{ChunkStore, Manifest, ManifestBuilder, VolumeDevice};

#[tokio::test]
async fn dirty_reopen_trim_and_cache_integrity() {
    let dir = tempfile::tempdir().unwrap();
    let memory = Arc::new(InMemory::new());
    let store = ChunkStore::new(memory.clone());
    let hash = store
        .put_chunk(&vec![13; CHUNK_SIZE as usize])
        .await
        .unwrap()
        .hash;
    let mut builder = ManifestBuilder::new(
        memory.clone(),
        Manifest::empty(u64::from(CHUNK_SIZE) * 2).unwrap(),
    );
    builder.set_chunk(0, hash).unwrap();
    let manifest = builder.build().await.unwrap();
    let cache = dir.path().join("cache");
    let dirty = dir.path().join("dirty");
    let device = VolumeDevice::open(store.clone(), manifest.clone(), &cache, &dirty, 0)
        .await
        .unwrap();
    assert!(
        VolumeDevice::open(store.clone(), manifest.clone(), &cache, &dirty, 0)
            .await
            .is_err()
    );
    assert_eq!(device.read(0, 4096).await.unwrap(), vec![13; 4096]);
    assert_eq!(device.stats().cold_reads, 1);
    tokio::fs::write(cache.join(hash.to_string()), b"broken")
        .await
        .unwrap();
    assert_eq!(device.read(0, 4096).await.unwrap(), vec![13; 4096]);
    assert_eq!(device.stats().cold_reads, 2);
    let hex = hash.to_string();
    memory
        .delete(&object_store::path::Path::from(format!(
            "chunks/{}/{hex}",
            &hex[..2]
        )))
        .await
        .unwrap();
    assert_eq!(device.read(0, 4096).await.unwrap(), vec![13; 4096]);
    assert_eq!(device.stats().cache_hits, 1);
    device.write(4096, &vec![42; 8192]).await.unwrap();
    device.write(4096, &vec![43; 4096]).await.unwrap();
    device.trim(0, 4096).await.unwrap();
    device.flush().await.unwrap();
    assert_eq!(device.stats().dirty_bytes, 12288);
    drop(device);
    let device = VolumeDevice::open(store, manifest, &cache, &dirty, 0)
        .await
        .unwrap();
    assert_eq!(device.stats().dirty_bytes, 12288);
    assert_eq!(device.read(0, 4096).await.unwrap(), vec![0; 4096]);
    assert_eq!(device.read(4096, 4096).await.unwrap(), vec![43; 4096]);
    assert_eq!(device.read(8192, 4096).await.unwrap(), vec![42; 4096]);
    assert_eq!(
        device.read(u64::from(CHUNK_SIZE), 4096).await.unwrap(),
        vec![0; 4096]
    );
    assert!(device.write(1, &[0; 4096]).await.is_err());
    assert!(device.trim(device.size(), 4096).await.is_err());
}

#[tokio::test]
async fn sequential_readahead_reduces_foreground_fetches() {
    let dir = tempfile::tempdir().unwrap();
    let memory = Arc::new(InMemory::new());
    let store = ChunkStore::new(memory.clone());
    let mut builder =
        ManifestBuilder::new(memory, Manifest::empty(16 * u64::from(CHUNK_SIZE)).unwrap());
    for index in 0..16_u8 {
        let hash = store
            .put_chunk(&vec![index + 1; CHUNK_SIZE as usize])
            .await
            .unwrap()
            .hash;
        builder.set_chunk(u64::from(index), hash).unwrap();
    }
    let manifest = builder.build().await.unwrap();
    let mut stats = Vec::new();
    for ahead in [0, 4] {
        let device = VolumeDevice::open(
            store.clone(),
            manifest.clone(),
            dir.path().join(format!("cache-{ahead}")),
            dir.path().join(format!("dirty-{ahead}")),
            ahead,
        )
        .await
        .unwrap();
        for index in 0..16_u8 {
            assert_eq!(
                device
                    .read(
                        u64::from(index) * u64::from(CHUNK_SIZE),
                        CHUNK_SIZE as usize
                    )
                    .await
                    .unwrap(),
                vec![index + 1; CHUNK_SIZE as usize]
            );
            // Model work performed by the consumer between chunks. Wait for
            // observable progress rather than depending on a fixed scheduler delay.
            if ahead != 0 && index > 0 && index < 15 {
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    while device.stats().readahead_fetches < u64::from(index) {
                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    }
                })
                .await
                .unwrap();
            }
        }
        stats.push(device.stats());
    }
    assert_eq!(stats[0].cold_reads, 16);
    assert!(stats[1].cold_reads < stats[0].cold_reads, "{stats:?}");
    assert!(stats[1].readahead_hits > 0);
    assert_eq!(stats[1].cold_reads + stats[1].readahead_fetches, 16);
}

#[tokio::test]
async fn whole_disk_trim_exceeds_read_write_limit() {
    let dir = tempfile::tempdir().unwrap();
    let size = 64 * 1024 * 1024;
    let device = VolumeDevice::open(
        ChunkStore::new(Arc::new(InMemory::new())),
        Manifest::empty(size).unwrap(),
        dir.path().join("cache"),
        dir.path().join("dirty"),
        0,
    )
    .await
    .unwrap();
    device.write(size - 4096, &[7; 4096]).await.unwrap();
    device
        .trim(0, usize::try_from(size).unwrap())
        .await
        .unwrap();
    device.flush().await.unwrap();
    assert_eq!(device.read(size - 4096, 4096).await.unwrap(), vec![0; 4096]);
    assert_eq!(device.stats().dirty_bytes, size);
}
