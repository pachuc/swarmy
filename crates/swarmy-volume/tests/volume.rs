use std::sync::Arc;

use futures::TryStreamExt;
use object_store::{ObjectStore, memory::InMemory, path::Path};
use swarmy_core::{CHUNK_SIZE, ContentHash, ManifestHeader, STORAGE_VERSION, encode};
use swarmy_volume::{BLOCKS_PER_LEAF, ChunkStore, Manifest, ManifestBuilder, VolumeError};

const DISK_SIZE: u64 = 32 * 1024 * 1024 * 1024;

async fn objects(store: &InMemory) -> Vec<object_store::ObjectMeta> {
    store.list(None).try_collect().await.unwrap()
}

#[tokio::test]
async fn duplicate_chunk_does_not_upload_and_zero_chunks_need_no_object() {
    let memory = Arc::new(InMemory::new());
    let chunks = ChunkStore::new(memory.clone());
    let zeros = vec![0; CHUNK_SIZE as usize];
    let zero = chunks.put_chunk(&zeros).await.unwrap();
    assert_eq!(zero.hash, ContentHash::ZERO);
    assert!(!zero.uploaded);
    assert_eq!(chunks.get_chunk(zero.hash).await.unwrap(), zeros);
    assert!(objects(&memory).await.is_empty());

    let data = vec![7; CHUNK_SIZE as usize];
    let first = chunks.put_chunk(&data).await.unwrap();
    assert!(first.uploaded);
    assert_eq!(first.hash.0, *blake3::hash(&data).as_bytes());
    let before = objects(&memory).await;
    assert_eq!(before.len(), 1);
    let second = chunks.put_chunk(&data).await.unwrap();
    assert_eq!(first.hash, second.hash);
    assert!(!second.uploaded);
    // InMemory changes the ETag on each put, including an identical overwrite.
    assert_eq!(objects(&memory).await, before);
    assert_eq!(chunks.get_chunk(first.hash).await.unwrap(), data);
    let hex = first.hash.to_string();
    assert_eq!(
        before[0].location,
        Path::from(format!("chunks/{}/{hex}", &hex[..2]))
    );
    let stored = memory
        .get(&before[0].location)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(stored[0], STORAGE_VERSION);
}

#[tokio::test]
async fn concurrent_chunk_puts_have_one_creator() {
    let memory = Arc::new(InMemory::new());
    let chunks = ChunkStore::new(memory);
    let data = vec![8; CHUNK_SIZE as usize];
    let (a, b) = tokio::join!(chunks.put_chunk(&data), chunks.put_chunk(&data));
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(a.hash, b.hash);
    assert_ne!(a.uploaded, b.uploaded);
}

async fn sparse_manifest(memory: Arc<InMemory>) -> Manifest {
    let chunks = ChunkStore::new(memory.clone());
    let hash = chunks
        .put_chunk(&vec![1; CHUNK_SIZE as usize])
        .await
        .unwrap()
        .hash;
    let mut builder = ManifestBuilder::new(memory, Manifest::empty(DISK_SIZE).unwrap());
    for block in [0, 10 * 4096 + 7, 31 * 4096 + 4095] {
        builder.set_chunk(block, hash).unwrap();
    }
    builder.build().await.unwrap()
}

#[tokio::test]
async fn sparse_32_gib_disk_stores_only_three_leaves_and_one_root() {
    let memory = Arc::new(InMemory::new());
    let empty = Manifest::empty(DISK_SIZE).unwrap();
    assert_eq!(empty.leaf_hashes().len(), 32);
    let empty = ManifestBuilder::new(memory.clone(), empty)
        .build()
        .await
        .unwrap();
    assert_eq!(empty.header().root_hash, ContentHash::ZERO);
    assert!(objects(&memory).await.is_empty());
    let manifest = sparse_manifest(memory.clone()).await;
    let loaded = Manifest::load(&*memory, manifest.header().clone())
        .await
        .unwrap();
    assert_eq!(loaded.leaf_hashes(), manifest.leaf_hashes());
    assert_eq!(objects(&memory).await.len(), 5); // One chunk, three leaves, one root.
    for (index, &hash) in manifest.leaf_hashes().iter().enumerate() {
        assert_eq!(hash != ContentHash::ZERO, [0, 10, 31].contains(&index));
    }
    for block in [0, 10 * 4096 + 7, 31 * 4096 + 4095] {
        let hash = loaded.chunk_hash(&*memory, block).await.unwrap();
        assert_ne!(hash, ContentHash::ZERO);
        assert_eq!(
            ChunkStore::new(memory.clone())
                .get_chunk(hash)
                .await
                .unwrap(),
            vec![1; CHUNK_SIZE as usize]
        );
    }
    assert_eq!(
        loaded.chunk_hash(&*memory, 4096).await.unwrap(),
        ContentHash::ZERO
    );
}

#[tokio::test]
async fn rewriting_one_block_shares_every_untouched_leaf() {
    let memory = Arc::new(InMemory::new());
    let old = sparse_manifest(memory.clone()).await;
    let before = objects(&memory).await;
    let mut builder = ManifestBuilder::new(memory.clone(), old.clone());
    builder
        .set_chunk(10 * 4096 + 7, ContentHash([9; 32]))
        .unwrap();
    let new = builder.build().await.unwrap();
    assert_ne!(new.header().root_hash, old.header().root_hash);
    for index in 0..32 {
        assert_eq!(
            new.leaf_hashes()[index] == old.leaf_hashes()[index],
            index != 10
        );
    }
    let after = objects(&memory).await;
    assert_eq!(after.len(), before.len() + 2);
    for object in before {
        assert!(after.contains(&object));
    }
    assert_ne!(
        old.chunk_hash(&*memory, 10 * 4096 + 7).await.unwrap(),
        ContentHash([9; 32])
    );
    assert_eq!(
        new.chunk_hash(&*memory, 10 * 4096 + 7).await.unwrap(),
        ContentHash([9; 32])
    );
}

#[tokio::test]
async fn builder_never_reads_untouched_leaves_and_elides_noop_writes() {
    let memory = Arc::new(InMemory::new());
    let old = sparse_manifest(memory.clone()).await;
    // Removing an untouched leaf makes any accidental read fail.
    memory
        .delete(&Path::from(format!("manifests/{}", old.leaf_hashes()[31])))
        .await
        .unwrap();
    let mut builder = ManifestBuilder::new(memory.clone(), old.clone());
    builder.set_chunk(0, ContentHash([4; 32])).unwrap();
    let changed = builder.build().await.unwrap();
    assert_eq!(changed.leaf_hashes()[31], old.leaf_hashes()[31]);
    let before = objects(&memory).await;
    let mut builder = ManifestBuilder::new(memory.clone(), changed.clone());
    builder.set_chunk(0, ContentHash([5; 32])).unwrap();
    builder.set_chunk(0, ContentHash([4; 32])).unwrap();
    let same = builder.build().await.unwrap();
    assert_eq!(same.header(), changed.header());
    assert_eq!(objects(&memory).await, before);
}

#[tokio::test]
async fn clearing_last_block_restores_implicit_zero_root_including_partial_leaf() {
    let memory = Arc::new(InMemory::new());
    let size = (BLOCKS_PER_LEAF as u64 + 1) * u64::from(CHUNK_SIZE);
    let mut builder = ManifestBuilder::new(memory.clone(), Manifest::empty(size).unwrap());
    builder
        .set_chunk(BLOCKS_PER_LEAF as u64, ContentHash([1; 32]))
        .unwrap();
    let written = builder.build().await.unwrap();
    let loaded = Manifest::load(&*memory, written.header().clone())
        .await
        .unwrap();
    assert_eq!(
        loaded
            .chunk_hash(&*memory, BLOCKS_PER_LEAF as u64)
            .await
            .unwrap(),
        ContentHash([1; 32])
    );
    let before = objects(&memory).await;
    let mut builder = ManifestBuilder::new(memory.clone(), loaded);
    builder
        .set_chunk(BLOCKS_PER_LEAF as u64, ContentHash::ZERO)
        .unwrap();
    let cleared = builder.build().await.unwrap();
    assert_eq!(cleared.header().root_hash, ContentHash::ZERO);
    assert_eq!(objects(&memory).await, before);
}

#[tokio::test]
async fn invalid_dimensions_missing_objects_and_corruption_are_rejected() {
    let memory = Arc::new(InMemory::new());
    let chunks = ChunkStore::new(memory.clone());
    assert!(matches!(
        chunks.put_chunk(&[1]).await,
        Err(VolumeError::InvalidChunkSize)
    ));
    for size in [0, 1, u64::from(CHUNK_SIZE) + 1] {
        assert!(Manifest::empty(size).is_err());
    }
    let header = ManifestHeader {
        size: DISK_SIZE,
        chunk_size: 4096,
        root_hash: ContentHash::ZERO,
    };
    assert!(matches!(
        Manifest::load(&*memory, header).await,
        Err(VolumeError::InvalidDiskSize)
    ));
    let mut builder = ManifestBuilder::new(memory.clone(), Manifest::empty(DISK_SIZE).unwrap());
    assert!(matches!(
        builder.set_chunk(DISK_SIZE / u64::from(CHUNK_SIZE), ContentHash::ZERO),
        Err(VolumeError::InvalidBlock(_))
    ));
    assert!(chunks.get_chunk(ContentHash([2; 32])).await.is_err());
    let put = chunks
        .put_chunk(&vec![3; CHUNK_SIZE as usize])
        .await
        .unwrap();
    let path = objects(&memory).await.pop().unwrap().location;
    memory
        .put(
            &path,
            encode(&vec![4_u8; CHUNK_SIZE as usize]).unwrap().into(),
        )
        .await
        .unwrap();
    assert!(matches!(
        chunks.get_chunk(put.hash).await,
        Err(VolumeError::Corrupt)
    ));
    let manifest = sparse_manifest(memory.clone()).await;
    memory
        .put(
            &Path::from(format!("manifests/{}", manifest.header().root_hash)),
            vec![1, 2, 3].into(),
        )
        .await
        .unwrap();
    assert!(matches!(
        Manifest::load(&*memory, manifest.header().clone()).await,
        Err(VolumeError::Corrupt)
    ));
}
