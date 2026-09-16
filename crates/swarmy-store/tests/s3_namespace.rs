//! Uses a dedicated empty bucket because the empty-prefix collector owns the
//! whole bucket. Set `SWARMY_S3_TEST_BUCKET` in addition to the usual S3/FDB env.
use std::{num::NonZeroU64, panic::AssertUnwindSafe, sync::Arc, time::Duration};

use foundationdb::{Database, tuple::Subspace};
use futures::{FutureExt, StreamExt, TryStreamExt, stream};
use object_store::{ObjectStore, path::Path};
use swarmy_config::{GarbageCollection, Settings};
use swarmy_core::{CHUNK_SIZE, ContentHash, ImageTag, ManifestId};
use swarmy_store::{Store, blob::ObjectBlobStore};
use swarmy_volume::{ChunkStore, Manifest, ManifestBuilder, gc::collect};

const OBJECTS: usize = 1005;

fn chunk_path(hash: ContentHash) -> Path {
    let hex = hash.to_string();
    Path::from(format!("chunks/{}/{hex}", &hex[..2]))
}

async fn exercise(settings: &Settings, store: &Store, sibling: &dyn ObjectStore) {
    let objects = settings.object_store().unwrap();
    let chunks = ChunkStore::new(objects.clone());
    let live = chunks
        .put_chunk(&vec![17; CHUNK_SIZE as usize])
        .await
        .unwrap()
        .hash;
    let mut builder = ManifestBuilder::new(
        objects.clone(),
        Manifest::empty(u64::from(CHUNK_SIZE)).unwrap(),
    );
    builder.set_chunk(0, live).unwrap();
    let manifest = builder.build().await.unwrap();
    let id = ManifestId::from_ulid(ulid::Ulid::generate());
    store.put_manifest(id, manifest.header()).await.unwrap();
    store
        .put_image("s3-test", &ImageTag("live".into()), id)
        .await
        .unwrap();

    // All orphans share one shard, so both the direct listing and collector
    // must follow S3 continuation tokens beyond its 1000-object page limit.
    let paths: Vec<_> = (0..OBJECTS)
        .map(|index| {
            let mut hash = [1; 32];
            hash[24..].copy_from_slice(&u64::try_from(index).unwrap().to_be_bytes());
            chunk_path(ContentHash(hash))
        })
        .collect();
    stream::iter(paths.iter())
        .map(|path| {
            let objects = &objects;
            async move { objects.put(path, b"orphan".to_vec().into()).await }
        })
        .buffer_unordered(32)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    sibling
        .put(&paths[0], b"sibling".to_vec().into())
        .await
        .unwrap();
    assert_eq!(
        objects.get(&paths[0]).await.unwrap().bytes().await.unwrap(),
        "orphan"
    );
    let head = objects.head(&paths[0]).await.unwrap();
    assert_eq!(head.location, paths[0]);
    assert_eq!(head.size, 6);
    check_listings_and_legacy(settings, &*objects, &paths).await;

    // S3 last-modified has second precision and the collector truncates its cutoff.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let policy = GarbageCollection {
        grace_seconds: NonZeroU64::new(1).unwrap(),
        ..GarbageCollection::default()
    };
    let dry = collect(store, objects.clone(), policy, true).await.unwrap();
    assert_eq!(dry.candidates, u64::try_from(OBJECTS).unwrap());
    assert_eq!(dry.deleted, 0);
    let unchanged: Vec<_> = objects
        .list(Some(&Path::from("chunks/01")))
        .map_ok(|meta| meta.location)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(unchanged, paths);
    let real = collect(store, objects.clone(), policy, false)
        .await
        .unwrap();
    assert_eq!(real.deleted, dry.candidates);
    assert!(
        objects
            .list(Some(&Path::from("chunks/01")))
            .try_next()
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(real.bytes_freed, u64::try_from(OBJECTS * 6).unwrap());
    assert!(matches!(
        objects.head(&paths[0]).await,
        Err(object_store::Error::NotFound { .. })
    ));
    assert_eq!(
        chunks.get_chunk(live).await.unwrap(),
        vec![17; CHUNK_SIZE as usize]
    );
    assert_eq!(
        sibling.get(&paths[0]).await.unwrap().bytes().await.unwrap(),
        "sibling"
    );
    objects.delete(&chunk_path(live)).await.unwrap();
    assert!(matches!(
        objects.get(&chunk_path(live)).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

async fn check_listings_and_legacy(settings: &Settings, objects: &dyn ObjectStore, paths: &[Path]) {
    let mut listed: Vec<_> = objects
        .list(Some(&Path::from("chunks/01")))
        .map_ok(|meta| meta.location)
        .try_collect()
        .await
        .unwrap();
    listed.sort();
    assert_eq!(listed, paths);
    let offset: Vec<_> = objects
        .list_with_offset(Some(&Path::from("chunks/01")), &paths[999])
        .map_ok(|meta| meta.location)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(offset, paths[1000..]);
    let delimited = objects
        .list_with_delimiter(Some(&Path::from("chunks")))
        .await
        .unwrap();
    assert!(delimited.common_prefixes.contains(&Path::from("chunks/01")));
    let all: Vec<_> = objects.list(None).try_collect().await.unwrap();
    if !settings.s3_prefix.as_str().is_empty() {
        assert!(
            all.iter()
                .all(|meta| meta.location.as_ref().starts_with("chunks/")
                    || meta.location.as_ref().starts_with("manifests/"))
        );
        let mut legacy = settings.clone();
        legacy.s3_bucket = format!("{}/{}", settings.s3_bucket, settings.s3_prefix.as_str());
        legacy.s3_prefix = swarmy_config::ObjectPrefix::default();
        let legacy = legacy.object_store().unwrap();
        assert_eq!(legacy.head(&paths[0]).await.unwrap().location, paths[0]);
        legacy
            .put(&Path::from("legacy"), b"old".to_vec().into())
            .await
            .unwrap();
        assert_eq!(
            objects
                .get(&Path::from("legacy"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            "old"
        );
        objects.delete(&Path::from("legacy")).await.unwrap();
    }
}

#[tokio::test]
async fn s3_empty_and_nested_namespaces_paginate_and_collect() {
    for name in [
        "SWARMY_S3_ENDPOINT",
        "SWARMY_FDB_CLUSTER_FILE",
        "SWARMY_S3_TEST_BUCKET",
    ] {
        if std::env::var_os(name).is_none() {
            eprintln!("skipping S3 acceptance test: {name} is unset");
            return;
        }
    }
    let mut settings = Settings::load().unwrap().settings;
    settings.s3_bucket = std::env::var("SWARMY_S3_TEST_BUCKET").unwrap();
    settings.s3_prefix = swarmy_config::ObjectPrefix::default();
    assert!(
        !settings.s3_bucket.contains('/'),
        "test bucket must be a physical bucket"
    );
    let raw = settings.object_store().unwrap();
    assert!(
        raw.list(None).try_next().await.unwrap().is_none(),
        "SWARMY_S3_TEST_BUCKET must be empty and dedicated to this test"
    );
    let _network = swarmy_store::boot();
    let db = Arc::new(Database::new(Some(&settings.fdb_cluster_file)).unwrap());
    for prefix in ["", "runs/nested"] {
        settings.s3_prefix = prefix.parse().unwrap();
        let mut sibling_settings = settings.clone();
        sibling_settings.s3_prefix = "runs/nested-sibling".parse().unwrap();
        let sibling = sibling_settings.object_store().unwrap();
        let root = Subspace::all().subspace(&(format!("s3-test-{}", ulid::Ulid::generate()),));
        let store = Store::with_subspace(
            db.clone(),
            root.clone(),
            Arc::new(ObjectBlobStore::new(settings.object_store().unwrap())),
        );
        let result = AssertUnwindSafe(exercise(&settings, &store, &*sibling))
            .catch_unwind()
            .await;
        // Clean up even after assertion failures. The empty-bucket precondition
        // permits removal of this test's objects without touching existing data.
        let paths: Vec<_> = raw
            .list(None)
            .map_ok(|meta| meta.location)
            .try_collect()
            .await
            .unwrap();
        raw.delete_stream(stream::iter(paths.into_iter().map(Ok)).boxed())
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let root = &root;
        db.run(|trx, _| async move {
            let (begin, end) = root.range();
            trx.clear_range(&begin, &end);
            Ok(())
        })
        .await
        .unwrap();
        if let Err(panic) = result {
            std::panic::resume_unwind(panic);
        }
    }
}
