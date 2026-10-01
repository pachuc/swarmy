#![deny(clippy::disallowed_methods)]
//! Uses a dedicated empty bucket because the empty-prefix collector owns the
//! whole bucket. Set `SWARMY_S3_TEST_BUCKET` in addition to the usual S3/FDB env.
use std::{panic::AssertUnwindSafe, sync::Arc, time::Duration};

use bytes::Bytes;
use foundationdb::tuple::Subspace;
use futures::{FutureExt, StreamExt, TryStreamExt, stream};
use object_store::{ObjectStore, PutMode, path::Path};
use swarmy_config::{BucketCredentials, BucketSpec, GarbageCollection, ObjectPrefix, Settings};
use swarmy_core::{CHUNK_SIZE, ContentHash, ImageTag, ManifestId};
use swarmy_store::{
    Store,
    blob::{BlobStore, ObjectBlobStore},
};
use swarmy_volume::{ChunkStore, Manifest, ManifestBuilder, gc::collect};

const OBJECTS: usize = 1005;

fn chunk_path(hash: ContentHash) -> Path {
    let hex = hash.to_string();
    Path::from(format!("chunks/{}/{hex}", &hex[..2]))
}

/// Wait until the orphans age past the collector's grace cutoff. S3
/// last-modified has second precision, so once the integer second ticks two
/// past the write the object is a candidate under any truncation of the
/// one-second grace cutoff.
async fn wait_orphans_aged(objects: &Arc<dyn ObjectStore>, path: &Path) {
    swarmy_testkit::eventually(
        "orphans age past the grace period",
        Duration::from_secs(30),
        async || {
            let modified = objects.head(path).await.unwrap().last_modified.timestamp();
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            (now >= u64::try_from(modified).unwrap() + 2).then_some(())
        },
    )
    .await;
}

async fn exercise(settings: &Settings, store: &Store, sibling: &dyn ObjectStore) {
    let objects = swarmy_store::objects::from_settings(settings).unwrap();
    let (chunks, live) = seed_live_image(&objects, store).await;
    let paths = seed_orphans(&objects, sibling).await;
    check_listings(settings, &*objects, &paths).await;

    // S3 last-modified has second precision and the collector truncates its cutoff.
    // Poll the same timestamp source instead of a fixed wait: once the
    // integer second ticks two past the write, the object is a candidate
    // under any truncation of the one-second grace cutoff.
    wait_orphans_aged(&objects, &paths[0]).await;
    let policy = GarbageCollection {
        grace_secs: Duration::from_secs(1),
        ..GarbageCollection::default()
    };
    check_dry_run(store, &objects, policy, &paths).await;
    check_real_collect(store, &objects, &chunks, live, sibling, &paths, policy).await;
}

/// Store one live chunk, manifest, and image so the collector has a
/// survivor. Returns the chunk store and the live chunk hash.
async fn seed_live_image(
    objects: &Arc<dyn ObjectStore>,
    store: &Store,
) -> (ChunkStore, ContentHash) {
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
        .put_image("s3-test", &ImageTag("live".into()), id, None)
        .await
        .unwrap();
    (chunks, live)
}

/// Write more orphans than one S3 page into a single shard and check the
/// sibling namespace still reads its own object. All orphans share one
/// shard, so both the direct listing and collector must follow S3
/// continuation tokens beyond its 1000-object page limit.
async fn seed_orphans(objects: &Arc<dyn ObjectStore>, sibling: &dyn ObjectStore) -> Vec<Path> {
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
    paths
}

/// A dry run must find every orphan and delete nothing.
async fn check_dry_run(
    store: &Store,
    objects: &Arc<dyn ObjectStore>,
    policy: GarbageCollection,
    paths: &[Path],
) {
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
}

/// A real run must delete every orphan, keep the live chunk, and leave the
/// sibling namespace untouched.
async fn check_real_collect(
    store: &Store,
    objects: &Arc<dyn ObjectStore>,
    chunks: &ChunkStore,
    live: ContentHash,
    sibling: &dyn ObjectStore,
    paths: &[Path],
    policy: GarbageCollection,
) {
    let real = collect(store, objects.clone(), policy, false)
        .await
        .unwrap();
    assert_eq!(real.deleted, u64::try_from(OBJECTS).unwrap());
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

async fn check_listings(settings: &Settings, objects: &dyn ObjectStore, paths: &[Path]) {
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
    if !settings.s3.prefix.as_str().is_empty() {
        assert!(
            all.iter()
                .all(|meta| meta.location.as_ref().starts_with("chunks/")
                    || meta.location.as_ref().starts_with("manifests/"))
        );
    }
}

#[tokio::test]
async fn s3_empty_and_nested_namespaces_paginate_and_collect() {
    for name in ["SWARMY_S3_ENDPOINT", "SWARMY_FDB_CLUSTER_FILE"] {
        if swarmy_testkit::require_stack(name).is_none() {
            return;
        }
    }
    let Some(bucket) = swarmy_testkit::optional_env("SWARMY_S3_TEST_BUCKET") else {
        return;
    };
    let mut settings = swarmy_testkit::test_settings(&[
        "SWARMY_FDB_CLUSTER_FILE",
        "SWARMY_S3_ENDPOINT",
        "SWARMY_S3_ACCESS_KEY",
        "SWARMY_S3_SECRET_KEY",
        "SWARMY_S3_BUCKET",
        "SWARMY_S3_REGION",
    ]);
    settings.s3.bucket = bucket;
    settings.s3.prefix = ObjectPrefix::default();
    assert!(
        !settings.s3.bucket.contains('/'),
        "test bucket must be a physical bucket"
    );
    let raw = swarmy_store::objects::from_settings(&settings).unwrap();
    assert!(
        raw.list(None).try_next().await.unwrap().is_none(),
        "SWARMY_S3_TEST_BUCKET must be empty and dedicated to this test"
    );
    swarmy_testkit::boot_fdb();
    let db = Arc::new(swarmy_store::database(&settings.store.cluster_file).unwrap());
    for prefix in ["", "runs/nested"] {
        settings.s3.prefix = prefix.parse().unwrap();
        let mut sibling_settings = settings.clone();
        sibling_settings.s3.prefix = "runs/nested-sibling".parse().unwrap();
        let sibling = swarmy_store::objects::from_settings(&sibling_settings).unwrap();
        let root = Subspace::all().subspace(&(format!("s3-test-{}", ulid::Ulid::generate()),));
        let store = Store::with_subspace(
            db.clone(),
            root.clone(),
            Arc::new(ObjectBlobStore::new(
                swarmy_store::objects::from_settings(&settings).unwrap(),
            )),
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

#[tokio::test]
async fn bucket_spec_with_static_keys_round_trips_objects() {
    // Runs the same object operations through the new bucket description
    // with static keys and a custom endpoint. Skips without the dev stack.
    if swarmy_testkit::require_stack("SWARMY_S3_ENDPOINT").is_none() {
        return;
    }
    let loaded = Settings::load().unwrap().settings;
    let spec = BucketSpec {
        endpoint: loaded.s3.endpoint.clone(),
        region: loaded.s3.region.clone(),
        bucket: loaded.s3.bucket.clone(),
        prefix: ObjectPrefix::default(),
        credentials: BucketCredentials::StaticKeys {
            access_key: loaded.s3.access_key.clone(),
            secret_key: loaded.s3.secret_key.clone(),
        },
        conditional_create: true,
    };
    for conditional_create in [true, false] {
        // The description reaches the client the way the nodes read it:
        // region filled, coordinates applied to settings.
        let mut owned = spec.clone();
        owned.resolve_region(&loaded.s3.region);
        let mut settings = Settings::default();
        owned.apply_to_settings(&mut settings);
        settings.s3.conditional_create = conditional_create;
        let store = swarmy_store::objects::from_settings(&settings).unwrap();
        let scope = format!("bucket-spec-test-{}", ulid::Ulid::generate());
        let path = Path::from(format!("{scope}/object"));
        store.put(&path, "payload".into()).await.unwrap();
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap(),
            "payload"
        );
        assert_eq!(store.head(&path).await.unwrap().location, path);
        // A create-only PUT of identical bytes stays safe in both modes.
        let result = store
            .put_opts(&path, "payload".into(), PutMode::Create.into())
            .await;
        if conditional_create {
            assert!(
                matches!(result, Err(object_store::Error::AlreadyExists { .. })),
                "unexpected {result:?}"
            );
        } else {
            result.unwrap();
        }
        store.delete(&path).await.unwrap();
        assert!(matches!(
            store.head(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));
    }
}

#[tokio::test]
async fn s3_namespace_lists_relative_keys_and_keeps_siblings() {
    if swarmy_testkit::require_stack("SWARMY_S3_ENDPOINT").is_none() {
        return;
    }
    let mut settings = Settings::load().unwrap().settings;
    settings.s3.prefix = format!("prefix-test-{}", ulid::Ulid::generate())
        .parse()
        .unwrap();
    let raw = swarmy_store::objects::from_settings(&settings).unwrap();
    let root = ObjectBlobStore::new(raw.clone());
    settings.s3.prefix = format!("{}/inside", settings.s3.prefix.as_str())
        .parse()
        .unwrap();
    let scoped_raw = swarmy_store::objects::from_settings(&settings).unwrap();
    let scoped = ObjectBlobStore::new(scoped_raw.clone());
    let payload = Bytes::from_static(b"prefix regression");
    let outside = root.put("outside", payload.clone()).await;
    let written = scoped.put("chunks/value", payload.clone()).await;
    let read = scoped.get("chunks/value").await;
    let listing = scoped_raw
        .list(Some(&Path::from("chunks/")))
        .try_collect::<Vec<_>>()
        .await;
    let deleted = scoped.delete("chunks/value").await;
    let sibling = root.get("outside").await;
    let remaining = raw.list(None).try_collect::<Vec<_>>().await;
    // Finish cleanup before assertions so a failed listing does not leave
    // this test's sentinel behind in the shared development bucket.
    let cleanup = root.delete("outside").await;
    outside.unwrap();
    written.unwrap();
    deleted.unwrap();
    cleanup.unwrap();
    assert_eq!(read.unwrap(), payload);
    assert_eq!(sibling.unwrap(), payload);
    let listing = listing.unwrap();
    assert_eq!(listing.len(), 1);
    assert_eq!(listing[0].location, Path::from("chunks/value"));
    let remaining = remaining.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].location, Path::from("outside"));
}
