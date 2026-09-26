//! Metadata-only image for tests that never materialize a computer.
use swarmy_core::{CHUNK_SIZE, ContentHash, ImageTag, ManifestHeader, ManifestId};
use swarmy_store::Store;

pub async fn image(store: &Store) -> &'static str {
    let manifest = ManifestId::from_ulid(ulid::Ulid::from_parts(1, 1));
    store
        .put_manifest(
            manifest,
            &ManifestHeader {
                size: u64::from(CHUNK_SIZE),
                chunk_size: CHUNK_SIZE,
                root_hash: ContentHash::ZERO,
            },
        )
        .await
        .unwrap();
    store
        .put_image("fixture", &ImageTag("test".into()), manifest)
        .await
        .unwrap();
    "fixture:test"
}
