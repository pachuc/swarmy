//! Metadata-only image for tests that never materialize a computer.

use swarmy_core::{CHUNK_SIZE, ContentHash, ImageTag, ManifestHeader, ManifestId};

/// Register the shared `fixture:test` metadata image and return its tag.
///
/// Every control-plane suite needs an image reference for session setup but
/// never boots a computer from it. One helper keeps the manifest bytes
/// identical instead of copying the literal into each fixture.
///
/// # Panics
/// Panics when the store write fails; fixture setup has no recovery.
pub async fn image(store: &swarmy_store::Store) -> &'static str {
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
        .expect("fixture image manifest must store");
    store
        .put_image("fixture", &ImageTag("test".into()), manifest, None)
        .await
        .expect("fixture image tag must store");
    "fixture:test"
}
