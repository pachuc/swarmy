use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{Lease, ManifestId, VolumeId};

pub const CHUNK_SIZE: u32 = 256 * 1024;

/// BLAKE3 content address. Zero is reserved for implicit zero data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ContentHash(pub [u8; 32]);

impl ContentHash {
    pub const ZERO: Self = Self([0; 32]);
}

impl fmt::Display for ContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Stored in `FoundationDB`; the root and leaves live in object storage.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestHeader {
    /// Disk size in bytes, a positive multiple of `chunk_size`.
    pub size: u64,
    pub chunk_size: u32,
    pub root_hash: ContentHash,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeRecord {
    pub head_manifest: ManifestId,
    pub writer_lease: Option<Lease>,
    pub parent: Option<VolumeId>,
}

/// A registered name and tag pointing at an immutable manifest.
/// Embedded in a stored session record. Keep this type's serialized shape frozen;
/// append session fields through `trailing` instead.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageRecord {
    pub name: String,
    pub tag: crate::ImageTag,
    pub manifest_id: ManifestId,
}

#[cfg(test)]
pub(crate) mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use crate::{ImageTag, LeaseOwnerId, encoding::tests::assert_round_trip};
    use ulid::Ulid;

    pub(crate) fn volume_types_round_trip() {
        assert_round_trip(&ManifestHeader {
            size: 32 * 1024 * 1024 * 1024,
            chunk_size: CHUNK_SIZE,
            root_hash: ContentHash([7; 32]),
        });
        for writer_lease in [
            None,
            Some(Lease {
                owner: LeaseOwnerId::from_ulid(Ulid::from_parts(1, 1)),
                expires_at: jiff::Timestamp::UNIX_EPOCH,
                seq: 42,
            }),
        ] {
            assert_round_trip(&VolumeRecord {
                head_manifest: ManifestId::from_ulid(Ulid::from_parts(1, 2)),
                writer_lease,
                parent: Some(VolumeId::from_ulid(Ulid::from_parts(1, 3))),
            });
        }
        assert_round_trip(&ImageTag("stable".into()));
    }
}

/// Durable accounting for one collector attempt. An unfinished record means the
/// process stopped before it could persist its final counters.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcRun {
    pub owner: crate::LeaseOwnerId,
    pub started_at: jiff::Timestamp,
    pub dry_run: bool,
    pub finished: bool,
    pub error: Option<String>,
    pub manifests: u64,
    pub scanned: u64,
    pub candidates: u64,
    pub candidate_bytes: u64,
    pub deleted: u64,
    pub bytes_freed: u64,
    pub duration_ms: u64,
}

/// Object-store key of a content-addressed payload.
#[must_use]
pub fn blob_key(bytes: &[u8]) -> String {
    format!("blobs/{}", blake3::hash(bytes).to_hex())
}

/// Whether `key` has the shape `blob_key` produces. It does not check content.
#[must_use]
pub fn is_blob_key(key: &str) -> bool {
    is_hashed_key(key, "blobs/")
}

/// Most chunks a boot set records: 1 GiB of 256 KiB chunks.
pub const MAX_BOOT_SET_CHUNKS: usize = 4096;

/// Chunk hashes in first-read order and the manifest leaves that hold them.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BootSet {
    pub leaves: Vec<u32>,
    pub chunks: Vec<ContentHash>,
}

/// Content-addressed object key of encoded boot-set bytes.
#[must_use]
pub fn boot_set_key(bytes: &[u8]) -> String {
    format!("bootsets/{}", blake3::hash(bytes).to_hex())
}

/// Whether the key has the shape of a boot-set object key.
#[must_use]
pub fn is_boot_set_key(key: &str) -> bool {
    is_hashed_key(key, "bootsets/")
}

fn is_hashed_key(key: &str, prefix: &str) -> bool {
    key.strip_prefix(prefix).is_some_and(|hash| {
        hash.len() == 64
            && hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

#[cfg(test)]
mod blob_key_tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    #[test]
    fn boot_set_key_shape() {
        assert!(is_boot_set_key(&boot_set_key(b"x")));
        assert!(!is_boot_set_key(&blob_key(b"x")));
        assert!(!is_blob_key(&boot_set_key(b"x")));
        assert!(!is_boot_set_key("bootsets/"));
        assert!(!is_boot_set_key(&format!("bootsets/{}", "A".repeat(64))));
        assert!(!is_boot_set_key(&format!("bootsets/{}", "a".repeat(63))));
        assert!(!is_boot_set_key(&"a".repeat(64)));
        assert!(is_blob_key(&blob_key(b"x")));
    }
    #[test]
    fn blob_key_shape() {
        assert!(is_blob_key(&blob_key(b"x")));
        assert!(!is_blob_key("blobs/"));
        assert!(!is_blob_key(&format!("blobs/{}", "A".repeat(64))));
        assert!(!is_blob_key(&format!("blobs/{}", "a".repeat(63))));
        assert!(!is_blob_key(&"a".repeat(64)));
    }
}
