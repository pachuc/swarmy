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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageRecord {
    pub name: String,
    pub tag: crate::ImageTag,
    pub manifest_id: ManifestId,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ImageTag, LeaseOwnerId, encoding::tests::assert_round_trip};
    use ulid::Ulid;

    #[test]
    fn volume_types_round_trip() {
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
