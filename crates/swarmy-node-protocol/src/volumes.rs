use crate::WriterToken;
use serde::{Deserialize, Serialize};
/// Publish a head with the server retention policy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumePublishRequest {
    pub writer: WriterToken,
    pub previous: swarmy_core::ManifestId,
    pub next: swarmy_core::ManifestId,
    pub header: swarmy_core::ManifestHeader,
}
/// Release a writer lease owned by the calling node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeReleaseRequest {
    pub writer: WriterToken,
}
/// Content reused by the node that a collector must preserve.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeProtectRequest {
    pub hashes: Vec<swarmy_core::ContentHash>,
}
/// Content already being deleted, which must be uploaded again.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeProtectResponse {
    pub server_now: jiff::Timestamp,
    pub deleting: Vec<swarmy_core::ContentHash>,
}
