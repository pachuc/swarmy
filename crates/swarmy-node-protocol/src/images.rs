use serde::{Deserialize, Serialize};

/// Details required to boot an image's probe sandbox.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageProbeOpenResponse {
    pub server_now: jiff::Timestamp,
    pub volume: swarmy_core::VolumeId,
    pub writer: swarmy_core::Lease,
    pub head: swarmy_core::ManifestId,
    pub header: swarmy_core::ManifestHeader,
    pub spec: swarmy_core::SandboxSpec,
}

/// The boot set recorded by an image probe, or none if no chunks were read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageProbeFinishRequest {
    pub manifest: swarmy_core::ManifestId,
    pub key: Option<String>,
}
