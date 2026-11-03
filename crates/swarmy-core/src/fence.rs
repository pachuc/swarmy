//! Which fencing token a metadata write found stale.

/// Shared by the volume library and node API without carrying store types.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, thiserror::Error,
)]
pub enum FenceKind {
    #[error("placement lease or epoch no longer matches")]
    Placement,
    #[error("placed tool claim no longer matches")]
    PlacedToolClaim,
    #[error("volume writer lease no longer matches")]
    VolumeLease,
    #[error("volume head changed since this writer opened it")]
    VolumeHead,
}
