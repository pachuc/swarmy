use serde::{Deserialize, Serialize};
/// Fence a node can encounter.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
pub enum FenceKind {
    #[error("placement lease or epoch no longer matches")]
    Placement,
    #[error("placed tool claim no longer matches")]
    PlacedToolClaim,
    #[error("volume writer lease no longer matches")]
    VolumeLease,
    #[error("volume head changed")]
    VolumeHead,
    #[error("expected head {expected}, found {actual}")]
    StaleSequence { expected: u64, actual: u64 },
    #[error("tool job mismatch")]
    ToolJob,
    #[error("{detail}")]
    DispatchPlacementStale { detail: String },
}
/// Final refusal that the node must act on.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
pub enum NodeRefusal {
    #[error("fenced: {0}")]
    Fenced(FenceKind),
    #[error("epoch {epoch} already stopped")]
    EpochStopped { epoch: u64 },
    #[error("computer deleted")]
    ComputerDeleted,
    #[error("tool call already completed")]
    AlreadyDone,
    #[error("placed on {node} at epoch {epoch}")]
    PlacedElsewhere {
        node: swarmy_core::NodeId,
        epoch: u64,
    },
    #[error("token belongs to another node")]
    NotYours,
    #[error("unavailable: {0}")]
    Unavailable(String),
}
/// JSON body of every failed request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "error", rename_all = "snake_case")]
pub enum Failure {
    #[error("unauthorized")]
    Unauthorized,
    #[error("node build {node:?} does not match {server:?}")]
    BuildMismatch { server: String, node: String },
    #[error("refused: {refusal}")]
    Refused { refusal: NodeRefusal },
    #[error("rejected: {message}")]
    Rejected { message: String },
    #[error("unavailable: {message}")]
    Unavailable { message: String },
    #[error("internal: {message}")]
    Internal { message: String },
}
impl Failure {
    /// HTTP status matching this failure.
    #[must_use]
    pub const fn status(&self) -> reqwest::StatusCode {
        use reqwest::StatusCode;
        match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::BuildMismatch { .. } | Self::Refused { .. } => StatusCode::CONFLICT,
            Self::Rejected { .. } => StatusCode::BAD_REQUEST,
            Self::Unavailable { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Self::Internal { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    #[test]
    fn failures_round_trip_and_status() {
        let cases = [
            (Failure::Unauthorized, 401),
            (
                Failure::BuildMismatch {
                    server: "a".into(),
                    node: "b".into(),
                },
                409,
            ),
            (
                Failure::Refused {
                    refusal: NodeRefusal::Fenced(FenceKind::StaleSequence {
                        expected: 1,
                        actual: 2,
                    }),
                },
                409,
            ),
            (
                Failure::Refused {
                    refusal: NodeRefusal::PlacedElsewhere {
                        node: swarmy_core::NodeId::from_ulid(ulid::Ulid::generate()),
                        epoch: 2,
                    },
                },
                409,
            ),
            (
                Failure::Rejected {
                    message: "bad".into(),
                },
                400,
            ),
            (
                Failure::Unavailable {
                    message: "wait".into(),
                },
                503,
            ),
            (
                Failure::Internal {
                    message: "broken".into(),
                },
                500,
            ),
        ];
        for (failure, status) in cases {
            assert_eq!(failure.status().as_u16(), status);
            assert_eq!(
                serde_json::from_slice::<Failure>(&serde_json::to_vec(&failure).unwrap()).unwrap(),
                failure
            );
        }
    }
}
