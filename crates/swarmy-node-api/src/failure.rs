use axum::{
    Json,
    response::{IntoResponse, Response},
};
use std::future::Future;
use swarmy_node_protocol::{Failure, FenceKind, NodeRefusal};
use swarmy_store::{DomainError, FenceError, StorageError, StoreError};
/// Serialize an error with its matching status.
pub(crate) struct Answer(pub(crate) Failure);
impl IntoResponse for Answer {
    fn into_response(self) -> Response {
        (self.0.status(), Json(self.0)).into_response()
    }
}
impl From<StoreError> for Answer {
    fn from(error: StoreError) -> Self {
        Self(failure(&error))
    }
}
pub(crate) fn failure(error: &StoreError) -> Failure {
    let message = swarmy_core::error_chain(error);
    match error {
        StoreError::Fence(fence) => {
            let kind = match fence {
                FenceError::PlacementMismatch | FenceError::PlacementAgentMismatch => {
                    Some(FenceKind::Placement)
                }
                FenceError::PlacedToolClaimMismatch => Some(FenceKind::PlacedToolClaim),
                FenceError::VolumeLeaseMismatch => Some(FenceKind::VolumeLease),
                FenceError::VolumeHeadMismatch => Some(FenceKind::VolumeHead),
                FenceError::StaleSequence { expected, actual } => Some(FenceKind::StaleSequence {
                    expected: *expected,
                    actual: *actual,
                }),
                FenceError::ToolJobMismatch => Some(FenceKind::ToolJob),
                FenceError::DispatchPlacementStale { detail } => {
                    Some(FenceKind::DispatchPlacementStale {
                        detail: (*detail).into(),
                    })
                }
                FenceError::InflightMismatch
                | FenceError::SessionAgentMismatch
                | FenceError::ToolClaimMismatch
                | FenceError::LeaseMismatch
                | FenceError::GcLeaseMismatch
                | FenceError::ProvisionerLeaseMismatch
                | FenceError::CredentialRefreshMismatch => None,
            };
            if let Some(kind) = kind {
                Failure::Refused {
                    refusal: NodeRefusal::Fenced(kind),
                }
            } else {
                internal(message)
            }
        }
        StoreError::Domain(DomainError::ComputerDeleted) => Failure::Refused {
            refusal: NodeRefusal::ComputerDeleted,
        },
        StoreError::Domain(
            DomainError::NodeAtCapacity { .. }
            | DomainError::GpuUnavailable { .. }
            | DomainError::ImageMissing { .. },
        ) => Failure::Refused {
            refusal: NodeRefusal::Unavailable(message),
        },
        StoreError::Domain(_) => Failure::Rejected { message },
        StoreError::Storage(
            StorageError::CommitUnknown | StorageError::FoundationDb(_) | StorageError::Binding(_),
        ) => Failure::Unavailable { message },
        StoreError::Storage(_) => internal(message),
    }
}
fn internal(message: String) -> Failure {
    tracing::error!(error = %message, "node api internal error");
    Failure::Internal { message }
}
pub(crate) async fn idempotent<T, F, Fut>(operation: F) -> swarmy_store::Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = swarmy_store::Result<T>>,
{
    match operation().await {
        Err(StoreError::Storage(StorageError::CommitUnknown)) => operation().await,
        other => other,
    }
}
