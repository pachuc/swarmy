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
                internal(error, message)
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
        StoreError::Storage(_) => internal(error, message),
    }
}
fn internal(error: &StoreError, message: String) -> Failure {
    tracing::error!(error = %swarmy_core::error_chain(error), "node api internal error");
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
#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[test]
    fn maps_store_failures() {
        let cases = [
            (StoreError::Fence(FenceError::PlacementMismatch), 409),
            (StoreError::Fence(FenceError::PlacementAgentMismatch), 409),
            (StoreError::Fence(FenceError::PlacedToolClaimMismatch), 409),
            (StoreError::Fence(FenceError::VolumeLeaseMismatch), 409),
            (StoreError::Fence(FenceError::VolumeHeadMismatch), 409),
            (
                StoreError::Fence(FenceError::StaleSequence {
                    expected: 1,
                    actual: 2,
                }),
                409,
            ),
            (StoreError::Fence(FenceError::ToolJobMismatch), 409),
            (
                StoreError::Fence(FenceError::DispatchPlacementStale { detail: "stale" }),
                409,
            ),
            (StoreError::Fence(FenceError::InflightMismatch), 500),
            (StoreError::Fence(FenceError::SessionAgentMismatch), 500),
            (StoreError::Fence(FenceError::ToolClaimMismatch), 500),
            (StoreError::Fence(FenceError::LeaseMismatch), 500),
            (StoreError::Fence(FenceError::GcLeaseMismatch), 500),
            (StoreError::Fence(FenceError::ProvisionerLeaseMismatch), 500),
            (
                StoreError::Fence(FenceError::CredentialRefreshMismatch),
                500,
            ),
            (StoreError::Domain(DomainError::ComputerDeleted), 409),
            (
                StoreError::Domain(DomainError::NodeAtCapacity {
                    detail: "full".into(),
                }),
                409,
            ),
            (
                StoreError::Domain(DomainError::GpuUnavailable {
                    detail: "none".into(),
                }),
                409,
            ),
            (
                StoreError::Domain(DomainError::ImageMissing {
                    image: "x".into(),
                    registered: "".into(),
                }),
                409,
            ),
            (StoreError::Domain(DomainError::NodeMissing), 400),
            (StoreError::Storage(StorageError::CommitUnknown), 503),
            (StoreError::Storage(StorageError::Corrupt), 500),
        ];
        for (error, status) in cases {
            assert_eq!(failure(&error).status().as_u16(), status);
        }
    }
    #[tokio::test]
    async fn repeats_only_unknown_commits_once() {
        let calls = AtomicUsize::new(0);
        let result = idempotent(|| async {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(StoreError::Storage(StorageError::CommitUnknown))
            } else {
                Ok(5)
            }
        })
        .await
        .unwrap();
        assert_eq!(result, 5);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        calls.store(0, Ordering::SeqCst);
        let result = idempotent(|| async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(StoreError::Storage(StorageError::CommitUnknown))
        })
        .await;
        assert!(matches!(
            result,
            Err(StoreError::Storage(StorageError::CommitUnknown))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        calls.store(0, Ordering::SeqCst);
        let result = idempotent(|| async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(StoreError::Fence(FenceError::PlacementMismatch))
        })
        .await;
        assert!(matches!(
            result,
            Err(StoreError::Fence(FenceError::PlacementMismatch))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
