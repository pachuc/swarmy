//! Hosting decision before a node boots or reuses a computer.
use crate::{Result, Store, StoreError, read};
use std::time::Duration;
use swarmy_core::{AgentId, NodeId, PlacementRecord, ToolJob};

/// Result of opening a placement for hosting.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpenHosting {
    Opened(PlacementRecord),
    EpochStopped { epoch: u64 },
    PlacedElsewhere { node: NodeId, epoch: u64 },
}

impl Store {
    /// Open a placement, refusing an epoch already stopped before renewing it.
    /// # Errors
    /// Rejects stale dispatch, admission failures and storage failures.
    pub async fn open_hosting(
        &self,
        node: NodeId,
        agent: AgentId,
        job: &ToolJob,
        previous_epoch: Option<u64>,
        lease: Duration,
    ) -> Result<OpenHosting> {
        let (dispatched, current) = self
            .transaction(|trx| async move {
                let dispatch_key = self.keys().tool_placement(job.request_id);
                let placement_key = self.keys().placement(agent);
                futures::try_join!(
                    read::<PlacementRecord>(&trx, &dispatch_key),
                    read::<PlacementRecord>(&trx, &placement_key),
                )
            })
            .await?;
        let now = self.now();
        let expiry = now
            .checked_add(lease)
            .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?;
        let placement = match current {
            None => {
                if dispatched.is_some() {
                    return Err(stale("dispatch placement was released"));
                }
                self.place(agent, node, expiry).await?
            }
            Some(old) if old.expires_at <= now => {
                if dispatched.is_some() {
                    return Err(stale(
                        "dispatch placement expired; worker must recover the call",
                    ));
                }
                self.take_over(&old, node, expiry).await?
            }
            Some(old) if old.node_id != node => {
                return Ok(OpenHosting::PlacedElsewhere {
                    node: old.node_id,
                    epoch: old.epoch,
                });
            }
            Some(old) => {
                if dispatched
                    .as_ref()
                    .is_some_and(|expected| expected.epoch != old.epoch)
                {
                    return Err(stale("dispatch placement epoch changed"));
                }
                if previous_epoch == Some(old.epoch) {
                    return Ok(OpenHosting::EpochStopped { epoch: old.epoch });
                }
                self.renew_hosted(&old, expiry).await?.0
            }
        };
        self.claim_placement(&placement).await?;
        Ok(OpenHosting::Opened(placement))
    }
}
fn stale(detail: &'static str) -> StoreError {
    StoreError::Fence(crate::FenceError::DispatchPlacementStale { detail })
}
