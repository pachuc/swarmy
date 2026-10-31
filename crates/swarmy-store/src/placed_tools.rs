//! Tool claims on persistent agent volumes. Completion never publishes disk state.
use crate::{Result, Store, StoreError, read, scan, write};
use foundationdb::Transaction;
use jiff::Timestamp;
#[cfg(any(test, feature = "test-support"))]
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};
use swarmy_core::{
    Event, LeaseOwnerId, PlacedToolClaim, PlacementRecord, SessionId, SessionState, ToolJob,
    ToolResult, VolumeId, VolumeRecord,
};
#[cfg(any(test, feature = "test-support"))]
use tokio::sync::Mutex;

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredPlacedClaim {
    owner: LeaseOwnerId,
    placement: PlacementRecord,
    expires_at: Timestamp,
    job_digest: [u8; 32],
}

fn job_digest(job: &ToolJob) -> Result<[u8; 32]> {
    Ok(*blake3::hash(&swarmy_core::encode(job)?).as_bytes())
}

impl Store {
    /// Resolve the shared agent volume, initializing it from the session image.
    /// Existing slice 2 disks are imported once from their published head.
    /// Bind publication authority before attaching; never rebind a live writer.
    /// # Errors
    /// Rejects stale placements, live writers from a previous epoch, or missing images.
    pub async fn agent_volume(
        &self,
        session: SessionId,
        placement: &PlacementRecord,
    ) -> Result<VolumeId> {
        self.transaction(|trx| async move {
            let id = VolumeId::from_ulid(placement.agent_id.as_ulid());
            let key = self.keys().volume_placement(id);
            let volume_key = self.keys().volume(id);
            let (live, stored, binding, volume) = futures::join!(
                self.check_live_placement(&trx, placement),
                self.session(&trx, session),
                read::<PlacementRecord>(&trx, &key),
                read::<VolumeRecord>(&trx, &volume_key),
            );
            live?;
            let stored = stored?;
            if stored.agent_id != placement.agent_id {
                return Err(StoreError::Fence(crate::FenceError::PlacementAgentMismatch));
            }
            let binding = binding?;
            if let Some(volume) = volume? {
                if volume
                    .writer_lease
                    .is_some_and(|lease| lease.expires_at > self.now())
                    && !binding.is_some_and(|old| {
                        old.node_id == placement.node_id && old.epoch == placement.epoch
                    })
                {
                    return Err(StoreError::Fence(crate::FenceError::PlacementMismatch));
                }
            } else {
                let manifest = stored
                    .image
                    .as_ref()
                    .ok_or(StoreError::Domain(crate::DomainError::ManifestMissing))?
                    .manifest_id;
                self.write_new_volume(
                    &trx,
                    id,
                    &VolumeRecord {
                        head_manifest: manifest,
                        writer_lease: None,
                        parent: None,
                    },
                )?;
            }
            write(&trx, &key, placement)?;
            Ok(id)
        })
        .await
    }

    pub(crate) async fn check_volume_placement(
        &self,
        trx: &Transaction,
        id: VolumeId,
        owner: LeaseOwnerId,
    ) -> Result<()> {
        if let Some(placement) =
            read::<PlacementRecord>(trx, &self.keys().volume_placement(id)).await?
        {
            if owner.as_ulid() != placement.node_id.as_ulid() {
                return Err(StoreError::Fence(crate::FenceError::PlacementMismatch));
            }
            self.check_live_placement(trx, &placement).await?;
        }
        Ok(())
    }

    /// Claim a pending job without creating an attempt volume.
    /// # Errors
    /// Rejects mismatched jobs, sessions, placements, and storage failures.
    pub async fn claim_placed_tool(&self, claim: &PlacedToolClaim) -> Result<bool> {
        self.transaction(|trx| async move { self.claim_placed_tool_in(&trx, claim).await })
            .await
    }

    async fn claim_placed_tool_in(
        &self,
        trx: &Transaction,
        claim: &PlacedToolClaim,
    ) -> Result<bool> {
        let job_key = self.keys().tool_job(claim.job.request_id);
        let key = self.keys().placed_tool_claim(claim.job.request_id);
        let (live, dispatch, value, session, current) = futures::join!(
            self.check_live_placement(trx, &claim.placement),
            self.check_tool_dispatch(trx, &claim.job, &claim.placement),
            trx.get(&job_key, false),
            self.session(trx, claim.job.session_id),
            read::<StoredPlacedClaim>(trx, &key),
        );
        live?;
        dispatch?;
        let Some(value) = value? else {
            return Ok(false);
        };
        let session = session?;
        if session.agent_id != claim.placement.agent_id
            || session.state != SessionState::WaitingTools
            || claim.expires_at <= self.now()
        {
            return Err(StoreError::Fence(crate::FenceError::ToolClaimMismatch));
        }
        if !crate::stored_matches(&value, &claim.job)? {
            return Err(StoreError::Fence(crate::FenceError::ToolJobMismatch));
        }
        if current?.is_some_and(|old| old.expires_at > self.now()) {
            return Ok(false);
        }
        write(
            trx,
            &key,
            &StoredPlacedClaim {
                owner: claim.owner,
                placement: claim.placement.clone(),
                expires_at: claim.expires_at,
                job_digest: job_digest(&claim.job)?,
            },
        )?;
        Ok(true)
    }

    /// Claim like `claim_placed_tool`, but await `race` after the claim's reads
    /// and before its commit on the first attempt. Returns the result and attempt count.
    /// Test-only entry point, also available with the `test-support` feature.
    /// # Errors
    /// Rejects mismatched jobs, sessions, placements, and storage failures.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn claim_placed_tool_racing(
        &self,
        claim: &PlacedToolClaim,
        race: impl Future<Output = ()>,
    ) -> Result<(bool, u32)> {
        let race = Arc::new(Mutex::new(Some(Box::pin(race))));
        let attempts = AtomicU32::new(0);
        let claimed = self
            .transaction(|trx| {
                let race = Arc::clone(&race);
                let attempts = &attempts;
                async move {
                    attempts.fetch_add(1, Ordering::Relaxed);
                    let claimed = self.claim_placed_tool_in(&trx, claim).await?;
                    let first = race.lock().await.take();
                    if let Some(first) = first {
                        first.await;
                    }
                    Ok(claimed)
                }
            })
            .await?;
        Ok((claimed, attempts.load(Ordering::Relaxed)))
    }

    async fn check_placed_tool(
        &self,
        trx: &Transaction,
        claim: &PlacedToolClaim,
    ) -> Result<StoredPlacedClaim> {
        let key = self.keys().placed_tool_claim(claim.job.request_id);
        let ((), (), current) = futures::try_join!(
            self.check_live_placement(trx, &claim.placement),
            self.check_tool_dispatch(trx, &claim.job, &claim.placement),
            read::<StoredPlacedClaim>(trx, &key),
        )?;
        let current = current.ok_or(StoreError::Fence(
            crate::FenceError::PlacedToolClaimMismatch,
        ))?;
        if current.owner != claim.owner
            || current.job_digest != job_digest(&claim.job)?
            || current.placement != claim.placement
            || current.expires_at <= self.now()
        {
            return Err(StoreError::Fence(
                crate::FenceError::PlacedToolClaimMismatch,
            ));
        }
        Ok(current)
    }

    /// # Errors
    /// Rejects expired or replaced tool claims and placement epochs.
    pub async fn renew_placed_tool(
        &self,
        claim: &PlacedToolClaim,
        expires_at: Timestamp,
    ) -> Result<()> {
        self.transaction(|trx| async move {
            let mut current = self.check_placed_tool(&trx, claim).await?;
            if expires_at <= current.expires_at {
                return Err(StoreError::Fence(
                    crate::FenceError::PlacedToolClaimMismatch,
                ));
            }
            current.expires_at = expires_at;
            write(
                &trx,
                &self.keys().placed_tool_claim(claim.job.request_id),
                &current,
            )
        })
        .await
    }

    /// Record output under the live placement and tool leases, without advancing a volume.
    /// The result's manifest names the latest observed checkpoint, not this call's writes.
    /// # Errors
    /// Rejects stale heads, claims, placement epochs, and invalid sessions.
    pub async fn complete_placed_tool(
        &self,
        claim: &PlacedToolClaim,
        expected_head: u64,
        result: &ToolResult,
    ) -> Result<()> {
        let job = &claim.job;
        let head = crate::seq_after(expected_head, 1)?;
        let event = self
            .prepare(&Event::ToolCallCompleted {
                seq: head,
                request_id: job.request_id,
                call_id: job.call_id.clone(),
                result: result.clone(),
            })
            .await?;
        self.transaction(|trx| {
            let event = &event;
            async move {
                let (_, mut session) = futures::try_join!(
                    self.check_placed_tool(&trx, claim),
                    self.session(&trx, job.session_id),
                )?;
                crate::check_head(session.head_seq, expected_head)?;
                if session.state != SessionState::WaitingTools
                    || session.agent_id != claim.placement.agent_id
                {
                    return Err(StoreError::Fence(crate::FenceError::ToolClaimMismatch));
                }
                trx.set(&self.keys().event(job.session_id, head), event);
                trx.clear(&self.keys().tool_job(job.request_id));
                trx.clear(&self.keys().tool_placement(job.request_id));
                trx.clear(&self.keys().placed_tool_claim(job.request_id));
                write(&trx, &self.keys().tool_done(job.request_id), &true)?;
                let pending = self.keys().session_tools_space(job.session_id);
                trx.clear(&self.keys().session_tools(job.session_id, job.request_id));
                session.head_seq = head;
                if scan(&trx, pending.range(), 1).await?.is_empty() {
                    self.transition(&trx, session, SessionState::Runnable, self.now())
                        .await
                } else {
                    self.write_session(&trx, &session)
                }
            }
        })
        .await
    }
}
