//! Tool claims on persistent agent volumes. Completion never publishes disk state.
use crate::{Result, Store, StoreError, read, scan, write};
use foundationdb::Transaction;
use jiff::Timestamp;
use swarmy_core::{
    BashResult, Event, ImageRecord, LeaseOwnerId, PlacedToolClaim, PlacementRecord, SessionId,
    SessionState, ToolJob, VolumeId, VolumeRecord,
};

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredPlacedClaim {
    owner: LeaseOwnerId,
    placement: PlacementRecord,
    expires_at: Timestamp,
    job_digest: [u8; 32],
}

impl Store {
    fn volume_placement_key(&self, id: VolumeId) -> Vec<u8> {
        self.root
            .pack(&("volume_placement", id.as_ulid().to_bytes().as_slice()))
    }

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
            self.check_live_placement(&trx, placement).await?;
            if self.session(&trx, session).await?.agent_id != placement.agent_id {
                return Err(StoreError::InvalidState);
            }
            let id = VolumeId::from_ulid(placement.agent_id.as_ulid());
            let key = self.volume_placement_key(id);
            let binding: Option<PlacementRecord> = read(&trx, &key).await?;
            if let Some(volume) = read::<VolumeRecord>(&trx, &self.volume_key(id)).await? {
                if volume
                    .writer_lease
                    .is_some_and(|lease| lease.expires_at > Timestamp::now())
                    && !binding.is_some_and(|old| {
                        old.node_id == placement.node_id && old.epoch == placement.epoch
                    })
                {
                    return Err(StoreError::LeaseMismatch);
                }
            } else {
                let legacy_key = self
                    .root
                    .pack(&("sandbox", session.as_ulid().to_bytes().as_slice()));
                let manifest = if let Some(legacy) =
                    read::<swarmy_core::SandboxRecord>(&trx, &legacy_key).await?
                {
                    legacy.manifest_id
                } else {
                    read::<ImageRecord>(
                        &trx,
                        &self
                            .root
                            .pack(&("session_image", session.as_ulid().to_bytes().as_slice())),
                    )
                    .await?
                    .ok_or(StoreError::ManifestMissing)?
                    .manifest_id
                };
                write(
                    &trx,
                    &self.volume_key(id),
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
            read::<PlacementRecord>(trx, &self.volume_placement_key(id)).await?
        {
            if owner.as_ulid() != placement.node_id.as_ulid() {
                return Err(StoreError::LeaseMismatch);
            }
            self.check_live_placement(trx, &placement).await?;
        }
        Ok(())
    }

    /// Claim a pending job without creating an attempt volume.
    /// # Errors
    /// Rejects mismatched jobs, sessions, placements, and storage failures.
    pub async fn claim_placed_tool(&self, claim: &PlacedToolClaim) -> Result<bool> {
        self.transaction(|trx| async move {
            self.check_live_placement(&trx, &claim.placement).await?;
            self.check_tool_dispatch(&trx, &claim.job, &claim.placement)
                .await?;
            let Some(value) = trx
                .get(&self.tool_key("tool_job", claim.job.request_id), false)
                .await?
            else {
                return Ok(false);
            };
            let session = self.session(&trx, claim.job.session_id).await?;
            if session.agent_id != claim.placement.agent_id
                || session.state != SessionState::WaitingTools
                || claim.expires_at <= Timestamp::now()
            {
                return Err(StoreError::InvalidState);
            }
            if self.hydrate::<ToolJob>(&value).await? != claim.job {
                return Err(StoreError::InvalidState);
            }
            let key = self.tool_key("placed_tool_claim", claim.job.request_id);
            if read::<StoredPlacedClaim>(&trx, &key)
                .await?
                .is_some_and(|old| old.expires_at > Timestamp::now())
            {
                return Ok(false);
            }
            write(
                &trx,
                &key,
                &StoredPlacedClaim {
                    owner: claim.owner,
                    placement: claim.placement.clone(),
                    expires_at: claim.expires_at,
                    job_digest: crate::tools::job_digest(&claim.job)?,
                },
            )?;
            Ok(true)
        })
        .await
    }

    async fn check_placed_tool(
        &self,
        trx: &Transaction,
        claim: &PlacedToolClaim,
    ) -> Result<StoredPlacedClaim> {
        self.check_live_placement(trx, &claim.placement).await?;
        self.check_tool_dispatch(trx, &claim.job, &claim.placement)
            .await?;
        let current: StoredPlacedClaim = read(
            trx,
            &self.tool_key("placed_tool_claim", claim.job.request_id),
        )
        .await?
        .ok_or(StoreError::LeaseMismatch)?;
        if current.owner != claim.owner
            || current.job_digest != crate::tools::job_digest(&claim.job)?
            || current.placement != claim.placement
            || current.expires_at <= Timestamp::now()
        {
            return Err(StoreError::LeaseMismatch);
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
                return Err(StoreError::LeaseMismatch);
            }
            current.expires_at = expires_at;
            write(
                &trx,
                &self.tool_key("placed_tool_claim", claim.job.request_id),
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
        result: &BashResult,
    ) -> Result<()> {
        let job = &claim.job;
        let head = expected_head
            .checked_add(1)
            .ok_or(StoreError::SequenceOverflow)?;
        let event = self
            .prepare(&Event::ToolCallCompleted {
                seq: head,
                request_id: job.request_id,
                call_id: job.call_id.clone(),
                result: result.tool_result(),
            })
            .await?;
        self.transaction(|trx| {
            let event = &event;
            async move {
                self.check_placed_tool(&trx, claim).await?;
                let mut session = self.session(&trx, job.session_id).await?;
                if session.head_seq != expected_head {
                    return Err(StoreError::StaleSequence {
                        expected: expected_head,
                        actual: session.head_seq,
                    });
                }
                if session.state != SessionState::WaitingTools
                    || session.agent_id != claim.placement.agent_id
                {
                    return Err(StoreError::InvalidState);
                }
                trx.set(&self.event_space(job.session_id).pack(&(head,)), event);
                trx.clear(&self.tool_key("tool_job", job.request_id));
                trx.clear(&self.tool_key("tool_placement", job.request_id));
                trx.clear(&self.tool_key("placed_tool_claim", job.request_id));
                write(&trx, &self.tool_key("tool_done", job.request_id), &true)?;
                let pending = self.pending_space(job.session_id);
                trx.clear(&pending.pack(&(job.request_id.as_bytes().as_slice(),)));
                session.head_seq = head;
                if scan(&trx, pending.range(), 1).await?.is_empty() {
                    self.transition(&trx, session, SessionState::Runnable, Timestamp::now())
                        .await
                } else {
                    write(&trx, &self.session_key(job.session_id), &session)
                }
            }
        })
        .await
    }
}
