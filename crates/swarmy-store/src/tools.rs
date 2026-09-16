//! Sandbox placement and durable tool handoffs. Attempt disks stay private until
//! their manifest and the completion event are committed together.
use crate::{Result, Store, StoreError, read, scan, write};
use jiff::Timestamp;
use std::time::Duration;
use swarmy_core::{
    BashResult, Event, ImageRecord, ImageTag, Lease, ManifestId, NodeRole, RequestId,
    SandboxRecord, SessionId, SessionState, ToolClaim, ToolJob, VolumeId, VolumeRecord,
};

// Keep claims inline even when a command input uses the blob path.
#[derive(serde::Serialize, serde::Deserialize)]
struct StoredToolClaim {
    owner: swarmy_core::LeaseOwnerId,
    node_id: swarmy_core::NodeId,
    expires_at: Timestamp,
    attempt_volume: VolumeId,
    job_digest: [u8; 32],
}

pub(crate) fn job_digest(job: &ToolJob) -> Result<[u8; 32]> {
    Ok(*blake3::hash(&swarmy_core::encode(job)?).as_bytes())
}

impl Store {
    fn sandbox_key(&self, id: SessionId) -> Vec<u8> {
        self.root
            .pack(&("sandbox", id.as_ulid().to_bytes().as_slice()))
    }
    pub(crate) fn tool_key(&self, kind: &str, id: RequestId) -> Vec<u8> {
        self.root.pack(&(kind, id.as_bytes().as_slice()))
    }
    pub(crate) fn pending_space(&self, id: SessionId) -> foundationdb::tuple::Subspace {
        self.root
            .subspace(&("session_tools", id.as_ulid().to_bytes().as_slice()))
    }

    /// # Errors
    /// Returns storage or decoding failures.
    pub async fn session_image(&self, id: SessionId) -> Result<Option<ManifestId>> {
        self.transaction(|trx| async move {
            Ok(read::<ImageRecord>(
                &trx,
                &self
                    .root
                    .pack(&("session_image", id.as_ulid().to_bytes().as_slice())),
            )
            .await?
            .map(|image| image.manifest_id))
        })
        .await
    }

    /// Resolve and pin an image before the session is woken for its first turn.
    /// # Errors
    /// Rejects missing images, sessions already started, and storage failures.
    pub async fn set_session_image(&self, id: SessionId, name: &str, tag: &ImageTag) -> Result<()> {
        let manifest = self
            .get_image(name, tag)
            .await?
            .ok_or(StoreError::ManifestMissing)?;
        self.transaction(|trx| async move {
            let session = self.session(&trx, id).await?;
            if session.state != SessionState::Idle || session.head_seq != 0 {
                return Err(StoreError::InvalidState);
            }
            write(
                &trx,
                &self
                    .root
                    .pack(&("session_image", id.as_ulid().to_bytes().as_slice())),
                &ImageRecord {
                    name: name.into(),
                    tag: tag.clone(),
                    manifest_id: manifest,
                },
            )
        })
        .await
    }

    /// # Errors
    /// Returns storage or decoding failures.
    pub async fn get_sandbox(&self, id: SessionId) -> Result<Option<SandboxRecord>> {
        self.transaction(|trx| async move { read(&trx, &self.sandbox_key(id)).await })
            .await
    }

    /// Place on the first live sandbox node. Existing live placements are reused;
    /// moving a dead placement waits for its active tool lease to expire.
    /// # Errors
    /// Rejects absent images/nodes, active calls on dead nodes, and storage failures.
    pub async fn place_sandbox(&self, id: SessionId, now: Timestamp) -> Result<SandboxRecord> {
        let since = now
            .checked_sub(Duration::from_secs(30))
            .map_err(|_| StoreError::InvalidState)?;
        let mut cursor = None;
        let node = loop {
            let (nodes, next) = self
                .scan_live_nodes(cursor, since, crate::MAX_SCAN_LIMIT)
                .await?;
            if let Some(node) = nodes
                .into_iter()
                .find(|node| node.roles.contains(&NodeRole::Sandbox))
            {
                break node.node_id;
            }
            if next.is_none() {
                return Err(StoreError::InvalidState);
            }
            cursor = next;
        };
        self.transaction(|trx| async move {
            self.session(&trx, id).await?;
            let selected: swarmy_core::NodeRecord = read(&trx, &self.node_key(node))
                .await?
                .ok_or(StoreError::InvalidState)?;
            if selected.last_heartbeat < since {
                return Err(StoreError::InvalidState);
            }
            let key = self.sandbox_key(id);
            if let Some(mut record) = read::<SandboxRecord>(&trx, &key).await? {
                if read::<swarmy_core::NodeRecord>(&trx, &self.node_key(record.node_id))
                    .await?
                    .is_some_and(|old| old.last_heartbeat >= since)
                {
                    return Ok(record);
                }
                if let Some(active) = record.active_call
                    && read::<StoredToolClaim>(&trx, &self.tool_key("tool_claim", active))
                        .await?
                        .is_some_and(|claim| claim.expires_at > now)
                {
                    return Err(StoreError::LeaseMismatch);
                }
                record.node_id = node;
                record.active_call = None;
                write(&trx, &key, &record)?;
                return Ok(record);
            }
            let image: ImageRecord = read(
                &trx,
                &self
                    .root
                    .pack(&("session_image", id.as_ulid().to_bytes().as_slice())),
            )
            .await?
            .ok_or(StoreError::ManifestMissing)?;
            let manifest = image.manifest_id;
            let volume_id = VolumeId::from_ulid(id.as_ulid());
            if read::<VolumeRecord>(&trx, &self.volume_key(volume_id))
                .await?
                .is_some()
            {
                return Err(StoreError::VolumeExists);
            }
            write(
                &trx,
                &self.volume_key(volume_id),
                &VolumeRecord {
                    head_manifest: manifest,
                    writer_lease: None,
                    parent: None,
                },
            )?;
            let record = SandboxRecord {
                session_id: id,
                node_id: node,
                volume_id,
                manifest_id: manifest,
                active_call: None,
            };
            write(&trx, &key, &record)?;
            Ok(record)
        })
        .await
    }

    /// Save all remote jobs and release the worker in one transaction after their
    /// request events were recorded. Scans repair a lost publish after this commit.
    /// # Errors
    /// Rejects stale workers, unrelated jobs, and storage failures.
    pub async fn dispatch_tool_jobs(
        &self,
        id: SessionId,
        lease: &Lease,
        jobs: &[ToolJob],
    ) -> Result<()> {
        let mut values = Vec::new();
        for job in jobs {
            values.push(self.prepare(job).await?);
        }
        self.transaction(|trx| {
            let values = &values;
            async move {
                self.check_worker_lease(&trx, id, lease, Timestamp::now())
                    .await?;
                if jobs.is_empty() {
                    return Err(StoreError::InvalidState);
                }
                for (job, value) in jobs.iter().zip(values) {
                    if job.session_id != id
                        || RequestId::for_step(id, job.step) != job.request_id
                        || !job.arguments.valid()
                    {
                        return Err(StoreError::InvalidState);
                    }
                    let event = trx
                        .get(&self.event_space(id).pack(&(job.step,)), false)
                        .await?
                        .ok_or(StoreError::InvalidState)?;
                    match self.hydrate::<Event>(&event).await? {
                        Event::ToolCallRequested {
                            request_id, call, ..
                        } if request_id == job.request_id
                            && call.call_id == job.call_id
                            && swarmy_core::SandboxArguments::parse(
                                &call.tool,
                                call.arguments.clone(),
                            )
                            .ok()
                            .as_ref()
                                == Some(&job.arguments) => {}
                        _ => return Err(StoreError::InvalidState),
                    }
                    trx.set(&self.tool_key("tool_job", job.request_id), value);
                    write(
                        &trx,
                        &self
                            .pending_space(id)
                            .pack(&(job.request_id.as_bytes().as_slice(),)),
                        &(),
                    )?;
                }
                let session = self.session(&trx, id).await?;
                self.transition(&trx, session, SessionState::WaitingTools, Timestamp::now())
                    .await
            }
        })
        .await
    }

    /// # Errors
    /// Returns invalid-limit, decoding, or storage failures.
    pub async fn scan_tool_jobs(
        &self,
        after: Option<RequestId>,
        limit: usize,
    ) -> Result<Vec<ToolJob>> {
        let values = self
            .transaction(|trx| async move {
                let (mut begin, end) = self.root.subspace(&("tool_job",)).range();
                if let Some(id) = after {
                    begin = self.tool_key("tool_job", id);
                    begin.push(0);
                }
                scan(&trx, (begin, end), limit).await
            })
            .await?;
        let mut jobs = Vec::new();
        for (_, value) in values {
            jobs.push(self.hydrate(&value).await?);
        }
        Ok(jobs)
    }

    /// Claim one pending call and create its private disk from the recorded head.
    /// Returns false for busy or completed calls. Serializes calls sharing a disk.
    /// # Errors
    /// Rejects invalid tokens, wrong nodes/jobs, and storage failures.
    pub async fn claim_tool(&self, claim: &ToolClaim) -> Result<bool> {
        self.transaction(|trx| async move {
            let now = Timestamp::now();
            if claim.expires_at <= now {
                return Err(StoreError::LeaseMismatch);
            }
            let job = &claim.job;
            let Some(value) = trx
                .get(&self.tool_key("tool_job", job.request_id), false)
                .await?
            else {
                return Ok(false);
            };
            if self.hydrate::<ToolJob>(&value).await? != *job {
                return Err(StoreError::InvalidState);
            }
            if self.session(&trx, job.session_id).await?.state != SessionState::WaitingTools {
                return Err(StoreError::InvalidState);
            }
            let key = self.sandbox_key(job.session_id);
            let mut sandbox: SandboxRecord =
                read(&trx, &key).await?.ok_or(StoreError::InvalidState)?;
            if sandbox.node_id != claim.node_id {
                return Err(StoreError::LeaseMismatch);
            }
            if let Some(active) = sandbox.active_call
                && read::<StoredToolClaim>(&trx, &self.tool_key("tool_claim", active))
                    .await?
                    .is_some_and(|old| old.expires_at > now)
            {
                return Ok(false);
            }
            if read::<VolumeRecord>(&trx, &self.volume_key(claim.attempt_volume))
                .await?
                .is_some()
            {
                return Err(StoreError::VolumeExists);
            }
            write(
                &trx,
                &self.volume_key(claim.attempt_volume),
                &VolumeRecord {
                    head_manifest: sandbox.manifest_id,
                    parent: Some(sandbox.volume_id),
                    writer_lease: None,
                },
            )?;
            sandbox.active_call = Some(job.request_id);
            write(&trx, &key, &sandbox)?;
            write(
                &trx,
                &self.tool_key("tool_claim", job.request_id),
                &StoredToolClaim {
                    owner: claim.owner,
                    node_id: claim.node_id,
                    expires_at: claim.expires_at,
                    attempt_volume: claim.attempt_volume,
                    job_digest: job_digest(job)?,
                },
            )?;
            Ok(true)
        })
        .await
    }

    /// # Errors
    /// Rejects expired or replaced claims, wrong placements, and storage failures.
    pub async fn renew_tool(&self, claim: &ToolClaim, expires_at: Timestamp) -> Result<()> {
        self.transaction(|trx| async move {
            let mut current = self.check_tool(&trx, claim).await?;
            if expires_at <= current.expires_at {
                return Err(StoreError::LeaseMismatch);
            }
            current.expires_at = expires_at;
            write(
                &trx,
                &self.tool_key("tool_claim", claim.job.request_id),
                &current,
            )
        })
        .await
    }

    async fn check_tool(
        &self,
        trx: &foundationdb::Transaction,
        claim: &ToolClaim,
    ) -> Result<StoredToolClaim> {
        let current: StoredToolClaim =
            read(trx, &self.tool_key("tool_claim", claim.job.request_id))
                .await?
                .ok_or(StoreError::LeaseMismatch)?;
        let sandbox: SandboxRecord = read(trx, &self.sandbox_key(claim.job.session_id))
            .await?
            .ok_or(StoreError::InvalidState)?;
        if current.owner != claim.owner
            || current.job_digest != job_digest(&claim.job)?
            || current.attempt_volume != claim.attempt_volume
            || current.node_id != claim.node_id
            || current.expires_at <= Timestamp::now()
            || sandbox.active_call != Some(claim.job.request_id)
            || sandbox.node_id != claim.node_id
        {
            return Err(StoreError::LeaseMismatch);
        }
        Ok(current)
    }

    /// Atomically advance the committed disk, append output and manifest, clear
    /// the claim, and wake the session only after its last outstanding call.
    /// # Errors
    /// Rejects stale heads/claims, unflushed disks, and storage failures.
    pub async fn complete_tool(
        &self,
        claim: &ToolClaim,
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
                if read::<bool>(&trx, &self.tool_key("tool_done", job.request_id)).await?
                    == Some(true)
                {
                    return Ok(());
                }
                self.check_tool(&trx, claim).await?;
                let mut session = self.session(&trx, job.session_id).await?;
                if session.head_seq != expected_head {
                    return Err(StoreError::StaleSequence {
                        expected: expected_head,
                        actual: session.head_seq,
                    });
                }
                if session.state != SessionState::WaitingTools {
                    return Err(StoreError::InvalidState);
                }
                let key = self.sandbox_key(job.session_id);
                let mut sandbox: SandboxRecord =
                    read(&trx, &key).await?.ok_or(StoreError::InvalidState)?;
                let attempt: VolumeRecord = read(&trx, &self.volume_key(claim.attempt_volume))
                    .await?
                    .ok_or(StoreError::VolumeMissing)?;
                let mut volume: VolumeRecord = read(&trx, &self.volume_key(sandbox.volume_id))
                    .await?
                    .ok_or(StoreError::VolumeMissing)?;
                if attempt.head_manifest != result.manifest_id
                    || attempt.writer_lease.is_some()
                    || volume.head_manifest != sandbox.manifest_id
                    || volume.writer_lease.is_some()
                {
                    return Err(StoreError::VolumeHeadMismatch);
                }
                volume.head_manifest = result.manifest_id;
                sandbox.manifest_id = result.manifest_id;
                sandbox.active_call = None;
                write(&trx, &self.volume_key(sandbox.volume_id), &volume)?;
                write(&trx, &key, &sandbox)?;
                trx.set(&self.event_space(job.session_id).pack(&(head,)), event);
                trx.clear(&self.tool_key("tool_job", job.request_id));
                trx.clear(&self.tool_key("tool_claim", job.request_id));
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

    /// # Errors
    /// Returns storage or decoding failures.
    pub async fn tool_completed(&self, id: RequestId) -> Result<bool> {
        self.transaction(|trx| async move {
            Ok(read::<bool>(&trx, &self.tool_key("tool_done", id)).await? == Some(true))
        })
        .await
    }
}
