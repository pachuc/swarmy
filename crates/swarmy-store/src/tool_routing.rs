//! Durable dispatch fences and recovery notices shared by all worker replicas.
use foundationdb::Transaction;
use jiff::Timestamp;
use swarmy_core::{
    Event, ImageRecord, Message, MessageId, MessageRole, Part, PlacementChangeReason,
    PlacementRecord, SessionId, SessionState, ToolJob, ToolResult, VolumeId, VolumeRecord,
    computer_rebuilt_message,
};

use crate::{Result, Store, StoreError, read, scan, write};

impl Store {
    /// Publish sampled call occupancy only for the current live placement.
    /// # Errors
    /// Rejects stale placement epochs and storage or encoding failures.
    pub async fn put_agent_call_status(&self, status: &swarmy_core::AgentCallStatus) -> Result<()> {
        self.transaction(|trx| async move {
            let placement: PlacementRecord = read(
                &trx,
                &self
                    .root
                    .pack(&("placement", status.agent_id.as_ulid().to_bytes().as_slice())),
            )
            .await?
            .ok_or(StoreError::LeaseMismatch)?;
            if placement.node_id != status.node_id || placement.epoch != status.epoch {
                return Err(StoreError::LeaseMismatch);
            }
            self.check_live_placement(&trx, &placement).await?;
            let key = self.root.pack(&(
                "agent_call_status",
                status.agent_id.as_ulid().to_bytes().as_slice(),
            ));
            if read::<swarmy_core::AgentCallStatus>(&trx, &key)
                .await?
                .is_some_and(|old| {
                    old.epoch == status.epoch && old.observed_at > status.observed_at
                })
            {
                return Ok(());
            }
            write(&trx, &key, status)
        })
        .await
    }

    /// Return recent call occupancy, or none after expiry, release, deletion or takeover.
    /// A missing observation means unknown occupancy, not an idle computer.
    /// # Errors
    /// Returns storage or decoding failures.
    pub async fn agent_call_status(
        &self,
        agent: swarmy_core::AgentId,
    ) -> Result<Option<swarmy_core::AgentCallStatus>> {
        self.transaction(|trx| async move {
            let key = self
                .root
                .pack(&("agent_call_status", agent.as_ulid().to_bytes().as_slice()));
            let Some(status) = read::<swarmy_core::AgentCallStatus>(&trx, &key).await? else {
                return Ok(None);
            };
            let placement: Option<PlacementRecord> = read(
                &trx,
                &self
                    .root
                    .pack(&("placement", agent.as_ulid().to_bytes().as_slice())),
            )
            .await?;
            let now = Timestamp::now();
            Ok(placement
                .filter(|placement| {
                    placement.node_id == status.node_id
                        && placement.epoch == status.epoch
                        && placement.expires_at > now
                        && status.expires_at > now
                })
                .map(|_| status))
        })
        .await
    }

    /// Read the durable dispatch fence before a node starts or rebuilds a computer.
    /// # Errors
    /// Returns storage or decoding failures.
    pub async fn tool_placement(
        &self,
        id: swarmy_core::RequestId,
    ) -> Result<Option<PlacementRecord>> {
        self.transaction(
            |trx| async move { read(&trx, &self.tool_key("tool_placement", id)).await },
        )
        .await
    }

    /// Check delivery admission and find its agent in one read transaction.
    /// Execution still requires a live placement and a fenced tool claim.
    /// # Errors
    /// Rejects delivery to another node, stale placements, and missing sessions.
    pub async fn tool_agent(
        &self,
        job: &ToolJob,
        node: swarmy_core::NodeId,
    ) -> Result<Option<swarmy_core::AgentId>> {
        self.transaction(|trx| async move {
            if read::<bool>(&trx, &self.tool_key("tool_done", job.request_id))
                .await?
                .unwrap_or(false)
            {
                return Ok(None);
            }
            let session = self.session(&trx, job.session_id).await?;
            self.check_computer(&trx, session.agent_id).await?;
            if let Some(placement) =
                read::<PlacementRecord>(&trx, &self.tool_key("tool_placement", job.request_id))
                    .await?
            {
                if placement.node_id != node {
                    return Err(StoreError::LeaseMismatch);
                }
                self.check_live_placement(&trx, &placement).await?;
            }
            Ok(Some(self.session(&trx, job.session_id).await?.agent_id))
        })
        .await
    }

    /// Resolve a pending dispatch against the current placement on every retry.
    /// A changed epoch completes the old call as failed instead of repeating its effects.
    /// Returns false when the job was completed or recovered and must not be published.
    /// # Errors
    /// Rejects stale placements, changed job inputs, and storage failures.
    pub async fn route_tool_job(&self, job: &ToolJob, placement: &PlacementRecord) -> Result<bool> {
        self.transaction(|trx| async move {
            self.check_live_placement(&trx, placement).await?;
            let Some(value) = trx
                .get(&self.tool_key("tool_job", job.request_id), false)
                .await?
            else {
                return Ok(false);
            };
            if self.hydrate::<ToolJob>(&value).await? != *job
                || self.session(&trx, job.session_id).await?.agent_id != placement.agent_id
            {
                return Err(StoreError::InvalidState);
            }
            let key = self.tool_key("tool_placement", job.request_id);
            let dispatched: Option<PlacementRecord> = read(&trx, &key).await?;
            let notice = self
                .deliver_computer_notice(&trx, job.session_id, placement)
                .await?;
            if dispatched
                .is_some_and(|old| old.epoch != placement.epoch || old.node_id != placement.node_id)
            {
                // A dispatch can expire without any node hosting its epoch.
                // Fail the fenced call without inventing a computer loss.
                let explanation = notice.unwrap_or_else(|| {
                    "The tool call's placement expired before execution could be confirmed. The call failed; check external side effects before retrying.".into()
                });
                self.fail_lost_tool(&trx, job, explanation).await?;
                return Ok(false);
            }
            // Jobs written by older workers acquire their first dispatch fence here.
            write(&trx, &key, placement)?;
            Ok(true)
        })
        .await
    }

    pub(crate) async fn check_tool_dispatch(
        &self,
        trx: &Transaction,
        job: &ToolJob,
        placement: &PlacementRecord,
    ) -> Result<()> {
        if let Some(dispatched) =
            read::<PlacementRecord>(trx, &self.tool_key("tool_placement", job.request_id)).await?
            && (dispatched.agent_id != placement.agent_id
                || dispatched.node_id != placement.node_id
                || dispatched.epoch != placement.epoch)
        {
            return Err(StoreError::LeaseMismatch);
        }
        Ok(())
    }

    async fn fail_lost_tool(
        &self,
        trx: &Transaction,
        job: &ToolJob,
        explanation: String,
    ) -> Result<()> {
        let mut session = self.session(trx, job.session_id).await?;
        if !matches!(
            session.state,
            SessionState::WaitingTools | SessionState::Completed
        ) {
            return Err(StoreError::InvalidState);
        }
        session.head_seq = session
            .head_seq
            .checked_add(1)
            .ok_or(StoreError::SequenceOverflow)?;
        let event = self
            .prepare(&Event::ToolCallCompleted {
                seq: session.head_seq,
                request_id: job.request_id,
                call_id: job.call_id.clone(),
                result: ToolResult::Error { error: explanation },
            })
            .await?;
        trx.set(
            &self.event_space(job.session_id).pack(&(session.head_seq,)),
            &event,
        );
        for kind in ["tool_job", "tool_placement", "placed_tool_claim"] {
            trx.clear(&self.tool_key(kind, job.request_id));
        }
        write(trx, &self.tool_key("tool_done", job.request_id), &true)?;
        let pending = self.pending_space(job.session_id);
        trx.clear(&pending.pack(&(job.request_id.as_bytes().as_slice(),)));
        if session.state != SessionState::Completed
            && scan(trx, pending.range(), 1).await?.is_empty()
        {
            self.transition(trx, session, SessionState::Runnable, Timestamp::now())
                .await
        } else {
            write(trx, &self.session_key(job.session_id), &session)
        }
    }

    /// Finish a pending call with the deletion refusal, without placing another computer.
    /// Returns false when the computer still exists.
    /// # Errors
    /// Returns storage, job validation, or decoding failures.
    pub async fn fail_deleted_computer_tool(&self, job: &ToolJob) -> Result<bool> {
        self.transaction(|trx| async move {
            let session = self.session(&trx, job.session_id).await?;
            if !self.computer_deleted(&trx, session.agent_id).await? {
                return Ok(false);
            }
            if let Some(value) = trx
                .get(&self.tool_key("tool_job", job.request_id), false)
                .await?
            {
                if self.hydrate::<ToolJob>(&value).await? != *job {
                    return Err(StoreError::InvalidState);
                }
                self.fail_lost_tool(&trx, job, StoreError::ComputerDeleted.to_string())
                    .await?;
            }
            Ok(true)
        })
        .await
    }

    pub(crate) async fn deliver_computer_notice(
        &self,
        trx: &Transaction,
        id: SessionId,
        placement: &PlacementRecord,
    ) -> Result<Option<String>> {
        if matches!(
            placement.last_change_reason,
            PlacementChangeReason::Initial | PlacementChangeReason::Unstarted
        ) {
            return Ok(None);
        }
        // Retain each observed epoch's explanation, even after later snapshots or
        // placement changes. Sessions have independent delivery cursors.
        let key = self.root.pack(&(
            "computer_notice",
            placement.agent_id.as_ulid().to_bytes().as_slice(),
            placement.epoch,
        ));
        let message = if let Some(message) = read::<Message>(trx, &key).await? {
            message
        } else {
            let volume = VolumeId::from_ulid(placement.agent_id.as_ulid());
            let manifest =
                if let Some(volume) = read::<VolumeRecord>(trx, &self.volume_key(volume)).await? {
                    volume.head_manifest
                } else {
                    read::<ImageRecord>(
                        trx,
                        &self
                            .root
                            .pack(&("session_image", id.as_ulid().to_bytes().as_slice())),
                    )
                    .await?
                    .ok_or(StoreError::ManifestMissing)?
                    .manifest_id
                };
            // Manifest IDs are time-ordered ULIDs minted for snapshot publication.
            let millis = i64::try_from(manifest.as_ulid().timestamp_ms())
                .map_err(|_| StoreError::Corrupt)?;
            let snapshot = Timestamp::from_millisecond(millis).map_err(|_| StoreError::Corrupt)?;
            let text = computer_rebuilt_message(
                placement.last_change_reason,
                snapshot,
                placement.last_changed_at,
                self.read_placement_hosting(trx, placement)
                    .await?
                    .and_then(|hosting| hosting.failure_estimate),
            )
            .ok_or(StoreError::Corrupt)?;
            let message = Message {
                id: MessageId::from_ulid(ulid::Ulid::generate()),
                role: MessageRole::System,
                parts: vec![Part::Text { text }],
            };
            write(trx, &key, &message)?;
            message
        };
        let Some(Part::Text { text }) = message.parts.first() else {
            return Err(StoreError::Corrupt);
        };
        let explanation = text.clone();
        // The index read conflicts with concurrent session creation, so every
        // session present at delivery receives the same epoch atomically. Include
        // the caller for legacy sessions created before the index existed.
        let (mut begin, end) = self
            .root
            .subspace(&(
                "session_by_agent",
                placement.agent_id.as_ulid().to_bytes().as_slice(),
            ))
            .range();
        self.append_computer_notice(trx, id, placement.epoch, &message)
            .await?;
        loop {
            let page = scan(trx, (begin.clone(), end.clone()), crate::MAX_SCAN_LIMIT).await?;
            if page.is_empty() {
                break;
            }
            for (key, value) in page {
                let session = swarmy_core::decode::<SessionId>(&value)?;
                self.append_computer_notice(trx, session, placement.epoch, &message)
                    .await?;
                begin = key;
                begin.push(0);
            }
        }
        Ok(Some(explanation))
    }

    async fn append_computer_notice(
        &self,
        trx: &Transaction,
        id: SessionId,
        epoch: u64,
        message: &Message,
    ) -> Result<()> {
        let delivered = self.root.pack(&(
            "computer_notice_delivered",
            id.as_ulid().to_bytes().as_slice(),
            epoch,
        ));
        if read::<bool>(trx, &delivered).await? != Some(true) {
            let mut session = self.session(trx, id).await?;
            session.head_seq = session
                .head_seq
                .checked_add(1)
                .ok_or(StoreError::SequenceOverflow)?;
            let event = self
                .prepare(&Event::MessageAppended {
                    seq: session.head_seq,
                    message: message.clone(),
                })
                .await?;
            trx.set(&self.event_space(id).pack(&(session.head_seq,)), &event);
            write(trx, &self.session_key(id), &session)?;
            write(trx, &delivered, &true)?;
        }
        Ok(())
    }
}
