//! Permanent computer deletion fences both placement and volume recreation.
use crate::{MAX_SCAN_LIMIT, Result, Store, StoreError, StoredSession, read, scan, write};
use foundationdb::Transaction;
use jiff::Timestamp;
use swarmy_core::{AgentId, SessionId, SessionKind, SessionState, VolumeId, decode};

impl Store {
    /// Whether this computer has been permanently deleted.
    /// # Errors
    /// Returns storage or decoding failures.
    pub async fn is_computer_deleted(&self, agent: AgentId) -> Result<bool> {
        self.transaction(|trx| async move { self.computer_deleted(&trx, agent).await })
            .await
    }
    pub(crate) async fn computer_deleted(&self, trx: &Transaction, agent: AgentId) -> Result<bool> {
        Ok(read(trx, &self.computer_deleted_key(agent))
            .await?
            .unwrap_or(false))
    }

    pub(crate) async fn check_computer(&self, trx: &Transaction, agent: AgentId) -> Result<()> {
        if self.computer_deleted(trx, agent).await? {
            return Err(StoreError::ComputerDeleted);
        }
        Ok(())
    }

    /// Check immediately before running a tool; placement claims also fence node execution.
    /// # Errors
    /// Returns `ComputerDeleted` with the user-facing refusal, or a storage failure.
    pub async fn ensure_session_computer(&self, id: SessionId) -> Result<()> {
        self.transaction(|trx| async move {
            let session = self.session(&trx, id).await?;
            self.check_computer(&trx, session.agent_id).await
        })
        .await
    }

    /// Permanently delete a computer, retaining the identity and all transcripts.
    /// The tombstone marks every associated session, including legacy sessions,
    /// without an unbounded transaction. Node renewal fails and discards local state.
    /// # Errors
    /// Returns storage or decoding failures.
    pub async fn delete_computer(&self, agent: AgentId) -> Result<()> {
        self.transaction(|trx| async move { self.delete_computer_in(&trx, agent).await })
            .await
    }

    pub(crate) async fn delete_computer_in(&self, trx: &Transaction, agent: AgentId) -> Result<()> {
        // Reading the current epoch conflicts with renewal, takeover, dispatch and
        // publication. No holder of an earlier token can commit after this deletion.
        self.release_deleted_computer(trx, agent).await?;
        write(trx, &self.computer_deleted_key(agent), &true)?;
        let volume = VolumeId::from_ulid(agent.as_ulid());
        trx.clear(&self.volume_key(volume));
        trx.clear(&self.volume_snapshots_key(volume));
        Ok(())
    }

    /// Close a side or ephemeral session. Only ephemeral computers are deleted. Safe to repeat.
    /// # Errors
    /// Rejects main sessions and returns storage or decoding failures.
    pub async fn close_session(&self, id: SessionId, now: Timestamp) -> Result<()> {
        self.transaction(|trx| async move { self.close_session_in(&trx, id, now).await })
            .await
    }

    async fn close_session_in(
        &self,
        trx: &Transaction,
        id: SessionId,
        now: Timestamp,
    ) -> Result<()> {
        let session = self.session(trx, id).await?;
        match self.session_kind(trx, id).await? {
            SessionKind::Ephemeral => self.delete_computer_in(trx, session.agent_id).await?,
            SessionKind::Named { agent_id } => {
                if self
                    .read_agent(trx, agent_id)
                    .await?
                    .is_some_and(|agent| agent.main_session == Some(id))
                {
                    return Err(StoreError::MainSessionClose);
                }
            }
        }
        self.transition(trx, session, SessionState::Completed, now)
            .await
    }

    /// Close idle ephemeral sessions older than the retention interval, in bounded transactions.
    /// Legacy idle sessions without a timestamp start their retention clock on first observation.
    /// Each candidate is rechecked transactionally against concurrent activity and deletion.
    /// # Errors
    /// Returns storage, decoding, or timestamp-range failures.
    pub async fn sweep_ephemeral_sessions(
        &self,
        now: Timestamp,
        retention: std::time::Duration,
    ) -> Result<usize> {
        let cutoff = now
            .checked_sub(retention)
            .map_err(|_| StoreError::InvalidState)?;
        let mut cursor = None;
        let mut closed = 0;
        loop {
            let page: Vec<StoredSession> = self
                .transaction(|trx| async move {
                    let (mut begin, end) = self.root.subspace(&("session",)).range();
                    if let Some(id) = cursor {
                        begin = self.session_key(id);
                        begin.push(0);
                    }
                    scan(&trx, (begin, end), MAX_SCAN_LIMIT)
                        .await?
                        .into_iter()
                        .map(|(_, value)| decode(&value).map_err(Into::into))
                        .collect()
                })
                .await?;
            if page.is_empty() {
                return Ok(closed);
            }
            cursor = page.last().map(|s| s.session_id);
            for candidate in page {
                if candidate.state != SessionState::Idle {
                    continue;
                }
                closed += usize::from(
                    self.transaction(|trx| async move {
                        let id = candidate.session_id;
                        let session = self.session(&trx, id).await?;
                        if session.state != SessionState::Idle
                            || self.session_kind(&trx, id).await? != SessionKind::Ephemeral
                            || self.computer_deleted(&trx, session.agent_id).await?
                        {
                            return Ok(false);
                        }
                        let key = self.session_idle_key(id);
                        let Some(idle_since) = read::<Timestamp>(&trx, &key).await? else {
                            write(&trx, &key, &now)?;
                            return Ok(false);
                        };
                        if idle_since >= cutoff {
                            return Ok(false);
                        }
                        self.close_session_in(&trx, id, now).await?;
                        Ok(true)
                    })
                    .await?,
                );
            }
        }
    }
}
