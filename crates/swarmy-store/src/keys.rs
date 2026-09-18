use foundationdb::{Transaction, tuple::Subspace};
use jiff::Timestamp;
use swarmy_core::{ImageTag, ManifestId, RunnableEntry, SessionId, SessionState, VolumeId};

use crate::{Result, Store, StoreError, read, scan, write};

pub use swarmy_core::{RUNNABLE_PARTITIONS, runnable_partition};

impl Store {
    pub(crate) fn request_turn_key(&self, id: swarmy_core::RequestId) -> Vec<u8> {
        self.root.pack(&("request_turn", id.as_bytes().as_slice()))
    }

    /// Resolve the original user turn even when old work is redelivered later.
    /// # Errors
    /// Returns database and decoding errors.
    pub async fn request_turn_id(
        &self,
        id: swarmy_core::RequestId,
    ) -> Result<Option<swarmy_core::MessageId>> {
        self.transaction(|trx| async move { read(&trx, &self.request_turn_key(id)).await })
            .await
    }

    pub(crate) fn turn_key(&self, id: SessionId) -> Vec<u8> {
        self.root
            .pack(&("turn", id.as_ulid().to_bytes().as_slice()))
    }

    /// The latest user message identifies the turn, including after snapshots.
    /// # Errors
    /// Returns database and decoding errors.
    pub async fn turn_id(&self, id: SessionId) -> Result<Option<swarmy_core::MessageId>> {
        self.transaction(|trx| async move { read(&trx, &self.turn_key(id)).await })
            .await
    }

    pub(crate) fn volume_key(&self, id: VolumeId) -> Vec<u8> {
        self.root
            .pack(&("volume", id.as_ulid().to_bytes().as_slice()))
    }

    pub(crate) fn manifest_key(&self, id: ManifestId) -> Vec<u8> {
        self.root
            .pack(&("manifest", id.as_ulid().to_bytes().as_slice()))
    }

    pub(crate) fn image_key(&self, name: &str, tag: &ImageTag) -> Vec<u8> {
        self.root.pack(&("image", name, tag.0.as_str()))
    }

    pub(crate) fn volume_lease_seq_key(&self, id: VolumeId) -> Vec<u8> {
        self.root
            .pack(&("volume_lease_seq", id.as_ulid().to_bytes().as_slice()))
    }

    pub(crate) fn session_key(&self, id: SessionId) -> Vec<u8> {
        self.root
            .pack(&("session", id.as_ulid().to_bytes().as_slice()))
    }

    pub(crate) fn event_space(&self, id: SessionId) -> Subspace {
        self.root
            .subspace(&("event", id.as_ulid().to_bytes().as_slice()))
    }

    fn runnable_key(&self, entry: &RunnableEntry) -> Vec<u8> {
        self.root.pack(&(
            "runnable",
            runnable_partition(entry.session_id),
            entry.priority,
            (entry.wake_at.as_second(), entry.wake_at.subsec_nanosecond()),
            entry.session_id.as_ulid().to_bytes().as_slice(),
        ))
    }

    fn runnable_lookup(&self, id: SessionId) -> Vec<u8> {
        self.root
            .pack(&("runnable_by_session", id.as_ulid().to_bytes().as_slice()))
    }

    pub(crate) async fn remove_runnable(&self, trx: &Transaction, id: SessionId) -> Result<()> {
        let lookup = self.runnable_lookup(id);
        if let Some(entry) = read::<RunnableEntry>(trx, &lookup).await? {
            trx.clear(&self.runnable_key(&entry));
            trx.clear(&lookup);
        }
        Ok(())
    }

    pub(crate) async fn index_runnable(
        &self,
        trx: &Transaction,
        entry: &RunnableEntry,
    ) -> Result<()> {
        self.remove_runnable(trx, entry.session_id).await?;
        self.write_runnable(trx, entry)
    }

    pub(crate) fn write_runnable(&self, trx: &Transaction, entry: &RunnableEntry) -> Result<()> {
        write(trx, &self.runnable_key(entry), &())?;
        write(trx, &self.runnable_lookup(entry.session_id), entry)
    }

    /// Insert or reschedule a Runnable session, replacing its previous index entry.
    /// # Errors
    /// Rejects missing or non-Runnable sessions and transaction failures.
    pub async fn insert_runnable(&self, entry: &RunnableEntry) -> Result<()> {
        self.transaction(|trx| async move {
            if self.session(&trx, entry.session_id).await?.state != SessionState::Runnable {
                return Err(StoreError::InvalidState);
            }
            self.index_runnable(&trx, entry).await
        })
        .await
    }

    /// Scan one partition by priority, wake time, and id. `after` is exclusive.
    /// Wake times are returned for the scheduler to evaluate against its clock.
    /// # Errors
    /// Rejects invalid partitions, cursors, limits, and malformed stored keys.
    pub async fn scan_runnable(
        &self,
        partition: u16,
        after: Option<&RunnableEntry>,
        limit: usize,
    ) -> Result<Vec<RunnableEntry>> {
        if partition >= RUNNABLE_PARTITIONS
            || after.is_some_and(|entry| runnable_partition(entry.session_id) != partition)
        {
            return Err(StoreError::InvalidState);
        }
        self.transaction(|trx| async move {
            let space = self.root.subspace(&("runnable", partition));
            let (mut begin, end) = space.range();
            if let Some(entry) = after {
                begin = self.runnable_key(entry);
                begin.push(0);
            }
            let mut entries = Vec::new();
            for (key, _) in scan(&trx, (begin, end), limit).await? {
                let (priority, (seconds, nanos), id): (i64, (i64, i32), Vec<u8>) =
                    space.unpack(&key).map_err(|_| StoreError::Corrupt)?;
                entries.push(RunnableEntry {
                    session_id: session_id(id)?,
                    priority,
                    wake_at: Timestamp::new(seconds, nanos).map_err(|_| StoreError::Corrupt)?,
                });
            }
            Ok(entries)
        })
        .await
    }
}

pub(crate) fn session_id(bytes: Vec<u8>) -> Result<SessionId> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|_| StoreError::Corrupt)?;
    Ok(SessionId::from_ulid(u128::from_be_bytes(bytes).into()))
}

impl Store {
    pub(crate) fn agent_key(&self, id: swarmy_core::AgentId) -> Vec<u8> {
        self.root
            .pack(&("agent", id.as_ulid().to_bytes().as_slice()))
    }
    pub(crate) fn agent_name_key(&self, name: &str) -> Vec<u8> {
        self.root.pack(&("agent_by_name", name))
    }
    pub(crate) fn computer_deleted_key(&self, id: swarmy_core::AgentId) -> Vec<u8> {
        self.root
            .pack(&("computer_deleted", id.as_ulid().to_bytes().as_slice()))
    }
    pub(crate) fn session_kind_key(&self, id: SessionId) -> Vec<u8> {
        self.root
            .pack(&("session_kind", id.as_ulid().to_bytes().as_slice()))
    }
    pub(crate) fn session_idle_key(&self, id: SessionId) -> Vec<u8> {
        self.root
            .pack(&("session_idle", id.as_ulid().to_bytes().as_slice()))
    }
    pub(crate) fn session_agent_key(&self, agent: swarmy_core::AgentId, id: SessionId) -> Vec<u8> {
        self.root.pack(&(
            "session_by_agent",
            agent.as_ulid().to_bytes().as_slice(),
            id.as_ulid().to_bytes().as_slice(),
        ))
    }
}
