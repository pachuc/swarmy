//! Runnable index: turn ids and the scheduler partition scan.
use foundationdb::Transaction;
use jiff::Timestamp;
use swarmy_core::{RunnableEntry, SessionId};

#[cfg(any(test, feature = "test-support"))]
use swarmy_core::SessionState;

use crate::{Result, Store, StoreError, read, scan, write};

pub use swarmy_core::{RUNNABLE_PARTITIONS, runnable_partition};

impl Store {
    /// Resolve the original user turn even when old work is redelivered later.
    /// # Errors
    /// Returns database and decoding errors.
    pub async fn request_turn_id(
        &self,
        id: swarmy_core::RequestId,
    ) -> Result<Option<swarmy_core::MessageId>> {
        self.transaction(|trx| async move { read(&trx, &self.keys().request_turn(id)).await })
            .await
    }

    /// The latest user message identifies the turn, including after snapshots.
    /// # Errors
    /// Returns database and decoding errors.
    pub async fn turn_id(&self, id: SessionId) -> Result<Option<swarmy_core::MessageId>> {
        self.transaction(|trx| async move { read(&trx, &self.keys().turn(id)).await })
            .await
    }

    /// The last durable state transition, if it happened after this field was introduced.
    /// # Errors
    /// Returns database or decoding errors.
    pub async fn session_state_since(&self, id: SessionId) -> Result<Option<Timestamp>> {
        self.transaction(|trx| async move { Ok(self.session(&trx, id).await?.state_since) })
            .await
    }

    fn runnable_key(&self, entry: &RunnableEntry) -> Vec<u8> {
        self.keys().runnable(
            runnable_partition(entry.session_id),
            entry.priority,
            entry.wake_at,
            entry.session_id,
        )
    }

    pub(crate) async fn remove_runnable(&self, trx: &Transaction, id: SessionId) -> Result<()> {
        let lookup = self.keys().runnable_by_session(id);
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
        write(
            trx,
            &self.keys().runnable_by_session(entry.session_id),
            entry,
        )
    }

    /// Insert or reschedule a Runnable session, replacing its previous index entry.
    /// # Errors
    /// Rejects missing or non-Runnable sessions and transaction failures.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn insert_runnable(&self, entry: &RunnableEntry) -> Result<()> {
        self.transaction(|trx| async move {
            if self.session(&trx, entry.session_id).await?.state != SessionState::Runnable {
                return Err(StoreError::Domain(
                    crate::DomainError::UnexpectedSessionState,
                ));
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
            return Err(StoreError::Domain(crate::DomainError::InvalidPartition));
        }
        self.transaction(|trx| async move {
            let space = self.keys().runnable_space(partition);
            let (mut begin, end) = space.range();
            if let Some(entry) = after {
                begin = self.runnable_key(entry);
                begin = crate::next_cursor(&begin);
            }
            let mut entries = Vec::new();
            for (key, _) in scan(&trx, (begin, end), limit).await? {
                let (priority, (seconds, nanos), id): (i64, (i64, i32), Vec<u8>) = space
                    .unpack(&key)
                    .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?;
                entries.push(RunnableEntry {
                    session_id: crate::keys::session_id(id)?,
                    priority,
                    wake_at: Timestamp::new(seconds, nanos)
                        .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?,
                });
            }
            Ok(entries)
        })
        .await
    }
}
