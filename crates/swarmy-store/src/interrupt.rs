//! Operator interruption is fenced by the session state and log head.
use foundationdb::Transaction;
use jiff::Timestamp;
use swarmy_core::{Event, RequestId, SessionId, SessionState, decode, encode};

use crate::{Result, Store, StoreError, StoredSession, StoredValue, read, write};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InterruptResult {
    Finished,
    Requested,
}

impl Store {
    pub(crate) async fn last_event_is_operator_interrupt(
        &self,
        trx: &Transaction,
        id: SessionId,
        seq: u64,
    ) -> Result<bool> {
        let Some(bytes) = trx.get(&self.event_space(id).pack(&(seq,)), false).await? else {
            return Ok(false);
        };
        let event: Event = self.hydrate(&bytes).await?;
        Ok(
            matches!(event, Event::InferenceFailed { retryable: false, error, .. }
            if error == "interrupted by operator"),
        )
    }

    pub(crate) fn interrupt_key(&self, id: SessionId) -> Vec<u8> {
        self.root
            .pack(&("interrupt_requested", id.as_ulid().to_bytes().as_slice()))
    }

    /// Request the current turn to end, or finish a parked inference atomically.
    /// # Errors
    /// Returns an error when the session has no active turn or storage fails.
    pub async fn interrupt_session(&self, id: SessionId) -> Result<InterruptResult> {
        self.transaction(|trx| async move {
            let session = self.session(&trx, id).await?;
            match session.state {
                SessionState::Idle | SessionState::Completed => Err(StoreError::NothingToInterrupt),
                SessionState::Sleeping => {
                    let wait = read::<crate::InferenceWait>(&trx, &self.wait_key(id))
                        .await?
                        .ok_or(StoreError::InvalidState)?;
                    let request_id = if wait.last_failure_seq == 0 {
                        RequestId::for_step(
                            id,
                            session
                                .head_seq
                                .checked_add(1)
                                .ok_or(StoreError::SequenceOverflow)?,
                        )
                    } else {
                        self.request_from_event(&trx, id, wait.last_failure_seq)
                            .await?
                            .ok_or(StoreError::Corrupt)?
                    };
                    self.append_interrupted(&trx, session, request_id, Timestamp::now())
                        .await?;
                    trx.clear(&self.wait_due_key(id, wait.wake_at));
                    trx.clear(&self.wait_key(id));
                    Ok(InterruptResult::Finished)
                }
                _ => {
                    write(&trx, &self.interrupt_key(id), &true)?;
                    Ok(InterruptResult::Requested)
                }
            }
        })
        .await
    }

    /// Read the marker without fetching the rest of the session.
    /// # Errors
    /// Returns storage failures.
    pub async fn interrupt_requested(&self, id: SessionId) -> Result<bool> {
        self.transaction(|trx| async move {
            Ok(read(&trx, &self.interrupt_key(id)).await?.unwrap_or(false))
        })
        .await
    }

    /// Finish a marked runnable session without handing it to a worker.
    /// # Errors
    /// Returns storage failures.
    pub async fn finish_runnable_interrupt(&self, id: SessionId) -> Result<bool> {
        self.transaction(|trx| async move {
            let session = self.session(&trx, id).await?;
            if session.state != SessionState::Runnable
                || !read::<bool>(&trx, &self.interrupt_key(id))
                    .await?
                    .unwrap_or(false)
            {
                return Ok(false);
            }
            let request_id = if session.head_seq == 0 {
                RequestId::for_step(id, 1)
            } else {
                self.request_from_event(&trx, id, session.head_seq)
                    .await?
                    .unwrap_or(RequestId::for_step(
                        id,
                        session
                            .head_seq
                            .checked_add(1)
                            .ok_or(StoreError::SequenceOverflow)?,
                    ))
            };
            self.append_interrupted(&trx, session, request_id, Timestamp::now())
                .await?;
            if let Some(wait) = read::<crate::InferenceWait>(&trx, &self.wait_key(id)).await? {
                trx.clear(&self.wait_due_key(id, wait.wake_at));
                trx.clear(&self.wait_key(id));
            }
            trx.clear(&self.interrupt_key(id));
            Ok(true)
        })
        .await
    }

    async fn request_from_event(
        &self,
        trx: &Transaction,
        id: SessionId,
        seq: u64,
    ) -> Result<Option<RequestId>> {
        let Some(bytes) = trx.get(&self.event_space(id).pack(&(seq,)), false).await? else {
            return Ok(None);
        };
        let event: Event = match decode::<StoredValue>(&bytes)? {
            StoredValue::Inline(bytes) => decode(&bytes)?,
            StoredValue::Blob(key) => self.hydrate(&encode(&StoredValue::Blob(key))?).await?,
        };
        Ok(match event {
            Event::InferenceRequested { request_id, .. }
            | Event::InferenceFailed { request_id, .. }
            | Event::InferenceCompleted { request_id, .. } => Some(request_id),
            _ => None,
        })
    }

    async fn append_interrupted(
        &self,
        trx: &Transaction,
        mut session: StoredSession,
        request_id: RequestId,
        now: Timestamp,
    ) -> Result<()> {
        let head = session
            .head_seq
            .checked_add(1)
            .ok_or(StoreError::SequenceOverflow)?;
        let event = Event::InferenceFailed {
            seq: head,
            request_id,
            error: "interrupted by operator".into(),
            retryable: false,
            retry_at: None,
        };
        let value = encode(&StoredValue::Inline(encode(&event)?))?;
        trx.set(&self.event_space(session.session_id).pack(&(head,)), &value);
        session.head_seq = head;
        self.transition(trx, session, SessionState::Idle, now).await
    }
}
