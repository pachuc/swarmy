//! Operator interruption is fenced by the session state and log head.
use foundationdb::Transaction;
use jiff::Timestamp;
use swarmy_core::{Event, RequestId, SessionId, SessionState, decode, encode};

use crate::{Result, Store, StoreError, StoredSession, StoredValue, read};

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
        let Some(bytes) = trx.get(&self.keys().event(id, seq), false).await? else {
            return Ok(false);
        };
        let event: Event = self.hydrate(&bytes).await?;
        Ok(matches!(
            event,
            Event::InferenceFailed {
                retryable: false,
                failure_kind: swarmy_core::FailureKind::OperatorInterrupted,
                ..
            }
        ))
    }

    /// Request the current turn to end, or finish a parked inference (asleep,
    /// or between gateway retries) atomically.
    /// # Errors
    /// Returns an error when the session has no active turn or storage fails.
    pub async fn interrupt_session(&self, id: SessionId) -> Result<InterruptResult> {
        self.transaction(|trx| async move {
            let session = self.session(&trx, id).await?;
            match session.state {
                SessionState::Idle | SessionState::Completed => {
                    Err(StoreError::Domain(crate::DomainError::NothingToInterrupt))
                }
                SessionState::Sleeping => {
                    let wait = read::<crate::InferenceWait>(&trx, &self.keys().inference_wait(id))
                        .await?
                        .ok_or(StoreError::Domain(crate::DomainError::MissingInferenceWait))?;
                    let request_id = if wait.last_failure_seq == 0 {
                        RequestId::for_step(id, crate::seq_after(session.head_seq, 1)?)
                    } else {
                        self.request_from_event(&trx, id, wait.last_failure_seq)
                            .await?
                            .ok_or(StoreError::Storage(crate::StorageError::Corrupt))?
                    };
                    self.append_interrupted(&trx, session, request_id, self.now())
                        .await?;
                    self.clear_wait(&trx, id, &wait);
                    Ok(InterruptResult::Finished)
                }
                SessionState::WaitingInference => {
                    self.interrupt_parked_inference(&trx, session).await
                }
                _ => self.mark_interrupt(&trx, session),
            }
        })
        .await
    }

    /// Read the marker without fetching the rest of the session.
    /// # Errors
    /// Returns storage failures.
    pub async fn interrupt_requested(&self, id: SessionId) -> Result<bool> {
        self.transaction(|trx| async move { Ok(self.session(&trx, id).await?.interrupt_requested) })
            .await
    }

    /// Finish a marked runnable session without handing it to a worker.
    /// # Errors
    /// Returns storage failures.
    pub async fn finish_runnable_interrupt(&self, id: SessionId) -> Result<bool> {
        self.transaction(|trx| async move {
            let session = self.session(&trx, id).await?;
            if session.state != SessionState::Runnable || !session.interrupt_requested {
                return Ok(false);
            }
            let request_id = if session.head_seq == 0 {
                RequestId::for_step(id, 1)
            } else {
                self.request_from_event(&trx, id, session.head_seq)
                    .await?
                    .unwrap_or(RequestId::for_step(
                        id,
                        crate::seq_after(session.head_seq, 1)?,
                    ))
            };
            self.append_interrupted(&trx, session, request_id, self.now())
                .await?;
            self.clear_wait_in(&trx, id).await?;
            Ok(true)
        })
        .await
    }

    /// Finish a request parked between gateway retries, not one held by a live gateway.
    async fn interrupt_parked_inference(
        &self,
        trx: &Transaction,
        session: StoredSession,
    ) -> Result<InterruptResult> {
        let now = self.now();
        let request_id = RequestId::for_step(session.session_id, session.head_seq);
        if self
            .inference_parked_for_retry(trx, request_id, now)
            .await?
        {
            self.clear_inference_rows(trx, request_id);
            self.clear_wait_in(trx, session.session_id).await?;
            self.append_interrupted(trx, session, request_id, now)
                .await?;
            Ok(InterruptResult::Finished)
        } else {
            self.mark_interrupt(trx, session)
        }
    }

    fn mark_interrupt(
        &self,
        trx: &Transaction,
        mut session: StoredSession,
    ) -> Result<InterruptResult> {
        session.interrupt_requested = true;
        self.write_session(trx, &session)?;
        Ok(InterruptResult::Requested)
    }

    async fn clear_wait_in(&self, trx: &Transaction, id: SessionId) -> Result<()> {
        if let Some(wait) =
            read::<crate::InferenceWait>(trx, &self.keys().inference_wait(id)).await?
        {
            self.clear_wait(trx, id, &wait);
        }
        Ok(())
    }

    async fn request_from_event(
        &self,
        trx: &Transaction,
        id: SessionId,
        seq: u64,
    ) -> Result<Option<RequestId>> {
        let Some(bytes) = trx.get(&self.keys().event(id, seq), false).await? else {
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
        let head = crate::seq_after(session.head_seq, 1)?;
        let event = swarmy_core::interrupted_event(head, request_id);
        let value = encode(&StoredValue::Inline(encode(&event)?))?;
        trx.set(&self.keys().event(session.session_id, head), &value);
        session.head_seq = head;
        session.interrupt_requested = false;
        let state = if self.has_queued_in(trx, session.session_id).await? {
            SessionState::Runnable
        } else {
            SessionState::Idle
        };
        self.transition(trx, session, state, now).await
    }
}
