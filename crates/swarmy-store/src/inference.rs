use jiff::Timestamp;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use swarmy_core::{
    Event, IdempotencyRecord, IdempotencyState, InflightRecord, LeaseOwnerId, RequestId, SessionId,
    SessionState,
};

use crate::{Result, Store, StoreError, read, write};

/// A fresh owner per delivery fences concurrent duplicates and expired streams.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InferenceClaim {
    pub session_id: SessionId,
    pub request_id: RequestId,
    pub owner: LeaseOwnerId,
    pub expires_at: Timestamp,
}

/// Inputs to the atomic terminal update. Both success and exhausted retries wake
/// the session; the next worker reads the corresponding event.
pub struct InferenceCompletion {
    pub claim: InferenceClaim,
    pub expected_head: u64,
    pub event: Event,
    pub now: Timestamp,
}

impl Store {
    fn inference_key(&self, kind: &str, id: RequestId) -> Vec<u8> {
        self.root.pack(&(kind, id.as_bytes().as_slice()))
    }

    /// Claim a request or renew the same owner's claim and record it as started.
    /// Returns false for completed requests or a live claim held by another owner.
    /// `Requested` is the durable started state; it remains retryable after expiry.
    /// # Errors
    /// Rejects invalid expiry, non-waiting sessions, mismatched inflight work,
    /// and storage failures.
    pub async fn start_inference(&self, claim: &InferenceClaim, now: Timestamp) -> Result<bool> {
        if claim.expires_at <= now {
            return Err(StoreError::LeaseMismatch);
        }
        let requested = self
            .prepare(&IdempotencyRecord {
                state: IdempotencyState::Requested,
                result_ref: None,
            })
            .await?;
        self.transaction(|trx| {
            let requested = &requested;
            async move {
                let idem_key = self.inference_key("idem", claim.request_id);
                if let Some(value) = trx.get(&idem_key, false).await? {
                    let record: IdempotencyRecord = self.hydrate(&value).await?;
                    if record.state == IdempotencyState::Completed {
                        return Ok(false);
                    }
                }
                let key = self.inference_key("inference_claim", claim.request_id);
                if let Some(old) = read::<InferenceClaim>(&trx, &key).await?
                    && old.expires_at > now
                    && old.owner != claim.owner
                {
                    return Ok(false);
                }
                if self.session(&trx, claim.session_id).await?.state
                    != SessionState::WaitingInference
                {
                    return Err(StoreError::InvalidState);
                }
                let inflight = trx
                    .get(&self.inference_key("inflight", claim.request_id), false)
                    .await?
                    .ok_or(StoreError::InvalidState)?;
                let inflight: InflightRecord = self.hydrate(&inflight).await?;
                if inflight.session_id != claim.session_id {
                    return Err(StoreError::InvalidState);
                }
                write(&trx, &key, claim)?;
                trx.set(&idem_key, requested);
                Ok(true)
            }
        })
        .await
    }

    /// Release only this delivery's claim after a provider error, allowing backoff.
    /// # Errors
    /// Returns storage failures.
    pub async fn release_inference(&self, claim: &InferenceClaim) -> Result<()> {
        self.transaction(|trx| async move {
            let key = self.inference_key("inference_claim", claim.request_id);
            if read::<InferenceClaim>(&trx, &key)
                .await?
                .is_some_and(|old| old.owner == claim.owner)
            {
                trx.clear(&key);
            }
            Ok(())
        })
        .await
    }

    /// Store the full result, append its event, clear inflight and the claim, mark
    /// idempotency complete, and index Runnable in one transaction. Large result
    /// and event values use the blob path before the transaction starts.
    /// Repeating a committed completion is harmless, even with a stale head.
    /// # Errors
    /// Rejects stale heads, expired/replaced claims, unrelated events, invalid
    /// states, and storage failures. Unknown commits can be resolved by idempotency.
    pub async fn complete_inference<T: Serialize>(
        &self,
        completion: &InferenceCompletion,
        response: &T,
    ) -> Result<()> {
        let claim = &completion.claim;
        let head = completion
            .expected_head
            .checked_add(1)
            .ok_or(StoreError::SequenceOverflow)?;
        let mut event = completion.event.clone();
        match &mut event {
            Event::InferenceCompleted {
                seq, request_id, ..
            }
            | Event::InferenceFailed {
                seq, request_id, ..
            } if *request_id == claim.request_id => *seq = head,
            _ => return Err(StoreError::InvalidState),
        }
        let event = self.prepare(&event).await?;
        let response = self.prepare(response).await?;
        let completed = self
            .prepare(&IdempotencyRecord {
                state: IdempotencyState::Completed,
                result_ref: Some(format!("inference_result/{}", claim.request_id)),
            })
            .await?;
        self.transaction(|trx| {
            let (event, response, completed) = (&event, &response, &completed);
            async move {
                let idem_key = self.inference_key("idem", claim.request_id);
                if let Some(value) = trx.get(&idem_key, false).await? {
                    let record: IdempotencyRecord = self.hydrate(&value).await?;
                    if record.state == IdempotencyState::Completed {
                        return Ok(());
                    }
                }
                let claim_key = self.inference_key("inference_claim", claim.request_id);
                let current = read::<InferenceClaim>(&trx, &claim_key)
                    .await?
                    .ok_or(StoreError::LeaseMismatch)?;
                if current.owner != claim.owner
                    || current.session_id != claim.session_id
                    || current.expires_at <= completion.now
                {
                    return Err(StoreError::LeaseMismatch);
                }
                let mut session = self.session(&trx, claim.session_id).await?;
                if session.head_seq != completion.expected_head {
                    return Err(StoreError::StaleSequence {
                        expected: completion.expected_head,
                        actual: session.head_seq,
                    });
                }
                if session.state != SessionState::WaitingInference {
                    return Err(StoreError::InvalidState);
                }
                trx.set(&self.event_space(claim.session_id).pack(&(head,)), event);
                trx.set(
                    &self.inference_key("inference_result", claim.request_id),
                    response,
                );
                trx.set(&idem_key, completed);
                trx.clear(&self.inference_key("inflight", claim.request_id));
                trx.clear(&claim_key);
                session.head_seq = head;
                self.transition(&trx, session, SessionState::Runnable, completion.now)
                    .await
            }
        })
        .await
    }

    /// Read the full terminal result, including provider usage and stop reason.
    /// The caller chooses the same type supplied to `complete_inference`.
    /// # Errors
    /// Returns storage, blob, or decoding failures.
    pub async fn get_inference_result<T: DeserializeOwned>(
        &self,
        id: RequestId,
    ) -> Result<Option<T>> {
        self.get_payload(self.inference_key("inference_result", id))
            .await
    }
}

impl Store {
    /// Save the exact inference input before its request event is appended.
    /// A replacement worker can resume submission without rebuilding the prompt.
    /// # Errors
    /// Rejects stale leases, heads, and storage failures.
    pub async fn put_inference_input<T: Serialize>(
        &self,
        session_id: SessionId,
        expected_head: u64,
        lease: &swarmy_core::Lease,
        now: Timestamp,
        input: &T,
    ) -> Result<()> {
        let step = expected_head
            .checked_add(1)
            .ok_or(StoreError::SequenceOverflow)?;
        let request_id = RequestId::for_step(session_id, step);
        let value = self.prepare(input).await?;
        self.transaction(|trx| {
            let value = &value;
            async move {
                self.check_worker_lease(&trx, session_id, lease, now)
                    .await?;
                let session = self.session(&trx, session_id).await?;
                if session.head_seq != expected_head {
                    return Err(StoreError::StaleSequence {
                        expected: expected_head,
                        actual: session.head_seq,
                    });
                }
                trx.set(&self.inference_key("inference_input", request_id), value);
                Ok(())
            }
        })
        .await
    }

    /// Read the input saved before a request event. The caller supplies its type.
    /// # Errors
    /// Returns storage, blob, or decoding errors.
    pub async fn get_inference_input<T: DeserializeOwned>(
        &self,
        id: RequestId,
    ) -> Result<Option<T>> {
        self.get_payload(self.inference_key("inference_input", id))
            .await
    }

    /// Scan in-flight requests in request-id order, strictly after the cursor.
    /// The cursor is derived from the last record's session id and sequence.
    /// # Errors
    /// Rejects invalid limits, inconsistent request keys, and storage failures.
    pub async fn scan_inflight(
        &self,
        after: Option<RequestId>,
        limit: usize,
    ) -> Result<Vec<InflightRecord>> {
        crate::check_limit(limit)?;
        let values = self
            .transaction(|trx| async move {
                let space = self.root.subspace(&("inflight",));
                let mut begin = space.range().0;
                if let Some(id) = after {
                    begin = self.inference_key("inflight", id);
                    begin.push(0);
                }
                crate::scan(&trx, (begin, space.range().1), limit).await
            })
            .await?;
        let mut records = Vec::with_capacity(values.len());
        for (key, value) in values {
            let record: InflightRecord = self.hydrate(&value).await?;
            if key
                != self.inference_key(
                    "inflight",
                    RequestId::for_step(record.session_id, record.seq),
                )
            {
                return Err(StoreError::Corrupt);
            }
            records.push(record);
        }
        Ok(records)
    }
}

impl Store {
    /// Record submission progress only while the worker still owns the session.
    /// # Errors
    /// Rejects mismatched request ids, stale leases, and storage failures.
    pub async fn put_inflight_leased(
        &self,
        id: RequestId,
        record: &InflightRecord,
        lease: &swarmy_core::Lease,
        now: Timestamp,
    ) -> Result<()> {
        if RequestId::for_step(record.session_id, record.seq) != id {
            return Err(StoreError::InvalidState);
        }
        let value = self.prepare(record).await?;
        self.transaction(|trx| {
            let value = &value;
            async move {
                self.check_worker_lease(&trx, record.session_id, lease, now)
                    .await?;
                trx.set(&self.inference_key("inflight", id), value);
                Ok(())
            }
        })
        .await
    }
}
