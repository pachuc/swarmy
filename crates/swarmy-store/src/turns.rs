//! Atomic worker boundaries. Blob uploads happen before the fenced transaction.
use jiff::Timestamp;
use serde::Serialize;
use swarmy_core::{Event, InflightRecord, Lease, RequestId, SessionId, SessionState, SnapshotRef};

use crate::{Result, Store, StoreError, read, write};

impl Store {
    /// Persist the input, request event and inflight outbox, then release the lease.
    /// A crash before publication is recovered from the durable inflight outbox.
    /// # Errors
    /// Rejects a stale head, expired or replaced lease, and invalid request identity.
    pub async fn submit_inference<T: Serialize>(
        &self,
        expected_head: u64,
        lease: &Lease,
        record: &InflightRecord,
        input: &T,
    ) -> Result<Event> {
        self.submit_inference_after(expected_head, lease, record, input, &[])
            .await
    }

    /// Include worker-generated conversation events in the inference handoff.
    /// Tool results and system notices commit with the request so retries cannot repeat them.
    /// # Errors
    /// Rejects stale heads, expired or replaced leases, and invalid request identity.
    pub async fn submit_inference_after<T: Serialize>(
        &self,
        expected_head: u64,
        lease: &Lease,
        record: &InflightRecord,
        input: &T,
        before: &[Event],
    ) -> Result<Event> {
        let step = expected_head
            .checked_add(u64::try_from(before.len()).map_err(|_| StoreError::SequenceOverflow)?)
            .and_then(|head| head.checked_add(1))
            .ok_or(StoreError::SequenceOverflow)?;
        if record.seq != step {
            return Err(StoreError::InvalidState);
        }
        let id = record.session_id;
        let request_id = RequestId::for_step(id, step);
        let event = Event::InferenceRequested {
            seq: step,
            request_id,
            step,
        };
        let (input, inflight, value) = (
            self.prepare(input).await?,
            self.prepare(record).await?,
            self.prepare(&event).await?,
        );
        let mut preceding = Vec::with_capacity(before.len());
        for (event, seq) in before.iter().zip(expected_head + 1..) {
            if !matches!(event, Event::MessageAppended { message, .. }
                if matches!(message.role, swarmy_core::MessageRole::Tool | swarmy_core::MessageRole::System))
            {
                return Err(StoreError::InvalidState);
            }
            let mut event = event.clone();
            event.set_seq(seq);
            preceding.push((seq, self.prepare(&event).await?));
        }
        self.transaction(|trx| {
            let (input, inflight, value, preceding) = (&input, &inflight, &value, &preceding);
            async move {
                let now = Timestamp::now();
                let turn_key = self.turn_key(id);
                let ((), mut session, turn) = futures::try_join!(
                    self.check_worker_lease(&trx, id, lease, now),
                    self.session(&trx, id),
                    read::<swarmy_core::MessageId>(&trx, &turn_key),
                )?;
                if session.head_seq != expected_head {
                    return Err(StoreError::StaleSequence {
                        expected: expected_head,
                        actual: session.head_seq,
                    });
                }
                trx.set(
                    &self
                        .root
                        .pack(&("inference_input", request_id.as_bytes().as_slice())),
                    input,
                );
                trx.set(
                    &self
                        .root
                        .pack(&("inflight", request_id.as_bytes().as_slice())),
                    inflight,
                );
                for (seq, value) in preceding {
                    trx.set(&self.event_space(id).pack(&(*seq,)), value);
                }
                trx.set(&self.event_space(id).pack(&(step,)), value);
                if let Some(turn) = turn {
                    write(&trx, &self.request_turn_key(request_id), &turn)?;
                }
                session.head_seq = step;
                self.transition(&trx, session, SessionState::WaitingInference, now)
                    .await
            }
        })
        .await?;
        Ok(event)
    }

    /// Commit the final log event, snapshot pointer, and idle state together.
    /// The snapshot must include the new idle event at `expected_head + 1`.
    /// # Errors
    /// Rejects stale heads, expired or replaced leases, and inconsistent snapshots.
    pub async fn finish_turn(
        &self,
        id: SessionId,
        expected_head: u64,
        lease: &Lease,
        snapshot: &SnapshotRef,
    ) -> Result<Event> {
        let head = expected_head
            .checked_add(1)
            .ok_or(StoreError::SequenceOverflow)?;
        if snapshot.seq != head {
            return Err(StoreError::InvalidState);
        }
        let event = Event::StateChanged {
            seq: head,
            from: SessionState::Leased,
            to: SessionState::Idle,
        };
        let value = self.prepare(&event).await?;
        let reference = self.prepare(snapshot).await?;
        self.transaction(|trx| {
            let (value, reference) = (&value, &reference);
            async move {
                let now = Timestamp::now();
                self.check_worker_lease(&trx, id, lease, now).await?;
                let mut session = self.session(&trx, id).await?;
                if session.head_seq != expected_head {
                    return Err(StoreError::StaleSequence {
                        expected: expected_head,
                        actual: session.head_seq,
                    });
                }
                if read::<bool>(&trx, &self.interrupt_key(id))
                    .await?
                    .unwrap_or(false)
                    && !self
                        .last_event_is_operator_interrupt(&trx, id, expected_head)
                        .await?
                {
                    return Err(StoreError::InterruptPending);
                }
                trx.set(&self.event_space(id).pack(&(head,)), value);
                trx.set(&self.snapshot_key(id, head), reference);
                session.head_seq = head;
                session.snapshot_seq = Some(head);
                trx.clear(&self.interrupt_key(id));
                self.transition(&trx, session, SessionState::Idle, now)
                    .await
            }
        })
        .await?;
        Ok(event)
    }
}
