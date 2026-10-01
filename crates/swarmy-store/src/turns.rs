//! Atomic worker boundaries. Blob uploads happen before the fenced transaction.
use jiff::Timestamp;
use serde::Serialize;
use swarmy_core::{Event, InflightRecord, Lease, RequestId, SessionId, SessionState, SnapshotRef};

use crate::{InferenceWait, Result, Store, StoreError, read, write};

/// Route position to persist with the inference handoff. The submitter
/// resolved the route against open breakers before building the request, so
/// the picked step commits with the request event instead of in a separate
/// transaction. Skipped-step reasons join the wait history; the failure
/// sequence is untouched because no failure is handled here.
#[derive(Clone, Debug)]
pub struct SubmitRouteStep {
    pub step: u32,
    pub reasons: Vec<String>,
}

/// Commit extras for the inference handoff. Worker-generated conversation
/// events commit with the request so retries cannot repeat them; the gateway
/// request commits alongside them, and the picked route step lands in the
/// same transaction so a retryable failure advances from the attempt that
/// actually ran.
#[derive(Clone, Debug)]
pub struct SubmitInferenceOptions<'a, R = ()> {
    /// Gateway request payload; a failed transaction leaves only an uploaded
    /// blob, which the collector can reclaim.
    pub request: Option<&'a R>,
    /// Conversation events to commit before the request event.
    pub before: &'a [Event],
    /// Route position to persist with the handoff.
    pub route: Option<SubmitRouteStep>,
}

impl<R> Default for SubmitInferenceOptions<'_, R> {
    fn default() -> Self {
        Self {
            request: None,
            before: &[],
            route: None,
        }
    }
}

impl Store {
    /// Persist the input, request event and inflight outbox, then release the lease.
    /// A crash before publication is recovered from the durable inflight outbox.
    /// The handoff commits input, request, outbox, events, and route step
    /// atomically; splitting the parameters would separate that one write.
    /// # Errors
    /// Rejects a stale head, expired or replaced lease, and invalid request identity.
    pub async fn submit_inference<T: Serialize, R: Serialize>(
        &self,
        expected_head: u64,
        lease: &Lease,
        record: &InflightRecord,
        input: &T,
        options: Option<SubmitInferenceOptions<'_, R>>,
    ) -> Result<Event> {
        let options = options.unwrap_or_default();
        self.submit_inference_after_inner(
            expected_head,
            lease,
            record,
            input,
            options.request,
            options.before,
            options.route,
        )
        .await
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the handoff commits input, request, outbox, events, and route step atomically; splitting the parameters would separate that one write"
    )]
    async fn submit_inference_after_inner<T: Serialize, R: Serialize>(
        &self,
        expected_head: u64,
        lease: &Lease,
        record: &InflightRecord,
        input: &T,
        request: Option<&R>,
        before: &[Event],
        route: Option<SubmitRouteStep>,
    ) -> Result<Event> {
        let step = crate::seq_after(expected_head, before.len() + 1)?;
        if record.seq != step {
            return Err(StoreError::Domain(
                crate::DomainError::InvalidInferenceRequest,
            ));
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
        let request = match request {
            Some(request) => Some(self.prepare(request).await?),
            None => None,
        };
        let mut preceding = Vec::with_capacity(before.len());
        for (event, seq) in before.iter().zip(crate::seq_after(expected_head, 1)?..) {
            if !matches!(event, Event::MessageAppended { message, .. }
                if matches!(message.role, swarmy_core::MessageRole::Tool | swarmy_core::MessageRole::System))
            {
                return Err(StoreError::Domain(crate::DomainError::InvalidMessageRole));
            }
            let mut event = event.clone();
            event.set_seq(seq);
            preceding.push((seq, self.prepare(&event).await?));
        }
        self.transaction(|trx| {
            let (input, inflight, value, request, preceding) =
                (&input, &inflight, &value, &request, &preceding);
            let route = &route;
            async move {
                let now = self.now();
                let turn_key = self.keys().turn(id);
                let ((), mut session, turn) = futures::try_join!(
                    self.check_worker_lease(&trx, id, lease, now),
                    self.session(&trx, id),
                    read::<swarmy_core::MessageId>(&trx, &turn_key),
                )?;
                crate::check_head(session.head_seq, expected_head)?;
                if let Some(route) = route {
                    self.write_submit_route_step(&trx, id, &mut session, route, now)
                        .await?;
                }
                trx.set(&self.keys().inference_input(request_id), input);
                if let Some(request) = request {
                    trx.set(&self.keys().inference_request(request_id), request);
                }
                trx.set(&self.keys().inflight(request_id), inflight);
                for (seq, value) in preceding {
                    trx.set(&self.keys().event(id, *seq), value);
                }
                trx.set(&self.keys().event(id, step), value);
                if let Some(turn) = turn {
                    write(&trx, &self.keys().request_turn(request_id), &turn)?;
                }
                session.head_seq = step;
                self.transition(&trx, session, SessionState::WaitingInference, now)
                    .await
            }
        })
        .await?;
        Ok(event)
    }

    /// Persist the picked step with the request so a retryable failure
    /// advances from the attempt that actually ran, not from a stale
    /// position. No failure is handled here, so the handled-failure sequence
    /// is left untouched.
    /// # Errors
    /// Returns storage failures.
    async fn write_submit_route_step(
        &self,
        trx: &foundationdb::Transaction,
        id: SessionId,
        session: &mut crate::StoredSession,
        route: &SubmitRouteStep,
        now: Timestamp,
    ) -> Result<()> {
        let current = session.route_step;
        if route.step == current && route.reasons.is_empty() {
            return Ok(());
        }
        session.route_step = route.step;
        if route.reasons.is_empty() {
            return Ok(());
        }
        let wait_key = self.keys().inference_wait(id);
        let mut wait = read::<InferenceWait>(trx, &wait_key)
            .await?
            .unwrap_or(InferenceWait {
                since: now,
                wake_at: now,
                last_failure_seq: 0,
                reasons: Vec::new(),
                attempts: 0,
            });
        for reason in &route.reasons {
            let summary: String = reason.chars().take(256).collect();
            if !wait.reasons.contains(&summary) && wait.reasons.len() < 32 {
                wait.reasons.push(summary);
            }
        }
        write(trx, &wait_key, &wait)?;
        Ok(())
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
        let head = crate::seq_after(expected_head, 1)?;
        if snapshot.seq != head {
            return Err(StoreError::Domain(crate::DomainError::InvalidSnapshot));
        }
        let reference = self.prepare(snapshot).await?;
        self.transaction(|trx| {
            let reference = &reference;
            async move {
                let now = self.now();
                self.check_worker_lease(&trx, id, lease, now).await?;
                let mut session = self.session(&trx, id).await?;
                crate::check_head(session.head_seq, expected_head)?;
                if session.interrupt_requested
                    && !self
                        .last_event_is_operator_interrupt(&trx, id, expected_head)
                        .await?
                {
                    return Err(StoreError::Domain(crate::DomainError::InterruptPending));
                }
                // A queue write races the snapshot upload without changing
                // the log head. Conflict on the queue key here so the worker
                // can deliver the message before closing this turn.
                if !session.interrupt_requested && self.has_queued_in(&trx, id).await? {
                    return Err(StoreError::Domain(crate::DomainError::QueuedInputPending));
                }
                let state = if self.has_queued_in(&trx, id).await? {
                    SessionState::Runnable
                } else {
                    SessionState::Idle
                };
                let event = Event::StateChanged {
                    seq: head,
                    from: SessionState::Leased,
                    to: state,
                };
                trx.set(&self.keys().event(id, head), &self.prepare(&event).await?);
                trx.set(&self.keys().snapshot(id, head), reference);
                session.head_seq = head;
                session.snapshot_seq = Some(head);
                session.interrupt_requested = false;
                // Turn end restarts the route chain with the next turn.
                session.route_step = 0;
                self.transition(&trx, session, state, now).await?;
                Ok(event)
            }
        })
        .await
    }
}
