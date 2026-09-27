//! Durable tool handoffs on persistent agent computers.
use crate::{Result, Store, StoreError, read, scan, write};
use swarmy_core::{Event, Lease, RequestId, SessionId, SessionState, ToolJob};

type PreparedToolRequests = [(Event, Vec<u8>)];

impl Store {
    pub(crate) fn tool_key(&self, kind: &str, id: RequestId) -> Vec<u8> {
        {
            let keys = crate::keys::Keys::new(&self.root);
            let suffix = &(id.as_bytes().as_slice(),);
            match kind {
                "tool_job" => keys.tool_job(suffix),
                "tool_placement" => keys.tool_placement(suffix),
                "placed_tool_claim" => keys.placed_tool_claim(suffix),
                "tool_done" => keys.tool_done(suffix),
                _ => unreachable!("unknown tool key family"),
            }
        }
    }
    pub(crate) fn pending_space(&self, id: SessionId) -> foundationdb::tuple::Subspace {
        crate::keys::Keys::new(&self.root)
            .session_tools_space(&(id.as_ulid().to_bytes().as_slice()))
    }

    /// Persist dispatch epochs with the jobs so a lost publication cannot lose its fence.
    /// # Errors
    /// Rejects stale workers, placements, unrelated jobs, and storage failures.
    pub async fn dispatch_placed_tool_jobs(
        &self,
        id: SessionId,
        lease: &Lease,
        jobs: &[ToolJob],
        placement: &swarmy_core::PlacementRecord,
    ) -> Result<()> {
        self.dispatch_jobs(id, lease, jobs, placement, None).await
    }

    /// Append sandbox call requests and persist their fenced handoff together.
    /// # Errors
    /// Rejects invalid calls, stale heads, worker leases, or placement epochs.
    pub async fn dispatch_tool_calls(
        &self,
        id: SessionId,
        expected_head: u64,
        lease: &Lease,
        calls: &[swarmy_core::ToolCallRecord],
        placement: &swarmy_core::PlacementRecord,
    ) -> Result<(Vec<Event>, Vec<ToolJob>)> {
        let mut events = Vec::with_capacity(calls.len());
        let mut jobs = Vec::with_capacity(calls.len());
        let mut size = 0;
        for (index, call) in calls.iter().enumerate() {
            let step = expected_head
                .checked_add(u64::try_from(index).map_err(|_| StoreError::SequenceOverflow)?)
                .and_then(|head| head.checked_add(1))
                .ok_or(StoreError::SequenceOverflow)?;
            let request_id = RequestId::for_step(id, step);
            let event = Event::ToolCallRequested {
                seq: step,
                request_id,
                call: call.clone(),
            };
            let value = self.prepare(&event).await?;
            size += value.len();
            if size > crate::MAX_BATCH_BYTES {
                return Err(StoreError::TooLarge);
            }
            events.push((event, value));
            jobs.push(ToolJob {
                session_id: id,
                request_id,
                step,
                call_id: call.call_id.clone(),
                arguments: swarmy_core::SandboxArguments::parse(&call.tool, call.arguments.clone())
                    .map_err(|_| StoreError::InvalidState)?,
            });
        }
        self.dispatch_jobs(id, lease, &jobs, placement, Some((expected_head, &events)))
            .await?;
        Ok((events.into_iter().map(|(event, _)| event).collect(), jobs))
    }

    async fn dispatch_jobs(
        &self,
        id: SessionId,
        lease: &Lease,
        jobs: &[ToolJob],
        placement: &swarmy_core::PlacementRecord,
        append: Option<(u64, &PreparedToolRequests)>,
    ) -> Result<()> {
        let mut values = Vec::new();
        for job in jobs {
            values.push(self.prepare(job).await?);
        }
        self.transaction(|trx| {
            let values = &values;
            async move {
                futures::try_join!(
                    self.check_worker_lease(&trx, id, lease, self.now()),
                    self.check_live_placement(&trx, placement),
                )?;
                if jobs.is_empty() {
                    return Err(StoreError::InvalidState);
                }
                if let Some((expected_head, events)) = append {
                    self.append_tool_requests(&trx, id, expected_head, events)
                        .await?;
                }
                if self.session(&trx, id).await?.agent_id != placement.agent_id {
                    return Err(StoreError::InvalidState);
                }
                self.deliver_computer_notice(&trx, id, placement).await?;
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
                    trx.set(
                        &crate::keys::Keys::new(&self.root)
                            .tool_job(&(job.request_id.as_bytes().as_slice(),)),
                        value,
                    );
                    write(
                        &trx,
                        &crate::keys::Keys::new(&self.root)
                            .tool_placement(&(job.request_id.as_bytes().as_slice(),)),
                        placement,
                    )?;
                    write(
                        &trx,
                        &self
                            .pending_space(id)
                            .pack(&(job.request_id.as_bytes().as_slice(),)),
                        &(),
                    )?;
                }
                let session = self.session(&trx, id).await?;
                self.transition(&trx, session, SessionState::WaitingTools, self.now())
                    .await
            }
        })
        .await
    }

    async fn append_tool_requests(
        &self,
        trx: &foundationdb::Transaction,
        id: SessionId,
        expected_head: u64,
        events: &PreparedToolRequests,
    ) -> Result<()> {
        let mut session = self.session(trx, id).await?;
        if session.head_seq != expected_head {
            return Err(StoreError::StaleSequence {
                expected: expected_head,
                actual: session.head_seq,
            });
        }
        let turn = read::<swarmy_core::MessageId>(trx, &self.turn_key(id)).await?;
        for (event, value) in events {
            let Event::ToolCallRequested {
                seq, request_id, ..
            } = event
            else {
                return Err(StoreError::InvalidState);
            };
            trx.set(&self.event_space(id).pack(&(*seq,)), value);
            if let Some(turn) = turn {
                write(trx, &self.request_turn_key(*request_id), &turn)?;
            }
            session.head_seq = *seq;
        }
        self.write_session(trx, &session)
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
                let (mut begin, end) = crate::keys::Keys::new(&self.root)
                    .tool_job_space(&())
                    .range();
                if let Some(id) = after {
                    begin =
                        crate::keys::Keys::new(&self.root).tool_job(&(id.as_bytes().as_slice(),));
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

    /// # Errors
    /// Returns storage or decoding failures.
    pub async fn tool_completed(&self, id: RequestId) -> Result<bool> {
        self.transaction(|trx| async move {
            Ok(read::<bool>(
                &trx,
                &crate::keys::Keys::new(&self.root).tool_done(&(id.as_bytes().as_slice(),)),
            )
            .await?
                == Some(true))
        })
        .await
    }
}
