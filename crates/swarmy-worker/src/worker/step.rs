use super::inference::warn_on_route_fallback;
use super::{
    Action, Arc, Bus, Context, Event, FailoverAction, HeldLease, LiveFeed, MAX_SCAN_LIMIT,
    MessageId, RequestId, Result, SandboxArguments, SessionId, SessionRecord, SessionState,
    Snapshot, SnapshotRef, StoreError, Timestamp, ToolCallRecord, TurnStage, Ulid, Worker, decode,
    encode,
};

/// State carried from claim through the final fenced write. The lease remains
/// owned here while the heartbeat holds a clone of its handle.
pub(super) struct StepContext {
    pub session: SessionRecord,
    pub lease: Arc<HeldLease>,
    pub snapshot: Option<Snapshot>,
    pub events: Vec<Event>,
    pub turn: Option<MessageId>,
}

impl Worker {
    pub(super) async fn tail(&self, id: SessionId, after: u64, through: u64) -> Result<Vec<Event>> {
        let mut events = Vec::new();
        let mut cursor = after;
        loop {
            let page = self.store.read_events(id, cursor, MAX_SCAN_LIMIT).await?;
            let Some(last) = page.last() else {
                return Ok(events);
            };
            cursor = last.seq();
            events.extend(page.into_iter().filter(|event| event.seq() <= through));
            if cursor >= through {
                return Ok(events);
            }
        }
    }

    pub(super) async fn publish_events(&self, id: SessionId, events: &[Event]) -> Result<()> {
        for event in events {
            if let Err(error) = self
                .bus
                .publish_live(LiveFeed::SessionEvents(id), event)
                .await
            {
                if matches!(error, swarmy_bus::Error::PayloadTooLarge { .. }) {
                    // The event is already durable. Live observers can read it by cursor.
                    tracing::warn!(%id, seq = event.seq(), %error, "live event exceeds bus limit");
                } else {
                    return Err(error.into());
                }
            }
        }
        Ok(())
    }

    pub(super) async fn publish_tail(&self, id: SessionId, events: &[Event]) -> Result<()> {
        for event in events {
            // This session is leased. Historical idle events, including ones
            // written before the atomic finish API, cannot announce readiness.
            if !matches!(
                event,
                Event::StateChanged {
                    to: SessionState::Idle,
                    ..
                }
            ) {
                self.publish_events(id, std::slice::from_ref(event)).await?;
            }
        }
        Ok(())
    }

    pub(super) async fn append(
        &self,
        session: &mut SessionRecord,
        lease: &HeldLease,
        events: &mut Vec<Event>,
        batch: &[Event],
    ) -> Result<()> {
        let before = session.head_seq;
        {
            let token = lease.lock().await;
            session.head_seq = self
                .store
                .append_events_leased(
                    session.session_id,
                    before,
                    batch,
                    token.as_ref().context("lease released")?,
                    Timestamp::now(),
                )
                .await?;
        }
        let appended: Vec<_> = batch
            .iter()
            .cloned()
            .zip(before + 1..)
            .map(|(mut event, seq)| {
                event.set_seq(seq);
                event
            })
            .collect();
        self.publish_events(session.session_id, &appended).await?;
        events.extend(appended);
        Ok(())
    }

    pub(super) async fn snapshot(&self, key: &str) -> Result<Snapshot> {
        if let Some(snapshot) = self.snapshots.lock().await.get(key) {
            return Ok(snapshot.clone());
        }
        let bytes = self.blobs.get(key).await?;
        let snapshot: Snapshot = decode(&bytes)?;
        // Content-addressed snapshots are immutable. Bound retained data while
        // avoiding repeat downloads for the claim, dispatch, and fold of a turn.
        if bytes.len() <= 1024 * 1024 {
            let mut cache = self.snapshots.lock().await;
            if cache.len() >= 16 {
                cache.clear();
            }
            cache.insert(key.to_owned(), snapshot.clone());
        }
        Ok(snapshot)
    }

    pub(super) async fn load_history(
        &self,
        session: &SessionRecord,
        events: &mut Vec<Event>,
    ) -> Result<Snapshot> {
        let (snapshot, after) = if let Some(reference) = &session.snapshot_ref {
            (self.snapshot(&reference.object_key).await?, reference.seq)
        } else {
            (Snapshot::default(), 0)
        };
        let cursor = events.last().map_or(after, Event::seq);
        if cursor < session.head_seq {
            events.extend(
                self.tail(session.session_id, cursor, session.head_seq)
                    .await?,
            );
        }
        Ok(snapshot)
    }
    pub(super) async fn step(&self, ctx: &mut StepContext) -> Result<()> {
        let Some(snapshot) = self.prepare_step(ctx).await? else {
            return Ok(());
        };
        ctx.snapshot = Some(snapshot);
        let StepContext {
            session,
            lease,
            snapshot,
            events,
            turn,
        } = ctx;
        let snapshot = snapshot.as_ref().context("step snapshot missing")?;
        let (lease, turn, id) = (lease.as_ref(), *turn, session.session_id);
        loop {
            if self
                .interrupt_if_requested(&mut *session, lease, snapshot, &mut *events, turn)
                .await?
            {
                return Ok(());
            }
            let message_id = fold_id(
                id,
                session
                    .head_seq
                    .checked_add(1)
                    .context("sequence overflow")?,
            );
            match self
                .config
                .harness
                .step(session, snapshot, events, message_id)
            {
                Action::BuildInference(request) => {
                    return self
                        .build_inference(&mut *session, lease, &[], request)
                        .await;
                }
                Action::DispatchTools(calls) => {
                    let display = self.session_display(session).await?;
                    if self.store.ensure_session_computer(id).await.is_ok()
                        && calls
                            .iter()
                            .all(|call| self.can_dispatch_tool(call, display))
                    {
                        return self.dispatch_calls(session, lease, &calls, turn).await;
                    }
                    let batch: Vec<_> = calls
                        .into_iter()
                        .enumerate()
                        .map(|(index, call)| Event::ToolCallRequested {
                            seq: 0,
                            request_id: RequestId::for_step(
                                id,
                                session.head_seq
                                    + 1
                                    + u64::try_from(index).expect("batch fits u64"),
                            ),
                            call,
                        })
                        .collect();
                    self.append(&mut *session, lease, &mut *events, &batch)
                        .await?;
                    if self
                        .execute_pending(&mut *session, lease, &mut *events, turn)
                        .await?
                    {
                        return Ok(());
                    }
                }
                Action::FoldResults(message) => {
                    return self
                        .fold_results(&mut *session, lease, snapshot, &mut *events, message)
                        .await;
                }
                Action::Wait => {
                    if let Some(request_id) = pending_inference(events) {
                        let job = self.load_job(request_id).await?;
                        return self.submit(&job, lease).await;
                    }
                    if pending_tools(events).is_empty() {
                        return self
                            .finish(&mut *session, lease, snapshot, &mut *events, turn)
                            .await;
                    }
                    if self
                        .execute_pending(&mut *session, lease, &mut *events, turn)
                        .await?
                    {
                        return Ok(());
                    }
                }
                Action::EndTurn => {
                    return self
                        .finish(&mut *session, lease, snapshot, &mut *events, turn)
                        .await;
                }
            }
        }
    }

    pub(super) async fn prepare_step(&self, ctx: &mut StepContext) -> Result<Option<Snapshot>> {
        let StepContext {
            session,
            lease,
            events,
            turn,
            ..
        } = ctx;
        let lease = lease.as_ref();
        let turn = *turn;
        let snapshot = self.load_history(session, events).await?;
        // Replaying the tail also fans out events written by the gateway or a caller,
        // and retries a publication interrupted by the previous worker's death.
        self.publish_tail(session.session_id, events).await?;
        if self
            .interrupt_if_requested(session, lease, &snapshot, events, turn)
            .await?
            || self
                .handle_inference_wait(session, turn, lease, &snapshot, events)
                .await?
        {
            return Ok(None);
        }
        if self.summary_completed(session, events).await? {
            self.finish(session, lease, &snapshot, events, turn).await?;
            return Ok(None);
        }
        Ok(Some(snapshot))
    }

    pub(super) async fn interrupt_if_requested(
        &self,
        session: &mut SessionRecord,
        lease: &HeldLease,
        snapshot: &Snapshot,
        events: &mut Vec<Event>,
        turn: Option<MessageId>,
    ) -> Result<bool> {
        if !self.store.interrupt_requested(session.session_id).await? {
            return Ok(false);
        }
        let request_id = events
            .iter()
            .rev()
            .find_map(|event| match event {
                Event::InferenceRequested { request_id, .. }
                | Event::InferenceFailed { request_id, .. } => Some(*request_id),
                _ => None,
            })
            .unwrap_or(RequestId::for_step(
                session.session_id,
                session
                    .head_seq
                    .checked_add(1)
                    .context("sequence overflow")?,
            ));
        self.append(
            session,
            lease,
            events,
            &[swarmy_core::interrupted_event(0, request_id)],
        )
        .await?;
        session.interrupt_requested = true;
        self.store.clear_inference_wait(session.session_id).await?;
        self.finish(session, lease, snapshot, events, turn).await?;
        Ok(true)
    }

    pub(super) async fn handle_inference_wait(
        &self,
        session: &mut SessionRecord,
        turn: Option<MessageId>,
        lease: &HeldLease,
        snapshot: &Snapshot,
        events: &mut Vec<Event>,
    ) -> Result<bool> {
        let id = session.session_id;
        if matches!(
            events.iter().rev().find(|event| matches!(
                event,
                Event::InferenceFailed { .. } | Event::InferenceCompleted { .. }
            )),
            Some(
                Event::InferenceCompleted { .. }
                    | Event::InferenceFailed {
                        retryable: false,
                        ..
                    }
            )
        ) {
            self.store.clear_inference_wait(id).await?;
        }
        if let Some(wait) = self.store.inference_wait(id).await?
            && wait
                .since
                .checked_add(self.config.max_inference_wait)
                .is_ok_and(|limit| jiff::Timestamp::now() >= limit)
        {
            let request_id = events
                .iter()
                .rev()
                .find_map(|event| match event {
                    Event::InferenceFailed { request_id, .. } => Some(*request_id),
                    _ => None,
                })
                .unwrap_or(RequestId::for_step(
                    id,
                    session
                        .head_seq
                        .checked_add(1)
                        .context("sequence overflow")?,
                ));
            let terminal = Event::InferenceFailed {
                seq: 0,
                request_id,
                error: format!(
                    "inference wait exceeded: {} ({} attempts)",
                    wait.reasons.join("; "),
                    wait.attempts
                ),
                retryable: false,
                retry_at: None,
                failure_kind: swarmy_core::FailureKind::WaitExceeded,
            };
            self.append(session, lease, events, &[terminal]).await?;
            self.store.clear_inference_wait(id).await?;
            self.finish(session, lease, snapshot, events, turn).await?;
            return Ok(true);
        }
        if let Some(Event::InferenceFailed {
            seq,
            error,
            failure_kind,
            retryable: true,
            retry_at: Some(retry_at),
            ..
        }) = events.iter().rev().find(|event| {
            matches!(
                event,
                Event::InferenceFailed { .. } | Event::InferenceCompleted { .. }
            )
        }) {
            let now = jiff::Timestamp::now();
            let wait = self.store.inference_wait(id).await?;
            if wait
                .as_ref()
                .is_none_or(|wait| wait.last_failure_seq != *seq)
            {
                return self
                    .failover_or_park(session, lease, *seq, (error, *failure_kind), *retry_at, now)
                    .await;
            }
        }
        Ok(false)
    }

    /// Failover happens at attempt boundaries. The store chooses the next
    /// usable route step (or earliest retry) and commits that decision with
    /// the wait, so a retried worker cannot select a different step.
    pub(super) async fn failover_or_park(
        &self,
        session: &mut SessionRecord,
        lease: &HeldLease,
        seq: u64,
        failure: (&str, swarmy_core::FailureKind),
        retry_at: Timestamp,
        now: Timestamp,
    ) -> Result<bool> {
        let (error, failure_kind) = failure;
        let outcome = {
            let token = lease.lock().await;
            let lease_ref = token.as_ref().context("lease released")?;
            self.store
                .failover_route_step(
                    session.session_id,
                    lease_ref,
                    seq,
                    error,
                    failure_kind,
                    retry_at,
                    session.route_step,
                    session.route.as_deref(),
                    session.inference.provider.as_deref(),
                    self.config.default_route.as_deref(),
                    &self.config.provider,
                    now,
                    self.config.max_inference_wait,
                )
                .await?
        };
        match outcome.action {
            FailoverAction::AdvanceTo(step) => {
                warn_on_route_fallback(session, outcome.route.as_deref(), &outcome.skipped);
                session.route_step = step;
                self.kill("after_advance");
                Ok(false)
            }
            FailoverAction::Park => {
                warn_on_route_fallback(session, outcome.route.as_deref(), &outcome.skipped);
                session.route_step = 0;
                lease.release().await;
                Ok(true)
            }
            // The failure was already handled before a restart or lease
            // lapse: continue the turn without moving again or parking
            // behind the successor's in-flight request.
            FailoverAction::AlreadyHandled => Ok(false),
        }
    }

    pub(super) async fn fold_results(
        &self,
        session: &mut SessionRecord,
        lease: &HeldLease,
        snapshot: &Snapshot,
        events: &mut Vec<Event>,
        message: swarmy_core::Message,
    ) -> Result<()> {
        let message_id = message.id;
        let folded = Event::MessageAppended {
            seq: session
                .head_seq
                .checked_add(1)
                .context("sequence overflow")?,
            message,
        };
        // Fleet tasks run hundreds of tool rounds inside a single turn and
        // never reach turn end before the provider window fills, so compare
        // the last inference usage with the side threshold here, between the
        // folded tool results and the next request. A chat-shaped turn still
        // gets its check at turn end in `finish`.
        if self
            .summarize(session, lease, snapshot, events, Some(&folded))
            .await?
        {
            return Ok(());
        }
        events.push(folded.clone());
        let Action::BuildInference(request) = self
            .config
            .harness
            .step(session, snapshot, events, message_id)
        else {
            anyhow::bail!("folded tool results did not produce inference");
        };
        self.build_inference(session, lease, &[folded], request)
            .await
    }

    pub(super) fn can_dispatch_tool(&self, call: &ToolCallRecord, display: bool) -> bool {
        (!swarmy_tools::is_display_name(&call.tool) || display)
            && self
                .config
                .harness
                .tools
                .get(&call.tool)
                .is_some_and(swarmy_harness::Tool::sandbox_bound)
            && SandboxArguments::parse(&call.tool, call.arguments.clone()).is_ok()
    }

    pub(crate) async fn session_display(&self, session: &SessionRecord) -> Result<bool> {
        let mut cache = self.display_by_session.lock().await;
        if let Some(display) = cache.get(&session.session_id) {
            return Ok(*display);
        }
        let display = if let Some(agent) = self.store.get_agent(session.agent_id).await? {
            self.store.image_display(&agent.image).await?
        } else {
            false
        };
        // Session images do not change during a worker lifetime. A worker restart
        // drops this cache; restart workers after changing an agent's image.
        cache.insert(session.session_id, display);
        Ok(display)
    }

    pub(super) async fn transition(
        &self,
        id: SessionId,
        lease: &HeldLease,
        state: SessionState,
    ) -> Result<()> {
        let mut token = lease.lock().await;
        self.store
            .set_state(
                id,
                state,
                Some(token.as_ref().context("lease released")?),
                Timestamp::now(),
            )
            .await?;
        token.release();
        Ok(())
    }

    pub(super) async fn finish(
        &self,
        session: &mut SessionRecord,
        lease: &HeldLease,
        snapshot: &Snapshot,
        events: &mut Vec<Event>,
        turn: Option<MessageId>,
    ) -> Result<()> {
        if !session.interrupt_requested
            && self
                .summarize(session, lease, snapshot, events, None)
                .await?
        {
            return Ok(());
        }
        let event = loop {
            let head = session
                .head_seq
                .checked_add(1)
                .context("sequence overflow")?;
            events.push(Event::StateChanged {
                seq: head,
                from: SessionState::Leased,
                to: SessionState::Idle,
            });
            let bytes = encode(&snapshot.replay(events))?;
            let reference = SnapshotRef {
                object_key: format!("blobs/{}", blake3::hash(&bytes).to_hex()),
                seq: head,
            };
            self.blobs.put(&reference.object_key, bytes.into()).await?;
            self.kill("before_release");
            let result = {
                let mut token = lease.lock().await;
                let result = self
                    .store
                    .finish_turn(
                        session.session_id,
                        session.head_seq,
                        token.as_ref().context("lease released")?,
                        &reference,
                    )
                    .await;
                if result.is_ok() {
                    token.release();
                }
                result
            };
            match result {
                Ok(event) => break event,
                Err(StoreError::InterruptPending) => {
                    events.pop();
                    let request_id = events
                        .iter()
                        .rev()
                        .find_map(|event| match event {
                            Event::InferenceRequested { request_id, .. }
                            | Event::InferenceFailed { request_id, .. } => Some(*request_id),
                            _ => None,
                        })
                        .unwrap_or(RequestId::for_step(session.session_id, head));
                    self.append(
                        session,
                        lease,
                        events,
                        &[swarmy_core::interrupted_event(0, request_id)],
                    )
                    .await?;
                    session.interrupt_requested = true;
                }
                Err(error) => {
                    tracing::warn!(
                        session_id = %session.session_id,
                        %error,
                        "finish_turn failed"
                    );
                    return Err(error.into());
                }
            }
        };
        if let Some(turn) = turn {
            let event = Bus::turn_event(session.session_id, turn, TurnStage::Idle, None);
            self.bus.record_turn(&event).await;
            self.store.observe_turn_stage(event);
        }
        self.publish_events(session.session_id, &[event]).await
    }
}
pub(super) fn fold_id(id: SessionId, step: u64) -> MessageId {
    let request = RequestId::for_step(id, step);
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&request.as_bytes()[..16]);
    MessageId::from_ulid(Ulid::from_bytes(bytes))
}
pub(super) fn pending_inference(events: &[Event]) -> Option<RequestId> {
    events
        .iter()
        .rev()
        .find_map(|event| match event {
            Event::InferenceRequested { request_id, .. } => Some(Some(*request_id)),
            Event::InferenceCompleted { .. } | Event::InferenceFailed { .. } => Some(None),
            _ => None,
        })
        .flatten()
}

pub(super) fn pending_tools(events: &[Event]) -> Vec<(RequestId, ToolCallRecord)> {
    events.iter().filter_map(|event| {
        if let Event::ToolCallRequested { request_id, call, .. } = event
            && !events.iter().any(|event| matches!(event, Event::ToolCallCompleted { request_id: completed, call_id, .. } if completed == request_id && *call_id == call.call_id)) {
                return Some((*request_id, call.clone()));
        }
        None
    }).collect()
}
