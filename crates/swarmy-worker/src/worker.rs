use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use jiff::Timestamp;
use swarmy_bus::{Bus, LiveFeed, SubjectToken, WorkMessage, WorkQueue};
use swarmy_core::{
    Event, InflightRecord, Lease, LeaseOwnerId, MessageId, Nudge, RequestId, SandboxArguments,
    SessionId, SessionRecord, SessionState, SnapshotRef, ToolCallRecord, ToolJob, TurnStage,
    decode, encode,
};
use swarmy_harness::{Action, Snapshot, execution_result};
use swarmy_llm::InferenceJob;
use swarmy_store::{MAX_SCAN_LIMIT, Store, StoreError, blob::BlobStore, runnable_partition};
use tokio::{
    sync::Mutex,
    time::{MissedTickBehavior, interval},
};
use ulid::Ulid;

use crate::config::Config;

type ActiveLease = Mutex<Option<Lease>>;

pub struct Worker {
    store: Store,
    bus: Bus,
    blobs: Arc<dyn BlobStore>,
    config: Config,
    pub owner: LeaseOwnerId,
}

impl Worker {
    pub fn new(store: Store, bus: Bus, blobs: Arc<dyn BlobStore>, config: Config) -> Self {
        Self {
            store,
            bus,
            blobs,
            config,
            owner: LeaseOwnerId::from_ulid(Ulid::generate()),
        }
    }

    fn kill(&self, point: &str) {
        if self.config.kill_point.as_deref() == Some(point) {
            tracing::warn!(point, "instrumented worker exit");
            std::process::exit(137);
        }
    }

    pub async fn handle(&self, message: &WorkMessage<Nudge>) -> Result<()> {
        let id = message.value.session_id;
        let turn = self.store.turn_id(id).await?;
        let lease = match self
            .store
            .claim_lease(
                id,
                self.owner,
                Timestamp::now().checked_add(self.config.lease_duration)?,
            )
            .await
        {
            Ok(lease) => lease,
            Err(StoreError::InvalidState) => {
                return Ok(message.acknowledge().await?);
            }
            Err(error) => return Err(error.into()),
        };
        tracing::info!(session_id = %id, owner = %lease.owner, step = lease.seq, "claimed step");
        if let Some(turn) = turn {
            self.bus
                .record_turn(&Bus::turn_event(
                    id,
                    turn,
                    swarmy_core::TurnStage::Claimed,
                    None,
                ))
                .await;
        }
        self.kill("after_claim");
        let lease = Mutex::new(Some(lease));
        tokio::select! {
            result = self.step(id, &lease) => result?,
            result = self.heartbeat(id, &lease, message) => result?,
        }
        message.acknowledge().await?;
        Ok(())
    }

    async fn heartbeat(
        &self,
        id: SessionId,
        lease: &ActiveLease,
        message: &WorkMessage<Nudge>,
    ) -> Result<()> {
        let period = (self.config.lease_duration / 3).min(self.config.bus.ack_wait / 3);
        let mut ticks = interval(period);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticks.tick().await;
            message.extend_deadline().await?;
            let mut token = lease.lock().await;
            if let Some(current) = token.as_ref() {
                let now = Timestamp::now();
                *token = Some(
                    self.store
                        .renew_lease(
                            id,
                            current,
                            now,
                            now.checked_add(self.config.lease_duration)?,
                        )
                        .await?,
                );
            }
        }
    }

    async fn tail(&self, id: SessionId, after: u64, through: u64) -> Result<Vec<Event>> {
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

    async fn publish_events(&self, id: SessionId, events: &[Event]) -> Result<()> {
        for event in events {
            self.bus
                .publish_live(LiveFeed::SessionEvents(id), event)
                .await?;
        }
        Ok(())
    }

    async fn append(
        &self,
        session: &mut SessionRecord,
        lease: &ActiveLease,
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
        // Read assigned sequences from durable storage, also resolving oversized payloads.
        let appended = self
            .tail(session.session_id, before, session.head_seq)
            .await?;
        self.publish_events(session.session_id, &appended).await?;
        events.extend(appended);
        Ok(())
    }

    async fn step(&self, id: SessionId, lease: &ActiveLease) -> Result<()> {
        let mut session = self
            .store
            .fetch_session(id)
            .await?
            .context("session missing")?;
        let (snapshot, after) = if let Some(reference) = &session.snapshot_ref {
            (
                decode::<Snapshot>(&self.blobs.get(&reference.object_key).await?)?,
                reference.seq,
            )
        } else {
            (Snapshot::default(), 0)
        };
        let mut events = self.tail(id, after, session.head_seq).await?;
        // Replaying the tail also fans out events written by the gateway or a caller,
        // and retries a publication interrupted by the previous worker's death.
        self.publish_events(id, &events).await?;
        loop {
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
                .step(&session, &snapshot, &events, message_id)
            {
                Action::BuildInference(request) => {
                    return self
                        .build_inference(&mut session, lease, &mut events, request)
                        .await;
                }
                Action::DispatchTools(calls) => {
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
                    self.append(&mut session, lease, &mut events, &batch)
                        .await?;
                    if self
                        .execute_pending(&mut session, lease, &mut events)
                        .await?
                    {
                        return Ok(());
                    }
                }
                Action::FoldResults(message) => {
                    self.append(
                        &mut session,
                        lease,
                        &mut events,
                        &[Event::MessageAppended { seq: 0, message }],
                    )
                    .await?;
                }
                Action::Wait => {
                    if let Some(request_id) = pending_inference(&events) {
                        let job = self.load_job(request_id).await?;
                        return self.submit(&job, lease).await;
                    }
                    if pending_tools(&events).is_empty() {
                        return self
                            .finish(&mut session, lease, &snapshot, &mut events)
                            .await;
                    }
                    if self
                        .execute_pending(&mut session, lease, &mut events)
                        .await?
                    {
                        return Ok(());
                    }
                }
                Action::EndTurn => {
                    return self
                        .finish(&mut session, lease, &snapshot, &mut events)
                        .await;
                }
            }
        }
    }

    async fn build_inference(
        &self,
        session: &mut SessionRecord,
        lease: &ActiveLease,
        events: &mut Vec<Event>,
        request: swarmy_llm::Request,
    ) -> Result<()> {
        let id = session.session_id;
        let step = session
            .head_seq
            .checked_add(1)
            .context("sequence overflow")?;
        let job = InferenceJob {
            session_id: id,
            step,
            request_id: RequestId::for_step(id, step),
            request,
        };
        {
            let token = lease.lock().await;
            self.store
                .put_inference_input(
                    id,
                    session.head_seq,
                    token.as_ref().context("lease released")?,
                    Timestamp::now(),
                    &job,
                )
                .await?;
        }
        self.append(
            session,
            lease,
            events,
            &[Event::InferenceRequested {
                seq: 0,
                request_id: job.request_id,
                step,
            }],
        )
        .await?;
        self.kill("after_request_event");
        self.submit(&job, lease).await
    }

    async fn execute_pending(
        &self,
        session: &mut SessionRecord,
        lease: &ActiveLease,
        events: &mut Vec<Event>,
    ) -> Result<bool> {
        let mut jobs = Vec::new();
        let id = session.session_id;
        let turn = self.store.turn_id(session.session_id).await?;
        for (request_id, call) in pending_tools(events) {
            let result = match self.config.harness.tools.get(&call.tool) {
                Some(tool) if tool.sandbox_bound() => {
                    match SandboxArguments::parse(&call.tool, call.arguments.clone()) {
                        Ok(arguments) => {
                            let step = events
                                .iter()
                                .find_map(|event| match event {
                                    Event::ToolCallRequested {
                                        seq,
                                        request_id: requested,
                                        ..
                                    } if *requested == request_id => Some(*seq),
                                    _ => None,
                                })
                                .context("tool request missing")?;
                            jobs.push(ToolJob {
                                session_id: session.session_id,
                                request_id,
                                call_id: call.call_id,
                                step,
                                arguments,
                            });
                            continue;
                        }
                        Err(error) => Err(error.to_string()),
                    }
                }
                Some(tool) => {
                    self.tool_stage(id, turn, TurnStage::ToolDispatched, request_id)
                        .await;
                    tool.execute(call.arguments).await
                }
                None => Err(format!("unknown tool: {}", call.tool)),
            };
            self.append(
                session,
                lease,
                events,
                &[Event::ToolCallCompleted {
                    seq: 0,
                    request_id,
                    call_id: call.call_id,
                    result: execution_result(&call.tool, result),
                }],
            )
            .await?;
            self.tool_stage(id, turn, TurnStage::ToolCompleted, request_id)
                .await;
        }
        if jobs.is_empty() {
            return Ok(false);
        }
        // Resolve before releasing the step, and persist the epoch with the jobs.
        let placement = self.place(session.session_id).await?;
        {
            let mut token = lease.lock().await;
            self.store
                .dispatch_placed_tool_jobs(
                    session.session_id,
                    token.as_ref().context("lease released")?,
                    &jobs,
                    &placement,
                )
                .await?;
            *token = None;
        }
        self.kill("after_release");
        for job in jobs {
            self.route_tool(&job).await?;
        }
        Ok(true)
    }

    async fn tool_stage(
        &self,
        id: SessionId,
        turn: Option<MessageId>,
        stage: swarmy_core::TurnStage,
        request: RequestId,
    ) {
        if let Some(turn) = turn {
            self.bus
                .record_turn(&Bus::turn_event(id, turn, stage, Some(request)))
                .await;
        }
    }

    async fn place(&self, id: SessionId) -> Result<swarmy_core::PlacementRecord> {
        let agent = self
            .store
            .fetch_session(id)
            .await?
            .context("session missing")?
            .agent_id;
        crate::placement::resolve(&self.store, agent, self.config.placement_lease).await
    }

    async fn route_tool(&self, job: &ToolJob) -> Result<()> {
        let placement = self.place(job.session_id).await?;
        if self.store.route_tool_job(job, &placement).await? {
            let turn = self.store.request_turn_id(job.request_id).await?;
            if let Some(turn) = turn {
                self.bus
                    .record_turn(&Bus::turn_event(
                        job.session_id,
                        turn,
                        swarmy_core::TurnStage::ToolDispatched,
                        Some(job.request_id),
                    ))
                    .await;
            }
            self.bus
                .publish_work(&WorkQueue::NodeTools(placement.node_id), job)
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn recover_tools(&self) -> Result<()> {
        let mut after = None;
        loop {
            let jobs = self.store.scan_tool_jobs(after, MAX_SCAN_LIMIT).await?;
            if jobs.is_empty() {
                return Ok(());
            }
            for job in jobs {
                after = Some(job.request_id);
                if self
                    .config
                    .partitions
                    .contains(&runnable_partition(job.session_id))
                {
                    // A dead node cannot consume its own redeliveries. The durable
                    // outbox scan re-resolves every retry and repairs lost publishes.
                    let result = self.route_tool(&job).await;
                    if let Err(error) = result {
                        tracing::warn!(%error, request_id = %job.request_id, "tool recovery failed");
                    }
                }
            }
        }
    }

    async fn submit(&self, job: &InferenceJob, lease: &ActiveLease) -> Result<()> {
        {
            let token = lease.lock().await;
            self.store
                .put_inflight_leased(
                    job.request_id,
                    &InflightRecord {
                        session_id: job.session_id,
                        seq: job.step,
                        provider: self.config.provider.clone(),
                        key_id: String::new(),
                    },
                    token.as_ref().context("lease released")?,
                    Timestamp::now(),
                )
                .await?;
        }
        self.kill("before_release");
        self.transition(job.session_id, lease, SessionState::WaitingInference)
            .await?;
        self.kill("after_release");
        self.bus
            .publish_work(
                &WorkQueue::Inference(SubjectToken::new(&self.config.provider)?),
                job,
            )
            .await?;
        Ok(())
    }

    async fn transition(
        &self,
        id: SessionId,
        lease: &ActiveLease,
        state: SessionState,
    ) -> Result<()> {
        let turn = self.store.turn_id(id).await?;
        let mut token = lease.lock().await;
        self.store
            .set_state(
                id,
                state,
                Some(token.as_ref().context("lease released")?),
                Timestamp::now(),
            )
            .await?;
        *token = None;
        if state == SessionState::Idle
            && let Some(turn) = turn
        {
            self.bus
                .record_turn(&Bus::turn_event(
                    id,
                    turn,
                    swarmy_core::TurnStage::Idle,
                    None,
                ))
                .await;
        }
        Ok(())
    }

    async fn finish(
        &self,
        session: &mut SessionRecord,
        lease: &ActiveLease,
        snapshot: &Snapshot,
        events: &mut Vec<Event>,
    ) -> Result<()> {
        let last = self
            .store
            .read_events(session.session_id, session.head_seq.saturating_sub(1), 1)
            .await?;
        if !matches!(
            last.last(),
            Some(Event::StateChanged {
                to: SessionState::Idle,
                ..
            })
        ) {
            self.append(
                session,
                lease,
                events,
                &[Event::StateChanged {
                    seq: 0,
                    from: SessionState::Leased,
                    to: SessionState::Idle,
                }],
            )
            .await?;
        }
        let bytes = encode(&snapshot.replay(events))?;
        let reference = SnapshotRef {
            object_key: format!("blobs/{}", blake3::hash(&bytes).to_hex()),
            seq: session.head_seq,
        };
        self.blobs.put(&reference.object_key, bytes.into()).await?;
        self.store
            .write_snapshot(session.session_id, &reference)
            .await?;
        self.kill("before_release");
        self.transition(session.session_id, lease, SessionState::Idle)
            .await
    }

    async fn load_job(&self, id: RequestId) -> Result<InferenceJob> {
        let job: InferenceJob = self
            .store
            .get_inference_input(id)
            .await?
            .context("inference input missing")?;
        ensure!(
            job.request_id == id && RequestId::for_step(job.session_id, job.step) == id,
            "invalid stored inference job"
        );
        Ok(job)
    }

    pub async fn recovery_loop(&self) {
        let mut ticks = interval(self.config.recovery_interval);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticks.tick().await;
            if let Err(error) = self.recover_tools().await {
                tracing::warn!(%error, "tool recovery scan failed");
            }
            if let Err(error) = self.recover().await {
                tracing::warn!(%error, "inference recovery scan failed");
            }
        }
    }

    async fn recover(&self) -> Result<()> {
        let mut after = None;
        loop {
            let page = self.store.scan_inflight(after, MAX_SCAN_LIMIT).await?;
            if page.is_empty() {
                return Ok(());
            }
            for record in page {
                let request_id = RequestId::for_step(record.session_id, record.seq);
                after = Some(request_id);
                if self
                    .config
                    .partitions
                    .contains(&runnable_partition(record.session_id))
                    && let Err(error) = self.republish(&record, request_id).await
                {
                    tracing::warn!(%request_id, %error, "request recovery failed");
                }
            }
        }
    }

    async fn republish(&self, record: &InflightRecord, request_id: RequestId) -> Result<()> {
        let session = self
            .store
            .fetch_session(record.session_id)
            .await?
            .context("session missing")?;
        if session.state == SessionState::WaitingInference {
            let job = self.load_job(request_id).await?;
            self.bus
                .publish_work(
                    &WorkQueue::Inference(SubjectToken::new(&record.provider)?),
                    &job,
                )
                .await?;
        }
        Ok(())
    }
}

fn fold_id(id: SessionId, step: u64) -> MessageId {
    let request = RequestId::for_step(id, step);
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&request.as_bytes()[..16]);
    MessageId::from_ulid(Ulid::from_bytes(bytes))
}

fn pending_inference(events: &[Event]) -> Option<RequestId> {
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

fn pending_tools(events: &[Event]) -> Vec<(RequestId, ToolCallRecord)> {
    events.iter().filter_map(|event| {
        if let Event::ToolCallRequested { request_id, call, .. } = event
            && !events.iter().any(|event| matches!(event, Event::ToolCallCompleted { request_id: completed, call_id, .. } if completed == request_id && *call_id == call.call_id)) {
                return Some((*request_id, call.clone()));
        }
        None
    }).collect()
}
