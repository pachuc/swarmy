use std::{collections::HashMap, fmt::Write, sync::Arc};

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
    time::{Instant, MissedTickBehavior, interval, interval_at},
};
use ulid::Ulid;

use crate::config::Config;

type ActiveLease = Mutex<Option<Lease>>;

pub struct Worker {
    store: Store,
    bus: Bus,
    blobs: Arc<dyn BlobStore>,
    config: Config,
    placements: crate::placement::Cache,
    snapshots: Mutex<HashMap<String, Snapshot>>,
    pub owner: LeaseOwnerId,
}

impl Worker {
    pub fn new(store: Store, bus: Bus, blobs: Arc<dyn BlobStore>, config: Config) -> Self {
        Self {
            store,
            bus,
            blobs,
            config,
            placements: crate::placement::Cache::default(),
            snapshots: Mutex::default(),
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
        let (lease, session, turn, events) = match self
            .store
            .claim_step_with_tail(
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
            result = self.step(session.clone(), turn, &lease, events) => {
                if result.is_err() { self.placements.invalidate(session.agent_id).await; }
                result?;
            },
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
        let mut ticks = interval_at(Instant::now() + period, period);
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

    async fn publish_tail(&self, id: SessionId, events: &[Event]) -> Result<()> {
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

    async fn snapshot(&self, key: &str) -> Result<Snapshot> {
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

    async fn load_history(
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

    async fn step(
        &self,
        mut session: SessionRecord,
        turn: Option<MessageId>,
        lease: &ActiveLease,
        mut events: Vec<Event>,
    ) -> Result<()> {
        let id = session.session_id;
        let snapshot = self.load_history(&session, &mut events).await?;
        // Replaying the tail also fans out events written by the gateway or a caller,
        // and retries a publication interrupted by the previous worker's death.
        self.publish_tail(id, &events).await?;
        if self.summary_completed(&session, &events).await? {
            return self
                .finish(&mut session, lease, &snapshot, &mut events, turn)
                .await;
        }
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
                        .build_inference(&mut session, lease, &[], request)
                        .await;
                }
                Action::DispatchTools(calls) => {
                    if self.store.ensure_session_computer(id).await.is_ok()
                        && calls.iter().all(|call| {
                            self.config
                                .harness
                                .tools
                                .get(&call.tool)
                                .is_some_and(swarmy_harness::Tool::sandbox_bound)
                                && SandboxArguments::parse(&call.tool, call.arguments.clone())
                                    .is_ok()
                        })
                    {
                        return self.dispatch_calls(&session, lease, &calls, turn).await;
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
                    self.append(&mut session, lease, &mut events, &batch)
                        .await?;
                    if self
                        .execute_pending(&mut session, lease, &mut events, turn)
                        .await?
                    {
                        return Ok(());
                    }
                }
                Action::FoldResults(message) => {
                    return self
                        .fold_results(&mut session, lease, &snapshot, &mut events, message)
                        .await;
                }
                Action::Wait => {
                    if let Some(request_id) = pending_inference(&events) {
                        let job = self.load_job(request_id).await?;
                        return self.submit(&job, lease).await;
                    }
                    if pending_tools(&events).is_empty() {
                        return self
                            .finish(&mut session, lease, &snapshot, &mut events, turn)
                            .await;
                    }
                    if self
                        .execute_pending(&mut session, lease, &mut events, turn)
                        .await?
                    {
                        return Ok(());
                    }
                }
                Action::EndTurn => {
                    return self
                        .finish(&mut session, lease, &snapshot, &mut events, turn)
                        .await;
                }
            }
        }
    }

    async fn fold_results(
        &self,
        session: &mut SessionRecord,
        lease: &ActiveLease,
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

    async fn build_inference(
        &self,
        session: &mut SessionRecord,
        lease: &ActiveLease,
        preceding: &[Event],
        mut request: swarmy_llm::Request,
    ) -> Result<()> {
        // Resolve on every inference so existing sessions see later agent updates.
        // A summary request keeps its own prompt; every other request gets the agent's
        // prompt override first and then the memory directory and contents appended.
        let summarizing = request.system_prompt == swarmy_harness::SUMMARY_PROMPT;
        if let swarmy_core::SessionKind::Named { agent_id } = session.kind
            && let Some(agent) = self.store.get_agent(agent_id).await?
        {
            if let Some(prompt) = agent.system_prompt
                && !summarizing
            {
                request.system_prompt = prompt;
            }
            if let Some(model) = agent.model {
                request.settings.model = model;
            }
            if let Some(effort) = agent.reasoning_effort {
                request.settings.reasoning_effort = Some(effort);
            }
        }
        if !summarizing {
            request.system_prompt = request
                .system_prompt
                .replace("{memory_dir}", &self.config.memory_dir);
            if matches!(session.kind, swarmy_core::SessionKind::Named { .. }) {
                let memory = self.memory(session.agent_id).await?;
                write!(
                    request.system_prompt,
                    "\n\nAgent memory ({}):\n{memory}",
                    self.config.memory_dir
                )?;
            }
        }
        let id = session.session_id;
        let step = session
            .head_seq
            .checked_add(u64::try_from(preceding.len())?)
            .and_then(|head| head.checked_add(1))
            .context("sequence overflow")?;
        let job = InferenceJob {
            session_id: id,
            step,
            request_id: RequestId::for_step(id, step),
            request,
        };
        self.kill("before_release");
        let event = {
            let mut token = lease.lock().await;
            let event = self
                .store
                .submit_inference_after(
                    session.head_seq,
                    token.as_ref().context("lease released")?,
                    &InflightRecord {
                        session_id: id,
                        seq: step,
                        provider: self.config.provider.clone(),
                        key_id: String::new(),
                    },
                    &job,
                    preceding,
                )
                .await?;
            *token = None;
            event
        };
        session.head_seq = event.seq();
        self.publish_events(id, preceding).await?;
        self.publish_events(id, std::slice::from_ref(&event))
            .await?;
        self.kill("after_request_event");
        self.kill("after_release");
        self.bus
            .publish_work(
                &WorkQueue::Inference(SubjectToken::new(&self.config.provider)?),
                &job,
            )
            .await?;
        Ok(())
    }

    async fn execute_pending(
        &self,
        session: &mut SessionRecord,
        lease: &ActiveLease,
        events: &mut Vec<Event>,
        turn: Option<MessageId>,
    ) -> Result<bool> {
        let mut jobs = Vec::new();
        let id = session.session_id;
        for (request_id, call) in pending_tools(events) {
            let result = match self.store.ensure_session_computer(id).await {
                Err(StoreError::ComputerDeleted) => Err(StoreError::ComputerDeleted.to_string()),
                Err(error) => return Err(error.into()),
                Ok(()) => match self.config.harness.tools.get(&call.tool) {
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
                    Some(_) if call.tool == "update_plan" => {
                        self.tool_stage(id, turn, TurnStage::ToolDispatched, request_id)
                            .await;
                        let event = {
                            let token = lease.lock().await;
                            self.store
                                .complete_plan_tool(
                                    id,
                                    session.head_seq,
                                    token.as_ref().context("lease released")?,
                                    request_id,
                                    &call,
                                )
                                .await?
                        };
                        session.head_seq = event.seq();
                        if let Ok(arguments) =
                            swarmy_core::UpdatePlanArguments::parse(call.arguments.clone())
                        {
                            session.plan = arguments.plan;
                        }
                        self.publish_events(id, std::slice::from_ref(&event))
                            .await?;
                        events.push(event);
                        self.tool_stage(id, turn, TurnStage::ToolCompleted, request_id)
                            .await;
                        continue;
                    }
                    Some(tool) => {
                        self.tool_stage(id, turn, TurnStage::ToolDispatched, request_id)
                            .await;
                        tool.execute(call.arguments).await
                    }
                    None => Err(format!("unknown tool: {}", call.tool)),
                },
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
        self.dispatch_pending(session, lease, jobs, turn).await?;
        Ok(true)
    }

    async fn dispatch_calls(
        &self,
        session: &SessionRecord,
        lease: &ActiveLease,
        calls: &[ToolCallRecord],
        turn: Option<MessageId>,
    ) -> Result<()> {
        for attempt in 0..2 {
            let placement = self
                .placements
                .resolve(&self.store, session.agent_id, self.config.placement_lease)
                .await?;
            let (events, jobs) = {
                let mut token = lease.lock().await;
                let dispatched = self
                    .store
                    .dispatch_tool_calls(
                        session.session_id,
                        session.head_seq,
                        token.as_ref().context("lease released")?,
                        calls,
                        &placement,
                    )
                    .await;
                let dispatched = match dispatched {
                    Err(StoreError::LeaseMismatch) if attempt == 0 => {
                        // Eviction may release a placement before its cached expiry.
                        // The failed transaction made no changes; resolve once again.
                        self.placements.invalidate(session.agent_id).await;
                        continue;
                    }
                    result => result?,
                };
                *token = None;
                dispatched
            };
            self.kill("after_release");
            self.publish_events(session.session_id, &events).await?;
            return self
                .publish_tools(session.session_id, &placement, jobs, turn)
                .await;
        }
        unreachable!("the second dispatch attempt returns its result")
    }

    async fn dispatch_pending(
        &self,
        session: &SessionRecord,
        lease: &ActiveLease,
        jobs: Vec<ToolJob>,
        turn: Option<MessageId>,
    ) -> Result<()> {
        for attempt in 0..2 {
            // Resolve before releasing the step, and persist the epoch with the jobs.
            let placement = self
                .placements
                .resolve(&self.store, session.agent_id, self.config.placement_lease)
                .await?;
            {
                let mut token = lease.lock().await;
                let result = self
                    .store
                    .dispatch_placed_tool_jobs(
                        session.session_id,
                        token.as_ref().context("lease released")?,
                        &jobs,
                        &placement,
                    )
                    .await;
                match result {
                    Err(StoreError::LeaseMismatch) if attempt == 0 => {
                        self.placements.invalidate(session.agent_id).await;
                        continue;
                    }
                    result => result?,
                }
                *token = None;
            }
            self.kill("after_release");
            return self
                .publish_tools(session.session_id, &placement, jobs, turn)
                .await;
        }
        unreachable!("the second dispatch attempt returns its result")
    }

    async fn publish_tools(
        &self,
        id: SessionId,
        placement: &swarmy_core::PlacementRecord,
        jobs: Vec<ToolJob>,
        turn: Option<MessageId>,
    ) -> Result<()> {
        // Dispatch already checked the placement and persisted its epoch with
        // every job. The node checks that fence again before executing. Only
        // recovery needs to resolve placement and repair a changed epoch.
        futures::future::try_join_all(jobs.into_iter().map(|job| async move {
            self.tool_stage(id, turn, TurnStage::ToolDispatched, job.request_id)
                .await;
            self.bus
                .publish_work(&WorkQueue::NodeTools(placement.node_id), &job)
                .await
        }))
        .await?;
        Ok(())
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
        if self.store.fail_deleted_computer_tool(job).await? {
            return Ok(());
        }
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
        Ok(())
    }

    async fn finish(
        &self,
        session: &mut SessionRecord,
        lease: &ActiveLease,
        snapshot: &Snapshot,
        events: &mut Vec<Event>,
        turn: Option<MessageId>,
    ) -> Result<()> {
        if self.summarize(session, lease, snapshot, events).await? {
            return Ok(());
        }
        let head = session
            .head_seq
            .checked_add(1)
            .context("sequence overflow")?;
        let idle = Event::StateChanged {
            seq: head,
            from: SessionState::Leased,
            to: SessionState::Idle,
        };
        events.push(idle);
        let bytes = encode(&snapshot.replay(events))?;
        let reference = SnapshotRef {
            object_key: format!("blobs/{}", blake3::hash(&bytes).to_hex()),
            seq: head,
        };
        self.blobs.put(&reference.object_key, bytes.into()).await?;
        self.kill("before_release");
        let event = {
            let mut token = lease.lock().await;
            let event = self
                .store
                .finish_turn(
                    session.session_id,
                    session.head_seq,
                    token.as_ref().context("lease released")?,
                    &reference,
                )
                .await?;
            *token = None;
            event
        };
        if let Some(turn) = turn {
            self.bus
                .record_turn(&Bus::turn_event(
                    session.session_id,
                    turn,
                    TurnStage::Idle,
                    None,
                ))
                .await;
        }
        self.publish_events(session.session_id, &[event]).await
    }

    async fn summary_completed(&self, session: &SessionRecord, events: &[Event]) -> Result<bool> {
        if !matches!(session.kind, swarmy_core::SessionKind::Named { .. }) {
            return Ok(false);
        }
        let Some(Event::InferenceCompleted { request_id, .. }) =
            events.iter().rev().find(|event| {
                matches!(
                    event,
                    Event::InferenceCompleted { .. } | Event::InferenceFailed { .. }
                )
            })
        else {
            return Ok(false);
        };
        Ok(self
            .store
            .get_inference_input::<InferenceJob>(*request_id)
            .await?
            .is_some_and(|job| job.request.system_prompt == swarmy_harness::SUMMARY_PROMPT))
    }

    async fn summarize(
        &self,
        session: &mut SessionRecord,
        lease: &ActiveLease,
        snapshot: &Snapshot,
        events: &[Event],
    ) -> Result<bool> {
        if !matches!(session.kind, swarmy_core::SessionKind::Named { .. }) {
            return Ok(false);
        }
        let Some(agent) = self.store.get_agent(session.agent_id).await? else {
            return Ok(false);
        };
        if agent.main_session != Some(session.session_id) {
            return Ok(false);
        }
        let Some((request_id, message)) = events.iter().rev().find_map(|event| match event {
            Event::InferenceCompleted {
                request_id,
                message,
                ..
            } => Some((*request_id, Some(message))),
            Event::InferenceFailed { request_id, .. } => Some((*request_id, None)),
            _ => None,
        }) else {
            return Ok(false);
        };
        let Some(job) = self
            .store
            .get_inference_input::<InferenceJob>(request_id)
            .await?
        else {
            return Ok(false);
        };
        if job.request.system_prompt == swarmy_harness::SUMMARY_PROMPT {
            return self.archive_summary(session, lease, message).await;
        }

        let Some(Ok(response)) = self
            .store
            .get_inference_result::<Result<swarmy_llm::Response, String>>(request_id)
            .await?
        else {
            return Ok(false);
        };
        let tokens = response
            .usage
            .input_tokens
            .saturating_add(response.usage.output_tokens);
        if tokens < self.config.summarize_at_tokens {
            return Ok(false);
        }
        let request = swarmy_llm::Request {
            system_prompt: swarmy_harness::SUMMARY_PROMPT.into(),
            messages: snapshot.replay(events).messages().to_vec(),
            tools: Vec::new(),
            settings: job.request.settings,
        };
        self.build_inference(session, lease, &[], request).await?;
        Ok(true)
    }

    async fn archive_summary(
        &self,
        session: &SessionRecord,
        lease: &ActiveLease,
        message: Option<&swarmy_core::Message>,
    ) -> Result<bool> {
        let Some(message) = message else {
            return Ok(false);
        };
        let text: String = message
            .parts
            .iter()
            .filter_map(|part| match part {
                swarmy_core::Part::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        let Ok(summary) = serde_json::from_str::<swarmy_core::ConversationSummary>(&text) else {
            tracing::warn!(session_id = %session.session_id, "invalid summary; retaining current session");
            return Ok(false);
        };
        let opening = swarmy_core::Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role: swarmy_core::MessageRole::System,
            parts: vec![swarmy_core::Part::Text {
                text: format!(
                    "Conversation summarized. Previous session: {}. Its full transcript remains readable.\n{}",
                    session.session_id,
                    serde_json::to_string(&summary)?
                ),
            }],
        };
        let mut token = lease.lock().await;
        let (_, archived) = self
            .store
            .summarize_main_session(
                session.session_id,
                session.head_seq,
                token.as_ref().context("lease released")?,
                &opening,
            )
            .await?;
        *token = None;
        self.publish_events(session.session_id, &[archived]).await?;
        Ok(true)
    }

    async fn memory(&self, agent: swarmy_core::AgentId) -> Result<String> {
        let Some(placement) = self
            .store
            .get_by_agent(agent)
            .await?
            .filter(|placement| placement.expires_at > Timestamp::now())
        else {
            return Ok(String::new());
        };
        let reply = self
            .bus
            .request_memory(
                placement.node_id,
                &swarmy_core::MemoryRequest {
                    agent_id: agent,
                    epoch: placement.epoch,
                    directory: self.config.memory_dir.clone(),
                    max_bytes: self.config.memory_max_bytes,
                },
            )
            .await?;
        reply.map_err(anyhow::Error::msg)
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
