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
use swarmy_llm::{InferenceJob, InferenceJobRef};
use swarmy_store::{
    FailoverAction, MAX_SCAN_LIMIT, Store, StoreError, blob::BlobStore, runnable_partition,
};
use tokio::{
    sync::Mutex,
    time::{Instant, MissedTickBehavior, interval, interval_at},
};
use ulid::Ulid;

use crate::config::Config;

type ActiveLease = Mutex<Option<Lease>>;

/// One inference attempt on a resolved route step: the provider and entry
/// the gateway must use, the route that selected them, and the step index
/// recorded on the completion for metering.
struct ResolvedAttempt {
    provider: String,
    entry: Option<String>,
    route: Option<String>,
    route_step: u32,
    snapshot: swarmy_store::RouteSnapshot,
}

impl Worker {
    async fn route_snapshot(&self, session: &SessionRecord) -> Result<swarmy_store::RouteSnapshot> {
        Ok(self
            .store
            .route_snapshot(
                session.agent_id,
                session.route.as_deref(),
                session.inference.provider.as_deref(),
                self.config.default_route.as_deref(),
                &self.config.provider,
                Timestamp::now(),
            )
            .await?)
    }
}

fn warn_on_route_fallback(session: &SessionRecord, resolved: Option<&str>, skipped: &[String]) {
    // A deleted or renamed route falls back to the implicit chain; say so
    // once per resolution so the operator can fix the assignment. A route
    // whose named steps are all unready falls back the same way, but the
    // route itself exists, so name the skipped steps instead.
    let requested = session.route.as_deref();
    if requested.is_some() && resolved != requested {
        if skipped.is_empty() {
            tracing::warn!(
                session_id = %session.session_id,
                route = requested,
                "assigned route is missing; using the implicit provider chain",
            );
        } else {
            tracing::warn!(
                session_id = %session.session_id,
                route = requested,
                skipped = skipped.join("; "),
                "assigned route has no usable step; using the implicit provider chain",
            );
        }
    }
}

pub struct Worker {
    store: Store,
    bus: Bus,
    blobs: Arc<dyn BlobStore>,
    config: Config,
    placements: crate::placement::Cache,
    snapshots: Mutex<HashMap<String, Snapshot>>,
    display_by_session: Mutex<HashMap<SessionId, bool>>,
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
            display_by_session: Mutex::default(),
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
            let event = Bus::turn_event(id, turn, swarmy_core::TurnStage::Claimed, None);
            self.bus.record_turn(&event).await;
            self.store.observe_turn_stage(event);
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
        let Some(snapshot) = self
            .prepare_step(&mut session, turn, lease, &mut events)
            .await?
        else {
            return Ok(());
        };
        loop {
            if self
                .interrupt_if_requested(&mut session, lease, &snapshot, &mut events, turn)
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
                .step(&session, &snapshot, &events, message_id)
            {
                Action::BuildInference(request) => {
                    return self
                        .build_inference(&mut session, lease, &[], request)
                        .await;
                }
                Action::DispatchTools(calls) => {
                    let display = self.session_display(&session).await?;
                    if self.store.ensure_session_computer(id).await.is_ok()
                        && calls
                            .iter()
                            .all(|call| self.can_dispatch_tool(call, display))
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

    async fn prepare_step(
        &self,
        session: &mut SessionRecord,
        turn: Option<MessageId>,
        lease: &ActiveLease,
        events: &mut Vec<Event>,
    ) -> Result<Option<Snapshot>> {
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

    async fn interrupt_if_requested(
        &self,
        session: &mut SessionRecord,
        lease: &ActiveLease,
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
            &[Event::InferenceFailed {
                seq: 0,
                request_id,
                error: "interrupted by operator".into(),
                retryable: false,
                retry_at: None,
            }],
        )
        .await?;
        session.interrupt_requested = true;
        self.store.clear_inference_wait(session.session_id).await?;
        self.finish(session, lease, snapshot, events, turn).await?;
        Ok(true)
    }

    async fn handle_inference_wait(
        &self,
        session: &mut SessionRecord,
        turn: Option<MessageId>,
        lease: &ActiveLease,
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
            };
            self.append(session, lease, events, &[terminal]).await?;
            self.store.clear_inference_wait(id).await?;
            self.finish(session, lease, snapshot, events, turn).await?;
            return Ok(true);
        }
        if let Some(Event::InferenceFailed {
            seq,
            error,
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
                    .failover_or_park(session, lease, *seq, error, *retry_at, now)
                    .await;
            }
        }
        Ok(false)
    }

    /// Failover happens only here, at the turn boundary between attempts: a
    /// retryable failure moves the session to the next usable step of its
    /// route for the next attempt, wrapping to a recovered earlier step when
    /// every later step is open. When every step is open the session waits
    /// for the earliest retry among them. The snapshot, the step move, and
    /// the wait write commit in one store transaction, matching the
    /// pre-routes park cost; unrouted sessions resolve the implicit chain
    /// of their provider's entries here even though their first attempt
    /// skipped the snapshot read.
    async fn failover_or_park(
        &self,
        session: &mut SessionRecord,
        lease: &ActiveLease,
        seq: u64,
        error: &str,
        retry_at: Timestamp,
        now: Timestamp,
    ) -> Result<bool> {
        let outcome = {
            let token = lease.lock().await;
            let lease_ref = token.as_ref().context("lease released")?;
            self.store
                .failover_route_step(
                    session.session_id,
                    lease_ref,
                    seq,
                    error,
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
                *lease.lock().await = None;
                Ok(true)
            }
            // The failure was already handled before a restart or lease
            // lapse: continue the turn without moving again or parking
            // behind the successor's in-flight request.
            FailoverAction::AlreadyHandled => Ok(false),
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
        // Fleet tasks run hundreds of tool rounds inside a single turn and
        // never reach turn end before the provider window fills, so compare
        // the last inference usage with the side threshold here, between the
        // folded tool results and the next request. A chat-shaped turn still
        // gets its check at turn end in `finish`.
        if self
            .maybe_summarize_mid_turn(session, lease, snapshot, events, &folded)
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

    fn can_dispatch_tool(&self, call: &ToolCallRecord, display: bool) -> bool {
        (!swarmy_tools::is_display_name(&call.tool) || display)
            && self
                .config
                .harness
                .tools
                .get(&call.tool)
                .is_some_and(swarmy_harness::Tool::sandbox_bound)
            && SandboxArguments::parse(&call.tool, call.arguments.clone()).is_ok()
    }

    async fn session_display(&self, session: &SessionRecord) -> Result<bool> {
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

    async fn prepare_request(
        &self,
        session: &SessionRecord,
        request: &mut swarmy_llm::Request,
        preceding: &mut Vec<Event>,
    ) -> Result<ResolvedAttempt> {
        // Resolve on every inference so existing sessions see later agent updates.
        // A summary request keeps its own prompt; every other request gets the agent's
        // prompt override first and then the memory directory and contents appended.
        // The agent and its route resolve in one transaction so an inference
        // costs no more store transactions than before routes.
        let summarizing = request.system_prompt == swarmy_harness::SUMMARY_PROMPT;
        let mut defaults = swarmy_core::ResolvedSelection {
            provider: self.config.provider.clone(),
            model: request.settings.model.clone(),
            effort: request
                .settings
                .reasoning_effort
                .unwrap_or(swarmy_core::ReasoningEffort::None),
        };
        if let swarmy_core::SessionKind::Named { agent_id } = session.kind {
            let (record, snapshot) = self
                .store
                .agent_and_route_snapshot(
                    agent_id,
                    session.route.as_deref(),
                    session.inference.provider.as_deref(),
                    self.config.default_route.as_deref(),
                    &self.config.provider,
                    Timestamp::now(),
                )
                .await?;
            if let Some(record) = record.as_ref() {
                defaults = record.inference().resolve(&defaults);
                if let Some(prompt) = record.system_prompt.clone()
                    && !summarizing
                {
                    request.system_prompt = prompt;
                }
            }
            warn_on_route_fallback(session, snapshot.name.as_deref(), &snapshot.skipped);
            let selection = session.inference.resolve(&defaults);
            return self
                .finish_prepare(
                    session,
                    request,
                    preceding,
                    selection,
                    snapshot,
                    summarizing,
                )
                .await;
        }
        let selection = session.inference.resolve(&defaults);
        // A first attempt without any route assignment uses the implicit
        // single-step chain and the gateway pool picks the entry, so it
        // skips the snapshot read entirely. Later attempts resolve: a
        // failover may have moved the chain, and only the snapshot knows
        // which step serves next.
        let snapshot = if session.needs_route_snapshot(self.config.default_route.as_deref()) {
            let snapshot = self.route_snapshot(session).await?;
            warn_on_route_fallback(session, snapshot.name.as_deref(), &snapshot.skipped);
            snapshot
        } else {
            swarmy_store::RouteSnapshot {
                name: None,
                steps: vec![swarmy_store::RouteStepStatus {
                    provider: selection.provider.clone(),
                    label: None,
                    model: None,
                    open_until: None,
                    reason: None,
                }],
                skipped: Vec::new(),
            }
        };
        self.finish_prepare(
            session,
            request,
            preceding,
            selection,
            snapshot,
            summarizing,
        )
        .await
    }

    /// Build the request against the resolved route step: model override,
    /// reasoning downgrade, and modality handling. Shared by named sessions,
    /// which resolve the agent and route together, and ephemeral sessions,
    /// which resolve the implicit chain.
    async fn finish_prepare(
        &self,
        session: &SessionRecord,
        request: &mut swarmy_llm::Request,
        preceding: &mut Vec<Event>,
        selection: swarmy_core::ResolvedSelection,
        snapshot: swarmy_store::RouteSnapshot,
        summarizing: bool,
    ) -> Result<ResolvedAttempt> {
        if !summarizing {
            apply_display_tools(request, self.session_display(session).await?);
        }
        let index = snapshot.pick_or_earliest(session.route_step);
        let step = snapshot.steps.get(index).context("empty route snapshot")?;
        let model_id = step
            .model
            .clone()
            .unwrap_or_else(|| selection.model.clone());
        request.settings.model.clone_from(&model_id);
        request.settings.reasoning_effort = Some(selection.effort);
        // Reasoning blocks replay only for the provider and model that
        // produced them; a failover step must not inherit another step's
        // signatures, so downgrade them here where the request is built.
        swarmy_llm::reasoning::downgrade_mismatched_reasoning(
            &mut request.messages,
            &step.provider,
            &model_id,
        );

        if let Some(model) = self.config.catalog.model(&step.provider, &model_id) {
            if !model
                .input_modalities
                .iter()
                .any(|modality| modality == "image")
            {
                omit_unsupported_images(request);
            }
            if model
                .input_modalities
                .iter()
                .any(|modality| modality == "image")
            {
                self.hydrate_images(request).await?;
            }
            let (effort, changed) = model.clamp_effort(selection.effort);
            request.settings.reasoning_effort = Some(effort);
            if changed && !self.has_effort_notice(session.session_id).await? {
                preceding.push(Event::MessageAppended {
                    seq: 0,
                    message: swarmy_core::Message {
                        id: MessageId::from_ulid(Ulid::generate()),
                        role: swarmy_core::MessageRole::System,
                        parts: vec![swarmy_core::Part::Text {
                            text: format!(
                                "Reasoning effort clamped from {} to {effort} for {}/{}",
                                selection.effort, step.provider, model_id
                            ),
                        }],
                    },
                });
            }
        }
        if !summarizing {
            request.system_prompt = request
                .system_prompt
                .replace("{memory_dir}", &self.config.memory_dir);
            if matches!(session.kind, swarmy_core::SessionKind::Named { .. }) {
                let memory = self.prompt_context(session.agent_id, false).await?;
                write!(
                    request.system_prompt,
                    "\n\nAgent memory ({}):\n{memory}",
                    self.config.memory_dir
                )?;
            }
            let instructions = self.prompt_context(session.agent_id, true).await?;
            if !instructions.is_empty() {
                write!(
                    request.system_prompt,
                    "\n\nRepository instructions (apply within each listed repository):\n{instructions}"
                )?;
            }
        }
        Ok(ResolvedAttempt {
            provider: step.provider.clone(),
            entry: step.label.clone(),
            route: snapshot.name.clone(),
            route_step: u32::try_from(index).unwrap_or(u32::MAX),
            snapshot,
        })
    }

    async fn hydrate_images(&self, request: &mut swarmy_llm::Request) -> Result<()> {
        for message in &mut request.messages {
            for part in &mut message.parts {
                if let swarmy_core::Part::Image {
                    bytes,
                    object_key: Some(key),
                    ..
                } = part
                    && bytes.is_empty()
                {
                    *bytes = self.blobs.get(key).await?.to_vec();
                }
            }
        }
        let mut expanded = Vec::with_capacity(request.messages.len());
        for message in request.messages.drain(..) {
            let mut images = Vec::new();
            for part in &message.parts {
                if let swarmy_core::Part::ToolResult {
                    result: swarmy_core::ToolResult::Completed { metadata, .. },
                    ..
                } = part
                    && let (Some(key), Some(media_type)) = (
                        metadata
                            .get("image_object_key")
                            .and_then(serde_json::Value::as_str),
                        metadata
                            .get("image_media_type")
                            .and_then(serde_json::Value::as_str),
                    )
                {
                    images.push(swarmy_core::Part::Image {
                        media_type: media_type.to_owned(),
                        bytes: self.blobs.get(key).await?.to_vec(),
                        object_key: Some(key.to_owned()),
                        detail: None,
                    });
                }
            }
            expanded.push(message);
            if !images.is_empty() {
                expanded.push(swarmy_core::Message {
                    id: MessageId::from_ulid(Ulid::generate()),
                    role: swarmy_core::MessageRole::User,
                    parts: images,
                });
            }
        }
        request.messages = expanded;
        Ok(())
    }

    async fn build_inference(
        &self,
        session: &mut SessionRecord,
        lease: &ActiveLease,
        preceding: &[Event],
        mut request: swarmy_llm::Request,
    ) -> Result<()> {
        let mut preceding = preceding.to_vec();
        let attempt = self
            .prepare_request(session, &mut request, &mut preceding)
            .await?;
        // Persist the picked step with the request so a retryable failure
        // advances from the attempt that actually ran, not from a stale
        // position. A wrap to a recovered earlier step carries no range.
        let route = (attempt.route_step != session.route_step).then(|| {
            let mut reasons = attempt.snapshot.skipped.clone();
            if attempt.route_step > session.route_step {
                for skipped in session.route_step..attempt.route_step {
                    if let Some(step) = attempt
                        .snapshot
                        .steps
                        .get(usize::try_from(skipped).unwrap_or(usize::MAX))
                        && let Some(reason) = &step.reason
                    {
                        reasons.push(reason.clone());
                    }
                }
            }
            swarmy_store::SubmitRouteStep {
                step: attempt.route_step,
                reasons,
            }
        });
        for (event, seq) in preceding.iter_mut().zip(session.head_seq + 1..) {
            event.set_seq(seq);
        }
        let id = session.session_id;
        let step = session
            .head_seq
            .checked_add(u64::try_from(preceding.len())?)
            .and_then(|head| head.checked_add(1))
            .context("sequence overflow")?;
        let job = InferenceJob {
            provider: attempt.provider,
            entry: attempt.entry,
            route: attempt.route,
            route_step: attempt.route_step,
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
                .submit_inference_after_with_request_and_route(
                    session.head_seq,
                    token.as_ref().context("lease released")?,
                    &InflightRecord {
                        session_id: id,
                        seq: step,
                        provider: job.provider.clone(),
                        key_id: job.entry.clone().unwrap_or_default(),
                    },
                    &job,
                    &job.request,
                    &preceding,
                    route,
                )
                .await?;
            *token = None;
            event
        };
        session.route_step = attempt.route_step;
        session.head_seq = event.seq();
        self.publish_events(id, &preceding).await?;
        self.publish_events(id, std::slice::from_ref(&event))
            .await?;
        self.kill("after_request_event");
        self.kill("after_release");
        if self.fail_unserved(&job).await? {
            return Ok(());
        }
        self.publish_inference(&job).await
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
        let display = self.session_display(session).await?;
        for (request_id, call) in pending_tools(events) {
            let result = match self.store.ensure_session_computer(id).await {
                Err(StoreError::ComputerDeleted) => Err(StoreError::ComputerDeleted.to_string()),
                Err(error) => return Err(error.into()),
                Ok(()) if swarmy_tools::is_display_name(&call.tool) && !display => {
                    Err("display tools require a display image".into())
                }
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
                    Some(_)
                        if matches!(
                            call.tool.as_str(),
                            "update_plan" | "set_timer" | "list_timers" | "cancel_timer"
                        ) =>
                    {
                        self.observe_dispatch(id, turn, request_id, &call.tool)
                            .await;
                        let event = self
                            .complete_store_tool(session, lease, request_id, &call)
                            .await?;
                        session.head_seq = event.seq();
                        if call.tool == "update_plan"
                            && let Ok(arguments) =
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
                        self.observe_dispatch(id, turn, request_id, &call.tool)
                            .await;
                        tool.execute(call.arguments).await
                    }
                    None => Err(format!("unknown tool: {}", call.tool)),
                },
            };
            let result = result
                .map(|output| swarmy_core::cap_tool_output(&call.tool, &call.call_id.0, output));
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

    async fn complete_store_tool(
        &self,
        session: &SessionRecord,
        lease: &ActiveLease,
        request_id: RequestId,
        call: &ToolCallRecord,
    ) -> Result<Event> {
        let token = lease.lock().await;
        let token = token.as_ref().context("lease released")?;
        let event = if call.tool == "update_plan" {
            self.store
                .complete_plan_tool(
                    session.session_id,
                    session.head_seq,
                    token,
                    request_id,
                    call,
                )
                .await?
        } else {
            self.store
                .complete_timer_tool(
                    session.session_id,
                    session.head_seq,
                    token,
                    request_id,
                    call,
                )
                .await?
        };
        Ok(event)
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
        // The tool name folds into the dispatch stage so each dispatch costs
        // one metrics transaction instead of two.
        futures::future::try_join_all(jobs.into_iter().map(|job| async move {
            self.observe_dispatch(id, turn, job.request_id, job.arguments.name())
                .await;
            self.bus
                .publish_work(&WorkQueue::NodeTools(placement.node_id), &job)
                .await
        }))
        .await?;
        Ok(())
    }

    /// Fold the tool name into the dispatch stage it already writes.
    async fn observe_dispatch(
        &self,
        id: SessionId,
        turn: Option<MessageId>,
        request: RequestId,
        name: &str,
    ) {
        if let Some(turn) = turn {
            let event = Bus::turn_event(id, turn, TurnStage::ToolDispatched, Some(request));
            self.bus.record_turn(&event).await;
            self.store
                .observe_turn_metrics(id, turn, swarmy_store::dispatch_patches(event, name));
        }
    }

    async fn tool_stage(
        &self,
        id: SessionId,
        turn: Option<MessageId>,
        stage: swarmy_core::TurnStage,
        request: RequestId,
    ) {
        if let Some(turn) = turn {
            let event = Bus::turn_event(id, turn, stage, Some(request));
            self.bus.record_turn(&event).await;
            self.store.observe_turn_stage(event);
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
                let event = Bus::turn_event(
                    job.session_id,
                    turn,
                    swarmy_core::TurnStage::ToolDispatched,
                    Some(job.request_id),
                );
                self.bus.record_turn(&event).await;
                self.store.observe_turn_metrics(
                    job.session_id,
                    turn,
                    swarmy_store::dispatch_patches(event, job.arguments.name()),
                );
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
                        provider: self.job_provider(job).to_owned(),
                        key_id: job.entry.clone().unwrap_or_default(),
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
        if self.fail_unserved(job).await? {
            return Ok(());
        }
        self.publish_inference(job).await
    }

    async fn publish_inference(&self, job: &InferenceJob) -> Result<()> {
        let published = self
            .bus
            .publish_work(
                &WorkQueue::Inference(SubjectToken::new(self.job_provider(job))?),
                &InferenceJobRef::from(job),
            )
            .await;
        if let Err(error) = published {
            if error.permanent_publish_failure() {
                self.fail_publication(job, &error).await?;
            } else {
                return Err(error.into());
            }
        }
        Ok(())
    }

    async fn fail_publication(&self, job: &InferenceJob, error: &swarmy_bus::Error) -> Result<()> {
        let now = Timestamp::now();
        let claim = swarmy_store::InferenceClaim {
            session_id: job.session_id,
            request_id: job.request_id,
            owner: LeaseOwnerId::from_ulid(Ulid::generate()),
            expires_at: now.checked_add(std::time::Duration::from_secs(30))?,
        };
        if self.store.start_inference(&claim, now).await? {
            let session = self
                .store
                .fetch_session(job.session_id)
                .await?
                .context("session missing")?;
            let event = Event::InferenceFailed {
                seq: session
                    .head_seq
                    .checked_add(1)
                    .context("sequence overflow")?,
                request_id: job.request_id,
                error: error.to_string(),
                retryable: false,
                retry_at: None,
            };
            if self
                .store
                .complete_inference(
                    &swarmy_store::InferenceCompletion {
                        claim,
                        expected_head: session.head_seq,
                        event: event.clone(),
                        now,
                        entry: None,
                        entry_kind: None,
                        quota_remaining: std::collections::BTreeMap::new(),
                        quota_resets: std::collections::BTreeMap::new(),
                    },
                    &(),
                )
                .await?
            {
                self.publish_events(job.session_id, &[event]).await?;
            }
        }
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
        if !session.interrupt_requested && self.summarize(session, lease, snapshot, events).await? {
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
                    *token = None;
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
                        &[Event::InferenceFailed {
                            seq: 0,
                            request_id,
                            error: "interrupted by operator".into(),
                            retryable: false,
                            retry_at: None,
                        }],
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

    /// Turn-end summarization check for named sessions. Main sessions
    /// compare total tokens; side sessions compare input tokens and warn
    /// first. Fleet-shaped turns are checked mid-turn in `fold_results`; this
    /// stays for chat-shaped sessions that end their turn without tools.
    async fn summarize(
        &self,
        session: &mut SessionRecord,
        lease: &ActiveLease,
        snapshot: &Snapshot,
        events: &mut Vec<Event>,
    ) -> Result<bool> {
        if !matches!(session.kind, swarmy_core::SessionKind::Named { .. }) {
            return Ok(false);
        }
        let Some(agent) = self.store.get_agent(session.agent_id).await? else {
            return Ok(false);
        };
        let is_main = agent.main_session == Some(session.session_id);
        let Some((_request_id, job, response, message)) = self.last_inference(events).await? else {
            return Ok(false);
        };
        if job.request.system_prompt == swarmy_harness::SUMMARY_PROMPT {
            return self
                .archive_summary(session, lease, snapshot, message.as_ref(), events)
                .await;
        }
        if is_main {
            let tokens = response
                .usage
                .input_tokens
                .saturating_add(response.usage.output_tokens);
            if self
                .config
                .summarization_threshold(self.job_provider(&job), &job.request.settings.model)
                .is_none_or(|threshold| tokens < threshold)
            {
                return Ok(false);
            }
        } else {
            let provider = self.job_provider(&job).to_owned();
            let model = job.request.settings.model.clone();
            let threshold = self.config.side_summarization_threshold(&provider, &model);
            let input = response.usage.input_tokens;
            if input >= threshold {
                // Fall through to the shared summary request below.
            } else {
                let pressure = self.config.side_pressure_threshold(&provider, &model);
                if input >= pressure && !pressure_warned(snapshot, events) {
                    self.emit_pressure(session, lease, events, input, threshold)
                        .await?;
                }
                return Ok(false);
            }
        }
        self.issue_summary(session, lease, snapshot, events, &job, &[])
            .await
    }

    /// Mid-turn summarization check for side sessions, called after tool
    /// results are folded and before the next inference request is built. A
    /// fleet task is one user prompt followed by hundreds of tool rounds
    /// inside a single turn, so without this check the session grows until
    /// the provider rejects it. The folded results travel as the preceding
    /// events of the summary request, so no tool output is lost.
    ///
    /// Hot path: folds below the pressure level do no store reads. The last
    /// `InferenceCompleted` event already carries usage, provider, and model,
    /// so the threshold check runs on the history the worker holds. Only
    /// folds at or above pressure touch the store (agent lookup, and the
    /// summary input and result when the threshold is crossed).
    async fn maybe_summarize_mid_turn(
        &self,
        session: &mut SessionRecord,
        lease: &ActiveLease,
        snapshot: &Snapshot,
        events: &mut Vec<Event>,
        folded: &Event,
    ) -> Result<bool> {
        if !matches!(session.kind, swarmy_core::SessionKind::Named { .. }) {
            return Ok(false);
        }
        let Some((mut provider, mut model, input)) = last_side_usage(events) else {
            return Ok(false);
        };
        if provider.is_empty() {
            provider = session
                .inference
                .provider
                .clone()
                .unwrap_or_else(|| self.config.provider.clone());
        }
        if model.is_empty() {
            model = session
                .inference
                .model
                .clone()
                .unwrap_or_else(|| self.config.harness.settings.model.clone());
        }
        let pressure = self.config.side_pressure_threshold(&provider, &model);
        if input < pressure {
            return Ok(false);
        }
        if self
            .store
            .get_agent(session.agent_id)
            .await?
            .is_some_and(|agent| agent.main_session == Some(session.session_id))
        {
            return Ok(false);
        }
        let Some((_request_id, job, response, _message)) = self.last_inference(events).await?
        else {
            return Ok(false);
        };
        if job.request.system_prompt == swarmy_harness::SUMMARY_PROMPT {
            return Ok(false);
        }
        // Re-resolve in case the event predates an agent model update; the
        // usage above already gated the slow path, so this repeat is rare.
        let provider = self.job_provider(&job).to_owned();
        let model = job.request.settings.model.clone();
        let threshold = self.config.side_summarization_threshold(&provider, &model);
        let pressure = self.config.side_pressure_threshold(&provider, &model);
        let input = response.usage.input_tokens;
        if input >= threshold {
            return self
                .issue_summary(
                    session,
                    lease,
                    snapshot,
                    events,
                    &job,
                    std::slice::from_ref(folded),
                )
                .await;
        }
        if input >= pressure && !pressure_warned(snapshot, events) {
            self.emit_pressure(session, lease, events, input, threshold)
                .await?;
        }
        Ok(false)
    }

    /// Last stored inference input and output for the session tail.
    async fn last_inference(
        &self,
        events: &[Event],
    ) -> Result<
        Option<(
            RequestId,
            InferenceJob,
            swarmy_llm::Response,
            Option<swarmy_core::Message>,
        )>,
    > {
        let Some((request_id, message)) = events.iter().rev().find_map(|event| match event {
            Event::InferenceCompleted {
                request_id,
                message,
                ..
            } => Some((*request_id, Some(message.clone()))),
            Event::InferenceFailed { request_id, .. } => Some((*request_id, None)),
            _ => None,
        }) else {
            return Ok(None);
        };
        let Some(job) = self
            .store
            .get_inference_input::<InferenceJob>(request_id)
            .await?
        else {
            return Ok(None);
        };
        let Some(Ok(response)) = self
            .store
            .get_inference_result::<Result<swarmy_llm::Response, String>>(request_id)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some((request_id, job, response, message)))
    }

    /// Build the summary inference for the replayed history plus any
    /// preceding events (mid-turn folded tool results), and submit it with
    /// those events in one transaction.
    async fn issue_summary(
        &self,
        session: &mut SessionRecord,
        lease: &ActiveLease,
        snapshot: &Snapshot,
        events: &[Event],
        job: &InferenceJob,
        preceding: &[Event],
    ) -> Result<bool> {
        let mut history = snapshot.replay(events).messages().to_vec();
        for event in preceding {
            if let Event::MessageAppended { message, .. } = event {
                history.push(message.clone());
            }
        }
        let request = summary_request(
            &self.config,
            self.job_provider(job),
            &history,
            job.request.settings.clone(),
        );
        if !summary_fits(&self.config, &request, self.job_provider(job)) {
            tracing::warn!(
                session_id = %session.session_id,
                "summary prompt exceeds the model window; retaining current session"
            );
            return Ok(false);
        }
        self.build_inference(session, lease, preceding, request)
            .await?;
        Ok(true)
    }

    /// Append a `context_pressure` warning when input usage passes 75
    /// percent of the side threshold, so fleet status sees it coming.
    /// Callers check `pressure_warned` first; the append itself is best
    /// effort and never fails the step.
    async fn emit_pressure(
        &self,
        session: &mut SessionRecord,
        lease: &ActiveLease,
        events: &mut Vec<Event>,
        input_tokens: u64,
        threshold: u64,
    ) -> Result<()> {
        let message = swarmy_core::Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role: swarmy_core::MessageRole::System,
            parts: vec![swarmy_core::Part::Text {
                text: format!(
                    "context_pressure: input {input_tokens} tokens at 75 percent of the {threshold} token side-session threshold. Summarization will archive this session soon; push work to keep it safe."
                ),
            }],
        };
        if let Err(error) = self
            .append(
                session,
                lease,
                events,
                &[Event::MessageAppended { seq: 0, message }],
            )
            .await
        {
            tracing::warn!(
                session_id = %session.session_id,
                %error,
                "context_pressure append failed"
            );
            return Err(error);
        }
        Ok(())
    }

    async fn archive_summary(
        &self,
        session: &SessionRecord,
        lease: &ActiveLease,
        snapshot: &Snapshot,
        message: Option<&swarmy_core::Message>,
        events: &[Event],
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
        let is_main = self
            .store
            .get_agent(session.agent_id)
            .await?
            .is_some_and(|agent| agent.main_session == Some(session.session_id));
        let mut token = lease.lock().await;
        let (_, archived) = if is_main {
            self.store
                .summarize_main_session(
                    session.session_id,
                    session.head_seq,
                    token.as_ref().context("lease released")?,
                    &opening,
                )
                .await?
        } else {
            // Carry recent tool rounds forward so the successor keeps
            // immediate context alongside the summary; the next request stays
            // small. A mid-task rollover ends with a synthetic continue note
            // so the turn continues in the successor with the same pending
            // user intent; a chat-shaped rollover waits for input.
            let history = snapshot.replay(events).messages().to_vec();
            let tail = Self::side_successor_tail(&history, message);
            let (successor, archived_event) = self
                .store
                .summarize_side_session(
                    session.session_id,
                    session.head_seq,
                    token.as_ref().context("lease released")?,
                    &opening,
                    &tail,
                )
                .await?;
            *token = None;
            self.publish_events(session.session_id, &[archived_event])
                .await?;
            // The successor shares the old session's runnable partition; wake
            // The successor shares the old session's runnable partition; wake
            // it so a mid-task rollover continues without waiting for input.
            // A chat-shaped successor replays to end-of-turn and idles again.
            self.wake_successor(session.session_id, successor).await;
            return Ok(true);
        };
        *token = None;
        self.publish_events(session.session_id, &[archived]).await?;
        Ok(true)
    }
    /// Verbatim tail for a side successor from the replayed history: recent
    /// tool rounds plus a synthetic continue note for a mid-task rollover
    /// (tool results still waiting for their next inference). The summary
    /// output itself is the opening, not tail.
    fn side_successor_tail(
        history: &[swarmy_core::Message],
        summary: &swarmy_core::Message,
    ) -> Vec<swarmy_core::Message> {
        let mut tail = select_side_tail(history);
        tail.retain(|kept| kept.id != summary.id);
        if history
            .iter()
            .rev()
            .find(|kept| kept.id != summary.id)
            .is_some_and(|previous| previous.role == swarmy_core::MessageRole::Tool)
        {
            tail.push(continue_message());
        }
        tail
    }

    /// Wake a fresh successor so a mid-task rollover continues without
    /// waiting for input. Workers only claim runnable steps, so the session
    /// is marked runnable first: a bare nudge to an idle session is dropped.
    async fn wake_successor(&self, previous: SessionId, successor: SessionId) {
        if let Err(error) = self.store.wake_session(successor, Timestamp::now()).await {
            tracing::warn!(
                session_id = %previous,
                successor_id = %successor,
                %error,
                "successor wake failed; the scheduler scan still picks it up"
            );
            return;
        }
        if let Err(error) = self
            .bus
            .publish_work(
                &WorkQueue::Runnable(runnable_partition(successor)),
                &Nudge {
                    session_id: successor,
                },
            )
            .await
        {
            tracing::warn!(
                session_id = %previous,
                successor_id = %successor,
                %error,
                "successor wake failed; the scheduler scan still picks it up"
            );
        }
    }

    async fn prompt_context(
        &self,
        agent: swarmy_core::AgentId,
        instructions: bool,
    ) -> Result<String> {
        let Some(placement) = self
            .store
            .get_by_agent(agent)
            .await?
            .filter(|placement| placement.expires_at > Timestamp::now())
        else {
            return Ok(String::new());
        };
        let request = swarmy_core::MemoryRequest {
            agent_id: agent,
            epoch: placement.epoch,
            directory: if instructions {
                "/home/agent/work".into()
            } else {
                self.config.memory_dir.clone()
            },
            max_bytes: if instructions {
                32_768
            } else {
                self.config.memory_max_bytes
            },
        };
        let reply = if instructions {
            self.bus
                .request_instructions(placement.node_id, &request)
                .await?
        } else {
            self.bus.request_memory(placement.node_id, &request).await?
        };
        reply.map_err(anyhow::Error::msg)
    }

    fn job_provider<'a>(&'a self, job: &'a InferenceJob) -> &'a str {
        if job.provider.is_empty() {
            &self.config.provider
        } else {
            &job.provider
        }
    }

    async fn has_effort_notice(&self, id: SessionId) -> Result<bool> {
        let mut after = 0;
        loop {
            let events = self.store.read_events(id, after, MAX_SCAN_LIMIT).await?;
            if events.is_empty() {
                return Ok(false);
            }
            for event in events {
                after = event.seq();
                if let Event::MessageAppended { message, .. } = event
                    && message.role == swarmy_core::MessageRole::System
                    && message.parts.iter().any(|part| matches!(part, swarmy_core::Part::Text { text } if text.starts_with("Reasoning effort clamped from "))) {
                    return Ok(true);
                }
            }
        }
    }

    async fn fail_unserved(&self, job: &InferenceJob) -> Result<bool> {
        let provider = self.job_provider(job);
        let retryable = self.config.catalog.provider(provider).is_some()
            && self
                .config
                .allowed_providers
                .as_ref()
                .is_none_or(|allowed| allowed.iter().any(|id| id == provider));
        // The scripted provider needs no credentials; the gateway task advertises real providers.
        if retryable && (provider == "fake" || self.store.gateway_serves(provider).await?) {
            return Ok(false);
        }
        let now = Timestamp::now();
        let claim = swarmy_store::InferenceClaim {
            session_id: job.session_id,
            request_id: job.request_id,
            owner: LeaseOwnerId::from_ulid(Ulid::generate()),
            expires_at: now.checked_add(std::time::Duration::from_secs(30))?,
        };
        if self.store.start_inference(&claim, now).await? {
            let session = self
                .store
                .fetch_session(job.session_id)
                .await?
                .context("session missing")?;
            let event = Event::InferenceFailed {
                seq: session
                    .head_seq
                    .checked_add(1)
                    .context("sequence overflow")?,
                request_id: job.request_id,
                error: format!(
                    "no gateway serves provider {provider}; run swarmy auth set {provider} or start a gateway with it"
                ),
                retryable,
                retry_at: retryable
                    .then(|| now.checked_add(self.config.gateway_wait))
                    .transpose()?,
            };
            let committed = self
                .store
                .complete_inference(
                    &swarmy_store::InferenceCompletion {
                        claim,
                        expected_head: session.head_seq,
                        event: event.clone(),
                        now,
                        entry: None,
                        entry_kind: None,
                        quota_remaining: std::collections::BTreeMap::new(),
                        quota_resets: std::collections::BTreeMap::new(),
                    },
                    &(),
                )
                .await?;
            if committed {
                self.publish_events(job.session_id, std::slice::from_ref(&event))
                    .await?;
                if let Some(turn) = job
                    .request
                    .messages
                    .iter()
                    .rev()
                    .find(|message| message.role == swarmy_core::MessageRole::User)
                    .map(|message| message.id)
                {
                    self.store.observe_turn_metric(
                        job.session_id,
                        turn,
                        swarmy_store::MetricPatch::Wait {
                            request_id: job.request_id.to_string(),
                            kind: swarmy_store::WaitKind::MissingGateway,
                        },
                    );
                    if !retryable && let Event::InferenceFailed { error, .. } = event {
                        self.store.observe_turn_metric(
                            job.session_id,
                            turn,
                            swarmy_store::MetricPatch::Error(error),
                        );
                    }
                }
            }
        }
        Ok(true)
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
            if self.fail_unserved(&job).await? {
                return Ok(());
            }
            self.store_missing_request(&job).await?;
            let published = self
                .bus
                .publish_work(
                    &WorkQueue::Inference(SubjectToken::new(&record.provider)?),
                    &InferenceJobRef::from(&job),
                )
                .await;
            if let Err(error) = published {
                if error.permanent_publish_failure() {
                    self.fail_publication(&job, &error).await?;
                } else {
                    return Err(error.into());
                }
            }
        }
        Ok(())
    }
}

impl Worker {
    /// Jobs published inline before requests were stored by reference have an
    /// input but no request row. Store it so the gateway can resolve the
    /// reference, unless the request already completed and was cleared.
    async fn store_missing_request(&self, job: &InferenceJob) -> Result<()> {
        if self
            .store
            .get_inference_request::<swarmy_llm::Request>(job.request_id)
            .await?
            .is_some()
        {
            return Ok(());
        }
        let completed = self
            .store
            .get_idempotency(job.request_id)
            .await?
            .is_some_and(|record| record.state == swarmy_core::IdempotencyState::Completed);
        if completed {
            return Ok(());
        }
        tracing::info!(request_id = %job.request_id, "storing request for inline job");
        self.store
            .put_inference_request(job.request_id, &job.request)
            .await?;
        Ok(())
    }
}

fn fold_id(id: SessionId, step: u64) -> MessageId {
    let request = RequestId::for_step(id, step);
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&request.as_bytes()[..16]);
    MessageId::from_ulid(Ulid::from_bytes(bytes))
}

/// Recent context kept verbatim in a side successor, in tokens. Twenty
/// thousand covers a few tool-heavy turns; the summary plus this tail plus a
/// new turn stays far under the 400k default threshold.
const SIDE_TAIL_BUDGET_TOKENS: u64 = 20_000;

/// Summary output cap in tokens. Four thousand fits the structured goals,
/// state, questions, and facts format with file paths and identifiers.
const SUMMARY_OUTPUT_TOKENS: u64 = 4_096;

/// Rough token estimate for one message, chars divided by four like the Pi
/// and `OpenCode` heuristics. Images count as a fixed 4,800 chars.
fn estimate_message_tokens(message: &swarmy_core::Message) -> u64 {
    let mut chars = 0;
    for part in &message.parts {
        chars += match part {
            swarmy_core::Part::Text { text } | swarmy_core::Part::Reasoning { text, .. } => {
                text.len()
            }
            swarmy_core::Part::ToolCall { tool, input, .. } => {
                tool.len() + serde_json::to_string(input).map_or(0, |json| json.len())
            }
            swarmy_core::Part::ToolResult { result, .. } => match result {
                swarmy_core::ToolResult::Completed { output, .. } => output.len(),
                swarmy_core::ToolResult::Error { error } => error.len(),
            },
            swarmy_core::Part::Image { .. } => 4_800,
        };
    }
    chars.div_ceil(4) as u64
}

/// Recent tail for a side successor: whole tool rounds up to the token
/// budget, cut only between a completed tool result and the next assistant
/// message. A tool call never separates from its result, and reasoning parts
/// stay with their assistant message because messages are never split. At
/// least the last complete tool round is kept even when it alone exceeds the
/// budget; when no round boundary fits, only the last assistant message is
/// kept. The tail never fails: an oversized history still rolls over.
fn select_side_tail(messages: &[swarmy_core::Message]) -> Vec<swarmy_core::Message> {
    use swarmy_core::MessageRole::Assistant;
    // A stale pressure warning belongs to the archived session; the successor
    // warns again on its own usage, so drop it from the carried tail.
    let messages: Vec<swarmy_core::Message> = messages
        .iter()
        .filter(|message| !is_pressure_warning(message))
        .cloned()
        .collect();
    let messages = messages.as_slice();
    if messages.is_empty() {
        return Vec::new();
    }
    let mut total = 0;
    let mut start = messages.len();
    for (index, message) in messages.iter().enumerate().rev() {
        total += estimate_message_tokens(message);
        start = index;
        if total >= SIDE_TAIL_BUDGET_TOKENS {
            break;
        }
    }
    // Snap the cut forward to a safe boundary: the start, or just after a
    // tool-result message before an assistant message.
    let mut cut = start;
    while cut < messages.len() && !is_safe_tail_cut(messages, cut) {
        cut += 1;
    }
    // The budget bounds what comes before the last round, never the round
    // itself: a single oversized tool result still travels with its assistant
    // call instead of orphaning the call with a synthetic error.
    let round = last_tool_round(messages);
    if cut >= messages.len() {
        if let Some(round) = round {
            return messages[round..].to_vec();
        }
        return messages
            .iter()
            .rev()
            .find(|message| message.role == Assistant)
            .cloned()
            .map_or_else(Vec::new, |message| vec![message]);
    }
    // Keep at least the last complete tool round: the latest assistant
    // message carrying tool calls plus the tool results after it.
    if let Some(round) = round
        && cut > round
    {
        cut = round;
    }
    messages[cut..].to_vec()
}

/// A tail cut is safe at the start, or between a completed tool result and
/// the next assistant message. Cuts never land between a tool call and its
/// result, and never inside an assistant message that carries reasoning.
fn is_safe_tail_cut(messages: &[swarmy_core::Message], cut: usize) -> bool {
    use swarmy_core::MessageRole::{Assistant, Tool};
    cut == 0 || (messages[cut - 1].role == Tool && messages[cut].role == Assistant)
}

/// Start of the last complete tool round, if any: the latest assistant
/// message with tool calls that has tool results after it.
fn last_tool_round(messages: &[swarmy_core::Message]) -> Option<usize> {
    use swarmy_core::MessageRole::Tool;
    let round = messages.iter().rposition(|message| {
        message
            .parts
            .iter()
            .any(|part| matches!(part, swarmy_core::Part::ToolCall { .. }))
    })?;
    messages
        .iter()
        .skip(round + 1)
        .any(|message| message.role == Tool)
        .then_some(round)
}

/// Whether a message is a `context_pressure` warning.
fn is_pressure_warning(message: &swarmy_core::Message) -> bool {
    message.role == swarmy_core::MessageRole::System
        && message.parts.iter().any(|part| match part {
            swarmy_core::Part::Text { text } => text.contains("context_pressure"),
            _ => false,
        })
}

/// Whether the replayed conversation already carries a `context_pressure`
/// warning. The event tail starts after `snapshot_ref`, which advances every
/// turn, so scanning only the tail would warn once per turn instead of once
/// per session (and once more per successor, which starts a fresh session).
fn pressure_warned(snapshot: &Snapshot, events: &[Event]) -> bool {
    snapshot
        .replay(events)
        .messages()
        .iter()
        .any(is_pressure_warning)
}

/// Synthetic user note that ends a mid-task successor so the turn continues
/// with the same pending intent: replaying it reaches the ready phase and
/// the worker builds the next inference from the summary plus the tail.
fn continue_message() -> swarmy_core::Message {
    use swarmy_core::{MessageId, MessageRole, Part};
    swarmy_core::Message {
        id: MessageId::from_ulid(Ulid::generate()),
        role: MessageRole::User,
        parts: vec![Part::Text {
            text: "Continue the summarized task from the recent tool context above. Do the next steps, or finish with a concise status when nothing remains."
                .into(),
        }],
    }
}

/// Summary request with bounded output: the smaller of the model's output
/// limit and the structured-summary cap, so the request fits providers that
/// reject oversized max-token values.
fn summary_request(
    config: &crate::config::Config,
    provider: &str,
    messages: &[swarmy_core::Message],
    settings: swarmy_llm::GenerationSettings,
) -> swarmy_llm::Request {
    let mut settings = settings;
    let cap = config
        .catalog
        .model(provider, &settings.model)
        .and_then(|model| model.limit.output)
        .map_or(SUMMARY_OUTPUT_TOKENS, |limit| {
            limit.min(SUMMARY_OUTPUT_TOKENS)
        });
    settings.max_output_tokens = Some(cap);
    swarmy_llm::Request {
        system_prompt: swarmy_harness::SUMMARY_PROMPT.into(),
        messages: messages.to_vec(),
        tools: Vec::new(),
        settings,
    }
}

/// Whether the summary request fits the model window. The estimate covers
/// the replayed messages plus the prompt template, reserving the bounded
/// output. Unknown windows skip the check; the early threshold keeps the
/// prompt small in practice.
fn summary_fits(
    config: &crate::config::Config,
    request: &swarmy_llm::Request,
    provider: &str,
) -> bool {
    let Some(context) = config
        .catalog
        .model(provider, &request.settings.model)
        .map(|model| model.limit.context)
        .or(config.model_context_window_tokens)
    else {
        return true;
    };
    let output = request.settings.max_output_tokens.unwrap_or(0);
    let mut chars: u64 = u64::try_from(swarmy_harness::SUMMARY_PROMPT.len()).unwrap_or(u64::MAX);
    for message in &request.messages {
        chars = chars.saturating_add(estimate_message_tokens(message).saturating_mul(4));
    }
    let estimated = chars.div_ceil(4);
    estimated.saturating_add(output) <= context
}

/// Last inference usage from the events the worker already holds, with no
/// store reads. The gateway records provider, model, and usage on every
/// `InferenceCompleted` event. The mid-turn hot path uses this so folds below
/// pressure need no transaction and no read; only folds at or above pressure
/// touch the store.
fn last_side_usage(events: &[Event]) -> Option<(String, String, u64)> {
    events.iter().rev().find_map(|event| match event {
        Event::InferenceCompleted {
            provider,
            model,
            usage,
            ..
        } => Some((provider.clone(), model.clone(), usage.input_tokens)),
        _ => None,
    })
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

fn apply_display_tools(request: &mut swarmy_llm::Request, display: bool) {
    if display {
        request.system_prompt.push_str("\n\n");
        request.system_prompt.push_str(swarmy_tools::DISPLAY_PROMPT);
    } else {
        request
            .tools
            .retain(|tool| !swarmy_tools::is_display_name(&tool.name));
    }
}

fn omit_unsupported_images(request: &mut swarmy_llm::Request) {
    for message in &mut request.messages {
        for part in &mut message.parts {
            if matches!(part, swarmy_core::Part::Image { .. }) {
                *part = swarmy_core::Part::Text {
                    text: "[An image was omitted because this model does not accept images.]"
                        .into(),
                };
            } else if let swarmy_core::Part::ToolResult {
                result:
                    swarmy_core::ToolResult::Completed {
                        output, metadata, ..
                    },
                ..
            } = part
                && metadata.contains_key("image_object_key")
            {
                output
                    .push_str(" [An image was omitted because this model does not accept images.]");
                metadata.remove("image_object_key");
                metadata.remove("image_media_type");
            }
        }
    }
}

#[cfg(test)]
mod image_tests {
    use super::*;
    use swarmy_core::{Message, MessageRole, Part};

    #[test]
    fn display_image_controls_tool_schema_and_prompt() {
        use swarmy_llm::ToolDefinition;
        let request = || swarmy_llm::Request {
            system_prompt: "Base prompt".into(),
            messages: Vec::new(),
            tools: ["bash", "browser_snapshot", "screen_screenshot"]
                .into_iter()
                .map(|name| ToolDefinition {
                    name: name.into(),
                    description: String::new(),
                    parameters: serde_json::json!({"type":"object","properties":{}}),
                })
                .collect(),
            settings: swarmy_llm::GenerationSettings::default(),
        };
        let mut coding = request();
        apply_display_tools(&mut coding, false);
        assert_eq!(
            coding
                .tools
                .iter()
                .map(|tool| tool.name.as_str())
                .collect::<Vec<_>>(),
            ["bash"]
        );
        assert_eq!(coding.system_prompt, "Base prompt");
        let mut desktop = request();
        apply_display_tools(&mut desktop, true);
        assert_eq!(desktop.tools.len(), 3);
        assert!(desktop.system_prompt.contains("prefer browser_snapshot"));
    }

    #[test]
    fn unsupported_model_gets_a_note_instead_of_image_bytes() {
        let mut request = swarmy_llm::Request {
            system_prompt: String::new(),
            messages: vec![Message {
                id: MessageId::from_ulid(Ulid::nil()),
                role: MessageRole::User,
                parts: vec![Part::Image {
                    media_type: "image/png".into(),
                    bytes: vec![1, 2, 3],
                    object_key: None,
                    detail: None,
                }],
            }],
            tools: Vec::new(),
            settings: swarmy_llm::GenerationSettings::default(),
        };
        request.messages[0].parts.push(Part::ToolResult {
            call_id: swarmy_core::ToolCallId("shot".into()),
            result: swarmy_core::ToolResult::Completed {
                title: "browser_screenshot".into(),
                output: "PNG screenshot".into(),
                metadata: std::collections::BTreeMap::from([
                    ("image_object_key".into(), serde_json::json!("blob")),
                    ("image_media_type".into(), serde_json::json!("image/png")),
                ]),
            },
        });
        omit_unsupported_images(&mut request);
        assert!(
            matches!(&request.messages[0].parts[0], Part::Text { text } if text.contains("image was omitted"))
        );
        assert!(
            matches!(&request.messages[0].parts[1], Part::ToolResult { result: swarmy_core::ToolResult::Completed { output, metadata, .. }, .. } if output.contains("image was omitted") && !metadata.contains_key("image_object_key"))
        );
    }
}

#[cfg(test)]
mod tool_output_tests {
    #[test]
    fn small_output_passes_through_unchanged() {
        let output = "hello".to_owned();
        assert_eq!(
            swarmy_core::cap_tool_output("grep", "call_small", output.clone()),
            output
        );
    }

    #[test]
    fn huge_sandbox_result_is_capped_with_spill_marker() {
        let tool = "process_list";
        let call_id = "call_01HUGE";
        let head = "HEAD-MARKER-";
        let tail = "-TAIL-MARKER";
        let mut original = String::with_capacity(1024 * 1024);
        original.push_str(head);
        original.push_str(&"x".repeat(1024 * 1024 - head.len() - tail.len()));
        original.push_str(tail);
        assert_eq!(original.len(), 1024 * 1024);
        // Feed a fake sandbox tool result through the same ceiling the worker
        // applies before persisting a `ToolCallCompleted` event.
        let result = swarmy_harness::execution_result(tool, Ok(original.clone()));
        let swarmy_core::ToolResult::Completed { output, .. } = result else {
            panic!("expected completed tool result");
        };
        let capped = swarmy_core::cap_tool_output(tool, call_id, output);
        let spill = swarmy_core::tool_spill_path(call_id);
        let dropped = original.len() - swarmy_core::MAX_TOOL_OUTPUT_BYTES;
        assert!(capped.len() <= swarmy_core::MAX_TOOL_OUTPUT_BYTES + 512);
        assert!(capped.contains(tool));
        assert!(capped.contains(&dropped.to_string()));
        assert!(capped.contains(&spill));
        assert!(spill.starts_with("/home/agent/.swarmy/output/"));
        assert!(capped.starts_with(head));
        assert!(capped.ends_with(tail));
        let keep = swarmy_core::MAX_TOOL_OUTPUT_BYTES;
        let head_len = keep.div_ceil(2);
        assert_eq!(&capped[..head_len], &original[..head_len]);
        assert_eq!(
            &capped[capped.len() - keep / 2..],
            &original[original.len() - keep / 2..]
        );
    }
}

#[cfg(test)]
mod side_tail_tests {
    use super::{estimate_message_tokens, select_side_tail};
    use std::collections::BTreeMap;
    use swarmy_core::{Message, MessageId, MessageRole, Part, ToolCallId, ToolResult};
    use ulid::Ulid;

    fn text(role: MessageRole, text: &str) -> Message {
        Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role,
            parts: vec![Part::Text { text: text.into() }],
        }
    }

    fn assistant_calls(id: &str, reasoning: bool) -> Message {
        let mut parts = Vec::new();
        if reasoning {
            parts.push(Part::Reasoning {
                text: format!("thinking for {id}"),
                metadata: BTreeMap::new(),
            });
        }
        parts.push(Part::ToolCall {
            call_id: ToolCallId(id.into()),
            tool: "get_time".into(),
            input: serde_json::json!({}),
        });
        Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role: MessageRole::Assistant,
            parts,
        }
    }

    fn tool_result(id: &str) -> Message {
        tool_result_sized(id, 0)
    }

    fn tool_result_sized(id: &str, padding: usize) -> Message {
        Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role: MessageRole::Tool,
            parts: vec![Part::ToolResult {
                call_id: ToolCallId(id.into()),
                result: ToolResult::Completed {
                    output: format!("result for {id} {}", "x".repeat(padding)),
                    title: String::new(),
                    metadata: BTreeMap::new(),
                },
            }],
        }
    }

    fn call_ids(messages: &[Message]) -> Vec<String> {
        messages
            .iter()
            .flat_map(|message| &message.parts)
            .filter_map(|part| match part {
                Part::ToolCall { call_id, .. } => Some(call_id.0.clone()),
                _ => None,
            })
            .collect()
    }

    fn result_ids(messages: &[Message]) -> Vec<String> {
        messages
            .iter()
            .flat_map(|message| &message.parts)
            .filter_map(|part| match part {
                Part::ToolResult { call_id, .. } => Some(call_id.0.clone()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn estimate_counts_chars_like_pi_and_opencode() {
        let message = text(MessageRole::User, &"x".repeat(400));
        assert_eq!(estimate_message_tokens(&message), 100);
        let image = Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role: MessageRole::User,
            parts: vec![Part::Image {
                media_type: "image/png".into(),
                bytes: Vec::new(),
                object_key: None,
                detail: None,
            }],
        };
        assert_eq!(estimate_message_tokens(&image), 1_200);
    }

    #[test]
    fn tail_cuts_between_tool_result_and_assistant() {
        // A short history fits the budget whole.
        let history = vec![
            text(MessageRole::User, "launch"),
            assistant_calls("a", false),
            tool_result("a"),
            text(MessageRole::Assistant, "halfway"),
            assistant_calls("b", false),
            tool_result("b"),
            text(MessageRole::Assistant, "summary output"),
        ];
        let tail = select_side_tail(&history);
        assert_eq!(tail.len(), history.len());
        // A long history cuts at a tool-result boundary and keeps pairs.
        let mut long = vec![text(MessageRole::User, "launch")];
        for round in 0..30 {
            let id = format!("round-{round}");
            long.push(assistant_calls(&id, false));
            long.push(tool_result(&id));
            long.push(text(
                MessageRole::Assistant,
                &format!("note {round} {}", "x".repeat(3_000)),
            ));
        }
        let tail = select_side_tail(&long);
        assert!(tail.len() < long.len());
        assert_eq!(tail[0].role, MessageRole::Assistant);
        assert!(tail.windows(2).all(|pair| {
            !(pair[0]
                .parts
                .iter()
                .any(|part| matches!(part, Part::ToolCall { .. }))
                && pair[1].role != MessageRole::Tool)
        }));
        for id in call_ids(&tail) {
            assert!(result_ids(&tail).contains(&id), "call {id} lost its result");
        }
    }

    #[test]
    fn tail_never_walks_back_to_the_launch_prompt() {
        // One user prompt plus many tool rounds: the tail must stay bounded
        // by tokens instead of reaching back to the launch prompt.
        let mut history = vec![text(
            MessageRole::User,
            &format!("launch {}", "x".repeat(4_000)),
        )];
        for round in 0..40 {
            let id = format!("round-{round}");
            history.push(assistant_calls(&id, round % 5 == 0));
            history.push(tool_result_sized(&id, 3_000));
        }
        let tail = select_side_tail(&history);
        assert!(!tail.iter().any(|message| message.role == MessageRole::User));
        for id in call_ids(&tail) {
            assert!(result_ids(&tail).contains(&id), "call {id} lost its result");
        }
        // Reasoning stays inside its assistant message.
        assert!(
            tail.iter()
                .filter(|message| message.role == MessageRole::Assistant)
                .flat_map(|message| &message.parts)
                .any(|part| matches!(part, Part::Reasoning { .. }))
        );
    }

    #[test]
    fn tail_drops_stale_pressure_warnings() {
        let warning = Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role: MessageRole::System,
            parts: vec![Part::Text {
                text: "context_pressure: input 150 tokens at 75 percent".into(),
            }],
        };
        let history = vec![
            text(MessageRole::User, "launch"),
            assistant_calls("a", false),
            tool_result("a"),
            warning,
            assistant_calls("b", false),
            tool_result("b"),
        ];
        let tail = select_side_tail(&history);
        assert!(
            tail.iter()
                .all(|message| message.role != MessageRole::System)
        );
        for id in call_ids(&tail) {
            assert!(result_ids(&tail).contains(&id), "call {id} lost its result");
        }
    }

    #[test]
    fn tail_falls_back_to_last_assistant_without_rounds() {
        let history = vec![
            text(MessageRole::User, "launch"),
            text(MessageRole::Assistant, &"x".repeat(100_000)),
        ];
        let tail = select_side_tail(&history);
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].role, MessageRole::Assistant);
    }

    #[test]
    fn tail_keeps_oversized_last_round_with_its_result() {
        // One 128 KiB tool result is about 32k tokens, over the 20k budget
        // on its own. The budget bounds what comes before the last round,
        // never the round itself: the successor must keep the assistant call
        // together with its result instead of orphaning the call.
        let mut history = vec![text(MessageRole::User, "launch")];
        for round in 0..5 {
            let id = format!("small-{round}");
            history.push(assistant_calls(&id, false));
            history.push(tool_result_sized(&id, 1_000));
        }
        history.push(assistant_calls("huge", false));
        history.push(tool_result_sized("huge", 128 * 1024));
        let tail = select_side_tail(&history);
        assert_eq!(call_ids(&tail), vec!["huge".to_owned()]);
        assert_eq!(result_ids(&tail), vec!["huge".to_owned()]);
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].role, MessageRole::Assistant);
        assert_eq!(tail[1].role, MessageRole::Tool);
    }

    #[test]
    fn mid_turn_fast_path_needs_no_store_reads() {
        use super::last_side_usage;
        use swarmy_core::{Event, RequestId, SessionId, TokenUsage};
        // Folds below pressure decide from the events the worker already
        // holds: zero transactions, zero store reads. This test pins that by
        // deciding without a `Store` at all.
        let session = SessionId::from_ulid(Ulid::generate());
        let request_id = RequestId::for_step(session, 1);
        let events = vec![Event::InferenceCompleted {
            seq: 1,
            request_id,
            message: text(MessageRole::Assistant, "working"),
            provider: "fake".into(),
            model: "base".into(),
            effort_used: None,
            usage: TokenUsage {
                input_tokens: 10,
                ..Default::default()
            },
            cost_micros: 0,
            effort_requested: None,
            effort_clamped: false,
        }];
        let Some((provider, model, input)) = last_side_usage(&events) else {
            panic!("expected usage from the held events");
        };
        assert_eq!(provider, "fake");
        assert_eq!(model, "base");
        assert_eq!(input, 10);
        // The default side threshold is 400k with pressure at 300k: an input
        // of 10 stays on the fast path, which returns before any store read.
        // Transaction count: 0. Store read count: 0.
        assert!(input < 300_000);
    }
}
