use super::step::pending_tools;
use super::{
    Bus, Context, Event, HeldLease, MAX_SCAN_LIMIT, MessageId, RequestId, Result, SandboxArguments,
    KillPoint, SessionId, SessionRecord, StoreError, ToolCallRecord, ToolJob, TurnStage, WorkQueue, Worker,
    execution_result, runnable_partition,
};

enum Dispatch<'a> {
    Calls(&'a [ToolCallRecord]),
    Pending(&'a [ToolJob]),
}

impl Worker {
    pub(super) async fn execute_pending(
        &self,
        session: &mut SessionRecord,
        lease: &HeldLease,
        events: &mut Vec<Event>,
        turn: Option<MessageId>,
    ) -> Result<bool> {
        let mut jobs = Vec::new();
        let display = self.session_display(session).await?;
        for (request_id, call) in pending_tools(events) {
            if let Some(job) = self
                .resolve_pending_call(session, lease, events, turn, display, request_id, call)
                .await?
            {
                jobs.push(job);
            }
        }
        if jobs.is_empty() {
            return Ok(false);
        }
        self.dispatch_pending(session, lease, jobs, turn).await?;
        Ok(true)
    }

    /// Store-side tools (plan, timers) complete without leaving the worker.
    /// Kept in sync with `complete_store_tool`, which runs the fenced store
    /// transition for exactly these names.
    fn is_inline_store_tool(tool: &str) -> bool {
        matches!(
            tool,
            "update_plan" | "set_timer" | "list_timers" | "cancel_timer"
        )
    }

    /// Complete one inline store tool: run the fenced transition, fold plan
    /// updates, publish, and record the stage.
    async fn complete_inline_tool(
        &self,
        session: &mut SessionRecord,
        lease: &HeldLease,
        events: &mut Vec<Event>,
        turn: Option<MessageId>,
        request_id: RequestId,
        call: &ToolCallRecord,
    ) -> Result<()> {
        let id = session.session_id;
        self.observe_dispatch(id, turn, request_id, &call.tool)
            .await;
        let event = self
            .complete_store_tool(session, lease, request_id, call)
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
        Ok(())
    }

    /// Resolve one pending call: a sandbox-bound job to dispatch after the
    /// loop, or `None` when the call completed inline or appended its
    /// failure below.
    async fn resolve_pending_call(
        &self,
        session: &mut SessionRecord,
        lease: &HeldLease,
        events: &mut Vec<Event>,
        turn: Option<MessageId>,
        display: bool,
        request_id: RequestId,
        call: ToolCallRecord,
    ) -> Result<Option<ToolJob>> {
        let id = session.session_id;
        let result = match self.store.ensure_session_computer(id).await {
            Err(StoreError::Domain(swarmy_store::DomainError::ComputerDeleted)) => {
                Err(StoreError::Domain(swarmy_store::DomainError::ComputerDeleted).to_string())
            }
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
                            return Ok(Some(ToolJob {
                                session_id: session.session_id,
                                request_id,
                                call_id: call.call_id,
                                step,
                                arguments,
                            }));
                        }
                        Err(error) => Err(error.to_string()),
                    }
                }
                Some(_) if Self::is_inline_store_tool(&call.tool) => {
                    self.complete_inline_tool(session, lease, events, turn, request_id, &call)
                        .await?;
                    return Ok(None);
                }
                Some(tool) => {
                    self.observe_dispatch(id, turn, request_id, &call.tool)
                        .await;
                    tool.execute(call.arguments).await
                }
                None => Err(format!("unknown tool: {}", call.tool)),
            },
        };
        let result =
            result.map(|output| swarmy_core::cap_tool_output(&call.tool, &call.call_id.0, output));
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
        Ok(None)
    }

    pub(super) async fn complete_store_tool(
        &self,
        session: &SessionRecord,
        lease: &HeldLease,
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

    pub(super) async fn dispatch_calls(
        &self,
        session: &SessionRecord,
        lease: &HeldLease,
        calls: &[ToolCallRecord],
        turn: Option<MessageId>,
    ) -> Result<()> {
        self.dispatch(session, lease, Dispatch::Calls(calls), turn)
            .await
    }

    pub(super) async fn dispatch_pending(
        &self,
        session: &SessionRecord,
        lease: &HeldLease,
        jobs: Vec<ToolJob>,
        turn: Option<MessageId>,
    ) -> Result<()> {
        self.dispatch(session, lease, Dispatch::Pending(&jobs), turn)
            .await
    }

    async fn dispatch(
        &self,
        session: &SessionRecord,
        lease: &HeldLease,
        dispatch: Dispatch<'_>,
        turn: Option<MessageId>,
    ) -> Result<()> {
        let mut retried = false;
        let (placement, events, jobs) = loop {
            // The failed transaction made no changes; an eviction may have
            // released a cached placement before its expiry, so the first
            // placement or lease fence invalidates the cache and retries once.
            let placement = self
                .placements
                .resolve(&self.store, session.agent_id, self.config.placement_lease)
                .await?;
            let mut token = lease.lock().await;
            let result = match dispatch {
                Dispatch::Calls(calls) => {
                    self.store
                        .dispatch_tool_calls(
                            session.session_id,
                            session.head_seq,
                            token.as_ref().context("lease released")?,
                            calls,
                            &placement,
                        )
                        .await
                }
                Dispatch::Pending(jobs) => self
                    .store
                    .dispatch_placed_tool_jobs(
                        session.session_id,
                        token.as_ref().context("lease released")?,
                        jobs,
                        &placement,
                    )
                    .await
                    .map(|()| (Vec::new(), jobs.to_vec())),
            };
            match result {
                Err(StoreError::Fence(
                    swarmy_store::FenceError::PlacementMismatch
                    | swarmy_store::FenceError::LeaseMismatch,
                )) if !retried => {
                    retried = true;
                    self.placements.invalidate(session.agent_id).await;
                    continue;
                }
                result => {
                    let (events, jobs) = result?;
                    token.release();
                    drop(token);
                    break (placement, events, jobs);
                }
            }
        };
        self.kill(KillPoint::AfterRelease);
        self.publish_events(session.session_id, &events).await?;
        self.publish_tools(session.session_id, &placement, jobs, turn)
            .await
    }

    pub(super) async fn publish_tools(
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
    pub(super) async fn observe_dispatch(
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

    pub(super) async fn tool_stage(
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

    pub(super) async fn place(&self, id: SessionId) -> Result<swarmy_core::PlacementRecord> {
        let agent = self
            .store
            .fetch_session(id)
            .await?
            .context("session missing")?
            .agent_id;
        crate::placement::resolve(&self.store, agent, self.config.placement_lease).await
    }

    pub(super) async fn route_tool(&self, job: &ToolJob) -> Result<()> {
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
}
