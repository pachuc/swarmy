//! Timer mutations and their tool results share the worker's head and lease fence.
use foundationdb::{Transaction, tuple::Subspace};
use jiff::Timestamp;
use swarmy_core::{
    AgentId, CancelTimerArguments, EmptyArguments, Event, Lease, MAX_AGENT_TIMERS, Message,
    MessageId, MessageRole, Part, RequestId, SessionId, SessionState, SetTimerArguments, TimerId,
    TimerRecord, TimerStatus, ToolCallRecord, ToolResult, decode,
};

use crate::{MAX_SCAN_LIMIT, Result, Store, StoreError, read, scan, write};

impl Store {
    fn timer_key(&self, agent: AgentId, timer: TimerId) -> Vec<u8> {
        self.root.pack(&(
            "timer",
            agent.as_ulid().to_bytes().as_slice(),
            timer.as_ulid().to_bytes().as_slice(),
        ))
    }

    fn active_timers(&self, agent: AgentId) -> Subspace {
        self.root
            .subspace(&("timer_active", agent.as_ulid().to_bytes().as_slice()))
    }

    fn timer_due_key(&self, timer: &TimerRecord) -> Vec<u8> {
        self.root.pack(&(
            "timer_due",
            timer.due_at.as_millisecond(),
            timer.agent_id.as_ulid().to_bytes().as_slice(),
            timer.timer_id.as_ulid().to_bytes().as_slice(),
        ))
    }

    /// The session whose worker set the timer. Timers stay agent-scoped so a
    /// summarized or closed origin cannot strand a note, but delivery prefers
    /// this idle session over the main conversation.
    fn timer_origin_key(&self, agent: AgentId, timer: TimerId) -> Vec<u8> {
        self.root.pack(&(
            "timer_origin",
            agent.as_ulid().to_bytes().as_slice(),
            timer.as_ulid().to_bytes().as_slice(),
        ))
    }

    fn save_timer(&self, trx: &Transaction, timer: &TimerRecord) -> Result<()> {
        write(trx, &self.timer_key(timer.agent_id, timer.timer_id), timer)?;
        let active = self.active_timers(timer.agent_id).pack(&(timer
            .timer_id
            .as_ulid()
            .to_bytes()
            .as_slice(),));
        if timer.status == TimerStatus::Pending {
            write(trx, &active, timer)?;
            write(trx, &self.timer_due_key(timer), timer)?;
        } else {
            trx.clear(&active);
            trx.clear(&self.timer_due_key(timer));
            trx.clear(&self.timer_origin_key(timer.agent_id, timer.timer_id));
        }
        Ok(())
    }

    async fn active_timers_in(
        &self,
        trx: &Transaction,
        agent: AgentId,
    ) -> Result<Vec<TimerRecord>> {
        scan(trx, self.active_timers(agent).range(), MAX_AGENT_TIMERS)
            .await?
            .into_iter()
            .map(|(_, value)| decode(&value).map_err(Into::into))
            .collect()
    }

    /// List pending timers for an agent. Each agent can have at most 32 pending timers.
    /// # Errors
    /// Returns database or decoding failures.
    pub async fn list_timers(&self, agent: AgentId) -> Result<Vec<TimerRecord>> {
        self.transaction(|trx| async move { self.active_timers_in(&trx, agent).await })
            .await
    }

    /// Read a timer including its durable delivery receipt or cancellation status.
    /// # Errors
    /// Returns database or decoding failures.
    pub async fn get_timer(&self, agent: AgentId, timer: TimerId) -> Result<Option<TimerRecord>> {
        self.transaction(|trx| async move { read(&trx, &self.timer_key(agent, timer)).await })
            .await
    }

    /// Execute an agent timer tool and commit its result under the worker fence.
    /// Retrying a stale completion cannot create or cancel another timer.
    /// # Errors
    /// Rejects stale heads or leases, unknown tool names, and storage failures.
    pub async fn complete_timer_tool(
        &self,
        id: SessionId,
        expected_head: u64,
        lease: &Lease,
        request_id: RequestId,
        call: &ToolCallRecord,
    ) -> Result<Event> {
        if !matches!(
            call.tool.as_str(),
            "set_timer" | "list_timers" | "cancel_timer"
        ) {
            return Err(StoreError::InvalidState);
        }
        let timer_id = TimerId::from_ulid(ulid::Ulid::generate());
        let now = Timestamp::now();
        self.transaction(|trx| async move {
            self.check_worker_lease(&trx, id, lease, Timestamp::now())
                .await?;
            let mut session = self.session(&trx, id).await?;
            if session.head_seq != expected_head {
                return Err(StoreError::StaleSequence {
                    expected: expected_head,
                    actual: session.head_seq,
                });
            }
            self.check_computer(&trx, session.agent_id).await?;
            let result = if self.read_agent(&trx, session.agent_id).await?.is_some() {
                self.timer_tool_in(&trx, session.agent_id, id, timer_id, call, now)
                    .await?
            } else {
                Err("timers require a named agent".into())
            };
            let result = match result {
                Ok(output) => ToolResult::Completed {
                    title: call.tool.clone(),
                    output,
                    metadata: std::collections::BTreeMap::new(),
                },
                Err(error) => ToolResult::Error { error },
            };
            let seq = expected_head
                .checked_add(1)
                .ok_or(StoreError::SequenceOverflow)?;
            let event = Event::ToolCallCompleted {
                seq,
                request_id,
                call_id: call.call_id.clone(),
                result,
            };
            let value = self.prepare(&event).await?;
            trx.set(&self.event_space(id).pack(&(seq,)), &value);
            session.head_seq = seq;
            write(&trx, &self.session_key(id), &session)?;
            Ok(event)
        })
        .await
    }

    async fn timer_tool_in(
        &self,
        trx: &Transaction,
        agent: AgentId,
        origin: SessionId,
        timer_id: TimerId,
        call: &ToolCallRecord,
        now: Timestamp,
    ) -> Result<std::result::Result<String, String>> {
        let output = match call.tool.as_str() {
            "set_timer" => {
                let args = serde_json::from_value::<SetTimerArguments>(call.arguments.clone());
                let (args, due_at) = match args
                    .map_err(|error| error.to_string())
                    .and_then(|args| args.due_at(now).map(|due| (args, due)))
                {
                    Ok(parsed) => parsed,
                    Err(error) => return Ok(Err(error)),
                };
                if self.active_timers_in(trx, agent).await?.len() == MAX_AGENT_TIMERS {
                    return Ok(Err("at most 32 pending timers per agent".into()));
                }
                let timer = TimerRecord {
                    timer_id,
                    agent_id: agent,
                    due_at,
                    note: args.note,
                    status: TimerStatus::Pending,
                };
                self.save_timer(trx, &timer)?;
                write(trx, &self.timer_origin_key(agent, timer_id), &origin)?;
                serde_json::to_string(&timer)
            }
            "cancel_timer" => {
                let args =
                    match serde_json::from_value::<CancelTimerArguments>(call.arguments.clone()) {
                        Ok(args) => args,
                        Err(error) => return Ok(Err(error.to_string())),
                    };
                let Some(mut timer) =
                    read::<TimerRecord>(trx, &self.timer_key(agent, args.timer_id)).await?
                else {
                    return Ok(Err("timer not found for this agent".into()));
                };
                if matches!(timer.status, TimerStatus::Fired { .. }) {
                    return Ok(Err("timer already fired".into()));
                }
                timer.status = TimerStatus::Cancelled;
                self.save_timer(trx, &timer)?;
                serde_json::to_string(&timer)
            }
            _ => {
                if let Err(error) = serde_json::from_value::<EmptyArguments>(call.arguments.clone())
                {
                    return Ok(Err(error.to_string()));
                }
                serde_json::to_string(&self.active_timers_in(trx, agent).await?)
            }
        };
        Ok(Ok(output.map_err(|_| StoreError::Corrupt)?))
    }

    /// Page due timers without allowing a busy agent to block later timers.
    /// # Errors
    /// Returns database or decoding failures.
    pub async fn scan_due_timers(
        &self,
        now: Timestamp,
        after: Option<&TimerRecord>,
    ) -> Result<Vec<TimerRecord>> {
        let space = self.root.subspace(&("timer_due",));
        let (mut begin, _) = space.range();
        if let Some(after) = after {
            begin = self.timer_due_key(after);
            begin.push(0);
        }
        let end = space.subspace(&(now.as_millisecond(),)).range().1;
        self.transaction(|trx| {
            let range = (begin.clone(), end.clone());
            async move {
                scan(&trx, range, MAX_SCAN_LIMIT)
                    .await?
                    .into_iter()
                    .map(|(_, value)| decode(&value).map_err(Into::into))
                    .collect()
            }
        })
        .await
    }

    /// Append a due note and its delivery receipt atomically, preferring the idle
    /// session that set the timer and falling back to the current main session.
    /// Busy sessions leave the timer pending until a later tick. A lost nudge is
    /// recovered by the runnable scan; a failed append never marks the timer fired.
    /// # Errors
    /// Returns database, encoding, or sequence overflow failures.
    pub async fn fire_timer(
        &self,
        agent: AgentId,
        timer: TimerId,
        now: Timestamp,
    ) -> Result<Option<(SessionId, Event)>> {
        self.transaction(|trx| async move {
            let Some(mut timer) = read::<TimerRecord>(&trx, &self.timer_key(agent, timer)).await?
            else {
                return Ok(None);
            };
            if timer.status != TimerStatus::Pending || timer.due_at > now {
                return Ok(None);
            }
            let Some(mut agent) = self.read_agent(&trx, agent).await? else {
                timer.status = TimerStatus::Cancelled;
                self.save_timer(&trx, &timer)?;
                return Ok(None);
            };
            if let Some(origin) =
                read::<SessionId>(&trx, &self.timer_origin_key(timer.agent_id, timer.timer_id))
                    .await?
            {
                // Timers are lease-fenced to their agent, so a mismatched origin
                // only means stale state: fall through to the main conversation.
                if let Some(session) =
                    read::<crate::StoredSession>(&trx, &self.session_key(origin)).await?
                    && session.agent_id == agent.agent_id
                    && session.state == SessionState::Idle
                {
                    self.check_computer(&trx, agent.agent_id).await?;
                    let (id, event) = self.deliver_timer(&trx, session, &timer, now).await?;
                    timer.status = TimerStatus::Fired {
                        session_id: id,
                        seq: event.seq(),
                    };
                    self.save_timer(&trx, &timer)?;
                    return Ok(Some((id, event)));
                }
            }
            let id = if let Some(id) = agent.main_session {
                id
            } else {
                let id = SessionId::from_ulid(ulid::Ulid::generate());
                let session = swarmy_core::SessionRecord {
                    interrupt_requested: false,
                    session_id: id,
                    agent_id: agent.agent_id,
                    kind: swarmy_core::SessionKind::Named {
                        agent_id: agent.agent_id,
                    },
                    computer_deleted: false,
                    state: SessionState::Idle,
                    head_seq: 0,
                    snapshot_ref: None,
                    inference: swarmy_core::InferenceSelection::default(),
                    plan: Vec::new(),
                    route: None,
                    route_step: 0,
                };
                self.create_session_in(&trx, &session, now, None).await?;
                agent.main_session = Some(id);
                write(&trx, &self.agent_key(agent.agent_id), &agent)?;
                id
            };
            let session = self.session(&trx, id).await?;
            if session.state != SessionState::Idle {
                return Ok(None);
            }
            self.check_computer(&trx, agent.agent_id).await?;
            let (id, event) = self.deliver_timer(&trx, session, &timer, now).await?;
            timer.status = TimerStatus::Fired {
                session_id: id,
                seq: event.seq(),
            };
            self.save_timer(&trx, &timer)?;
            Ok(Some((id, event)))
        })
        .await
    }

    /// Append the timer note to an idle session and make it runnable. Callers
    /// check idleness first so a busy session leaves the timer pending.
    async fn deliver_timer(
        &self,
        trx: &Transaction,
        mut session: crate::StoredSession,
        timer: &TimerRecord,
        now: Timestamp,
    ) -> Result<(SessionId, Event)> {
        let id = session.session_id;
        let seq = session
            .head_seq
            .checked_add(1)
            .ok_or(StoreError::SequenceOverflow)?;
        let event = Event::MessageAppended {
            seq,
            message: Message {
                id: MessageId::from_ulid(timer.timer_id.as_ulid()),
                role: MessageRole::System,
                parts: vec![Part::Text {
                    text: timer.note.clone(),
                }],
            },
        };
        let value = self.prepare(&event).await?;
        trx.set(&self.event_space(id).pack(&(seq,)), &value);
        session.head_seq = seq;
        self.transition(trx, session, SessionState::Runnable, now)
            .await?;
        Ok((id, event))
    }
}
