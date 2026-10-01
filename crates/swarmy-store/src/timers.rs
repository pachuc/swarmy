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
    fn active_timers(&self, agent: AgentId) -> Subspace {
        self.keys().timer_active_space(agent)
    }

    fn timer_due_key(&self, timer: &TimerRecord) -> Vec<u8> {
        self.keys()
            .timer_due(timer.due_at, timer.agent_id, timer.timer_id)
    }

    fn save_timer(&self, trx: &Transaction, timer: &TimerRecord) -> Result<()> {
        write(
            trx,
            &self.keys().timer(timer.agent_id, timer.timer_id),
            timer,
        )?;
        let active = self.keys().timer_active(timer.agent_id, timer.timer_id);
        if timer.status == TimerStatus::Pending {
            write(trx, &active, timer)?;
            write(trx, &self.timer_due_key(timer), timer)?;
        } else {
            trx.clear(&active);
            trx.clear(&self.timer_due_key(timer));
            trx.clear(&self.keys().timer_origin(timer.agent_id, timer.timer_id));
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
        self.transaction(|trx| async move { read(&trx, &self.keys().timer(agent, timer)).await })
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
            return Err(StoreError::Domain(crate::DomainError::InvalidToolCall(
                "expected a timer tool (set_timer, list_timers, or cancel_timer)".into(),
            )));
        }
        let timer_id = TimerId::from_ulid(ulid::Ulid::generate());
        let now = self.now();
        self.transaction(|trx| async move {
            self.check_worker_lease(&trx, id, lease, self.now()).await?;
            let mut session = self.session(&trx, id).await?;
            crate::check_head(session.head_seq, expected_head)?;
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
                .ok_or(StoreError::Storage(crate::StorageError::SequenceOverflow))?;
            let event = Event::ToolCallCompleted {
                seq,
                request_id,
                call_id: call.call_id.clone(),
                result,
            };
            let value = self.prepare(&event).await?;
            trx.set(&self.keys().event(id, seq), &value);
            session.head_seq = seq;
            self.write_session(&trx, &session)?;
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
                // Timer tool results are model-visible text by schema.
                // ast-grep-ignore: no-stringified-errors
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
                write(trx, &self.keys().timer_origin(agent, timer_id), &origin)?;
                serde_json::to_string(&timer)
            }
            "cancel_timer" => {
                let args =
                    match serde_json::from_value::<CancelTimerArguments>(call.arguments.clone()) {
                        Ok(args) => args,
                        Err(error) => return Ok(Err(error.to_string())),
                    };
                let Some(mut timer) =
                    read::<TimerRecord>(trx, &self.keys().timer(agent, args.timer_id)).await?
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
        Ok(Ok(output.map_err(|_| {
            StoreError::Storage(crate::StorageError::Corrupt)
        })?))
    }

    /// Page due timers without allowing a busy agent to block later timers.
    /// # Errors
    /// Returns database or decoding failures.
    pub async fn scan_due_timers(
        &self,
        now: Timestamp,
        after: Option<&TimerRecord>,
    ) -> Result<Vec<TimerRecord>> {
        let space = self.keys().timer_due_space_root();
        let (mut begin, _) = space.range();
        if let Some(after) = after {
            begin = crate::next_cursor(&self.timer_due_key(after));
        }
        let end = self.keys().timer_due_space(now).range().1;
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
            let Some(mut timer) =
                read::<TimerRecord>(&trx, &self.keys().timer(agent, timer)).await?
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
            if let Some(origin) = read::<SessionId>(
                &trx,
                &self.keys().timer_origin(timer.agent_id, timer.timer_id),
            )
            .await?
            {
                // Timers are lease-fenced to their agent, so a mismatched origin
                // only means stale state: fall through to the main conversation.
                let origin_session = match self.session(&trx, origin).await {
                    Ok(session) => Some(session),
                    Err(StoreError::Domain(crate::DomainError::SessionMissing)) => None,
                    Err(error) => return Err(error),
                };
                if let Some(session) = origin_session
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
                let session = swarmy_core::SessionRecord::new(
                    swarmy_core::SessionKind::Named {
                        agent_id: agent.agent_id,
                    },
                    agent.agent_id,
                    swarmy_core::SessionSettings::new(id),
                );
                self.create_session_in(&trx, &session, now, None).await?;
                agent.main_session = Some(id);
                write(&trx, &self.keys().agent(agent.agent_id), &agent)?;
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
            .ok_or(StoreError::Storage(crate::StorageError::SequenceOverflow))?;
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
        trx.set(&self.keys().event(id, seq), &value);
        session.head_seq = seq;
        self.transition(trx, session, SessionState::Runnable, now)
            .await?;
        Ok((id, event))
    }
}
