//! Named identities and session creation.
use crate::{Result, Store, StoreError, StoredSession, check_limit, read, scan, write};
use foundationdb::Transaction;
use jiff::Timestamp;
use swarmy_core::{
    AgentId, AgentRecord, AgentSettings, ImageRecord, ImageTag, InferenceSelection, RouteRecord,
    SessionId, SessionKind, SessionRecord, SessionSettings, SessionState, decode,
};

/// Options for creating a named agent. The replay key and private token are
/// committed with a new agent atomically; a retry after an unknown commit
/// returns the original record instead of another agent.
#[derive(Clone, Debug, Default)]
pub struct CreateAgentOptions<'a> {
    /// Inference overrides pinned with the agent.
    pub settings: Option<&'a AgentSettings>,
    /// Private GitHub token, stored as a side field so records and public
    /// views never contain it.
    pub github_token: Option<&'a str>,
    /// Idempotency key for the creation.
    pub replay_key: Option<&'a str>,
}

impl Store {
    /// Create a named agent, resolving and pinning the registered image atomically.
    /// # Errors
    /// Rejects duplicate names or ids, invalid names, unknown images, and storage failures.
    pub async fn create_agent(
        &self,
        name: &str,
        image: &str,
        description: &str,
        now: Timestamp,
        options: Option<CreateAgentOptions<'_>>,
    ) -> Result<AgentRecord> {
        let options = options.unwrap_or_default();
        let defaults = AgentSettings::default();
        let settings = options.settings.unwrap_or(&defaults);
        let github_token = options.github_token;
        let replay_key = options.replay_key;
        validate_github_token(github_token)?;
        if name.is_empty() || name.chars().any(char::is_control) {
            return Err(StoreError::Domain(crate::DomainError::InvalidAgentName));
        }
        if name.len() > 1024 || description.len() > crate::INLINE_LIMIT / 2 {
            return Err(StoreError::Storage(crate::StorageError::TooLarge));
        }
        let id = AgentId::from_ulid(ulid::Ulid::generate());
        let result = self
            .transaction(|trx| async move {
                if let Some(key) = replay_key {
                    let replay_key = self.keys().api_idempotency(key);
                    if let Some(previous) =
                        read::<crate::api_idempotency::ApiReplay>(&trx, &replay_key).await?
                        && previous.expires_at > now
                    {
                        return serde_json::from_str(&previous.result)
                            .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt));
                    }
                }
                if trx
                    .get(&self.keys().agent_by_name(name), false)
                    .await?
                    .is_some()
                    || trx.get(&self.keys().agent(id), false).await?.is_some()
                {
                    return Err(StoreError::Domain(crate::DomainError::AgentExists));
                }
                let image = self.resolve_image(&trx, image).await?;
                if let Some(name) = settings.route.as_deref()
                    && read::<RouteRecord>(&trx, &self.keys().route(name))
                        .await?
                        .is_none()
                {
                    return Err(StoreError::Domain(crate::DomainError::RouteMissing));
                }
                let default_memory: Option<u64> = read(
                    &trx,
                    &self
                        .keys()
                        .image_memory(&image.name, &image.tag, image.manifest_id),
                )
                .await?
                .flatten();
                let record = AgentRecord {
                    agent_id: id,
                    name: name.into(),
                    image,
                    description: description.into(),
                    created_at: now,
                    main_session: None,
                    system_prompt: settings.system_prompt.clone(),
                    model: settings.model.clone(),
                    reasoning_effort: settings.reasoning_effort,
                    provider: settings.provider.clone(),
                    route: settings.route.clone(),
                    requirements: swarmy_core::SandboxRequirements {
                        memory_mib: settings.memory_mib.or(default_memory).unwrap_or(768),
                        gpu: settings.gpu.unwrap_or_default(),
                    },
                };
                write(&trx, &self.keys().agent(id), &record)?;
                if let Some(token) = github_token {
                    write(&trx, &self.keys().agent_github_token(id), &token)?;
                }
                write(&trx, &self.keys().agent_by_name(name), &id)?;
                if let Some(key) = replay_key {
                    let result = serde_json::to_string(&record)
                        .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?;
                    write(
                        &trx,
                        &self.keys().api_idempotency(key),
                        &crate::api_idempotency::ApiReplay {
                            result,
                            expires_at: now
                                .checked_add(jiff::Span::new().hours(1))
                                .unwrap_or(jiff::Timestamp::MAX),
                        },
                    )?;
                }
                Ok(record)
            })
            .await;
        if matches!(
            result,
            Err(StoreError::Domain(crate::DomainError::ImageMissing { .. }))
        ) {
            return Err(self.unregistered_image(image).await?);
        }
        result
    }

    /// # Errors
    /// Returns database or decoding failures.
    pub async fn get_agent(&self, id: AgentId) -> Result<Option<AgentRecord>> {
        self.transaction(|trx| async move { self.read_agent(&trx, id).await })
            .await
    }

    /// # Errors
    /// Returns database or decoding failures.
    pub async fn get_agent_by_name(&self, name: &str) -> Result<Option<AgentRecord>> {
        self.transaction(|trx| async move {
            match read(&trx, &self.keys().agent_by_name(name)).await? {
                Some(id) => self.read_agent(&trx, id).await,
                None => Ok(None),
            }
        })
        .await
    }

    /// Read the private credential field on each use; never include it in agent views.
    /// # Errors
    /// Returns database or decoding failures.
    pub async fn agent_github_token(&self, id: AgentId) -> Result<Option<String>> {
        self.transaction(|trx| async move {
            if trx.get(&self.keys().agent(id), false).await?.is_none() {
                return Ok(None);
            }
            read(&trx, &self.keys().agent_github_token(id)).await
        })
        .await
    }

    /// Rotate or clear an agent's private GitHub token.
    /// # Errors
    /// Rejects unknown agents, invalid tokens, and database failures.
    pub async fn set_agent_github_token(&self, id: AgentId, token: Option<&str>) -> Result<()> {
        validate_github_token(token)?;
        self.transaction(|trx| async move {
            if trx.get(&self.keys().agent(id), false).await?.is_none() {
                return Err(StoreError::Domain(crate::DomainError::AgentMissing));
            }
            let key = self.keys().agent_github_token(id);
            if let Some(token) = token {
                write(&trx, &key, &token)?;
            } else {
                trx.clear(&key);
            }
            Ok(())
        })
        .await
    }

    /// List agents by id, strictly after the cursor.
    /// # Errors
    /// Rejects invalid page limits and storage failures.
    pub async fn list_agents(
        &self,
        after: Option<AgentId>,
        limit: usize,
    ) -> Result<Vec<AgentRecord>> {
        check_limit(limit)?;
        self.transaction(|trx| async move {
            let (mut begin, end) = self.keys().agent_space().range();
            if let Some(id) = after {
                begin = self.keys().agent(id);
                begin = crate::next_cursor(&begin);
            }
            scan(&trx, (begin, end), limit)
                .await?
                .into_iter()
                .map(|(_, value)| Ok(decode::<AgentRecord>(&value)?))
                .collect()
        })
        .await
    }

    /// Atomically change supplied inference settings, preserving omitted fields.
    /// # Errors
    /// Rejects unknown agents, oversized records, and storage failures.
    pub async fn set_agent(&self, id: AgentId, settings: &AgentSettings) -> Result<AgentRecord> {
        self.set_agent_with_resets(id, settings, &[]).await
    }

    /// Apply overrides and explicit resets in the same transaction.
    /// # Errors
    /// Rejects unknown agents, missing routes, oversized records, and storage failures.
    pub async fn set_agent_with_resets(
        &self,
        id: AgentId,
        settings: &AgentSettings,
        resets: &[swarmy_core::InferenceField],
    ) -> Result<AgentRecord> {
        if let Some(name) = settings.route.as_deref() {
            // Fail fast outside the write transaction; the transaction below
            // rechecks so a concurrent deletion cannot slip through.
            if self.get_route(name).await?.is_none() {
                return Err(StoreError::Domain(crate::DomainError::RouteMissing));
            }
        }
        self.transaction(|trx| async move {
            let mut agent = self
                .read_agent(&trx, id)
                .await?
                .ok_or(StoreError::Domain(crate::DomainError::AgentMissing))?;
            if let Some(name) = settings.route.as_deref()
                && read::<RouteRecord>(&trx, &self.keys().route(name))
                    .await?
                    .is_none()
            {
                return Err(StoreError::Domain(crate::DomainError::RouteMissing));
            }
            let previous_requirements = agent.requirements;
            settings.apply_to(&mut agent, resets);
            if agent.requirements != previous_requirements
                && read::<swarmy_core::PlacementRecord>(&trx, &self.keys().placement(id))
                    .await?
                    .is_some()
            {
                return Err(StoreError::Domain(
                    crate::DomainError::ActiveSandboxRequirements,
                ));
            }
            write(&trx, &self.keys().agent(id), &agent)?;
            Ok(agent)
        })
        .await
    }

    /// Delete the identity and its computer atomically, retaining its sessions.
    /// Repeating deletion is safe; a reused name receives a fresh agent id.
    /// # Errors
    /// Returns database or decoding failures.
    pub async fn delete_agent(&self, id: AgentId) -> Result<()> {
        self.transaction(|trx| async move {
            if let Some(agent) = self.read_agent(&trx, id).await? {
                self.delete_computer_in(&trx, id).await?;
                trx.clear(&self.keys().agent_by_name(&agent.name));
                trx.clear(&self.keys().agent(id));
                trx.clear(&self.keys().agent_github_token(id));
            }
            Ok(())
        })
        .await
    }
}

/// Options for creating a session attached to a named agent or an anonymous one.
#[derive(Clone, Debug, Default)]
pub struct AgentSessionOptions<'a> {
    /// Image for ephemeral sessions; forbidden for named sessions.
    pub image: Option<&'a str>,
    /// Immutable inference overrides for the session's first turn.
    pub inference: Option<&'a InferenceSelection>,
    /// Named route override; the route must exist.
    pub route: Option<&'a str>,
}

impl Store {
    /// Create an idle session, minting an anonymous agent or attaching to a named one.
    /// `image` is required for ephemeral sessions and forbidden for named sessions.
    /// # Errors
    /// Rejects unknown agents/images, deleted computers, image overrides,
    /// missing routes, and duplicate sessions.
    pub async fn create_agent_session(
        &self,
        id: SessionId,
        agent: Option<AgentId>,
        now: Timestamp,
        options: Option<AgentSessionOptions<'_>>,
    ) -> Result<SessionRecord> {
        let options = options.unwrap_or_default();
        let mut settings = SessionSettings::new(id);
        settings.inference = options.inference.cloned().unwrap_or_default();
        settings.route = options.route.map(str::to_owned);
        let session = SessionRecord::new(
            agent.map_or(SessionKind::Ephemeral, |agent_id| SessionKind::Named {
                agent_id,
            }),
            agent.unwrap_or_else(|| AgentId::from_ulid(ulid::Ulid::generate())),
            settings,
        );
        self.create_session_record(&session, now, options.image)
            .await?;
        Ok(session)
    }

    pub(crate) async fn create_session_record(
        &self,
        session: &SessionRecord,
        now: Timestamp,
        image: Option<&str>,
    ) -> Result<()> {
        if session.head_seq != 0
            || session.snapshot_ref.is_some()
            || session.computer_deleted
            || session.interrupt_requested
            || !matches!(session.state, SessionState::Idle | SessionState::Runnable)
        {
            return Err(StoreError::Domain(
                crate::DomainError::UnexpectedSessionState,
            ));
        }
        if matches!(session.kind, SessionKind::Named { .. }) && image.is_some() {
            return Err(StoreError::Domain(crate::DomainError::NamedAgentImage));
        }
        let result = self
            .transaction(
                |trx| async move { self.create_session_in(&trx, session, now, image).await },
            )
            .await;
        if matches!(
            result,
            Err(StoreError::Domain(crate::DomainError::ImageMissing { .. }))
        ) {
            return Err(self.unregistered_image(image.unwrap_or_default()).await?);
        }
        result
    }

    pub(crate) async fn create_session_in(
        &self,
        trx: &Transaction,
        session: &SessionRecord,
        now: Timestamp,
        image: Option<&str>,
    ) -> Result<()> {
        let id = session.session_id;
        let key = self.keys().session(id);
        if trx.get(&key, false).await?.is_some() {
            return Err(StoreError::Domain(crate::DomainError::SessionExists));
        }
        if let Some(name) = &session.route
            && read::<RouteRecord>(trx, &self.keys().route(name))
                .await?
                .is_none()
        {
            return Err(StoreError::Domain(crate::DomainError::RouteMissing));
        }
        self.check_computer(trx, session.agent_id).await?;
        let selected = match session.kind {
            SessionKind::Ephemeral => {
                // Never attach ephemeral lifetime rules to a named identity.
                if trx
                    .get(&self.keys().agent(session.agent_id), false)
                    .await?
                    .is_some()
                {
                    return Err(StoreError::Domain(
                        crate::DomainError::SessionComputerExists,
                    ));
                }
                self.resolve_image(
                    trx,
                    image.ok_or(StoreError::Domain(crate::DomainError::SessionImageRequired))?,
                )
                .await?
            }
            SessionKind::Named { agent_id } => {
                if agent_id != session.agent_id {
                    return Err(StoreError::Fence(crate::FenceError::SessionAgentMismatch));
                }
                self.read_agent(trx, agent_id)
                    .await?
                    .ok_or(StoreError::Domain(crate::DomainError::AgentMissing))?
                    .image
            }
        };
        if matches!(session.kind, SessionKind::Ephemeral) {
            let memory: Option<u64> = read(
                trx,
                &self
                    .keys()
                    .image_memory(&selected.name, &selected.tag, selected.manifest_id),
            )
            .await?
            .flatten();
            write(
                trx,
                &self.keys().computer_memory(session.agent_id),
                &memory.unwrap_or(768),
            )?;
        }
        swarmy_core::UpdatePlanArguments {
            plan: session.plan.clone(),
        }
        .validate()
        .map_err(|_| StoreError::Domain(crate::DomainError::InvalidSessionRecord))?;
        write(
            trx,
            &self.keys().session_by_agent(session.agent_id, id),
            &id,
        )?;
        self.write_session(
            trx,
            &StoredSession {
                session_id: id,
                agent_id: session.agent_id,
                state: session.state,
                head_seq: 0,
                snapshot_seq: None,
                inference: session.inference.clone(),
                kind: session.kind,
                computer_deleted: false,
                plan: session.plan.clone(),
                interrupt_requested: false,
                route: session.route.clone(),
                route_step: session.route_step,
                image: Some(selected),
                idle_since: (session.state == SessionState::Idle).then_some(now),
                state_since: Some(now),
            },
        )?;
        if session.state == SessionState::Runnable {
            self.index_runnable(
                trx,
                &swarmy_core::RunnableEntry {
                    session_id: id,
                    priority: 0,
                    wake_at: now,
                },
            )
            .await?;
        }
        Ok(())
    }

    async fn resolve_image(&self, trx: &Transaction, image: &str) -> Result<ImageRecord> {
        let (name, tag) = image
            .split_once(':')
            .filter(|(name, tag)| !name.is_empty() && !tag.is_empty() && !tag.contains(':'))
            .ok_or(StoreError::Domain(crate::DomainError::InvalidImage))?;
        let tag = ImageTag(tag.into());
        let manifest_id = read(trx, &self.keys().image(name, &tag))
            .await?
            .ok_or_else(|| {
                StoreError::Domain(crate::DomainError::ImageMissing {
                    image: image.into(),
                    registered: String::new(),
                })
            })?;
        Ok(ImageRecord {
            name: name.into(),
            tag,
            manifest_id,
        })
    }

    /// Open the main conversation, creating it and its pointer in one transaction.
    /// Returns the session id and whether this call created it.
    /// # Errors
    /// Rejects missing agents, deleted computers, and storage failures.
    pub async fn open_main_session(
        &self,
        agent: AgentId,
        now: Timestamp,
    ) -> Result<(SessionId, bool)> {
        let id = SessionId::from_ulid(ulid::Ulid::generate());
        self.transaction(|trx| async move {
            let mut record = self
                .read_agent(&trx, agent)
                .await?
                .ok_or(StoreError::Domain(crate::DomainError::AgentMissing))?;
            self.check_computer(&trx, agent).await?;
            if let Some(main) = record.main_session {
                return Ok((main, false));
            }
            let session = SessionRecord::new(
                SessionKind::Named { agent_id: agent },
                agent,
                SessionSettings::new(id),
            );
            self.create_session_in(&trx, &session, now, None).await?;
            record.main_session = Some(id);
            write(&trx, &self.keys().agent(agent), &record)?;
            Ok((id, true))
        })
        .await
    }

    /// Atomically replace the main pointer with an open session belonging to this agent.
    /// # Errors
    /// Rejects missing agents/sessions, foreign or completed sessions, and deleted computers.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn set_main_session(&self, agent: AgentId, id: SessionId) -> Result<()> {
        self.transaction(|trx| async move {
            let mut record = self
                .read_agent(&trx, agent)
                .await?
                .ok_or(StoreError::Domain(crate::DomainError::AgentMissing))?;
            let session = self.session(&trx, id).await?;
            self.check_computer(&trx, agent).await?;
            if session.agent_id != agent
                || session.kind != (SessionKind::Named { agent_id: agent })
                || session.state == SessionState::Completed
            {
                return Err(StoreError::Domain(crate::DomainError::InvalidMainSession));
            }
            record.main_session = Some(id);
            write(&trx, &self.keys().agent(agent), &record)
        })
        .await
    }

    /// Replace a leased main session with a fresh idle session containing a summary.
    /// Archival, both links, opening context, and the pointer commit together.
    /// # Errors
    /// Rejects stale heads or leases, a changed main pointer, and storage failures.
    pub async fn summarize_main_session(
        &self,
        old: SessionId,
        expected_head: u64,
        lease: &swarmy_core::Lease,
        opening: &swarmy_core::Message,
        tail: &[swarmy_core::Message],
    ) -> Result<(SessionId, swarmy_core::Event)> {
        if opening.role != swarmy_core::MessageRole::User {
            return Err(StoreError::Domain(crate::DomainError::InvalidMessageRole));
        }
        let id = SessionId::from_ulid(ulid::Ulid::generate());
        let messages = side_messages(opening, tail);
        let (prepared, archived_value, archived, new_head) =
            self.prepare_side_rollover(&messages, expected_head).await?;
        self.transaction(|trx| {
            let (prepared, archived_value) = (&prepared, &archived_value);
            async move {
                let now = self.now();
                self.check_worker_lease(&trx, old, lease, now).await?;
                let mut previous = self.session(&trx, old).await?;
                crate::check_head(previous.head_seq, expected_head)?;
                let mut agent = self
                    .read_agent(&trx, previous.agent_id)
                    .await?
                    .ok_or(StoreError::Domain(crate::DomainError::AgentMissing))?;
                if agent.main_session != Some(old) {
                    return Err(StoreError::Domain(crate::DomainError::InvalidMainSession));
                }
                let session = SessionRecord::new(
                    SessionKind::Named {
                        agent_id: agent.agent_id,
                    },
                    agent.agent_id,
                    SessionSettings::new(id),
                );
                self.create_session_in(&trx, &session, now, None).await?;
                self.transfer_queued(&trx, old, id).await?;
                let mut created = self.session(&trx, id).await?;
                created.head_seq = new_head;
                self.write_session(&trx, &created)?;
                // The queued-input check shares the rollover transaction, so a
                // successor is runnable even if the worker dies before nudging it.
                if self.has_queued_in(&trx, id).await? {
                    self.transition(&trx, created, SessionState::Runnable, now)
                        .await?;
                }
                for (index, value) in prepared.iter().enumerate() {
                    let seq = u64::try_from(index)
                        .map_err(|_| StoreError::Storage(crate::StorageError::SequenceOverflow))?
                        + 1;
                    trx.set(&self.keys().event(id, seq), value);
                }
                let head = expected_head + 1;
                trx.set(&self.keys().event(old, head), archived_value);
                previous.head_seq = head;
                self.transition(&trx, previous, SessionState::Completed, now)
                    .await?;
                write(&trx, &self.keys().session_chain("next", old), &id)?;
                write(&trx, &self.keys().session_chain("previous", id), &old)?;
                agent.main_session = Some(id);
                write(&trx, &self.keys().agent(agent.agent_id), &agent)
            }
        })
        .await?;
        Ok((id, archived))
    }

    /// Replace a leased side session with a fresh idle side session.
    /// The summary opening plus the recent tool rounds carry context forward;
    /// archival and both links commit together without moving the main pointer.
    /// The worker bounds the tail by tokens, so the store accepts any tail
    /// length and never fails a step because of tail size.
    /// # Errors
    /// Rejects stale heads or leases, main sessions, ephemeral sessions, and storage failures.
    pub async fn summarize_side_session(
        &self,
        old: SessionId,
        expected_head: u64,
        lease: &swarmy_core::Lease,
        opening: &swarmy_core::Message,
        tail: &[swarmy_core::Message],
    ) -> Result<(SessionId, swarmy_core::Event)> {
        if opening.role != swarmy_core::MessageRole::User {
            return Err(StoreError::Domain(crate::DomainError::InvalidMessageRole));
        }
        // Keep the successor in the old session's runnable partition so the
        // same scheduler and worker continue the task without rebalancing.
        let id = loop {
            let id = SessionId::from_ulid(ulid::Ulid::generate());
            if swarmy_core::runnable_partition(id) == swarmy_core::runnable_partition(old) {
                break id;
            }
        };
        let messages = side_messages(opening, tail);
        let (prepared, archived_value, archived, new_head) =
            self.prepare_side_rollover(&messages, expected_head).await?;
        self.transaction(|trx| {
            let (prepared, archived_value) = (&prepared, &archived_value);
            async move {
                let now = self.now();
                self.check_worker_lease(&trx, old, lease, now).await?;
                let mut previous = self.session(&trx, old).await?;
                crate::check_head(previous.head_seq, expected_head)?;
                let agent = self
                    .read_agent(&trx, previous.agent_id)
                    .await?
                    .ok_or(StoreError::Domain(crate::DomainError::AgentMissing))?;
                let kind = previous.kind;
                let SessionKind::Named { agent_id } = kind else {
                    return Err(StoreError::Domain(crate::DomainError::InvalidSessionRecord));
                };
                if agent_id != agent.agent_id {
                    return Err(StoreError::Fence(crate::FenceError::SessionAgentMismatch));
                }
                if agent.main_session == Some(old) {
                    return Err(StoreError::Domain(crate::DomainError::InvalidMainSession));
                }
                let inference = previous.inference.clone();
                let plan = previous.plan.clone();
                let route = previous.route.clone();
                let route_step = previous.route_step;
                let mut settings = SessionSettings::new(id);
                settings.inference = inference;
                settings.plan = plan;
                settings.route = route;
                settings.route_step = route_step;
                let session = SessionRecord::new(
                    SessionKind::Named {
                        agent_id: agent.agent_id,
                    },
                    agent.agent_id,
                    settings,
                );
                self.create_session_in(&trx, &session, now, None).await?;
                self.transfer_queued(&trx, old, id).await?;
                self.write_side_events(&trx, id, old, prepared, archived_value, new_head)
                    .await?;
                if self.has_queued_in(&trx, id).await? {
                    let created = self.session(&trx, id).await?;
                    self.transition(&trx, created, SessionState::Runnable, now)
                        .await?;
                }
                previous.head_seq = expected_head
                    .checked_add(1)
                    .ok_or(StoreError::Storage(crate::StorageError::SequenceOverflow))?;
                self.transition(&trx, previous, SessionState::Completed, now)
                    .await?;
                write(&trx, &self.keys().session_chain("next", old), &id)?;
                write(&trx, &self.keys().session_chain("previous", id), &old)?;
                Ok(())
            }
        })
        .await?;
        Ok((id, archived))
    }

    async fn prepare_side_rollover(
        &self,
        messages: &[swarmy_core::Message],
        expected_head: u64,
    ) -> Result<(Vec<Vec<u8>>, Vec<u8>, swarmy_core::Event, u64)> {
        let mut prepared = Vec::with_capacity(messages.len());
        for (index, message) in messages.iter().enumerate() {
            let seq = u64::try_from(index)
                .ok()
                .and_then(|i| i.checked_add(1))
                .ok_or(StoreError::Storage(crate::StorageError::SequenceOverflow))?;
            let event = swarmy_core::Event::MessageAppended {
                seq,
                message: message.clone(),
            };
            prepared.push(self.prepare(&event).await?);
        }
        let head = expected_head
            .checked_add(1)
            .ok_or(StoreError::Storage(crate::StorageError::SequenceOverflow))?;
        let archived = swarmy_core::Event::StateChanged {
            seq: head,
            from: SessionState::Leased,
            to: SessionState::Completed,
        };
        let archived_value = self.prepare(&archived).await?;
        let new_head = u64::try_from(messages.len())
            .map_err(|_| StoreError::Storage(crate::StorageError::SequenceOverflow))?;
        Ok((prepared, archived_value, archived, new_head))
    }

    async fn write_side_events(
        &self,
        trx: &foundationdb::Transaction,
        id: SessionId,
        old: SessionId,
        prepared: &[Vec<u8>],
        archived_value: &[u8],
        new_head: u64,
    ) -> Result<()> {
        let mut created = self.session(trx, id).await?;
        created.head_seq = new_head;
        self.write_session(trx, &created)?;
        for (index, value) in prepared.iter().enumerate() {
            let seq = u64::try_from(index)
                .ok()
                .and_then(|i| i.checked_add(1))
                .ok_or(StoreError::Storage(crate::StorageError::SequenceOverflow))?;
            trx.set(&self.keys().event(id, seq), value);
        }
        let head = self
            .session(trx, old)
            .await?
            .head_seq
            .checked_add(1)
            .ok_or(StoreError::Storage(crate::StorageError::SequenceOverflow))?;
        trx.set(&self.keys().event(old, head), archived_value);
        Ok(())
    }

    /// The successor of an archived main session, if it has been summarized.
    /// # Errors
    /// Returns database or decoding failures.
    pub async fn next_session(&self, id: SessionId) -> Result<Option<SessionId>> {
        self.transaction(
            |trx| async move { read(&trx, &self.keys().session_chain("next", id)).await },
        )
        .await
    }

    /// The conversation whose summary opened this session.
    /// # Errors
    /// Returns database or decoding failures.
    pub async fn previous_session(&self, id: SessionId) -> Result<Option<SessionId>> {
        self.transaction(|trx| async move {
            read(&trx, &self.keys().session_chain("previous", id)).await
        })
        .await
    }

    pub(crate) async fn read_agent(
        &self,
        trx: &Transaction,
        id: AgentId,
    ) -> Result<Option<AgentRecord>> {
        trx.get(&self.keys().agent(id), false)
            .await?
            .map(|value| Ok(decode::<AgentRecord>(&value)?))
            .transpose()
    }

    /// List sessions attached to an agent by id, strictly after the cursor.
    /// The index includes all sessions created by this version of the store.
    /// # Errors
    /// Rejects invalid limits and returns storage or hydration failures.
    pub async fn list_sessions_by_agent(
        &self,
        agent: AgentId,
        after: Option<SessionId>,
        limit: usize,
    ) -> Result<Vec<SessionRecord>> {
        check_limit(limit)?;
        let ids: Vec<SessionId> = self
            .transaction(|trx| async move {
                let (mut begin, end) = self.keys().session_by_agent_space(agent).range();
                if let Some(id) = after {
                    begin = self.keys().session_by_agent(agent, id);
                    begin = crate::next_cursor(&begin);
                }
                scan(&trx, (begin, end), limit)
                    .await?
                    .into_iter()
                    .map(|(_, value)| decode(&value).map_err(Into::into))
                    .collect()
            })
            .await?;
        let mut sessions = Vec::with_capacity(ids.len());
        for id in ids {
            sessions.push(
                self.fetch_session(id)
                    .await?
                    .ok_or(StoreError::Storage(crate::StorageError::Corrupt))?,
            );
        }
        Ok(sessions)
    }
}

fn validate_github_token(token: Option<&str>) -> Result<()> {
    if token.is_some_and(|token| {
        token.is_empty() || token.len() > 4096 || !token.bytes().all(|byte| byte.is_ascii_graphic())
    }) {
        return Err(StoreError::Domain(crate::DomainError::InvalidGithubToken));
    }
    Ok(())
}

/// Opening summary plus the recent turns a side successor keeps.
fn side_messages(
    opening: &swarmy_core::Message,
    tail: &[swarmy_core::Message],
) -> Vec<swarmy_core::Message> {
    let mut messages = Vec::with_capacity(1 + tail.len());
    messages.push(opening.clone());
    messages.extend(tail.iter().cloned());
    messages
}

#[cfg(test)]
mod tests {
    use super::*;
    use swarmy_core::{ManifestId, encode};

    #[test]
    fn current_agent_record_has_fixed_bytes() {
        let record = AgentRecord {
            agent_id: AgentId::from_ulid(ulid::Ulid::from(0_u128)),
            name: "current".into(),
            image: ImageRecord {
                name: "base".into(),
                tag: ImageTag("test".into()),
                manifest_id: ManifestId::from_ulid(ulid::Ulid::from(0_u128)),
            },
            description: String::new(),
            created_at: Timestamp::UNIX_EPOCH,
            main_session: None,
            system_prompt: None,
            model: None,
            reasoning_effort: None,
            provider: None,
            route: None,
            requirements: swarmy_core::SandboxRequirements::default(),
        };
        let bytes = encode(&record).unwrap();
        assert_eq!(
            bytes,
            [
                1, 26, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
                48, 48, 48, 48, 48, 48, 48, 7, 99, 117, 114, 114, 101, 110, 116, 4, 98, 97, 115,
                101, 4, 116, 101, 115, 116, 26, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
                48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 0, 20, 49, 57, 55, 48, 45, 48,
                49, 45, 48, 49, 84, 48, 48, 58, 48, 48, 58, 48, 48, 90, 0, 0, 0, 0, 0, 128, 6, 0,
                0
            ]
        );
        assert_eq!(decode::<AgentRecord>(&bytes).unwrap(), record);
    }
}
