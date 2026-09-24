//! Named identities and session metadata. Side rows preserve legacy postcard headers.
use crate::{Result, Store, StoreError, StoredSession, check_limit, read, scan, write};
use foundationdb::Transaction;
use jiff::Timestamp;
use swarmy_core::{
    AgentId, AgentRecord, AgentSettings, ImageRecord, ImageTag, SessionId, SessionKind,
    SessionRecord, SessionState, decode,
};

struct CreationOptions<'a> {
    github_token: Option<&'a str>,
    replay_key: Option<&'a str>,
}

impl Store {
    pub(crate) async fn session_kind(
        &self,
        trx: &Transaction,
        id: SessionId,
    ) -> Result<SessionKind> {
        Ok(read(trx, &self.session_kind_key(id))
            .await?
            .unwrap_or_default())
    }

    /// Create a named agent, resolving and pinning the registered image atomically.
    /// # Errors
    /// Rejects duplicate names or ids, invalid names, unknown images, and storage failures.
    pub async fn create_agent(
        &self,
        name: &str,
        image: &str,
        description: &str,
        now: Timestamp,
    ) -> Result<AgentRecord> {
        self.create_agent_with(
            name,
            image,
            description,
            &AgentSettings::default(),
            None,
            now,
        )
        .await
    }

    /// Create a named agent with inference overrides, pinning its image atomically.
    /// # Errors
    /// Rejects duplicate names or ids, invalid names, unknown images, and storage failures.
    pub async fn create_agent_with_settings(
        &self,
        name: &str,
        image: &str,
        description: &str,
        settings: &AgentSettings,
        now: Timestamp,
    ) -> Result<AgentRecord> {
        self.create_agent_with(name, image, description, settings, None, now)
            .await
    }

    /// Create an agent and its private GitHub token in one transaction.
    /// The token is a side field so legacy records and public views never contain it.
    /// # Errors
    /// Rejects invalid credentials and the same conditions as `create_agent`.
    pub async fn create_agent_with_github_token(
        &self,
        name: &str,
        image: &str,
        description: &str,
        github_token: Option<&str>,
        now: Timestamp,
    ) -> Result<AgentRecord> {
        self.create_agent_with(
            name,
            image,
            description,
            &AgentSettings::default(),
            github_token,
            now,
        )
        .await
    }

    /// Create a named agent with inference overrides and a private GitHub token,
    /// writing the public record and the token side field in one transaction.
    /// # Errors
    /// Rejects duplicate names or ids, invalid names or credentials, unknown images,
    /// and storage failures.
    pub async fn create_agent_with(
        &self,
        name: &str,
        image: &str,
        description: &str,
        settings: &AgentSettings,
        github_token: Option<&str>,
        now: Timestamp,
    ) -> Result<AgentRecord> {
        self.create_agent_with_replay(
            name,
            image,
            description,
            settings,
            now,
            CreationOptions {
                github_token,
                replay_key: None,
            },
        )
        .await
    }

    /// Create an agent and its replay marker in the same transaction. A retry
    /// after an unknown commit returns the original record instead of another agent.
    /// # Errors
    /// Returns validation and storage errors.
    pub async fn create_agent_with_settings_replay(
        &self,
        name: &str,
        image: &str,
        description: &str,
        settings: &AgentSettings,
        now: Timestamp,
        key: &str,
    ) -> Result<AgentRecord> {
        self.create_agent_with_replay(
            name,
            image,
            description,
            settings,
            now,
            CreationOptions {
                github_token: None,
                replay_key: Some(key),
            },
        )
        .await
    }

    async fn create_agent_with_replay(
        &self,
        name: &str,
        image: &str,
        description: &str,
        settings: &AgentSettings,
        now: Timestamp,
        options: CreationOptions<'_>,
    ) -> Result<AgentRecord> {
        let CreationOptions {
            github_token,
            replay_key,
        } = options;
        validate_github_token(github_token)?;
        if name.is_empty() || name.chars().any(char::is_control) {
            return Err(StoreError::InvalidAgentName);
        }
        if name.len() > 1024 || description.len() > crate::INLINE_LIMIT / 2 {
            return Err(StoreError::TooLarge);
        }
        let id = AgentId::from_ulid(ulid::Ulid::generate());
        let result = self
            .transaction(|trx| async move {
                if let Some(key) = replay_key {
                    let replay_key = self.root.pack(&("api_idempotency", key));
                    if let Some(previous) =
                        read::<crate::api_idempotency::ApiReplay>(&trx, &replay_key).await?
                        && previous.expires_at > now
                    {
                        return serde_json::from_str(&previous.result)
                            .map_err(|_| StoreError::Corrupt);
                    }
                }
                if trx.get(&self.agent_name_key(name), false).await?.is_some()
                    || trx.get(&self.agent_key(id), false).await?.is_some()
                {
                    return Err(StoreError::AgentExists);
                }
                let image = self.resolve_image(&trx, image).await?;
                let default_memory: Option<u64> = read(
                    &trx,
                    &self.image_memory_key(&image.name, &image.tag, image.manifest_id),
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
                    requirements: swarmy_core::SandboxRequirements {
                        memory_mib: settings.memory_mib.or(default_memory).unwrap_or(768),
                        gpu: settings.gpu.unwrap_or_default(),
                    },
                };
                write(&trx, &self.agent_key(id), &record)?;
                if let Some(token) = github_token {
                    write(&trx, &self.agent_github_token_key(id), &token)?;
                }
                write(&trx, &self.agent_name_key(name), &id)?;
                if let Some(key) = replay_key {
                    let result = serde_json::to_string(&record).map_err(|_| StoreError::Corrupt)?;
                    write(
                        &trx,
                        &self.root.pack(&("api_idempotency", key)),
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
        if matches!(result, Err(StoreError::ImageMissing { .. })) {
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
            match read(&trx, &self.agent_name_key(name)).await? {
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
            if trx.get(&self.agent_key(id), false).await?.is_none() {
                return Ok(None);
            }
            read(&trx, &self.agent_github_token_key(id)).await
        })
        .await
    }

    /// Rotate or clear an agent's private GitHub token.
    /// # Errors
    /// Rejects unknown agents, invalid tokens, and database failures.
    pub async fn set_agent_github_token(&self, id: AgentId, token: Option<&str>) -> Result<()> {
        validate_github_token(token)?;
        self.transaction(|trx| async move {
            if trx.get(&self.agent_key(id), false).await?.is_none() {
                return Err(StoreError::AgentMissing);
            }
            let key = self.agent_github_token_key(id);
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
            let (mut begin, end) = self.root.subspace(&("agent",)).range();
            if let Some(id) = after {
                begin = self.agent_key(id);
                begin.push(0);
            }
            scan(&trx, (begin, end), limit)
                .await?
                .into_iter()
                .map(|(_, value)| decode_agent(&value))
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
    /// Rejects unknown agents, oversized records, and storage failures.
    pub async fn set_agent_with_resets(
        &self,
        id: AgentId,
        settings: &AgentSettings,
        resets: &[swarmy_core::InferenceField],
    ) -> Result<AgentRecord> {
        self.transaction(|trx| async move {
            let mut agent = self
                .read_agent(&trx, id)
                .await?
                .ok_or(StoreError::AgentMissing)?;
            let previous_requirements = agent.requirements;
            settings.apply_to(&mut agent, resets);
            if agent.requirements != previous_requirements
                && read::<swarmy_core::PlacementRecord>(&trx, &self.placement_key("placement", id))
                    .await?
                    .is_some()
            {
                return Err(StoreError::ActiveSandboxRequirements);
            }
            write(&trx, &self.agent_key(id), &agent)?;
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
                trx.clear(&self.agent_name_key(&agent.name));
                trx.clear(&self.agent_key(id));
                trx.clear(&self.agent_github_token_key(id));
            }
            Ok(())
        })
        .await
    }

    /// Create an idle session, minting an anonymous agent or attaching to a named one.
    /// `image` is required for ephemeral sessions and forbidden for named sessions.
    /// # Errors
    /// Rejects unknown agents/images, deleted computers, image overrides, and duplicate sessions.
    pub async fn create_session_for_agent(
        &self,
        id: SessionId,
        agent: Option<AgentId>,
        image: Option<&str>,
        now: Timestamp,
    ) -> Result<SessionRecord> {
        self.create_session_with_inference(
            id,
            agent,
            image,
            now,
            &swarmy_core::InferenceSelection::default(),
        )
        .await
    }

    /// Create a session with immutable inference overrides before its first turn.
    /// # Errors
    /// Rejects invalid agents/images and duplicate sessions.
    pub async fn create_session_with_inference(
        &self,
        id: SessionId,
        agent: Option<AgentId>,
        image: Option<&str>,
        now: Timestamp,
        inference: &swarmy_core::InferenceSelection,
    ) -> Result<SessionRecord> {
        let session = SessionRecord {
            interrupt_requested: false,
            session_id: id,
            agent_id: agent.unwrap_or_else(|| AgentId::from_ulid(ulid::Ulid::generate())),
            kind: agent.map_or(SessionKind::Ephemeral, |agent_id| SessionKind::Named {
                agent_id,
            }),
            computer_deleted: false,
            plan: Vec::new(),
            state: SessionState::Idle,
            head_seq: 0,
            snapshot_ref: None,
            inference: inference.clone(),
        };
        self.create_session_record(&session, now, image).await?;
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
            return Err(StoreError::InvalidState);
        }
        if matches!(session.kind, SessionKind::Named { .. }) && image.is_some() {
            return Err(StoreError::NamedAgentImage);
        }
        let result = self
            .transaction(
                |trx| async move { self.create_session_in(&trx, session, now, image).await },
            )
            .await;
        if matches!(result, Err(StoreError::ImageMissing { .. })) {
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
        let key = self.session_key(id);
        if trx.get(&key, false).await?.is_some() {
            return Err(StoreError::SessionExists);
        }
        self.check_computer(trx, session.agent_id).await?;
        let selected = match session.kind {
            SessionKind::Ephemeral => {
                // The compatibility entry point accepts an existing anonymous id,
                // but must never attach ephemeral lifetime rules to a named identity.
                if trx
                    .get(&self.agent_key(session.agent_id), false)
                    .await?
                    .is_some()
                {
                    return Err(StoreError::InvalidState);
                }
                self.resolve_image(trx, image.ok_or(StoreError::SessionImageRequired)?)
                    .await?
            }
            SessionKind::Named { agent_id } => {
                if agent_id != session.agent_id {
                    return Err(StoreError::InvalidState);
                }
                self.read_agent(trx, agent_id)
                    .await?
                    .ok_or(StoreError::AgentMissing)?
                    .image
            }
        };
        if matches!(session.kind, SessionKind::Ephemeral) {
            let memory: Option<u64> = read(
                trx,
                &self.image_memory_key(&selected.name, &selected.tag, selected.manifest_id),
            )
            .await?
            .flatten();
            write(
                trx,
                &self.computer_memory_key(session.agent_id),
                &memory.unwrap_or(768),
            )?;
        }
        write(trx, &self.session_image_key(id), &selected)?;
        swarmy_core::UpdatePlanArguments {
            plan: session.plan.clone(),
        }
        .validate()
        .map_err(|_| StoreError::InvalidState)?;
        write(trx, &self.session_plan_key(id), &session.plan)?;
        write(trx, &self.session_inference_key(id), &session.inference)?;
        write(trx, &self.session_kind_key(id), &session.kind)?;
        write(trx, &self.session_agent_key(session.agent_id, id), &id)?;
        write(trx, &self.session_idle_key(id), &now)?;
        write(
            trx,
            &key,
            &StoredSession {
                session_id: id,
                agent_id: session.agent_id,
                state: session.state,
                head_seq: 0,
                snapshot_seq: None,
                inference: session.inference.clone(),
                kind: session.kind,
                computer_deleted: false,
                plan: Vec::new(),
                interrupt_requested: false,
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
            .ok_or(StoreError::InvalidImage)?;
        let tag = ImageTag(tag.into());
        let manifest_id = read(trx, &self.image_key(name, &tag))
            .await?
            .ok_or_else(|| StoreError::ImageMissing {
                image: image.into(),
                registered: String::new(),
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
                .ok_or(StoreError::AgentMissing)?;
            self.check_computer(&trx, agent).await?;
            if let Some(main) = record.main_session {
                return Ok((main, false));
            }
            let session = SessionRecord {
                interrupt_requested: false,
                session_id: id,
                agent_id: agent,
                kind: SessionKind::Named { agent_id: agent },
                computer_deleted: false,
                state: SessionState::Idle,
                head_seq: 0,
                snapshot_ref: None,
                inference: swarmy_core::InferenceSelection::default(),
                plan: Vec::new(),
            };
            self.create_session_in(&trx, &session, now, None).await?;
            record.main_session = Some(id);
            write(&trx, &self.agent_key(agent), &record)?;
            Ok((id, true))
        })
        .await
    }

    /// Atomically replace the main pointer with an open session belonging to this agent.
    /// # Errors
    /// Rejects missing agents/sessions, foreign or completed sessions, and deleted computers.
    pub async fn set_main_session(&self, agent: AgentId, id: SessionId) -> Result<()> {
        self.transaction(|trx| async move {
            let mut record = self
                .read_agent(&trx, agent)
                .await?
                .ok_or(StoreError::AgentMissing)?;
            let session = self.session(&trx, id).await?;
            self.check_computer(&trx, agent).await?;
            if session.agent_id != agent
                || self.session_kind(&trx, id).await? != (SessionKind::Named { agent_id: agent })
                || session.state == SessionState::Completed
            {
                return Err(StoreError::InvalidMainSession);
            }
            record.main_session = Some(id);
            write(&trx, &self.agent_key(agent), &record)
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
    ) -> Result<(SessionId, swarmy_core::Event)> {
        if opening.role != swarmy_core::MessageRole::System {
            return Err(StoreError::InvalidState);
        }
        let id = SessionId::from_ulid(ulid::Ulid::generate());
        let event = swarmy_core::Event::MessageAppended {
            seq: 1,
            message: opening.clone(),
        };
        let opening = self.prepare(&event).await?;
        let head = expected_head
            .checked_add(1)
            .ok_or(StoreError::SequenceOverflow)?;
        let archived = swarmy_core::Event::StateChanged {
            seq: head,
            from: SessionState::Leased,
            to: SessionState::Completed,
        };
        let archived_value = self.prepare(&archived).await?;
        self.transaction(|trx| {
            let (opening, archived_value) = (&opening, &archived_value);
            async move {
                let now = Timestamp::now();
                self.check_worker_lease(&trx, old, lease, now).await?;
                let mut previous = self.session(&trx, old).await?;
                if previous.head_seq != expected_head {
                    return Err(StoreError::StaleSequence {
                        expected: expected_head,
                        actual: previous.head_seq,
                    });
                }
                let mut agent = self
                    .read_agent(&trx, previous.agent_id)
                    .await?
                    .ok_or(StoreError::AgentMissing)?;
                if agent.main_session != Some(old) {
                    return Err(StoreError::InvalidMainSession);
                }
                let session = SessionRecord {
                    interrupt_requested: false,
                    session_id: id,
                    agent_id: agent.agent_id,
                    kind: SessionKind::Named {
                        agent_id: agent.agent_id,
                    },
                    computer_deleted: false,
                    state: SessionState::Idle,
                    head_seq: 0,
                    snapshot_ref: None,
                    inference: swarmy_core::InferenceSelection::default(),
                    plan: Vec::new(),
                };
                self.create_session_in(&trx, &session, now, None).await?;
                let mut created = self.session(&trx, id).await?;
                created.head_seq = 1;
                write(&trx, &self.session_key(id), &created)?;
                trx.set(&self.event_space(id).pack(&(1_u64,)), opening);
                trx.set(&self.event_space(old).pack(&(head,)), archived_value);
                previous.head_seq = head;
                self.transition(&trx, previous, SessionState::Completed, now)
                    .await?;
                write(&trx, &self.session_link_key("next", old), &id)?;
                write(&trx, &self.session_link_key("previous", id), &old)?;
                agent.main_session = Some(id);
                write(&trx, &self.agent_key(agent.agent_id), &agent)
            }
        })
        .await?;
        Ok((id, archived))
    }

    fn session_link_key(&self, direction: &str, id: SessionId) -> Vec<u8> {
        self.root.pack(&(
            "session_chain",
            direction,
            id.as_ulid().to_bytes().as_slice(),
        ))
    }

    /// The successor of an archived main session, if it has been summarized.
    /// # Errors
    /// Returns database or decoding failures.
    pub async fn next_session(&self, id: SessionId) -> Result<Option<SessionId>> {
        self.transaction(|trx| async move { read(&trx, &self.session_link_key("next", id)).await })
            .await
    }

    /// The conversation whose summary opened this session.
    /// # Errors
    /// Returns database or decoding failures.
    pub async fn previous_session(&self, id: SessionId) -> Result<Option<SessionId>> {
        self.transaction(
            |trx| async move { read(&trx, &self.session_link_key("previous", id)).await },
        )
        .await
    }

    pub(crate) async fn read_agent(
        &self,
        trx: &Transaction,
        id: AgentId,
    ) -> Result<Option<AgentRecord>> {
        trx.get(&self.agent_key(id), false)
            .await?
            .map(|value| decode_agent(&value))
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
                let (mut begin, end) = self
                    .root
                    .subspace(&("session_by_agent", agent.as_ulid().to_bytes().as_slice()))
                    .range();
                if let Some(id) = after {
                    begin = self.session_agent_key(agent, id);
                    begin.push(0);
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
            sessions.push(self.fetch_session(id).await?.ok_or(StoreError::Corrupt)?);
        }
        Ok(sessions)
    }
}

fn validate_github_token(token: Option<&str>) -> Result<()> {
    if token.is_some_and(|token| {
        token.is_empty() || token.len() > 4096 || !token.bytes().all(|byte| byte.is_ascii_graphic())
    }) {
        return Err(StoreError::InvalidGithubToken);
    }
    Ok(())
}

/// Postcard structs have no field count, so Serde defaults alone cannot read an
/// old record. Only accept the legacy schema when it consumes the entire value,
/// so existing agents acquire no main session or inference overrides on upgrade.
// Legacy postcard layouts require explicit decoding rather than serde defaults.
#[allow(clippy::too_many_lines)]
pub(crate) fn decode_agent(bytes: &[u8]) -> Result<AgentRecord> {
    #[derive(serde::Deserialize)]
    struct LegacyAgent {
        agent_id: AgentId,
        name: String,
        image: ImageRecord,
        description: String,
        created_at: Timestamp,
    }

    // Records written after the main-session pointer landed but before the
    // per-agent settings did carry six fields.
    #[derive(serde::Deserialize)]
    struct MainSessionAgent {
        agent_id: AgentId,
        name: String,
        image: ImageRecord,
        description: String,
        created_at: Timestamp,
        main_session: Option<SessionId>,
    }

    #[derive(serde::Deserialize)]
    struct SettingsAgent {
        agent_id: AgentId,
        name: String,
        image: ImageRecord,
        description: String,
        created_at: Timestamp,
        main_session: Option<SessionId>,
        system_prompt: Option<String>,
        model: Option<String>,
        reasoning_effort: Option<swarmy_core::ReasoningEffort>,
    }

    #[derive(serde::Deserialize)]
    struct ProviderAgent {
        agent_id: AgentId,
        name: String,
        image: ImageRecord,
        description: String,
        created_at: Timestamp,
        main_session: Option<SessionId>,
        system_prompt: Option<String>,
        model: Option<String>,
        reasoning_effort: Option<swarmy_core::ReasoningEffort>,
        provider: Option<String>,
    }

    match decode(bytes) {
        Ok(agent) => Ok(agent),
        Err(error) => {
            if let Ok(old) = decode::<ProviderAgent>(bytes) {
                return Ok(AgentRecord {
                    agent_id: old.agent_id,
                    name: old.name,
                    image: old.image,
                    description: old.description,
                    created_at: old.created_at,
                    main_session: old.main_session,
                    system_prompt: old.system_prompt,
                    model: old.model,
                    reasoning_effort: old.reasoning_effort,
                    provider: old.provider,
                    requirements: swarmy_core::SandboxRequirements::default(),
                });
            }
            if let Ok(old) = decode::<SettingsAgent>(bytes) {
                return Ok(AgentRecord {
                    agent_id: old.agent_id,
                    name: old.name,
                    image: old.image,
                    description: old.description,
                    created_at: old.created_at,
                    main_session: old.main_session,
                    system_prompt: old.system_prompt,
                    model: old.model,
                    reasoning_effort: old.reasoning_effort,
                    provider: None,
                    requirements: swarmy_core::SandboxRequirements::default(),
                });
            }
            if let Ok(old) = decode::<MainSessionAgent>(bytes) {
                return Ok(AgentRecord {
                    agent_id: old.agent_id,
                    name: old.name,
                    image: old.image,
                    description: old.description,
                    created_at: old.created_at,
                    main_session: old.main_session,
                    system_prompt: None,
                    model: None,
                    reasoning_effort: None,
                    provider: None,
                    requirements: swarmy_core::SandboxRequirements::default(),
                });
            }
            match decode::<LegacyAgent>(bytes) {
                Ok(old) => Ok(AgentRecord {
                    agent_id: old.agent_id,
                    name: old.name,
                    image: old.image,
                    description: old.description,
                    created_at: old.created_at,
                    main_session: None,
                    system_prompt: None,
                    model: None,
                    reasoning_effort: None,
                    provider: None,
                    requirements: swarmy_core::SandboxRequirements::default(),
                }),
                Err(_) => Err(error.into()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use swarmy_core::{ManifestId, ReasoningEffort, encode};

    #[test]
    fn main_session_only_records_decode_with_default_settings() {
        let session = swarmy_core::SessionId::from_ulid(ulid::Ulid::generate());
        let bytes = encode(&(
            AgentId::from_ulid(ulid::Ulid::generate()),
            "six".to_owned(),
            ImageRecord {
                name: "base".into(),
                tag: swarmy_core::ImageTag("test".into()),
                manifest_id: ManifestId::from_ulid(ulid::Ulid::generate()),
            },
            String::new(),
            Timestamp::now(),
            Some(session),
        ))
        .unwrap();
        let record = decode_agent(&bytes).unwrap();
        assert_eq!(record.name, "six");
        assert_eq!(record.main_session, Some(session));
        assert!(record.system_prompt.is_none() && record.model.is_none());
        assert!(record.reasoning_effort.is_none());
    }

    #[test]
    fn settings_records_decode_without_provider() {
        let record = AgentRecord {
            agent_id: AgentId::from_ulid(ulid::Ulid::generate()),
            name: "old".into(),
            image: ImageRecord {
                name: "base".into(),
                tag: ImageTag("test".into()),
                manifest_id: ManifestId::from_ulid(ulid::Ulid::generate()),
            },
            description: String::new(),
            created_at: Timestamp::UNIX_EPOCH,
            main_session: None,
            system_prompt: Some("prompt".into()),
            model: Some("gpt-5.5".into()),
            reasoning_effort: Some(ReasoningEffort::Max),
            provider: None,
            requirements: swarmy_core::SandboxRequirements::default(),
        };
        let bytes = encode(&(
            record.agent_id,
            &record.name,
            &record.image,
            &record.description,
            record.created_at,
            record.main_session,
            &record.system_prompt,
            &record.model,
            record.reasoning_effort,
        ))
        .unwrap();
        assert_eq!(decode_agent(&bytes).unwrap(), record);
        assert_eq!(decode_agent(&encode(&record).unwrap()).unwrap(), record);
    }

    #[test]
    fn malformed_extended_agent_is_not_treated_as_a_legacy_record() {
        let record = AgentRecord {
            agent_id: AgentId::from_ulid(ulid::Ulid::generate()),
            name: "test".into(),
            image: ImageRecord {
                name: "base".into(),
                tag: ImageTag("test".into()),
                manifest_id: ManifestId::from_ulid(ulid::Ulid::generate()),
            },
            description: String::new(),
            created_at: Timestamp::UNIX_EPOCH,
            main_session: Some(SessionId::from_ulid(ulid::Ulid::generate())),
            system_prompt: Some("prompt".into()),
            model: Some("model".into()),
            reasoning_effort: Some(ReasoningEffort::High),
            provider: Some("openai".into()),
            requirements: swarmy_core::SandboxRequirements::default(),
        };
        let mut bytes = encode(&record).unwrap();
        assert_eq!(decode_agent(&bytes).unwrap(), record);
        bytes.pop();
        assert!(decode_agent(&bytes).is_err());
        let mut bytes = encode(&record).unwrap();
        bytes.push(0);
        assert!(decode_agent(&bytes).is_err());
    }
}
