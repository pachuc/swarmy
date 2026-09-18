//! Named identities and session metadata. Side rows preserve legacy postcard headers.
use crate::{Result, Store, StoreError, StoredSession, check_limit, read, scan, write};
use foundationdb::Transaction;
use jiff::Timestamp;
use swarmy_core::{
    AgentId, AgentRecord, AgentSettings, ImageRecord, ImageTag, SessionId, SessionKind,
    SessionRecord, SessionState, decode,
};

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
        self.create_agent_with_settings(name, image, description, &AgentSettings::default(), now)
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
        if name.is_empty() || name.chars().any(char::is_control) {
            return Err(StoreError::InvalidAgentName);
        }
        if name.len() > 1024 || description.len() > crate::INLINE_LIMIT / 2 {
            return Err(StoreError::TooLarge);
        }
        let id = AgentId::from_ulid(ulid::Ulid::generate());
        let result = self
            .transaction(|trx| async move {
                if trx.get(&self.agent_name_key(name), false).await?.is_some()
                    || trx.get(&self.agent_key(id), false).await?.is_some()
                {
                    return Err(StoreError::AgentExists);
                }
                let record = AgentRecord {
                    agent_id: id,
                    name: name.into(),
                    image: self.resolve_image(&trx, image).await?,
                    description: description.into(),
                    created_at: now,
                    main_session: None,
                    system_prompt: settings.system_prompt.clone(),
                    model: settings.model.clone(),
                    reasoning_effort: settings.reasoning_effort,
                };
                write(&trx, &self.agent_key(id), &record)?;
                write(&trx, &self.agent_name_key(name), &id)?;
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
        self.transaction(|trx| async move {
            let mut agent = self
                .read_agent(&trx, id)
                .await?
                .ok_or(StoreError::AgentMissing)?;
            if let Some(prompt) = &settings.system_prompt {
                agent.system_prompt = Some(prompt.clone());
            }
            if let Some(model) = &settings.model {
                agent.model = Some(model.clone());
            }
            if let Some(effort) = settings.reasoning_effort {
                agent.reasoning_effort = Some(effort);
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
        let session = SessionRecord {
            session_id: id,
            agent_id: agent.unwrap_or_else(|| AgentId::from_ulid(ulid::Ulid::generate())),
            kind: agent.map_or(SessionKind::Ephemeral, |agent_id| SessionKind::Named {
                agent_id,
            }),
            computer_deleted: false,
            state: SessionState::Idle,
            head_seq: 0,
            snapshot_ref: None,
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

    async fn create_session_in(
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
        write(trx, &self.session_image_key(id), &selected)?;
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
                kind: session.kind,
                computer_deleted: false,
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
                session_id: id,
                agent_id: agent,
                kind: SessionKind::Named { agent_id: agent },
                computer_deleted: false,
                state: SessionState::Idle,
                head_seq: 0,
                snapshot_ref: None,
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

/// Postcard structs have no field count, so Serde defaults alone cannot read an
/// old record. Only accept the legacy schema when it consumes the entire value,
/// so existing agents acquire no main session or inference overrides on upgrade.
pub(crate) fn decode_agent(bytes: &[u8]) -> Result<AgentRecord> {
    #[derive(serde::Deserialize)]
    struct LegacyAgent {
        agent_id: AgentId,
        name: String,
        image: ImageRecord,
        description: String,
        created_at: Timestamp,
    }

    match decode(bytes) {
        Ok(agent) => Ok(agent),
        Err(error) => match decode::<LegacyAgent>(bytes) {
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
            }),
            Err(_) => Err(error.into()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use swarmy_core::{ManifestId, ReasoningEffort, encode};

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
