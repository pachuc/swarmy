use crate::{Result, Store, StoreError, read, scan, write};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use swarmy_core::{
    AgentId, Event, GpuRequirement, Lease, Message, MessageId, MessageRole, Part, PlacementRecord,
    SessionId, SessionState,
};

/// How long a session waits before trying again to find a node.
pub const CAPACITY_WAIT_RETRY: std::time::Duration = std::time::Duration::from_secs(30);

/// Why a parked session waits, and since when. One row per session survives a wake.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionWait {
    pub since: Timestamp,
    pub wake_at: Timestamp,
    pub kind: WaitKind,
}
/// The kind of wait that currently blocks a session.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WaitKind {
    /// Inference could not run: a breaker, exhausted route, or missing gateway.
    Inference(InferenceHistory),
    /// Tool dispatch found no live node that can host the computer.
    Capacity { requirement: GpuRequirement },
}
/// Failures accumulated across one inference wait.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InferenceHistory {
    pub last_failure_seq: u64,
    pub reasons: Vec<String>,
    pub attempts: u32,
}
/// What a capacity park did. The notice is boxed to keep expiry cheap.
#[derive(Clone, Debug)]
pub enum CapacityPark {
    Parked { notice: Option<Box<Event>> },
    Expired,
}
impl InferenceHistory {
    pub(crate) fn record_reason(&mut self, reason: &str) {
        let summary: String = reason.chars().take(256).collect();
        if !self.reasons.contains(&summary) && self.reasons.len() < 32 {
            self.reasons.push(summary);
        }
    }
}
impl SessionWait {
    #[must_use]
    pub fn inference_new(since: Timestamp, wake_at: Timestamp) -> Self {
        Self {
            since,
            wake_at,
            kind: WaitKind::Inference(InferenceHistory::default()),
        }
    }
    #[must_use]
    pub fn inference(&self) -> Option<&InferenceHistory> {
        if let WaitKind::Inference(history) = &self.kind {
            Some(history)
        } else {
            None
        }
    }
    pub(crate) fn inference_mut(&mut self) -> &mut InferenceHistory {
        if !matches!(self.kind, WaitKind::Inference(_)) {
            self.kind = WaitKind::Inference(InferenceHistory::default());
        }
        let WaitKind::Inference(history) = &mut self.kind else {
            unreachable!("kind was replaced above")
        };
        history
    }
    #[must_use]
    pub fn capacity(&self) -> Option<GpuRequirement> {
        if let WaitKind::Capacity { requirement } = self.kind {
            Some(requirement)
        } else {
            None
        }
    }
    #[must_use]
    pub fn reasons(&self) -> Vec<String> {
        match &self.kind {
            WaitKind::Inference(history) => history.reasons.clone(),
            WaitKind::Capacity { requirement } => {
                vec![swarmy_core::capacity_wait_reason(*requirement)]
            }
        }
    }
}
/// A session parked because no node could host its GPU computer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GpuCapacityWait {
    pub session_id: SessionId,
    pub agent_id: AgentId,
    pub agent_name: Option<String>,
    pub requirement: GpuRequirement,
    pub since: Timestamp,
    /// Set when the agent asked for the card itself with `request_gpu`.
    pub gpu_request: Option<swarmy_core::GpuRequest>,
    /// The agent already holds a GPU placement; its wait clears when its tools dispatch.
    pub placed: bool,
}

/// One provider failure being parked by the worker.
pub struct InferenceFailureWait<'a> {
    pub seq: u64,
    pub reason: &'a str,
    pub wake_at: Timestamp,
}
/// One session's parking and lease rows read together, so a checker sees no
/// torn view of a park. Test-only, also available with `test-support`.
#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Debug)]
pub struct LeaseAudit {
    pub session_id: SessionId,
    pub state: SessionState,
    /// A `session_wait` row exists. The row outlives a wake, so a parked
    /// session is `state == Sleeping && waiting`.
    pub waiting: bool,
    /// A worker `lease` row exists.
    pub leased: bool,
}

enum WakeRule {
    Due,
    Capacity,
}

impl Store {
    /// Every GPU capacity wait of a live session, oldest first.
    /// # Errors
    /// Storage or decoding errors.
    pub async fn gpu_capacity_waits(&self) -> Result<Vec<GpuCapacityWait>> {
        let space = self.keys().session_wait_space();
        let (begin, end) = space.range();
        let mut waits = Vec::new();
        for (key, value) in self.scan_all_pages(begin, end).await? {
            let (bytes,): (Vec<u8>,) = space.unpack(&key)?;
            let id = crate::keys::session_id(&bytes)?;
            let wait = swarmy_core::decode::<SessionWait>(&value)?;
            let Some(requirement) = wait.capacity().filter(|r| r.needs_card()) else {
                continue;
            };
            if let Some(row) = self
                .transaction(|trx| async move {
                    let Some((session, _)) = self.fetch_session_in(&trx, id).await? else {
                        return Ok(None);
                    };
                    if !matches!(
                        session.state,
                        SessionState::Sleeping | SessionState::Runnable | SessionState::Leased
                    ) {
                        return Ok(None);
                    }
                    let agent = self.read_agent(&trx, session.agent_id).await?;
                    let placed =
                        read::<PlacementRecord>(&trx, &self.keys().placement(session.agent_id))
                            .await?
                            .is_some_and(|p| p.gpu.needs_card());
                    Ok(Some(GpuCapacityWait {
                        session_id: id,
                        agent_id: session.agent_id,
                        agent_name: agent.as_ref().map(|a| a.name.clone()),
                        requirement,
                        since: wait.since,
                        gpu_request: agent.and_then(|a| a.gpu_request),
                        placed,
                    }))
                })
                .await?
            {
                waits.push(row);
            }
        }
        waits.sort_by_key(|w| (w.since, w.session_id));
        Ok(waits)
    }

    /// Read each session's state, session wait, and lease in one transaction.
    /// # Errors
    /// Returns missing-session, decoding, or storage failures.
    /// Test-only entry point, also available with the `test-support` feature.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn lease_audit(&self, ids: &[SessionId]) -> Result<Vec<LeaseAudit>> {
        self.transaction(|trx| async move {
            let mut rows = Vec::with_capacity(ids.len());
            for id in ids {
                rows.push(LeaseAudit {
                    session_id: *id,
                    state: self.session(&trx, *id).await?.state,
                    waiting: read::<SessionWait>(&trx, &self.keys().session_wait(*id))
                        .await?
                        .is_some(),
                    leased: read::<Lease>(&trx, &self.keys().lease(*id))
                        .await?
                        .is_some(),
                });
            }
            Ok(rows)
        })
        .await
    }

    /// Remove both wait keys without reading them; callers already hold the wait.
    pub(crate) fn clear_wait(
        &self,
        trx: &foundationdb::Transaction,
        id: SessionId,
        wait: &SessionWait,
    ) {
        trx.clear(&self.keys().session_wait_due(wait.wake_at, id));
        trx.clear(&self.keys().session_wait(id));
    }

    /// Read a session's current wait of either kind.
    /// # Errors
    /// Returns storage failures.
    pub async fn session_wait(&self, id: SessionId) -> Result<Option<SessionWait>> {
        self.transaction(|trx| async move { read(&trx, &self.keys().session_wait(id)).await })
            .await
    }

    /// Park a runnable session before it can submit work to an open entry.
    /// # Errors
    /// Returns storage failures or an invalid session state.
    pub async fn park_runnable_for_breaker(
        &self,
        id: SessionId,
        reason: &str,
        until: Timestamp,
        now: Timestamp,
        max_wait: std::time::Duration,
    ) -> Result<bool> {
        self.transaction(|trx| async move {
            let session = self.session(&trx, id).await?;
            if session.state != SessionState::Runnable {
                return Ok(false);
            }
            if session.interrupt_requested {
                return Ok(false);
            }
            let key = self.keys().session_wait(id);
            let existing = read::<SessionWait>(&trx, &key).await?;
            if existing.as_ref().is_some_and(|w| w.capacity().is_some()) {
                return Ok(false);
            }
            let mut wait = existing.unwrap_or_else(|| SessionWait::inference_new(now, until));
            let limit = wait
                .since
                .checked_add(max_wait)
                .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?;
            if now >= limit {
                return Ok(false);
            }
            wait.inference_mut().record_reason(reason);
            wait.wake_at = until.min(limit);
            write(&trx, &key, &wait)?;
            write(&trx, &self.keys().session_wait_due(wait.wake_at, id), &())?;
            self.transition(&trx, session, SessionState::Sleeping, now)
                .await?;
            Ok(true)
        })
        .await
    }

    /// Release a worker lease and park the session until the retry time.
    /// # Errors
    /// Rejects a stale lease or returns storage failures.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn park_inference(
        &self,
        id: SessionId,
        lease: &Lease,
        failure: &InferenceFailureWait<'_>,
        now: Timestamp,
        max_wait: std::time::Duration,
    ) -> Result<bool> {
        self.transaction(|trx| async move {
            let session = self.session(&trx, id).await?;
            if session.state != SessionState::Leased
                || self.verify_lease(&trx, id, lease).await?.expires_at <= now
            {
                return Err(StoreError::Fence(crate::FenceError::LeaseMismatch));
            }
            self.park_leased_in(&trx, id, session, failure, now, max_wait)
                .await
        })
        .await
    }

    /// Park a session whose lease was already checked in this transaction.
    /// Route failover and the test-only parked path record the same wait history.
    pub(crate) async fn park_leased_in(
        &self,
        trx: &foundationdb::Transaction,
        id: SessionId,
        session: crate::StoredSession,
        failure: &InferenceFailureWait<'_>,
        now: Timestamp,
        max_wait: std::time::Duration,
    ) -> Result<bool> {
        let key = self.keys().session_wait(id);
        let mut wait = read::<SessionWait>(trx, &key)
            .await?
            .unwrap_or_else(|| SessionWait::inference_new(now, failure.wake_at));
        let previous_seq = wait.inference().map_or(0, |h| h.last_failure_seq);
        if previous_seq != failure.seq {
            let history = wait.inference_mut();
            history.attempts = history.attempts.saturating_add(1);
            history.record_reason(failure.reason);
        }
        let limit = wait
            .since
            .checked_add(max_wait)
            .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?;
        if previous_seq != 0 {
            self.clear_wait(trx, id, &wait);
        }
        wait.inference_mut().last_failure_seq = failure.seq;
        wait.wake_at = if now >= limit {
            now
        } else {
            failure.wake_at.min(limit)
        };
        write(trx, &key, &wait)?;
        write(trx, &self.keys().session_wait_due(wait.wake_at, id), &())?;
        self.transition(trx, session, SessionState::Sleeping, now)
            .await?;
        Ok(true)
    }

    /// Scan the next page of session waits ready to wake.
    /// # Errors
    /// Returns storage or decoding failures.
    pub async fn scan_due_waits(&self, now: Timestamp) -> Result<Vec<SessionId>> {
        let space = self.keys().session_wait_due_space_root();
        let end = self.keys().session_wait_due_space(now).range().1;
        Ok(self
            .due_page(space.range().0, end)
            .await?
            .into_iter()
            .map(|(_, id)| id)
            .collect())
    }

    async fn due_page(&self, begin: Vec<u8>, end: Vec<u8>) -> Result<Vec<(Vec<u8>, SessionId)>> {
        let space = self.keys().session_wait_due_space_root();
        self.transaction(|trx| {
            let space = space.clone();
            let begin = begin.clone();
            let end = end.clone();
            async move {
                scan(&trx, (begin, end), crate::MAX_SCAN_LIMIT)
                    .await?
                    .into_iter()
                    .map(|(key, _)| {
                        let (_, id): ((i64, i32), Vec<u8>) = space.unpack(&key)?;
                        Ok((key, crate::keys::session_id(&id)?))
                    })
                    .collect()
            }
        })
        .await
    }

    /// Wake a due sleeping session, remove its due index and set `wake_at` to now.
    /// # Errors
    /// Returns storage failures.
    pub async fn wake_wait(&self, id: SessionId, now: Timestamp) -> Result<bool> {
        self.transaction(|trx| async move { self.wake_in(&trx, id, now, WakeRule::Due).await })
            .await
    }

    async fn wake_in(
        &self,
        trx: &foundationdb::Transaction,
        id: SessionId,
        now: Timestamp,
        rule: WakeRule,
    ) -> Result<bool> {
        let key = self.keys().session_wait(id);
        let Some(mut wait) = read::<SessionWait>(trx, &key).await? else {
            return Ok(false);
        };
        if match rule {
            WakeRule::Due => wait.wake_at > now,
            WakeRule::Capacity => wait.capacity().is_none(),
        } {
            return Ok(false);
        }
        self.clear_wait(trx, id, &wait);
        wait.wake_at = now;
        write(trx, &key, &wait)?;
        let session = self.session(trx, id).await?;
        if session.state == SessionState::Sleeping {
            self.transition(trx, session, SessionState::Runnable, now)
                .await?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Wake all capacity waits immediately. A capacity wait's due time is never
    /// later than its park plus the retry, so this window includes every one.
    /// Callers with a bus may nudge the woken sessions.
    /// # Errors
    /// Returns storage or decoding failures.
    pub async fn wake_capacity_waits(&self, now: Timestamp) -> Result<Vec<SessionId>> {
        let space = self.keys().session_wait_due_space_root();
        let mut begin = space.range().0;
        let end = self
            .keys()
            .session_wait_due_space(
                now.checked_add(CAPACITY_WAIT_RETRY)
                    .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?,
            )
            .range()
            .1;
        let mut woken = Vec::new();
        loop {
            let page = self.due_page(begin.clone(), end.clone()).await?;
            if page.is_empty() {
                break;
            }
            for (_, id) in &page {
                if self
                    .transaction(|trx| async move {
                        self.wake_in(&trx, *id, now, WakeRule::Capacity).await
                    })
                    .await?
                {
                    woken.push(*id);
                }
            }
            if let Some((key, _)) = page.last() {
                begin = crate::next_cursor(key);
            }
            if page.len() < crate::MAX_SCAN_LIMIT {
                break;
            }
        }
        Ok(woken)
    }

    /// Park a leased session whose tool dispatch found no eligible node.
    /// # Errors
    /// Rejects stale leases and storage failures.
    pub async fn park_for_capacity(
        &self,
        id: SessionId,
        lease: &Lease,
        requirement: GpuRequirement,
        now: Timestamp,
        max_wait: std::time::Duration,
    ) -> Result<CapacityPark> {
        self.transaction(|trx| async move {
            self.check_worker_lease(&trx, id, lease, now).await?;
            let previous = read::<SessionWait>(&trx, &self.keys().session_wait(id)).await?;
            let since = previous
                .as_ref()
                .filter(|w| w.capacity().is_some())
                .map_or(now, |w| w.since);
            let limit = since
                .checked_add(max_wait)
                .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?;
            if now >= limit {
                if let Some(wait) = previous {
                    self.clear_wait(&trx, id, &wait);
                }
                return Ok(CapacityPark::Expired);
            }
            let first = previous.as_ref().is_none_or(|w| w.capacity().is_none());
            if let Some(wait) = previous {
                self.clear_wait(&trx, id, &wait);
            }
            let wake_at = now
                .checked_add(CAPACITY_WAIT_RETRY)
                .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?
                .min(limit);
            let wait = SessionWait {
                since,
                wake_at,
                kind: WaitKind::Capacity { requirement },
            };
            write(&trx, &self.keys().session_wait(id), &wait)?;
            write(&trx, &self.keys().session_wait_due(wake_at, id), &())?;
            let mut session = self.session(&trx, id).await?;
            let notice = if first {
                session.head_seq = crate::seq_after(session.head_seq, 1)?;
                let event = Event::MessageAppended {
                    seq: session.head_seq,
                    message: Message {
                        id: MessageId::from_ulid(ulid::Ulid::generate()),
                        role: MessageRole::System,
                        parts: vec![Part::Text {
                            text: swarmy_core::capacity_wait_notice(requirement, max_wait),
                        }],
                    },
                };
                let bytes = self.prepare(&event).await?;
                trx.set(&self.keys().event(id, session.head_seq), &bytes);
                Some(Box::new(event))
            } else {
                None
            };
            self.transition(&trx, session, SessionState::Sleeping, now)
                .await?;
            Ok(CapacityPark::Parked { notice })
        })
        .await
    }

    /// Clear a wait of either kind on interruption or terminal failure.
    /// # Errors
    /// Returns storage failures.
    pub async fn clear_session_wait(&self, id: SessionId) -> Result<()> {
        self.clear_wait_kind(id, false).await
    }

    pub(crate) async fn clear_capacity_wait_in(
        &self,
        trx: &foundationdb::Transaction,
        id: SessionId,
    ) -> Result<()> {
        if let Some(wait) = read::<SessionWait>(trx, &self.keys().session_wait(id)).await?
            && wait.capacity().is_some()
        {
            self.clear_wait(trx, id, &wait);
        }
        Ok(())
    }

    /// Remove a completed inference wait and its due index, preserving a
    /// capacity wait. A completed attempt restarts the route chain, so the next turn
    /// probes from the first step again.
    /// # Errors
    /// Returns storage failures.
    pub async fn clear_inference_wait(&self, id: SessionId) -> Result<()> {
        self.clear_wait_kind(id, true).await
    }

    async fn clear_wait_kind(&self, id: SessionId, only_inference: bool) -> Result<()> {
        self.transaction(|trx| async move {
            if let Some(wait) = read::<SessionWait>(&trx, &self.keys().session_wait(id)).await?
                && (!only_inference || wait.inference().is_some())
            {
                self.clear_wait(&trx, id, &wait);
            }
            let mut session = self.session(&trx, id).await?;
            session.route_step = 0;
            self.write_session(&trx, &session)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use swarmy_core::{decode, encode};

    #[test]
    fn session_wait_has_fixed_bytes() {
        let ts = [
            49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 84, 48, 48, 58, 48, 48, 58, 48, 48, 90,
        ];
        let capacity = SessionWait {
            since: Timestamp::UNIX_EPOCH,
            wake_at: Timestamp::UNIX_EPOCH,
            kind: WaitKind::Capacity {
                requirement: GpuRequirement::Shared,
            },
        };
        let inference = SessionWait {
            since: Timestamp::UNIX_EPOCH,
            wake_at: Timestamp::UNIX_EPOCH,
            kind: WaitKind::Inference(InferenceHistory {
                last_failure_seq: 7,
                reasons: vec!["x".into()],
                attempts: 2,
            }),
        };
        let capacity_bytes = [vec![1, 20], ts.to_vec(), vec![20], ts.to_vec(), vec![1, 1]].concat();
        let inference_bytes = [
            vec![1, 20],
            ts.to_vec(),
            vec![20],
            ts.to_vec(),
            vec![0, 7, 1, 1, 120, 2],
        ]
        .concat();
        for (wait, expected) in [(capacity, capacity_bytes), (inference, inference_bytes)] {
            let encoded = encode(&wait).unwrap();
            assert_eq!(encoded, expected);
            assert_eq!(decode::<SessionWait>(&encoded).unwrap(), wait);
        }
    }
}
