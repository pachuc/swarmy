use foundationdb::Transaction;
use jiff::Timestamp;
use swarmy_core::{Lease, LeaseOwnerId, RunnableEntry, SessionId, SessionState, can_transition};

use crate::{
    Result, Store, StoreError, StoredSession, check_limit, keys::session_id, read, scan, write,
};

impl Store {
    /// Wake an Idle session and return its previous state.
    /// Unlike an unconditional `set_state`, this cannot wake a session that
    /// concurrently started waiting for inference, tools, or a timer.
    /// Repeated requests leave the runnable entry and its wake time intact.
    /// # Errors
    /// Returns missing-session and transaction errors.
    pub async fn wake_session(&self, id: SessionId, now: Timestamp) -> Result<SessionState> {
        self.transaction(|trx| async move {
            let session = self.session(&trx, id).await?;
            if session.state == SessionState::Idle {
                self.transition(&trx, session, SessionState::Runnable, now)
                    .await?;
                Ok(SessionState::Idle)
            } else {
                Ok(session.state)
            }
        })
        .await
    }

    fn lease_key(&self, id: SessionId) -> Vec<u8> {
        self.root
            .pack(&("lease", id.as_ulid().to_bytes().as_slice()))
    }

    fn expiry_key(&self, id: SessionId, expires: Timestamp) -> Vec<u8> {
        self.root.pack(&(
            "lease_by_expiry",
            (expires.as_second(), expires.subsec_nanosecond()),
            id.as_ulid().to_bytes().as_slice(),
        ))
    }

    async fn clear_lease(&self, trx: &Transaction, id: SessionId) -> Result<()> {
        if let Some(lease) = read::<Lease>(trx, &self.lease_key(id)).await? {
            trx.clear(&self.expiry_key(id, lease.expires_at));
            trx.clear(&self.lease_key(id));
        }
        Ok(())
    }

    fn store_lease(&self, trx: &Transaction, id: SessionId, lease: &Lease) -> Result<()> {
        write(trx, &self.lease_key(id), lease)?;
        write(trx, &self.expiry_key(id, lease.expires_at), lease)
    }

    /// Claim only a Runnable session and atomically transition it to Leased.
    /// A fresh owner id identifies each worker incarnation. The supplied expiry
    /// may be in the past so recovery can be tested without reading a local clock.
    /// # Errors
    /// Rejects missing or non-Runnable sessions and transaction failures.
    pub async fn claim_lease(
        &self,
        id: SessionId,
        owner: LeaseOwnerId,
        expires_at: Timestamp,
    ) -> Result<Lease> {
        self.transaction(|trx| async move { Ok(self.claim(&trx, id, owner, expires_at).await?.0) })
            .await
    }

    /// Claim work and read its session and turn from the same database version.
    /// # Errors
    /// Rejects non-runnable sessions and storage or snapshot decoding failures.
    pub async fn claim_step(
        &self,
        id: SessionId,
        owner: LeaseOwnerId,
        expires_at: Timestamp,
    ) -> Result<(
        Lease,
        swarmy_core::SessionRecord,
        Option<swarmy_core::MessageId>,
    )> {
        let (lease, session, turn, _) = self.claim_step_with_tail(id, owner, expires_at).await?;
        Ok((lease, session, turn))
    }

    /// Claim and fetch the first replay page at the same database version.
    /// Subsequent pages use `read_events` and stop at the returned session head.
    /// # Errors
    /// Rejects non-runnable sessions and storage or decoding failures.
    pub async fn claim_step_with_tail(
        &self,
        id: SessionId,
        owner: LeaseOwnerId,
        expires_at: Timestamp,
    ) -> Result<(
        Lease,
        swarmy_core::SessionRecord,
        Option<swarmy_core::MessageId>,
        Vec<swarmy_core::Event>,
    )> {
        let (lease, session, snapshot, turn, values) = self
            .transaction(|trx| async move {
                let turn_key = self.turn_key(id);
                let ((lease, session), turn) = futures::try_join!(
                    self.claim(&trx, id, owner, expires_at),
                    read(&trx, &turn_key),
                )?;
                let space = self.event_space(id);
                let mut begin = space.pack(&(session.snapshot_seq.unwrap_or(0),));
                begin.push(0);
                let (snapshot, values) = futures::try_join!(
                    async {
                        Ok::<_, StoreError>(if let Some(seq) = session.snapshot_seq {
                            Some(
                                trx.get(&self.snapshot_key(id, seq), false)
                                    .await?
                                    .ok_or(StoreError::Corrupt)?
                                    .to_vec(),
                            )
                        } else {
                            None
                        })
                    },
                    scan(&trx, (begin, space.range().1), crate::MAX_SCAN_LIMIT),
                )?;
                let session = self.session_metadata(&trx, session).await?;
                Ok((lease, session, snapshot, turn, values))
            })
            .await?;
        let mut events = Vec::with_capacity(values.len());
        for (_, value) in values {
            events.push(self.hydrate(&value).await?);
        }
        Ok((
            lease,
            self.hydrate_session(session, snapshot).await?,
            turn,
            events,
        ))
    }

    async fn claim(
        &self,
        trx: &Transaction,
        id: SessionId,
        owner: LeaseOwnerId,
        expires_at: Timestamp,
    ) -> Result<(Lease, StoredSession)> {
        let (mut session, ()) =
            futures::try_join!(self.session(trx, id), self.remove_runnable(trx, id),)?;
        if session.state != SessionState::Runnable {
            return Err(StoreError::InvalidState);
        }
        if read::<bool>(trx, &self.interrupt_key(id))
            .await?
            .unwrap_or(false)
        {
            return Err(StoreError::InvalidState);
        }
        let lease = Lease {
            owner,
            expires_at,
            seq: session
                .head_seq
                .checked_add(1)
                .ok_or(StoreError::SequenceOverflow)?,
        };
        self.store_lease(trx, id, &lease)?;
        session.state = SessionState::Leased;
        write(trx, &self.session_state_since_key(id), &Timestamp::now())?;
        write(trx, &self.session_key(id), &session)?;
        Ok((lease, session))
    }

    pub(crate) async fn verify_lease(
        &self,
        trx: &Transaction,
        id: SessionId,
        expected: &Lease,
    ) -> Result<Lease> {
        let lease = read::<Lease>(trx, &self.lease_key(id))
            .await?
            .ok_or(StoreError::LeaseMismatch)?;
        if &lease != expected {
            return Err(StoreError::LeaseMismatch);
        }
        Ok(lease)
    }

    /// Renew a still-live lease using the full previous record as a fencing token.
    /// # Errors
    /// Rejects expired or replaced leases and non-increasing expiry times.
    pub async fn renew_lease(
        &self,
        id: SessionId,
        expected: &Lease,
        now: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Lease> {
        self.transaction(|trx| async move {
            let mut lease = self.verify_lease(&trx, id, expected).await?;
            if lease.expires_at <= now || expires_at <= lease.expires_at {
                return Err(StoreError::LeaseMismatch);
            }
            self.clear_lease(&trx, id).await?;
            lease.expires_at = expires_at;
            self.store_lease(&trx, id, &lease)?;
            Ok(lease)
        })
        .await
    }

    /// Release a live lease and return the session to Runnable.
    /// # Errors
    /// Rejects expired or replaced leases and transaction failures.
    pub async fn release_lease(
        &self,
        id: SessionId,
        expected: &Lease,
        now: Timestamp,
    ) -> Result<()> {
        self.set_state(id, SessionState::Runnable, Some(expected), now)
            .await
    }

    /// Change state and update both indexes atomically. Leaving Leased requires
    /// its live token; entering Leased is only possible through `claim_lease`.
    /// # Errors
    /// Rejects invalid transitions, stale lease tokens, and transaction failures.
    pub async fn set_state(
        &self,
        id: SessionId,
        state: SessionState,
        lease: Option<&Lease>,
        now: Timestamp,
    ) -> Result<()> {
        self.transaction(|trx| async move {
            let session = self.session(&trx, id).await?;
            if state == SessionState::Leased || !can_transition(session.state, state) {
                return Err(StoreError::InvalidState);
            }
            if session.state == SessionState::Leased {
                let expected = lease.ok_or(StoreError::LeaseMismatch)?;
                if self.verify_lease(&trx, id, expected).await?.expires_at <= now {
                    return Err(StoreError::LeaseMismatch);
                }
            } else if lease.is_some() {
                return Err(StoreError::LeaseMismatch);
            }
            self.transition(&trx, session, state, now).await
        })
        .await
    }

    pub(crate) async fn transition(
        &self,
        trx: &Transaction,
        mut session: StoredSession,
        state: SessionState,
        now: Timestamp,
    ) -> Result<()> {
        // Index ownership follows the state machine. Idle and waiting sessions
        // have neither index, so reading those absent rows adds network waits.
        match session.state {
            SessionState::Leased => self.clear_lease(trx, session.session_id).await?,
            SessionState::Runnable => self.remove_runnable(trx, session.session_id).await?,
            _ => {}
        }
        if state == SessionState::Idle {
            write(trx, &self.session_idle_key(session.session_id), &now)?;
        }
        session.state = state;
        write(trx, &self.session_state_since_key(session.session_id), &now)?;
        if state == SessionState::Runnable {
            self.write_runnable(
                trx,
                &RunnableEntry {
                    session_id: session.session_id,
                    priority: 0,
                    wake_at: now,
                },
            )?;
        }
        write(trx, &self.session_key(session.session_id), &session)
    }

    /// Return a page of leases expiring at or before `now`, ordered by expiry/id.
    /// The cursor is the last `(session_id, lease)` returned by the previous page.
    /// # Errors
    /// Rejects invalid limits, malformed keys, and transaction failures.
    pub async fn scan_expired_leases(
        &self,
        now: Timestamp,
        after: Option<&(SessionId, Lease)>,
        limit: usize,
    ) -> Result<Vec<(SessionId, Lease)>> {
        check_limit(limit)?;
        self.transaction(|trx| async move {
            let space = self.root.subspace(&("lease_by_expiry",));
            let mut begin = space.range().0;
            if let Some((id, lease)) = after {
                begin = self.expiry_key(*id, lease.expires_at);
                begin.push(0);
            }
            let end = space
                .subspace(&((now.as_second(), now.subsec_nanosecond()),))
                .range()
                .1;
            if begin >= end {
                return Ok(Vec::new());
            }
            let mut leases = Vec::new();
            for (key, value) in scan(&trx, (begin, end), limit).await? {
                let (_, id): ((i64, i32), Vec<u8>) =
                    space.unpack(&key).map_err(|_| StoreError::Corrupt)?;
                leases.push((session_id(id)?, swarmy_core::decode(&value)?));
            }
            Ok(leases)
        })
        .await
    }

    /// Recheck an expired scan result and return that session to Runnable.
    /// # Errors
    /// Rejects renewed/replaced leases, live leases, and transaction failures.
    pub async fn reap_lease(&self, id: SessionId, expected: &Lease, now: Timestamp) -> Result<()> {
        self.transaction(|trx| async move {
            let lease = self.verify_lease(&trx, id, expected).await?;
            let session = self.session(&trx, id).await?;
            if lease.expires_at > now || session.state != SessionState::Leased {
                return Err(StoreError::LeaseMismatch);
            }
            self.transition(&trx, session, SessionState::Runnable, now)
                .await
        })
        .await
    }
}

impl Store {
    pub(crate) async fn check_worker_lease(
        &self,
        trx: &Transaction,
        id: SessionId,
        lease: &Lease,
        now: Timestamp,
    ) -> Result<()> {
        let (current, session) =
            futures::try_join!(self.verify_lease(trx, id, lease), self.session(trx, id),)?;
        if current.expires_at <= now || session.state != SessionState::Leased {
            return Err(StoreError::LeaseMismatch);
        }
        Ok(())
    }
}
