use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use swarmy_core::{Lease, SessionId, SessionState};

use crate::{Result, Store, StoreError, read, scan, write};

/// Breaker identity: one record per auth entry. A rate limit on one key opens
/// only that key's breaker and leaves the provider's other entries closed.
/// Entries without a stored label (environment keys, ambient host chains, the
/// fake provider) share the provider's unlabeled record.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CredentialKey {
    pub provider: String,
    pub label: Option<String>,
}

impl CredentialKey {
    /// The shared record for a provider without a stored entry.
    #[must_use]
    pub fn provider(provider: &str) -> Self {
        Self {
            provider: provider.to_owned(),
            label: None,
        }
    }

    /// The record for one labelled auth entry.
    #[must_use]
    pub fn entry(provider: &str, label: &str) -> Self {
        Self {
            provider: provider.to_owned(),
            label: Some(label.to_owned()),
        }
    }

    /// The record for a resolution that may or may not carry an entry label.
    #[must_use]
    pub fn for_label(provider: &str, label: Option<String>) -> Self {
        Self {
            provider: provider.to_owned(),
            label,
        }
    }

    /// Display identity used in waiting reasons: `provider/label`, or the
    /// provider alone for the shared unlabeled record.
    #[must_use]
    pub fn name(&self) -> String {
        self.label.as_ref().map_or_else(
            || self.provider.clone(),
            |label| format!("{}/{}", self.provider, label),
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Breaker {
    pub open_until: Timestamp,
    pub failures: u32,
    pub probe_until: Option<Timestamp>,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InferenceWait {
    pub since: Timestamp,
    pub wake_at: Timestamp,
    pub last_failure_seq: u64,
    pub reasons: Vec<String>,
    pub attempts: u32,
}

/// One provider failure being parked by the worker.
pub struct InferenceFailureWait<'a> {
    pub seq: u64,
    pub reason: &'a str,
    pub wake_at: Timestamp,
}

impl Store {
    /// Breaker records live under `(provider, label)`. The previous
    /// provider-only tuple is never read, so open provider-keyed breakers are
    /// dropped at upgrade; a stale one only costs one probe.
    fn breaker_key(&self, key: &CredentialKey) -> Vec<u8> {
        self.root.pack(&(
            "inference_breaker",
            key.provider.as_str(),
            key.label.as_deref().unwrap_or(""),
        ))
    }

    pub(crate) fn wait_key(&self, id: SessionId) -> Vec<u8> {
        self.root
            .pack(&("inference_wait", id.as_ulid().to_bytes().as_slice()))
    }

    pub(crate) fn wait_due_key(&self, id: SessionId, at: Timestamp) -> Vec<u8> {
        self.root.pack(&(
            "inference_wait_due",
            (at.as_second(), at.subsec_nanosecond()),
            id.as_ulid().to_bytes().as_slice(),
        ))
    }

    /// Grant one entry probe after the open period, or return the next eligible time.
    /// # Errors
    /// Returns storage failures.
    pub async fn claim_entry(
        &self,
        key: &CredentialKey,
        now: Timestamp,
    ) -> Result<Option<Timestamp>> {
        self.transaction(|trx| async move {
            let Some(mut breaker) = read::<Breaker>(&trx, &self.breaker_key(key)).await? else {
                return Ok(None);
            };
            if breaker.open_until > now {
                return Ok(Some(breaker.open_until));
            }
            if breaker.probe_until.is_some_and(|until| until > now) {
                return Ok(Some(
                    now.checked_add(std::time::Duration::from_secs(1))
                        .map_err(|_| StoreError::Corrupt)?,
                ));
            }
            breaker.probe_until = Some(
                now.checked_add(std::time::Duration::from_secs(120))
                    .map_err(|_| StoreError::Corrupt)?,
            );
            write(&trx, &self.breaker_key(key), &breaker)?;
            Ok(None)
        })
        .await
    }

    /// Read the time after which the entry can accept another request.
    /// # Errors
    /// Returns storage failures.
    pub async fn entry_open_until(
        &self,
        key: &CredentialKey,
        now: Timestamp,
    ) -> Result<Option<Timestamp>> {
        self.transaction(|trx| async move {
            Ok(read::<Breaker>(&trx, &self.breaker_key(key))
                .await?
                .and_then(|breaker| {
                    if breaker.open_until > now {
                        Some(breaker.open_until)
                    } else if breaker.probe_until.is_some_and(|until| until > now) {
                        now.checked_add(std::time::Duration::from_secs(1)).ok()
                    } else {
                        None
                    }
                }))
        })
        .await
    }

    /// Open the entry breaker after a retryable failure.
    /// # Errors
    /// Returns storage failures.
    pub async fn entry_failure(
        &self,
        key: &CredentialKey,
        until: Timestamp,
        reason: &str,
    ) -> Result<()> {
        self.transaction(|trx| async move {
            let mut breaker = read::<Breaker>(&trx, &self.breaker_key(key))
                .await?
                .unwrap_or(Breaker {
                    open_until: until,
                    failures: 0,
                    probe_until: None,
                    reason: String::new(),
                });
            breaker.failures = breaker.failures.saturating_add(1);
            breaker.open_until = breaker.open_until.max(until);
            breaker.probe_until = None;
            breaker.reason = reason.chars().take(256).collect();
            write(&trx, &self.breaker_key(key), &breaker)
        })
        .await
    }

    /// Count consecutive failures for exponential backoff.
    /// # Errors
    /// Returns storage failures.
    pub async fn entry_failures(&self, key: &CredentialKey) -> Result<u32> {
        self.transaction(|trx| async move {
            Ok(read::<Breaker>(&trx, &self.breaker_key(key))
                .await?
                .map_or(0, |b| b.failures))
        })
        .await
    }

    /// Read the entry text recorded with the breaker.
    /// # Errors
    /// Returns storage failures.
    pub async fn entry_reason(&self, key: &CredentialKey) -> Result<Option<String>> {
        self.transaction(|trx| async move {
            Ok(read::<Breaker>(&trx, &self.breaker_key(key))
                .await?
                .map(|b| b.reason))
        })
        .await
    }

    /// Close the breaker after a successful probe.
    /// # Errors
    /// Returns storage failures.
    pub async fn entry_success(&self, key: &CredentialKey) -> Result<()> {
        self.transaction(|trx| async move {
            let key = self.breaker_key(key);
            if read::<Breaker>(&trx, &key)
                .await?
                .is_some_and(|breaker| breaker.probe_until.is_some())
            {
                trx.clear(&key);
            }
            Ok(())
        })
        .await
    }

    /// Read a session's current inference wait and accumulated reasons.
    /// # Errors
    /// Returns storage failures.
    pub async fn inference_wait(&self, id: SessionId) -> Result<Option<InferenceWait>> {
        self.transaction(|trx| async move { read(&trx, &self.wait_key(id)).await })
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
            if read::<bool>(&trx, &self.interrupt_key(id))
                .await?
                .unwrap_or(false)
            {
                return Ok(false);
            }
            let key = self.wait_key(id);
            let mut wait = read::<InferenceWait>(&trx, &key)
                .await?
                .unwrap_or(InferenceWait {
                    since: now,
                    wake_at: until,
                    last_failure_seq: 0,
                    reasons: Vec::new(),
                    attempts: 0,
                });
            let limit = wait
                .since
                .checked_add(max_wait)
                .map_err(|_| StoreError::Corrupt)?;
            if now >= limit {
                return Ok(false);
            }
            let summary: String = reason.chars().take(256).collect();
            if !wait.reasons.contains(&summary) && wait.reasons.len() < 32 {
                wait.reasons.push(summary);
            }
            wait.wake_at = until.min(limit);
            write(&trx, &key, &wait)?;
            write(&trx, &self.wait_due_key(id, wait.wake_at), &())?;
            self.transition(&trx, session, SessionState::Sleeping, now)
                .await?;
            Ok(true)
        })
        .await
    }

    /// Release a worker lease and park the session until the retry time.
    /// # Errors
    /// Rejects a stale lease or returns storage failures.
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
                return Err(StoreError::LeaseMismatch);
            }
            let key = self.wait_key(id);
            let mut wait = read::<InferenceWait>(&trx, &key)
                .await?
                .unwrap_or(InferenceWait {
                    since: now,
                    wake_at: failure.wake_at,
                    last_failure_seq: 0,
                    reasons: Vec::new(),
                    attempts: 0,
                });
            if wait.last_failure_seq != failure.seq {
                wait.attempts = wait.attempts.saturating_add(1);
                let summary: String = failure.reason.chars().take(256).collect();
                if !wait.reasons.contains(&summary) && wait.reasons.len() < 32 {
                    wait.reasons.push(summary);
                }
            }
            let limit = wait
                .since
                .checked_add(max_wait)
                .map_err(|_| StoreError::Corrupt)?;
            if wait.last_failure_seq != 0 {
                trx.clear(&self.wait_due_key(id, wait.wake_at));
            }
            wait.last_failure_seq = failure.seq;
            wait.wake_at = if now >= limit {
                now
            } else {
                failure.wake_at.min(limit)
            };
            write(&trx, &key, &wait)?;
            write(&trx, &self.wait_due_key(id, wait.wake_at), &())?;
            self.transition(&trx, session, SessionState::Sleeping, now)
                .await?;
            Ok(true)
        })
        .await
    }

    /// Scan the next page of inference waits ready to wake.
    /// # Errors
    /// Returns storage or decoding failures.
    pub async fn scan_due_inference_waits(&self, now: Timestamp) -> Result<Vec<SessionId>> {
        let space = self.root.subspace(&("inference_wait_due",));
        let end = space
            .subspace(&((now.as_second(), now.subsec_nanosecond()),))
            .range()
            .1;
        self.transaction(|trx| {
            let space = space.clone();
            let end = end.clone();
            async move {
                scan(&trx, (space.range().0, end), crate::MAX_SCAN_LIMIT)
                    .await?
                    .into_iter()
                    .map(|(key, _)| {
                        let (_, id): ((i64, i32), Vec<u8>) =
                            space.unpack(&key).map_err(|_| StoreError::Corrupt)?;
                        crate::keys::session_id(id)
                    })
                    .collect()
            }
        })
        .await
    }

    /// Wake a due sleeping session and remove its due index entry.
    /// # Errors
    /// Returns storage failures.
    pub async fn wake_inference_wait(&self, id: SessionId, now: Timestamp) -> Result<bool> {
        self.transaction(|trx| async move {
            let key = self.wait_key(id);
            let Some(wait) = read::<InferenceWait>(&trx, &key).await? else {
                return Ok(false);
            };
            if wait.wake_at > now {
                return Ok(false);
            }
            trx.clear(&self.wait_due_key(id, wait.wake_at));
            let session = self.session(&trx, id).await?;
            if session.state == SessionState::Sleeping {
                self.transition(&trx, session, SessionState::Runnable, now)
                    .await?;
                return Ok(true);
            }
            Ok(false)
        })
        .await
    }

    /// Remove a completed turn's wait and any remaining due index entry.
    /// # Errors
    /// Returns storage failures.
    pub async fn clear_inference_wait(&self, id: SessionId) -> Result<()> {
        self.transaction(|trx| async move {
            if let Some(wait) = read::<InferenceWait>(&trx, &self.wait_key(id)).await? {
                trx.clear(&self.wait_due_key(id, wait.wake_at));
                trx.clear(&self.wait_key(id));
            }
            Ok(())
        })
        .await
    }
}
