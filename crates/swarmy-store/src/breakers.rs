use jiff::Timestamp;
use serde::{Deserialize, Serialize};

use crate::{Result, Store, StoreError, read, write};

/// How long a granted entry probe stays visibly open after its window ends,
/// so waiters see one stable deadline instead of a flapping breaker.
pub(crate) const BREAKER_PROBE_GRACE: std::time::Duration = std::time::Duration::from_secs(1);
/// How long a granted probe may spend one request before the entry opens again.
pub(crate) const BREAKER_PROBE_WINDOW: std::time::Duration = std::time::Duration::from_secs(120);

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
pub(crate) struct Breaker {
    pub open_until: Timestamp,
    pub failures: u32,
    pub probe_until: Option<Timestamp>,
    pub reason: String,
}

/// One pool candidate with its live breaker record, read in a single
/// transaction by `Store::breaker_snapshot` for a scheduler tick. `reason`
/// is present only while the breaker is open.
#[derive(Clone, Debug)]
pub struct BreakerCandidate {
    pub key: CredentialKey,
    pub open_until: Option<Timestamp>,
    pub reason: Option<String>,
}

/// Split a breaker record into its open retry time and stored reason. A
/// record inside its probe window reads as open for one more second so only
/// one probe runs at a time. Shared by route snapshots and the scheduler's
/// tick cache so both see the same open steps.
pub(crate) fn open_state(
    breaker: Option<&Breaker>,
    now: Timestamp,
) -> (Option<Timestamp>, Option<String>) {
    let open_until = breaker.and_then(|record| {
        if record.open_until > now {
            Some(record.open_until)
        } else if record.probe_until.is_some_and(|until| until > now) {
            now.checked_add(BREAKER_PROBE_GRACE).ok()
        } else {
            None
        }
    });
    let reason = breaker
        .filter(|_| open_until.is_some())
        .map(|record| record.reason.clone());
    (open_until, reason)
}

impl Store {
    /// Breaker records live under `(provider, label)`. The previous
    /// provider-only tuple is never read, so open provider-keyed breakers are
    /// dropped at upgrade; a stale one only costs one probe.
    pub(crate) fn breaker_key(&self, key: &CredentialKey) -> Vec<u8> {
        self.keys()
            .inference_breaker(key.provider.as_str(), key.label.as_deref().unwrap_or(""))
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
                    now.checked_add(BREAKER_PROBE_GRACE)
                        .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?,
                ));
            }
            breaker.probe_until = Some(
                now.checked_add(BREAKER_PROBE_WINDOW)
                    .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?,
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
                        now.checked_add(BREAKER_PROBE_GRACE).ok()
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
}
