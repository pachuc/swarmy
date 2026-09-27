//! Encrypted credentials and fenced refresh transactions.
use std::{future::Future, time::Duration};

use chacha20poly1305::{
    XChaCha20Poly1305, XNonce,
    aead::{Aead, KeyInit, Payload},
};
use jiff::Timestamp;
use rand::TryRngCore;
use serde::{Deserialize, Serialize};
use swarmy_config::Keyring;
use swarmy_core::{
    AgentId, CredentialKind, CredentialRecord, CredentialScope, CredentialStatus, Lease,
    LeaseOwnerId, decode, encode,
};

#[cfg(any(test, feature = "test-support"))]
use crate::{BreakerCandidate, CredentialKey, inference_wait::Breaker};
use crate::{Result, Store, StoreError, read, scan, write};
use foundationdb::RetryableTransaction;

/// No secrets are returned by list operations. Each encrypted value is read once.
#[derive(Debug, Serialize)]
pub struct CredentialSummary {
    pub provider: String,
    pub kind: String,
    pub label: String,
    pub status: CredentialStatus,
    pub updated_at: Timestamp,
    pub created_at: Timestamp,
    pub last_used_at: Option<Timestamp>,
    pub expires_at: Option<Timestamp>,
}

impl CredentialSummary {
    #[must_use]
    pub fn new(provider: String, record: &CredentialRecord, now: Timestamp) -> Self {
        Self {
            provider,
            kind: entry_kind(record),
            label: "default".into(),
            status: record.status(now),
            updated_at: record.updated_at,
            created_at: record.updated_at,
            last_used_at: None,
            expires_at: match record.kind {
                CredentialKind::OAuth { expires_at, .. } => Some(expires_at),
                CredentialKind::ApiKey { .. } => None,
            },
        }
    }
}

fn entry_identity(provider: &str, label: &str) -> String {
    format!("{provider}\0{label}")
}

/// Invert the scope display encoding (`cluster` or `agent:<ULID>`) used in
/// the legacy `("credential", scope, provider)` keys. Returns `None` for
/// corrupt encodings so the boot migration leaves the row in place.
fn parse_scope(text: &str) -> Option<CredentialScope> {
    if text == "cluster" {
        return Some(CredentialScope::Cluster);
    }
    let id = text.strip_prefix("agent:")?;
    let ulid: ulid::Ulid = id.parse().ok()?;
    Some(CredentialScope::Agent(AgentId::from_ulid(ulid)))
}

fn entry_kind(record: &CredentialRecord) -> String {
    match &record.kind {
        CredentialKind::OAuth { .. } => "subscription",
        CredentialKind::ApiKey { .. } if record.bookkeeping.cloud => "cloud",
        CredentialKind::ApiKey { .. } => "api-key",
    }
    .into()
}

#[derive(Serialize, Deserialize)]
pub(crate) struct EntryValue {
    pub(crate) created_at: Timestamp,
    pub(crate) last_used_at: Option<Timestamp>,
    pub(crate) ciphertext: Vec<u8>,
    /// Plaintext copy of the record's login state, so the scheduler can skip
    /// entries needing login without decrypting. The flag is stable: unlike
    /// expiry it never changes with time.
    #[serde(default)]
    pub(crate) needs_login: bool,
    /// Plaintext OAuth expiry, so the scheduler can skip expired entries
    /// without decrypting. Absent for API keys, which do not expire.
    #[serde(default)]
    pub(crate) expires_at: Option<Timestamp>,
}

pub(crate) fn decode_entry(bytes: &[u8]) -> Result<EntryValue> {
    Ok(decode::<EntryValue>(bytes)?)
}

async fn read_entry(trx: &RetryableTransaction, key: &[u8]) -> Result<Option<EntryValue>> {
    trx.get(key, false)
        .await?
        .map(|value| decode_entry(&value))
        .transpose()
}

/// Split a record's status into the stable login flag and the time-dependent
/// expiry, so the scheduler can reconstruct readiness without decrypting.
/// The reconstruction matches `CredentialRecord::status`: the login arms of
/// that check never consult the clock, and only OAuth records expire.
fn entry_readiness(record: &CredentialRecord, now: Timestamp) -> (bool, Option<Timestamp>) {
    let needs_login = record.status(now) == CredentialStatus::NeedsLogin;
    let expires_at = match record.kind {
        CredentialKind::OAuth { expires_at, .. } => Some(expires_at),
        CredentialKind::ApiKey { .. } => None,
    };
    (needs_login, expires_at)
}

/// Scheduler view of one entry's readiness from its plaintext hints.
pub(crate) fn entry_ready(
    needs_login: bool,
    expires_at: Option<Timestamp>,
    now: Timestamp,
) -> bool {
    !needs_login && expires_at.is_none_or(|expiry| expiry > now)
}

#[derive(Clone)]
pub struct CredentialStore {
    store: Store,
    keyring: Keyring,
}

impl Store {
    #[must_use]
    pub fn credentials(&self, keyring: Keyring) -> CredentialStore {
        CredentialStore {
            store: self.clone(),
            keyring,
        }
    }

    /// Probe presence without needing the key, so gateways can diagnose a missing keyring.
    /// # Errors
    /// Returns database errors.
    pub async fn has_credential(&self, scope: CredentialScope, provider: &str) -> Result<bool> {
        Ok(self
            .credential_fingerprint(scope, provider)
            .await?
            .is_some())
    }

    /// Entry labels for breaker checks, without decrypting.
    /// # Errors
    /// Returns database or decoding errors.
    /// Test-only entry point, also available with the `test-support` feature.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn credential_entry_labels(
        &self,
        scope: CredentialScope,
        provider: &str,
    ) -> Result<Vec<String>> {
        let space =
            crate::keys::Keys::new(&self.root).credential_entry_space_provider(scope, provider);
        let (begin, end) = space.range();
        let rows = self
            .transaction(|trx| {
                let range = (begin.clone(), end.clone());
                async move { scan(&trx, range, crate::MAX_SCAN_LIMIT).await }
            })
            .await?;
        let mut labels = Vec::new();
        for (key, _) in rows {
            let (label,): (String,) = space
                .unpack(&key)
                .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?;
            labels.push(label);
        }
        labels.sort();
        Ok(labels)
    }

    /// One-transaction snapshot of a provider's breaker pool for a scheduler
    /// tick: the ready entries (or every entry when none is ready, matching
    /// the gateway pool), each with its live breaker record. Without stored
    /// entries the provider shares one unlabeled record. The
    /// entry listing pages past `MAX_SCAN_LIMIT` inside the same transaction
    /// instead of truncating at 64 entries.
    /// # Errors
    /// Returns database or decoding errors.
    /// Test-only entry point, also available with the `test-support` feature.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn breaker_snapshot(
        &self,
        scope: CredentialScope,
        provider: &str,
        now: Timestamp,
    ) -> Result<Vec<BreakerCandidate>> {
        self.transaction(|trx| async move {
            let space =
                crate::keys::Keys::new(&self.root).credential_entry_space_provider(scope, provider);
            let (mut begin, end) = space.range();
            let mut entries: Vec<(String, bool)> = Vec::new();
            loop {
                let rows = scan(&trx, (begin.clone(), end.clone()), crate::MAX_SCAN_LIMIT).await?;
                let complete = rows.len() < crate::MAX_SCAN_LIMIT;
                for (key, value) in rows {
                    let (label,): (String,) = space
                        .unpack(&key)
                        .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?;
                    let entry = decode_entry(&value)?;
                    entries.push((label, entry_ready(entry.needs_login, entry.expires_at, now)));
                    begin = key;
                    begin.push(0);
                }
                if complete {
                    break;
                }
            }
            // Mirror the gateway pool: ready entries when any is ready, else
            // every entry so a provider with no usable key still parks behind
            // its breaker instead of spinning through the worker.
            let any_ready = entries.iter().any(|(_, ready)| *ready);
            let mut keys = Vec::new();
            for (label, ready) in &entries {
                if *ready || !any_ready {
                    keys.push(CredentialKey::entry(provider, label));
                }
            }
            if keys.is_empty() {
                keys.push(CredentialKey::provider(provider));
            }
            let mut candidates = Vec::with_capacity(keys.len());
            for key in &keys {
                let breaker: Option<Breaker> = read(&trx, &self.breaker_key(key)).await?;
                let open_until = breaker.as_ref().and_then(|record| {
                    if record.open_until > now {
                        Some(record.open_until)
                    } else if record.probe_until.is_some_and(|until| until > now) {
                        now.checked_add(Duration::from_secs(1)).ok()
                    } else {
                        None
                    }
                });
                candidates.push(BreakerCandidate {
                    key: key.clone(),
                    reason: breaker
                        .filter(|_| open_until.is_some())
                        .map(|record| record.reason),
                    open_until,
                });
            }
            Ok(candidates)
        })
        .await
    }

    /// Fingerprint all entries so a change to any labelled entry invalidates gateway state.
    /// # Errors
    /// Returns database or decoding errors.
    pub async fn credential_fingerprint(
        &self,
        scope: CredentialScope,
        provider: &str,
    ) -> Result<Option<[u8; 32]>> {
        let space =
            crate::keys::Keys::new(&self.root).credential_entry_space_provider(scope, provider);
        let (mut begin, end) = space.range();
        let mut hash = blake3::Hasher::new();
        let mut found = false;
        loop {
            let rows = self
                .transaction(|trx| {
                    let range = (begin.clone(), end.clone());
                    async move { scan(&trx, range, crate::MAX_SCAN_LIMIT).await }
                })
                .await?;
            if rows.is_empty() {
                break;
            }
            for (key, bytes) in rows {
                found = true;
                hash.update(&key);
                let entry: EntryValue = decode_entry(&bytes)?;
                hash.update(&entry.ciphertext);
                begin = key;
                begin.push(0);
            }
        }
        Ok(found.then(|| *hash.finalize().as_bytes()))
    }

    /// Count rows written by the retired single-record credential API at
    /// `("credential", scope, provider)`. Entry storage replaced those rows.
    /// Boot migrates them with
    /// [`CredentialStore::migrate_legacy_credentials`]; any row left after
    /// that failed to decrypt with the local keyring. `swarmy doctor`
    /// reports this count; the message states the count.
    /// # Errors
    /// Returns database errors.
    pub async fn legacy_credential_count(&self) -> Result<u64> {
        let space = crate::keys::Keys::new(&self.root).credential_space();
        let (mut begin, end) = space.range();
        let mut count: u64 = 0;
        loop {
            let rows = self
                .transaction(|trx| {
                    let range = (begin.clone(), end.clone());
                    async move { scan(&trx, range, crate::MAX_SCAN_LIMIT).await }
                })
                .await?;
            let complete = rows.len() < crate::MAX_SCAN_LIMIT;
            count += u64::try_from(rows.len()).unwrap_or(u64::MAX - count);
            if complete {
                break;
            }
            if let Some((last, _)) = rows.last() {
                begin.clone_from(last);
                begin.push(0);
            } else {
                break;
            }
        }
        Ok(count)
    }

    /// Migrate retired single-record credential rows without failing boot.
    /// Each `("credential", scope, provider)` row is decrypted with the
    /// cluster keyring and written as the provider's `default` entry when no
    /// such entry exists; the legacy row is cleared in the same transaction.
    /// Rows are read in bounded batches. Rows that fail to decrypt
    /// (wrong or missing key) are left in place and skipped, so a later boot
    /// with the right keyring can still migrate them. Returns the written and
    /// cleared counts for the startup log; database errors still propagate so
    /// the caller can log them, but callers must not refuse to start.
    /// # Errors
    /// Returns database, encoding, or encryption errors.
    pub async fn migrate_legacy_credentials(&self, keyring: &Keyring) -> Result<LegacyMigration> {
        self.credentials(keyring.clone())
            .migrate_legacy_credentials()
            .await
    }
}

/// Outcome of the one-shot boot migration of retired single-record
/// credential rows. `written` counts rows that produced a new `default`
/// entry; `cleared` counts rows cleared without writing because the
/// `default` entry already existed. Rows that fail to decrypt are left in
/// place and counted in neither.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LegacyMigration {
    pub written: u64,
    pub cleared: u64,
}

impl CredentialStore {
    /// Add or replace a labelled entry without changing its creation order.
    /// # Errors
    /// Returns encryption, encoding, or database errors.
    pub async fn put_entry(
        &self,
        scope: CredentialScope,
        provider: &str,
        label: &str,
        record: &CredentialRecord,
    ) -> Result<()> {
        let mut record = record.clone();
        record.migrate_bookkeeping();
        let ciphertext = encrypt(
            &self.keyring,
            scope,
            &entry_identity(provider, label),
            &record,
        )?;
        let key = crate::keys::Keys::new(&self.store.root).credential_entry(scope, provider, label);
        let (needs_login, expires_at) = entry_readiness(&record, self.store.now());
        self.store
            .transaction(|trx| {
                let key = &key;
                let ciphertext = &ciphertext;
                async move {
                    let previous: Option<EntryValue> = read_entry(&trx, key).await?;
                    write(
                        &trx,
                        key,
                        &EntryValue {
                            created_at: previous
                                .map_or_else(|| self.store.now(), |entry| entry.created_at),
                            last_used_at: None,
                            needs_login,
                            expires_at,
                            ciphertext: ciphertext.clone(),
                        },
                    )?;
                    trx.clear(
                        &crate::keys::Keys::new(&self.store.root)
                            .credential_entry_lease(scope, provider, label),
                    );
                    Ok(())
                }
            })
            .await
    }

    /// Read one labelled entry.
    /// # Errors
    /// Returns decryption, encoding, or database errors.
    pub async fn get_entry(
        &self,
        scope: CredentialScope,
        provider: &str,
        label: &str,
    ) -> Result<Option<CredentialRecord>> {
        let key = crate::keys::Keys::new(&self.store.root).credential_entry(scope, provider, label);
        let entry: Option<EntryValue> = self
            .store
            .transaction(|trx| {
                let key = &key;
                async move { read_entry(&trx, key).await }
            })
            .await?;
        entry
            .map(|entry| {
                decrypt(
                    &self.keyring,
                    scope,
                    &entry_identity(provider, label),
                    &entry.ciphertext,
                )
            })
            .transpose()
    }

    /// Record last use without changing the encrypted credential version.
    /// # Errors
    /// Returns missing credentials or database errors.
    pub async fn touch_entry(
        &self,
        scope: CredentialScope,
        provider: &str,
        label: &str,
    ) -> Result<()> {
        let key = crate::keys::Keys::new(&self.store.root).credential_entry(scope, provider, label);
        self.store
            .transaction(|trx| {
                let key = &key;
                async move {
                    let mut entry: EntryValue = read_entry(&trx, key)
                        .await?
                        .ok_or(StoreError::Domain(crate::DomainError::CredentialMissing))?;
                    entry.last_used_at = Some(self.store.now());
                    write(&trx, key, &entry)
                }
            })
            .await
    }

    /// # Errors
    /// Returns database errors.
    pub async fn delete_entry(
        &self,
        scope: CredentialScope,
        provider: &str,
        label: &str,
    ) -> Result<()> {
        self.store
            .transaction(|trx| async move {
                trx.clear(
                    &crate::keys::Keys::new(&self.store.root)
                        .credential_entry(scope, provider, label),
                );
                trx.clear(
                    &crate::keys::Keys::new(&self.store.root)
                        .credential_entry_lease(scope, provider, label),
                );
                Ok(())
            })
            .await?;
        if scope == CredentialScope::Cluster {
            self.store.clear_entry_quota(provider, label).await?;
        }
        Ok(())
    }

    /// One-shot boot migration of retired single-record credential rows.
    /// Reads `("credential", scope, provider)` rows in bounded batches,
    /// decrypts each with the cluster keyring, writes it as the provider's
    /// `default` entry when no such entry exists, and clears the legacy row
    /// in the same transaction. Rows that fail to decrypt are left in place
    /// so a later boot with the right keyring retries them; corrupt scope
    /// encodings are left as well. Returns the written and cleared counts for
    /// the startup log line. Boot never fails on this: database errors
    /// propagate for logging, but callers continue starting.
    /// # Errors
    /// Returns database, encoding, or encryption errors.
    pub async fn migrate_legacy_credentials(&self) -> Result<LegacyMigration> {
        // Bounded batches; `scan` rejects limits above `MAX_SCAN_LIMIT`.
        const BATCH: usize = crate::MAX_SCAN_LIMIT;
        let space = crate::keys::Keys::new(&self.store.root).credential_space();
        let (mut begin, end) = space.range();
        let mut outcome = LegacyMigration::default();
        loop {
            let rows = self
                .store
                .transaction(|trx| {
                    let range = (begin.clone(), end.clone());
                    async move { scan(&trx, range, BATCH).await }
                })
                .await?;
            if rows.is_empty() {
                break;
            }
            let complete = rows.len() < BATCH;
            for (key, bytes) in &rows {
                let Ok(parts) = space.unpack(key) else {
                    continue;
                };
                let (scope_text, provider): (String, String) = parts;
                let Some(scope) = parse_scope(&scope_text) else {
                    continue;
                };
                let Ok(record) = decrypt(&self.keyring, scope, provider.as_str(), bytes) else {
                    tracing::warn!(provider = %provider, "legacy credential row did not decrypt; leaving it for a later boot");
                    continue;
                };
                let (needs_login, expires_at) = entry_readiness(&record, self.store.now());
                let Ok(ciphertext) = encrypt(
                    &self.keyring,
                    scope,
                    &entry_identity(provider.as_str(), "default"),
                    &record,
                ) else {
                    continue;
                };
                let entry_key = crate::keys::Keys::new(&self.store.root)
                    .credential_entry(scope, &provider, "default");
                let legacy_key =
                    crate::keys::Keys::new(&self.store.root).credential(scope, &provider);
                let lease_key =
                    crate::keys::Keys::new(&self.store.root).credential_lease(scope, &provider);
                let created_at = record.updated_at;
                let wrote = self
                    .store
                    .transaction(|trx| {
                        let entry_key = &entry_key;
                        let legacy_key = &legacy_key;
                        let lease_key = &lease_key;
                        let ciphertext = &ciphertext;
                        async move {
                            let wrote = read_entry(&trx, entry_key).await?.is_none();
                            if wrote {
                                write(
                                    &trx,
                                    entry_key,
                                    &EntryValue {
                                        created_at,
                                        last_used_at: None,
                                        ciphertext: ciphertext.clone(),
                                        needs_login,
                                        expires_at,
                                    },
                                )?;
                            }
                            trx.clear(legacy_key);
                            trx.clear(lease_key);
                            Ok(wrote)
                        }
                    })
                    .await?;
                if wrote {
                    outcome.written += 1;
                } else {
                    outcome.cleared += 1;
                }
            }
            if complete {
                break;
            }
            if let Some((last, _)) = rows.last() {
                begin.clone_from(last);
                begin.push(0);
            } else {
                break;
            }
        }
        self.migrate_entry_bookkeeping().await?;
        Ok(outcome)
    }

    /// Rewrite legacy string flags in place without losing entries that cannot decrypt.
    async fn migrate_entry_bookkeeping(&self) -> Result<()> {
        let space = crate::keys::Keys::new(&self.store.root).credential_entry_space_root();
        let (mut begin, end) = space.range();
        loop {
            let rows = self
                .store
                .transaction(|trx| {
                    let range = (begin.clone(), end.clone());
                    async move { scan(&trx, range, crate::MAX_SCAN_LIMIT).await }
                })
                .await?;
            if rows.is_empty() {
                break;
            }
            let complete = rows.len() < crate::MAX_SCAN_LIMIT;
            for (key, bytes) in &rows {
                let Ok((scope_text, provider, label)): std::result::Result<
                    (String, String, String),
                    _,
                > = space.unpack(key) else {
                    continue;
                };
                let Some(scope) = parse_scope(&scope_text) else {
                    continue;
                };
                let Ok(mut entry) = decode_entry(bytes) else {
                    continue;
                };
                let identity = entry_identity(&provider, &label);
                let Ok(mut record) =
                    decrypt_raw(&self.keyring, scope, &identity, &entry.ciphertext)
                else {
                    continue;
                };
                if !record.migrate_bookkeeping() {
                    continue;
                }
                let old = entry.ciphertext.clone();
                entry.ciphertext = encrypt(&self.keyring, scope, &identity, &record)?;
                (entry.needs_login, entry.expires_at) = entry_readiness(&record, self.store.now());
                self.store
                    .transaction(|trx| {
                        let entry = &entry;
                        let old = &old;
                        async move {
                            if read_entry(&trx, key)
                                .await?
                                .is_some_and(|current| current.ciphertext == *old)
                            {
                                write(&trx, key, entry)?;
                            }
                            Ok(())
                        }
                    })
                    .await?;
            }
            if complete {
                break;
            }
            if let Some((last, _)) = rows.last() {
                begin.clone_from(last);
                begin.push(0);
            }
        }
        Ok(())
    }

    /// # Errors
    /// Returns decryption, encoding, or database errors.
    pub async fn list_entries(&self, scope: CredentialScope) -> Result<Vec<CredentialSummary>> {
        let space = crate::keys::Keys::new(&self.store.root).credential_entry_space(scope);
        let (mut begin, end) = space.range();
        let mut result = Vec::new();
        loop {
            let rows = self
                .store
                .transaction(|trx| {
                    let range = (begin.clone(), end.clone());
                    async move { scan(&trx, range, crate::MAX_SCAN_LIMIT).await }
                })
                .await?;
            if rows.is_empty() {
                break;
            }
            for (key, value) in rows {
                let (provider, label): (String, String) = space
                    .unpack(&key)
                    .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?;
                let entry: EntryValue = decode_entry(&value)?;
                let record = decrypt(
                    &self.keyring,
                    scope,
                    &entry_identity(&provider, &label),
                    &entry.ciphertext,
                )?;
                let mut summary = CredentialSummary::new(provider, &record, self.store.now());
                summary.label = label;
                summary.created_at = entry.created_at;
                summary.last_used_at = entry.last_used_at;
                result.push(summary);
                begin = key;
                begin.push(0);
            }
        }
        result.sort_by_key(|entry| {
            (
                entry.provider.clone(),
                entry.created_at,
                entry.label.clone(),
            )
        });
        Ok(result)
    }

    /// Read one provider's entries, oldest first. Only this provider's keys
    /// are scanned and decrypted, so per-job resolution stays one read.
    /// # Errors
    /// Returns decryption, encoding, or database errors.
    pub async fn provider_entries(
        &self,
        scope: CredentialScope,
        provider: &str,
    ) -> Result<Vec<(String, CredentialRecord)>> {
        let space = crate::keys::Keys::new(&self.store.root)
            .credential_entry_space_provider(scope, provider);
        let (begin, end) = space.range();
        let rows = self
            .store
            .transaction(|trx| {
                let range = (begin.clone(), end.clone());
                async move { scan(&trx, range, crate::MAX_SCAN_LIMIT).await }
            })
            .await?;
        let mut entries = Vec::new();
        for (key, value) in rows {
            let (label,): (String,) = space
                .unpack(&key)
                .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?;
            let entry: EntryValue = decode_entry(&value)?;
            let record = decrypt(
                &self.keyring,
                scope,
                &entry_identity(provider, &label),
                &entry.ciphertext,
            )?;
            entries.push((label, entry.created_at, record));
        }
        entries.sort_by(|left, right| left.1.cmp(&right.1).then(left.0.cmp(&right.0)));
        Ok(entries
            .into_iter()
            .map(|(label, _, record)| (label, record))
            .collect())
    }

    /// Serve the oldest ready entry so a rotated replacement takes over once
    /// it is usable; fall back to the oldest entry when none is ready.
    /// # Errors
    /// Returns decryption, encoding, or database errors.
    pub async fn first_entry(
        &self,
        scope: CredentialScope,
        provider: &str,
    ) -> Result<Option<(String, CredentialRecord)>> {
        let entries = self.provider_entries(scope, provider).await?;
        let now = self.store.now();
        if let Some((label, record)) = entries
            .iter()
            .find(|(_, record)| record.status(now) == CredentialStatus::Ready)
        {
            return Ok(Some((label.clone(), record.clone())));
        }
        Ok(entries.into_iter().next())
    }

    /// Fenced refresh of a single labelled entry. Other labels have independent leases.
    /// # Errors
    /// Returns lease loss, missing credentials, refresh, or storage errors.
    pub async fn refresh_entry_with_lease<F, Fut>(
        &self,
        scope: CredentialScope,
        provider: &str,
        label: &str,
        ttl: Duration,
        f: F,
    ) -> Result<CredentialRecord>
    where
        F: FnOnce(CredentialRecord) -> Fut,
        Fut: Future<Output = Result<CredentialRecord>>,
    {
        if ttl.is_zero() {
            return Err(StoreError::Domain(crate::DomainError::InvalidLeaseTtl));
        }
        let key = crate::keys::Keys::new(&self.store.root).credential_entry(scope, provider, label);
        let lease_key =
            crate::keys::Keys::new(&self.store.root).credential_entry_lease(scope, provider, label);
        let observed: EntryValue = self
            .store
            .transaction(|trx| {
                let key = &key;
                async move { read_entry(&trx, key).await }
            })
            .await?
            .ok_or(StoreError::Domain(crate::DomainError::CredentialMissing))?;
        let owner = LeaseOwnerId::from_ulid(ulid::Ulid::generate());
        let (lease, current) = loop {
            let result = self
                .claim_entry_refresh(
                    EntryClaim {
                        scope,
                        provider,
                        label,
                        key: &key,
                        lease_key: &lease_key,
                        observed: &observed,
                    },
                    ttl,
                    owner,
                )
                .await?;
            match result {
                Claim::Changed(bytes) => {
                    return decrypt(
                        &self.keyring,
                        scope,
                        &entry_identity(provider, label),
                        &bytes,
                    );
                }
                Claim::Acquired(lease, record) => break (lease, record),
                Claim::Busy => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        };
        let remaining = lease
            .expires_at
            .duration_since(self.store.now())
            .try_into()
            .unwrap_or(Duration::ZERO);
        let refreshed = tokio::time::timeout(remaining, f(current.clone())).await;
        let (mut replacement, failed) = match refreshed {
            Ok(Ok(record)) => (record, false),
            Ok(Err(_)) => {
                let mut record = current.clone();
                record.bookkeeping.needs_login = true;
                (record, true)
            }
            Err(_) => {
                return Err(StoreError::Fence(
                    crate::FenceError::CredentialRefreshMismatch,
                ));
            }
        };
        replacement.migrate_bookkeeping();
        let bytes = if replacement == current {
            observed.ciphertext.clone()
        } else {
            replacement.updated_at = self.store.now();
            encrypt(
                &self.keyring,
                scope,
                &entry_identity(provider, label),
                &replacement,
            )?
        };
        self.finish_entry_refresh(&key, &lease_key, &observed, &bytes, &replacement, &lease)
            .await?;
        if failed {
            Err(StoreError::Domain(crate::DomainError::CredentialRefresh))
        } else {
            Ok(replacement)
        }
    }

    async fn claim_entry_refresh(
        &self,
        claim: EntryClaim<'_>,
        ttl: Duration,
        owner: LeaseOwnerId,
    ) -> Result<Claim> {
        let EntryClaim {
            scope,
            provider,
            label,
            key,
            lease_key,
            observed,
        } = claim;
        self.store
            .transaction(|trx| async move {
                let entry: EntryValue = read_entry(&trx, key)
                    .await?
                    .ok_or(StoreError::Domain(crate::DomainError::CredentialMissing))?;
                if entry.ciphertext != observed.ciphertext {
                    return Ok(Claim::Changed(entry.ciphertext));
                }
                let now = self.store.now();
                if read::<Lease>(&trx, lease_key)
                    .await?
                    .is_some_and(|lease| lease.expires_at > now)
                {
                    return Ok(Claim::Busy);
                }
                let record = decrypt(
                    &self.keyring,
                    scope,
                    &entry_identity(provider, label),
                    &entry.ciphertext,
                )?;
                if record.status(now) == CredentialStatus::NeedsLogin {
                    return Err(StoreError::Domain(crate::DomainError::CredentialRefresh));
                }
                let lease = Lease {
                    owner,
                    seq: 0,
                    expires_at: now.checked_add(ttl).map_err(|_| {
                        StoreError::Fence(crate::FenceError::CredentialRefreshMismatch)
                    })?,
                };
                write(&trx, lease_key, &lease)?;
                Ok(Claim::Acquired(lease, record))
            })
            .await
    }

    async fn finish_entry_refresh(
        &self,
        key: &[u8],
        lease_key: &[u8],
        observed: &EntryValue,
        bytes: &[u8],
        replacement: &CredentialRecord,
        lease: &Lease,
    ) -> Result<()> {
        // A failed refresh marks the replacement as needing login; record its
        // hints alongside the new ciphertext so the scheduler pool stays exact.
        let (needs_login, expires_at) = entry_readiness(replacement, self.store.now());
        self.store
            .transaction(|trx| {
                let key = &key;
                let lease_key = &lease_key;
                let observed = &observed;
                async move {
                    let held: Lease = read(&trx, lease_key).await?.ok_or(StoreError::Fence(
                        crate::FenceError::CredentialRefreshMismatch,
                    ))?;
                    if held != *lease || held.expires_at <= self.store.now() {
                        return Err(StoreError::Fence(
                            crate::FenceError::CredentialRefreshMismatch,
                        ));
                    }
                    let entry: EntryValue = read_entry(&trx, key)
                        .await?
                        .ok_or(StoreError::Domain(crate::DomainError::CredentialMissing))?;
                    if entry.ciphertext != observed.ciphertext {
                        return Err(StoreError::Fence(
                            crate::FenceError::CredentialRefreshMismatch,
                        ));
                    }
                    if bytes != observed.ciphertext {
                        write(
                            &trx,
                            key,
                            &EntryValue {
                                ciphertext: bytes.to_vec(),
                                needs_login,
                                expires_at,
                                ..entry
                            },
                        )?;
                    }
                    trx.clear(lease_key);
                    Ok(())
                }
            })
            .await?;
        Ok(())
    }
}

struct EntryClaim<'a> {
    scope: CredentialScope,
    provider: &'a str,
    label: &'a str,
    key: &'a [u8],
    lease_key: &'a [u8],
    observed: &'a EntryValue,
}

enum Claim {
    Busy,
    Changed(Vec<u8>),
    Acquired(Lease, CredentialRecord),
}

fn associated_data(scope: CredentialScope, provider: &str) -> Result<Vec<u8>> {
    Ok(encode(&(scope, provider))?)
}

fn encrypt(
    key: &Keyring,
    scope: CredentialScope,
    provider: &str,
    record: &CredentialRecord,
) -> Result<Vec<u8>> {
    let plaintext = encode(record)?;
    if plaintext.len() > crate::INLINE_LIMIT {
        return Err(StoreError::Storage(crate::StorageError::TooLarge));
    }
    let mut nonce = [0; 24];
    rand::rngs::OsRng
        .try_fill_bytes(&mut nonce)
        .map_err(|_| StoreError::Storage(crate::StorageError::Keyring))?;
    let ciphertext = XChaCha20Poly1305::new(key.key().into())
        .encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: &plaintext,
                aad: &associated_data(scope, provider)?,
            },
        )
        .map_err(|_| StoreError::Storage(crate::StorageError::Keyring))?;
    let mut bytes = nonce.to_vec();
    bytes.extend(ciphertext);
    Ok(bytes)
}

fn decrypt_raw(
    key: &Keyring,
    scope: CredentialScope,
    provider: &str,
    bytes: &[u8],
) -> Result<CredentialRecord> {
    if bytes.len() < 24 {
        return Err(StoreError::Storage(crate::StorageError::Keyring));
    }
    let (nonce, ciphertext) = bytes.split_at(24);
    let plaintext = XChaCha20Poly1305::new(key.key().into())
        .decrypt(
            XNonce::from_slice(nonce),
            Payload {
                msg: ciphertext,
                aad: &associated_data(scope, provider)?,
            },
        )
        .map_err(|_| StoreError::Storage(crate::StorageError::Keyring))?;
    Ok(decode(&plaintext)?)
}

fn decrypt(
    key: &Keyring,
    scope: CredentialScope,
    provider: &str,
    bytes: &[u8],
) -> Result<CredentialRecord> {
    let mut record = decrypt_raw(key, scope, provider, bytes)?;
    record.migrate_bookkeeping();
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn authenticated_round_trip() {
        let key = Keyring::from_bytes([1; 32]);
        let record = CredentialRecord {
            bookkeeping: swarmy_core::CredentialBookkeeping::default(),
            kind: CredentialKind::ApiKey {
                key: "secret".into(),
                extra: std::collections::BTreeMap::new(),
            },
            updated_at: Timestamp::now(),
        };
        let bytes = encrypt(&key, CredentialScope::Cluster, "openai", &record).unwrap();
        assert!(decrypt(&key, CredentialScope::Cluster, "openai", &bytes).unwrap() == record);
        assert!(matches!(
            decrypt(&key, CredentialScope::Cluster, "anthropic", &bytes),
            Err(StoreError::Storage(crate::StorageError::Keyring))
        ));
        assert!(matches!(
            decrypt(
                &Keyring::from_bytes([2; 32]),
                CredentialScope::Cluster,
                "openai",
                &bytes
            ),
            Err(StoreError::Storage(crate::StorageError::Keyring))
        ));
        let agent = CredentialScope::Agent(swarmy_core::AgentId::from_ulid(ulid::Ulid::generate()));
        assert!(matches!(
            decrypt(&key, agent, "openai", &bytes),
            Err(StoreError::Storage(crate::StorageError::Keyring))
        ));
        for short in [vec![], vec![0; 23], vec![0; 24]] {
            assert!(matches!(
                decrypt(&key, CredentialScope::Cluster, "openai", &short),
                Err(StoreError::Storage(crate::StorageError::Keyring))
            ));
        }
        assert_ne!(
            bytes,
            encrypt(&key, CredentialScope::Cluster, "openai", &record).unwrap()
        );
    }
}
