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
    CredentialKind, CredentialRecord, CredentialScope, CredentialStatus, Lease, LeaseOwnerId,
    decode, encode,
};

use crate::{
    BreakerCandidate, CredentialKey, Result, Store, StoreError, inference_wait::Breaker, read,
    scan, write,
};
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

fn entry_kind(record: &CredentialRecord) -> String {
    match &record.kind {
        CredentialKind::OAuth { .. } => "subscription",
        CredentialKind::ApiKey { extra, .. }
            if extra.get("auth_kind").is_some_and(|s| s == "cloud") =>
        {
            "cloud"
        }
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

/// Rows written before the readiness hints existed carry no plaintext
/// status; they read as ready, which matches the old listing that treated
/// every stored entry as a candidate. Rewriting the entry (login, refresh,
/// replacement) records its hints.
#[derive(Deserialize)]
struct LegacyEntryValue {
    created_at: Timestamp,
    last_used_at: Option<Timestamp>,
    ciphertext: Vec<u8>,
}

pub(crate) fn decode_entry(bytes: &[u8]) -> Result<EntryValue> {
    if let Ok(entry) = decode::<EntryValue>(bytes) {
        return Ok(entry);
    }
    let legacy = decode::<LegacyEntryValue>(bytes)?;
    Ok(EntryValue {
        created_at: legacy.created_at,
        last_used_at: legacy.last_used_at,
        ciphertext: legacy.ciphertext,
        needs_login: false,
        expires_at: None,
    })
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
fn entry_readiness(record: &CredentialRecord) -> (bool, Option<Timestamp>) {
    let needs_login = record.status(Timestamp::now()) == CredentialStatus::NeedsLogin;
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

    /// Entry labels for breaker checks, without decrypting. A legacy single
    /// record that has not migrated yet counts as the `default` entry, placed
    /// first because the migration keeps its original creation time.
    /// # Errors
    /// Returns database or decoding errors.
    pub async fn credential_entry_labels(
        &self,
        scope: CredentialScope,
        provider: &str,
    ) -> Result<Vec<String>> {
        let space = self
            .root
            .subspace(&("credential_entry", scope.to_string(), provider));
        let (begin, end) = space.range();
        let rows = self
            .transaction(|trx| {
                let range = (begin.clone(), end.clone());
                async move { scan(&trx, range, crate::MAX_SCAN_LIMIT).await }
            })
            .await?;
        let mut labels = Vec::new();
        for (key, _) in rows {
            let (label,): (String,) = space.unpack(&key).map_err(|_| StoreError::Corrupt)?;
            labels.push(label);
        }
        let legacy = self
            .transaction(|trx| async move {
                Ok(trx
                    .get(
                        &self.root.pack(&("credential", scope.to_string(), provider)),
                        false,
                    )
                    .await?
                    .is_some())
            })
            .await?;
        if legacy && !labels.iter().any(|label| label == "default") {
            labels.insert(0, "default".into());
        }
        labels.sort();
        Ok(labels)
    }

    /// One-transaction snapshot of a provider's breaker pool for a scheduler
    /// tick: the ready entries (or every entry when none is ready, matching
    /// the gateway pool), each with its live breaker record. Without stored
    /// entries the provider shares one unlabeled record, and an unmigrated
    /// legacy record counts as the `default` entry with unknown status. The
    /// entry listing pages past `MAX_SCAN_LIMIT` inside the same transaction
    /// instead of truncating at 64 entries.
    /// # Errors
    /// Returns database or decoding errors.
    pub async fn breaker_snapshot(
        &self,
        scope: CredentialScope,
        provider: &str,
        now: Timestamp,
    ) -> Result<Vec<BreakerCandidate>> {
        self.transaction(|trx| async move {
            let space = self
                .root
                .subspace(&("credential_entry", scope.to_string(), provider));
            let (mut begin, end) = space.range();
            let mut entries: Vec<(String, bool)> = Vec::new();
            loop {
                let rows = scan(&trx, (begin.clone(), end.clone()), crate::MAX_SCAN_LIMIT).await?;
                let complete = rows.len() < crate::MAX_SCAN_LIMIT;
                for (key, value) in rows {
                    let (label,): (String,) =
                        space.unpack(&key).map_err(|_| StoreError::Corrupt)?;
                    let entry = decode_entry(&value)?;
                    entries.push((label, entry_ready(entry.needs_login, entry.expires_at, now)));
                    begin = key;
                    begin.push(0);
                }
                if complete {
                    break;
                }
            }
            let legacy = trx
                .get(
                    &self.root.pack(&("credential", scope.to_string(), provider)),
                    false,
                )
                .await?
                .is_some();
            if legacy && !entries.iter().any(|(label, _)| label == "default") {
                // The legacy record is encrypted, so its status is unknown
                // without the keyring; keep it a candidate. The gateway
                // migrates it to a hinted entry on first resolution.
                entries.push(("default".into(), true));
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
        let space = self
            .root
            .subspace(&("credential_entry", scope.to_string(), provider));
        let (mut begin, end) = space.range();
        let legacy = self
            .transaction(|trx| async move {
                Ok(trx
                    .get(
                        &self.root.pack(&("credential", scope.to_string(), provider)),
                        false,
                    )
                    .await?
                    .map(|value| value.to_vec()))
            })
            .await?;
        let mut hash = blake3::Hasher::new();
        let mut found = false;
        if let Some(bytes) = legacy {
            hash.update(&bytes);
            found = true;
        }
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
}

impl CredentialStore {
    fn key(&self, table: &str, scope: CredentialScope, provider: &str) -> Vec<u8> {
        self.store.root.pack(&(table, scope.to_string(), provider))
    }

    fn entry_key(
        &self,
        table: &str,
        scope: CredentialScope,
        provider: &str,
        label: &str,
    ) -> Vec<u8> {
        self.store
            .root
            .pack(&(table, scope.to_string(), provider, label))
    }

    // Migration is committed in one transaction; competing readers cannot create two entries.
    // The legacy read and the entry write share the caller's transaction so
    // resolution stays one read.
    async fn migrate_legacy(
        &self,
        trx: &RetryableTransaction,
        scope: CredentialScope,
        provider: &str,
    ) -> Result<()> {
        let old = self.key("credential", scope, provider);
        if let Some(bytes) = trx.get(&old, false).await?.map(|value| value.to_vec()) {
            let record = decrypt(&self.keyring, scope, provider, &bytes)?;
            let entry = self.entry_key("credential_entry", scope, provider, "default");
            let previous: Option<EntryValue> = read_entry(trx, &entry).await?;
            let (needs_login, expires_at) = entry_readiness(&record);
            write(
                trx,
                &entry,
                &EntryValue {
                    created_at: previous.map_or(record.updated_at, |entry| entry.created_at),
                    last_used_at: None,
                    needs_login,
                    expires_at,
                    ciphertext: encrypt(
                        &self.keyring,
                        scope,
                        &entry_identity(provider, "default"),
                        &record,
                    )?,
                },
            )?;
            trx.clear(&old);
            trx.clear(&self.key("credential_lease", scope, provider));
        }
        Ok(())
    }

    async fn migrate(&self, scope: CredentialScope, provider: &str) -> Result<()> {
        self.store
            .transaction(|trx| async move { self.migrate_legacy(&trx, scope, provider).await })
            .await
    }

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
        self.migrate(scope, provider).await?;
        let ciphertext = encrypt(
            &self.keyring,
            scope,
            &entry_identity(provider, label),
            record,
        )?;
        let key = self.entry_key("credential_entry", scope, provider, label);
        let (needs_login, expires_at) = entry_readiness(record);
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
                                .map_or_else(Timestamp::now, |entry| entry.created_at),
                            last_used_at: None,
                            needs_login,
                            expires_at,
                            ciphertext: ciphertext.clone(),
                        },
                    )?;
                    trx.clear(&self.entry_key("credential_entry_lease", scope, provider, label));
                    Ok(())
                }
            })
            .await
    }

    /// Read one labelled entry, migrating the provider's legacy record in the
    /// same transaction so a resolution is one read.
    /// # Errors
    /// Returns decryption, encoding, or database errors.
    pub async fn get_entry(
        &self,
        scope: CredentialScope,
        provider: &str,
        label: &str,
    ) -> Result<Option<CredentialRecord>> {
        let key = self.entry_key("credential_entry", scope, provider, label);
        let entry: Option<EntryValue> = self
            .store
            .transaction(|trx| {
                let key = &key;
                async move {
                    self.migrate_legacy(&trx, scope, provider).await?;
                    read_entry(&trx, key).await
                }
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
        let key = self.entry_key("credential_entry", scope, provider, label);
        self.store
            .transaction(|trx| {
                let key = &key;
                async move {
                    let mut entry: EntryValue = read_entry(&trx, key)
                        .await?
                        .ok_or(StoreError::CredentialMissing)?;
                    entry.last_used_at = Some(Timestamp::now());
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
        self.migrate(scope, provider).await?;
        self.store
            .transaction(|trx| async move {
                trx.clear(&self.entry_key("credential_entry", scope, provider, label));
                trx.clear(&self.entry_key("credential_entry_lease", scope, provider, label));
                Ok(())
            })
            .await
    }

    /// # Errors
    /// Returns decryption, encoding, or database errors.
    pub async fn list_entries(&self, scope: CredentialScope) -> Result<Vec<CredentialSummary>> {
        // Legacy keys share a prefix; migrate them before scanning entries.
        let legacy = self.store.root.subspace(&("credential", scope.to_string()));
        let (mut begin, end) = legacy.range();
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
            for (key, _) in rows {
                let (provider,): (String,) =
                    legacy.unpack(&key).map_err(|_| StoreError::Corrupt)?;
                self.migrate(scope, &provider).await?;
                begin = key;
                begin.push(0);
            }
        }
        let space = self
            .store
            .root
            .subspace(&("credential_entry", scope.to_string()));
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
                let (provider, label): (String, String) =
                    space.unpack(&key).map_err(|_| StoreError::Corrupt)?;
                let entry: EntryValue = decode_entry(&value)?;
                let record = decrypt(
                    &self.keyring,
                    scope,
                    &entry_identity(&provider, &label),
                    &entry.ciphertext,
                )?;
                let mut summary = CredentialSummary::new(provider, &record, Timestamp::now());
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

    /// Read one provider's entries, oldest first, migrating its legacy record
    /// in the same transaction. Only this provider's keys are scanned and
    /// decrypted, so per-job resolution stays one read.
    /// # Errors
    /// Returns decryption, encoding, or database errors.
    pub async fn provider_entries(
        &self,
        scope: CredentialScope,
        provider: &str,
    ) -> Result<Vec<(String, CredentialRecord)>> {
        let space = self
            .store
            .root
            .subspace(&("credential_entry", scope.to_string(), provider));
        let (begin, end) = space.range();
        let rows = self
            .store
            .transaction(|trx| {
                let range = (begin.clone(), end.clone());
                async move {
                    self.migrate_legacy(&trx, scope, provider).await?;
                    scan(&trx, range, crate::MAX_SCAN_LIMIT).await
                }
            })
            .await?;
        let mut entries = Vec::new();
        for (key, value) in rows {
            let (label,): (String,) = space.unpack(&key).map_err(|_| StoreError::Corrupt)?;
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
        let now = Timestamp::now();
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
            return Err(StoreError::LeaseMismatch);
        }
        self.migrate(scope, provider).await?;
        let key = self.entry_key("credential_entry", scope, provider, label);
        let lease_key = self.entry_key("credential_entry_lease", scope, provider, label);
        let observed: EntryValue = self
            .store
            .transaction(|trx| {
                let key = &key;
                async move { read_entry(&trx, key).await }
            })
            .await?
            .ok_or(StoreError::CredentialMissing)?;
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
            .duration_since(Timestamp::now())
            .try_into()
            .unwrap_or(Duration::ZERO);
        let refreshed = tokio::time::timeout(remaining, f(current.clone())).await;
        let (mut replacement, failed) = match refreshed {
            Ok(Ok(record)) => (record, false),
            Ok(Err(_)) => {
                let mut record = current.clone();
                let extra = match &mut record.kind {
                    CredentialKind::ApiKey { extra, .. } | CredentialKind::OAuth { extra, .. } => {
                        extra
                    }
                };
                extra.insert("needs_login".into(), "true".into());
                (record, true)
            }
            Err(_) => return Err(StoreError::LeaseMismatch),
        };
        let bytes = if replacement == current {
            observed.ciphertext.clone()
        } else {
            replacement.updated_at = Timestamp::now();
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
            Err(StoreError::CredentialRefresh)
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
                    .ok_or(StoreError::CredentialMissing)?;
                if entry.ciphertext != observed.ciphertext {
                    return Ok(Claim::Changed(entry.ciphertext));
                }
                let now = Timestamp::now();
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
                    return Err(StoreError::CredentialRefresh);
                }
                let lease = Lease {
                    owner,
                    seq: 0,
                    expires_at: now
                        .checked_add(ttl)
                        .map_err(|_| StoreError::LeaseMismatch)?,
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
        let (needs_login, expires_at) = entry_readiness(replacement);
        self.store
            .transaction(|trx| {
                let key = &key;
                let lease_key = &lease_key;
                let observed = &observed;
                async move {
                    let held: Lease = read(&trx, lease_key)
                        .await?
                        .ok_or(StoreError::LeaseMismatch)?;
                    if held != *lease || held.expires_at <= Timestamp::now() {
                        return Err(StoreError::LeaseMismatch);
                    }
                    let entry: EntryValue = read_entry(&trx, key)
                        .await?
                        .ok_or(StoreError::CredentialMissing)?;
                    if entry.ciphertext != observed.ciphertext {
                        return Err(StoreError::LeaseMismatch);
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

    /// Explicit replacement also fences any in-flight refresh.
    /// # Errors
    /// Returns encryption, size, and database errors.
    pub async fn put_credential(
        &self,
        scope: CredentialScope,
        provider: &str,
        record: &CredentialRecord,
    ) -> Result<()> {
        let value = encrypt(&self.keyring, scope, provider, record)?;
        self.store
            .transaction(|trx| {
                let value = &value;
                async move {
                    trx.set(&self.key("credential", scope, provider), value);
                    trx.clear(&self.key("credential_lease", scope, provider));
                    Ok(())
                }
            })
            .await
    }

    async fn raw(&self, scope: CredentialScope, provider: &str) -> Result<Option<Vec<u8>>> {
        self.store
            .transaction(|trx| async move {
                Ok(trx
                    .get(&self.key("credential", scope, provider), false)
                    .await?
                    .map(|v| v.to_vec()))
            })
            .await
    }

    /// Legacy single-record read used by migration tests; entry reads migrate.
    /// # Errors
    /// Returns `Keyring` for failed authentication, or storage/encoding errors.
    pub async fn get_credential(
        &self,
        scope: CredentialScope,
        provider: &str,
    ) -> Result<Option<CredentialRecord>> {
        self.raw(scope, provider)
            .await?
            .map(|bytes| decrypt(&self.keyring, scope, provider, &bytes))
            .transpose()
    }

    /// # Errors
    /// Returns database errors. Deletion also fences in-flight refreshes.
    pub async fn delete_credential(&self, scope: CredentialScope, provider: &str) -> Result<()> {
        self.store
            .transaction(|trx| async move {
                trx.clear(&self.key("credential", scope, provider));
                trx.clear(&self.key("credential_lease", scope, provider));
                trx.clear(&self.entry_key("credential_entry", scope, provider, "default"));
                trx.clear(&self.entry_key("credential_entry_lease", scope, provider, "default"));
                Ok(())
            })
            .await
    }

    /// Legacy single-scope listing; entry reads migrate instead.
    /// Page through encrypted records, decrypting each once to derive status.
    /// # Errors
    /// Returns keyring, encoding, or database errors.
    pub async fn list_credentials(&self, scope: CredentialScope) -> Result<Vec<CredentialSummary>> {
        let space = self.store.root.subspace(&("credential", scope.to_string()));
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
                let (provider,): (String,) = space.unpack(&key).map_err(|_| StoreError::Corrupt)?;
                let record = decrypt(&self.keyring, scope, &provider, &value)?;
                result.push(CredentialSummary::new(provider, &record, Timestamp::now()));
                begin = key;
                begin.push(0);
            }
        }
        Ok(result)
    }

    /// Serialize refresh outside retryable transactions. Waiters use the winner's
    /// record; an expired owner cannot publish after another owner or explicit set.
    /// Failed refreshes retain tokens and persist `needs_login` under the same fence.
    /// # Errors
    /// Returns missing credentials, refresh failure, lease loss, or storage errors.
    pub async fn refresh_with_lease<F, Fut>(
        &self,
        scope: CredentialScope,
        provider: &str,
        ttl: Duration,
        f: F,
    ) -> Result<CredentialRecord>
    where
        F: FnOnce(CredentialRecord) -> Fut,
        Fut: Future<Output = Result<CredentialRecord>>,
    {
        if ttl.is_zero() {
            return Err(StoreError::LeaseMismatch);
        }
        let observed = self
            .raw(scope, provider)
            .await?
            .ok_or(StoreError::CredentialMissing)?;
        let owner = LeaseOwnerId::from_ulid(ulid::Ulid::generate());
        let (lease, current) = loop {
            let claim = self
                .claim_refresh(scope, provider, ttl, owner, &observed)
                .await?;
            match claim {
                Claim::Changed(bytes) => return decrypt(&self.keyring, scope, provider, &bytes),
                Claim::Acquired(lease, record) => break (lease, record),
                Claim::Busy => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        };
        // Cancel slow refreshes before the lease can be reused. The commit still
        // checks expiry because transaction retries can outlast this timeout.
        let remaining = lease
            .expires_at
            .duration_since(Timestamp::now())
            .try_into()
            .unwrap_or(Duration::ZERO);
        let replacement = tokio::time::timeout(remaining, f(current.clone())).await;
        let (mut replacement, failed) = match replacement {
            Ok(Ok(record)) => (record, false),
            Ok(Err(_)) => {
                let mut record = current.clone();
                let extra = match &mut record.kind {
                    CredentialKind::ApiKey { extra, .. } | CredentialKind::OAuth { extra, .. } => {
                        extra
                    }
                };
                extra.insert("needs_login".into(), "true".into());
                (record, true)
            }
            Err(_) => return Err(StoreError::LeaseMismatch),
        };
        let bytes = if replacement == current {
            observed.clone()
        } else {
            replacement.updated_at = Timestamp::now();
            encrypt(&self.keyring, scope, provider, &replacement)?
        };
        self.finish_refresh(scope, provider, &lease, &observed, &bytes)
            .await?;
        if failed {
            Err(StoreError::CredentialRefresh)
        } else {
            Ok(replacement)
        }
    }

    async fn claim_refresh(
        &self,
        scope: CredentialScope,
        provider: &str,
        ttl: Duration,
        owner: LeaseOwnerId,
        observed: &[u8],
    ) -> Result<Claim> {
        let lease_key = self.key("credential_lease", scope, provider);
        let record_key = self.key("credential", scope, provider);
        self.store
            .transaction(|trx| {
                let lease_key = &lease_key;
                let record_key = &record_key;
                async move {
                    let bytes = trx
                        .get(record_key, false)
                        .await?
                        .ok_or(StoreError::CredentialMissing)?;
                    if bytes.as_ref() != observed {
                        return Ok(Claim::Changed(bytes.to_vec()));
                    }
                    let now = Timestamp::now();
                    if read::<Lease>(&trx, lease_key)
                        .await?
                        .is_some_and(|lease| lease.expires_at > now)
                    {
                        return Ok(Claim::Busy);
                    }
                    let record = decrypt(&self.keyring, scope, provider, &bytes)?;
                    if record.status(now) == CredentialStatus::NeedsLogin {
                        return Err(StoreError::CredentialRefresh);
                    }
                    let lease = Lease {
                        owner,
                        seq: 0,
                        expires_at: now
                            .checked_add(ttl)
                            .map_err(|_| StoreError::LeaseMismatch)?,
                    };
                    write(&trx, lease_key, &lease)?;
                    Ok(Claim::Acquired(lease, record))
                }
            })
            .await
    }

    async fn finish_refresh(
        &self,
        scope: CredentialScope,
        provider: &str,
        lease: &Lease,
        observed: &[u8],
        bytes: &[u8],
    ) -> Result<()> {
        let lease_key = self.key("credential_lease", scope, provider);
        let record_key = self.key("credential", scope, provider);
        self.store
            .transaction(|trx| {
                let lease_key = &lease_key;
                let record_key = &record_key;
                async move {
                    let held: Lease = read(&trx, lease_key)
                        .await?
                        .ok_or(StoreError::LeaseMismatch)?;
                    if held != *lease || held.expires_at <= Timestamp::now() {
                        return Err(StoreError::LeaseMismatch);
                    }
                    let current = trx
                        .get(record_key, false)
                        .await?
                        .ok_or(StoreError::CredentialMissing)?;
                    if current.as_ref() != observed {
                        return Err(StoreError::LeaseMismatch);
                    }
                    if bytes != observed {
                        trx.set(record_key, bytes);
                    }
                    trx.clear(lease_key);
                    Ok(())
                }
            })
            .await
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
        return Err(StoreError::TooLarge);
    }
    let mut nonce = [0; 24];
    rand::rngs::OsRng
        .try_fill_bytes(&mut nonce)
        .map_err(|_| StoreError::Keyring)?;
    let ciphertext = XChaCha20Poly1305::new(key.key().into())
        .encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: &plaintext,
                aad: &associated_data(scope, provider)?,
            },
        )
        .map_err(|_| StoreError::Keyring)?;
    let mut bytes = nonce.to_vec();
    bytes.extend(ciphertext);
    Ok(bytes)
}

fn decrypt(
    key: &Keyring,
    scope: CredentialScope,
    provider: &str,
    bytes: &[u8],
) -> Result<CredentialRecord> {
    if bytes.len() < 24 {
        return Err(StoreError::Keyring);
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
        .map_err(|_| StoreError::Keyring)?;
    Ok(decode(&plaintext)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn authenticated_round_trip() {
        let key = Keyring::from_bytes([1; 32]);
        let record = CredentialRecord {
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
            Err(StoreError::Keyring)
        ));
        assert!(matches!(
            decrypt(
                &Keyring::from_bytes([2; 32]),
                CredentialScope::Cluster,
                "openai",
                &bytes
            ),
            Err(StoreError::Keyring)
        ));
        let agent = CredentialScope::Agent(swarmy_core::AgentId::from_ulid(ulid::Ulid::generate()));
        assert!(matches!(
            decrypt(&key, agent, "openai", &bytes),
            Err(StoreError::Keyring)
        ));
        for short in [vec![], vec![0; 23], vec![0; 24]] {
            assert!(matches!(
                decrypt(&key, CredentialScope::Cluster, "openai", &short),
                Err(StoreError::Keyring)
            ));
        }
        assert_ne!(
            bytes,
            encrypt(&key, CredentialScope::Cluster, "openai", &record).unwrap()
        );
    }
}
