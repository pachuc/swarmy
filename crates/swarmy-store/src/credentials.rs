//! Encrypted credentials and fenced refresh transactions.
use std::{future::Future, time::Duration};

use chacha20poly1305::{
    XChaCha20Poly1305, XNonce,
    aead::{Aead, KeyInit, Payload},
};
use jiff::Timestamp;
use rand::TryRngCore;
use serde::Serialize;
use swarmy_config::Keyring;
use swarmy_core::{
    CredentialKind, CredentialRecord, CredentialScope, CredentialStatus, Lease, LeaseOwnerId,
    decode, encode,
};

use crate::{Result, Store, StoreError, read, scan, write};

/// No secrets are returned by list operations. Each encrypted value is read once.
#[derive(Debug, Serialize)]
pub struct CredentialSummary {
    pub provider: String,
    pub kind: String,
    pub label: String,
    pub status: CredentialStatus,
    pub updated_at: Timestamp,
    pub expires_at: Option<Timestamp>,
}

impl CredentialSummary {
    #[must_use]
    pub fn new(provider: String, record: &CredentialRecord, now: Timestamp) -> Self {
        Self {
            provider,
            kind: record.kind_name().into(),
            label: match &record.kind {
                CredentialKind::ApiKey { extra, .. } | CredentialKind::OAuth { extra, .. } => {
                    extra.get("label").cloned().unwrap_or_default()
                }
            },
            status: record.status(now),
            updated_at: record.updated_at,
            expires_at: match record.kind {
                CredentialKind::OAuth { expires_at, .. } => Some(expires_at),
                CredentialKind::ApiKey { .. } => None,
            },
        }
    }
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
        self.transaction(|trx| async move {
            Ok(trx
                .get(
                    &self.root.pack(&("credential", scope.to_string(), provider)),
                    false,
                )
                .await?
                .is_some())
        })
        .await
    }

    /// Fingerprint the encrypted record without exposing or decrypting its contents.
    /// # Errors
    /// Returns database errors.
    pub async fn credential_fingerprint(
        &self,
        scope: CredentialScope,
        provider: &str,
    ) -> Result<Option<[u8; 32]>> {
        self.transaction(|trx| async move {
            Ok(trx
                .get(
                    &self.root.pack(&("credential", scope.to_string(), provider)),
                    false,
                )
                .await?
                .map(|bytes| *blake3::hash(&bytes).as_bytes()))
        })
        .await
    }
}

impl CredentialStore {
    fn key(&self, table: &str, scope: CredentialScope, provider: &str) -> Vec<u8> {
        self.store.root.pack(&(table, scope.to_string(), provider))
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

    /// # Errors
    /// Returns `Keyring` for failed authentication, or storage/encoding errors.
    pub async fn get_credential(
        &self,
        scope: CredentialScope,
        provider: &str,
    ) -> Result<Option<CredentialRecord>> {
        self.raw(scope, provider)
            .await?
            .map(|v| decrypt(&self.keyring, scope, provider, &v))
            .transpose()
    }

    /// # Errors
    /// Returns database errors. Deletion also fences in-flight refreshes.
    pub async fn delete_credential(&self, scope: CredentialScope, provider: &str) -> Result<()> {
        self.store
            .transaction(|trx| async move {
                trx.clear(&self.key("credential", scope, provider));
                trx.clear(&self.key("credential_lease", scope, provider));
                Ok(())
            })
            .await
    }

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
