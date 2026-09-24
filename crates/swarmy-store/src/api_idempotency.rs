//! Bounded replay cache for HTTP mutations, separate from inference request ids.
use crate::{Result, Store, StoreError, read, write};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use swarmy_core::SessionId;
use ulid::Ulid;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ApiReplay {
    pub result: String,
    pub expires_at: Timestamp,
}

impl Store {
    /// Reserve a real, time-ordered session id before creating its computer.
    /// Retrying after an unknown create outcome reuses the same id on any API host.
    /// # Errors
    /// Returns database or encoding failures.
    pub async fn api_session_id(&self, key: &str) -> Result<SessionId> {
        let fresh = SessionId::from_ulid(Ulid::generate());
        self.transaction(|trx| async move {
            let storage_key = self.root.pack(&("api_session_id", key));
            if let Some(id) = read::<SessionId>(&trx, &storage_key).await? {
                Ok(id)
            } else {
                write(&trx, &storage_key, &fresh)?;
                Ok(fresh)
            }
        })
        .await
    }

    /// Read a non-expired API response for a scoped idempotency key.
    /// # Errors
    /// Returns database and decoding errors.
    pub async fn api_replay(&self, key: &str) -> Result<Option<serde_json::Value>> {
        let record = self
            .transaction(|trx| async move {
                let key = self.root.pack(&("api_idempotency", key));
                read::<ApiReplay>(&trx, &key).await
            })
            .await?;
        record
            .filter(|entry| entry.expires_at > Timestamp::now())
            .map(|entry| serde_json::from_str(&entry.result).map_err(|_| StoreError::Corrupt))
            .transpose()
    }

    /// Save a response for retry after an unknown HTTP outcome.
    /// # Errors
    /// Returns database and encoding errors.
    pub async fn put_api_replay(&self, key: &str, result: serde_json::Value) -> Result<()> {
        let entry = ApiReplay {
            result: serde_json::to_string(&result).map_err(|_| StoreError::Corrupt)?,
            expires_at: Timestamp::now()
                .checked_add(jiff::Span::new().hours(1))
                .unwrap_or(Timestamp::MAX),
        };
        self.transaction(|trx| {
            let entry = &entry;
            async move { write(&trx, &self.root.pack(&("api_idempotency", key)), entry) }
        })
        .await
    }
}
