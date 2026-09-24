//! Bounded replay cache for HTTP mutations, separate from inference request ids.
use crate::{Result, Store, StoreError, read, write};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ApiReplay {
    pub result: String,
    pub expires_at: Timestamp,
}

impl Store {
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
