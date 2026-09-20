//! Inference metadata lives beside session headers to preserve their binary layout.
use crate::{Result, Store, read, write};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use swarmy_core::SessionId;

/// Gateway tasks refresh this record every 30 seconds with a future expiry.
/// A skipped provider is recorded with an already expired advertisement so the
/// reason stays visible without granting routing authority.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GatewayProvider {
    pub expires_at: Timestamp,
    /// Discovery outcome of the last writing gateway; empty for legacy records.
    #[serde(default, with = "swarmy_core::trailing")]
    pub reason: String,
}

impl Store {
    pub(crate) fn session_inference_key(&self, id: SessionId) -> Vec<u8> {
        self.root
            .pack(&("session_inference", id.as_ulid().to_bytes().as_slice()))
    }

    /// Advertise provider availability; expired advertisements are ignored.
    /// # Errors
    /// Returns database or encoding errors.
    pub async fn put_gateway_provider(
        &self,
        provider: &str,
        record: &GatewayProvider,
    ) -> Result<()> {
        self.transaction(|trx| async move {
            write(
                &trx,
                &self.root.pack(&("gateway_provider", provider)),
                record,
            )
        })
        .await
    }

    /// Read the last advertisement for a provider, expired or not.
    /// # Errors
    /// Returns database or decoding errors.
    pub async fn gateway_provider(&self, provider: &str) -> Result<Option<GatewayProvider>> {
        self.transaction(|trx| async move {
            read(&trx, &self.root.pack(&("gateway_provider", provider))).await
        })
        .await
    }

    /// Whether any gateway has an unexpired advertisement for this provider.
    /// # Errors
    /// Returns database or decoding errors.
    pub async fn gateway_serves(&self, provider: &str) -> Result<bool> {
        self.transaction(|trx| async move {
            Ok(
                read::<GatewayProvider>(&trx, &self.root.pack(&("gateway_provider", provider)))
                    .await?
                    .is_some_and(|record| record.expires_at > Timestamp::now()),
            )
        })
        .await
    }
}
