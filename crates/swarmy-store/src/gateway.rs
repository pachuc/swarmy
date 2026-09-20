use crate::{Result, Store, read, write};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

/// Last startup observation. This record does not grant routing authority or liveness.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GatewayProviderRecord {
    pub provider: String,
    pub served: bool,
    pub reason: String,
    pub observed_at: Timestamp,
}

impl Store {
    /// Record the gateway's provider discovery outcome without credential material.
    /// # Errors
    /// Returns database or encoding errors.
    pub async fn record_gateway_provider(
        &self,
        provider: &str,
        served: bool,
        reason: &str,
    ) -> Result<()> {
        let record = GatewayProviderRecord {
            provider: provider.into(),
            served,
            reason: reason.into(),
            observed_at: Timestamp::now(),
        };
        self.transaction(|trx| {
            let record = &record;
            async move {
                write(
                    &trx,
                    &self.root.pack(&("gateway_provider", provider)),
                    record,
                )
            }
        })
        .await
    }

    /// Read the last discovery observation for a provider.
    /// # Errors
    /// Returns database or decoding errors.
    pub async fn gateway_provider(&self, provider: &str) -> Result<Option<GatewayProviderRecord>> {
        self.transaction(|trx| async move {
            read(&trx, &self.root.pack(&("gateway_provider", provider))).await
        })
        .await
    }
}
