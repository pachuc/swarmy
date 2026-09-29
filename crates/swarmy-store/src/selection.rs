//! Provider availability advertisements.
use crate::{Result, Store, read, write};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

/// Gateway tasks refresh this record every 30 seconds with a future expiry.
/// A skipped provider is recorded with an already expired advertisement so the
/// reason stays visible without granting routing authority.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GatewayProvider {
    pub expires_at: Timestamp,
    /// Discovery outcome of the last writing gateway.
    pub reason: String,
}

impl Store {
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
                &crate::keys::Keys::new(&self.root).gateway_provider(provider),
                record,
            )
        })
        .await
    }

    /// Advertise an independently selectable auth entry beside the provider-level record.
    /// # Errors
    /// Returns database or encoding errors.
    pub async fn put_gateway_entry(
        &self,
        provider: &str,
        label: &str,
        record: &GatewayProvider,
    ) -> Result<()> {
        self.transaction(|trx| async move {
            write(
                &trx,
                &crate::keys::Keys::new(&self.root).gateway_provider_entry(provider, label),
                record,
            )
        })
        .await
    }

    /// # Errors
    /// Returns database or encoding errors.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn gateway_entry(
        &self,
        provider: &str,
        label: &str,
    ) -> Result<Option<GatewayProvider>> {
        self.transaction(|trx| async move {
            read(
                &trx,
                &crate::keys::Keys::new(&self.root).gateway_provider_entry(provider, label),
            )
            .await
        })
        .await
    }

    /// Read the last advertisement for a provider, expired or not.
    /// # Errors
    /// Returns database or decoding errors.
    /// Test-only entry point, also available with the `test-support` feature.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn gateway_provider(&self, provider: &str) -> Result<Option<GatewayProvider>> {
        self.transaction(|trx| async move {
            read(
                &trx,
                &crate::keys::Keys::new(&self.root).gateway_provider(provider),
            )
            .await
        })
        .await
    }

    /// Whether any gateway has an unexpired advertisement for this provider.
    /// # Errors
    /// Returns database or decoding errors.
    pub async fn gateway_serves(&self, provider: &str) -> Result<bool> {
        self.transaction(|trx| async move {
            Ok(read::<GatewayProvider>(
                &trx,
                &crate::keys::Keys::new(&self.root).gateway_provider(provider),
            )
            .await?
            .is_some_and(|record| record.expires_at > self.now()))
        })
        .await
    }
}
