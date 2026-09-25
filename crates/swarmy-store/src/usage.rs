use serde::{Deserialize, Serialize};
use swarmy_core::{AgentId, RequestId, SessionId, TokenUsage, UsageTotals};

use crate::{Result, Store, read};

/// Per-completion attribution; totals remain available under their existing keys.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UsageRecord {
    pub provider: String,
    pub entry: Option<String>,
    pub usage: TokenUsage,
    pub cost_micros: u64,
    /// Route that selected the entry, when a named route resolved it.
    #[serde(default)]
    pub route: Option<String>,
    /// Index into the resolved route, so metering names the exact step.
    #[serde(default)]
    pub route_step: Option<u32>,
}

/// Rows written before routes existed carry no route attribution.
#[derive(Deserialize)]
struct LegacyUsageRecord {
    provider: String,
    entry: Option<String>,
    usage: TokenUsage,
    cost_micros: u64,
}

pub(crate) struct UsageAttribution<'a> {
    pub request: RequestId,
    pub provider: &'a str,
    pub route: Option<String>,
    pub route_step: Option<u32>,
}

impl Store {
    /// Read the entry and route step attributed to a completed inference request.
    /// # Errors
    /// Returns database or decoding errors.
    pub async fn inference_usage_record(&self, request: RequestId) -> Result<Option<UsageRecord>> {
        self.transaction(|trx| async move {
            let bytes: Option<Vec<u8>> = trx
                .get(
                    &self
                        .root
                        .pack(&("usage_record", request.as_bytes().as_slice())),
                    false,
                )
                .await?
                .map(|value| value.to_vec());
            bytes
                .map(|bytes| {
                    swarmy_core::decode::<UsageRecord>(&bytes).or_else(|_| {
                        swarmy_core::decode::<LegacyUsageRecord>(&bytes).map(|legacy| UsageRecord {
                            provider: legacy.provider,
                            entry: legacy.entry,
                            usage: legacy.usage,
                            cost_micros: legacy.cost_micros,
                            route: None,
                            route_step: None,
                        })
                    })
                })
                .transpose()
                .map_err(crate::StoreError::from)
        })
        .await
    }

    /// Gateway records the chosen entry before its completion transaction.
    /// # Errors
    /// Returns database or encoding errors.
    pub async fn set_inference_entry(&self, request: RequestId, entry: Option<&str>) -> Result<()> {
        self.transaction(|trx| async move {
            crate::write(
                &trx,
                &self
                    .root
                    .pack(&("inference_entry", request.as_bytes().as_slice())),
                &entry.map(str::to_owned),
            )
        })
        .await
    }

    /// Read billed totals committed with successful inference completions.
    /// # Errors
    /// Returns database or decoding errors.
    pub async fn session_usage(&self, id: SessionId) -> Result<UsageTotals> {
        self.transaction(|trx| async move {
            Ok(read(
                &trx,
                &self
                    .root
                    .pack(&("usage", id.as_ulid().to_bytes().as_slice())),
            )
            .await?
            .unwrap_or_default())
        })
        .await
    }

    /// Read billed totals across all of an agent's sessions, including archives.
    /// # Errors
    /// Returns database or decoding errors.
    pub async fn agent_usage(&self, id: AgentId) -> Result<UsageTotals> {
        self.transaction(|trx| async move {
            Ok(read(
                &trx,
                &self
                    .root
                    .pack(&("usage_by_agent", id.as_ulid().to_bytes().as_slice())),
            )
            .await?
            .unwrap_or_default())
        })
        .await
    }
}

impl Store {
    pub(crate) async fn record_usage(
        &self,
        trx: &foundationdb::Transaction,
        session: SessionId,
        agent: AgentId,
        attribution: UsageAttribution<'_>,
        usage: &swarmy_core::TokenUsage,
        cost_micros: u64,
    ) -> Result<()> {
        let session_key = self
            .root
            .pack(&("usage", session.as_ulid().to_bytes().as_slice()));
        let agent_key = self
            .root
            .pack(&("usage_by_agent", agent.as_ulid().to_bytes().as_slice()));
        let (session, agent) = futures::try_join!(
            read::<UsageTotals>(trx, &session_key),
            read::<UsageTotals>(trx, &agent_key)
        )?;
        let entry_key = self
            .root
            .pack(&("inference_entry", attribution.request.as_bytes().as_slice()));
        let entry: Option<Option<String>> = read(trx, &entry_key).await?;
        crate::write(
            trx,
            &self
                .root
                .pack(&("usage_record", attribution.request.as_bytes().as_slice())),
            &UsageRecord {
                provider: attribution.provider.into(),
                entry: entry.flatten(),
                usage: usage.clone(),
                cost_micros,
                route: attribution.route.clone(),
                route_step: attribution.route_step,
            },
        )?;
        trx.clear(&entry_key);
        for (key, totals) in [(session_key, session), (agent_key, agent)] {
            let mut totals = totals.unwrap_or_default();
            totals.add(usage, cost_micros);
            crate::write(trx, &key, &totals)?;
        }
        Ok(())
    }
}
