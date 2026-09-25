use serde::{Deserialize, Serialize};
use swarmy_core::{AgentId, RequestId, SessionId, TokenUsage, UsageTotals};

use crate::{Result, Store, read};

/// Per-completion attribution; totals remain available under their existing keys.
///
/// New trailing fields keep old records readable: a missing presence byte
/// decodes to its default, while a truncated new value is still rejected.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UsageRecord {
    pub provider: String,
    pub entry: Option<String>,
    pub usage: TokenUsage,
    pub cost_micros: u64,
    #[serde(default, with = "swarmy_core::trailing")]
    pub session: Option<SessionId>,
    #[serde(default, with = "swarmy_core::trailing")]
    pub agent: Option<AgentId>,
    #[serde(default, with = "swarmy_core::trailing")]
    pub model: String,
    #[serde(default, with = "swarmy_core::trailing")]
    pub entry_kind: Option<String>,
    #[serde(default, with = "swarmy_core::trailing")]
    pub recorded_at: Option<jiff::Timestamp>,
}

pub struct UsageAttribution<'a> {
    pub request: RequestId,
    pub provider: &'a str,
    pub model: &'a str,
    pub recorded_at: jiff::Timestamp,
}

impl Store {
    /// Read the entry attributed to a completed inference request.
    /// # Errors
    /// Returns database or decoding errors.
    pub async fn inference_usage_record(&self, request: RequestId) -> Result<Option<UsageRecord>> {
        self.transaction(|trx| async move {
            read(
                &trx,
                &self
                    .root
                    .pack(&("usage_record", request.as_bytes().as_slice())),
            )
            .await
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

    /// Gateway records the entry kind beside the label for rollup dimensions.
    /// # Errors
    /// Returns database or encoding errors.
    pub async fn set_inference_entry_kind(
        &self,
        request: RequestId,
        kind: Option<&str>,
    ) -> Result<()> {
        self.transaction(|trx| async move {
            crate::write(
                &trx,
                &self
                    .root
                    .pack(&("inference_entry_kind", request.as_bytes().as_slice())),
                &kind.map(str::to_owned),
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
        attribution: &UsageAttribution<'_>,
        usage: &swarmy_core::TokenUsage,
        cost_micros: u64,
    ) -> Result<()> {
        let session_key = self
            .root
            .pack(&("usage", session.as_ulid().to_bytes().as_slice()));
        let agent_key = self
            .root
            .pack(&("usage_by_agent", agent.as_ulid().to_bytes().as_slice()));
        let (session_totals, agent_totals) = futures::try_join!(
            read::<UsageTotals>(trx, &session_key),
            read::<UsageTotals>(trx, &agent_key)
        )?;
        let entry_key = self
            .root
            .pack(&("inference_entry", attribution.request.as_bytes().as_slice()));
        let kind_key = self.root.pack(&(
            "inference_entry_kind",
            attribution.request.as_bytes().as_slice(),
        ));
        let (entry, kind): (Option<Option<String>>, Option<Option<String>>) =
            futures::try_join!(read(trx, &entry_key), read(trx, &kind_key))?;
        let entry = entry.flatten();
        let kind = kind.flatten();
        crate::write(
            trx,
            &self
                .root
                .pack(&("usage_record", attribution.request.as_bytes().as_slice())),
            &UsageRecord {
                provider: attribution.provider.into(),
                entry: entry.clone(),
                usage: usage.clone(),
                cost_micros,
                session: Some(session),
                agent: Some(agent),
                model: attribution.model.into(),
                entry_kind: kind.clone(),
                recorded_at: Some(attribution.recorded_at),
            },
        )?;
        trx.clear(&entry_key);
        trx.clear(&kind_key);
        for (key, totals) in [(session_key, session_totals), (agent_key, agent_totals)] {
            let mut totals = totals.unwrap_or_default();
            totals.add(usage, cost_micros);
            crate::write(trx, &key, &totals)?;
        }
        let input = BucketInput {
            session,
            agent,
            provider: attribution.provider,
            model: attribution.model,
            recorded_at: attribution.recorded_at,
            entry: entry.as_deref(),
            kind: kind.as_deref(),
            usage,
            cost: cost_micros,
        };
        self.record_buckets(trx, &input);
        Ok(())
    }

    fn record_buckets(&self, trx: &foundationdb::Transaction, input: &BucketInput<'_>) {
        let hour = crate::metering::hour_floor(input.recorded_at.as_second());
        let entry_id = input.entry.map_or_else(
            || "unknown".into(),
            |label| crate::metering::entry_key(input.provider, label),
        );
        let dimensions = [
            (
                crate::metering::MeteringDimension::Session.as_str(),
                input.session.to_string(),
            ),
            (
                crate::metering::MeteringDimension::Agent.as_str(),
                input.agent.to_string(),
            ),
            (
                crate::metering::MeteringDimension::Provider.as_str(),
                input.provider.into(),
            ),
            (crate::metering::MeteringDimension::Entry.as_str(), entry_id),
            (
                crate::metering::MeteringDimension::EntryKind.as_str(),
                input.kind.unwrap_or("unknown").into(),
            ),
            (
                crate::metering::MeteringDimension::Model.as_str(),
                input.model.into(),
            ),
        ];
        for (dimension, key) in &dimensions {
            self.metering_add(trx, dimension, key, hour, input.usage, input.cost);
        }
    }
}

/// Bucket counters for one completion, grouped to keep the record path small.
struct BucketInput<'a> {
    session: SessionId,
    agent: AgentId,
    provider: &'a str,
    model: &'a str,
    recorded_at: jiff::Timestamp,
    entry: Option<&'a str>,
    kind: Option<&'a str>,
    usage: &'a TokenUsage,
    cost: u64,
}
