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
    pub session: Option<SessionId>,
    pub agent: Option<AgentId>,
    pub model: String,
    pub entry_kind: Option<String>,
    pub recorded_at: Option<jiff::Timestamp>,
    /// Route that selected the entry, when a named route resolved it.
    pub route: Option<String>,
    /// Index into the resolved route, so metering names the exact step.
    pub route_step: Option<u32>,
}

pub struct UsageAttribution<'a> {
    pub request: RequestId,
    pub provider: &'a str,
    pub model: &'a str,
    pub recorded_at: jiff::Timestamp,
    pub entry: Option<&'a str>,
    pub entry_kind: Option<&'a str>,
    pub route: Option<String>,
    pub route_step: Option<u32>,
}

impl Store {
    /// Read the entry and route step attributed to a completed inference request.
    /// # Errors
    /// Returns database or decoding errors.
    /// Test-only entry point, also available with the `test-support` feature.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn inference_usage_record(&self, request: RequestId) -> Result<Option<UsageRecord>> {
        self.transaction(|trx| async move { read(&trx, &self.keys().usage_record(request)).await })
            .await
    }

    /// Read billed totals committed with successful inference completions.
    /// # Errors
    /// Returns database or decoding errors.
    pub async fn session_usage(&self, id: SessionId) -> Result<UsageTotals> {
        self.transaction(|trx| async move {
            Ok(read(&trx, &self.keys().usage(id))
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
            Ok(read(&trx, &self.keys().usage_by_agent(id))
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
        let session_key = self.keys().usage(session);
        let agent_key = self.keys().usage_by_agent(agent);
        let (session_totals, agent_totals) = futures::try_join!(
            read::<UsageTotals>(trx, &session_key),
            read::<UsageTotals>(trx, &agent_key)
        )?;
        // The completion carries the entry, so the hot path needs no extra reads.
        let (entry, kind) = (
            attribution.entry.map(str::to_owned),
            attribution.entry_kind.map(str::to_owned),
        );
        crate::write(
            trx,
            &self.keys().usage_record(attribution.request),
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
                route: attribution.route.clone(),
                route_step: attribution.route_step,
            },
        )?;
        // Secondary index for bounded pruning, written in the same transaction.
        let hour = crate::metering::hour_floor(attribution.recorded_at.as_second());
        trx.set(
            &self.keys().usage_record_by_time(hour, attribution.request),
            &[],
        );
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
        // Completions without an entry keep their provider, so breakdowns
        // name it and only the entry is missing (`provider/-`).
        let entry_id = input.entry.map_or_else(
            || crate::metering::unknown_entry_key(input.provider),
            |label| crate::metering::entry_key(input.provider, label),
        );
        let singles = [
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
                input.provider.to_owned(),
            ),
            (
                crate::metering::MeteringDimension::Entry.as_str(),
                entry_id.clone(),
            ),
            (
                crate::metering::MeteringDimension::EntryKind.as_str(),
                input.kind.unwrap_or("unknown").to_owned(),
            ),
            (
                crate::metering::MeteringDimension::Model.as_str(),
                input.model.to_owned(),
            ),
        ];
        for (dimension, key) in &singles {
            self.metering_add_single(
                trx,
                key,
                &crate::metering::BucketWrite {
                    dimension,
                    hour,
                    usage: input.usage,
                    cost_micros: input.cost,
                },
            );
        }
        // Combined dimensions keep per-owner entry attribution without
        // scanning completion records, which prune after their retention
        // window. Providers derive from the entry names on read.
        let agent = input.agent.to_string();
        let session = input.session.to_string();
        for (dimension, owner) in [
            (
                crate::metering::MeteringDimension::AgentEntry.as_str(),
                agent.as_str(),
            ),
            (
                crate::metering::MeteringDimension::SessionEntry.as_str(),
                session.as_str(),
            ),
        ] {
            self.metering_add_combined(
                trx,
                owner,
                &entry_id,
                &crate::metering::BucketWrite {
                    dimension,
                    hour,
                    usage: input.usage,
                    cost_micros: input.cost,
                },
            );
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_record_has_fixed_bytes() {
        let record = UsageRecord {
            provider: "p".into(),
            entry: None,
            usage: TokenUsage::default(),
            cost_micros: 0,
            session: None,
            agent: None,
            model: "m".into(),
            entry_kind: None,
            recorded_at: None,
            route: None,
            route_step: None,
        };
        let bytes = swarmy_core::encode(&record).unwrap();
        assert_eq!(
            bytes,
            [1, 1, 112, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 109, 0, 0, 0, 0]
        );
        let decoded: UsageRecord = swarmy_core::decode(&bytes).unwrap();
        assert_eq!(decoded.provider, "p");
    }
}
