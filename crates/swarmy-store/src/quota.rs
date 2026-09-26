//! Quota observation per auth entry.
//!
//! Providers that publish remaining-quota headers update the entry's latest
//! observed values with their timestamp. Entries without published quotas
//! (such as `ChatGPT` subscriptions) use an operator-configured `limit` and
//! `window`; `used` is then computed from the entry's hourly rollups.
//! Requests and tokens are separate dimensions: observed quotas expose both
//! without combining them, and configured quotas count completions.

use std::collections::BTreeMap;

use foundationdb::Transaction;
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

use crate::{Result, Store, read};

/// Latest observed remaining-quota values for one entry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObservedQuota {
    pub values: BTreeMap<String, u64>,
    pub resets: BTreeMap<String, u64>,
    pub window_seconds: Option<u64>,
    pub updated_at: Timestamp,
}

/// Operator-configured quota for entries without published quotas.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct QuotaConfig {
    pub limit: u64,
    pub window_seconds: u64,
}

/// Where an entry's quota numbers came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaSource {
    Observed,
    Configured,
}

/// Quota view for one entry. Configured quotas compute `used` from rollups
/// over the window; observed quotas report the latest published `remaining`
/// with requests and tokens kept separate. Rollup buckets are hourly, so a
/// configured `used` counts whole hourly buckets overlapping
/// `[now - window, now)` rather than individual completions in the window.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EntryQuota {
    pub source: QuotaSource,
    pub used: u64,
    pub free: Option<u64>,
    pub limit: Option<u64>,
    pub window_seconds: Option<u64>,
    pub observed_at: Option<Timestamp>,
    pub remaining: BTreeMap<String, u64>,
    pub requests_remaining: Option<u64>,
    pub tokens_remaining: Option<u64>,
}

/// Parse windows like `30m`, `5h`, `7d` into seconds. The parser lives in
/// the core crate so the client parses `--window` without linking the store.
pub use swarmy_core::quota::parse_window;

impl Store {
    fn observed_key(&self, provider: &str, label: &str) -> Vec<u8> {
        self.root.pack(&("entry_quota_observed", provider, label))
    }

    fn config_key(&self, provider: &str, label: &str) -> Vec<u8> {
        self.root.pack(&("entry_quota_config", provider, label))
    }

    pub(crate) fn write_observed(
        &self,
        trx: &Transaction,
        provider: &str,
        label: &str,
        remaining: &BTreeMap<String, u64>,
        resets: &BTreeMap<String, u64>,
        now: Timestamp,
    ) -> Result<()> {
        if remaining.is_empty() {
            return Ok(());
        }
        let window_seconds = resets.values().copied().min();
        crate::write(
            trx,
            &self.observed_key(provider, label),
            &ObservedQuota {
                values: remaining.clone(),
                resets: resets.clone(),
                window_seconds,
                updated_at: now,
            },
        )
    }

    /// Record the latest published remaining-quota values for one entry.
    /// # Errors
    /// Returns encoding or storage errors.
    pub async fn observe_entry_quota(
        &self,
        provider: &str,
        label: &str,
        remaining: &BTreeMap<String, u64>,
        resets: &BTreeMap<String, u64>,
    ) -> Result<()> {
        if remaining.is_empty() {
            return Ok(());
        }
        let now = Timestamp::now();
        let remaining = remaining.clone();
        let resets = resets.clone();
        let key = self.observed_key(provider, label);
        let value = crate::encode(&ObservedQuota {
            values: remaining,
            window_seconds: resets.values().copied().min(),
            resets,
            updated_at: now,
        })?;
        if value.len() > crate::INLINE_LIMIT {
            return Err(crate::StoreError::TooLarge);
        }
        self.transaction(|trx| {
            let key = &key;
            let value = &value;
            async move {
                trx.set(key, value);
                Ok(())
            }
        })
        .await
    }

    /// Set the operator-configured limit and window for one entry.
    /// # Errors
    /// Returns encoding or storage errors.
    pub async fn set_entry_quota_config(
        &self,
        provider: &str,
        label: &str,
        limit: u64,
        window_seconds: u64,
    ) -> Result<()> {
        let key = self.config_key(provider, label);
        self.transaction(|trx| {
            let key = &key;
            async move {
                crate::write(
                    &trx,
                    key,
                    &QuotaConfig {
                        limit,
                        window_seconds,
                    },
                )
            }
        })
        .await
    }

    async fn quota_rows(
        &self,
        provider: &str,
        label: &str,
    ) -> Result<(Option<ObservedQuota>, Option<QuotaConfig>)> {
        let observed_key = self.observed_key(provider, label);
        let config_key = self.config_key(provider, label);
        self.transaction(|trx| {
            let observed_key = &observed_key;
            let config_key = &config_key;
            async move {
                let observed = read::<ObservedQuota>(&trx, observed_key).await?;
                let config = read::<QuotaConfig>(&trx, config_key).await?;
                Ok((observed, config))
            }
        })
        .await
    }

    /// Quota view for one entry. A configured limit takes precedence and
    /// computes `used` from rollups over the exact `[now - window, now)`
    /// range (counting whole hourly buckets); otherwise the latest observed
    /// requests and tokens are reported separately with the smallest reset
    /// window, if any.
    /// # Errors
    /// Returns decoding, storage, or time errors.
    pub async fn entry_quota(&self, provider: &str, label: &str) -> Result<EntryQuota> {
        let (observed, config) = self.quota_rows(provider, label).await?;
        if let Some(config) = config {
            let now = Timestamp::now();
            let from = now
                .as_second()
                .checked_sub(i64::try_from(config.window_seconds).unwrap_or(i64::MAX))
                .and_then(|second| Timestamp::from_second(second).ok())
                .unwrap_or(Timestamp::UNIX_EPOCH);
            let key = crate::metering::entry_key(provider, label);
            let groups = self
                .usage(
                    crate::metering::MeteringDimension::Entry,
                    &key,
                    from,
                    now,
                    crate::metering::UsageGroupBy::Day,
                )
                .await?;
            let used: u64 = groups.iter().map(|group| group.completions).sum();
            return Ok(EntryQuota {
                source: QuotaSource::Configured,
                used,
                free: Some(config.limit.saturating_sub(used)),
                limit: Some(config.limit),
                window_seconds: Some(config.window_seconds),
                observed_at: observed.map(|quota| quota.updated_at),
                remaining: BTreeMap::new(),
                requests_remaining: None,
                tokens_remaining: None,
            });
        }
        let Some(observed) = observed else {
            return Ok(EntryQuota {
                source: QuotaSource::Observed,
                used: 0,
                free: None,
                limit: None,
                window_seconds: None,
                observed_at: None,
                remaining: BTreeMap::new(),
                requests_remaining: None,
                tokens_remaining: None,
            });
        };
        let requests = requests_remaining(&observed.values);
        let tokens = tokens_remaining(&observed.values);
        Ok(EntryQuota {
            source: QuotaSource::Observed,
            used: 0,
            free: requests,
            limit: None,
            window_seconds: observed.window_seconds,
            observed_at: Some(observed.updated_at),
            remaining: observed.values,
            requests_remaining: requests,
            tokens_remaining: tokens,
        })
    }

    /// Remove quota rows alongside credential deletion.
    /// # Errors
    /// Returns storage errors.
    pub async fn clear_entry_quota(&self, provider: &str, label: &str) -> Result<()> {
        let observed = self.observed_key(provider, label);
        let config = self.config_key(provider, label);
        self.transaction(|trx| {
            let observed = &observed;
            let config = &config;
            async move {
                trx.clear(observed);
                trx.clear(config);
                Ok(())
            }
        })
        .await
    }
}

fn requests_remaining(remaining: &BTreeMap<String, u64>) -> Option<u64> {
    remaining
        .iter()
        .filter(|(name, _)| name.contains("request"))
        .map(|(_, value)| *value)
        .min()
}

fn tokens_remaining(remaining: &BTreeMap<String, u64>) -> Option<u64> {
    remaining
        .iter()
        .filter(|(name, _)| name.contains("token"))
        .map(|(_, value)| *value)
        .min()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_strings_parse_to_seconds() {
        assert_eq!(parse_window("30s"), Some(30));
        assert_eq!(parse_window("5h"), Some(18_000));
        assert_eq!(parse_window("7d"), Some(604_800));
        assert_eq!(parse_window("2w"), Some(1_209_600));
        assert_eq!(parse_window("5x"), None);
        assert_eq!(parse_window("h"), None);
        assert_eq!(parse_window(""), None);
        assert_eq!(parse_window("5é"), None);
        assert_eq!(parse_window("é"), None);
        assert_eq!(parse_window(" 5h "), Some(18_000));
    }
}
