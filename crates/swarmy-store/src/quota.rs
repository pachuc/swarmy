//! Quota observation per auth entry.
//!
//! Providers that publish remaining-quota headers update the entry's latest
//! observed values with their timestamp. Entries without published quotas
//! (such as `ChatGPT` subscriptions) use an operator-configured `limit` and
//! `window`; `used` is then computed from the entry's hourly rollups.

use std::collections::BTreeMap;

use jiff::Timestamp;
use serde::{Deserialize, Serialize};

use crate::{Result, Store, read};

/// Latest observed remaining-quota values for one entry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObservedQuota {
    pub values: BTreeMap<String, u64>,
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
/// over the window; observed quotas report the latest published `remaining`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EntryQuota {
    pub source: QuotaSource,
    pub used: u64,
    pub free: Option<u64>,
    pub limit: Option<u64>,
    pub window_seconds: Option<u64>,
    pub observed_at: Option<Timestamp>,
    pub remaining: BTreeMap<String, u64>,
}

/// Parse windows like `30m`, `5h`, `7d` into seconds.
#[must_use]
pub fn parse_window(value: &str) -> Option<u64> {
    let (number, unit) = value.split_at(value.len().checked_sub(1)?);
    let number: u64 = number.parse().ok()?;
    match unit {
        "s" => Some(number),
        "m" => number.checked_mul(60),
        "h" => number.checked_mul(3_600),
        "d" => number.checked_mul(86_400),
        "w" => number.checked_mul(604_800),
        _ => None,
    }
}

impl Store {
    fn observed_key(&self, provider: &str, label: &str) -> Vec<u8> {
        self.root.pack(&("entry_quota_observed", provider, label))
    }

    fn config_key(&self, provider: &str, label: &str) -> Vec<u8> {
        self.root.pack(&("entry_quota_config", provider, label))
    }

    /// Record the latest published remaining-quota values for one entry.
    /// # Errors
    /// Returns encoding or storage errors.
    pub async fn observe_entry_quota(
        &self,
        provider: &str,
        label: &str,
        remaining: &BTreeMap<String, u64>,
    ) -> Result<()> {
        if remaining.is_empty() {
            return Ok(());
        }
        let value = ObservedQuota {
            values: remaining.clone(),
            updated_at: Timestamp::now(),
        };
        let observed = self.observed_key(provider, label);
        let value_bytes = crate::encode(&value)?;
        if value_bytes.len() > crate::INLINE_LIMIT {
            return Err(crate::StoreError::TooLarge);
        }
        self.transaction(|trx| {
            let observed = &observed;
            let value_bytes = &value_bytes;
            async move {
                trx.set(observed, value_bytes);
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
    /// computes `used` from rollups; otherwise the latest observed values
    /// are reported with no window.
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
            });
        };
        let free = observed.values.values().copied().min();
        Ok(EntryQuota {
            source: QuotaSource::Observed,
            used: 0,
            free,
            limit: None,
            window_seconds: None,
            observed_at: Some(observed.updated_at),
            remaining: observed.values,
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
    }
}
