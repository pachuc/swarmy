//! Remaining-quota headers published by inference providers.
//!
//! `OpenAI` publishes `x-ratelimit-remaining-*`, Anthropic publishes
//! `anthropic-ratelimit-*`. Bedrock surfaces throttling through SDK retry
//! metadata rather than headers, so the gateway records nothing for it.
//! Entries without published quotas use an operator-configured limit instead.

use std::collections::BTreeMap;

/// Collect numeric remaining-quota headers with the given prefix.
fn remaining_with(headers: &BTreeMap<String, String>, prefix: &str) -> BTreeMap<String, u64> {
    headers
        .iter()
        .filter(|(name, _)| name.starts_with(prefix))
        .filter_map(|(name, value)| value.trim().parse::<u64>().ok().map(|n| (name.clone(), n)))
        .collect()
}

/// Lowercase header names for prefix matching.
fn lowered(headers: &reqwest::header::HeaderMap) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_ascii_lowercase(), value.to_owned()))
        })
        .collect()
}

/// `OpenAI` `x-ratelimit-remaining-*` values as `(header, remaining)`.
#[must_use]
pub fn openai_remaining(headers: &reqwest::header::HeaderMap) -> BTreeMap<String, u64> {
    remaining_with(&lowered(headers), "x-ratelimit-remaining-")
}

/// Anthropic `anthropic-ratelimit-*` remaining values.
#[must_use]
pub fn anthropic_remaining(headers: &reqwest::header::HeaderMap) -> BTreeMap<String, u64> {
    let all = remaining_with(&lowered(headers), "anthropic-ratelimit-");
    all.into_iter()
        .filter(|(name, _)| name.contains("remaining"))
        .collect()
}

/// Parse already-lowered headers without a live HTTP response, for tests.
#[must_use]
pub fn remaining_from_lowered(
    headers: &BTreeMap<String, String>,
    prefix: &str,
) -> BTreeMap<String, u64> {
    remaining_with(headers, prefix)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openai_prefix_collects_numeric_remaining_only() {
        let headers = BTreeMap::from([
            ("x-ratelimit-remaining-requests".into(), "99".into()),
            ("x-ratelimit-remaining-tokens".into(), "12000".into()),
            ("x-ratelimit-limit-requests".into(), "100".into()),
            ("x-ratelimit-remaining-bad".into(), "many".into()),
        ]);
        let remaining = remaining_from_lowered(&headers, "x-ratelimit-remaining-");
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining["x-ratelimit-remaining-requests"], 99);
    }

    #[test]
    fn anthropic_prefix_keeps_remaining_entries() {
        let headers = BTreeMap::from([
            ("anthropic-ratelimit-requests-remaining".into(), "50".into()),
            ("anthropic-ratelimit-tokens-limit".into(), "100".into()),
        ]);
        let remaining = remaining_with(&headers, "anthropic-ratelimit-");
        assert_eq!(remaining.len(), 2);
        let filtered: BTreeMap<_, _> = remaining
            .into_iter()
            .filter(|(name, _)| name.contains("remaining"))
            .collect();
        assert_eq!(filtered.len(), 1);
    }
}
