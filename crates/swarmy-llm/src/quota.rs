//! Remaining-quota headers published by inference providers.
//!
//! `OpenAI` publishes `x-ratelimit-remaining-*` with `x-ratelimit-reset-*`
//! windows, Anthropic publishes `anthropic-ratelimit-*-remaining` with
//! `anthropic-ratelimit-*-reset` windows. Bedrock surfaces throttling through
//! SDK retry metadata rather than headers, so the gateway records nothing for
//! it. Entries without published quotas use an operator-configured limit
//! instead. Requests and tokens are separate dimensions and are never
//! combined into one number.

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

/// Parse a reset value into seconds until the window resets. Accepts plain
/// seconds (`90`), durations (`500ms`, `1s`, `6m`, and compound forms like
/// `6m0s`, `1m30s`, `2h0m0s`), and `RFC 3339` timestamps (seconds from now,
/// saturating to zero when in the past).
#[must_use]
pub(crate) fn parse_reset_seconds(value: &str) -> Option<u64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(seconds) = trimmed.parse::<u64>() {
        return Some(seconds);
    }
    if let Ok(seconds) = parse_duration_seconds(trimmed) {
        return Some(seconds);
    }
    if let Ok(stamp) = trimmed.parse::<jiff::Timestamp>() {
        let now = jiff::Timestamp::now().as_second();
        return Some(u64::try_from(stamp.as_second().saturating_sub(now)).unwrap_or(u64::MAX));
    }
    None
}

/// Parse a duration into seconds. Accepts bare seconds (`90`), single units
/// (`500ms`, `2s`, `6m`), and compound forms (`6m0s`, `1m30s`, `2h0m0s`) as
/// published in `OpenAI` reset headers. Units are `ms`, `s`, `m`, `h`, `d`.
fn parse_duration_seconds(value: &str) -> Result<u64, ()> {
    let mut rest = value.trim();
    if rest.is_empty() {
        return Err(());
    }
    // Bare seconds carry no unit.
    if rest.bytes().all(|byte| byte.is_ascii_digit()) {
        return rest.parse::<u64>().map_err(|_| ());
    }
    let mut total: u64 = 0;
    let mut matched = false;
    while !rest.is_empty() {
        let digits = rest.find(|c: char| !c.is_ascii_digit()).ok_or(())?;
        if digits == 0 {
            return Err(());
        }
        let (number, units) = rest.split_at(digits);
        let amount: u64 = number.parse().map_err(|_| ())?;
        let unit_end = units
            .find(|c: char| c.is_ascii_digit())
            .unwrap_or(units.len());
        let (unit, next) = units.split_at(unit_end);
        match unit {
            "ms" => {
                total = total.checked_add(amount.div_ceil(1_000)).ok_or(())?;
            }
            "s" => {
                total = total.checked_add(amount).ok_or(())?;
            }
            "m" => {
                total = total
                    .checked_add(amount.checked_mul(60).ok_or(())?)
                    .ok_or(())?;
            }
            "h" => {
                total = total
                    .checked_add(amount.checked_mul(3_600).ok_or(())?)
                    .ok_or(())?;
            }
            "d" => {
                total = total
                    .checked_add(amount.checked_mul(86_400).ok_or(())?)
                    .ok_or(())?;
            }
            _ => return Err(()),
        }
        matched = true;
        rest = next;
    }
    if matched { Ok(total) } else { Err(()) }
}

fn resets_with(headers: &BTreeMap<String, String>, prefix: &str) -> BTreeMap<String, u64> {
    headers
        .iter()
        .filter(|(name, _)| name.starts_with(prefix))
        .filter_map(|(name, value)| {
            parse_reset_seconds(value).map(|seconds| (name.clone(), seconds))
        })
        .collect()
}

/// `OpenAI` `x-ratelimit-remaining-*` values as `(header, remaining)`.
#[must_use]
pub(crate) fn openai_remaining(headers: &reqwest::header::HeaderMap) -> BTreeMap<String, u64> {
    remaining_with(&lowered(headers), "x-ratelimit-remaining-")
}

/// `OpenAI` `x-ratelimit-reset-*` windows in seconds until reset.
#[must_use]
pub(crate) fn openai_resets(headers: &reqwest::header::HeaderMap) -> BTreeMap<String, u64> {
    resets_with(&lowered(headers), "x-ratelimit-reset-")
}

/// Anthropic `anthropic-ratelimit-*` remaining values.
#[must_use]
pub(crate) fn anthropic_remaining(headers: &reqwest::header::HeaderMap) -> BTreeMap<String, u64> {
    let all = remaining_with(&lowered(headers), "anthropic-ratelimit-");
    all.into_iter()
        .filter(|(name, _)| name.contains("remaining"))
        .collect()
}

/// Anthropic `anthropic-ratelimit-*-reset` windows in seconds until reset.
#[must_use]
pub(crate) fn anthropic_resets(headers: &reqwest::header::HeaderMap) -> BTreeMap<String, u64> {
    let all = resets_with(&lowered(headers), "anthropic-ratelimit-");
    all.into_iter()
        .filter(|(name, _)| name.contains("reset"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(entries: &[(&str, &str)]) -> reqwest::header::HeaderMap {
        let mut map = reqwest::header::HeaderMap::new();
        for (name, value) in entries {
            map.insert(
                name.parse::<reqwest::header::HeaderName>().unwrap(),
                value.parse::<reqwest::header::HeaderValue>().unwrap(),
            );
        }
        map
    }

    #[test]
    fn openai_quota_headers_record_numeric_remaining_and_reset_windows() {
        let map = headers(&[
            ("x-ratelimit-remaining-requests", "99"),
            ("x-ratelimit-remaining-tokens", "12000"),
            ("x-ratelimit-limit-requests", "100"),
            ("x-ratelimit-remaining-bad", "many"),
            // Header names match case-insensitively.
            ("X-Ratelimit-Reset-Requests", "6m0s"),
            ("x-ratelimit-reset-tokens", "1500ms"),
            ("x-ratelimit-reset-date", "2099-01-01T00:00:00Z"),
            ("x-ratelimit-reset-empty", ""),
        ]);
        let remaining = openai_remaining(&map);
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining["x-ratelimit-remaining-requests"], 99);
        let resets = openai_resets(&map);
        assert_eq!(resets.len(), 3);
        assert_eq!(resets["x-ratelimit-reset-requests"], 360);
        assert_eq!(resets["x-ratelimit-reset-tokens"], 2);
        assert!(resets["x-ratelimit-reset-date"] > 1_000_000);
    }

    #[test]
    fn anthropic_quota_headers_keep_remaining_entries_and_parse_resets() {
        let map = headers(&[
            ("anthropic-ratelimit-requests-remaining", "50"),
            ("anthropic-ratelimit-tokens-limit", "100"),
            ("anthropic-ratelimit-tokens-reset", "90"),
            ("anthropic-ratelimit-requests-reset", "1d2h"),
            ("anthropic-ratelimit-foo-reset", "bogus"),
        ]);
        let remaining = anthropic_remaining(&map);
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining["anthropic-ratelimit-requests-remaining"], 50);
        let resets = anthropic_resets(&map);
        assert_eq!(resets.len(), 2);
        assert_eq!(resets["anthropic-ratelimit-tokens-reset"], 90);
        assert_eq!(resets["anthropic-ratelimit-requests-reset"], 93_600);
    }
}
