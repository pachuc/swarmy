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
pub fn parse_reset_seconds(value: &str) -> Option<u64> {
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
pub fn openai_remaining(headers: &reqwest::header::HeaderMap) -> BTreeMap<String, u64> {
    remaining_with(&lowered(headers), "x-ratelimit-remaining-")
}

/// `OpenAI` `x-ratelimit-reset-*` windows in seconds until reset.
#[must_use]
pub fn openai_resets(headers: &reqwest::header::HeaderMap) -> BTreeMap<String, u64> {
    resets_with(&lowered(headers), "x-ratelimit-reset-")
}

/// Anthropic `anthropic-ratelimit-*` remaining values.
#[must_use]
pub fn anthropic_remaining(headers: &reqwest::header::HeaderMap) -> BTreeMap<String, u64> {
    let all = remaining_with(&lowered(headers), "anthropic-ratelimit-");
    all.into_iter()
        .filter(|(name, _)| name.contains("remaining"))
        .collect()
}

/// Anthropic `anthropic-ratelimit-*-reset` windows in seconds until reset.
#[must_use]
pub fn anthropic_resets(headers: &reqwest::header::HeaderMap) -> BTreeMap<String, u64> {
    let all = resets_with(&lowered(headers), "anthropic-ratelimit-");
    all.into_iter()
        .filter(|(name, _)| name.contains("reset"))
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

/// Parse already-lowered reset headers without a live HTTP response.
#[must_use]
pub fn resets_from_lowered(
    headers: &BTreeMap<String, String>,
    prefix: &str,
) -> BTreeMap<String, u64> {
    resets_with(headers, prefix)
}

/// Smallest reset window in seconds, if any reset header parsed.
#[must_use]
pub fn reset_window_seconds(resets: &BTreeMap<String, u64>) -> Option<u64> {
    resets.values().copied().min()
}

/// Remaining requests, matching headers whose name mentions requests.
#[must_use]
pub fn requests_remaining(remaining: &BTreeMap<String, u64>) -> Option<u64> {
    remaining
        .iter()
        .filter(|(name, _)| name.contains("request"))
        .map(|(_, value)| *value)
        .min()
}

/// Remaining tokens, matching headers whose name mentions tokens.
#[must_use]
pub fn tokens_remaining(remaining: &BTreeMap<String, u64>) -> Option<u64> {
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
        assert_eq!(requests_remaining(&remaining), Some(99));
        assert_eq!(tokens_remaining(&remaining), Some(12_000));
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

    #[test]
    fn reset_values_parse_durations_and_timestamps() {
        assert_eq!(parse_reset_seconds("90"), Some(90));
        assert_eq!(parse_reset_seconds("2s"), Some(2));
        assert_eq!(parse_reset_seconds("500ms"), Some(1));
        assert_eq!(parse_reset_seconds("2m"), Some(120));
        assert_eq!(parse_reset_seconds("bogus"), None);
        let resets = BTreeMap::from([
            ("x-ratelimit-reset-requests".into(), 60_u64),
            ("x-ratelimit-reset-tokens".into(), 300_u64),
        ]);
        assert_eq!(reset_window_seconds(&resets), Some(60));
    }

    #[test]
    fn compound_openai_reset_values_parse_to_seconds() {
        assert_eq!(parse_reset_seconds("6m0s"), Some(360));
        assert_eq!(parse_reset_seconds("1m30s"), Some(90));
        assert_eq!(parse_reset_seconds("2h0m0s"), Some(7_200));
        assert_eq!(parse_reset_seconds("1d2h"), Some(93_600));
        assert_eq!(parse_reset_seconds("1500ms"), Some(2));
    }
}
