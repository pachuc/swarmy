//! Absolute and relative time bounds for the cost and quota views.
//!
//! The parser is pure string handling with no store access, so it lives in
//! the core crate: the client binary parses `--since` and `--until`
//! without linking the `FoundationDB` store.

use jiff::{Timestamp, ToSpan};

/// Parse a `--since` or `--until` bound against `now`.
///
/// Accepts RFC 3339 timestamps, calendar dates (`2026-09-01`, read as UTC
/// midnight), `now`, and relative spans (`30s`, `5m`, `12h`, `7d`, `2w`,
/// `3mo`, `1y`). Month and year spans step the calendar, so `1mo` before
/// March 31 lands at the end of February rather than a fixed day count.
/// `m` means minutes to match the quota window parser; months spell `mo`.
/// Never panics on non-`ASCII` input.
#[must_use]
pub fn parse_bound(value: &str, now: Timestamp) -> Option<Timestamp> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if value == "now" {
        return Some(now);
    }
    if let Ok(stamp) = value.parse::<Timestamp>() {
        return Some(stamp);
    }
    if let Ok(date) = value.parse::<jiff::civil::Date>() {
        return date
            .to_zoned(jiff::tz::TimeZone::UTC)
            .ok()
            .map(|zoned| zoned.timestamp());
    }
    let (number, unit) = split_span(value)?;
    if number.is_empty() {
        return None;
    }
    let number: i64 = number.parse().ok()?;
    if number < 0 {
        return None;
    }
    match unit {
        "s" => checked_sub(now, number, 1),
        "m" => checked_sub(now, number, 60),
        "h" => checked_sub(now, number, 3_600),
        "d" => checked_sub(now, number, 86_400),
        "w" => checked_sub(now, number, 604_800),
        "mo" => now
            .to_zoned(jiff::tz::TimeZone::UTC)
            .checked_sub(number.months())
            .ok()
            .map(|zoned| zoned.timestamp()),
        "y" => now
            .to_zoned(jiff::tz::TimeZone::UTC)
            .checked_sub(number.years())
            .ok()
            .map(|zoned| zoned.timestamp()),
        _ => None,
    }
}

/// Subtract `number * unit_seconds` from `now`, rejecting overflow instead
/// of wrapping or panicking on huge spans like `9999999999d`.
fn checked_sub(now: Timestamp, number: i64, unit_seconds: i64) -> Option<Timestamp> {
    let seconds = number.checked_mul(unit_seconds)?;
    now.checked_sub(seconds.seconds()).ok()
}

fn split_span(value: &str) -> Option<(&str, &str)> {
    // `mo` must win over `m`, and the unit is always trailing `ASCII`, so
    // byte slicing at the split point stays on a character boundary.
    for unit in ["mo", "s", "m", "h", "d", "w", "y"] {
        if let Some(number) = value.strip_suffix(unit) {
            return Some((number, unit));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> Timestamp {
        "2026-09-26T12:00:00Z".parse().unwrap()
    }

    #[test]
    fn absolute_dates_and_now_parse() {
        let now = now();
        assert_eq!(parse_bound("now", now), Some(now));
        assert_eq!(
            parse_bound("2026-09-01T00:00:00Z", now),
            Some("2026-09-01T00:00:00Z".parse().unwrap())
        );
        assert_eq!(
            parse_bound("2026-09-01", now),
            Some("2026-09-01T00:00:00Z".parse().unwrap())
        );
        assert_eq!(parse_bound(" 2026-09-01 ", now), parse_bound("2026-09-01", now));
    }

    #[test]
    fn fixed_spans_count_back_from_now() {
        let now = now();
        assert_eq!(
            parse_bound("7d", now),
            Some("2026-09-19T12:00:00Z".parse().unwrap())
        );
        assert_eq!(
            parse_bound("90m", now),
            Some("2026-09-26T10:30:00Z".parse().unwrap())
        );
        assert_eq!(
            parse_bound("2w", now),
            Some("2026-09-12T12:00:00Z".parse().unwrap())
        );
    }

    #[test]
    fn calendar_spans_step_months_and_years() {
        let now = now();
        assert_eq!(
            parse_bound("3mo", now),
            Some("2026-06-26T12:00:00Z".parse().unwrap())
        );
        assert_eq!(
            parse_bound("1y", now),
            Some("2025-09-26T12:00:00Z".parse().unwrap())
        );
        // Month ends saturate instead of overflowing into the next month.
        let end_of_march: Timestamp = "2026-03-31T12:00:00Z".parse().unwrap();
        assert_eq!(
            parse_bound("1mo", end_of_march),
            Some("2026-02-28T12:00:00Z".parse().unwrap())
        );
    }

    #[test]
    fn invalid_bounds_are_rejected() {
        let now = now();
        for value in ["", "yesterday", "5x", "mo", "1M", "-7d", "5é", "é", "2026-13-01"] {
            assert_eq!(parse_bound(value, now), None, "{value}");
        }
    }
}
