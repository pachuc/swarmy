//! Absolute and relative time bounds for the cost and quota views.
//!
//! The parser is pure string handling with no store access, so it lives in
//! the core crate: the client binary parses `--since` and `--until`
//! without linking the `FoundationDB` store.

use jiff::{Timestamp, ToSpan};

/// Parse a `--since` or `--until` bound against `now`.
///
/// Accepts RFC 3339 timestamps, calendar dates (`2026-09-01`, read as UTC
/// midnight), `now`, relative spans (`30s`, `5m`, `12h`, `7d`, `2w`,
/// `3mo`, `1y`), and calendar-aligned words (`day`, `2days`, `week`,
/// `3weeks`, `month`, `2months`, `year`, `5years`). Month and year spans
/// step the calendar, so `1mo` before March 31 lands at the end of February
/// rather than a fixed day count. `m` means minutes to match the quota
/// window parser; months spell `mo`. A bare calendar word means the start
/// of the current UTC day, week (Monday), month, or year; a leading number
/// `N` means the start of the period `N - 1` back, so `month` starts this
/// month and `2months` starts last month. Never panics on non-`ASCII`
/// input.
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
    if let Some(start) = calendar_start(value, now) {
        return Some(start);
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

/// Start of the calendar period named by `value`, or `None` when it is not
/// a calendar word. Accepts `day`, `week`, `month`, `year`, their plurals,
/// and a leading count (`2days`, `3months`): `N` periods means the start of
/// the period `N - 1` back, so `month` starts this month and `2months`
/// starts last month. Counts below one are rejected.
fn calendar_start(value: &str, now: Timestamp) -> Option<Timestamp> {
    // Longest suffixes first so `days` wins over `s` in the span parser
    // below; this check runs before spans anyway.
    let (number, unit) = [
        ("days", "day"),
        ("weeks", "week"),
        ("months", "month"),
        ("years", "year"),
        ("day", "day"),
        ("week", "week"),
        ("month", "month"),
        ("year", "year"),
    ]
    .iter()
    .find_map(|(suffix, unit)| value.strip_suffix(suffix).map(|number| (number, *unit)))?;
    let back: i64 = if number.is_empty() {
        1
    } else {
        number.parse().ok()?
    };
    if back < 1 {
        return None;
    }
    let zoned = now.to_zoned(jiff::tz::TimeZone::UTC);
    let date = zoned.date();
    let start = match unit {
        "day" => date,
        "week" => {
            let days: i64 = match date.weekday() {
                jiff::civil::Weekday::Monday => 0,
                jiff::civil::Weekday::Tuesday => 1,
                jiff::civil::Weekday::Wednesday => 2,
                jiff::civil::Weekday::Thursday => 3,
                jiff::civil::Weekday::Friday => 4,
                jiff::civil::Weekday::Saturday => 5,
                jiff::civil::Weekday::Sunday => 6,
            };
            date.saturating_sub(jiff::ToSpan::days(days))
        }
        "month" => date.first_of_month(),
        _ => date.first_of_year(),
    };
    let span = match unit {
        "day" => jiff::ToSpan::days(back - 1),
        "week" => jiff::ToSpan::days((back - 1).checked_mul(7)?),
        "month" => (back - 1).months(),
        _ => (back - 1).years(),
    };
    let start = start.checked_sub(span).ok()?;
    start
        .to_zoned(jiff::tz::TimeZone::UTC)
        .ok()
        .map(|zoned| zoned.timestamp())
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
        assert_eq!(
            parse_bound(" 2026-09-01 ", now),
            parse_bound("2026-09-01", now)
        );
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
    fn calendar_words_align_to_period_starts() {
        // 2026-09-26 is a Saturday.
        let now = now();
        assert_eq!(
            parse_bound("day", now),
            Some("2026-09-26T00:00:00Z".parse().unwrap())
        );
        assert_eq!(
            parse_bound("week", now),
            Some("2026-09-21T00:00:00Z".parse().unwrap())
        );
        assert_eq!(
            parse_bound("month", now),
            Some("2026-09-01T00:00:00Z".parse().unwrap())
        );
        assert_eq!(
            parse_bound("year", now),
            Some("2026-01-01T00:00:00Z".parse().unwrap())
        );
        // A leading count reaches the start of the period N - 1 back, so
        // `2months` starts last month for a quota question about last month.
        assert_eq!(
            parse_bound("2days", now),
            Some("2026-09-25T00:00:00Z".parse().unwrap())
        );
        assert_eq!(
            parse_bound("2weeks", now),
            Some("2026-09-14T00:00:00Z".parse().unwrap())
        );
        assert_eq!(
            parse_bound("2months", now),
            Some("2026-08-01T00:00:00Z".parse().unwrap())
        );
        assert_eq!(
            parse_bound("3months", now),
            Some("2026-07-01T00:00:00Z".parse().unwrap())
        );
        assert_eq!(
            parse_bound("2years", now),
            Some("2025-01-01T00:00:00Z".parse().unwrap())
        );
        // Singular with a count of one matches the bare word.
        assert_eq!(parse_bound("1month", now), parse_bound("month", now));
    }

    #[test]
    fn invalid_bounds_are_rejected() {
        let now = now();
        for value in [
            "",
            "yesterday",
            "5x",
            "mo",
            "1M",
            "-7d",
            "0months",
            "5é",
            "é",
            "2026-13-01",
        ] {
            assert_eq!(parse_bound(value, now), None, "{value}");
        }
    }
}
