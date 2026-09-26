//! Quota window parsing shared by the control plane and the client.
//!
//! The parser is pure string handling with no store access, so it lives in
//! the core crate: the client binary parses `--window` without linking the
//! FoundationDB store.

/// Parse windows like `30m`, `5h`, `7d` into seconds. Never panics on
/// non-`ASCII` input; the unit is the final `ASCII` character.
#[must_use]
pub fn parse_window(value: &str) -> Option<u64> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    let (number, unit) = split_window(value)?;
    if number.is_empty() {
        return None;
    }
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

fn split_window(value: &str) -> Option<(&str, &str)> {
    if let Some(number) = value.strip_suffix('s') {
        Some((number, "s"))
    } else if let Some(number) = value.strip_suffix('m') {
        Some((number, "m"))
    } else if let Some(number) = value.strip_suffix('h') {
        Some((number, "h"))
    } else if let Some(number) = value.strip_suffix('d') {
        Some((number, "d"))
    } else if let Some(number) = value.strip_suffix('w') {
        Some((number, "w"))
    } else {
        None
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
        assert_eq!(parse_window("5é"), None);
        assert_eq!(parse_window("é"), None);
        assert_eq!(parse_window(" 5h "), Some(18_000));
    }
}
