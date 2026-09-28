//! Partition selection shared by the worker and scheduler.
use std::collections::BTreeSet;
use swarmy_core::RUNNABLE_PARTITIONS;

/// Parse a comma-separated list of partition numbers and inclusive ranges.
/// The runnable partition space is fixed at 256 entries.
///
/// # Errors
/// Returns an error for malformed or out-of-range components.
pub fn parse_partitions(value: &str) -> Result<BTreeSet<u16>, String> {
    let mut partitions = BTreeSet::new();
    for component in value.split(',').map(str::trim) {
        let (first, last) = component.split_once('-').unwrap_or((component, component));
        let parse = |part: &str| {
            part.trim()
                .parse::<u16>()
                .map_err(|_| format!("invalid partition component: {component}"))
        };
        let (first, last) = (parse(first)?, parse(last)?);
        if first > last || last >= RUNNABLE_PARTITIONS {
            return Err(format!(
                "{value}: partitions must be in 0-{} with ascending ranges",
                RUNNABLE_PARTITIONS - 1
            ));
        }
        partitions.extend(first..=last);
    }
    Ok(partitions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partitions_accept_ranges_lists_and_duplicates() {
        assert_eq!(parse_partitions("0-255").unwrap().len(), 256);
        assert_eq!(
            parse_partitions(" 0, 2-4, 3,255 ").unwrap(),
            BTreeSet::from([0, 2, 3, 4, 255])
        );
        for invalid in ["", "256", "4-2", "-1", "0-256", "1,", "1-2-3", "x"] {
            assert!(parse_partitions(invalid).is_err(), "accepted {invalid:?}");
        }
    }
}
