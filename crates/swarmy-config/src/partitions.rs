//! Partition selection shared by the worker and scheduler.
use std::collections::BTreeSet;
use std::fmt::{self, Display};
use std::str::FromStr;
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
                "partitions must be in 0-{} with ascending ranges",
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

    #[test]
    fn typed_partitions_display_compact_ranges_and_round_trip() {
        let full = Partitions::default();
        assert_eq!(full.0.len(), 256);
        assert_eq!(full.to_string(), "0-255");
        let parsed: Partitions = " 0, 2-4, 3,255 ".parse().unwrap();
        assert_eq!(parsed.0, BTreeSet::from([0, 2, 3, 4, 255]));
        assert_eq!(parsed.to_string(), "0, 2-4, 255");
        assert!("256".parse::<Partitions>().is_err());
    }
}

/// A validated set of runnable partitions, serialized as compact ranges.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Partitions(pub BTreeSet<u16>);

impl Default for Partitions {
    fn default() -> Self {
        Self((0..swarmy_core::RUNNABLE_PARTITIONS).collect())
    }
}

impl Partitions {
    /// The full runnable partition space.
    #[must_use]
    pub fn all() -> Self {
        Self::default()
    }
}

impl FromStr for Partitions {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_partitions(value).map(Self)
    }
}

impl TryFrom<String> for Partitions {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<Partitions> for String {
    fn from(value: Partitions) -> Self {
        value.to_string()
    }
}

impl Display for Partitions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut iter = self.0.iter().peekable();
        let mut first = true;
        while let Some(&start) = iter.next() {
            let mut end = start;
            while let Some(&&next) = iter.peek() {
                if next != end.checked_add(1).unwrap_or(u16::MAX) {
                    break;
                }
                end = next;
                iter.next();
            }
            if !first {
                f.write_str(", ")?;
            }
            first = false;
            if start == end {
                write!(f, "{start}")?;
            } else {
                write!(f, "{start}-{end}")?;
            }
        }
        Ok(())
    }
}
