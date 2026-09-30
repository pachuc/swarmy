//! Partition selection shared by the worker and scheduler.
use std::collections::BTreeSet;
use std::fmt::{self, Display};
use std::str::FromStr;
use swarmy_core::RUNNABLE_PARTITIONS;

/// Invalid partition selection, naming the offending component.
#[derive(Clone, Debug, thiserror::Error)]
pub enum PartitionsError {
    /// A component is not a number or range, or is outside the runnable space.
    #[error("invalid partition component: {component}")]
    Invalid {
        /// The comma-separated piece that failed to parse or is out of range.
        component: String,
    },
}

/// A validated set of runnable partitions, serialized as compact ranges.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Partitions(pub BTreeSet<u16>);

impl Default for Partitions {
    fn default() -> Self {
        Self((0..RUNNABLE_PARTITIONS).collect())
    }
}

impl FromStr for Partitions {
    type Err = PartitionsError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut partitions = BTreeSet::new();
        for component in value.split(',').map(str::trim) {
            let (first, last) = component.split_once('-').unwrap_or((component, component));
            let parse = |part: &str| {
                part.trim()
                    .parse::<u16>()
                    .map_err(|_| PartitionsError::Invalid {
                        component: component.into(),
                    })
            };
            let (first, last) = (parse(first)?, parse(last)?);
            if first > last || last >= RUNNABLE_PARTITIONS {
                return Err(PartitionsError::Invalid {
                    component: component.into(),
                });
            }
            partitions.extend(first..=last);
        }
        Ok(Self(partitions))
    }
}

impl TryFrom<String> for Partitions {
    type Error = PartitionsError;

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
                if next != end.saturating_add(1) {
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

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;

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
