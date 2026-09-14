use std::{collections::BTreeSet, time::Duration};

use anyhow::{Context, bail, ensure};
use swarmy_store::RUNNABLE_PARTITIONS;

pub struct Config {
    pub partitions: BTreeSet<u16>,
    pub scan_interval: Duration,
    pub resend_interval: Duration,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            partitions: parse_partitions(&setting("SWARMY_SCHEDULER_PARTITIONS", "0-255")?)?,
            scan_interval: interval("SWARMY_SCHEDULER_SCAN_INTERVAL_MS", "1000")?,
            resend_interval: interval("SWARMY_SCHEDULER_RESEND_INTERVAL_MS", "5000")?,
        })
    }
}

pub fn setting(name: &str, default: &str) -> anyhow::Result<String> {
    match std::env::var(name) {
        Ok(value) => Ok(value),
        Err(std::env::VarError::NotPresent) => Ok(default.to_owned()),
        Err(error) => Err(error).with_context(|| format!("invalid {name}")),
    }
}

fn interval(name: &str, default: &str) -> anyhow::Result<Duration> {
    let millis: u64 = setting(name, default)?
        .parse()
        .with_context(|| format!("{name} must be a positive number of milliseconds"))?;
    ensure!(millis > 0, "{name} must be positive");
    Ok(Duration::from_millis(millis))
}

fn parse_partitions(value: &str) -> anyhow::Result<BTreeSet<u16>> {
    let mut partitions = BTreeSet::new();
    for component in value.split(',').map(str::trim) {
        let (first, last) = component.split_once('-').unwrap_or((component, component));
        let parse = |value: &str| {
            value.trim().parse::<u16>().with_context(|| {
                format!("invalid SWARMY_SCHEDULER_PARTITIONS component: {component}")
            })
        };
        let (first, last) = (parse(first)?, parse(last)?);
        if first > last || last >= RUNNABLE_PARTITIONS {
            bail!("SWARMY_SCHEDULER_PARTITIONS must be in 0-255 with ascending ranges");
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
