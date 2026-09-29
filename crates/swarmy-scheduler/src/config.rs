use std::{collections::BTreeSet, time::Duration};

use anyhow::ensure;

pub struct Config {
    pub partitions: BTreeSet<u16>,
    pub scan_interval: Duration,
    pub resend_interval: Duration,
    pub provider: String,
    pub max_inference_wait: Duration,
    pub default_route: Option<String>,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let settings = swarmy_config::Settings::load()?.settings;
        Ok(Self {
            partitions: settings.scheduler.partitions.0,
            scan_interval: interval(settings.scheduler.scan_interval)?,
            resend_interval: interval(settings.scheduler.resend_interval)?,
            provider: settings.selection.provider,
            max_inference_wait: settings.inference.max_wait,
            default_route: settings.inference.default_route,
        })
    }
}

fn interval(value: Duration) -> anyhow::Result<Duration> {
    ensure!(!value.is_zero(), "scheduler interval must be positive");
    Ok(value)
}
