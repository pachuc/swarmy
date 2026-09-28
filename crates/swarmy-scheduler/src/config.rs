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
            partitions: swarmy_config::parse_partitions(&settings.scheduler_partitions)
                .map_err(|error| anyhow::anyhow!("SWARMY_SCHEDULER_PARTITIONS: {error}"))?,
            scan_interval: interval(settings.scheduler_scan_interval_ms)?,
            resend_interval: interval(settings.scheduler_resend_interval_ms)?,
            provider: settings.provider,
            max_inference_wait: Duration::from_secs(settings.inference.max_wait_seconds.get()),
            default_route: settings.inference.default_route,
        })
    }
}

fn interval(millis: u64) -> anyhow::Result<Duration> {
    ensure!(millis > 0, "scheduler interval must be positive");
    Ok(Duration::from_millis(millis))
}
