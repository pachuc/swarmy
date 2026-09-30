use std::{collections::BTreeSet, time::Duration};

pub(crate) struct Config {
    pub partitions: BTreeSet<u16>,
    pub scan_interval: Duration,
    pub resend_interval: Duration,
    pub provider: String,
    pub max_inference_wait: Duration,
    pub default_route: Option<String>,
}

impl Config {
    pub(crate) fn from_env() -> anyhow::Result<Self> {
        let settings = swarmy_config::Settings::load()?.settings;
        Ok(Self {
            partitions: settings.scheduler.partitions.0,
            scan_interval: settings.scheduler.scan_interval_ms,
            resend_interval: settings.scheduler.resend_interval_ms,
            provider: settings.selection.provider,
            max_inference_wait: settings.inference.max_wait_secs,
            default_route: settings.inference.default_route,
        })
    }
}
