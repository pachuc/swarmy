use std::time::Duration;

use crate::{Error, Result};

pub struct Config {
    pub nats: String,
    pub bus: swarmy_bus::Config,
    pub settings: swarmy_config::Settings,
    pub concurrency: usize,
    pub resend_interval: Duration,
}

impl Config {
    /// Load and validate gateway settings.
    /// # Errors
    /// Returns invalid settings or transport configuration.
    pub fn from_env() -> Result<Self> {
        let settings = swarmy_config::Settings::load()?.settings;
        let concurrency = settings.gateway.concurrency;
        let ack_wait = settings.bus.ack_wait_ms;
        if concurrency == 0 || ack_wait < Duration::from_millis(30) {
            return Err(Error::Configuration(
                "concurrency must be positive and ack wait at least 30 ms",
            ));
        }
        Ok(Self {
            settings: settings.clone(),
            nats: settings.bus.nats_url.clone(),
            bus: settings.bus.bus_config()?,
            resend_interval: settings.scheduler.resend_interval_ms,
            concurrency,
        })
    }
}
