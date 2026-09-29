use std::time::Duration;

use crate::{Error, Result};
use swarmy_bus::{Config as BusConfig, SubjectToken};

pub struct Config {
    pub nats: String,
    pub bus: BusConfig,
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
        let ack_wait = settings.bus.ack_wait;
        if concurrency == 0 || ack_wait < Duration::from_millis(30) {
            return Err(Error::Configuration(
                "concurrency must be positive and ack wait at least 30 ms",
            ));
        }
        Ok(Self {
            settings: settings.clone(),
            nats: settings.bus.nats_url.clone(),
            bus: BusConfig {
                prefix: if settings.bus.prefix.is_empty() {
                    None
                } else {
                    Some(SubjectToken::new(settings.bus.prefix.clone())?)
                },
                ack_wait,
                max_deliver: settings.bus.max_deliver_i64(),
            },
            resend_interval: settings.scheduler.resend_interval,
            concurrency,
        })
    }
}
