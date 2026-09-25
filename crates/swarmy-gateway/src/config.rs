use std::time::Duration;

use crate::{Error, Result};
use swarmy_bus::{Config as BusConfig, SubjectToken};

pub struct Config {
    pub cluster: String,
    pub directory: Vec<String>,
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
        let concurrency = settings.gateway_concurrency;
        let ack_wait = Duration::from_millis(settings.bus_ack_wait_ms);
        if concurrency == 0 || ack_wait < Duration::from_millis(30) {
            return Err(Error::Configuration(
                "concurrency must be positive and ack wait at least 30 ms",
            ));
        }
        Ok(Self {
            settings: settings.clone(),
            cluster: settings.fdb_cluster_file,
            directory: settings
                .store_directory
                .split('/')
                .map(str::to_owned)
                .collect(),
            nats: settings.nats_url,
            bus: BusConfig {
                prefix: if settings.bus_prefix.is_empty() {
                    None
                } else {
                    Some(SubjectToken::new(settings.bus_prefix)?)
                },
                ack_wait,
                max_deliver: settings.bus_max_deliver,
            },
            resend_interval: Duration::from_millis(settings.scheduler_resend_interval_ms),
            concurrency,
        })
    }
}
