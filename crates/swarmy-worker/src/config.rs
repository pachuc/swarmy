use anyhow::{Result, ensure};
use std::{collections::BTreeSet, time::Duration};
use swarmy_bus::{Config as BusConfig, SubjectToken};
use swarmy_harness::{GetTime, Harness, ToolRegistry};
use swarmy_llm::GenerationSettings;

pub struct Config {
    pub settings: swarmy_config::Settings,
    pub nats: String,
    pub bus: BusConfig,
    pub partitions: BTreeSet<u16>,
    pub provider: String,
    pub lease_duration: Duration,
    pub placement_lease: Duration,
    pub recovery_interval: Duration,
    pub harness: Harness,
    pub summarize_at_tokens: Option<u64>,
    pub model_context_window_tokens: Option<u64>,
    pub catalog: swarmy_llm::catalog::Catalog,
    pub memory_dir: String,
    pub memory_max_bytes: usize,
    pub kill_point: Option<String>,
    pub max_inference_wait: Duration,
    pub gateway_wait: Duration,
    pub allowed_providers: Option<Vec<String>>,
    pub default_route: Option<String>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let settings = swarmy_config::Settings::load()?.settings;
        let catalog = settings.catalog()?;
        let provider = settings.selection.provider.clone();
        ensure!(
            catalog.provider(&provider).is_some(),
            "unsupported SWARMY_PROVIDER"
        );
        let prefix = settings.bus.prefix.clone();
        let effort = settings.selection.effort;
        let kill_point = settings.worker.kill_point.clone();
        ensure!(
            kill_point.as_deref().is_none_or(|value| matches!(
                value,
                "after_claim"
                    | "after_request_event"
                    | "before_release"
                    | "after_release"
                    | "after_advance"
            )),
            "invalid SWARMY_WORKER_KILL_POINT"
        );
        let mut tools = ToolRegistry::default();
        tools.register(Box::new(GetTime));
        swarmy_tools::register(&mut tools);
        swarmy_tools::register_display(&mut tools);
        Ok(Self {
            nats: settings.bus.nats_url.clone(),
            bus: BusConfig {
                prefix: if prefix.is_empty() {
                    None
                } else {
                    Some(SubjectToken::new(prefix)?)
                },
                ack_wait: duration(settings.bus.ack_wait)?,
                max_deliver: settings.bus.max_deliver_i64(),
            },
            partitions: settings.worker.partitions.0.clone(),
            provider,
            lease_duration: duration(settings.worker.lease)?,
            placement_lease: settings.placement_lease,
            recovery_interval: duration(settings.worker.recovery_interval)?,
            harness: Harness {
                system_prompt_template: settings.context.system_prompt.clone(),
                settings: GenerationSettings {
                    model: settings.selection.model.clone(),
                    reasoning_effort: Some(effort),
                    ..Default::default()
                },
                tools,
            },
            summarize_at_tokens: settings.context.summarize_at.map(std::num::NonZeroU64::get),
            model_context_window_tokens: settings
                .context
                .context_window
                .map(std::num::NonZeroU64::get),
            catalog,
            memory_dir: settings
                .memory
                .dir
                .to_string_lossy()
                .into_owned(),
            memory_max_bytes: settings.memory.max_bytes.get(),
            kill_point,
            max_inference_wait: settings.inference.max_wait,
            gateway_wait: settings.inference.gateway_wait,
            allowed_providers: settings.selection.providers.clone(),
            default_route: settings.inference.default_route.clone(),
            settings,
        })
    }
}

impl Config {
    pub fn summarization_threshold(&self, provider: &str, model: &str) -> Option<u64> {
        self.summarize_at_tokens
            .or_else(|| {
                self.model_context_window_tokens
                    .map(|context| context.saturating_sub(16_384))
            })
            .or_else(|| self.catalog.summarize_at(provider, model))
    }

    pub fn side_summarization_threshold(&self, provider: &str, model: &str) -> u64 {
        self.summarization_threshold(provider, model)
            .unwrap_or(u64::MAX)
    }
}

fn duration(value: Duration) -> Result<Duration> {
    ensure!(
        value >= Duration::from_millis(30),
        "worker duration must be at least 30 ms"
    );
    Ok(value)
}
