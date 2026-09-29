use anyhow::{Result, ensure};
use std::{collections::BTreeSet, path::PathBuf, time::Duration};
use swarmy_bus::Config as BusConfig;
use swarmy_harness::{GetTime, Harness, ToolRegistry};
use swarmy_llm::GenerationSettings;

/// Chaos kill points crash the worker at fixed step boundaries. The config
/// carries the selected point as an enum so chaos runs set it by name
/// without rebuilding; every call site and the setting parser share this
/// one list instead of repeating string literals.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KillPoint {
    AfterClaim,
    BeforeRelease,
    AfterRequestEvent,
    AfterRelease,
    AfterAdvance,
}

impl KillPoint {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::AfterClaim => "after_claim",
            Self::BeforeRelease => "before_release",
            Self::AfterRequestEvent => "after_request_event",
            Self::AfterRelease => "after_release",
            Self::AfterAdvance => "after_advance",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "after_claim" => Some(Self::AfterClaim),
            "before_release" => Some(Self::BeforeRelease),
            "after_request_event" => Some(Self::AfterRequestEvent),
            "after_release" => Some(Self::AfterRelease),
            "after_advance" => Some(Self::AfterAdvance),
            _ => None,
        }
    }
}

pub(crate) struct Config {
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
    pub memory_dir: PathBuf,
    pub memory_max_bytes: usize,
    pub kill_point: Option<KillPoint>,
    pub max_inference_wait: Duration,
    pub gateway_wait: Duration,
    pub allowed_providers: Option<Vec<String>>,
    pub default_route: Option<String>,
}

impl Config {
    pub(crate) fn from_settings(settings: &swarmy_config::Settings) -> Result<Self> {
        let catalog = settings.catalog()?;
        let provider = settings.selection.provider.clone();
        ensure!(
            catalog.provider(&provider).is_some(),
            "unsupported SWARMY_PROVIDER"
        );
        let effort = settings.selection.effort;
        let kill_point = settings
            .worker
            .kill_point
            .as_deref()
            .map(|value| {
                KillPoint::parse(value)
                    .ok_or_else(|| anyhow::anyhow!("invalid SWARMY_WORKER_KILL_POINT"))
            })
            .transpose()?;
        let mut tools = ToolRegistry::default();
        tools.register(Box::new(GetTime));
        swarmy_tools::register(&mut tools);
        swarmy_tools::register_display(&mut tools);
        Ok(Self {
            nats: settings.bus.nats_url.clone(),
            bus: settings.bus.bus_config()?,
            partitions: settings.worker.partitions.0.clone(),
            provider,
            lease_duration: settings.worker.lease_ms,
            placement_lease: settings.scheduler.placement_lease_secs,
            recovery_interval: settings.worker.recovery_interval_ms,
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
            memory_dir: settings.memory.dir.clone(),
            memory_max_bytes: settings.memory.max_bytes.get(),
            kill_point,
            max_inference_wait: settings.inference.max_wait_secs,
            gateway_wait: settings.inference.gateway_wait_secs,
            allowed_providers: settings.selection.providers.clone(),
            default_route: settings.inference.default_route.clone(),
        })
    }
}

impl Config {
    pub(crate) fn summarization_threshold(&self, provider: &str, model: &str) -> Option<u64> {
        self.summarize_at_tokens
            .or_else(|| {
                self.model_context_window_tokens
                    .map(|context| context.saturating_sub(16_384))
            })
            .or_else(|| self.catalog.summarize_at(provider, model))
    }

    pub(crate) fn side_summarization_threshold(&self, provider: &str, model: &str) -> u64 {
        self.summarization_threshold(provider, model)
            .unwrap_or(u64::MAX)
    }
}
