use anyhow::{Context, Result, bail, ensure};
use std::{collections::BTreeSet, time::Duration};
use swarmy_bus::{Config as BusConfig, SubjectToken};
use swarmy_harness::{GetTime, Harness, ToolRegistry};
use swarmy_llm::{GenerationSettings, ReasoningEffort};
use swarmy_store::RUNNABLE_PARTITIONS;

pub struct Config {
    pub cluster: String,
    pub directory: Vec<String>,
    pub nats: String,
    pub bus: BusConfig,
    pub partitions: BTreeSet<u16>,
    pub provider: String,
    pub lease_duration: Duration,
    pub placement_lease: Duration,
    pub recovery_interval: Duration,
    pub harness: Harness,
    pub summarize_at_tokens: u64,
    pub memory_dir: String,
    pub memory_max_bytes: usize,
    pub kill_point: Option<String>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let settings = swarmy_config::Settings::load()?.settings;
        let provider = settings.provider;
        ensure!(
            matches!(provider.as_str(), "fake" | "chatgpt"),
            "unsupported SWARMY_PROVIDER"
        );
        let directory: Vec<_> = settings
            .store_directory
            .split('/')
            .map(str::to_owned)
            .collect();
        ensure!(
            directory.iter().all(|part| !part.is_empty()),
            "empty store directory component"
        );
        let prefix = settings.bus_prefix;
        let effort = match settings.reasoning_effort.as_str() {
            "none" => ReasoningEffort::None,
            "minimal" => ReasoningEffort::Minimal,
            "low" => ReasoningEffort::Low,
            "medium" => ReasoningEffort::Medium,
            "high" => ReasoningEffort::High,
            "xhigh" => ReasoningEffort::Xhigh,
            _ => bail!("invalid SWARMY_REASONING_EFFORT"),
        };
        let kill_point = settings.worker_kill_point;
        ensure!(
            kill_point.as_deref().is_none_or(|value| matches!(
                value,
                "after_claim" | "after_request_event" | "before_release" | "after_release"
            )),
            "invalid SWARMY_WORKER_KILL_POINT"
        );
        let mut tools = ToolRegistry::default();
        tools.register(Box::new(GetTime));
        swarmy_tools::register(&mut tools);
        Ok(Self {
            cluster: settings.fdb_cluster_file,
            directory,
            nats: settings.nats_url,
            bus: BusConfig {
                prefix: if prefix.is_empty() {
                    None
                } else {
                    Some(SubjectToken::new(prefix)?)
                },
                ack_wait: duration(settings.bus_ack_wait_ms)?,
                max_deliver: settings.bus_max_deliver,
            },
            partitions: parse_partitions(&settings.worker_partitions)?,
            provider,
            lease_duration: duration(settings.worker_lease_ms)?,
            placement_lease: Duration::from_secs(settings.placement_lease_seconds.get()),
            recovery_interval: duration(settings.worker_recovery_interval_ms)?,
            harness: Harness {
                system_prompt_template: settings.system_prompt,
                settings: GenerationSettings {
                    model: settings.model,
                    reasoning_effort: Some(effort),
                    ..Default::default()
                },
                tools,
            },
            summarize_at_tokens: settings.summarize_at_tokens.map_or(
                settings.model_context_window_tokens.get()
                    - settings.model_context_window_tokens.get() / 4,
                std::num::NonZeroU64::get,
            ),
            memory_dir: settings.memory_dir,
            memory_max_bytes: settings.memory_max_bytes.get(),
            kill_point,
        })
    }
}

fn duration(millis: u64) -> Result<Duration> {
    ensure!(millis >= 30, "worker duration must be at least 30 ms");
    Ok(Duration::from_millis(millis))
}

fn parse_partitions(value: &str) -> anyhow::Result<BTreeSet<u16>> {
    let mut partitions = BTreeSet::new();
    for component in value.split(',').map(str::trim) {
        let (first, last) = component.split_once('-').unwrap_or((component, component));
        let parse = |value: &str| {
            value
                .trim()
                .parse::<u16>()
                .with_context(|| format!("invalid SWARMY_WORKER_PARTITIONS component: {component}"))
        };
        let (first, last) = (parse(first)?, parse(last)?);
        if first > last || last >= RUNNABLE_PARTITIONS {
            bail!("SWARMY_WORKER_PARTITIONS must be in 0-255 with ascending ranges");
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
