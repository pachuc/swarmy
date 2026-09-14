use anyhow::{Context, Result, bail, ensure};
use std::{collections::BTreeSet, env, time::Duration};
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
    pub recovery_interval: Duration,
    pub harness: Harness,
    pub kill_point: Option<String>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let provider = setting("SWARMY_PROVIDER", "fake")?;
        ensure!(
            matches!(provider.as_str(), "fake" | "chatgpt"),
            "unsupported SWARMY_PROVIDER"
        );
        let directory: Vec<_> = setting("SWARMY_STORE_DIRECTORY", "swarmy")?
            .split('/')
            .map(str::to_owned)
            .collect();
        ensure!(
            directory.iter().all(|part| !part.is_empty()),
            "empty store directory component"
        );
        let prefix = setting("SWARMY_BUS_PREFIX", "")?;
        let effort = match setting("SWARMY_REASONING_EFFORT", "medium")?.as_str() {
            "none" => ReasoningEffort::None,
            "minimal" => ReasoningEffort::Minimal,
            "low" => ReasoningEffort::Low,
            "medium" => ReasoningEffort::Medium,
            "high" => ReasoningEffort::High,
            "xhigh" => ReasoningEffort::Xhigh,
            _ => bail!("invalid SWARMY_REASONING_EFFORT"),
        };
        let kill_point = env::var("SWARMY_WORKER_KILL_POINT").ok();
        ensure!(
            kill_point.as_deref().is_none_or(|value| matches!(
                value,
                "after_claim" | "after_request_event" | "before_release" | "after_release"
            )),
            "invalid SWARMY_WORKER_KILL_POINT"
        );
        let mut tools = ToolRegistry::default();
        tools.register(Box::new(GetTime));
        Ok(Self {
            cluster: env::var("SWARMY_FDB_CLUSTER_FILE")?,
            directory,
            nats: env::var("SWARMY_NATS_URL")?,
            bus: BusConfig {
                prefix: if prefix.is_empty() {
                    None
                } else {
                    Some(SubjectToken::new(prefix)?)
                },
                ack_wait: duration("SWARMY_BUS_ACK_WAIT_MS", "30000")?,
                max_deliver: setting("SWARMY_BUS_MAX_DELIVER", "5")?.parse()?,
            },
            partitions: parse_partitions(&setting("SWARMY_WORKER_PARTITIONS", "0-255")?)?,
            provider,
            lease_duration: duration("SWARMY_WORKER_LEASE_MS", "30000")?,
            recovery_interval: duration("SWARMY_WORKER_RECOVERY_INTERVAL_MS", "5000")?,
            harness: Harness {
                system_prompt_template: setting(
                    "SWARMY_SYSTEM_PROMPT",
                    "You are a helpful assistant. Use tools when needed.",
                )?,
                settings: GenerationSettings {
                    model: setting("SWARMY_MODEL", "gpt-5")?,
                    reasoning_effort: Some(effort),
                    ..Default::default()
                },
                tools,
            },
            kill_point,
        })
    }
}

fn setting(name: &str, default: &str) -> Result<String> {
    match env::var(name) {
        Ok(value) => Ok(value),
        Err(env::VarError::NotPresent) => Ok(default.into()),
        Err(error) => Err(error).with_context(|| format!("invalid {name}")),
    }
}

fn duration(name: &str, default: &str) -> Result<Duration> {
    let millis: u64 = setting(name, default)?.parse()?;
    ensure!(millis >= 30, "{name} must be at least 30 ms");
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
