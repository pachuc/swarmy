//! Benchmark the same API append and SSE idle path as a human client.
use crate::{bench_command::Command, client_conversation::Conversation};
use anyhow::{Context, Result, ensure};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};
use swarmy_bus::{Bus, Config, LiveFeed, SubjectToken};
use swarmy_client::Client;
use swarmy_core::{MessageId, SessionId, ToolResult, TurnEvent, TurnStage};

const SCRIPT: &str = include_str!("../../../scripts/benchmarks/turn-fake.json");

#[derive(Serialize)]
struct Sample {
    shape: String,
    warmup: bool,
    turn_id: MessageId,
    events: Vec<TurnEvent>,
    elapsed_ms: BTreeMap<String, f64>,
    cross_host_wall_clock: bool,
}

async fn timeline_bus(settings: &swarmy_config::Settings) -> Result<Bus> {
    let config = Config {
        prefix: if settings.bus_prefix.is_empty() {
            None
        } else {
            Some(SubjectToken::new(settings.bus_prefix.clone())?)
        },
        ack_wait: Duration::from_millis(settings.bus_ack_wait_ms),
        max_deliver: settings.bus_max_deliver,
    };
    Ok(tokio::time::timeout(
        Duration::from_secs(3),
        Bus::connect(&settings.nats_url, config),
    )
    .await
    .context("cannot reach turn timeline bus")??)
}

pub async fn run(client: Client, command: Command, json: bool) -> Result<()> {
    let Command::Turn {
        turns,
        image,
        output,
        timeout_secs,
    } = command;
    let settings = swarmy_config::Settings::load()?.settings;
    ensure!(
        settings.provider == "fake",
        "bench turn requires provider=fake"
    );
    let configured: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&settings.fake.script).context(
            "read fake script; configure scripts/benchmarks/turn-fake.json and restart dev up",
        )?)?;
    ensure!(
        configured == serde_json::from_str::<serde_json::Value>(SCRIPT)?,
        "bench turn requires scripts/benchmarks/turn-fake.json; configure it and restart the gateway"
    );
    let bus = timeline_bus(&settings).await?;
    let mut samples = Vec::new();
    for shape in ["no_tool", "bash"] {
        let mut conversation = Conversation::open(
            client.clone(),
            None,
            Some(image.clone()),
            None,
            false,
            swarmy_core::InferenceSelection::default(),
        )
        .await?;
        let id = SessionId::from_ulid(conversation.id.parse()?);
        let mut timeline = bus
            .subscribe_live::<TurnEvent>(LiveFeed::TurnTimeline(id))
            .await?;
        for index in 0..=turns {
            let sample = tokio::time::timeout(
                Duration::from_secs(timeout_secs),
                measure(&mut conversation, &bus, &mut timeline, shape, index == 0),
            )
            .await
            .with_context(|| {
                format!("{shape} turn {index} timed out: incomplete timeline or stalled stack")
            })??;
            samples.push(sample);
        }
    }
    if let Some(path) = output {
        std::fs::write(path, serde_json::to_vec_pretty(&samples)?)?;
    }
    if !json && samples.iter().any(|sample| sample.cross_host_wall_clock) {
        println!(
            "Cross-host stages use synchronized UTC clocks; end-to-end uses client monotonic time."
        );
    }
    for shape in ["no_tool", "bash"] {
        let mut metrics: BTreeMap<&str, Vec<f64>> = BTreeMap::new();
        for sample in samples
            .iter()
            .filter(|sample| sample.shape == shape && !sample.warmup)
        {
            for (stage, value) in &sample.elapsed_ms {
                metrics.entry(stage).or_default().push(*value);
            }
        }
        if !json {
            println!("\n{shape}: {turns} turns, one excluded warmup; elapsed from submitted (ms)");
        }
        for (stage, mut values) in metrics {
            values.sort_by(f64::total_cmp);
            let p50 = percentile(&values, 50);
            let p95 = percentile(&values, 95);
            if json {
                println!(
                    "{}",
                    serde_json::json!({"shape":shape,"stage":stage,"turns":turns,"p50_ms":p50,"p95_ms":p95})
                );
            } else {
                println!("{stage:24} p50 {p50:10.3}  p95 {p95:10.3}");
            }
        }
    }
    Ok(())
}

async fn measure(
    conversation: &mut Conversation,
    bus: &Bus,
    timeline: &mut swarmy_bus::LiveMessages<TurnEvent>,
    shape: &str,
    warmup: bool,
) -> Result<Sample> {
    let (sender, mut stages) = tokio::sync::mpsc::unbounded_channel();
    conversation.observe_with(sender);
    let start = Instant::now();
    let turn = conversation
        .send(format!("swarmy bench turn {shape}"))
        .await?;
    let turn_id = MessageId::from_ulid(turn.parse()?);
    let id = SessionId::from_ulid(conversation.id.parse()?);
    let mut events = Vec::new();
    let mut idle = false;
    let mut client_elapsed = Duration::ZERO;
    {
        let done = conversation.until_idle(false, true);
        tokio::pin!(done);
        loop {
            tokio::select! {
                observation = timeline.next() => {
                    let observation = observation.context("timeline feed closed")??;
                    if observation.turn_id == turn_id { events.push(observation); }
                }
                stage = stages.recv() => {
                    if let Some(stage) = stage { bus.record_turn(&Bus::turn_event(id, turn_id, stage, None)).await; }
                }
                outcome = &mut done, if !idle => { outcome?; client_elapsed = start.elapsed(); idle = true; }
            }
            if idle && complete(&events, shape == "bash") {
                break;
            }
        }
    }
    ensure!(
        conversation.last_text == "TURN_OK\n",
        "unexpected fake response"
    );
    ensure!(
        conversation.tool_count == usize::from(shape == "bash"),
        "wrong tool count"
    );
    if shape == "bash" {
        let result: ToolResult = serde_json::from_value(
            conversation
                .tool_result
                .clone()
                .context("bash result missing")?,
        )?;
        let ToolResult::Completed { metadata, .. } = result else {
            anyhow::bail!("bash tool failed: {result:?}");
        };
        let bash: swarmy_core::BashResult =
            serde_json::from_value(serde_json::to_value(metadata)?)?;
        ensure!(
            bash.exit_code == 0 && !bash.timed_out && bash.stdout == "TURN_TOOL_OK",
            "unexpected bash result"
        );
    }
    let mut elapsed_ms = elapsed(&events)?;
    elapsed_ms.insert("end_to_end".into(), client_elapsed.as_secs_f64() * 1000.0);
    Ok(Sample {
        shape: shape.into(),
        warmup,
        turn_id,
        cross_host_wall_clock: events
            .iter()
            .any(|event| event.clock_id != events[0].clock_id),
        events,
        elapsed_ms,
    })
}

fn complete(events: &[TurnEvent], tool: bool) -> bool {
    use TurnStage::{
        Appended, Claimed, FinalTextRendered, Idle, InferenceFinished, InferenceStarted,
        InputEnabled, Nudged, ToolCompleted, ToolDispatched,
    };
    [
        TurnStage::Submitted,
        Appended,
        Nudged,
        Claimed,
        InferenceStarted,
        InferenceFinished,
        Idle,
        FinalTextRendered,
        InputEnabled,
    ]
    .iter()
    .all(|stage| events.iter().any(|event| event.stage == *stage))
        && (!tool
            || [ToolDispatched, ToolCompleted]
                .iter()
                .all(|stage| events.iter().any(|event| event.stage == *stage)))
        && events
            .iter()
            .filter(|event| event.stage == InferenceFinished)
            .count()
            >= if tool { 2 } else { 1 }
}

fn elapsed(events: &[TurnEvent]) -> Result<BTreeMap<String, f64>> {
    let appended = events
        .iter()
        .find(|event| event.stage == TurnStage::Submitted)
        .context("submitted missing")?;
    let mut metrics = BTreeMap::new();
    for event in events {
        // Repeated work is retained in raw samples; milestones show the last
        // occurrence so the final inference and scheduler passes remain visible.
        let duration = difference(appended, event)?;
        let stage = serde_json::to_value(event.stage)?
            .as_str()
            .context("stage name missing")?
            .to_owned();
        let value = duration.as_secs_f64() * 1000.0;
        metrics
            .entry(stage)
            .and_modify(|old: &mut f64| *old = old.max(value))
            .or_insert(value);
    }
    metrics.insert(
        "end_to_end".into(),
        *metrics
            .get("input_enabled")
            .context("input enabled missing")?,
    );
    for (name, start, end) in [
        (
            "inference_total_duration",
            TurnStage::InferenceStarted,
            TurnStage::InferenceFinished,
        ),
        (
            "tool_total_duration",
            TurnStage::ToolDispatched,
            TurnStage::ToolCompleted,
        ),
    ] {
        let mut total = Duration::ZERO;
        for finished in events.iter().filter(|event| event.stage == end) {
            let started = events
                .iter()
                .find(|event| event.stage == start && event.request_id == finished.request_id)
                .context("completion has no matching start")?;
            total += difference(started, finished)?;
        }
        if events.iter().any(|event| event.stage == end) {
            metrics.insert(name.into(), total.as_secs_f64() * 1000.0);
        }
    }
    Ok(metrics)
}

fn difference(start: &TurnEvent, end: &TurnEvent) -> Result<Duration> {
    let ns = if start.clock_id == end.clock_id {
        i128::from(end.monotonic_ns) - i128::from(start.monotonic_ns)
    } else {
        end.unix_ns - start.unix_ns
    };
    Ok(Duration::from_nanos(
        u64::try_from(ns).context("negative duration; synchronize host clocks")?,
    ))
}

fn percentile(sorted: &[f64], percentile: usize) -> f64 {
    sorted[(sorted.len() * percentile).div_ceil(100).saturating_sub(1)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use swarmy_core::SessionId;
    #[test]
    fn every_required_stage_must_be_present() {
        use TurnStage::*;
        let id = SessionId::from_ulid(ulid::Ulid::generate());
        let turn = MessageId::from_ulid(ulid::Ulid::generate());
        let events: Vec<_> = [
            Submitted,
            Appended,
            Nudged,
            Claimed,
            InferenceStarted,
            InferenceFinished,
            ToolDispatched,
            ToolCompleted,
            InferenceStarted,
            InferenceFinished,
            Idle,
            FinalTextRendered,
            InputEnabled,
        ]
        .into_iter()
        .map(|stage| swarmy_bus::Bus::turn_event(id, turn, stage, None))
        .collect();
        assert!(complete(&events, true));
        for stage in [
            Submitted,
            Appended,
            Nudged,
            Claimed,
            InferenceStarted,
            InferenceFinished,
            ToolDispatched,
            ToolCompleted,
            Idle,
            FinalTextRendered,
            InputEnabled,
        ] {
            let incomplete: Vec<_> = events
                .iter()
                .filter(|event| event.stage != stage)
                .cloned()
                .collect();
            assert!(!complete(&incomplete, true), "missing {stage:?}");
        }
        let metrics = elapsed(&events).unwrap();
        assert_eq!(metrics["submitted"].to_bits(), 0.0_f64.to_bits());
        assert_eq!(
            metrics["input_enabled"].to_bits(),
            metrics["end_to_end"].to_bits()
        );
    }
    #[test]
    fn nearest_rank_percentiles_include_small_sample_tail() {
        assert_eq!(
            percentile(&[1.0, 2.0, 3.0, 4.0], 50).to_bits(),
            2.0_f64.to_bits()
        );
        assert_eq!(
            percentile(&[1.0, 2.0, 3.0, 4.0], 95).to_bits(),
            4.0_f64.to_bits()
        );
    }
}

#[cfg(test)]
mod clock_tests {
    use super::*;
    #[test]
    fn cross_host_intervals_use_wall_time_and_reject_clock_reversal() {
        let id = swarmy_core::SessionId::from_ulid(ulid::Ulid::generate());
        let turn = MessageId::from_ulid(ulid::Ulid::generate());
        let mut start = swarmy_bus::Bus::turn_event(id, turn, TurnStage::Submitted, None);
        start.monotonic_ns = 100;
        start.unix_ns = 1_000;
        let mut end = start.clone();
        end.monotonic_ns = 200;
        end.unix_ns = 900;
        assert_eq!(difference(&start, &end).unwrap(), Duration::from_nanos(100));
        end.clock_id = "another host".into();
        assert!(difference(&start, &end).is_err());
        end.unix_ns = 1_050;
        assert_eq!(difference(&start, &end).unwrap(), Duration::from_nanos(50));
    }
}
