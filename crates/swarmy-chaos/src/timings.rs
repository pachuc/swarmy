//! Per-tool-call timing breakdown from the run's turn metrics.
use anyhow::{Result, bail};
use std::io::Write;
use swarmy_core::{MessageId, SessionId};
use swarmy_store::{MAX_SCAN_LIMIT, Store, TurnMetrics};

pub(crate) struct Gates {
    pub(crate) max_warm_overhead_ms: Option<u64>,
    pub(crate) max_cold_start_ms: Option<u64>,
    pub(crate) require_stamps: bool,
}

pub(crate) struct CallTiming {
    pub(crate) session: String,
    pub(crate) request_id: String,
    pub(crate) tool: String,
    pub(crate) cold: bool,
    pub(crate) delivery_ms: Option<f64>,
    pub(crate) claim_ms: Option<f64>,
    pub(crate) run_ms: Option<f64>,
    pub(crate) complete_ms: Option<f64>,
    pub(crate) wake_ms: Option<f64>,
    pub(crate) total_ms: Option<f64>,
    pub(crate) process_wall_ms: Option<f64>,
    pub(crate) overhead_ms: Option<f64>,
    pub(crate) missing: Vec<&'static str>,
}

pub(crate) struct PlacementTiming {
    pub(crate) session: String,
    pub(crate) turn: String,
    pub(crate) request_id: Option<String>,
    pub(crate) placement_ms: Option<f64>,
    pub(crate) open_ms: Option<f64>,
    pub(crate) boot_ms: Option<f64>,
    pub(crate) cold_start_ms: Option<f64>,
    pub(crate) chunks_fetched: u64,
    pub(crate) bytes_fetched: u64,
}

pub(crate) struct Report {
    pub(crate) calls: Vec<CallTiming>,
    pub(crate) placements: Vec<PlacementTiming>,
}

pub(crate) async fn load(store: &Store, sessions: &[SessionId]) -> Result<Vec<TurnMetrics>> {
    let mut turns = Vec::new();
    for &session in sessions {
        let mut after = None;
        loop {
            let page = store
                .list_turn_metrics_paged(session, after, MAX_SCAN_LIMIT, None, None)
                .await?;
            let count = page.len();
            after = page
                .last()
                .map(|turn| ulid::Ulid::from_string(&turn.turn_id).map(MessageId::from_ulid))
                .transpose()?;
            turns.extend(page);
            if count < MAX_SCAN_LIMIT {
                break;
            }
        }
    }
    Ok(turns)
}

#[expect(
    clippy::cast_precision_loss,
    reason = "wall-clock intervals are approximate millisecond measurements"
)]
fn ms_between(from: Option<i64>, to: Option<i64>) -> Option<f64> {
    Some(to?.checked_sub(from?)?)
        .filter(|value| *value >= 0)
        .map(|value| value as f64 / 1_000_000.0)
}

fn call(turn: &TurnMetrics, tool: &swarmy_store::ToolMetric) -> CallTiming {
    let process = tool.process_wall_ms;
    let total = ms_between(tool.dispatched_ns, tool.woken_ns);
    let pre_run = ms_between(tool.claimed_ns, tool.started_ns);
    let node_run = ms_between(tool.started_ns, tool.completed_ns);
    CallTiming {
        session: turn.session_id.clone(),
        request_id: tool.request_id.clone(),
        tool: tool.name.clone(),
        cold: false,
        delivery_ms: ms_between(tool.dispatched_ns, tool.delivered_ns),
        claim_ms: ms_between(tool.delivered_ns, tool.claimed_ns),
        run_ms: pre_run.zip(process).map(|(a, b)| a + b),
        complete_ms: node_run.zip(process).map(|(a, b)| a - b),
        wake_ms: ms_between(tool.completed_ns, tool.woken_ns),
        total_ms: total,
        process_wall_ms: process,
        overhead_ms: total.zip(process).map(|(a, b)| a - b),
        missing: [
            ("delivered_ns", tool.delivered_ns),
            ("claimed_ns", tool.claimed_ns),
            ("completed_ns", tool.completed_ns),
            ("woken_ns", tool.woken_ns),
        ]
        .into_iter()
        .filter_map(|(name, value)| value.is_none().then_some(name))
        .collect(),
    }
}

pub(crate) fn build(turns: &[TurnMetrics]) -> Report {
    let mut report = Report {
        calls: Vec::new(),
        placements: Vec::new(),
    };
    for turn in turns {
        let start = report.calls.len();
        report.calls.extend(
            turn.tools
                .iter()
                .filter(|tool| tool.delivered_ns.is_some())
                .map(|tool| call(turn, tool)),
        );
        if let Some(computer) = turn
            .computer
            .as_ref()
            .filter(|computer| computer.placement_ms.is_some())
        {
            let first = turn
                .tools
                .iter()
                .filter(|tool| tool.delivered_ns.is_some())
                .min_by_key(|tool| tool.delivered_ns);
            let cold = first.and_then(|tool| {
                report.calls[start..]
                    .iter_mut()
                    .find(|row| row.request_id == tool.request_id)
            });
            let (request_id, cold_start_ms) = match cold {
                Some(row) => {
                    row.cold = true;
                    (Some(row.request_id.clone()), row.overhead_ms)
                }
                None => (None, None),
            };
            report.placements.push(PlacementTiming {
                session: turn.session_id.clone(),
                turn: turn.turn_id.clone(),
                request_id,
                placement_ms: computer.placement_ms,
                open_ms: computer.open_ms,
                boot_ms: computer.boot_ms,
                cold_start_ms,
                chunks_fetched: computer.chunks_fetched,
                bytes_fetched: computer.bytes_fetched,
            });
        }
    }
    report
}

fn percentile(sorted: &[f64], fraction: f64) -> Option<f64> {
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "bounded percentile index is positive and fits the slice length"
    )]
    sorted
        .get(((fraction * sorted.len() as f64).ceil() as usize).saturating_sub(1))
        .copied()
}

impl Report {
    fn warm(&self) -> Vec<f64> {
        let mut values: Vec<_> = self
            .calls
            .iter()
            .filter(|row| !row.cold)
            .filter_map(|row| row.overhead_ms)
            .collect();
        values.sort_by(f64::total_cmp);
        values
    }
    pub(crate) fn json(&self) -> serde_json::Value {
        let calls: Vec<_> = self.calls.iter().map(|c| serde_json::json!({
            "session": c.session, "request_id": c.request_id, "tool": c.tool, "cold": c.cold,
            "delivery_ms": c.delivery_ms, "claim_ms": c.claim_ms, "run_ms": c.run_ms,
            "complete_ms": c.complete_ms, "wake_ms": c.wake_ms, "total_ms": c.total_ms,
            "process_wall_ms": c.process_wall_ms, "overhead_ms": c.overhead_ms
        })).collect();
        let placements: Vec<_> = self.placements.iter().map(|p| serde_json::json!({
            "session": p.session, "turn": p.turn, "request_id": p.request_id,
            "placement_ms": p.placement_ms, "open_ms": p.open_ms, "boot_ms": p.boot_ms,
            "cold_start_ms": p.cold_start_ms, "chunks_fetched": p.chunks_fetched, "bytes_fetched": p.bytes_fetched
        })).collect();
        let warm = self.warm();
        serde_json::json!({"calls": calls, "placements": placements, "warm_overhead_ms": {
            "count": warm.len(), "p50": percentile(&warm, 0.5), "p95": percentile(&warm, 0.95), "max": warm.last()
        }})
    }
    pub(crate) fn render(&self) -> String {
        use std::fmt::Write as _;
        fn number(value: Option<f64>) -> String {
            value.map_or_else(|| "-".into(), |n| format!("{n:.1}"))
        }
        let mut out = String::from(
            "== tool timings (ms; delivery and wake compare worker and node wall clocks)\n",
        );
        for c in &self.calls {
            writeln!(
                out,
                "{} {} {} {} {} {} {} {} {} {} {} {}",
                c.session
                    .chars()
                    .rev()
                    .take(6)
                    .collect::<String>()
                    .chars()
                    .rev()
                    .collect::<String>(),
                c.request_id.chars().take(12).collect::<String>(),
                c.tool,
                if c.cold { "cold" } else { "warm" },
                number(c.delivery_ms),
                number(c.claim_ms),
                number(c.run_ms),
                number(c.complete_ms),
                number(c.wake_ms),
                number(c.total_ms),
                number(c.process_wall_ms),
                number(c.overhead_ms)
            )
            .expect("writing to String cannot fail");
        }
        out.push_str("== cold starts\n");
        for p in &self.placements {
            writeln!(
                out,
                "{} {} {} {} {} {} chunks={} bytes={}",
                p.session,
                p.turn,
                p.request_id.as_deref().unwrap_or("-"),
                number(p.cold_start_ms),
                number(p.placement_ms),
                number(p.open_ms),
                number(p.boot_ms),
                p.chunks_fetched,
                p.bytes_fetched
            )
            .expect("writing to String cannot fail");
        }
        let warm = self.warm();
        writeln!(
            out,
            "warm overhead count={} p50={} p95={} max={}",
            warm.len(),
            number(percentile(&warm, 0.5)),
            number(percentile(&warm, 0.95)),
            number(warm.last().copied())
        )
        .expect("writing to String cannot fail");
        writeln!(out, "TOOL_TIMINGS {}", self.json()).expect("writing to String cannot fail");
        out
    }
    pub(crate) fn check(&self, gates: &Gates) -> Result<()> {
        if gates.require_stamps {
            if self.calls.is_empty() {
                bail!("no node calls to check stamps");
            }
            let missing: Vec<_> = self
                .calls
                .iter()
                .filter(|c| !c.missing.is_empty())
                .map(|c| format!("{}: {}", c.request_id, c.missing.join(", ")))
                .collect();
            if !missing.is_empty() {
                bail!("missing node stamps: {}", missing.join("; "));
            }
        }
        if let Some(max) = gates.max_warm_overhead_ms {
            let warm: Vec<_> = self.calls.iter().filter(|c| !c.cold).collect();
            if warm.iter().all(|c| c.overhead_ms.is_none()) {
                bail!("no warm calls with overhead");
            }
            let slow: Vec<_> = warm
                .iter()
                .filter(|c| c.overhead_ms.is_none_or(|ms| ms > max as f64))
                .map(|c| format!("{}: {:?}", c.request_id, c.overhead_ms))
                .collect();
            if !slow.is_empty() {
                bail!("warm overhead exceeds {max} ms: {}", slow.join("; "));
            }
        }
        if let Some(max) = gates.max_cold_start_ms {
            if self.placements.is_empty() {
                bail!("no placements to check cold starts");
            }
            let slow: Vec<_> = self
                .placements
                .iter()
                .filter(|p| p.cold_start_ms.is_none_or(|ms| ms > max as f64))
                .map(|p| {
                    format!(
                        "{}: {:?}",
                        p.request_id.as_deref().unwrap_or("-"),
                        p.cold_start_ms
                    )
                })
                .collect();
            if !slow.is_empty() {
                bail!("cold start exceeds {max} ms: {}", slow.join("; "));
            }
        }
        Ok(())
    }
}

pub(crate) async fn report(store: &Store, sessions: &[SessionId], gates: &Gates) -> Result<()> {
    let report = build(&load(store, sessions).await?);
    std::io::stdout()
        .lock()
        .write_all(report.render().as_bytes())?;
    report.check(gates)
}
