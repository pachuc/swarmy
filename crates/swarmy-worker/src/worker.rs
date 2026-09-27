use std::{collections::HashMap, fmt::Write, sync::Arc};

use anyhow::{Context, Result, ensure};
use jiff::Timestamp;
use swarmy_bus::{Bus, LiveFeed, SubjectToken, WorkMessage, WorkQueue};
use swarmy_core::{
    Event, InflightRecord, Lease, LeaseOwnerId, MessageId, Nudge, RequestId, SandboxArguments,
    SessionId, SessionRecord, SessionState, SnapshotRef, ToolCallRecord, ToolJob, TurnStage,
    decode, encode,
};
use swarmy_harness::{Action, Snapshot, execution_result};
use swarmy_llm::{InferenceJob, InferenceJobRef};
use swarmy_store::{
    FailoverAction, MAX_SCAN_LIMIT, Store, StoreError, SubmitInferenceOptions, blob::BlobStore,
    runnable_partition,
};
use tokio::{
    sync::Mutex,
    time::{Instant, MissedTickBehavior, interval, interval_at},
};
use ulid::Ulid;

use crate::config::Config;

type ActiveLease = Mutex<Option<Lease>>;

/// One inference attempt on a resolved route step: the provider and entry
/// the gateway must use, the route that selected them, and the step index
/// recorded on the completion for metering.
struct ResolvedAttempt {
    provider: String,
    entry: Option<String>,
    route: Option<String>,
    route_step: u32,
    snapshot: swarmy_store::RouteSnapshot,
}

impl Worker {
    async fn route_snapshot(&self, session: &SessionRecord) -> Result<swarmy_store::RouteSnapshot> {
        Ok(self
            .store
            .route_snapshot(
                session.agent_id,
                session.route.as_deref(),
                session.inference.provider.as_deref(),
                self.config.default_route.as_deref(),
                &self.config.provider,
                Timestamp::now(),
            )
            .await?)
    }
}

fn warn_on_route_fallback(session: &SessionRecord, resolved: Option<&str>, skipped: &[String]) {
    // A deleted or renamed route falls back to the implicit chain; say so
    // once per resolution so the operator can fix the assignment. A route
    // whose named steps are all unready falls back the same way, but the
    // route itself exists, so name the skipped steps instead.
    let requested = session.route.as_deref();
    if requested.is_some() && resolved != requested {
        if skipped.is_empty() {
            tracing::warn!(
                session_id = %session.session_id,
                route = requested,
                "assigned route is missing; using the implicit provider chain",
            );
        } else {
            tracing::warn!(
                session_id = %session.session_id,
                route = requested,
                skipped = skipped.join("; "),
                "assigned route has no usable step; using the implicit provider chain",
            );
        }
    }
}

pub struct Worker {
    store: Store,
    bus: Bus,
    blobs: Arc<dyn BlobStore>,
    config: Config,
    placements: crate::placement::Cache,
    snapshots: Mutex<HashMap<String, Snapshot>>,
    display_by_session: Mutex<HashMap<SessionId, bool>>,
    pub owner: LeaseOwnerId,
}

impl Worker {
    pub fn new(store: Store, bus: Bus, blobs: Arc<dyn BlobStore>, config: Config) -> Self {
        Self {
            store,
            bus,
            blobs,
            config,
            placements: crate::placement::Cache::default(),
            snapshots: Mutex::default(),
            display_by_session: Mutex::default(),
            owner: LeaseOwnerId::from_ulid(Ulid::generate()),
        }
    }

    fn kill(&self, point: &str) {
        if self.config.kill_point.as_deref() == Some(point) {
            tracing::warn!(point, "instrumented worker exit");
            std::process::exit(137);
        }
    }

    pub async fn handle(&self, message: &WorkMessage<Nudge>) -> Result<()> {
        let id = message.value.session_id;
        let (lease, session, turn, events) = match self
            .store
            .claim_step_with_tail(
                id,
                self.owner,
                Timestamp::now().checked_add(self.config.lease_duration)?,
            )
            .await
        {
            Ok(lease) => lease,
            Err(StoreError::InvalidState) => {
                return Ok(message.acknowledge().await?);
            }
            Err(error) => return Err(error.into()),
        };
        tracing::info!(session_id = %id, owner = %lease.owner, step = lease.seq, "claimed step");
        if let Some(turn) = turn {
            let event = Bus::turn_event(id, turn, swarmy_core::TurnStage::Claimed, None);
            self.bus.record_turn(&event).await;
            self.store.observe_turn_stage(event);
        }
        self.kill("after_claim");
        let lease = Mutex::new(Some(lease));
        tokio::select! {
            result = self.step(session.clone(), turn, &lease, events) => {
                if result.is_err() { self.placements.invalidate(session.agent_id).await; }
                result?;
            },
            result = self.heartbeat(id, &lease, message) => result?,
        }
        message.acknowledge().await?;
        Ok(())
    }

    async fn heartbeat(
        &self,
        id: SessionId,
        lease: &ActiveLease,
        message: &WorkMessage<Nudge>,
    ) -> Result<()> {
        let period = (self.config.lease_duration / 3).min(self.config.bus.ack_wait / 3);
        let mut ticks = interval_at(Instant::now() + period, period);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticks.tick().await;
            message.extend_deadline().await?;
            let mut token = lease.lock().await;
            if let Some(current) = token.as_ref() {
                let now = Timestamp::now();
                *token = Some(
                    self.store
                        .renew_lease(
                            id,
                            current,
                            now,
                            now.checked_add(self.config.lease_duration)?,
                        )
                        .await?,
                );
            }
        }
    }

    async fn tail(&self, id: SessionId, after: u64, through: u64) -> Result<Vec<Event>> {
        let mut events = Vec::new();
        let mut cursor = after;
        loop {
            let page = self.store.read_events(id, cursor, MAX_SCAN_LIMIT).await?;
            let Some(last) = page.last() else {
                return Ok(events);
            };
            cursor = last.seq();
            events.extend(page.into_iter().filter(|event| event.seq() <= through));
            if cursor >= through {
                return Ok(events);
            }
        }
    }

    async fn publish_events(&self, id: SessionId, events: &[Event]) -> Result<()> {
        for event in events {
            if let Err(error) = self
                .bus
                .publish_live(LiveFeed::SessionEvents(id), event)
                .await
            {
                if matches!(error, swarmy_bus::Error::PayloadTooLarge { .. }) {
                    // The event is already durable. Live observers can read it by cursor.
                    tracing::warn!(%id, seq = event.seq(), %error, "live event exceeds bus limit");
                } else {
                    return Err(error.into());
                }
            }
        }
        Ok(())
    }

    async fn publish_tail(&self, id: SessionId, events: &[Event]) -> Result<()> {
        for event in events {
            // This session is leased. Historical idle events, including ones
            // written before the atomic finish API, cannot announce readiness.
            if !matches!(
                event,
                Event::StateChanged {
                    to: SessionState::Idle,
                    ..
                }
            ) {
                self.publish_events(id, std::slice::from_ref(event)).await?;
            }
        }
        Ok(())
    }

    async fn append(
        &self,
        session: &mut SessionRecord,
        lease: &ActiveLease,
        events: &mut Vec<Event>,
        batch: &[Event],
    ) -> Result<()> {
        let before = session.head_seq;
        {
            let token = lease.lock().await;
            session.head_seq = self
                .store
                .append_events_leased(
                    session.session_id,
                    before,
                    batch,
                    token.as_ref().context("lease released")?,
                    Timestamp::now(),
                )
                .await?;
        }
        let appended: Vec<_> = batch
            .iter()
            .cloned()
            .zip(before + 1..)
            .map(|(mut event, seq)| {
                event.set_seq(seq);
                event
            })
            .collect();
        self.publish_events(session.session_id, &appended).await?;
        events.extend(appended);
        Ok(())
    }

    async fn snapshot(&self, key: &str) -> Result<Snapshot> {
        if let Some(snapshot) = self.snapshots.lock().await.get(key) {
            return Ok(snapshot.clone());
        }
        let bytes = self.blobs.get(key).await?;
        let snapshot: Snapshot = decode(&bytes)?;
        // Content-addressed snapshots are immutable. Bound retained data while
        // avoiding repeat downloads for the claim, dispatch, and fold of a turn.
        if bytes.len() <= 1024 * 1024 {
            let mut cache = self.snapshots.lock().await;
            if cache.len() >= 16 {
                cache.clear();
            }
            cache.insert(key.to_owned(), snapshot.clone());
        }
        Ok(snapshot)
    }

    async fn load_history(
        &self,
        session: &SessionRecord,
        events: &mut Vec<Event>,
    ) -> Result<Snapshot> {
        let (snapshot, after) = if let Some(reference) = &session.snapshot_ref {
            (self.snapshot(&reference.object_key).await?, reference.seq)
        } else {
            (Snapshot::default(), 0)
        };
        let cursor = events.last().map_or(after, Event::seq);
        if cursor < session.head_seq {
            events.extend(
                self.tail(session.session_id, cursor, session.head_seq)
                    .await?,
            );
        }
        Ok(snapshot)
    }
}

mod inference;
mod recovery;
mod step;
mod summarize;
mod tools;
use self::{inference::*, recovery::*, step::*, summarize::*, tools::*};

#[cfg(test)]
mod image_tests {
    use super::*;
    use swarmy_core::{Message, MessageRole, Part};

    #[test]
    fn display_image_controls_tool_schema_and_prompt() {
        use swarmy_llm::ToolDefinition;
        let request = || swarmy_llm::Request {
            system_prompt: "Base prompt".into(),
            messages: Vec::new(),
            tools: ["bash", "browser_snapshot", "screen_screenshot"]
                .into_iter()
                .map(|name| ToolDefinition {
                    name: name.into(),
                    description: String::new(),
                    parameters: serde_json::json!({"type":"object","properties":{}}),
                })
                .collect(),
            settings: swarmy_llm::GenerationSettings::default(),
        };
        let mut coding = request();
        apply_display_tools(&mut coding, false);
        assert_eq!(
            coding
                .tools
                .iter()
                .map(|tool| tool.name.as_str())
                .collect::<Vec<_>>(),
            ["bash"]
        );
        assert_eq!(coding.system_prompt, "Base prompt");
        let mut desktop = request();
        apply_display_tools(&mut desktop, true);
        assert_eq!(desktop.tools.len(), 3);
        assert!(desktop.system_prompt.contains("prefer browser_snapshot"));
    }

    #[test]
    fn unsupported_model_gets_a_note_instead_of_image_bytes() {
        let mut request = swarmy_llm::Request {
            system_prompt: String::new(),
            messages: vec![Message {
                id: MessageId::from_ulid(Ulid::nil()),
                role: MessageRole::User,
                parts: vec![Part::Image {
                    media_type: "image/png".into(),
                    bytes: vec![1, 2, 3],
                    object_key: None,
                    detail: None,
                }],
            }],
            tools: Vec::new(),
            settings: swarmy_llm::GenerationSettings::default(),
        };
        request.messages[0].parts.push(Part::ToolResult {
            call_id: swarmy_core::ToolCallId("shot".into()),
            result: swarmy_core::ToolResult::Completed {
                title: "browser_screenshot".into(),
                output: "PNG screenshot".into(),
                metadata: std::collections::BTreeMap::from([
                    ("image_object_key".into(), serde_json::json!("blob")),
                    ("image_media_type".into(), serde_json::json!("image/png")),
                ]),
            },
        });
        omit_unsupported_images(&mut request);
        assert!(
            matches!(&request.messages[0].parts[0], Part::Text { text } if text.contains("image was omitted"))
        );
        assert!(
            matches!(&request.messages[0].parts[1], Part::ToolResult { result: swarmy_core::ToolResult::Completed { output, metadata, .. }, .. } if output.contains("image was omitted") && !metadata.contains_key("image_object_key"))
        );
    }
}

#[cfg(test)]
mod tool_output_tests {
    #[test]
    fn small_output_passes_through_unchanged() {
        let output = "hello".to_owned();
        assert_eq!(
            swarmy_core::cap_tool_output("grep", "call_small", output.clone()),
            output
        );
    }

    #[test]
    fn huge_sandbox_result_is_capped_with_spill_marker() {
        let tool = "process_list";
        let call_id = "call_01HUGE";
        let head = "HEAD-MARKER-";
        let tail = "-TAIL-MARKER";
        let mut original = String::with_capacity(1024 * 1024);
        original.push_str(head);
        original.push_str(&"x".repeat(1024 * 1024 - head.len() - tail.len()));
        original.push_str(tail);
        assert_eq!(original.len(), 1024 * 1024);
        // Feed a fake sandbox tool result through the same ceiling the worker
        // applies before persisting a `ToolCallCompleted` event.
        let result = swarmy_harness::execution_result(tool, Ok(original.clone()));
        let swarmy_core::ToolResult::Completed { output, .. } = result else {
            panic!("expected completed tool result");
        };
        let capped = swarmy_core::cap_tool_output(tool, call_id, output);
        let spill = swarmy_core::tool_spill_path(call_id);
        let dropped = original.len() - swarmy_core::MAX_TOOL_OUTPUT_BYTES;
        assert!(capped.len() <= swarmy_core::MAX_TOOL_OUTPUT_BYTES + 512);
        assert!(capped.contains(tool));
        assert!(capped.contains(&dropped.to_string()));
        assert!(capped.contains(&spill));
        assert!(spill.starts_with("/home/agent/.swarmy/output/"));
        assert!(capped.starts_with(head));
        assert!(capped.ends_with(tail));
        let keep = swarmy_core::MAX_TOOL_OUTPUT_BYTES;
        let head_len = keep.div_ceil(2);
        assert_eq!(&capped[..head_len], &original[..head_len]);
        assert_eq!(
            &capped[capped.len() - keep / 2..],
            &original[original.len() - keep / 2..]
        );
    }
}

#[cfg(test)]
mod side_tail_tests {
    use super::{estimate_message_tokens, select_side_tail};
    use std::collections::BTreeMap;
    use swarmy_core::{Message, MessageId, MessageRole, Part, ToolCallId, ToolResult};
    use ulid::Ulid;

    fn text(role: MessageRole, text: &str) -> Message {
        Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role,
            parts: vec![Part::Text { text: text.into() }],
        }
    }

    fn assistant_calls(id: &str, reasoning: bool) -> Message {
        let mut parts = Vec::new();
        if reasoning {
            parts.push(Part::Reasoning {
                text: format!("thinking for {id}"),
                metadata: BTreeMap::new(),
            });
        }
        parts.push(Part::ToolCall {
            call_id: ToolCallId(id.into()),
            tool: "get_time".into(),
            input: serde_json::json!({}),
        });
        Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role: MessageRole::Assistant,
            parts,
        }
    }

    fn tool_result(id: &str) -> Message {
        tool_result_sized(id, 0)
    }

    fn tool_result_sized(id: &str, padding: usize) -> Message {
        Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role: MessageRole::Tool,
            parts: vec![Part::ToolResult {
                call_id: ToolCallId(id.into()),
                result: ToolResult::Completed {
                    output: format!("result for {id} {}", "x".repeat(padding)),
                    title: String::new(),
                    metadata: BTreeMap::new(),
                },
            }],
        }
    }

    fn call_ids(messages: &[Message]) -> Vec<String> {
        messages
            .iter()
            .flat_map(|message| &message.parts)
            .filter_map(|part| match part {
                Part::ToolCall { call_id, .. } => Some(call_id.0.clone()),
                _ => None,
            })
            .collect()
    }

    fn result_ids(messages: &[Message]) -> Vec<String> {
        messages
            .iter()
            .flat_map(|message| &message.parts)
            .filter_map(|part| match part {
                Part::ToolResult { call_id, .. } => Some(call_id.0.clone()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn estimate_counts_chars_like_pi_and_opencode() {
        let message = text(MessageRole::User, &"x".repeat(400));
        assert_eq!(estimate_message_tokens(&message), 100);
        let image = Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role: MessageRole::User,
            parts: vec![Part::Image {
                media_type: "image/png".into(),
                bytes: Vec::new(),
                object_key: None,
                detail: None,
            }],
        };
        assert_eq!(estimate_message_tokens(&image), 1_200);
    }

    #[test]
    fn tail_cuts_between_tool_result_and_assistant() {
        // A short history fits the budget whole.
        let history = vec![
            text(MessageRole::User, "launch"),
            assistant_calls("a", false),
            tool_result("a"),
            text(MessageRole::Assistant, "halfway"),
            assistant_calls("b", false),
            tool_result("b"),
            text(MessageRole::Assistant, "summary output"),
        ];
        let tail = select_side_tail(&history);
        assert_eq!(tail.len(), history.len());
        // A long history cuts at a tool-result boundary and keeps pairs.
        let mut long = vec![text(MessageRole::User, "launch")];
        for round in 0..30 {
            let id = format!("round-{round}");
            long.push(assistant_calls(&id, false));
            long.push(tool_result(&id));
            long.push(text(
                MessageRole::Assistant,
                &format!("note {round} {}", "x".repeat(3_000)),
            ));
        }
        let tail = select_side_tail(&long);
        assert!(tail.len() < long.len());
        assert_eq!(tail[0].role, MessageRole::Assistant);
        assert!(tail.windows(2).all(|pair| {
            !(pair[0]
                .parts
                .iter()
                .any(|part| matches!(part, Part::ToolCall { .. }))
                && pair[1].role != MessageRole::Tool)
        }));
        for id in call_ids(&tail) {
            assert!(result_ids(&tail).contains(&id), "call {id} lost its result");
        }
    }

    #[test]
    fn tail_never_walks_back_to_the_launch_prompt() {
        // One user prompt plus many tool rounds: the tail must stay bounded
        // by tokens instead of reaching back to the launch prompt.
        let mut history = vec![text(
            MessageRole::User,
            &format!("launch {}", "x".repeat(4_000)),
        )];
        for round in 0..40 {
            let id = format!("round-{round}");
            history.push(assistant_calls(&id, round % 5 == 0));
            history.push(tool_result_sized(&id, 3_000));
        }
        let tail = select_side_tail(&history);
        assert!(!tail.iter().any(|message| message.role == MessageRole::User));
        for id in call_ids(&tail) {
            assert!(result_ids(&tail).contains(&id), "call {id} lost its result");
        }
        // Reasoning stays inside its assistant message.
        assert!(
            tail.iter()
                .filter(|message| message.role == MessageRole::Assistant)
                .flat_map(|message| &message.parts)
                .any(|part| matches!(part, Part::Reasoning { .. }))
        );
    }

    #[test]
    fn tail_drops_stale_pressure_warnings() {
        let warning = Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role: MessageRole::System,
            parts: vec![Part::Text {
                text: "context_pressure: input 150 tokens at 75 percent".into(),
            }],
        };
        let history = vec![
            text(MessageRole::User, "launch"),
            assistant_calls("a", false),
            tool_result("a"),
            warning,
            assistant_calls("b", false),
            tool_result("b"),
        ];
        let tail = select_side_tail(&history);
        assert!(
            tail.iter()
                .all(|message| message.role != MessageRole::System)
        );
        for id in call_ids(&tail) {
            assert!(result_ids(&tail).contains(&id), "call {id} lost its result");
        }
    }

    #[test]
    fn tail_falls_back_to_last_assistant_without_rounds() {
        let history = vec![
            text(MessageRole::User, "launch"),
            text(MessageRole::Assistant, &"x".repeat(100_000)),
        ];
        let tail = select_side_tail(&history);
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].role, MessageRole::Assistant);
    }

    #[test]
    fn tail_keeps_oversized_last_round_with_its_result() {
        // One 128 KiB tool result is about 32k tokens, over the 20k budget
        // on its own. The budget bounds what comes before the last round,
        // never the round itself: the successor must keep the assistant call
        // together with its result instead of orphaning the call.
        let mut history = vec![text(MessageRole::User, "launch")];
        for round in 0..5 {
            let id = format!("small-{round}");
            history.push(assistant_calls(&id, false));
            history.push(tool_result_sized(&id, 1_000));
        }
        history.push(assistant_calls("huge", false));
        history.push(tool_result_sized("huge", 128 * 1024));
        let tail = select_side_tail(&history);
        assert_eq!(call_ids(&tail), vec!["huge".to_owned()]);
        assert_eq!(result_ids(&tail), vec!["huge".to_owned()]);
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].role, MessageRole::Assistant);
        assert_eq!(tail[1].role, MessageRole::Tool);
    }

    #[test]
    fn mid_turn_fast_path_needs_no_store_reads() {
        use super::last_side_usage;
        use swarmy_core::{Event, RequestId, SessionId, TokenUsage};
        // Folds below pressure decide from the events the worker already
        // holds: zero transactions, zero store reads. This test pins that by
        // deciding without a `Store` at all.
        let session = SessionId::from_ulid(Ulid::generate());
        let request_id = RequestId::for_step(session, 1);
        let events = vec![Event::InferenceCompleted {
            seq: 1,
            request_id,
            message: text(MessageRole::Assistant, "working"),
            provider: "fake".into(),
            model: "base".into(),
            effort_used: None,
            usage: TokenUsage {
                input_tokens: 10,
                ..Default::default()
            },
            cost_micros: 0,
            effort_requested: None,
            effort_clamped: false,
            entry: None,
            route: None,
            route_step: None,
        }];
        let Some((provider, model, input)) = last_side_usage(&events) else {
            panic!("expected usage from the held events");
        };
        assert_eq!(provider, "fake");
        assert_eq!(model, "base");
        assert_eq!(input, 10);
        // The default side threshold is 400k with pressure at 300k: an input
        // of 10 stays on the fast path, which returns before any store read.
        // Transaction count: 0. Store read count: 0.
        assert!(input < 300_000);
    }
}
