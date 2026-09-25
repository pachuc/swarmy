use std::collections::BTreeMap;

use futures::{StreamExt, future::BoxFuture};
use serde_json::{Value, json};
use swarmy_core::{
    AgentId, Event, Message, MessageId, MessageRole, Part, RequestId, SessionId, SessionRecord,
    SessionState, ToolCallId, ToolCallRecord, ToolResult,
};
use swarmy_harness::{
    Action, GetTime, Harness, Snapshot, Tool, ToolRegistry, assemble_prompt, execution_result,
    result_part,
};
use swarmy_llm::{
    Delta, GenerationSettings, Provider, Response, StopReason, TokenUsage, fake::FakeProvider,
};
use ulid::Ulid;

fn message_id(value: u128) -> MessageId {
    MessageId::from_ulid(Ulid::from(value))
}

fn session() -> SessionRecord {
    SessionRecord {
        interrupt_requested: false,
        session_id: SessionId::from_ulid(Ulid::from(1_u128)),
        agent_id: AgentId::from_ulid(Ulid::from(2_u128)),
        state: SessionState::Leased,
        head_seq: 7,
        snapshot_ref: None,
        inference: swarmy_core::InferenceSelection::default(),
        kind: swarmy_core::SessionKind::Ephemeral,
        computer_deleted: false,
        plan: Vec::new(),
    }
}

fn harness() -> Harness {
    let mut tools = ToolRegistry::default();
    tools.register(Box::new(GetTime));
    Harness {
        system_prompt_template: "You are a helpful agent.\nUse tools when needed.\n".into(),
        settings: GenerationSettings {
            model: "fixture-model".into(),
            max_output_tokens: Some(256),
            ..GenerationSettings::default()
        },
        tools,
    }
}

fn call(name: &str) -> ToolCallRecord {
    ToolCallRecord {
        call_id: ToolCallId(name.into()),
        tool: "get_time".into(),
        arguments: json!({}),
        result: None,
    }
}

fn user_event() -> Event {
    Event::MessageAppended {
        seq: 1,
        message: Message {
            id: message_id(3),
            role: MessageRole::User,
            parts: vec![Part::Text {
                text: "What time is it?".into(),
            }],
        },
    }
}

fn inference_event(calls: &[ToolCallRecord]) -> Event {
    Event::InferenceCompleted {
        provider: String::new(),
        model: String::new(),
        effort_used: None,
        usage: swarmy_core::TokenUsage::default(),
        cost_micros: 0,
        effort_requested: None,
        effort_clamped: false,
        seq: 3,
        request_id: RequestId::for_step(session().session_id, 2),
        message: Message {
            id: message_id(4),
            role: MessageRole::Assistant,
            parts: calls
                .iter()
                .map(|call| Part::ToolCall {
                    call_id: call.call_id.clone(),
                    tool: call.tool.clone(),
                    input: call.arguments.clone(),
                })
                .collect(),
        },
    }
}

fn fixture() -> Vec<Event> {
    let request_id = RequestId::for_step(session().session_id, 2);
    vec![
        user_event(),
        Event::InferenceRequested {
            seq: 2,
            request_id,
            step: 2,
        },
        inference_event(&[call("first"), call("second")]),
        Event::ToolCallRequested {
            seq: 4,
            request_id,
            call: call("first"),
        },
        Event::ToolCallRequested {
            seq: 5,
            request_id,
            call: call("second"),
        },
        Event::ToolCallCompleted {
            seq: 6,
            request_id,
            call_id: ToolCallId("second".into()),
            result: execution_result("get_time", Ok("2026-09-14T12:00:02Z".into())),
        },
        Event::ToolCallCompleted {
            seq: 7,
            request_id,
            call_id: ToolCallId("first".into()),
            result: execution_result("get_time", Ok("2026-09-14T12:00:01Z".into())),
        },
    ]
}

fn step(events: &[Event]) -> Action {
    harness().step(&session(), &Snapshot::default(), events, message_id(5))
}

#[test]
fn user_message_builds_inference_with_system_prompt() {
    let Action::BuildInference(request) = step(&[user_event()]) else {
        panic!("expected inference");
    };
    assert_eq!(request.system_prompt, harness().system_prompt_template);
    assert_eq!(request.messages.len(), 1);
    assert_eq!(request.messages[0].role, MessageRole::User);
    assert_eq!(
        request.messages[0].parts[0],
        Part::Text {
            text: "What time is it?".into()
        }
    );
    assert_eq!(request.tools, harness().tools.definitions());
}

#[test]
fn single_model_tool_call_is_dispatched() {
    assert_eq!(
        step(&[user_event(), inference_event(&[call("first")])]),
        Action::DispatchTools(vec![call("first")])
    );
}

#[test]
fn partial_parallel_results_wait() {
    let events = fixture();
    for end in [4, 5, 6] {
        assert_eq!(step(&events[..end]), Action::Wait);
    }
}

#[test]
fn all_results_fold_in_request_order_and_then_build_inference() {
    let mut events = fixture();
    let Action::FoldResults(message) = step(&events) else {
        panic!("expected folded results");
    };
    assert_eq!(message.id, message_id(5));
    assert_eq!(message.role, MessageRole::Tool);
    assert_eq!(
        message.parts,
        vec![
            result_part(
                ToolCallId("first".into()),
                execution_result("get_time", Ok("2026-09-14T12:00:01Z".into()))
            ),
            result_part(
                ToolCallId("second".into()),
                execution_result("get_time", Ok("2026-09-14T12:00:02Z".into()))
            ),
        ]
    );
    events.push(Event::MessageAppended { seq: 8, message });
    let Action::BuildInference(request) = step(&events) else {
        panic!("expected next inference");
    };
    assert_eq!(request.messages.len(), 3);
    assert_eq!(request.messages[1].role, MessageRole::Assistant);
    assert_eq!(request.messages[2].role, MessageRole::Tool);
}

#[test]
fn request_event_order_controls_fold_order() {
    let mut events = fixture();
    if let Event::ToolCallRequested { call, .. } = &mut events[3] {
        call.call_id = ToolCallId("second".into());
    }
    if let Event::ToolCallRequested { call, .. } = &mut events[4] {
        call.call_id = ToolCallId("first".into());
    }
    let Action::FoldResults(message) = step(&events) else {
        panic!("expected folded results");
    };
    assert!(matches!(&message.parts[0], Part::ToolResult { call_id, .. } if call_id.0 == "second"));
}

#[test]
fn errors_and_success_metadata_survive_folding() {
    let mut events = fixture();
    let failure = execution_result("get_time", Err("clock unavailable".into()));
    let success = ToolResult::Completed {
        output: "timestamp".into(),
        title: "Worker clock".into(),
        metadata: BTreeMap::from([("source".into(), json!("worker"))]),
    };
    if let Event::ToolCallCompleted { result, .. } = &mut events[6] {
        *result = failure.clone();
    }
    if let Event::ToolCallCompleted { result, .. } = &mut events[5] {
        *result = success.clone();
    }
    let Action::FoldResults(message) = step(&events) else {
        panic!("expected folded results");
    };
    assert_eq!(
        message.parts,
        vec![
            result_part(ToolCallId("first".into()), failure),
            result_part(ToolCallId("second".into()), success),
        ]
    );
}

#[test]
fn model_without_tool_calls_ends_turn() {
    let mut reply = inference_event(&[]);
    if let Event::InferenceCompleted { message, .. } = &mut reply {
        message.parts.push(Part::Text {
            text: "It is noon.".into(),
        });
    }
    assert_eq!(step(&[user_event(), reply]), Action::EndTurn);
}

#[test]
fn assembled_request_matches_pretty_json_snapshot_exactly() {
    let Action::BuildInference(request) = step(&[user_event()]) else {
        panic!("expected inference");
    };
    assert_eq!(
        format!("{}\n", serde_json::to_string_pretty(&request).unwrap()),
        include_str!("fixtures/request.json")
    );
}

#[test]
fn replay_is_identical_across_every_snapshot_boundary() {
    let events = fixture();
    for split in 0..=events.len() {
        let snapshot = Snapshot::default().replay(&events[..split]);
        let decoded =
            swarmy_core::decode::<Snapshot>(&swarmy_core::encode(&snapshot).unwrap()).unwrap();
        assert_eq!(snapshot, decoded);
        assert_eq!(
            harness().step(&session(), &decoded, &events[split..], message_id(5)),
            step(&events)
        );
    }
}

#[test]
fn bookkeeping_does_not_trigger_extra_work() {
    assert_eq!(step(&[]), Action::Wait);
    let mut events = fixture();
    events.truncate(2);
    events.push(Event::StateChanged {
        seq: 3,
        from: SessionState::WaitingInference,
        to: SessionState::Runnable,
    });
    assert_eq!(step(&events), Action::Wait);
    events.push(user_event());
    assert_eq!(step(&events), Action::Wait);
    let terminal = SessionRecord {
        interrupt_requested: false,
        state: SessionState::Completed,
        ..session()
    };
    assert_eq!(
        harness().step(
            &terminal,
            &Snapshot::default(),
            &[user_event()],
            message_id(5)
        ),
        Action::EndTurn
    );
}

#[test]
fn unrelated_or_duplicate_completions_do_not_finish_pending_calls() {
    let mut events = fixture();
    events.pop();
    events.push(events[5].clone());
    let mut unrelated = fixture().pop().unwrap();
    if let Event::ToolCallCompleted { request_id, .. } = &mut unrelated {
        *request_id = RequestId::for_step(session().session_id, 99);
    }
    events.push(unrelated);
    assert_eq!(step(&events), Action::Wait);
}

#[test]
fn each_tool_can_have_its_own_request_id() {
    let mut events = fixture();
    for (indices, step_seq) in [([3, 6], 4), ([4, 5], 5)] {
        for index in indices {
            match &mut events[index] {
                Event::ToolCallRequested { request_id, .. }
                | Event::ToolCallCompleted { request_id, .. } => {
                    *request_id = RequestId::for_step(session().session_id, step_seq);
                }
                _ => panic!("expected tool event"),
            }
        }
    }
    assert_eq!(step(&events[..6]), Action::Wait);
    assert_eq!(step(&events), step(&fixture()));
}

struct Echo;

impl Tool for Echo {
    fn name(&self) -> &'static str {
        "echo"
    }
    fn description(&self) -> &'static str {
        "Echo JSON arguments."
    }
    fn parameters(&self) -> Value {
        json!({"type": "object"})
    }
    fn execute(&self, arguments: Value) -> BoxFuture<'_, Result<String, String>> {
        Box::pin(async move { Ok(arguments.to_string()) })
    }
}

#[test]
fn registry_sorts_definitions_and_supports_lookup_and_replacement() {
    let mut first = harness().tools;
    assert!(first.register(Box::new(Echo)).is_none());
    let mut second = ToolRegistry::default();
    second.register(Box::new(Echo));
    second.register(Box::new(GetTime));
    assert_eq!(first.definitions(), second.definitions());
    assert_eq!(first.definitions()[0].name, "echo");
    assert!(first.get("missing").is_none());
    assert_eq!(
        first.get("echo").unwrap().parameters(),
        json!({"type": "object"})
    );
    assert!(first.register(Box::new(Echo)).is_some());
    assert_eq!(
        assemble_prompt("", &[], &GenerationSettings::default(), &first)
            .tools
            .len(),
        2
    );
}

#[tokio::test]
async fn get_time_executes_as_rfc3339_and_rejects_invalid_arguments() {
    let registry = harness().tools;
    let tool = registry.get("get_time").unwrap();
    let before = jiff::Timestamp::now();
    let output = tool.execute(json!({})).await.unwrap();
    let after = jiff::Timestamp::now();
    let timestamp: jiff::Timestamp = output.parse().unwrap();
    assert!(output.contains('T') && output.ends_with('Z'));
    assert!(before <= timestamp && timestamp <= after);
    for arguments in [Value::Null, json!([]), json!({"timezone": "UTC"})] {
        assert!(tool.execute(arguments).await.is_err());
    }
}

fn fake_provider() -> FakeProvider {
    let mut provider = FakeProvider::default();
    provider.responses.insert(
        1,
        Response {
            parts: vec![Part::Text {
                text: "The clock result is above.".into(),
            }],
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage::default(),
            quota_remaining: std::collections::BTreeMap::new(),
            quota_resets: std::collections::BTreeMap::new(),
        },
    );
    provider.tool_calls = Some(BTreeMap::from([(
        0,
        vec![Part::ToolCall {
            call_id: ToolCallId("clock".into()),
            tool: "get_time".into(),
            input: json!({}),
        }],
    )]));
    provider
}

#[tokio::test]
async fn fake_provider_and_worker_tool_complete_a_turn() {
    let harness = harness();
    let provider = fake_provider();
    let mut events = vec![user_event()];
    for turn in 0..2 {
        let Action::BuildInference(request) = step(&events) else {
            panic!("expected inference")
        };
        let seq = u64::try_from(events.len()).unwrap() + 1;
        let request_id = RequestId::for_step(session().session_id, seq);
        events.push(Event::InferenceRequested {
            seq,
            request_id,
            step: seq,
        });
        let mut stream = provider.request(request);
        let mut completed = None;
        while let Some(delta) = stream.next().await {
            if let Delta::Completed(response) = delta.unwrap() {
                completed = Some(response);
            }
        }
        events.push(Event::InferenceCompleted {
            provider: String::new(),
            model: String::new(),
            effort_used: None,
            usage: swarmy_core::TokenUsage::default(),
            cost_micros: 0,
            effort_requested: None,
            effort_clamped: false,
            seq: seq + 1,
            request_id,
            message: Message {
                id: message_id(10 + turn),
                role: MessageRole::Assistant,
                parts: completed.unwrap().parts,
            },
        });
        if turn == 1 {
            assert_eq!(step(&events), Action::EndTurn);
            break;
        }
        let Action::DispatchTools(calls) = step(&events) else {
            panic!("expected tool dispatch")
        };
        assert_eq!(calls.len(), 1);
        let call = &calls[0];
        events.push(Event::ToolCallRequested {
            seq: seq + 2,
            request_id,
            call: call.clone(),
        });
        let output = harness
            .tools
            .get(&call.tool)
            .unwrap()
            .execute(call.arguments.clone())
            .await;
        events.push(Event::ToolCallCompleted {
            seq: seq + 3,
            request_id,
            call_id: call.call_id.clone(),
            result: execution_result(&call.tool, output),
        });
        let Action::FoldResults(message) = step(&events) else {
            panic!("expected results")
        };
        events.push(Event::MessageAppended {
            seq: seq + 4,
            message,
        });
    }
    assert_eq!(provider.call_count(), 2);
}

#[test]
fn exhausted_inference_ends_the_turn_instead_of_retrying() {
    let request_id = RequestId::for_step(session().session_id, 2);
    let events = [
        user_event(),
        Event::InferenceRequested {
            seq: 2,
            request_id,
            step: 2,
        },
        Event::InferenceFailed {
            seq: 3,
            request_id,
            error: "provider failed".into(),
            retryable: false,
            retry_at: None,
        },
    ];
    assert_eq!(step(&events), Action::EndTurn);
    // A failure for some other request does not disturb the wait.
    let other = RequestId::for_step(session().session_id, 9);
    let unrelated = [
        events[0].clone(),
        events[1].clone(),
        Event::InferenceFailed {
            seq: 3,
            request_id: other,
            error: "stale".into(),
            retryable: false,
            retry_at: None,
        },
    ];
    assert_eq!(step(&unrelated), Action::Wait);
}

#[test]
fn disk_manifest_and_command_status_survive_folding_and_snapshot_replay() {
    let result = swarmy_core::BashResult {
        stdout: "written\n".into(),
        stderr: "diagnostic\n".into(),
        exit_code: 7,
        timed_out: false,
        manifest_id: swarmy_core::ManifestId::from_ulid(Ulid::generate()),
    }
    .tool_result();
    let mut events = fixture();
    if let Event::ToolCallCompleted { result: stored, .. } = &mut events[6] {
        *stored = result.clone();
    }
    let Action::FoldResults(message) = step(&events) else {
        panic!("expected fold");
    };
    assert_eq!(
        message.parts[0],
        result_part(ToolCallId("first".into()), result)
    );
    events.push(Event::MessageAppended {
        seq: 8,
        message: message.clone(),
    });
    let snapshot = Snapshot::default().replay(&events);
    let bytes = swarmy_core::encode(&snapshot).unwrap();
    let restored: Snapshot = swarmy_core::decode(&bytes).unwrap();
    assert_eq!(restored.messages().last(), Some(&message));
}

#[test]
fn sandbox_tools_dispatch_with_validated_arguments_and_durability_descriptions() {
    let mut registry = swarmy_harness::ToolRegistry::default();
    swarmy_tools::register(&mut registry);
    let id = swarmy_core::ProcessId::from_ulid(ulid::Ulid::from(42_u128));
    for (name, arguments) in [
        ("bash", json!({"command":"echo hello"})),
        ("process_start", json!({"command":"sleep 300"})),
        ("process_list", json!({})),
        ("process_log", json!({"process_id":id})),
        ("process_stop", json!({"process_id":id})),
        ("checkpoint", json!({})),
        ("write_stdin", json!({"process_id":id, "text":"hello\n"})),
        ("web_fetch", json!({"url":"http://localhost/"})),
        ("read", json!({"path":"text"})),
        ("write", json!({"path":"text", "content":"hello"})),
        (
            "edit",
            json!({"path":"text", "old_string":"hello", "new_string":"world"}),
        ),
        ("glob", json!({"pattern":"*.rs"})),
        ("grep", json!({"pattern":"hello"})),
        ("ls", json!({})),
    ] {
        let call = ToolCallRecord {
            call_id: ToolCallId(name.into()),
            tool: name.into(),
            arguments: arguments.clone(),
            result: None,
        };
        assert_eq!(
            step(&[user_event(), inference_event(std::slice::from_ref(&call))]),
            Action::DispatchTools(vec![call])
        );
        let tool = registry.get(name).unwrap();
        assert!(tool.sandbox_bound());
        let parsed = swarmy_core::SandboxArguments::parse(name, arguments).unwrap();
        assert_eq!(parsed.name(), name);
        assert_eq!(
            swarmy_core::decode::<swarmy_core::SandboxArguments>(
                &swarmy_core::encode(&parsed).unwrap()
            )
            .unwrap(),
            parsed
        );
        assert!(futures::executor::block_on(tool.execute(parsed.parameters())).is_err());
        let description = tool.description();
        for wording in [
            "Files persist across failures up to the last snapshot",
            "every ten minutes",
            "checkpoint is called",
            "Processes do not survive a node failure or an idle eviction",
        ] {
            assert!(description.contains(wording), "{name}: {description}");
        }
        assert!(swarmy_core::SandboxArguments::parse(name, json!({"unexpected":true})).is_err());
    }
    assert_eq!(registry.definitions().len(), 18);
    assert!(
        swarmy_core::SandboxArguments::parse("process_stop", json!({"process_id":"../other"}))
            .is_err()
    );
}

#[test]
fn file_tool_schemas_reject_invalid_arguments() {
    for (name, arguments) in [
        ("read", json!({"path":"a", "offset":0})),
        ("read", json!({"path":"a", "limit":0})),
        ("read", json!({"path":"a", "offset":1.5})),
        ("write", json!({"path":"a"})),
        ("write", json!({"path":"", "content":"hello"})),
        (
            "edit",
            json!({"path":"a", "old_string":"", "new_string":"hello"}),
        ),
        (
            "edit",
            json!({"path":"a", "old_string":"hello", "new_string":"bye", "replace_all":1}),
        ),
        ("glob", json!({"pattern":""})),
        ("grep", json!({"pattern":"a", "path":4})),
        ("ls", json!({"path":""})),
    ] {
        assert!(
            swarmy_core::SandboxArguments::parse(name, arguments).is_err(),
            "{name}"
        );
    }
}

#[test]
fn update_plan_dispatches_without_a_sandbox_and_validates_steps() {
    use swarmy_core::UpdatePlanArguments;
    let mut registry = ToolRegistry::default();
    swarmy_tools::register(&mut registry);
    let tool = registry.get("update_plan").unwrap();
    assert!(!tool.sandbox_bound());
    let arguments = json!({"plan":[
        {"step":"Read", "status":"completed"},
        {"step":"Implement", "status":"in_progress"},
        {"step":"Test", "status":"pending"}
    ]});
    let call = ToolCallRecord {
        call_id: ToolCallId("plan".into()),
        tool: "update_plan".into(),
        arguments: arguments.clone(),
        result: None,
    };
    assert_eq!(
        step(&[user_event(), inference_event(std::slice::from_ref(&call))]),
        Action::DispatchTools(vec![call])
    );
    assert_eq!(UpdatePlanArguments::parse(arguments).unwrap().plan.len(), 3);
    assert!(
        UpdatePlanArguments::parse(json!({"plan":[]}))
            .unwrap()
            .plan
            .is_empty()
    );
    for invalid in [
        json!({"plan":[{"step":"One", "status":"in_progress"},{"step":"Two", "status":"in_progress"}]}),
        json!({"plan":[{"step":" ", "status":"pending"}]}),
        json!({"plan":[{"step":"One", "status":"running"}]}),
        json!({"plan":[{"step":"One", "status":"pending", "extra":true}]}),
        json!({"plan":[], "extra":true}),
        json!({}),
    ] {
        assert!(UpdatePlanArguments::parse(invalid).is_err());
    }
}

#[test]
fn system_timer_note_starts_a_turn_but_does_not_interrupt_inflight_work() {
    let note = Event::MessageAppended {
        seq: 8,
        message: Message {
            id: message_id(9),
            role: MessageRole::System,
            parts: vec![Part::Text {
                text: "timer reminder".into(),
            }],
        },
    };
    for history in [vec![], vec![user_event(), inference_event(&[])]] {
        let snapshot = Snapshot::default().replay(&history);
        let Action::BuildInference(request) = harness().step(
            &session(),
            &snapshot,
            std::slice::from_ref(&note),
            message_id(10),
        ) else {
            panic!("system note did not start inference")
        };
        assert_eq!(request.messages.last().unwrap().role, MessageRole::System);
    }
    for history in [&fixture()[..2], &fixture()[..5]] {
        let snapshot = Snapshot::default().replay(history);
        assert_eq!(
            harness().step(
                &session(),
                &snapshot,
                std::slice::from_ref(&note),
                message_id(10)
            ),
            Action::Wait
        );
    }
}

#[test]
fn timer_tools_dispatch_without_a_sandbox() {
    let mut registry = ToolRegistry::default();
    swarmy_tools::register(&mut registry);
    for (name, arguments) in [
        ("set_timer", json!({"delay_seconds":120,"note":"remember"})),
        ("list_timers", json!({})),
        (
            "cancel_timer",
            json!({"timer_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV"}),
        ),
    ] {
        let tool = registry.get(name).unwrap();
        assert!(!tool.sandbox_bound());
        let call = ToolCallRecord {
            call_id: ToolCallId(name.into()),
            tool: name.into(),
            arguments,
            result: None,
        };
        assert_eq!(
            step(&[user_event(), inference_event(std::slice::from_ref(&call))]),
            Action::DispatchTools(vec![call])
        );
    }
}
