use std::{
    collections::BTreeMap,
    fmt::Write,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures::{StreamExt, TryStreamExt};
use serde_json::{Value, json};
use swarmy_core::{Message, MessageId, MessageRole, Part, ToolCallId, ToolResult};
use swarmy_llm::{
    ClientAuth, Delta, Error, GenerationSettings, Provider, ReasoningEffort, Request, Response,
    StopReason, TokenUsage, ToolDefinition,
    api::completions::{CompletionsProvider, SseParser, request_json},
    catalog::{Api, Catalog, ModelInfo, ProviderInfo, ReasoningOptions},
    client_for,
    retry::RetryPolicy,
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path},
};

const MODEL: &str = "openai/gpt-5.5";

fn model() -> ModelInfo {
    Catalog::get().model("openrouter", MODEL).unwrap().clone()
}

fn provider_info(server: &MockServer) -> ProviderInfo {
    ProviderInfo {
        base_url: format!("{}/api/v1/", server.uri()),
        ..Catalog::get().provider("openrouter").unwrap().clone()
    }
}

fn request(model: &ModelInfo) -> Request {
    Request {
        system_prompt: "Be helpful.".into(),
        messages: vec![message(MessageRole::User, vec![text("Hello")])],
        tools: vec![],
        settings: GenerationSettings {
            model: model.id.clone(),
            ..Default::default()
        },
    }
}

fn message(role: MessageRole, parts: Vec<Part>) -> Message {
    Message {
        id: MessageId::from_ulid(ulid::Ulid::nil()),
        role,
        parts,
    }
}

fn text(value: &str) -> Part {
    Part::Text { text: value.into() }
}

fn chunk(delta: Value, finish: Value) -> Value {
    let mut choice = json!({"index": 0});
    choice["delta"] = delta;
    choice["finish_reason"] = finish;
    let mut event = json!({});
    event["choices"] = Value::Array(vec![choice]);
    event
}

fn sse(events: &[Value]) -> String {
    let mut data = String::new();
    for event in events {
        writeln!(data, "data: {event}\n").unwrap();
    }
    data.push_str("data: [DONE]\n\n");
    data
}

fn text_stream() -> String {
    sse(&[
        chunk(json!({"content": "Hello "}), Value::Null),
        chunk(json!({"content": "🌍"}), json!("stop")),
        json!({"choices": [], "usage": {"prompt_tokens": 21, "completion_tokens": 9, "total_tokens": 30,
            "prompt_tokens_details": {"cached_tokens": 12}, "completion_tokens_details": {"reasoning_tokens": 4}}}),
    ])
}

fn completed(deltas: &[Delta]) -> &Response {
    match deltas.last().unwrap() {
        Delta::Completed(response) => response,
        other => panic!("expected completion, got {other:?}"),
    }
}

async fn fixture(server: &MockServer, body: String) {
    Mock::given(method("POST"))
        .and(path("/api/v1/chat/completions"))
        .and(header("Authorization", "Bearer fixture-key"))
        .and(header("HTTP-Referer", "https://github.com/pachuc/swarmy"))
        .and(header("X-Title", "swarmy"))
        .and(header(
            "User-Agent",
            concat!("swarmy/", env!("CARGO_PKG_VERSION")),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "text/event-stream"))
        .mount(server)
        .await;
}

async fn turn(server: &MockServer, model: &ModelInfo, request: Request) -> Vec<Delta> {
    CompletionsProvider::new(
        &provider_info(server),
        model,
        ClientAuth::ApiKey("fixture-key".into()),
    )
    .unwrap()
    .request(request)
    .try_collect()
    .await
    .unwrap()
}

async fn bodies(server: &MockServer) -> Vec<Value> {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .map(|request| serde_json::from_slice(&request.body).unwrap())
        .collect()
}

#[tokio::test]
async fn text_turn_reports_usage_and_uses_catalog_dispatch() {
    let server = MockServer::start().await;
    fixture(&server, text_stream()).await;
    let model = model();
    let provider = client_for(
        &provider_info(&server),
        &model,
        ClientAuth::ApiKey("fixture-key".into()),
    )
    .unwrap();
    let deltas: Vec<_> = provider
        .request(request(&model))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        completed(&deltas),
        &Response {
            parts: vec![text("Hello 🌍")],
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage {
                input_tokens: 21,
                cached_input_tokens: 12,
                output_tokens: 9,
                reasoning_output_tokens: 4,
                total_tokens: 30,
                cache_write_input_tokens: 0,
            },
            quota_remaining: std::collections::BTreeMap::new(),
            quota_resets: std::collections::BTreeMap::new(),
        }
    );
    assert_eq!(
        deltas
            .iter()
            .filter(|delta| matches!(delta, Delta::Completed(_)))
            .count(),
        1
    );
    assert!(matches!(&deltas[0], Delta::Text { output_index: 0, text } if text == "Hello "));
    let body = bodies(&server).await.remove(0);
    assert_eq!(body["model"], MODEL);
    assert_eq!(body["stream"], true);
    assert_eq!(body["stream_options"], json!({"include_usage": true}));
    assert_eq!(body["usage"], json!({"include": true}));
    assert_eq!(body["messages"][0]["role"], "developer");
    assert_eq!(body["messages"][1]["content"][0]["text"], "Hello");
}

#[tokio::test]
async fn parallel_tool_calls_accumulate_interleaved_arguments_and_keep_ids() {
    let server = MockServer::start().await;
    fixture(&server, sse(&[
        chunk(json!({"tool_calls": [
            {"index": 0, "id": "call/a:1", "type": "function", "function": {"name": "read", "arguments": "{\"path\":"}},
            {"index": 1, "id": "call|b", "type": "function", "function": {"name": "run", "arguments": "{\"cmd\":"}}
        ]}), Value::Null),
        chunk(json!({"tool_calls": [{"index": 1, "function": {"name": "run", "arguments": "\"pwd\"}"}}]}), Value::Null),
        chunk(json!({"tool_calls": [{"index": 0, "function": {"arguments": "\"src\"}"}}]}), json!("tool_calls")),
    ])).await;
    let deltas = turn(&server, &model(), request(&model())).await;
    let response = completed(&deltas);
    assert_eq!(response.stop_reason, StopReason::ToolCalls);
    assert_eq!(
        response.parts,
        vec![
            Part::ToolCall {
                call_id: ToolCallId("call/a:1".into()),
                tool: "read".into(),
                input: json!({"path": "src"})
            },
            Part::ToolCall {
                call_id: ToolCallId("call|b".into()),
                tool: "run".into(),
                input: json!({"cmd": "pwd"})
            },
        ]
    );
    let indices: Vec<_> = deltas
        .iter()
        .filter_map(|delta| match delta {
            Delta::ToolArguments { output_index, .. } => Some(*output_index),
            _ => None,
        })
        .collect();
    assert_eq!(indices, vec![0, 1, 1, 0]);
}

#[tokio::test]
async fn reasoning_details_round_trip_only_for_the_same_provider_and_model() {
    let server = MockServer::start().await;
    let details =
        json!([{"type": "reasoning.encrypted", "data": "opaque", "id": "r1", "index": 0}]);
    fixture(&server, sse(&[
        chunk(json!({"reasoning": "Think first."}), Value::Null),
        json!({"choices": [{"delta": {"content": "Answer"}, "finish_reason": "stop", "message": {"reasoning_details": details}}]}),
    ])).await;
    let original = model();
    let deltas = turn(&server, &original, request(&original)).await;
    let parts = &completed(&deltas).parts;
    assert!(matches!(&parts[0], Part::Reasoning { text, metadata }
        if text == "Think first." && metadata["openrouter"] == details && metadata["model"] == MODEL));
    let mut same = request(&original);
    same.messages
        .push(message(MessageRole::Assistant, parts.clone()));
    turn(&server, &original, same.clone()).await;
    let mut other = original.clone();
    other.id = "other/model".into();
    same.settings.model.clone_from(&other.id);
    turn(&server, &other, same.clone()).await;
    let bodies = bodies(&server).await;
    assert_eq!(bodies[1]["messages"][2]["reasoning_details"], details);
    assert_eq!(bodies[1]["messages"][2]["content"], "Answer");
    assert!(bodies[2]["messages"][2].get("reasoning_details").is_none());
    assert_eq!(bodies[2]["messages"][2]["content"], "Think first.Answer");
    same.settings.model.clone_from(&original.id);
    let body = request_json(&same, "another-provider", &original).unwrap();
    assert!(body["messages"][2].get("reasoning_details").is_none());
    assert_eq!(body["messages"][2]["content"], "Think first.Answer");
}

#[tokio::test]
async fn cache_control_marks_only_the_last_system_and_last_two_user_parts() {
    let server = MockServer::start().await;
    fixture(&server, text_stream()).await;
    // The snapshot uses a dot; also exercise the task's custom model spelling.
    let claude = Catalog::get()
        .model("openrouter", "anthropic/claude-sonnet-4.6")
        .unwrap();
    for id in [
        "anthropic/claude-sonnet-4.6",
        "anthropic/claude-sonnet-4-6",
        MODEL,
    ] {
        let mut selected = if id == MODEL { model() } else { claude.clone() };
        selected.id = id.into();
        // Test the completions wire adapter directly: catalog Claude routing remains Messages.
        selected.base_url = None;
        let mut request = request(&selected);
        request.messages.extend([
            message(
                MessageRole::System,
                vec![text("Notice"), text("Last system")],
            ),
            message(MessageRole::User, vec![text("Second user")]),
            message(MessageRole::Assistant, vec![text("Reply")]),
            message(
                MessageRole::User,
                vec![text("Third user"), text("Last part")],
            ),
        ]);
        turn(&server, &selected, request).await;
    }
    let bodies = bodies(&server).await;
    for body in &bodies[..2] {
        let messages = &body["messages"];
        for (message, part) in [(2, 1), (3, 0), (5, 1)] {
            assert_eq!(
                messages[message]["content"][part]["cache_control"],
                json!({"type": "ephemeral"})
            );
        }
        assert!(messages[0]["content"][0].get("cache_control").is_none());
        assert!(messages[1]["content"][0].get("cache_control").is_none());
        assert!(messages[5]["content"][0].get("cache_control").is_none());
        assert_eq!(body.to_string().matches("cache_control").count(), 3);
    }
    assert!(!bodies[2].to_string().contains("cache_control"));
}

#[tokio::test]
async fn compatibility_controls_roles_limits_tools_and_provider_routing() {
    let server = MockServer::start().await;
    fixture(&server, text_stream()).await;
    for (field, developer) in [("max_tokens", false), ("max_completion_tokens", true)] {
        let mut model = model();
        model
            .compat
            .0
            .insert("max_tokens_field".into(), json!(field));
        model
            .compat
            .0
            .insert("supports_developer_role".into(), json!(developer));
        model.compat.0.insert(
            "openrouter_provider".into(),
            json!({"order": ["test"], "allow_fallbacks": false}),
        );
        let mut request = request(&model);
        request.settings.max_output_tokens = Some(1234);
        request.settings.temperature = Some(0.25);
        request.settings.reasoning_effort = Some(ReasoningEffort::Max);
        request.tools.push(ToolDefinition {
            name: "read".into(),
            description: "Read a file".into(),
            parameters: json!({"type": "object"}),
        });
        turn(&server, &model, request).await;
    }
    let bodies = bodies(&server).await;
    for (index, field, absent, role) in [
        (0, "max_tokens", "max_completion_tokens", "system"),
        (1, "max_completion_tokens", "max_tokens", "developer"),
    ] {
        assert_eq!(bodies[index][field], 1234);
        assert!(bodies[index].get(absent).is_none());
        assert_eq!(bodies[index]["messages"][0]["role"], role);
        assert_eq!(bodies[index]["temperature"], 0.25);
        assert_eq!(bodies[index]["reasoning"], json!({"effort": "xhigh"}));
        assert_eq!(
            bodies[index]["provider"],
            json!({"order": ["test"], "allow_fallbacks": false})
        );
        assert_eq!(
            bodies[index]["tools"],
            json!([{"type": "function", "function": {
                "name": "read", "description": "Read a file", "parameters": {"type": "object"}, "strict": false
            }}])
        );
    }
}

#[test]
fn reasoning_efforts_disable_clamp_or_use_bounded_budgets() {
    let mut model = model();
    let mut request = request(&model);
    request.settings.reasoning_effort = Some(ReasoningEffort::None);
    assert_eq!(
        request_json(&request, "openrouter", &model).unwrap()["reasoning"],
        json!({"enabled": false})
    );
    model.reasoning = Some(ReasoningOptions::Effort(vec![
        ReasoningEffort::Low,
        ReasoningEffort::High,
    ]));
    assert_eq!(
        request_json(&request, "openrouter", &model).unwrap()["reasoning"],
        json!({"effort": "low"})
    );
    request.settings.reasoning_effort = Some(ReasoningEffort::Medium);
    assert_eq!(
        request_json(&request, "openrouter", &model).unwrap()["reasoning"],
        json!({"effort": "high"})
    );
    model.reasoning = Some(ReasoningOptions::BudgetTokens {
        min: Some(4096),
        max: Some(12000),
    });
    request.settings.reasoning_effort = Some(ReasoningEffort::Low);
    assert_eq!(
        request_json(&request, "openrouter", &model).unwrap()["reasoning"],
        json!({"max_tokens": 4096})
    );
    request.settings.reasoning_effort = Some(ReasoningEffort::High);
    assert_eq!(
        request_json(&request, "openrouter", &model).unwrap()["reasoning"],
        json!({"max_tokens": 12000})
    );
    request.settings.max_output_tokens = Some(10000);
    assert_eq!(
        request_json(&request, "openrouter", &model).unwrap()["reasoning"],
        json!({"max_tokens": 9999})
    );
    model.reasoning = None;
    assert!(
        request_json(&request, "openrouter", &model)
            .unwrap()
            .get("reasoning")
            .is_none()
    );
}

#[tokio::test]
async fn tool_history_repairs_orphans_and_moves_results_before_notices() {
    let server = MockServer::start().await;
    fixture(&server, text_stream()).await;
    let model = model();
    let mut request = request(&model);
    let call = |id: &str| Part::ToolCall {
        call_id: ToolCallId(id.into()),
        tool: "read".into(),
        input: json!({}),
    };
    request.messages.extend([
        message(
            MessageRole::Assistant,
            vec![
                text("Checking"),
                call("one/1"),
                call("two|2"),
                call("three:3"),
            ],
        ),
        message(MessageRole::System, vec![text("Computer rebuilt")]),
        message(
            MessageRole::Tool,
            vec![
                Part::ToolResult {
                    call_id: ToolCallId("three:3".into()),
                    result: ToolResult::Error {
                        error: "Interrupted".into(),
                    },
                },
                Part::ToolResult {
                    call_id: ToolCallId("one/1".into()),
                    result: ToolResult::Completed {
                        output: "contents".into(),
                        title: String::new(),
                        metadata: BTreeMap::new(),
                    },
                },
            ],
        ),
        message(MessageRole::User, vec![text("Continue")]),
        message(MessageRole::Assistant, vec![call("tail")]),
    ]);
    turn(&server, &model, request).await;
    let body = bodies(&server).await.remove(0);
    let messages = &body["messages"];
    assert_eq!(messages[2]["content"], "Checking");
    assert_eq!(messages[2]["tool_calls"][0]["id"], "one/1");
    assert_eq!(messages[2]["tool_calls"][0]["function"]["arguments"], "{}");
    for (index, id, content) in [
        (3, "one/1", "contents"),
        (4, "two|2", "{\"error\":\"No result provided\"}"),
        (5, "three:3", "{\"error\":\"Interrupted\"}"),
        (9, "tail", "{\"error\":\"No result provided\"}"),
    ] {
        assert_eq!(
            messages[index],
            json!({"role": "tool", "tool_call_id": id, "content": content})
        );
    }
    assert_eq!(messages[6]["role"], "developer");
}

#[test]
fn result_without_any_call_gets_a_neutral_placeholder() {
    // A result whose call id appears nowhere in the history gets a neutral
    // placeholder call: the name must not guess the first declared tool.
    let model = model();
    let mut req = request(&model);
    req.tools.push(ToolDefinition {
        name: "get_time".into(),
        description: "Read time".into(),
        parameters: serde_json::json!({"type": "object"}),
    });
    req.messages.push(message(
        MessageRole::Tool,
        vec![Part::ToolResult {
            call_id: ToolCallId("call_1".into()),
            result: ToolResult::Completed {
                output: "12:00".into(),
                title: "get_time".into(),
                metadata: BTreeMap::new(),
            },
        }],
    ));
    let body = request_json(&req, "openrouter", &model).unwrap();
    let messages = body["messages"].as_array().unwrap();
    let assistant = messages
        .iter()
        .find(|m| m["role"] == "assistant" && m.get("tool_calls").is_some())
        .expect("synthesized assistant tool call");
    assert_eq!(assistant["tool_calls"][0]["id"], "call_1");
    assert_eq!(
        assistant["tool_calls"][0]["function"]["name"],
        "unknown_tool"
    );
    let positions: Vec<usize> = messages
        .iter()
        .enumerate()
        .filter_map(|(i, m)| {
            (m.get("tool_calls").is_some() || m["tool_call_id"] == "call_1").then_some(i)
        })
        .collect();
    assert_eq!(positions.len(), 2);
    assert_eq!(positions[0] + 1, positions[1]);
}

fn stalled_session_messages() -> Vec<Message> {
    let events: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/notice-between-call-and-result.json")).unwrap();
    events
        .iter()
        .map(|event| serde_json::from_value(event["message"].clone()).unwrap())
        .collect()
}

fn stalled_session_tools() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "edit".into(),
            description: "Edit a file".into(),
            parameters: serde_json::json!({"type": "object"}),
        },
        ToolDefinition {
            name: "bash".into(),
            description: "Run a command".into(),
            parameters: serde_json::json!({"type": "object"}),
        },
    ]
}

fn assert_results_follow_calls(messages: &[Value], calls: &[&str], notice: &str) {
    for id in calls {
        let call = messages
            .iter()
            .position(|m| {
                m.get("tool_calls").is_some_and(|calls| {
                    calls
                        .as_array()
                        .is_some_and(|calls| calls.iter().any(|call| call["id"] == *id))
                })
            })
            .unwrap_or_else(|| panic!("call {id} in wire messages"));
        let result = messages
            .iter()
            .position(|m| m["tool_call_id"] == *id)
            .unwrap_or_else(|| panic!("result {id} in wire messages"));
        assert_eq!(
            call + 1,
            result,
            "result for {id} must directly follow its call"
        );
    }
    let notice = messages
        .iter()
        .position(|m| m.to_string().contains(notice))
        .expect("notice in wire messages");
    for id in calls {
        let result = messages
            .iter()
            .position(|m| m["tool_call_id"] == *id)
            .unwrap();
        assert!(
            notice > result,
            "notice must come after the result for {id}"
        );
    }
    for id in calls {
        assert_eq!(
            messages
                .iter()
                .filter(|m| m.get("tool_calls").is_some_and(|calls| {
                    calls
                        .as_array()
                        .is_some_and(|calls| calls.iter().any(|call| call["id"] == *id))
                }))
                .count(),
            1,
            "call {id} emitted once"
        );
        assert_eq!(
            messages.iter().filter(|m| m["tool_call_id"] == *id).count(),
            1,
            "result {id} emitted once"
        );
    }
}

#[test]
fn stalled_session_notice_between_call_and_result_stays_paired() {
    // Worker-3's stalled session: every call is in the durable log, but a
    // system notice sits between the second call and its result. Converting
    // for chat completions must not fail; each result directly follows its
    // call and the notice moves after the results.
    let model = model();
    let mut req = request(&model);
    req.tools = stalled_session_tools();
    req.messages.extend(stalled_session_messages());
    let body = request_json(&req, "openrouter", &model).unwrap();
    let messages = body["messages"].as_array().unwrap();
    assert_results_follow_calls(
        messages,
        &[
            "call_ZpXtECGcYFPLx8p8AqOJVFGn",
            "call_rqd1dzkZ3kl2nd118QaUdWYW",
        ],
        "Your computer was evicted",
    );
}

#[test]
fn user_prompt_between_call_and_result_stays_paired() {
    // Mirror case: the next task's user prompt was appended while a call was
    // still in flight, so it sits between the call and its result.
    let model = model();
    let mut req = request(&model);
    req.tools = stalled_session_tools();
    let mut history = stalled_session_messages();
    let notice = history.remove(3);
    assert!(notice.parts.iter().any(
        |part| matches!(part, Part::Text { text } if text.contains("Your computer was evicted"))
    ));
    history.insert(
        3,
        message(
            MessageRole::User,
            vec![text("Continue with the next step while that runs.")],
        ),
    );
    req.messages.extend(history);
    let body = request_json(&req, "openrouter", &model).unwrap();
    let messages = body["messages"].as_array().unwrap();
    assert_results_follow_calls(
        messages,
        &[
            "call_ZpXtECGcYFPLx8p8AqOJVFGn",
            "call_rqd1dzkZ3kl2nd118QaUdWYW",
        ],
        "Continue with the next step",
    );
}

#[tokio::test]
async fn errors_in_bodies_and_streams_are_classified_without_retrying_streams() {
    for (status, body, mime, overflow) in [
        (
            200,
            "data: {\"error\":{\"message\":\"upstream unavailable\",\"code\":503}}\n\n",
            "text/event-stream",
            false,
        ),
        (
            200,
            "{\"error\":{\"message\":\"upstream unavailable\"}}",
            "application/json",
            false,
        ),
        (
            200,
            "{\"error\":{\"message\":\"upstream unavailable\"}}",
            "text/plain",
            false,
        ),
        (
            400,
            "{\"error\":{\"message\":\"too many tokens\",\"code\":\"context_length_exceeded\"}}",
            "application/json",
            true,
        ),
        (
            500,
            "{\"error\":{\"message\":\"Maximum context length exceeded\"}}",
            "application/json",
            true,
        ),
        (400, "input exceeds the context window", "text/plain", true),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(status).set_body_raw(body, mime))
            .expect(1)
            .mount(&server)
            .await;
        let provider = CompletionsProvider::new(
            &provider_info(&server),
            &model(),
            ClientAuth::Bearer("fixture-key".into()),
        )
        .unwrap();
        let error = provider
            .request(request(&model()))
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        if overflow {
            assert!(matches!(error, Error::ContextOverflow(_)), "{error}");
        } else {
            assert!(
                matches!(error, Error::Protocol(ref message) if message.contains("upstream unavailable")),
                "{error}"
            );
        }
    }
}

#[tokio::test]
async fn transient_statuses_retry_and_exhaustion_preserves_the_provider_message() {
    for status in [408, 409, 429, 500, 503] {
        let server = MockServer::start().await;
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = attempts.clone();
        Mock::given(method("POST"))
            .respond_with(move |_: &wiremock::Request| {
                if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                    ResponseTemplate::new(status)
                        .insert_header("Retry-After", "0")
                        .set_body_json(json!({"error": {"message": "try again"}}))
                } else {
                    ResponseTemplate::new(200).set_body_raw(text_stream(), "text/event-stream")
                }
            })
            .mount(&server)
            .await;
        let mut provider = CompletionsProvider::new(
            &provider_info(&server),
            &model(),
            ClientAuth::ApiKey("fixture-key".into()),
        )
        .unwrap();
        provider.retry_policy = RetryPolicy {
            max_attempts: 2,
            initial_delay: Duration::from_secs(30),
            max_delay: Duration::from_millis(1),
        };
        let deltas: Vec<_> = provider
            .request(request(&model()))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(completed(&deltas).parts, vec![text("Hello 🌍")]);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(429)
                .set_body_json(json!({"error": {"message": "quota exhausted"}})),
        )
        .expect(2)
        .mount(&server)
        .await;
    let mut provider = CompletionsProvider::new(
        &provider_info(&server),
        &model(),
        ClientAuth::ApiKey("fixture-key".into()),
    )
    .unwrap();
    provider.retry_policy = RetryPolicy {
        max_attempts: 2,
        initial_delay: Duration::ZERO,
        max_delay: Duration::ZERO,
    };
    let error = provider
        .request(request(&model()))
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::ProviderResponse {
            status: reqwest::StatusCode::TOO_MANY_REQUESTS,
            ..
        }
    ));
}

#[test]
fn sse_handles_every_byte_boundary_newlines_comments_and_late_usage() {
    let fixture = format!(": keepalive\n\n{}", text_stream());
    for newline in ["\n", "\r\n", "\r"] {
        let fixture = fixture.replace('\n', newline);
        for size in [1, 2, 7, 31, fixture.len()] {
            let mut parser = SseParser::new("openrouter", MODEL);
            let mut deltas = Vec::new();
            for bytes in fixture.as_bytes().chunks(size) {
                deltas.extend(parser.push(bytes).unwrap());
            }
            parser.finish().unwrap();
            assert_eq!(completed(&deltas).parts, vec![text("Hello 🌍")]);
            assert_eq!(completed(&deltas).usage.input_tokens, 21);
        }
    }
}

#[test]
fn streamed_reasoning_details_merge_and_indices_do_not_collide_with_tools() {
    let fixture = sse(&[
        chunk(
            json!({"reasoning": "Think", "reasoning_details": [{"type": "reasoning.text", "index": 0, "text": "Th"}]}),
            Value::Null,
        ),
        chunk(
            json!({"reasoning_details": [{"type": "reasoning.text", "index": 0, "text": "ink", "signature": "signed", "id": "r0"}]}),
            Value::Null,
        ),
        chunk(
            json!({"reasoning_details": [{"type": "reasoning.encrypted", "data": "opaque"}], "content": "Ready"}),
            Value::Null,
        ),
        chunk(
            json!({"tool_calls": [{"index": 0, "id": "t", "function": {"name": "read", "arguments": "{}"}}]}),
            json!("tool_calls"),
        ),
    ]);
    let mut parser = SseParser::new("openrouter", MODEL);
    let deltas = parser.push(fixture.as_bytes()).unwrap();
    parser.finish().unwrap();
    assert!(
        matches!(&completed(&deltas).parts[0], Part::Reasoning { metadata, .. }
        if metadata["openrouter"] == json!([{"type": "reasoning.text", "index": 0, "text": "Think", "signature": "signed", "id": "r0"}, {"type": "reasoning.encrypted", "data": "opaque"}]))
    );
    assert!(matches!(
        &deltas[0],
        Delta::Reasoning {
            output_index: 0,
            ..
        }
    ));
    assert!(matches!(
        &deltas[1],
        Delta::Text {
            output_index: 1,
            ..
        }
    ));
    assert!(matches!(
        &deltas[2],
        Delta::ToolArguments {
            output_index: 2,
            ..
        }
    ));
}

#[tokio::test]
async fn stream_errors_end_after_partial_output_without_completed_or_retry() {
    let server = MockServer::start().await;
    let body = format!(
        "data: {}\n\ndata: {{\"error\":{{\"message\":\"lost upstream\"}}}}\n\n",
        chunk(json!({"content": "partial"}), Value::Null)
    );
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "text/event-stream"))
        .expect(1)
        .mount(&server)
        .await;
    let provider = CompletionsProvider::new(
        &provider_info(&server),
        &model(),
        ClientAuth::ApiKey("fixture-key".into()),
    )
    .unwrap();
    let deltas: Vec<_> = provider.request(request(&model())).collect().await;
    assert!(
        matches!(deltas.last(), Some(Err(Error::Protocol(message))) if message == "lost upstream")
    );
    assert!(
        !deltas
            .iter()
            .any(|delta| matches!(delta, Ok(Delta::Completed(_))))
    );
}

#[test]
fn finish_reasons_truncation_and_malformed_tool_arguments() {
    for (wire, expected) in [
        ("stop", StopReason::EndTurn),
        ("length", StopReason::MaxOutputTokens),
        ("tool_calls", StopReason::ToolCalls),
        ("content_filter", StopReason::ContentFilter),
    ] {
        let mut parser = SseParser::new("openrouter", MODEL);
        assert_eq!(
            completed(
                &parser
                    .push(sse(&[chunk(json!({}), json!(wire))]).as_bytes())
                    .unwrap()
            )
            .stop_reason,
            expected
        );
    }
    assert!(
        SseParser::new("openrouter", MODEL)
            .push(b"data: [DONE]\n\n")
            .is_err()
    );
    let mut parser = SseParser::new("openrouter", MODEL);
    parser
        .push(format!("data: {}\n\n", chunk(json!({}), json!("stop"))).as_bytes())
        .unwrap();
    assert!(parser.finish().is_err());
    let malformed = sse(&[chunk(
        json!({"tool_calls": [{"index": 0, "id": "a", "function": {"name": "read", "arguments": "{"}}]}),
        json!("tool_calls"),
    )]);
    assert!(matches!(
        SseParser::new("openrouter", MODEL).push(malformed.as_bytes()),
        Err(Error::Protocol(_))
    ));
}

#[test]
fn openrouter_catalog_selects_messages_for_every_anthropic_model() {
    let router = Catalog::get().provider("openrouter").unwrap();
    assert!(!router.models.is_empty());
    for model in router.models.values() {
        let expected = if model.id.starts_with("anthropic/") {
            Api::AnthropicMessages
        } else {
            Api::OpenAiCompletions
        };
        assert_eq!(model.api.unwrap_or(router.api), expected, "{}", model.id);
    }
}

#[test]
fn reasoning_without_replay_details_is_kept_as_text() {
    let fixture = sse(&[chunk(json!({"reasoning": "Think first."}), json!("stop"))]);
    let deltas = SseParser::new("openrouter", MODEL)
        .push(fixture.as_bytes())
        .unwrap();
    let model = model();
    let mut request = request(&model);
    request.messages.push(message(
        MessageRole::Assistant,
        completed(&deltas).parts.clone(),
    ));
    let body = request_json(&request, "openrouter", &model).unwrap();
    assert_eq!(body["messages"][2]["content"], "Think first.");
    assert!(body["messages"][2].get("reasoning_details").is_none());
}

#[test]
fn image_request_body() {
    let model = Catalog::get()
        .provider("openrouter")
        .unwrap()
        .models
        .values()
        .next()
        .unwrap();
    let mut req = request(model);
    req.messages = vec![message(
        MessageRole::User,
        vec![Part::Image {
            media_type: "image/png".into(),
            bytes: vec![1, 2, 3],
            object_key: None,
            detail: Some("high".into()),
        }],
    )];
    let body = request_json(&req, "openrouter", model).unwrap();
    assert_eq!(
        body["messages"][1]["content"][0],
        json!({"type":"image_url", "image_url":{"url":"data:image/png;base64,AQID", "detail":"high"}})
    );
}
