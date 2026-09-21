use std::{
    collections::BTreeMap,
    fmt::Write as _,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use futures::{TryStreamExt, future::BoxFuture};
use serde_json::{Value, json};
use swarmy_core::{Message, MessageId, MessageRole, Part, ToolCallId, ToolResult};
use swarmy_llm::{
    BearerSource, ClientAuth, Delta, Error, GenerationSettings, Provider, ReasoningEffort, Request,
    Response, StopReason, TokenUsage, ToolDefinition,
    api::anthropic::{AnthropicProvider, Endpoint, SseParser, request_json},
    catalog::{Catalog, ModelInfo},
    client_for,
    retry::{RetryPolicy, with_retry},
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path},
};

fn model(id: &str) -> ModelInfo {
    Catalog::get().model("anthropic", id).unwrap().clone()
}

fn direct() -> Endpoint {
    Endpoint::Direct {
        api_key: "fixture-key".into(),
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

fn request(id: &str) -> Request {
    Request {
        system_prompt: "Be helpful.".into(),
        messages: vec![message(MessageRole::User, vec![text("Hello")])],
        tools: vec![ToolDefinition {
            name: "clock".into(),
            description: "Read time".into(),
            parameters: json!({"type": "object", "properties": {"zone": {"type": "string"}}}),
        }],
        settings: GenerationSettings {
            model: id.into(),
            max_output_tokens: Some(1000),
            temperature: Some(0.5),
            reasoning_effort: Some(ReasoningEffort::Low),
        },
    }
}

fn sse(events: &[Value]) -> String {
    let mut output = String::new();
    for event in events {
        write!(
            output,
            "event: {}\ndata: {event}\n\n",
            event["type"].as_str().unwrap()
        )
        .unwrap();
    }
    output
}

fn turn(block: &Value, deltas: Vec<Value>, reason: &str) -> String {
    let mut events = vec![
        json!({"type": "message_start", "message": {"usage": {"input_tokens": 10,
            "cache_read_input_tokens": 20, "cache_creation_input_tokens": 5, "output_tokens": 1}}}),
        json!({"type": "content_block_start", "index": 0, "content_block": block}),
    ];
    events.extend(
        deltas
            .into_iter()
            .map(|delta| json!({"type": "content_block_delta", "index": 0, "delta": delta})),
    );
    events.extend([
        json!({"type": "content_block_stop", "index": 0}),
        json!({"type": "message_delta", "delta": {"stop_reason": reason}, "usage": {"output_tokens": 8}}),
        json!({"type": "message_stop"}),
    ]);
    sse(&events)
}

fn text_turn(reason: &str) -> String {
    turn(
        &json!({"type": "text", "text": ""}),
        vec![json!({"type": "text_delta", "text": "Hello 🦀"})],
        reason,
    )
}

fn response(deltas: &[Delta]) -> &Response {
    let Some(Delta::Completed(response)) = deltas.last() else {
        panic!("missing completion")
    };
    assert_eq!(
        deltas
            .iter()
            .filter(|delta| matches!(delta, Delta::Completed(_)))
            .count(),
        1
    );
    response
}

async fn mount(server: &MockServer, body: String, expected_path: &str) {
    Mock::given(method("POST"))
        .and(path(expected_path))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(body),
        )
        .mount(server)
        .await;
}

async fn send(server: &MockServer, request: Request) -> Result<Vec<Delta>, Error> {
    AnthropicProvider::with_base(model(&request.settings.model), direct(), &server.uri())
        .unwrap()
        .request(request)
        .try_collect()
        .await
}

#[tokio::test]
async fn text_usage_cache_and_direct_headers() {
    let server = MockServer::start().await;
    mount(&server, text_turn("end_turn"), "/v1/messages").await;
    let deltas = send(&server, request("claude-sonnet-4-5")).await.unwrap();
    assert_eq!(
        deltas[0],
        Delta::Text {
            output_index: 0,
            text: "Hello 🦀".into()
        }
    );
    let result = response(&deltas);
    assert_eq!(result.parts, vec![text("Hello 🦀")]);
    assert_eq!(result.stop_reason, StopReason::EndTurn);
    assert_eq!(
        result.usage,
        TokenUsage {
            input_tokens: 35,
            cached_input_tokens: 20,
            cache_write_input_tokens: 5,
            output_tokens: 8,
            total_tokens: 43,
            reasoning_output_tokens: 0
        }
    );
    let requests = server.received_requests().await.unwrap();
    let headers = &requests[0].headers;
    assert_eq!(headers["x-api-key"], "fixture-key");
    assert_eq!(headers["anthropic-version"], "2023-06-01");
    assert_eq!(
        headers["user-agent"],
        concat!("swarmy/", env!("CARGO_PKG_VERSION"))
    );
    assert_eq!(
        headers["anthropic-beta"],
        "interleaved-thinking-2025-05-14,fine-grained-tool-streaming-2025-05-14"
    );
    for forbidden in ["authorization", "x-app", "originator", "chatgpt-account-id"] {
        assert!(!headers.contains_key(forbidden));
    }
    let body: Value = requests[0].body_json().unwrap();
    assert_eq!(body["system"][0]["text"], "Be helpful.");
}

#[tokio::test]
async fn tool_arguments_are_parsed_only_when_block_stops() {
    let server = MockServer::start().await;
    mount(
        &server,
        turn(
            &json!({"type": "tool_use", "id": "toolu_1", "name": "clock", "input": {}}),
            vec![
                json!({"type": "input_json_delta", "partial_json": "{\"zone\":"}),
                json!({"type": "input_json_delta", "partial_json": "\"UTC\"}"}),
            ],
            "tool_use",
        ),
        "/v1/messages",
    )
    .await;
    let deltas = send(&server, request("claude-sonnet-4-5")).await.unwrap();
    let call = Part::ToolCall {
        call_id: ToolCallId("toolu_1".into()),
        tool: "clock".into(),
        input: json!({"zone": "UTC"}),
    };
    assert_eq!(
        deltas[0],
        Delta::ToolArguments {
            output_index: 0,
            arguments: "{\"zone\":".into()
        }
    );
    assert_eq!(
        deltas[2],
        Delta::PartDone {
            output_index: 0,
            part: call.clone()
        }
    );
    assert_eq!(response(&deltas).parts, vec![call]);
    assert_eq!(response(&deltas).stop_reason, StopReason::ToolCalls);
}

#[tokio::test]
async fn thinking_signature_round_trips_and_foreign_history_becomes_text() {
    let server = MockServer::start().await;
    mount(
        &server,
        turn(
            &json!({"type": "thinking", "thinking": ""}),
            vec![
                json!({"type": "thinking_delta", "thinking": "Consider time."}),
                json!({"type": "signature_delta", "signature": "opaque-"}),
                json!({"type": "signature_delta", "signature": "signature"}),
            ],
            "end_turn",
        ),
        "/v1/messages",
    )
    .await;
    let deltas = send(&server, request("claude-sonnet-4-5")).await.unwrap();
    assert_eq!(
        deltas[0],
        Delta::Reasoning {
            output_index: 0,
            text: "Consider time.".into()
        }
    );
    let part = response(&deltas).parts[0].clone();
    assert!(matches!(&deltas[1], Delta::PartDone { part: done, .. } if done == &part));
    let mut next = request("claude-sonnet-4-5");
    next.messages
        .push(message(MessageRole::Assistant, vec![part]));
    next.messages
        .push(message(MessageRole::User, vec![text("Continue")]));
    send(&server, next.clone()).await.unwrap();
    let bodies = server.received_requests().await.unwrap();
    let body: Value = bodies[1].body_json().unwrap();
    assert_eq!(
        body["messages"][1]["content"][0],
        json!({"type": "thinking", "thinking": "Consider time.", "signature": "opaque-signature"})
    );
    next.settings.model = "claude-opus-4-8".into();
    send(&server, next.clone()).await.unwrap();
    let bodies = server.received_requests().await.unwrap();
    let body: Value = bodies[2].body_json().unwrap();
    assert_eq!(
        body["messages"][1]["content"][0],
        json!({"type": "text", "text": "Consider time."})
    );
    next.settings.model = "claude-sonnet-4-5".into();
    let router = Endpoint::OpenRouter {
        api_key: "key".into(),
    };
    let body = request_json(&next, &model("claude-sonnet-4-5"), &router).unwrap();
    assert_eq!(body["messages"][1]["content"][0]["type"], "text");
}

#[test]
fn adaptive_and_budget_request_snapshots() {
    for (id, fixture) in [
        (
            "claude-opus-4-8",
            include_str!("fixtures/anthropic-adaptive.json"),
        ),
        (
            "claude-sonnet-4-5",
            include_str!("fixtures/anthropic-budget.json"),
        ),
    ] {
        let body = request_json(&request(id), &model(id), &direct()).unwrap();
        assert_eq!(body, serde_json::from_str::<Value>(fixture).unwrap());
    }
}

#[test]
fn efforts_limits_and_temperature_follow_catalog() {
    let id = "claude-sonnet-4-5";
    let mut request = request(id);
    let mut model = model(id);
    for (effort, budget) in [
        (ReasoningEffort::Minimal, 1024),
        (ReasoningEffort::Low, 2048),
        (ReasoningEffort::Medium, 8192),
        (ReasoningEffort::High, 16384),
        (ReasoningEffort::Xhigh, 16384),
        (ReasoningEffort::Max, 16384),
    ] {
        request.settings.reasoning_effort = Some(effort);
        let body = request_json(&request, &model, &direct()).unwrap();
        assert_eq!(body["thinking"]["budget_tokens"], budget);
        assert_eq!(body["max_tokens"], 1000 + budget);
        assert!(body.get("temperature").is_none());
    }
    request.settings.max_output_tokens = model.limit.output;
    assert_eq!(
        request_json(&request, &model, &direct()).unwrap()["max_tokens"],
        model.limit.output.unwrap()
    );
    request.settings.reasoning_effort = Some(ReasoningEffort::None);
    let body = request_json(&request, &model, &direct()).unwrap();
    assert_eq!(body["thinking"], json!({"type": "disabled"}));
    assert_eq!(body["temperature"], 0.5);
    model
        .compat
        .0
        .insert("supports_temperature".into(), json!(false));
    assert!(
        request_json(&request, &model, &direct())
            .unwrap()
            .get("temperature")
            .is_none()
    );
}

#[test]
fn history_normalizes_calls_repairs_orphans_and_keeps_results_before_notices() {
    let mut request = request("claude-sonnet-4-5");
    let call = |id: &str| Part::ToolCall {
        call_id: ToolCallId(id.into()),
        tool: "clock".into(),
        input: json!({}),
    };
    request.messages.extend([
        message(
            MessageRole::Assistant,
            vec![call("foreign/id|1"), call("missing")],
        ),
        message(MessageRole::System, vec![text("Computer rebuilt.")]),
        message(
            MessageRole::Tool,
            vec![Part::ToolResult {
                call_id: ToolCallId("foreign/id|1".into()),
                result: ToolResult::Error {
                    error: "Interrupted".into(),
                },
            }],
        ),
    ]);
    let body = request_json(&request, &model("claude-sonnet-4-5"), &direct()).unwrap();
    let id = body["messages"][1]["content"][0]["id"].as_str().unwrap();
    assert!(
        id.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    );
    let results = &body["messages"][2]["content"];
    assert_eq!(results[0]["tool_use_id"], id);
    assert_eq!(results[0]["is_error"], true);
    assert_eq!(results[1]["tool_use_id"], "missing");
    assert_eq!(results[1]["is_error"], true);
    assert_eq!(results[2]["text"], "Computer rebuilt.");
    assert_eq!(results[2]["cache_control"], json!({"type": "ephemeral"}));
}

struct FixtureToken(AtomicUsize);
impl BearerSource for FixtureToken {
    fn token(&self) -> BoxFuture<'_, Result<String, Error>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok("vertex-token".into()) })
    }
}

#[tokio::test]
async fn vertex_dispatch_uses_resource_path_and_body_version() {
    let server = MockServer::start().await;
    let source = Arc::new(FixtureToken(AtomicUsize::new(0)));
    let mut provider = Catalog::get()
        .provider("google-vertex-anthropic")
        .unwrap()
        .clone();
    provider.base_url = server.uri();
    let model = model("claude-sonnet-4-5");
    mount(&server, text_turn("end_turn"), "/v1/projects/project-id/locations/us-central1/publishers/anthropic/models/claude-sonnet-4-5:streamRawPredict").await;
    let client = client_for(
        &provider,
        &model,
        ClientAuth::Vertex {
            project: "project-id".into(),
            location: "us-central1".into(),
            source: source.clone(),
        },
    )
    .unwrap();
    client
        .request(request(&model.id))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests[0].headers["authorization"], "Bearer vertex-token");
    assert!(!requests[0].headers.contains_key("x-api-key"));
    assert!(!requests[0].headers.contains_key("anthropic-version"));
    let body: Value = requests[0].body_json().unwrap();
    assert_eq!(body["anthropic_version"], "vertex-2023-10-16");
    assert!(body.get("model").is_none());
    assert_eq!(source.0.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn openrouter_catalog_override_uses_bearer_and_messages_path() {
    let server = MockServer::start().await;
    let provider = Catalog::get().provider("openrouter").unwrap();
    let mut model = provider.models["anthropic/claude-sonnet-4.5"].clone();
    model.base_url = Some(format!("{}/api", server.uri()));
    Mock::given(method("POST"))
        .and(path("/api/v1/messages"))
        .and(header("authorization", "Bearer router-key"))
        .respond_with(ResponseTemplate::new(200).set_body_string(text_turn("end_turn")))
        .expect(1)
        .mount(&server)
        .await;
    client_for(provider, &model, ClientAuth::ApiKey("router-key".into()))
        .unwrap()
        .request(request(&model.id))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let requests = server.received_requests().await.unwrap();
    assert!(!requests[0].headers.contains_key("x-api-key"));
    assert_eq!(requests[0].body_json::<Value>().unwrap()["model"], model.id);
}

#[tokio::test]
async fn transient_statuses_retry_and_stop_after_three_attempts() {
    for status in [408, 409, 429, 500, 529] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(status).insert_header("retry-after", "0"))
            .up_to_n_times(1)
            .with_priority(1)
            .expect(1)
            .mount(&server)
            .await;
        mount(&server, text_turn("end_turn"), "/v1/messages").await;
        send(&server, request("claude-sonnet-4-5")).await.unwrap();
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(529).insert_header("retry-after", "0"))
        .expect(3)
        .mount(&server)
        .await;
    assert!(matches!(
        send(&server, request("claude-sonnet-4-5")).await,
        Err(Error::Retryable { .. })
    ));
}

#[tokio::test]
async fn retry_after_http_date_is_parsed() {
    let server = MockServer::start().await;
    let date = httpdate::fmt_http_date(SystemTime::UNIX_EPOCH);
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(529).insert_header("retry-after", date.as_str()))
        .expect(3)
        .mount(&server)
        .await;
    assert!(matches!(send(&server, request("claude-sonnet-4-5")).await,
        Err(Error::Retryable { retry_after: Some(delay), .. }) if delay == Duration::ZERO));
}

#[tokio::test]
async fn context_overflow_and_other_client_errors_do_not_retry() {
    for (status, body, overflow) in [
        (400, "prompt is too long: 300000 tokens", true),
        (413, "request_too_large", true),
        (401, "invalid key", false),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(status).set_body_string(body))
            .expect(1)
            .mount(&server)
            .await;
        let error = send(&server, request("claude-sonnet-4-5"))
            .await
            .unwrap_err();
        if overflow {
            assert!(matches!(error, Error::ContextOverflow(_)));
        } else {
            // The provider's own text is kept for non-retryable failures.
            assert!(matches!(&error, Error::Protocol(message) if message.contains("invalid key")));
        }
    }
}

#[test]
fn sse_handles_every_byte_boundary_and_all_stop_reasons() {
    for (reason, expected) in [
        ("end_turn", StopReason::EndTurn),
        ("stop_sequence", StopReason::EndTurn),
        ("tool_use", StopReason::ToolCalls),
        ("max_tokens", StopReason::MaxOutputTokens),
        ("refusal", StopReason::ContentFilter),
        ("pause_turn", StopReason::Incomplete("pause_turn".into())),
    ] {
        let fixture = format!(
            ": comment\r\n\r\n{}",
            text_turn(reason).replace('\n', "\r\n")
        );
        for size in 1..=fixture.len() {
            let mut parser = SseParser::new("claude-sonnet-4-5", "anthropic");
            let mut deltas = Vec::new();
            for bytes in fixture.as_bytes().chunks(size) {
                deltas.extend(parser.push(bytes).unwrap());
            }
            parser.finish().unwrap();
            assert_eq!(response(&deltas).stop_reason, expected);
            assert_eq!(response(&deltas).parts, vec![text("Hello 🦀")]);
        }
    }
}

#[tokio::test]
async fn truncated_and_error_streams_fail_without_retry() {
    for body in [
        text_turn("end_turn").replace(
            "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
            "",
        ),
        sse(&[
            json!({"type": "error", "error": {"type": "overloaded_error", "message": "overloaded"}}),
        ]),
        turn(
            &json!({"type": "tool_use", "id": "a", "name": "clock", "input": {}}),
            vec![json!({"type": "input_json_delta", "partial_json": "{"})],
            "tool_use",
        ),
    ] {
        let server = MockServer::start().await;
        mount(&server, body, "/v1/messages").await;
        assert!(send(&server, request("claude-sonnet-4-5")).await.is_err());
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }
}

#[test]
fn usage_from_older_json_defaults_cache_writes_to_zero() {
    let usage: TokenUsage =
        serde_json::from_value(json!({"input_tokens": 10, "cached_input_tokens": 2,
        "output_tokens": 3, "reasoning_output_tokens": 0, "total_tokens": 13}))
        .unwrap();
    assert_eq!(usage.cache_write_input_tokens, 0);
}

#[tokio::test]
async fn retry_policy_honors_server_delay_and_backoff_cap() {
    let policy = RetryPolicy {
        initial_delay: Duration::ZERO,
        max_delay: Duration::from_millis(20),
        max_attempts: 3,
    };
    let attempts = AtomicUsize::new(0);
    let start = tokio::time::Instant::now();
    let result = with_retry(&policy, || async {
        if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(Error::Retryable {
                status: reqwest::StatusCode::TOO_MANY_REQUESTS,
                retry_after: Some(Duration::from_secs(10)),
            })
        } else {
            Ok(())
        }
    })
    .await;
    result.unwrap();
    assert!(start.elapsed() >= policy.max_delay);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[test]
fn unsigned_and_foreign_protocol_reasoning_is_plain_text() {
    for metadata in [
        BTreeMap::new(),
        BTreeMap::from([("chatgpt".into(), json!({"signature": "foreign"}))]),
    ] {
        let mut request = request("claude-sonnet-4-5");
        request.messages.push(message(
            MessageRole::Assistant,
            vec![Part::Reasoning {
                text: "Thought".into(),
                metadata,
            }],
        ));
        let body = request_json(&request, &model("claude-sonnet-4-5"), &direct()).unwrap();
        assert_eq!(
            body["messages"][1]["content"][0],
            json!({"type": "text", "text": "Thought"})
        );
    }
}

#[test]
fn adaptive_efforts_clamp_and_preserve_supported_levels() {
    let id = "claude-opus-4-8";
    let mut request = request(id);
    for (requested, expected) in [
        (ReasoningEffort::Minimal, "low"),
        (ReasoningEffort::Low, "low"),
        (ReasoningEffort::Medium, "medium"),
        (ReasoningEffort::High, "high"),
        (ReasoningEffort::Xhigh, "xhigh"),
        (ReasoningEffort::Max, "max"),
    ] {
        request.settings.reasoning_effort = Some(requested);
        let body = request_json(&request, &model(id), &direct()).unwrap();
        assert_eq!(
            body["thinking"],
            json!({"type": "adaptive", "display": "summarized"})
        );
        assert_eq!(body["output_config"]["effort"], expected);
        assert_eq!(body["max_tokens"], 1000);
        assert!(body.get("temperature").is_none());
    }
}

#[test]
fn multiple_blocks_keep_indices_and_merge_final_usage() {
    let events = [
        json!({"type": "message_start", "message": {"usage": {"input_tokens": 10,
            "cache_read_input_tokens": 20, "cache_creation_input_tokens": 5, "output_tokens": 1}}}),
        json!({"type": "content_block_start", "index": 1, "content_block": {"type": "text", "text": "Answer"}}),
        json!({"type": "content_block_start", "index": 0, "content_block": {"type": "thinking", "thinking": "Thought", "signature": "signed"}}),
        json!({"type": "content_block_stop", "index": 1}),
        json!({"type": "content_block_stop", "index": 0}),
        json!({"type": "message_delta", "delta": {"stop_reason": "end_turn"},
            "usage": {"input_tokens": 15, "cache_creation_input_tokens": 10, "output_tokens": 9}}),
        json!({"type": "message_stop"}),
        json!({"type": "message_stop"}),
    ];
    let mut parser = SseParser::new("claude-sonnet-4-5", "anthropic");
    let deltas = parser.push(sse(&events).as_bytes()).unwrap();
    parser.finish().unwrap();
    assert_eq!(
        deltas[0],
        Delta::Text {
            output_index: 1,
            text: "Answer".into()
        }
    );
    assert_eq!(
        deltas[1],
        Delta::Reasoning {
            output_index: 0,
            text: "Thought".into()
        }
    );
    let result = response(&deltas);
    assert!(matches!(&result.parts[0], Part::Reasoning { text, .. } if text == "Thought"));
    assert_eq!(result.parts[1], text("Answer"));
    assert_eq!(result.usage.input_tokens, 45);
    assert_eq!(result.usage.cache_write_input_tokens, 10);
    assert_eq!(result.usage.cached_input_tokens, 20);
    assert_eq!(result.usage.output_tokens, 9);
    assert_eq!(result.usage.total_tokens, 54);
}

#[test]
fn terminal_orphan_gets_error_and_completed_result_preserves_output() {
    let mut request = request("claude-sonnet-4-5");
    request.messages.push(message(
        MessageRole::Assistant,
        vec![Part::ToolCall {
            call_id: ToolCallId("call_1".into()),
            tool: "clock".into(),
            input: json!({}),
        }],
    ));
    let body = request_json(&request, &model("claude-sonnet-4-5"), &direct()).unwrap();
    assert_eq!(body["messages"][2]["content"][0]["is_error"], true);
    request.messages.push(message(
        MessageRole::Tool,
        vec![Part::ToolResult {
            call_id: ToolCallId("call_1".into()),
            result: ToolResult::Completed {
                output: "12:00".into(),
                title: "Time".into(),
                metadata: BTreeMap::new(),
            },
        }],
    ));
    let body = request_json(&request, &model("claude-sonnet-4-5"), &direct()).unwrap();
    assert_eq!(body["messages"][2]["content"].as_array().unwrap().len(), 1);
    assert_eq!(body["messages"][2]["content"][0]["content"], "12:00");
    assert_eq!(body["messages"][2]["content"][0]["is_error"], false);
}

#[test]
fn token_limit_uses_catalog_default_and_allows_unknown_model_limit() {
    let mut model = model("claude-sonnet-4-5");
    let mut request = request(&model.id);
    request.settings.max_output_tokens = None;
    assert_eq!(
        request_json(&request, &model, &direct()).unwrap()["max_tokens"],
        model.limit.output.unwrap()
    );
    model.limit.output = None;
    assert!(request_json(&request, &model, &direct()).is_err());
    request.settings.max_output_tokens = Some(1000);
    assert_eq!(
        request_json(&request, &model, &direct()).unwrap()["max_tokens"],
        3048
    );
}
