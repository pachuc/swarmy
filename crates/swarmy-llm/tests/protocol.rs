use futures::TryStreamExt;
use serde_json::{Value, json};
use std::{collections::BTreeMap, time::Duration};
use swarmy_core::{Message, MessageId, MessageRole, Part, ToolCallId, ToolResult};
use swarmy_llm::{
    Delta, GenerationSettings, Provider, ReasoningEffort, Request, Response, StopReason,
    TokenUsage, ToolDefinition,
    fake::FakeProvider,
    responses::{SseParser, request_json},
};

fn request() -> Request {
    Request {
        system_prompt: "Be helpful.".into(),
        messages: vec![],
        tools: vec![],
        settings: GenerationSettings {
            model: "fixture-model".into(),
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

fn text(text: &str) -> Part {
    Part::Text { text: text.into() }
}
fn call() -> Part {
    Part::ToolCall {
        call_id: ToolCallId("call_1".into()),
        tool: "get_time".into(),
        input: json!({"zone": "UTC"}),
    }
}
fn reasoning() -> Part {
    Part::Reasoning {
        text: "Think first.".into(),
        metadata: BTreeMap::from([(
            "chatgpt".into(),
            json!({"type": "reasoning", "id": "rs_1", "summary": [{"type": "summary_text", "text": "Think first."}], "encrypted_content": "opaque-reasoning"}),
        )]),
    }
}

#[test]
fn shared_request_matches_responses_fixture() {
    let mut request = request();
    request.messages = vec![
        message(MessageRole::User, vec![text("Hello")]),
        message(
            MessageRole::Assistant,
            vec![reasoning(), text("Checking."), call()],
        ),
        message(
            MessageRole::Tool,
            vec![
                Part::ToolResult {
                    call_id: ToolCallId("call_1".into()),
                    result: ToolResult::Completed {
                        output: "12:00".into(),
                        title: "Time".into(),
                        metadata: BTreeMap::new(),
                    },
                },
                Part::ToolResult {
                    call_id: ToolCallId("call_2".into()),
                    result: ToolResult::Error {
                        error: "unavailable".into(),
                    },
                },
            ],
        ),
    ];
    request.tools = vec![ToolDefinition {
        name: "get_time".into(),
        description: "Get time".into(),
        parameters: json!({"type": "object", "properties": {"zone": {"type": "string"}}, "required": ["zone"]}),
    }];
    request.settings.reasoning_effort = Some(ReasoningEffort::Low);
    request.settings.max_output_tokens = Some(100);
    request.settings.temperature = Some(0.5);
    let expected: Value = serde_json::from_str(include_str!("fixtures/request.json")).unwrap();
    assert_eq!(request_json(&request).unwrap(), expected);
}

#[test]
fn rebuild_notice_preserves_call_result_order_with_developer_role() {
    let mut request = request();
    request.messages = vec![
        message(MessageRole::Assistant, vec![call()]),
        message(
            MessageRole::System,
            vec![text("Computer rebuilt; processes were lost.")],
        ),
        message(
            MessageRole::Tool,
            vec![Part::ToolResult {
                call_id: ToolCallId("call_1".into()),
                result: ToolResult::Error {
                    error: "Interrupted by node loss".into(),
                },
            }],
        ),
    ];
    let value = request_json(&request).unwrap();
    assert_eq!(value["input"][0]["type"], "function_call");
    assert_eq!(
        value["input"][1],
        json!({"type":"message", "role":"developer", "content":[{"type":"input_text", "text":"Computer rebuilt; processes were lost."}]})
    );
    assert_eq!(value["input"][2]["type"], "function_call_output");
    assert_eq!(value["input"][2]["call_id"], "call_1");
    assert_eq!(
        value["input"][2]["output"],
        r#"{"error":"Interrupted by node loss"}"#
    );
}

fn parse(fixture: &str, chunk_size: usize) -> Vec<Delta> {
    let mut parser = SseParser::default();
    let mut deltas = vec![];
    for chunk in fixture.as_bytes().chunks(chunk_size) {
        deltas.extend(parser.push(chunk).unwrap());
    }
    parser.finish().unwrap();
    deltas
}

fn completed(parts: Vec<Part>, stop_reason: StopReason) -> Delta {
    Delta::Completed(Response {
        parts,
        stop_reason,
        usage: TokenUsage {
            input_tokens: 10,
            cached_input_tokens: 4,
            output_tokens: 3,
            reasoning_output_tokens: 2,
            total_tokens: 13,
            cache_write_input_tokens: 0,
        },
        quota_remaining: std::collections::BTreeMap::new(),
        quota_resets: std::collections::BTreeMap::new(),
    })
}

#[test]
fn sse_fixtures_preserve_text_calls_reasoning_and_usage_at_every_chunk_boundary() {
    let cases = [
        (
            include_str!("fixtures/text.sse"),
            vec![
                Delta::Text {
                    output_index: 0,
                    text: "Hello ".into(),
                },
                Delta::Text {
                    output_index: 0,
                    text: "🌍".into(),
                },
                Delta::PartDone {
                    output_index: 0,
                    part: text("Hello 🌍"),
                },
                completed(vec![text("Hello 🌍")], StopReason::EndTurn),
            ],
        ),
        (
            include_str!("fixtures/function.sse"),
            vec![
                Delta::ToolArguments {
                    output_index: 0,
                    arguments: "{\"zone\":".into(),
                },
                Delta::ToolArguments {
                    output_index: 0,
                    arguments: "\"UTC\"}".into(),
                },
                Delta::PartDone {
                    output_index: 0,
                    part: call(),
                },
                completed(vec![call()], StopReason::ToolCalls),
            ],
        ),
        (
            include_str!("fixtures/reasoning.sse"),
            vec![
                Delta::Reasoning {
                    output_index: 0,
                    text: "Think first.".into(),
                },
                Delta::PartDone {
                    output_index: 0,
                    part: reasoning(),
                },
                completed(vec![reasoning()], StopReason::EndTurn),
            ],
        ),
    ];
    for (fixture, expected) in cases {
        for size in [1, 2, 7, 31, fixture.len()] {
            for newline in ["\n", "\r\n", "\r"] {
                assert_eq!(parse(&fixture.replace('\n', newline), size), expected);
            }
        }
    }
}

#[test]
fn sse_errors_truncation_and_malformed_calls_fail() {
    let mut parser = SseParser::default();
    let error = parser
        .push(include_bytes!("fixtures/error.sse"))
        .unwrap_err();
    assert!(error.to_string().contains("quota_exceeded"));
    assert!(
        SseParser::default()
            .push(b"data: {\"type\":\"error\",\"message\":\"failed\"}\n\n")
            .is_err()
    );
    assert!(SseParser::default().push(b"data: [DONE]\n\n").is_err());
    assert!(SseParser::default().finish().is_err());
    assert!(SseParser::default().push(b"data: invalid\n\n").is_err());
    let bad = include_str!("fixtures/function.sse").replace("\\\"UTC\\\"}", "oops");
    assert!(SseParser::default().push(bad.as_bytes()).is_err());
}

#[test]
fn multiline_data_and_incomplete_response() {
    let deltas = parse(
        "data: {\"type\":\"response.incomplete\",\ndata: \"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"output\":[]}}\n\n",
        1,
    );
    assert!(matches!(
        &deltas[0],
        Delta::Completed(Response {
            stop_reason: StopReason::MaxOutputTokens,
            ..
        })
    ));
}

#[tokio::test]
async fn fake_scripts_and_counter_work_through_dyn_provider() {
    let response = Response {
        parts: vec![text("scripted")],
        stop_reason: StopReason::EndTurn,
        usage: TokenUsage::default(),
        quota_remaining: std::collections::BTreeMap::new(),
        quota_resets: std::collections::BTreeMap::new(),
    };
    let mut fake = FakeProvider::default();
    fake.latency = Duration::from_millis(20);
    fake.responses.insert(0, response.clone());
    fake.tool_calls = Some(BTreeMap::from([(1, vec![call()])]));
    let provider: &dyn Provider = &fake;
    let start = tokio::time::Instant::now();
    let first: Vec<_> = provider.request(request()).try_collect().await.unwrap();
    assert!(start.elapsed() >= fake.latency);
    assert_eq!(first.last(), Some(&Delta::Completed(response)));
    let second: Vec<_> = provider.request(request()).try_collect().await.unwrap();
    assert!(
        matches!(second.last(), Some(Delta::Completed(Response { stop_reason: StopReason::ToolCalls, parts, .. })) if parts == &vec![call()])
    );
    assert_eq!(fake.call_count(), 2);
    assert!(
        provider
            .request(request())
            .try_collect::<Vec<_>>()
            .await
            .is_err()
    );
    assert_eq!(fake.call_count(), 3);
}

#[test]
fn empty_output_in_the_terminal_event_falls_back_to_streamed_items() {
    // The live Codex backend reports "output": [] in response.completed.
    let fixture = include_str!("fixtures/text.sse");
    let start = fixture.find("\"output\": [").unwrap();
    let end = start + fixture[start..].find("]}}").unwrap() + 1;
    let emptied = format!("{}\"output\": []{}", &fixture[..start], &fixture[end..]);
    assert_ne!(emptied, fixture);
    for chunk_size in [1, 7, 4096] {
        let deltas = parse(&emptied, chunk_size);
        assert_eq!(
            deltas.last(),
            Some(&completed(vec![text("Hello 🌍")], StopReason::EndTurn))
        );
    }
}

#[test]
fn responses_image_request_body() {
    let mut req = request();
    req.messages = vec![message(
        MessageRole::User,
        vec![Part::Image {
            media_type: "image/png".into(),
            bytes: vec![1, 2, 3],
            object_key: None,
            detail: Some("low".into()),
        }],
    )];
    let body = request_json(&req).unwrap();
    assert_eq!(
        body["input"][0]["content"][0],
        json!({"type":"input_image", "image_url":"data:image/png;base64,AQID", "detail":"low"})
    );
}

fn stalled_session_messages() -> Vec<Message> {
    let events: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/notice-between-call-and-result.json")).unwrap();
    events
        .iter()
        .map(|event| serde_json::from_value(event["message"].clone()).unwrap())
        .collect()
}

fn stalled_session_request() -> Request {
    use swarmy_llm::catalog::Catalog;
    let openai = Catalog::get().model("openai", "gpt-5.5").unwrap().clone();
    let mut req = request();
    req.settings.model = openai.id;
    req.tools = vec![
        ToolDefinition {
            name: "edit".into(),
            description: "Edit a file".into(),
            parameters: json!({"type": "object"}),
        },
        ToolDefinition {
            name: "bash".into(),
            description: "Run a command".into(),
            parameters: json!({"type": "object"}),
        },
    ];
    req.messages = stalled_session_messages();
    req
}

fn responses_input(req: &Request) -> Vec<Value> {
    use swarmy_llm::catalog::Catalog;
    let openai = Catalog::get().model("openai", "gpt-5.5").unwrap().clone();
    let endpoint = swarmy_llm::api::responses::ResponsesEndpoint::from_catalog(
        Catalog::get().provider("openai").unwrap(),
        &openai,
        swarmy_llm::ClientAuth::ApiKey("test-key".into()),
    )
    .unwrap();
    swarmy_llm::responses::request_json_for(req, &endpoint, "openai", Some(&openai), None).unwrap()
        ["input"]
        .as_array()
        .unwrap()
        .clone()
}

fn completions_messages(req: &Request) -> Vec<Value> {
    use swarmy_llm::catalog::Catalog;
    let openrouter = Catalog::get()
        .model("openrouter", "openai/gpt-5.5")
        .unwrap()
        .clone();
    let mut req = req.clone();
    req.settings.model.clone_from(&openrouter.id);
    swarmy_llm::api::completions::request_json(&req, "openrouter", &openrouter).unwrap()["messages"]
        .as_array()
        .unwrap()
        .clone()
}

fn assert_responses_paired(input: &[Value], calls: &[&str], notice: &str) {
    for id in calls {
        let call = input
            .iter()
            .position(|i| i["type"] == "function_call" && i["call_id"] == *id)
            .unwrap_or_else(|| panic!("call {id} in Responses input"));
        let result = input
            .iter()
            .position(|i| i["type"] == "function_call_output" && i["call_id"] == *id)
            .unwrap_or_else(|| panic!("result {id} in Responses input"));
        assert_eq!(
            call + 1,
            result,
            "result for {id} must directly follow its call"
        );
    }
    let notice = input
        .iter()
        .position(|i| i.to_string().contains(notice))
        .expect("notice in Responses input");
    for id in calls {
        let result = input
            .iter()
            .position(|i| i["type"] == "function_call_output" && i["call_id"] == *id)
            .unwrap();
        assert!(
            notice > result,
            "notice must come after the result for {id}"
        );
    }
    for id in calls {
        assert_eq!(
            input
                .iter()
                .filter(|i| i["type"] == "function_call" && i["call_id"] == *id)
                .count(),
            1,
            "call {id} emitted once"
        );
        assert_eq!(
            input
                .iter()
                .filter(|i| i["type"] == "function_call_output" && i["call_id"] == *id)
                .count(),
            1,
            "result {id} emitted once"
        );
    }
}

fn assert_completions_paired(messages: &[Value], calls: &[&str], notice: &str) {
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
fn stalled_session_notice_between_call_and_result_pairs_in_both_protocols() {
    // Worker-3's stalled session: every call is in the durable log, but a
    // system notice sits between the second call and its result. Converting
    // for either protocol must not fail; each result directly follows its
    // call, the notice appears after the results, and nothing is duplicated.
    let req = stalled_session_request();
    let calls = [
        "call_ZpXtECGcYFPLx8p8AqOJVFGn",
        "call_rqd1dzkZ3kl2nd118QaUdWYW",
    ];
    assert_responses_paired(&responses_input(&req), &calls, "Your computer was evicted");
    assert_completions_paired(
        &completions_messages(&req),
        &calls,
        "Your computer was evicted",
    );
}

#[test]
fn user_prompt_between_call_and_result_pairs_in_both_protocols() {
    // Mirror case: the next task's user prompt was appended while a call was
    // still in flight, so it sits between the call and its result.
    let mut req = stalled_session_request();
    let notice = req.messages.remove(3);
    assert!(notice.parts.iter().any(
        |part| matches!(part, Part::Text { text } if text.contains("Your computer was evicted"))
    ));
    req.messages.insert(
        3,
        message(
            MessageRole::User,
            vec![text("Continue with the next step while that runs.")],
        ),
    );
    assert_responses_paired(
        &responses_input(&req),
        &["call_rqd1dzkZ3kl2nd118QaUdWYW"],
        "Continue with the next step",
    );
    assert_completions_paired(
        &completions_messages(&req),
        &["call_rqd1dzkZ3kl2nd118QaUdWYW"],
        "Continue with the next step",
    );
}
