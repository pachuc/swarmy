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
        },
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
