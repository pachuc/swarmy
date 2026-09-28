use std::{collections::BTreeMap, fmt::Write, sync::Arc};

use futures::{TryStreamExt, future::BoxFuture};
use serde_json::{Value, json};
use swarmy_core::{Message, MessageId, MessageRole, Part, ToolResult};
use swarmy_llm::{
    BearerSource, ClientAuth, Delta, Error, GenerationSettings, Provider, Request, Response,
    StopReason, ToolDefinition,
    api::gemini::{GeminiProvider, request_json},
    catalog::{Catalog, ModelInfo},
    client_for,
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path, query_param},
};

fn model(id: &str) -> ModelInfo {
    if let Some(model) = Catalog::get().model("google", id) {
        return model.clone();
    }
    // The catalog omits 2.5 for new keys, but the protocol must still parse
    // historical responses and preserve replay semantics for existing sessions.
    assert!(id.starts_with("gemini-2.5-"));
    let mut legacy = Catalog::get()
        .model("google", "gemini-3-flash-preview")
        .unwrap()
        .clone();
    legacy.id = id.into();
    legacy
}
fn message(role: MessageRole, parts: Vec<Part>) -> Message {
    Message {
        id: MessageId::from_ulid(ulid::Ulid::nil()),
        role,
        parts,
    }
}
fn request(id: &str) -> Request {
    Request {
        system_prompt: "Be helpful".into(),
        messages: vec![message(
            MessageRole::User,
            vec![Part::Text {
                text: "Hello".into(),
            }],
        )],
        tools: vec![ToolDefinition {
            name: "clock".into(),
            description: "Read time".into(),
            parameters: json!({"type":"object","properties":{"zone":{"type":"string"}}}),
        }],
        settings: GenerationSettings {
            model: id.into(),
            max_output_tokens: Some(1000),
            temperature: Some(0.3),
            ..Default::default()
        },
    }
}
fn provider(server: &MockServer, id: &str) -> GeminiProvider {
    let mut info = Catalog::get().provider("google").unwrap().clone();
    info.base_url = server.uri();
    GeminiProvider::new(&info, &model(id), ClientAuth::ApiKey("test-key".into())).unwrap()
}
fn sse(events: &[Value]) -> String {
    let mut wire = String::new();
    for event in events {
        write!(wire, "data: {event}\r\n\r\n").unwrap();
    }
    wire
}
async fn fixture(server: &MockServer, events: &[Value]) {
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(sse(events), "text/event-stream"))
        .mount(server)
        .await;
}
async fn complete(provider: &impl Provider, request: Request) -> Response {
    let deltas: Vec<_> = provider.request(request).try_collect().await.unwrap();
    assert_eq!(
        deltas
            .iter()
            .filter(|d| matches!(d, Delta::Completed(_)))
            .count(),
        1
    );
    let Delta::Completed(response) = deltas.last().unwrap() else {
        panic!("missing completion")
    };
    response.clone()
}

#[tokio::test]
async fn text_thoughts_usage_and_request_headers() {
    let server = MockServer::start().await;
    let id = "gemini-3-flash-preview";
    Mock::given(method("POST")).and(path(format!("/models/{id}:streamGenerateContent")))
        .and(query_param("alt", "sse")).and(header("x-goog-api-key", "test-key"))
        .and(header("user-agent", concat!("swarmy/", env!("CARGO_PKG_VERSION"))))
        .respond_with(ResponseTemplate::new(200).set_body_raw(sse(&[
            json!({"candidates":[{"content":{"parts":[{"text":"Thinking", "thought":true,"thoughtSignature":"thought-sig"}]}}]}),
            json!({"candidates":[{"content":{"parts":[{"text":"Hello "}]}}]}),
            json!({"candidates":[{"content":{"parts":[{"text":"世界"},{"thoughtSignature":"text-sig"}]},"finishReason":"STOP"}]}),
            json!({"usageMetadata":{"promptTokenCount":100,"cachedContentTokenCount":30,"candidatesTokenCount":10,"thoughtsTokenCount":5,"totalTokenCount":115}}),
        ]), "text/event-stream")).expect(1).mount(&server).await;
    let response = complete(&provider(&server, id), request(id)).await;
    assert_eq!(response.stop_reason, StopReason::EndTurn);
    assert_eq!(
        response.parts[1],
        Part::Text {
            text: "Hello 世界".into()
        }
    );
    assert_eq!(
        (
            response.usage.input_tokens,
            response.usage.cached_input_tokens,
            response.usage.output_tokens,
            response.usage.reasoning_output_tokens,
            response.usage.total_tokens
        ),
        (70, 30, 15, 5, 115)
    );
    let requests = server.received_requests().await.unwrap();
    let body: Value = requests[0].body_json().unwrap();
    assert_eq!(body["systemInstruction"]["parts"][0]["text"], "Be helpful");
    assert_eq!(
        body["tools"][0]["functionDeclarations"][0]["parametersJsonSchema"],
        request(id).tools[0].parameters
    );
    assert_eq!(body["toolConfig"]["functionCallingConfig"]["mode"], "AUTO");
    assert_eq!(body["generationConfig"]["maxOutputTokens"], 1000);
    assert_eq!(body["generationConfig"]["temperature"], 0.3);
}

#[tokio::test]
async fn function_calls_results_and_signature_replay() {
    let server = MockServer::start().await;
    let id = "gemini-3-flash-preview";
    fixture(&server, &[json!({"candidates":[{"content":{"parts":[
        {"text":"Let me check", "thought":true,"thoughtSignature":"thinking"},
        {"text":"Checking", "thoughtSignature":"text"},
        {"functionCall":{"id":"call-original","name":"clock","args":{"zone":"UTC"}},"thoughtSignature":"call"}
    ]},"finishReason":"STOP"}]})]).await;
    let response = complete(&provider(&server, id), request(id)).await;
    assert_eq!(response.stop_reason, StopReason::ToolCalls);
    let Part::ToolCall {
        call_id,
        tool,
        input,
    } = &response.parts[2]
    else {
        panic!("missing tool call")
    };
    assert_eq!(call_id.0, "call-original");
    assert_eq!(tool, "clock");
    assert_eq!(input, &json!({"zone":"UTC"}));
    let mut followup = request(id);
    followup
        .messages
        .push(message(MessageRole::Assistant, response.parts.clone()));
    followup.messages.push(message(
        MessageRole::Tool,
        vec![Part::ToolResult {
            call_id: call_id.clone(),
            result: ToolResult::Completed {
                output: "12:00".into(),
                title: "Time".into(),
                metadata: BTreeMap::new(),
            },
        }],
    ));
    complete(&provider(&server, id), followup.clone()).await;
    let requests = server.received_requests().await.unwrap();
    let body: Value = requests[1].body_json().unwrap();
    let parts = &body["contents"][1]["parts"];
    for (index, signature) in ["thinking", "text", "call"].iter().enumerate() {
        assert_eq!(parts[index]["thoughtSignature"], *signature);
    }
    assert_eq!(parts[2]["functionCall"]["id"], "call-original");
    assert_eq!(
        body["contents"][2]["parts"][0]["functionResponse"],
        json!({"id":"call-original","name":"clock","response":{"output":"12:00"}})
    );
    for (other_provider, other_model) in [
        ("google", model("gemini-2.5-pro")),
        ("google-vertex", model(id)),
    ] {
        let body = request_json(&followup, other_provider, &other_model).unwrap();
        let parts = body["contents"][1]["parts"].as_array().unwrap();
        assert!(parts.iter().all(|p| p.get("thoughtSignature").is_none()));
        assert_eq!(parts[0], json!({"text":"Let me check"}));
    }
    followup.messages.pop();
    let body = request_json(&followup, "google", &model(id)).unwrap();
    assert_eq!(
        body["contents"][2]["parts"][0]["functionResponse"]["response"],
        json!({"error":"No result provided"})
    );
    followup.messages.push(message(
        MessageRole::Tool,
        vec![Part::ToolResult {
            call_id: call_id.clone(),
            result: ToolResult::Error {
                error: "unavailable".into(),
            },
        }],
    ));
    let body = request_json(&followup, "google", &model(id)).unwrap();
    assert_eq!(
        body["contents"][2]["parts"][0]["functionResponse"]["response"],
        json!({"error":"unavailable"})
    );
}

#[tokio::test]
async fn thinking_levels_and_budgets() {
    for (id, expected) in [
        (
            "gemini-3-flash-preview",
            vec![json!("MINIMAL"), json!("LOW"), json!("HIGH")],
        ),
        (
            "gemini-3.1-pro-preview",
            vec![json!("LOW"), json!("LOW"), json!("HIGH")],
        ),
        ("gemini-2.5-pro", vec![json!(0), json!(2048), json!(32768)]),
        (
            "gemini-2.5-flash",
            vec![json!(0), json!(2048), json!(24576)],
        ),
        (
            "gemini-2.5-flash-lite",
            vec![json!(0), json!(2048), json!(24576)],
        ),
    ] {
        let server = MockServer::start().await;
        fixture(&server, &[json!({"candidates":[{"finishReason":"STOP"}]})]).await;
        for effort in ["none", "low", "max"] {
            let mut request = request(id);
            request.settings.reasoning_effort = Some(effort.parse().unwrap());
            complete(&provider(&server, id), request).await;
        }
        let requests = server.received_requests().await.unwrap();
        for (request, expected) in requests.iter().zip(expected) {
            let body: Value = request.body_json().unwrap();
            let config = &body["generationConfig"]["thinkingConfig"];
            // A zero budget disables thinking, so thoughts are not requested.
            if expected == json!(0) {
                assert!(config.get("includeThoughts").is_none());
            } else {
                assert_eq!(config["includeThoughts"], true);
            }
            assert_eq!(
                config[if id.starts_with("gemini-3") {
                    "thinkingLevel"
                } else {
                    "thinkingBudget"
                }],
                expected
            );
        }
        for (effort, expected) in [
            ("minimal", if id.contains("flash-lite") { 512 } else { 128 }),
            ("medium", 8192),
        ] {
            if id.starts_with("gemini-2.5") {
                let mut request = request(id);
                request.settings.reasoning_effort = Some(effort.parse().unwrap());
                assert_eq!(
                    request_json(&request, "google", &model(id)).unwrap()["generationConfig"]["thinkingConfig"]
                        ["thinkingBudget"],
                    expected
                );
            }
        }
    }
}

struct Token;
impl BearerSource for Token {
    fn token(&self) -> BoxFuture<'_, Result<String, Error>> {
        Box::pin(async { Ok("vertex-token".into()) })
    }
}
fn vertex_auth(location: &str) -> ClientAuth {
    ClientAuth::Vertex {
        project: "project-one".into(),
        location: location.into(),
        source: Arc::new(Token),
    }
}

#[tokio::test]
async fn vertex_url_bearer_and_dispatch() {
    let server = MockServer::start().await;
    let id = "gemini-2.5-pro";
    let mut info = Catalog::get().provider("google-vertex").unwrap().clone();
    for (location, host) in [
        ("global", "aiplatform.googleapis.com"),
        ("europe-west1", "europe-west1-aiplatform.googleapis.com"),
    ] {
        let provider = GeminiProvider::new(&info, &model(id), vertex_auth(location)).unwrap();
        assert_eq!(provider.url().host_str(), Some(host));
    }
    info.base_url = server.uri();
    Mock::given(method("POST")).and(path(format!("/v1/projects/project-one/locations/global/publishers/google/models/{id}:streamGenerateContent")))
        .and(header("authorization","Bearer vertex-token")).and(query_param("alt","sse"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(sse(&[json!({"candidates":[{"finishReason":"STOP"}]})]),"text/event-stream")).expect(1).mount(&server).await;
    let provider = client_for(&info, &model(id), vertex_auth("global")).unwrap();
    let _: Vec<_> = provider.request(request(id)).try_collect().await.unwrap();
    assert!(client_for(&info, &model(id), ClientAuth::None).is_err());
    let google = Catalog::get().provider("google").unwrap();
    assert!(client_for(google, &model(id), ClientAuth::ApiKey("key".into())).is_ok());
}

#[tokio::test]
async fn finish_reasons_errors_and_retry() {
    let server = MockServer::start().await;
    let id = "gemini-2.5-pro";
    for (reason, expected) in [
        ("SAFETY", StopReason::ContentFilter),
        ("RECITATION", StopReason::ContentFilter),
        ("MAX_TOKENS", StopReason::MaxOutputTokens),
    ] {
        server.reset().await;
        fixture(&server, &[json!({"candidates":[{"finishReason":reason}]})]).await;
        assert_eq!(
            complete(&provider(&server, id), request(id))
                .await
                .stop_reason,
            expected
        );
    }
    server.reset().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(400)
                .set_body_string("The input token count exceeds the maximum allowed"),
        )
        .mount(&server)
        .await;
    assert!(matches!(
        provider(&server, id)
            .request(request(id))
            .try_collect::<Vec<_>>()
            .await,
        Err(Error::ContextOverflow(_))
    ));
    server.reset().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "0"))
        .up_to_n_times(1)
        .expect(1)
        .with_priority(1)
        .mount(&server)
        .await;
    fixture(&server, &[json!({"candidates":[{"finishReason":"STOP"}]})]).await;
    assert_eq!(
        complete(&provider(&server, id), request(id))
            .await
            .stop_reason,
        StopReason::EndTurn
    );
    server.reset().await;
    fixture(
        &server,
        &[json!({"candidates":[{"content":{"parts":[{"text":"partial"}]}}]})],
    )
    .await;
    assert!(matches!(
        provider(&server, id)
            .request(request(id))
            .try_collect::<Vec<_>>()
            .await,
        Err(Error::Protocol(_))
    ));
}

#[tokio::test]
async fn legacy_function_calls_get_distinct_ids() {
    let server = MockServer::start().await;
    let id = "gemini-2.5-flash";
    fixture(&server, &[json!({"candidates":[{"content":{"parts":[{"functionCall":{"name":"clock","args":{}}}]},"finishReason":"STOP"}]})]).await;
    let first = complete(&provider(&server, id), request(id)).await;
    let second = complete(&provider(&server, id), request(id)).await;
    let Part::ToolCall { call_id: first, .. } = &first.parts[0] else {
        panic!("missing first call")
    };
    let Part::ToolCall {
        call_id: second, ..
    } = &second.parts[0]
    else {
        panic!("missing second call")
    };
    assert_ne!(first, second);
    assert!(!first.0.is_empty());
}
