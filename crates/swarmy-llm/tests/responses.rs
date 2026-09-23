use std::{collections::BTreeMap, sync::Arc, time::Duration};

use futures::TryStreamExt;
use serde_json::{Value, json};
use swarmy_core::{Message, MessageId, MessageRole, Part, SessionId, ToolCallId};
use swarmy_llm::{
    ClientAuth, Delta, Error, GenerationSettings, Provider, ReasoningEffort, Request, Response,
    StopReason, ToolDefinition,
    api::responses::{ResponsesEndpoint, ResponsesProvider},
    catalog::{Catalog, ModelInfo},
    client_for,
    retry::RetryPolicy,
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path},
};

fn request(model: &str) -> Request {
    Request {
        system_prompt: "Be helpful.".into(),
        messages: vec![message(
            vec![Part::Text {
                text: "Hello".into(),
            }],
            MessageRole::User,
        )],
        tools: vec![ToolDefinition {
            name: "clock".into(),
            description: "Read time".into(),
            parameters: json!({"type": "object", "properties": {}, "additionalProperties": false}),
        }],
        settings: GenerationSettings {
            model: model.into(),
            max_output_tokens: Some(100),
            reasoning_effort: Some(ReasoningEffort::Max),
            ..Default::default()
        },
    }
}

fn message(parts: Vec<Part>, role: MessageRole) -> Message {
    Message {
        id: MessageId::from_ulid(ulid::Ulid::nil()),
        role,
        parts,
    }
}

fn catalog_model(provider: &str, model: &str) -> ModelInfo {
    Catalog::get().model(provider, model).unwrap().clone()
}

fn client(server: &MockServer, provider: &str, model: &ModelInfo) -> Arc<dyn Provider> {
    let mut info = Catalog::get().provider(provider).unwrap().clone();
    info.base_url = server.uri();
    client_for(&info, model, ClientAuth::ApiKey("test-key".into())).unwrap()
}

async fn fixture(server: &MockServer, body: &str) {
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "text/event-stream"))
        .mount(server)
        .await;
}

async fn response(provider: &dyn Provider, request: Request) -> Response {
    let deltas: Vec<_> = provider.request(request).try_collect().await.unwrap();
    match deltas.last().unwrap() {
        Delta::Completed(response) => response.clone(),
        _ => panic!("missing completion"),
    }
}

async fn body(server: &MockServer) -> Value {
    server
        .received_requests()
        .await
        .unwrap()
        .last()
        .unwrap()
        .body_json()
        .unwrap()
}

#[tokio::test]
async fn openai_text_turn_uses_catalog_options_and_session_affinity() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/responses"))
        .and(header("authorization", "Bearer test-key"))
        .and(header(
            "user-agent",
            concat!("swarmy/", env!("CARGO_PKG_VERSION")),
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(include_str!("fixtures/text.sse"), "text/event-stream"),
        )
        .expect(1)
        .mount(&server)
        .await;
    let client = client(&server, "openai", &catalog_model("openai", "gpt-5.5"));
    let session = SessionId::from_ulid(ulid::Ulid::generate());
    let deltas: Vec<_> = client
        .request_for_session(request("gpt-5.5"), session)
        .try_collect()
        .await
        .unwrap();
    let Delta::Completed(response) = deltas.last().unwrap() else {
        panic!("missing completion")
    };
    assert_eq!(
        response.parts,
        vec![Part::Text {
            text: "Hello 🌍".into()
        }]
    );
    assert_eq!(response.usage.cached_input_tokens, 4);
    let body = body(&server).await;
    assert_eq!(body["input"][0]["role"], "developer");
    assert_eq!(body["input"][0]["content"][0]["text"], "Be helpful.");
    assert!(body.get("instructions").is_none());
    assert_eq!(
        body["reasoning"],
        json!({"effort": "xhigh", "summary": "auto"})
    );
    assert_eq!(body["text"], json!({"verbosity": "low"}));
    assert_eq!(body["prompt_cache_key"], session.to_string());
    assert_eq!(body["max_output_tokens"], 100);
    assert_eq!(body["parallel_tool_calls"], true);
    assert_eq!(body["store"], false);
    assert_eq!(body["include"], json!(["reasoning.encrypted_content"]));
    assert_eq!(body["tools"][0]["strict"], true);
    let requests = server.received_requests().await.unwrap();
    assert!(!requests[0].headers.contains_key("originator"));
    assert!(!requests[0].headers.contains_key("chatgpt-account-id"));
}

#[tokio::test]
async fn azure_resource_endpoint_and_deployment_use_api_key() {
    let server = MockServer::start().await;
    let info = Catalog::get().provider("azure").unwrap();
    let mut model = catalog_model("azure", "gpt-5.5");
    model.id = "my-deployment".into();
    let auth = ClientAuth::ApiKeyWithExtra {
        key: "azure-key".into(),
        extra: BTreeMap::from([("resource_name".into(), "test-resource".into())]),
    };
    let endpoint = ResponsesEndpoint::from_catalog(info, &model, auth.clone()).unwrap();
    assert_eq!(
        endpoint.url,
        "https://test-resource.openai.azure.com/openai/v1/responses"
    );
    model.base_url = Some(format!("{}/openai/v1", server.uri()));
    let client = client_for(info, &model, auth).unwrap();
    Mock::given(path("/openai/v1/responses"))
        .and(header("api-key", "azure-key"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(include_str!("fixtures/text.sse"), "text/event-stream"),
        )
        .expect(1)
        .mount(&server)
        .await;
    response(client.as_ref(), request("my-deployment")).await;
    assert_eq!(body(&server).await["model"], "my-deployment");
    assert_eq!(body(&server).await["text"]["verbosity"], "low");
    assert!(
        !server.received_requests().await.unwrap()[0]
            .headers
            .contains_key("authorization")
    );
}

#[test]
fn azure_grok_catalog_has_nonzero_prices() {
    let model = catalog_model("azure", "grok-4.6");
    assert!(model.cost.input > 0.0);
    assert!(model.cost.output > 0.0);
}

#[test]
fn azure_foundry_endpoint_from_credential_precedes_classic_resource() {
    let info = Catalog::get().provider("azure").unwrap();
    let model = catalog_model("azure", "gpt-5.5");
    for auth in [
        ClientAuth::ApiKeyWithExtra {
            key: "key".into(),
            extra: BTreeMap::from([
                ("resource_name".into(), "classic".into()),
                (
                    "base_url".into(),
                    "https://foundry.services.ai.azure.com".into(),
                ),
            ]),
        },
        ClientAuth::BearerWithExtra {
            token: "token".into(),
            extra: BTreeMap::from([(
                "base_url".into(),
                "https://foundry.services.ai.azure.com".into(),
            )]),
        },
    ] {
        let endpoint = ResponsesEndpoint::from_catalog(info, &model, auth).unwrap();
        assert_eq!(
            endpoint.url,
            "https://foundry.services.ai.azure.com/openai/v1/responses"
        );
    }
}

#[tokio::test]
async fn reasoning_replay_requires_the_same_provider_and_model() {
    let server = MockServer::start().await;
    fixture(&server, include_str!("fixtures/reasoning.sse")).await;
    let original = client(&server, "openai", &catalog_model("openai", "gpt-5.5"));
    let first = response(original.as_ref(), request("gpt-5.5")).await;
    let Part::Reasoning { metadata, .. } = &first.parts[0] else {
        panic!("missing reasoning")
    };
    assert!(!metadata.contains_key("chatgpt"));
    assert_eq!(
        metadata["openai_responses"]["item"]["encrypted_content"],
        "opaque-reasoning"
    );
    let mut replay = request("gpt-5.5");
    replay
        .messages
        .push(message(first.parts.clone(), MessageRole::Assistant));
    response(original.as_ref(), replay.clone()).await;
    assert_eq!(
        body(&server).await["input"][2]["encrypted_content"],
        "opaque-reasoning"
    );
    replay.settings.model = "gpt-4.1".into();
    response(original.as_ref(), replay.clone()).await;
    assert_eq!(
        body(&server).await["input"][2]["content"][0]["text"],
        "Think first."
    );
    assert!(!body(&server).await.to_string().contains("opaque-reasoning"));
    let azure = client(&server, "azure", &catalog_model("azure", "gpt-5.5"));
    replay.settings.model = "gpt-5.5".into();
    response(azure.as_ref(), replay.clone()).await;
    assert!(!body(&server).await.to_string().contains("opaque-reasoning"));
    if let Part::Reasoning { metadata, .. } = &mut replay.messages[1].parts[0] {
        let saved = metadata.remove("openai_responses").unwrap();
        metadata.insert("chatgpt".into(), saved);
    }
    response(original.as_ref(), replay.clone()).await;
    assert_eq!(
        body(&server).await["input"][2]["encrypted_content"],
        "opaque-reasoning"
    );
    // Historical records did not include a model, so their signatures cannot be verified.
    if let Part::Reasoning { metadata, .. } = &mut replay.messages[1].parts[0] {
        let saved = metadata.remove("chatgpt").unwrap();
        metadata.insert("chatgpt".into(), saved["item"].clone());
    }
    response(original.as_ref(), replay).await;
    assert_eq!(
        body(&server).await["input"][2]["content"][0]["text"],
        "Think first."
    );
}

#[tokio::test]
async fn non_reasoning_and_compat_flags_control_request_fields() {
    let server = MockServer::start().await;
    fixture(&server, include_str!("fixtures/text.sse")).await;
    let mut model = catalog_model("openai", "gpt-4.1");
    model
        .compat
        .0
        .insert("supports_developer_role".into(), json!(false));
    model
        .compat
        .0
        .insert("supports_strict_mode".into(), json!(false));
    let client = client(&server, "openai", &model);
    response(client.as_ref(), request("gpt-4.1")).await;
    let body = body(&server).await;
    assert!(body.get("reasoning").is_none());
    assert!(body.get("text").is_none());
    assert_eq!(body["input"][0]["role"], "system");
    assert_eq!(body["tools"][0]["strict"], false);
}

#[tokio::test]
async fn none_effort_is_sent_only_when_explicitly_supported() {
    let server = MockServer::start().await;
    fixture(&server, include_str!("fixtures/text.sse")).await;
    for (id, expected) in [("gpt-5.5", "none"), ("o3", "low")] {
        let provider = client(&server, "openai", &catalog_model("openai", id));
        let mut request = request(id);
        request.settings.reasoning_effort = Some(ReasoningEffort::None);
        response(provider.as_ref(), request).await;
        assert_eq!(body(&server).await["reasoning"]["effort"], expected);
    }
}

#[tokio::test]
async fn xai_and_meta_use_the_shared_transport_and_catalog_compat() {
    let server = MockServer::start().await;
    fixture(&server, include_str!("fixtures/text.sse")).await;
    for id in ["xai", "meta"] {
        let model = Catalog::get()
            .provider(id)
            .unwrap()
            .models
            .values()
            .next()
            .unwrap();
        response(client(&server, id, model).as_ref(), request(&model.id)).await;
        let body = body(&server).await;
        assert_eq!(body["model"], model.id);
        if id == "xai" {
            assert!(body.get("prompt_cache_retention").is_none());
        }
        let requests = server.received_requests().await.unwrap();
        assert_eq!(
            requests.last().unwrap().headers["authorization"],
            "Bearer test-key"
        );
    }
}

#[tokio::test]
async fn incomplete_preserves_text_done_and_cached_usage_without_output_items() {
    let server = MockServer::start().await;
    fixture(&server, concat!(
        "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"hel\"}\n\n",
        "data: {\"type\":\"response.output_text.done\",\"output_index\":0,\"text\":\"hello\"}\n\n",
        "data: {\"type\":\"response.incomplete\",\"response\":{\"output\":[],\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"usage\":{\"input_tokens\":10,\"input_tokens_details\":{\"cached_tokens\":7}}}}\n\n"
    )).await;
    let response = response(
        client(&server, "openai", &catalog_model("openai", "gpt-5.5")).as_ref(),
        request("gpt-5.5"),
    )
    .await;
    assert_eq!(response.stop_reason, StopReason::MaxOutputTokens);
    assert_eq!(
        response.parts,
        vec![Part::Text {
            text: "hello".into()
        }]
    );
    assert_eq!(response.usage.cached_input_tokens, 7);
}

fn retry_client(server: &MockServer) -> ResponsesProvider {
    let mut info = Catalog::get().provider("openai").unwrap().clone();
    info.base_url = server.uri();
    let model = catalog_model("openai", "gpt-5.5");
    let endpoint =
        ResponsesEndpoint::from_catalog(&info, &model, ClientAuth::ApiKey("key".into())).unwrap();
    let mut client = ResponsesProvider::new(endpoint, info.id, model).unwrap();
    client.retry_policy = RetryPolicy {
        max_attempts: 3,
        initial_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(2),
    };
    client
}

#[tokio::test]
async fn transient_http_errors_retry_and_permanent_errors_do_not() {
    for status in [408, 409, 429, 500, 503] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(status).insert_header("retry-after", "1000"))
            .up_to_n_times(1)
            .expect(1)
            .with_priority(1)
            .mount(&server)
            .await;
        fixture(&server, include_str!("fixtures/text.sse")).await;
        response(&retry_client(&server), request("gpt-5.5")).await;
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }
    for (status, calls) in [(429, 3), (400, 1), (401, 1)] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(status)
                    .set_body_json(json!({"error":{"message":"provider explanation"}})),
            )
            .expect(calls)
            .mount(&server)
            .await;
        let error = retry_client(&server)
            .request(request("gpt-5.5"))
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("provider explanation"));
        assert!(
            matches!(error, Error::ProviderResponse { status: code, .. } if code.as_u16() == status)
        );
    }
}

#[tokio::test]
async fn context_overflow_and_stream_failures_are_not_retried() {
    for (status, body, overflow) in [
        (400, r#"{"error":{"code":"context_length_exceeded"}}"#, true),
        (413, "", true),
        (
            200,
            "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"Your input exceeds the context window\"}}}\n\n",
            true,
        ),
        (
            200,
            "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"quota exhausted\"}}}\n\n",
            false,
        ),
        (
            200,
            "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"partial\"}\n\n",
            false,
        ),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(status).set_body_raw(body, "text/event-stream"))
            .expect(1)
            .mount(&server)
            .await;
        let error = retry_client(&server)
            .request(request("gpt-5.5"))
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert_eq!(
            matches!(error, Error::ContextOverflow(_)),
            overflow,
            "{error}"
        );
    }
}

#[tokio::test]
async fn orphaned_calls_get_results_and_ids_are_normalized() {
    let server = MockServer::start().await;
    fixture(&server, include_str!("fixtures/text.sse")).await;
    let provider = client(&server, "openai", &catalog_model("openai", "gpt-5.5"));
    let mut request = request("gpt-5.5");
    request.messages.push(message(
        vec![Part::ToolCall {
            call_id: ToolCallId("call|with spaces".into()),
            tool: "clock".into(),
            input: json!({}),
        }],
        MessageRole::Assistant,
    ));
    response(provider.as_ref(), request).await;
    let body = body(&server).await;
    let call = &body["input"][2];
    let result = &body["input"][3];
    assert_eq!(result["type"], "function_call_output");
    assert_eq!(result["call_id"], call["call_id"]);
    assert_eq!(result["output"], "Error: No result provided");
    assert!(
        call["call_id"]
            .as_str()
            .unwrap()
            .bytes()
            .all(|b| b.is_ascii_alphanumeric())
    );
}

#[tokio::test]
async fn codex_dispatch_preserves_auth_headers_and_instructions() {
    let server = MockServer::start().await;
    let mut credentials: Value = serde_json::from_str(include_str!("fixtures/auth.json")).unwrap();
    credentials["last_refresh"] = json!(jiff::Timestamp::now().to_string());
    let store = Arc::new(MemoryCredentials::new(credentials));
    let mut info = Catalog::get().provider("chatgpt").unwrap().clone();
    info.base_url = server.uri();
    let client = client_for(
        &info,
        &catalog_model("chatgpt", "gpt-5.5"),
        ClientAuth::ChatGpt(store),
    )
    .unwrap();
    Mock::given(method("POST"))
        .and(path("/responses"))
        .and(header("authorization", "Bearer access-fixture"))
        .and(header("chatgpt-account-id", "account-test"))
        .and(header("originator", "swarmy"))
        .and(header("openai-beta", "responses=experimental"))
        .and(header(
            "user-agent",
            concat!("swarmy/", env!("CARGO_PKG_VERSION")),
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(include_str!("fixtures/text.sse"), "text/event-stream"),
        )
        .expect(1)
        .mount(&server)
        .await;
    response(client.as_ref(), request("gpt-5.5")).await;
    let body = body(&server).await;
    assert_eq!(body["instructions"], "Be helpful.");
    assert_eq!(body["input"][0]["role"], "user");
    assert_eq!(body["tools"][0]["strict"], false);
}

#[tokio::test]
async fn azure_accepts_an_already_resolved_entra_bearer() {
    let server = MockServer::start().await;
    let mut info = Catalog::get().provider("azure").unwrap().clone();
    info.base_url = server.uri();
    let client = client_for(
        &info,
        &catalog_model("azure", "gpt-5.5"),
        ClientAuth::Bearer("entra-token".into()),
    )
    .unwrap();
    fixture(&server, include_str!("fixtures/text.sse")).await;
    response(client.as_ref(), request("gpt-5.5")).await;
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests[0].headers["authorization"], "Bearer entra-token");
    assert!(!requests[0].headers.contains_key("api-key"));
}

#[test]
fn azure_environment_resource_is_resolved_without_mutating_process_environment() {
    const CHILD: &str = "SWARMY_TEST_AZURE_RESOURCE";
    if std::env::var_os(CHILD).is_some() {
        let info = Catalog::get().provider("azure").unwrap();
        let endpoint = ResponsesEndpoint::from_catalog(
            info,
            &catalog_model("azure", "gpt-5.5"),
            ClientAuth::ApiKey("key".into()),
        )
        .unwrap();
        assert_eq!(
            endpoint.url,
            "https://environment-resource.openai.azure.com/openai/v1/responses"
        );
        return;
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "azure_environment_resource_is_resolved_without_mutating_process_environment",
        ])
        .env(CHILD, "1")
        .env("AZURE_RESOURCE_NAME", "environment-resource")
        .status()
        .unwrap();
    assert!(status.success());
}

#[tokio::test]
async fn oauth_refresh_failure_is_never_retried() {
    use swarmy_llm::{auth::OAuthClient, chatgpt::ChatGptProvider};
    let server = MockServer::start().await;
    let mut credentials: Value = serde_json::from_str(include_str!("fixtures/auth.json")).unwrap();
    credentials["last_refresh"] = json!("2000-01-01T00:00:00Z");
    let store = Arc::new(MemoryCredentials::new(credentials));
    Mock::given(path("/oauth/token"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&server)
        .await;
    let client = ChatGptProvider::with_endpoints(
        store,
        &server.uri(),
        OAuthClient::with_issuer(&server.uri()).unwrap(),
    )
    .unwrap();
    assert!(
        client
            .request(request("gpt-5.5"))
            .try_collect::<Vec<_>>()
            .await
            .is_err()
    );
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn strict_capability_does_not_make_optional_tool_parameters_required() {
    let server = MockServer::start().await;
    fixture(&server, include_str!("fixtures/text.sse")).await;
    let client = client(&server, "openai", &catalog_model("openai", "gpt-5.5"));
    let mut request = request("gpt-5.5");
    let schema = json!({"type":"object", "properties":{"command":{"type":"string"}, "timeout_ms":{"type":"integer"}}, "required":["command"], "additionalProperties":false});
    request.tools[0].parameters = schema.clone();
    response(client.as_ref(), request).await;
    let body = body(&server).await;
    assert_eq!(body["tools"][0]["strict"], false);
    assert_eq!(body["tools"][0]["parameters"], schema);
}

// Protocol fixtures use memory; production refresh ownership is tested against FDB.
struct MemoryCredentials(tokio::sync::Mutex<swarmy_llm::auth::Credentials>);

impl MemoryCredentials {
    fn new(value: Value) -> Self {
        Self(tokio::sync::Mutex::new(
            swarmy_llm::auth::Credentials::from_json(value).unwrap(),
        ))
    }
}

impl swarmy_llm::auth::CredentialStore for MemoryCredentials {
    fn load(
        &self,
    ) -> futures::future::BoxFuture<'_, Result<swarmy_llm::auth::Credentials, swarmy_llm::Error>>
    {
        Box::pin(async { Ok(self.0.lock().await.clone()) })
    }

    fn refresh<'a>(
        &'a self,
        client: &'a swarmy_llm::auth::OAuthClient,
        observed: &'a swarmy_llm::auth::Credentials,
    ) -> futures::future::BoxFuture<'a, Result<swarmy_llm::auth::Credentials, swarmy_llm::Error>>
    {
        Box::pin(async move {
            let mut stored = self.0.lock().await;
            if *stored != *observed {
                return Ok(stored.clone());
            }
            let updated = client.refresh_credentials(stored.clone()).await?;
            *stored = updated.clone();
            Ok(updated)
        })
    }
}
