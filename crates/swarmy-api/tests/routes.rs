#![deny(clippy::disallowed_methods)]
use std::sync::Arc;
use swarmy_api::{AppState, router};
use swarmy_api_types::{
    Agent, CreateAgent, CreateCredential, Credential, CredentialKind, Event, Image, ImageRef,
    Model, Provider, Session,
};
use swarmy_bus::{Bus, Config};
use swarmy_store::{ServiceDetail, ServiceHeartbeat, ServiceRole, Store, blob::MemoryBlobStore};
use ulid::Ulid;

#[tokio::test]
async fn authenticated_routes_and_create_replay() {
    let Some(stack) = swarmy_testkit::Stack::load("api") else {
        return;
    };
    let (store, _guard) = stack.open_store(Arc::new(MemoryBlobStore::default())).await;
    swarmy_testkit::image(&store).await;
    register_services(&store).await;
    let bus = Bus::connect(&stack.nats_url, Config::default())
        .await
        .unwrap();
    let mut state = AppState::new(
        store.clone(),
        bus,
        "test-token".into(),
        swarmy_llm::catalog::Catalog::get().clone(),
        Arc::new(object_store::memory::InMemory::new()),
    );
    state.default_image = Some("fixture:test".into());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(axum::serve(listener, router(state)).into_future());
    let client = reqwest::Client::new();
    let health = client
        .get(format!("{base}/v1/health"))
        .send()
        .await
        .unwrap();
    assert!(health.status().is_success());
    assert!(health.json::<serde_json::Value>().await.unwrap()["version"].is_string());
    check_doctor(&base).await;
    let denied = client
        .get(format!("{base}/v1/agents"))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), reqwest::StatusCode::UNAUTHORIZED);
    let models: Vec<Model> = client
        .get(format!("{base}/v1/models/search?q=gpt"))
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!models.is_empty());
    let body = CreateAgent {
        idempotency_key: "create-one".into(),
        name: "test-agent".into(),
        description: "test".into(),
        image: ImageRef {
            name: "fixture".into(),
            tag: "test".into(),
        },
        provider: None,
        model: None,
        effort: None,
        system_prompt: None,
        route: None,

        memory_mib: None,
        gpu: None,
        allow_gpu_request: None,
        gpu_reason: None,
        github_token: None,
    };
    let create = || {
        client
            .post(format!("{base}/v1/agents"))
            .bearer_auth("test-token")
            .json(&body)
    };
    let first = create().send().await.unwrap();
    assert!(first.status().is_success(), "{}", first.status());
    let first: Agent = first.json().await.unwrap();
    let second: Agent = create().send().await.unwrap().json().await.unwrap();
    assert_eq!(first.id, second.id);
    assert_eq!(store.list_agents(None, 10).await.unwrap().len(), 1);
    assert_reads(&client, &base, &first).await;
    assert_session_routes(&store, &client, &base, &first).await;
    assert_credentials(&client, &base).await;
    task.abort();
}

async fn assert_reads(client: &reqwest::Client, base: &str, first: &Agent) {
    assert_image_reads(client, base).await;
    assert_agent_reads(client, base, first).await;
    assert_provider_and_docs_reads(client, base).await;
}

/// Image list and detail agree, and the detail carries the header.
async fn assert_image_reads(client: &reqwest::Client, base: &str) {
    let images: Vec<Image> = client
        .get(format!("{base}/v1/images"))
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(images.len(), 1);
    let shown: Image = client
        .get(format!("{base}/v1/images/fixture/test"))
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(images[0].manifest_id, shown.manifest_id);
    assert_eq!(images[0].name, shown.name);
    assert_eq!(images[0].tag, shown.tag);
    assert!(
        shown.header.is_some(),
        "image detail includes the manifest header"
    );
}

/// Agent list rows are summaries; show carries the detail create returned.
async fn assert_agent_reads(client: &reqwest::Client, base: &str, first: &Agent) {
    let listed: Vec<Agent> = client
        .get(format!("{base}/v1/agents"))
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, first.id);
    assert_eq!(listed[0].name, first.name);
    assert!(listed[0].sessions.is_empty());
    assert!(listed[0].usage.is_none());
    let shown: Agent = client
        .get(format!("{base}/v1/agents/test-agent"))
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let mut created_value = serde_json::to_value(first).unwrap();
    let mut shown_value = serde_json::to_value(shown).unwrap();
    for value in [&mut created_value, &mut shown_value] {
        value
            .as_object_mut()
            .expect("agent serializes to an object")
            .remove("last_snapshot_age_seconds");
    }
    assert_eq!(created_value, shown_value);
}

/// Providers list and the public `OpenAPI` document both answer.
async fn assert_provider_and_docs_reads(client: &reqwest::Client, base: &str) {
    let providers: Vec<Provider> = client
        .get(format!("{base}/v1/providers"))
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!providers.is_empty());
    let openapi: serde_json::Value = client
        .get(format!("{base}/v1/openapi.json"))
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(openapi["components"]["schemas"]["Agent"].is_object());
    assert_docs_are_public(client, base, &openapi).await;
}

async fn assert_docs_are_public(
    client: &reqwest::Client,
    base: &str,
    authenticated: &serde_json::Value,
) {
    // The reference renders without a token so every swarm documents itself.
    let page = client.get(format!("{base}/v1/docs")).send().await.unwrap();
    assert!(page.status().is_success());
    let text = page.text().await.unwrap();
    assert!(text.to_lowercase().contains("swagger"), "{text:.200}");
    let anonymous: serde_json::Value = client
        .get(format!("{base}/v1/openapi.json"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(&anonymous, authenticated);
    for path in [
        "/v1/health",
        "/v1/openapi.json",
        "/v1/docs",
        "/v1/agents",
        "/v1/sessions",
        "/v1/sessions/{id}/messages",
        "/v1/events",
        "/v1/images",
        "/v1/models",
        "/v1/providers",
        "/v1/credentials",
    ] {
        assert!(anonymous["paths"][path].is_object(), "missing {path}");
    }
}

async fn assert_credentials(client: &reqwest::Client, base: &str) {
    if swarmy_config::Keyring::load().is_err() {
        return;
    }
    let secret = "route-test-secret-never-return";
    let body = CreateCredential {
        idempotency_key: Ulid::generate().to_string(),
        provider: "api-fixture".into(),
        kind: CredentialKind::ApiKey,
        label: "primary".into(),
        secret: secret.into(),
        extra: std::collections::BTreeMap::default(),
    };
    let created = client
        .post(format!("{base}/v1/credentials"))
        .bearer_auth("test-token")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert!(created.status().is_success());
    let raw = created.text().await.unwrap();
    assert!(!raw.contains(secret));
    let created: Credential = serde_json::from_str(&raw).unwrap();
    assert_eq!(created.label, "primary");
    let listed: Vec<Credential> = client
        .get(format!("{base}/v1/credentials"))
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        listed
            .iter()
            .any(|entry| entry.provider == body.provider && entry.label == "primary")
    );
    let checked: Credential = client
        .get(format!("{base}/v1/credentials/api-fixture"))
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(checked, created);
    let removed = client
        .delete(format!("{base}/v1/credentials/api-fixture"))
        .bearer_auth("test-token")
        .json(&serde_json::json!({"idempotency_key": Ulid::generate().to_string()}))
        .send()
        .await
        .unwrap();
    assert!(removed.status().is_success());
}

async fn assert_session(client: &reqwest::Client, base: &str, id: &str) {
    let listed: Vec<Session> = client
        .get(format!("{base}/v1/sessions?limit=1"))
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, id);
    // List rows are summaries; show hydrates usage, requirements, and placement.
    assert!(listed[0].usage.is_none());
    assert!(listed[0].requirements.is_none());
    let shown: Session = client
        .get(format!("{base}/v1/sessions/{id}"))
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(shown.id, listed[0].id);
    assert_eq!(shown.state, listed[0].state);
    assert!(shown.usage.is_some());
    assert!(shown.requirements.is_some());
    let events: Vec<Event> = client
        .get(format!("{base}/v1/sessions/{id}/events?after=0&limit=4"))
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(events.is_empty());
}

async fn assert_event(client: &reqwest::Client, base: &str, id: &str) {
    let events: Vec<Event> = client
        .get(format!("{base}/v1/sessions/{id}/events?after=0&limit=1"))
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].sequence, 1);
    let later: Vec<Event> = client
        .get(format!("{base}/v1/sessions/{id}/events?after=1&limit=1"))
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(later.is_empty());
}

async fn assert_session_routes(store: &Store, client: &reqwest::Client, base: &str, first: &Agent) {
    let agent_id = swarmy_core::AgentId::from_ulid(first.id.parse().unwrap());
    let (session_id, created) = store
        .open_main_session(agent_id, jiff::Timestamp::now())
        .await
        .unwrap();
    assert!(created);
    assert_session(client, base, &session_id.to_string()).await;
    let message = swarmy_core::Message {
        id: swarmy_core::MessageId::from_ulid(Ulid::generate()),
        role: swarmy_core::MessageRole::User,
        parts: vec![swarmy_core::Part::Text {
            text: "hello".into(),
        }],
    };
    store
        .append_user_message(session_id, 0, &message)
        .await
        .unwrap();
    assert_event(client, base, &session_id.to_string()).await;
}

async fn register_services(store: &Store) {
    for role in [
        ServiceRole::Scheduler,
        ServiceRole::Worker,
        ServiceRole::Gateway,
        ServiceRole::Node,
    ] {
        let now = jiff::Timestamp::now();
        store
            .put_service_heartbeat(&ServiceHeartbeat {
                role: role.clone(),
                instance_id: format!("{role:?}"),
                version: "0.1.0".into(),
                host: "test".into(),
                started_at: now,
                last_seen: now,
                detail: match role {
                    ServiceRole::Gateway => ServiceDetail::Providers(vec!["fake".into()]),
                    ServiceRole::Node => ServiceDetail::Capacity(swarmy_core::NodeCapacity {
                        cpu_millis: 1000,
                        memory_bytes: 4096,
                        disk_bytes: 8192,
                        sandboxes: 4,
                    }),
                    _ => ServiceDetail::None,
                },
            })
            .await
            .unwrap();
    }
}

async fn check_doctor(base: &str) {
    let doctor = swarmy_client::Client::new(base, "test-token")
        .unwrap()
        .doctor()
        .await
        .unwrap();
    assert_eq!(doctor.default_image.as_deref(), Some("fixture:test"));
    assert_eq!(doctor.images[0], "fixture:test");
    assert!(doctor.services.iter().any(|row| {
        row.role == swarmy_api_types::ServiceRole::Node
            && row
                .capacity
                .as_ref()
                .is_some_and(|capacity| capacity.sandboxes == 4)
    }));
    assert!(
        doctor
            .services
            .iter()
            .any(|row| row.role == swarmy_api_types::ServiceRole::Scheduler && row.alive)
    );
    assert!(doctor.services.iter().any(|row| {
        row.role == swarmy_api_types::ServiceRole::Gateway
            && row
                .providers
                .first()
                .is_some_and(|provider| provider == "fake")
    }));
}

fn fallback_steps() -> Vec<swarmy_api_types::RouteStep> {
    vec![
        swarmy_api_types::RouteStep {
            provider: "openai".into(),
            entry: "work-key".into(),
            model: None,
        },
        swarmy_api_types::RouteStep {
            provider: "azure".into(),
            entry: "prod".into(),
            model: Some("gpt-5.5".into()),
        },
    ]
}

fn route_input(steps: Vec<swarmy_api_types::RouteStep>) -> swarmy_api_types::SetRoute {
    swarmy_api_types::SetRoute {
        idempotency_key: Ulid::generate().to_string(),
        steps,
    }
}

async fn route_server() -> Option<(
    swarmy_client::Client,
    tokio::task::JoinHandle<Result<(), std::io::Error>>,
    swarmy_testkit::StackGuard,
)> {
    let stack = swarmy_testkit::Stack::load("api")?;
    let (store, guard) = stack.open_store(Arc::new(MemoryBlobStore::default())).await;
    swarmy_testkit::image(&store).await;
    register_services(&store).await;
    let bus = Bus::connect(&stack.nats_url, Config::default())
        .await
        .unwrap();
    let mut state = AppState::new(
        store.clone(),
        bus,
        "test-token".into(),
        swarmy_llm::catalog::Catalog::get().clone(),
        Arc::new(object_store::memory::InMemory::new()),
    );
    state.default_image = Some("fixture:test".into());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(axum::serve(listener, router(state)).into_future());
    let client = swarmy_client::Client::new(&base, "test-token").unwrap();
    Some((client, task, guard))
}

async fn assert_route_crud(client: &swarmy_client::Client) {
    // Unknown providers fail at set time instead of wedging turns later.
    assert!(
        client
            .set_route(
                "bad",
                &route_input(vec![swarmy_api_types::RouteStep {
                    provider: "no-such-provider".into(),
                    entry: "default".into(),
                    model: None,
                }],)
            )
            .await
            .is_err()
    );
    client
        .set_route("fallback", &route_input(fallback_steps()))
        .await
        .unwrap();
    let listed = client.routes().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].steps.len(), 2);
    assert_eq!(listed[0].steps[1].model.as_deref(), Some("gpt-5.5"));
    assert_eq!(client.route("fallback").await.unwrap().name, "fallback");
    assert_eq!(
        client.route("fallback").await.unwrap().steps,
        fallback_steps()
    );
    assert_eq!(client.routes().await.unwrap().len(), 1);
}

async fn assert_agent_route(client: &swarmy_client::Client) {
    // Agent assignment and clearing round-trips through the resource API.
    let agent = client
        .create_agent(&CreateAgent {
            idempotency_key: Ulid::generate().to_string(),
            name: "routed".into(),
            description: String::new(),
            image: ImageRef {
                name: "fixture".into(),
                tag: "test".into(),
            },
            provider: None,
            model: None,
            effort: None,
            system_prompt: None,
            route: Some("fallback".into()),

            memory_mib: None,
            gpu: None,
            allow_gpu_request: None,
            gpu_reason: None,
            github_token: None,
        })
        .await
        .unwrap();
    assert_eq!(agent.route.as_deref(), Some("fallback"));
    let agent = client
        .update_agent(
            "routed",
            &swarmy_api_types::UpdateAgent {
                idempotency_key: Ulid::generate().to_string(),
                description: None,
                provider: None,
                model: None,
                effort: None,
                system_prompt: None,
                route: None,

                memory_mib: None,
                gpu: None,
                allow_gpu_request: None,
                gpu_reason: None,
                resets: Vec::new(),
                github_token: None,
                clear_github_token: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(agent.route.as_deref(), Some("fallback"));
}

async fn route_session(client: &swarmy_client::Client) -> Session {
    // Session overrides apply per conversation; missing routes are rejected.
    let session = client
        .create_session(&swarmy_api_types::CreateSession {
            idempotency_key: Ulid::generate().to_string(),
            agent_id: None,
            new: false,
            image: Some(ImageRef {
                name: "fixture".into(),
                tag: "test".into(),
            }),
            provider: None,
            model: None,
            effort: None,
            route: None,
        })
        .await
        .unwrap();
    assert_eq!(session.route, None);
    assert!(
        client
            .set_session_route(
                &session.id,
                &swarmy_api_types::SetSessionRoute {
                    idempotency_key: Ulid::generate().to_string(),
                    route: Some("missing".into()),
                },
            )
            .await
            .is_err()
    );
    let session = client
        .set_session_route(
            &session.id,
            &swarmy_api_types::SetSessionRoute {
                idempotency_key: Ulid::generate().to_string(),
                route: Some("fallback".into()),
            },
        )
        .await
        .unwrap();
    assert_eq!(session.route.as_deref(), Some("fallback"));
    let detail = client.session(&session.id).await.unwrap();
    assert_eq!(detail.route.as_deref(), Some("fallback"));
    let session = client
        .set_session_route(
            &session.id,
            &swarmy_api_types::SetSessionRoute {
                idempotency_key: Ulid::generate().to_string(),
                route: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(session.route, None);
    session
}

async fn assert_route_deletion(client: &swarmy_client::Client) {
    // Deletion reports and assigned sessions fall back afterwards.
    assert!(
        client
            .remove_route("fallback", &Ulid::generate().to_string())
            .await
            .is_ok()
    );
    assert!(client.route("fallback").await.is_err());
    assert_eq!(client.routes().await.unwrap().len(), 0);
}

#[tokio::test]
async fn inference_routes_round_trip_through_resource_api() {
    let Some((client, task, _guard)) = route_server().await else {
        return;
    };
    assert_route_crud(&client).await;
    assert_agent_route(&client).await;
    route_session(&client).await;
    assert_route_deletion(&client).await;
    task.abort();
}

#[tokio::test]
async fn tool_image_route_returns_only_session_images() {
    use swarmy_core::{RequestId, SessionId};
    use swarmy_store::AgentSessionOptions;
    let Some(stack) = swarmy_testkit::Stack::load("api-tool-image") else {
        return;
    };
    let (store, _guard) = stack.open_store(Arc::new(MemoryBlobStore::default())).await;
    swarmy_testkit::image(&store).await;
    let session = SessionId::from_ulid(Ulid::generate());
    store
        .create_agent_session(
            session,
            None,
            jiff::Timestamp::now(),
            Some(AgentSessionOptions {
                image: Some("fixture:test"),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    let png = b"image bytes".to_vec();
    let key = store.put_tool_blob(png.clone()).await.unwrap();
    let events = image_route_events(RequestId::for_step(session, 1), &key);
    store.append_events(session, 0, &events).await.unwrap();
    let bus = Bus::connect(&stack.nats_url, Config::default())
        .await
        .unwrap();
    let state = AppState::new(
        store,
        bus,
        "test-token".into(),
        swarmy_llm::catalog::Catalog::get().clone(),
        Arc::new(object_store::memory::InMemory::new()),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(axum::serve(listener, router(state)).into_future());
    let client = reqwest::Client::new();
    let url = |id: String, call: &str| format!("{base}/v1/sessions/{id}/tool-images/{call}");
    let success = client
        .get(url(session.to_string(), "screenshot"))
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap();
    assert_eq!(success.status(), reqwest::StatusCode::OK);
    assert_eq!(
        success.headers()[reqwest::header::CONTENT_TYPE],
        "image/png"
    );
    assert_eq!(success.headers()["x-content-type-options"], "nosniff");
    assert_eq!(success.headers()["content-disposition"], "attachment");
    assert_eq!(success.bytes().await.unwrap().as_ref(), png);
    let api_client = swarmy_client::Client::new(&base, "test-token").unwrap();
    let (media_type, downloaded) = api_client
        .tool_image(&session.to_string(), "toolu/a%b c")
        .await
        .unwrap();
    assert_eq!(media_type, "image/png");
    assert_eq!(downloaded, png);
    let missing = api_client.tool_image(&session.to_string(), "unknown").await;
    assert!(
        matches!(missing, Err(swarmy_client::Error::Api { body, .. }) if body.code == "image_not_found")
    );
    assert_image_route_errors(&client, &base, session).await;
    task.abort();
}

/// Keep the metadata-forgery fixtures next to their storage shape; the test
/// above focuses on the HTTP and client behavior.
fn image_route_events(request_id: swarmy_core::RequestId, key: &str) -> [swarmy_core::Event; 6] {
    use swarmy_core::{Event as StoreEvent, ToolCallId, ToolCallRecord, ToolResult};
    [
        StoreEvent::ToolCallRequested {
            seq: 0,
            request_id,
            call: ToolCallRecord {
                call_id: ToolCallId("screenshot".into()),
                tool: "browser_screenshot".into(),
                arguments: serde_json::json!({}),
                result: None,
            },
        },
        StoreEvent::ToolCallCompleted {
            seq: 0,
            request_id,
            call_id: ToolCallId("screenshot".into()),
            result: ToolResult::Completed {
                output: "Screenshot".into(),
                title: "Screenshot".into(),
                metadata: std::collections::BTreeMap::from([
                    ("image_object_key".into(), serde_json::json!(key)),
                    ("image_media_type".into(), serde_json::json!("image/png")),
                ]),
            },
        },
        StoreEvent::ToolCallCompleted {
            seq: 0,
            request_id,
            call_id: ToolCallId("toolu/a%b c".into()),
            result: ToolResult::Completed {
                output: "Screenshot".into(),
                title: "Screenshot".into(),
                metadata: std::collections::BTreeMap::from([
                    ("image_object_key".into(), serde_json::json!(key)),
                    ("image_media_type".into(), serde_json::json!("image/png")),
                ]),
            },
        },
        StoreEvent::ToolCallCompleted {
            seq: 0,
            request_id,
            call_id: ToolCallId("bad-key".into()),
            result: ToolResult::Completed {
                output: "Screenshot".into(),
                title: "Screenshot".into(),
                metadata: std::collections::BTreeMap::from([
                    ("image_object_key".into(), serde_json::json!("not-a-blob")),
                    ("image_media_type".into(), serde_json::json!("image/png")),
                ]),
            },
        },
        StoreEvent::ToolCallCompleted {
            seq: 0,
            request_id,
            call_id: ToolCallId("bad-type".into()),
            result: ToolResult::Completed {
                output: "Screenshot".into(),
                title: "Screenshot".into(),
                metadata: std::collections::BTreeMap::from([
                    ("image_object_key".into(), serde_json::json!(key)),
                    ("image_media_type".into(), serde_json::json!("text/html")),
                ]),
            },
        },
        StoreEvent::ToolCallCompleted {
            seq: 0,
            request_id,
            call_id: ToolCallId("text".into()),
            result: ToolResult::Completed {
                output: "no image".into(),
                title: "Text".into(),
                metadata: std::collections::BTreeMap::default(),
            },
        },
    ]
}

async fn assert_image_route_errors(
    client: &reqwest::Client,
    base: &str,
    session: swarmy_core::SessionId,
) {
    let url = |id: String, call: &str| format!("{base}/v1/sessions/{id}/tool-images/{call}");
    for call in ["unknown", "text", "bad-key", "bad-type"] {
        let missing = client
            .get(url(session.to_string(), call))
            .bearer_auth("test-token")
            .send()
            .await
            .unwrap();
        assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
        assert_eq!(
            missing.json::<serde_json::Value>().await.unwrap()["code"],
            "image_not_found"
        );
    }
    let missing = client
        .get(url(Ulid::generate().to_string(), "screenshot"))
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
    assert_eq!(
        missing.json::<serde_json::Value>().await.unwrap()["code"],
        "session_not_found"
    );
    let denied = client
        .get(url(session.to_string(), "screenshot"))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), reqwest::StatusCode::UNAUTHORIZED);
}
