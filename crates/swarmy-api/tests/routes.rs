use std::sync::{Arc, OnceLock};
use swarmy_api::{AppState, router};
use swarmy_api_types::{
    Agent, CreateAgent, CreateCredential, Credential, CredentialKind, Event, Image, ImageRef,
    Model, Provider, Session,
};
use swarmy_bus::{Bus, Config};
use swarmy_core::{CHUNK_SIZE, ContentHash, ImageTag, ManifestHeader, ManifestId};
use swarmy_store::{ServiceDetail, ServiceHeartbeat, ServiceRole, Store, blob::MemoryBlobStore};
use ulid::Ulid;

static NETWORK: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();

#[tokio::test]
async fn authenticated_routes_and_create_replay() {
    let Ok(cluster) = std::env::var("SWARMY_FDB_CLUSTER_FILE") else {
        return;
    };
    let Ok(nats) = std::env::var("SWARMY_NATS_URL") else {
        return;
    };
    NETWORK.get_or_init(swarmy_store::boot);
    let path = vec!["swarmy-api-test".to_owned(), Ulid::generate().to_string()];
    let store = Store::open(
        Some(&cluster),
        Some(&path),
        Arc::new(MemoryBlobStore::default()),
    )
    .await
    .unwrap();
    let manifest = ManifestId::from_ulid(Ulid::generate());
    store
        .put_manifest(
            manifest,
            &ManifestHeader {
                size: u64::from(CHUNK_SIZE),
                chunk_size: CHUNK_SIZE,
                root_hash: ContentHash::ZERO,
            },
        )
        .await
        .unwrap();
    store
        .put_image("fixture", &ImageTag("test".into()), manifest)
        .await
        .unwrap();
    register_services(&store).await;
    let bus = Bus::connect(&nats, Config::default()).await.unwrap();
    let mut state = AppState::new(
        store.clone(),
        bus,
        "test-token".into(),
        swarmy_llm::catalog::Catalog::get().clone(),
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
    assert_eq!(images[0], shown);
    let listed: Vec<Agent> = client
        .get(format!("{base}/v1/agents"))
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed, vec![first.clone()]);
    let shown: Agent = client
        .get(format!("{base}/v1/agents/test-agent"))
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(&shown, first);
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
    let shown: Session = client
        .get(format!("{base}/v1/sessions/{id}"))
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(shown, listed[0]);
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
        row.role == "node"
            && row
                .capacity
                .as_ref()
                .is_some_and(|capacity| capacity.sandboxes == 4)
    }));
    assert!(
        doctor
            .services
            .iter()
            .any(|row| row.role == "scheduler" && row.alive)
    );
    assert!(doctor.services.iter().any(|row| {
        row.role == "gateway"
            && row
                .providers
                .first()
                .is_some_and(|provider| provider == "fake")
    }));
}
