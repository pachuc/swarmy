//! Exercise the public client against the real HTTP router and development services.
use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};
use swarmy_api::{AppState, router};
use swarmy_api_types as api;
use swarmy_bus::{Bus, Config};
use swarmy_client::{Client, Error};
use swarmy_core::{CHUNK_SIZE, ContentHash, ImageTag, ManifestHeader, ManifestId};
use swarmy_store::{Store, blob::MemoryBlobStore};
use ulid::Ulid;

static NETWORK: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();

async fn fixture() -> Option<(Client, tokio::task::JoinHandle<Result<(), std::io::Error>>)> {
    let cluster = std::env::var("SWARMY_FDB_CLUSTER_FILE").ok()?;
    let nats = std::env::var("SWARMY_NATS_URL").ok()?;
    NETWORK.get_or_init(swarmy_store::boot);
    let store = Store::open(
        Some(&cluster),
        Some(&["client-api-test".into(), Ulid::generate().to_string()]),
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
    let bus = Bus::connect(&nats, Config::default()).await.unwrap();
    let state = AppState::new(
        store,
        bus,
        "test-token".into(),
        swarmy_llm::catalog::Catalog::get().clone(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = Client::new(
        &format!("http://{}", listener.local_addr().unwrap()),
        "test-token",
    )
    .unwrap();
    let server = tokio::spawn(axum::serve(listener, router(state)).into_future());
    Some((client, server))
}

async fn assert_agent_routes(client: &Client) {
    let created = client
        .create_agent(&api::CreateAgent {
            idempotency_key: "agent".into(),
            name: "fixture-agent".into(),
            description: "test".into(),
            image: api::ImageRef {
                name: "fixture".into(),
                tag: "test".into(),
            },
            provider: None,
            model: None,
            effort: None,
            system_prompt: None,
        })
        .await
        .unwrap();
    assert_eq!(client.agent("fixture-agent").await.unwrap().id, created.id);
    assert_eq!(client.agents(None, 10).await.unwrap().len(), 1);
    assert_eq!(
        client
            .update_agent(
                "fixture-agent",
                &api::UpdateAgent {
                    idempotency_key: "update".into(),
                    description: Some("updated".into()),
                    provider: None,
                    model: None,
                    effort: None,
                    system_prompt: None,
                }
            )
            .await
            .unwrap()
            .id,
        created.id
    );
    let named = client
        .create_session(&api::CreateSession {
            idempotency_key: "named".into(),
            agent_id: Some(created.id),
            new: false,
            image: None,
            provider: None,
            model: None,
            effort: None,
        })
        .await
        .unwrap();
    assert!(named.agent_id.is_some());
    assert_eq!(
        client
            .delete_agent("fixture-agent", "delete")
            .await
            .unwrap()["deleted"],
        true
    );
}

async fn assert_catalog_and_credentials(client: &Client) {
    let first = &client.models().await.unwrap()[0];
    assert_eq!(
        client
            .model(&first.provider_id, &first.id)
            .await
            .unwrap()
            .id,
        first.id
    );
    assert!(!client.search_models("gpt").await.unwrap().is_empty());
    if swarmy_config::Keyring::load().is_ok() {
        let created = client
            .set_credential(&api::CreateCredential {
                idempotency_key: "credential".into(),
                provider: "fixture-provider".into(),
                kind: api::CredentialKind::ApiKey,
                label: "test".into(),
                secret: "secret".into(),
            })
            .await
            .unwrap();
        assert_eq!(
            client.credential("fixture-provider").await.unwrap(),
            created
        );
        assert!(!client.credentials().await.unwrap().is_empty());
        assert_eq!(
            client
                .remove_credential("fixture-provider", "delete-credential")
                .await
                .unwrap()["deleted"],
            true
        );
    }
}

#[tokio::test]
async fn client_round_trips_real_routes() {
    let Some((client, server)) = fixture().await else {
        return;
    };
    assert!(client.health().await.unwrap().get("version").is_some());
    assert!(client.openapi().await.unwrap().get("openapi").is_some());
    assert!(!client.providers().await.unwrap().is_empty());
    assert!(!client.models().await.unwrap().is_empty());
    assert_catalog_and_credentials(&client).await;
    assert_eq!(client.images().await.unwrap().len(), 1);
    assert_agent_routes(&client).await;
    assert_eq!(
        client.image("fixture", "test").await.unwrap().name,
        "fixture"
    );
    let session = client
        .create_session(&api::CreateSession {
            idempotency_key: "session".into(),
            agent_id: None,
            new: false,
            image: Some(api::ImageRef {
                name: "fixture".into(),
                tag: "test".into(),
            }),
            provider: None,
            model: None,
            effort: None,
        })
        .await
        .unwrap();
    assert_eq!(client.session(&session.id).await.unwrap().id, session.id);
    assert_eq!(client.sessions(None, 10).await.unwrap().len(), 2);
    let append = client
        .append_message(
            &session.id,
            &api::AppendMessage {
                idempotency_key: "append".into(),
                expected_head: session.head_sequence,
                text: "hello".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        client.events(&session.id, 0, 10).await.unwrap()[0].sequence,
        append.sequence
    );
    let replay = client
        .append_message(
            &session.id,
            &api::AppendMessage {
                idempotency_key: "append".into(),
                expected_head: session.head_sequence,
                text: "hello".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(append, replay);
    let Err(Error::Api { body, .. }) = client.session("missing").await else {
        panic!("expected error")
    };
    assert_eq!(body.code, "invalid_id");
    let _ = tokio::time::timeout(
        Duration::from_millis(100),
        client.wait_idle(&session.id, append.sequence, 10),
    )
    .await;
    let _ = client
        .interrupt(
            &session.id,
            &api::InterruptSession {
                idempotency_key: "interrupt".into(),
            },
        )
        .await
        .unwrap();
    assert!(
        client
            .close_session(
                &session.id,
                &api::CloseSession {
                    idempotency_key: "close".into()
                }
            )
            .await
            .unwrap()
            .closed
    );
    server.abort();
}
