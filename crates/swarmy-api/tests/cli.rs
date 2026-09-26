//! CLI projections retain the stored record shape without exposing a database to clients.
use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};
use swarmy_api::{AppState, router};
use swarmy_bus::{Bus, Config};
use swarmy_core::{CHUNK_SIZE, ContentHash, ImageTag, ManifestHeader, ManifestId};
use swarmy_store::{Store, blob::MemoryBlobStore};
use ulid::Ulid;

static NETWORK: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();
async fn fixture() -> Option<(
    swarmy_client::Client,
    Store,
    tokio::task::JoinHandle<Result<(), std::io::Error>>,
)> {
    let cluster = std::env::var("SWARMY_FDB_CLUSTER_FILE").ok()?;
    let nats = std::env::var("SWARMY_NATS_URL").ok()?;
    NETWORK.get_or_init(swarmy_store::boot);
    let store = Store::open(
        Some(&cluster),
        Some(&["cli-api-test".into(), Ulid::generate().to_string()]),
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
    let mut state = AppState::new(
        store.clone(),
        bus,
        "cli-test-token".into(),
        swarmy_llm::catalog::Catalog::get().clone(),
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );
    state.credential_keyring = Some(swarmy_config::Keyring::from_bytes([7; 32]));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = swarmy_client::Client::new(
        &format!("http://{}", listener.local_addr().unwrap()),
        "cli-test-token",
    )
    .unwrap();
    let server = tokio::spawn(axum::serve(listener, router(state)).into_future());
    Some((client, store, server))
}
#[tokio::test]
async fn projections_match_store_records_and_catalog() {
    let Some((client, store, server)) = fixture().await else {
        return;
    };
    let agent = store
        .create_agent(
            "fixture-agent",
            "fixture:test",
            "test",
            jiff::Timestamp::now(),
        )
        .await
        .unwrap();
    let (session, _) = store
        .open_main_session(agent.agent_id, jiff::Timestamp::now())
        .await
        .unwrap();
    assert_resource_projections(&client, &store, &agent, session).await;
    assert_credential_entries(&client).await;
    client
        .remove_credential("test-provider", "remove")
        .await
        .unwrap();
    server.abort();
}

async fn assert_resource_projections(
    client: &swarmy_client::Client,
    store: &Store,
    agent: &swarmy_core::AgentRecord,
    session: swarmy_core::SessionId,
) {
    let rows: Vec<serde_json::Value> = client
        .cli_agents(None, 10)
        .await
        .unwrap()
        .into_iter()
        .map(|row| serde_json::to_value(row).unwrap())
        .collect();
    assert_eq!(rows[0]["agent_id"], agent.agent_id.to_string());
    assert_eq!(rows[0]["session_count"], 1);
    let detailed = serde_json::to_value(client.cli_agent("fixture-agent").await.unwrap()).unwrap();
    assert_eq!(detailed["name"], agent.name);
    assert_eq!(detailed["sessions"][0]["session_id"], session.to_string());
    let rows: Vec<serde_json::Value> = client
        .cli_sessions(None, 10)
        .await
        .unwrap()
        .into_iter()
        .map(|row| serde_json::to_value(row).unwrap())
        .collect();
    let stored = store.fetch_session(session).await.unwrap().unwrap();
    assert_eq!(rows[0]["session_id"], session.to_string());
    assert_eq!(
        rows[0]["state"],
        serde_json::to_value(stored.state).unwrap()
    );
    let detail =
        serde_json::to_value(client.cli_session(&session.to_string()).await.unwrap()).unwrap();
    assert_eq!(detail["session"]["session_id"], session.to_string());
    assert_eq!(
        client.cli_image("fixture", "test").await.unwrap().name,
        "fixture"
    );
    assert!(
        !client
            .cli_models(None, None, false)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(!client.cli_providers().await.unwrap().is_empty());
}

async fn assert_credential_entries(client: &swarmy_client::Client) {
    let record = swarmy_core::CredentialRecord {
        kind: swarmy_core::CredentialKind::ApiKey {
            key: "synthetic-test-value".into(),
            extra: std::collections::BTreeMap::from([("label".into(), "fixture".into())]),
        },
        updated_at: jiff::Timestamp::now(),
    };
    client.cli_set_credential(&serde_json::from_value(serde_json::json!({"idempotency_key":"credential","provider":"test-provider","record":record})).unwrap()).await.unwrap();
    let summaries = client.cli_credentials().await.unwrap();
    assert_eq!(summaries[0].provider, "test-provider");
    assert_eq!(summaries[0].kind, "api-key");
    assert_eq!(summaries[0].label, "default");
    assert_eq!(
        client.cli_credential("test-provider").await.unwrap().status,
        "ready"
    );
    let cloud = client
        .set_credential(&swarmy_api_types::CreateCredential {
            idempotency_key: "cloud".into(),
            provider: "test-provider".into(),
            kind: swarmy_api_types::CredentialKind::Cloud,
            label: "backup".into(),
            secret: "synthetic-cloud-secret".into(),
        })
        .await
        .unwrap();
    assert_eq!(cloud.label, "backup");
    assert_eq!(cloud.kind, swarmy_api_types::CredentialKind::Cloud);
    assert_eq!(
        client
            .credential_entry("test-provider", "backup")
            .await
            .unwrap(),
        cloud
    );
    let listed = client.cli_credentials().await.unwrap();
    assert_eq!(listed.len(), 2);
    assert!(
        listed
            .iter()
            .any(|row| row.label == "backup" && row.kind == "cloud")
    );
    client
        .remove_credential_entry("test-provider", "backup", "remove-backup")
        .await
        .unwrap();
    assert_eq!(client.cli_credentials().await.unwrap().len(), 1);
}
#[tokio::test]
async fn stopped_api_reports_endpoint_quickly() {
    let Some((client, _store, server)) = fixture().await else {
        return;
    };
    server.abort();
    let result = tokio::time::timeout(Duration::from_secs(2), client.cli_agents(None, 10)).await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_err());
}

#[tokio::test]
async fn agent_management_uses_api_and_preserves_requirements() {
    let Some((client, store, server)) = fixture().await else {
        return;
    };
    let created = serde_json::to_value(client.cli_create_agent(&serde_json::from_value(serde_json::json!({
        "idempotency_key":"create", "name":"worker", "image":"fixture:test", "description":"test",
        "provider":"fake", "model":"scripted", "memory":2048, "gpu":"shared", "github_token":"synthetic-github-token"
    })).unwrap()).await.unwrap()).unwrap();
    assert_eq!(created["name"], "worker");
    assert!(!created.to_string().contains("synthetic-github-token"));
    let id =
        swarmy_core::AgentId::from_ulid(created["agent_id"].as_str().unwrap().parse().unwrap());
    assert_eq!(
        store.agent_github_token(id).await.unwrap().as_deref(),
        Some("synthetic-github-token")
    );
    assert_eq!(created["requirements"]["memory_mib"], 2048);
    let replayed = serde_json::to_value(client.cli_create_agent(&serde_json::from_value(serde_json::json!({
        "idempotency_key":"create", "name":"worker", "image":"fixture:test", "description":"test",
        "provider":"fake", "model":"scripted", "memory":2048, "gpu":"shared", "github_token":"synthetic-github-token"
    })).unwrap()).await.unwrap()).unwrap();
    assert_eq!(replayed["agent_id"], created["agent_id"]);
    let updated = serde_json::to_value(client
        .cli_update_agent(
            "worker",
            &serde_json::from_value(serde_json::json!({
                "idempotency_key":"update","memory":1024,"gpu":"none","resets":["provider","model"]
            })).unwrap(),
        )
        .await
        .unwrap()).unwrap();
    assert_eq!(updated["requirements"]["memory_mib"], 1024);
    assert!(updated["provider"].is_null());
    assert!(updated["model"].is_null());
    assert_eq!(
        client.cli_agent("worker").await.unwrap().agent_id,
        created["agent_id"]
    );
    client.delete_agent("worker", "delete").await.unwrap();
    assert!(store.get_agent_by_name("worker").await.unwrap().is_none());
    server.abort();
}

#[tokio::test]
async fn agent_update_rejects_invalid_merged_model() {
    let Some((client, store, server)) = fixture().await else {
        return;
    };
    let agent = store
        .create_agent("selection-test", "fixture:test", "", jiff::Timestamp::now())
        .await
        .unwrap();
    let request = serde_json::from_value(serde_json::json!({
        "idempotency_key":"invalid-model", "model":"bogus"
    }))
    .unwrap();
    let error = client
        .cli_update_agent("selection-test", &request)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("invalid_selection"), "{error}");
    assert!(
        store
            .get_agent(agent.agent_id)
            .await
            .unwrap()
            .unwrap()
            .model
            .is_none()
    );
    server.abort();
}
