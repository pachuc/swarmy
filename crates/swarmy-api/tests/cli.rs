#![deny(clippy::disallowed_methods)]
//! CLI projections retain the stored record shape without exposing a database to clients.
use std::{sync::Arc, time::Duration};
use swarmy_api::{AppState, router};
use swarmy_bus::{Bus, Config};
use swarmy_core::{CHUNK_SIZE, ContentHash, ImageTag, ManifestHeader, ManifestId};
use swarmy_store::{Store, blob::MemoryBlobStore};
use ulid::Ulid;

async fn fixture() -> Option<(
    swarmy_client::Client,
    Store,
    tokio::task::JoinHandle<Result<(), std::io::Error>>,
)> {
    let cluster = swarmy_testkit::require_stack("SWARMY_FDB_CLUSTER_FILE")?;
    let nats = swarmy_testkit::require_stack("SWARMY_NATS_URL")?;
    swarmy_testkit::boot_fdb();
    let store = Store::open(
        Some(std::path::Path::new(&cluster)),
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
        .put_image("fixture", &ImageTag("test".into()), manifest, None)
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
            None,
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
    let rows = client.agents(None, 10).await.unwrap();
    assert_eq!(rows[0].id, agent.agent_id.to_string());
    // List rows are summaries: no session scan, usage, or placement reads.
    assert!(rows[0].sessions.is_empty());
    assert!(rows[0].usage.is_none());
    assert!(rows[0].placement.is_none());
    let detailed = client.agent("fixture-agent").await.unwrap();
    assert_eq!(detailed.name, agent.name);
    assert_eq!(detailed.sessions.len(), 1);
    assert_eq!(detailed.sessions[0].id, session.to_string());
    assert!(detailed.usage.is_some());
    let rows = client.sessions(None, 10).await.unwrap();
    let stored = store.fetch_session(session).await.unwrap().unwrap();
    assert_eq!(rows[0].id, session.to_string());
    assert_eq!(rows[0].state, stored.state.into());
    // Session list rows carry the fleet fields but no detail hydration.
    assert!(rows[0].state_since.is_some());
    assert!(rows[0].usage.is_none());
    assert!(rows[0].requirements.is_none());
    let detail = client.session(&session.to_string()).await.unwrap();
    assert_eq!(detail.id, session.to_string());
    assert!(detail.usage.is_some());
    assert!(detail.requirements.is_some());
    assert_eq!(
        client.image("fixture", "test").await.unwrap().name,
        "fixture"
    );
    assert!(
        !client
            .models_filtered(None, None, false)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(!client.providers().await.unwrap().is_empty());
}

async fn assert_credential_entries(client: &swarmy_client::Client) {
    let record = swarmy_core::CredentialRecord {
        bookkeeping: swarmy_core::CredentialBookkeeping::default(),
        kind: swarmy_core::CredentialKind::ApiKey {
            key: "synthetic-test-value".into(),
            extra: std::collections::BTreeMap::from([("label".into(), "fixture".into())]),
        },
        updated_at: jiff::Timestamp::now(),
    };
    client
        .put_credential_record(&swarmy_api_types::PutCredentialRecord {
            idempotency_key: "credential".into(),
            provider: "test-provider".into(),
            label: "default".into(),
            record,
        })
        .await
        .unwrap();
    let summaries = client.credentials().await.unwrap();
    assert_eq!(summaries[0].provider, "test-provider");
    assert_eq!(summaries[0].kind, swarmy_api_types::CredentialKind::ApiKey);
    assert_eq!(summaries[0].label, "default");
    assert_eq!(
        client.credential("test-provider").await.unwrap().status,
        swarmy_api_types::CredentialStatus::Ready
    );
    let cloud = client
        .set_credential(&swarmy_api_types::CreateCredential {
            idempotency_key: "cloud".into(),
            provider: "test-provider".into(),
            kind: swarmy_api_types::CredentialKind::Cloud,
            label: "backup".into(),
            secret: "synthetic-cloud-secret".into(),
            extra: std::collections::BTreeMap::default(),
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
    let listed = client.credentials().await.unwrap();
    assert_eq!(listed.len(), 2);
    assert!(
        listed
            .iter()
            .any(|row| row.label == "backup" && row.kind == swarmy_api_types::CredentialKind::Cloud)
    );
    client
        .remove_credential_entry("test-provider", "backup", "remove-backup")
        .await
        .unwrap();
    assert_eq!(client.credentials().await.unwrap().len(), 1);
}
#[tokio::test]
async fn stopped_api_reports_endpoint_quickly() {
    let Some((client, _store, server)) = fixture().await else {
        return;
    };
    server.abort();
    let result = tokio::time::timeout(Duration::from_secs(2), client.agents(None, 10)).await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_err());
}

#[tokio::test]
async fn agent_management_uses_api_and_preserves_requirements() {
    let Some((client, store, server)) = fixture().await else {
        return;
    };
    let create = swarmy_api_types::CreateAgent {
        idempotency_key: "create".into(),
        name: "worker".into(),
        description: "test".into(),
        image: swarmy_api_types::ImageRef {
            name: "fixture".into(),
            tag: "test".into(),
        },
        provider: Some("fake".into()),
        model: Some("scripted".into()),
        effort: None,
        system_prompt: None,
        route: None,
        memory_mib: Some(2048),
        gpu: Some(swarmy_api_types::GpuMode::Shared),
        github_token: Some("synthetic-github-token".into()),
    };
    let created = client.create_agent(&create).await.unwrap();
    assert_eq!(created.name, "worker");
    // Create and show share the detail conversion, so the rows match apart
    // from the snapshot age, which is computed from the current time.
    let shown = client.agent("worker").await.unwrap();
    let mut created_value = serde_json::to_value(&created).unwrap();
    let mut shown_value = serde_json::to_value(shown).unwrap();
    for value in [&mut created_value, &mut shown_value] {
        value
            .as_object_mut()
            .expect("agent serializes to an object")
            .remove("last_snapshot_age_seconds");
    }
    assert_eq!(created_value, shown_value);
    assert!(
        !serde_json::to_string(&created)
            .unwrap()
            .contains("synthetic-github-token")
    );
    let id = swarmy_core::AgentId::from_ulid(created.id.parse().unwrap());
    assert_eq!(
        store.agent_github_token(id).await.unwrap().as_deref(),
        Some("synthetic-github-token")
    );
    assert_eq!(
        client
            .agent("worker")
            .await
            .unwrap()
            .requirements
            .memory_mib,
        2048
    );
    let replayed = client.create_agent(&create).await.unwrap();
    assert_eq!(replayed.id, created.id);
    client
        .update_agent(
            "worker",
            &swarmy_api_types::UpdateAgent {
                idempotency_key: "update".into(),
                description: None,
                provider: None,
                model: None,
                effort: None,
                system_prompt: None,
                route: None,
                memory_mib: Some(1024),
                gpu: Some(swarmy_api_types::GpuMode::None),
                resets: vec![
                    swarmy_api_types::AgentReset::Provider,
                    swarmy_api_types::AgentReset::Model,
                ],
                github_token: None,
                clear_github_token: false,
            },
        )
        .await
        .unwrap();
    let updated = client.agent("worker").await.unwrap();
    assert_eq!(updated.requirements.memory_mib, 1024);
    assert!(updated.provider.is_none());
    assert!(updated.model.is_none());
    assert_eq!(updated.id, created.id);
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
        .create_agent(
            "selection-test",
            "fixture:test",
            "",
            jiff::Timestamp::now(),
            None,
        )
        .await
        .unwrap();
    let error = client
        .update_agent(
            "selection-test",
            &swarmy_api_types::UpdateAgent {
                idempotency_key: "invalid-model".into(),
                description: None,
                provider: None,
                model: Some("bogus".into()),
                effort: None,
                system_prompt: None,
                route: None,
                memory_mib: None,
                gpu: None,
                resets: Vec::new(),
                github_token: None,
                clear_github_token: false,
            },
        )
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
