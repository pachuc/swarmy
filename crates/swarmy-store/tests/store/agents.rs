use super::*;
use swarmy_core::SessionKind;

#[tokio::test]
async fn named_agents_pin_images_enforce_names_and_retain_sessions_on_delete() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let image = image_fixture::image(store).await;
    let (a, b) = tokio::join!(
        store.create_agent("tommy", image, "coding", timestamp(0)),
        store.create_agent("tommy", image, "coding", timestamp(0))
    );
    let agent = match (a, b) {
        (Ok(agent), Err(StoreError::AgentExists)) | (Err(StoreError::AgentExists), Ok(agent)) => {
            agent
        }
        other => panic!("uniqueness failed: {other:?}"),
    };
    assert_eq!(
        store.get_agent(agent.agent_id).await.unwrap(),
        Some(agent.clone())
    );
    assert_eq!(
        store.get_agent_by_name("tommy").await.unwrap(),
        Some(agent.clone())
    );
    assert_eq!(
        store.list_agents(None, 1).await.unwrap(),
        vec![agent.clone()]
    );
    assert!(
        store
            .list_agents(Some(agent.agent_id), 1)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        store.create_agent("", image, "", timestamp(0)).await,
        Err(StoreError::InvalidAgentName)
    ));
    let id = assert_named_session_pin(store, &agent, image).await;
    store
        .append_events(id, 0, &[event("retained transcript")])
        .await
        .unwrap();
    store.delete_agent(agent.agent_id).await.unwrap();
    store.delete_agent(agent.agent_id).await.unwrap();
    assert!(store.get_agent_by_name("tommy").await.unwrap().is_none());
    assert!(store.get_agent(agent.agent_id).await.unwrap().is_none());
    assert!(
        store
            .fetch_session(id)
            .await
            .unwrap()
            .unwrap()
            .computer_deleted
    );
    assert_eq!(store.read_events(id, 0, 10).await.unwrap().len(), 1);
    assert!(matches!(
        store.ensure_session_computer(id).await,
        Err(StoreError::ComputerDeleted)
    ));
    assert_ne!(
        store
            .create_agent("tommy", image, "", timestamp(1))
            .await
            .unwrap()
            .agent_id,
        agent.agent_id
    );
    test.cleanup().await;
}

async fn assert_named_session_pin(
    store: &Store,
    agent: &swarmy_core::AgentRecord,
    image: &str,
) -> SessionId {
    let id = SessionId::from_ulid(Ulid::generate());
    assert!(matches!(
        store
            .create_session_for_agent(id, Some(agent.agent_id), Some(image), timestamp(0))
            .await,
        Err(StoreError::NamedAgentImage)
    ));
    assert!(store.fetch_session(id).await.unwrap().is_none());
    let replacement = ManifestId::from_ulid(Ulid::generate());
    store
        .put_manifest(
            replacement,
            &ManifestHeader {
                size: u64::from(CHUNK_SIZE),
                chunk_size: CHUNK_SIZE,
                root_hash: ContentHash::ZERO,
            },
        )
        .await
        .unwrap();
    store
        .put_image("fixture", &ImageTag("test".into()), replacement)
        .await
        .unwrap();
    let session = store
        .create_session_for_agent(id, Some(agent.agent_id), None, timestamp(0))
        .await
        .unwrap();
    assert_eq!(
        session.kind,
        SessionKind::Named {
            agent_id: agent.agent_id
        }
    );
    assert_eq!(
        store.session_image(id).await.unwrap(),
        Some(agent.image.manifest_id)
    );
    assert!(
        store
            .live_manifests()
            .await
            .unwrap()
            .contains(&agent.image.manifest_id)
    );
    assert!(matches!(
        store.close_session(id, timestamp(1)).await,
        Err(StoreError::NamedSessionClose)
    ));
    assert_eq!(
        store
            .list_sessions_by_agent(agent.agent_id, None, 1)
            .await
            .unwrap(),
        vec![session]
    );
    assert!(
        store
            .list_sessions_by_agent(agent.agent_id, Some(id), 1)
            .await
            .unwrap()
            .is_empty()
    );
    id
}

#[tokio::test]
async fn ephemeral_creation_closure_and_legacy_headers() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let image = image_fixture::image(store).await;
    let id = SessionId::from_ulid(Ulid::generate());
    assert!(matches!(
        store
            .create_session_for_agent(id, None, None, timestamp(0))
            .await,
        Err(StoreError::SessionImageRequired)
    ));
    let session = store
        .create_session_for_agent(id, None, Some(image), timestamp(0))
        .await
        .unwrap();
    assert_eq!(session.kind, SessionKind::Ephemeral);
    assert_eq!(
        store
            .list_sessions_by_agent(session.agent_id, None, 10)
            .await
            .unwrap(),
        vec![session.clone()]
    );
    // Simulate an old writer with no kind row and the unchanged session header.
    let key = test
        .root
        .pack(&("session_kind", id.as_ulid().to_bytes().as_slice()));
    test.db
        .run(|trx, _| {
            let key = &key;
            async move {
                trx.clear(key);
                Ok(())
            }
        })
        .await
        .unwrap();
    assert_eq!(
        store.fetch_session(id).await.unwrap().unwrap().kind,
        SessionKind::Ephemeral
    );
    store.close_session(id, timestamp(1)).await.unwrap();
    store.close_session(id, timestamp(2)).await.unwrap();
    let closed = store.fetch_session(id).await.unwrap().unwrap();
    assert_eq!(closed.state, SessionState::Completed);
    assert!(closed.computer_deleted);
    assert!(matches!(
        store.create_session(&session, timestamp(0), image).await,
        Err(StoreError::SessionExists)
    ));
    test.cleanup().await;
}

#[tokio::test]
async fn sweep_rechecks_activity_and_protects_named_sessions() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let image = image_fixture::image(store).await;
    let mut sessions = Vec::new();
    for _ in 0..3 {
        sessions.push(
            store
                .create_session_for_agent(
                    SessionId::from_ulid(Ulid::generate()),
                    None,
                    Some(image),
                    timestamp(0),
                )
                .await
                .unwrap(),
        );
    }
    store
        .wake_session(sessions[1].session_id, timestamp(9))
        .await
        .unwrap();
    store
        .wake_session(sessions[2].session_id, timestamp(8))
        .await
        .unwrap();
    let lease = store
        .claim_lease(sessions[2].session_id, owner(), timestamp(100))
        .await
        .unwrap();
    store
        .set_state(
            sessions[2].session_id,
            SessionState::Idle,
            Some(&lease),
            timestamp(9),
        )
        .await
        .unwrap();
    let agent = store
        .create_agent("named", image, "", timestamp(0))
        .await
        .unwrap();
    let named = store
        .create_session_for_agent(
            SessionId::from_ulid(Ulid::generate()),
            Some(agent.agent_id),
            None,
            timestamp(0),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .sweep_ephemeral_sessions(timestamp(10), std::time::Duration::from_secs(2))
            .await
            .unwrap(),
        1
    );
    assert!(
        store
            .fetch_session(sessions[0].session_id)
            .await
            .unwrap()
            .unwrap()
            .computer_deleted
    );
    for session in [&sessions[1], &sessions[2], &named] {
        assert!(
            !store
                .fetch_session(session.session_id)
                .await
                .unwrap()
                .unwrap()
                .computer_deleted
        );
    }
    test.cleanup().await;
}
