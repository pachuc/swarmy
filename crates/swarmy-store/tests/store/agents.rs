use super::*;
use swarmy_core::{AgentRecord, AgentSettings, ReasoningEffort, SessionKind};
use swarmy_store::{AgentSessionOptions, CreateAgentOptions};

#[tokio::test]
async fn named_agents_pin_images_enforce_names_and_retain_sessions_on_delete() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let image = image_fixture::image(store).await;
    let (a, b) = tokio::join!(
        store.create_agent("tommy", image, "coding", timestamp(0), None),
        store.create_agent("tommy", image, "coding", timestamp(0), None)
    );
    let agent = match (a, b) {
        (Ok(agent), Err(StoreError::Domain(swarmy_store::DomainError::AgentExists)))
        | (Err(StoreError::Domain(swarmy_store::DomainError::AgentExists)), Ok(agent)) => agent,
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
        store.create_agent("", image, "", timestamp(0), None).await,
        Err(StoreError::Domain(
            swarmy_store::DomainError::InvalidAgentName
        ))
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
        Err(StoreError::Domain(
            swarmy_store::DomainError::ComputerDeleted
        ))
    ));
    assert_ne!(
        store
            .create_agent("tommy", image, "", timestamp(1), None)
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
            .create_agent_session(
                id,
                Some(agent.agent_id),
                timestamp(0),
                Some(AgentSessionOptions {
                    image: Some(image),
                    ..Default::default()
                })
            )
            .await,
        Err(StoreError::Domain(
            swarmy_store::DomainError::NamedAgentImage
        ))
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
        .put_image("fixture", &ImageTag("test".into()), replacement, None)
        .await
        .unwrap();
    let session = store
        .create_agent_session(id, Some(agent.agent_id), timestamp(0), None)
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
    store.set_main_session(agent.agent_id, id).await.unwrap();
    assert!(matches!(
        store.close_session(id, timestamp(1)).await,
        Err(StoreError::Domain(
            swarmy_store::DomainError::MainSessionClose
        ))
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
async fn ephemeral_creation_and_closure() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let image = image_fixture::image(store).await;
    let id = SessionId::from_ulid(Ulid::generate());
    assert!(matches!(
        store
            .create_agent_session(id, None, timestamp(0), None)
            .await,
        Err(StoreError::Domain(
            swarmy_store::DomainError::SessionImageRequired
        ))
    ));
    let session = store
        .create_agent_session(
            id,
            None,
            timestamp(0),
            Some(AgentSessionOptions {
                image: Some(image),
                ..Default::default()
            }),
        )
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
    store.close_session(id, timestamp(1)).await.unwrap();
    store.close_session(id, timestamp(2)).await.unwrap();
    let closed = store.fetch_session(id).await.unwrap().unwrap();
    assert_eq!(closed.state, SessionState::Completed);
    assert!(closed.computer_deleted);
    assert!(matches!(
        store.create_session(&session, timestamp(0), image).await,
        Err(StoreError::Domain(swarmy_store::DomainError::SessionExists))
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
                .create_agent_session(
                    SessionId::from_ulid(Ulid::generate()),
                    None,
                    timestamp(0),
                    Some(AgentSessionOptions {
                        image: Some(image),
                        ..Default::default()
                    }),
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
        .create_agent("named", image, "", timestamp(0), None)
        .await
        .unwrap();
    let named = store
        .create_agent_session(
            SessionId::from_ulid(Ulid::generate()),
            Some(agent.agent_id),
            timestamp(0),
            None,
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

#[tokio::test]
async fn agent_inference_settings_create_and_independent_updates() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let image = image_fixture::image(store).await;
    let settings = AgentSettings {
        system_prompt: Some("  Review carefully.\n".into()),
        model: Some("agent-model".into()),
        reasoning_effort: Some(ReasoningEffort::High),
        provider: None,
        memory_mib: None,
        gpu: None,
        route: None,
    };
    let mut expected = store
        .create_agent(
            "custom",
            image,
            "reviewer",
            timestamp(0),
            Some(CreateAgentOptions {
                settings: Some(&settings),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    assert_eq!(expected.system_prompt, settings.system_prompt);
    assert_eq!(expected.model, settings.model);
    assert_eq!(expected.reasoning_effort, settings.reasoning_effort);
    assert_eq!(
        store.get_agent(expected.agent_id).await.unwrap(),
        Some(expected.clone())
    );
    for patch in [
        AgentSettings {
            system_prompt: Some(String::new()),
            ..Default::default()
        },
        AgentSettings {
            model: Some("other-model".into()),
            ..Default::default()
        },
        AgentSettings {
            reasoning_effort: Some(ReasoningEffort::None),
            ..Default::default()
        },
    ] {
        if let Some(prompt) = &patch.system_prompt {
            expected.system_prompt = Some(prompt.clone());
        }
        if let Some(model) = &patch.model {
            expected.model = Some(model.clone());
        }
        if let Some(effort) = patch.reasoning_effort {
            expected.reasoning_effort = Some(effort);
        }
        assert_eq!(
            store.set_agent(expected.agent_id, &patch).await.unwrap(),
            expected
        );
        assert_eq!(
            store.get_agent_by_name("custom").await.unwrap(),
            Some(expected.clone())
        );
        assert_eq!(
            store.list_agents(None, 64).await.unwrap(),
            [expected.clone()]
        );
    }
    concurrent_settings_and_rejected_updates(store, expected, image).await;
    test.cleanup().await;
}

async fn concurrent_settings_and_rejected_updates(
    store: &Store,
    mut expected: AgentRecord,
    image: &str,
) {
    // Transaction retries must preserve independent concurrent changes.
    let prompt = AgentSettings {
        system_prompt: Some("concurrent prompt".into()),
        ..Default::default()
    };
    let model = AgentSettings {
        model: Some("concurrent model".into()),
        ..Default::default()
    };
    let (a, b) = tokio::join!(
        store.set_agent(expected.agent_id, &prompt),
        store.set_agent(expected.agent_id, &model)
    );
    a.unwrap();
    b.unwrap();
    expected.system_prompt = prompt.system_prompt;
    expected.model = model.model;
    assert_eq!(
        store.get_agent(expected.agent_id).await.unwrap(),
        Some(expected.clone())
    );
    let oversized = AgentSettings {
        system_prompt: Some("x".repeat(100_000)),
        ..Default::default()
    };
    assert!(matches!(
        store.set_agent(expected.agent_id, &oversized).await,
        Err(StoreError::Storage(swarmy_store::StorageError::TooLarge))
    ));
    assert_eq!(
        store.get_agent(expected.agent_id).await.unwrap(),
        Some(expected.clone())
    );
    assert!(matches!(
        store
            .create_agent(
                "oversized",
                image,
                "",
                timestamp(0),
                Some(CreateAgentOptions {
                    settings: Some(&oversized),
                    ..Default::default()
                })
            )
            .await,
        Err(StoreError::Storage(swarmy_store::StorageError::TooLarge))
    ));
    assert!(
        store
            .get_agent_by_name("oversized")
            .await
            .unwrap()
            .is_none()
    );
    store.delete_agent(expected.agent_id).await.unwrap();
    assert!(matches!(
        store
            .set_agent(expected.agent_id, &AgentSettings::default())
            .await,
        Err(StoreError::Domain(swarmy_store::DomainError::AgentMissing))
    ));
}

#[tokio::test]
async fn main_session_creation_replacement_and_close_are_atomic() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let image = image_fixture::image(store).await;
    let agent = store
        .create_agent("main", image, "", timestamp(0), None)
        .await
        .unwrap();
    assert_eq!(agent.main_session, None);
    let side = store
        .create_agent_session(
            SessionId::from_ulid(Ulid::generate()),
            Some(agent.agent_id),
            timestamp(0),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .get_agent(agent.agent_id)
            .await
            .unwrap()
            .unwrap()
            .main_session,
        None
    );
    let (first, second) = tokio::join!(
        store.open_main_session(agent.agent_id, timestamp(0)),
        store.open_main_session(agent.agent_id, timestamp(0)),
    );
    let (first, created) = first.unwrap();
    let (second, also_created) = second.unwrap();
    assert_eq!(first, second);
    assert_ne!(created, also_created);
    assert_eq!(
        store
            .list_sessions_by_agent(agent.agent_id, None, 64)
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        store
            .get_agent_by_name("main")
            .await
            .unwrap()
            .unwrap()
            .main_session,
        Some(first)
    );
    store
        .set_main_session(agent.agent_id, side.session_id)
        .await
        .unwrap();
    assert_eq!(
        store
            .get_agent(agent.agent_id)
            .await
            .unwrap()
            .unwrap()
            .main_session,
        Some(side.session_id)
    );
    assert!(matches!(
        store.close_session(side.session_id, timestamp(1)).await,
        Err(StoreError::Domain(
            swarmy_store::DomainError::MainSessionClose
        ))
    ));
    store.close_session(first, timestamp(1)).await.unwrap();
    store.close_session(first, timestamp(1)).await.unwrap();
    let closed = store.fetch_session(first).await.unwrap().unwrap();
    assert_eq!(closed.state, SessionState::Completed);
    assert!(!closed.computer_deleted);
    store
        .ensure_session_computer(side.session_id)
        .await
        .unwrap();
    assert!(matches!(
        store.set_main_session(agent.agent_id, first).await,
        Err(StoreError::Domain(
            swarmy_store::DomainError::InvalidMainSession
        ))
    ));
    assert_eq!(
        store
            .open_main_session(agent.agent_id, timestamp(1))
            .await
            .unwrap(),
        (side.session_id, false)
    );
    test.cleanup().await;
}

#[tokio::test]
async fn main_pointer_rejects_foreign_sessions_and_races_with_close() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let image = image_fixture::image(store).await;
    let agent = store
        .create_agent("main", image, "", timestamp(0), None)
        .await
        .unwrap();
    let other = store
        .create_agent("other", image, "", timestamp(0), None)
        .await
        .unwrap();
    for owner in [None, Some(other.agent_id)] {
        let foreign = store
            .create_agent_session(
                SessionId::from_ulid(Ulid::generate()),
                owner,
                timestamp(0),
                Some(AgentSessionOptions {
                    image: owner.is_none().then_some(image),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        assert!(matches!(
            store
                .set_main_session(agent.agent_id, foreign.session_id)
                .await,
            Err(StoreError::Domain(
                swarmy_store::DomainError::InvalidMainSession
            ))
        ));
    }
    let side = store
        .create_agent_session(
            SessionId::from_ulid(Ulid::generate()),
            Some(agent.agent_id),
            timestamp(0),
            None,
        )
        .await
        .unwrap();
    let (set, close) = tokio::join!(
        store.set_main_session(agent.agent_id, side.session_id),
        store.close_session(side.session_id, timestamp(1)),
    );
    assert!(matches!(
        (set, close),
        (
            Ok(()),
            Err(StoreError::Domain(
                swarmy_store::DomainError::MainSessionClose
            ))
        ) | (
            Err(StoreError::Domain(
                swarmy_store::DomainError::InvalidMainSession
            )),
            Ok(())
        )
    ));
    test.cleanup().await;
}

#[tokio::test]
async fn gateway_advertisements_expire() {
    let Some(test) = TestStore::new(Arc::new(MemoryBlobStore::default())) else {
        return;
    };
    let store = &test.store;
    assert!(!store.gateway_serves("openai").await.unwrap());
    store
        .put_gateway_provider(
            "openai",
            &swarmy_store::GatewayProvider {
                expires_at: Timestamp::UNIX_EPOCH,
                reason: "expired".into(),
            },
        )
        .await
        .unwrap();
    assert!(!store.gateway_serves("openai").await.unwrap());
    store
        .put_gateway_provider(
            "openai",
            &swarmy_store::GatewayProvider {
                expires_at: Timestamp::now()
                    .checked_add(std::time::Duration::from_secs(60))
                    .unwrap(),
                reason: "credentials resolved".into(),
            },
        )
        .await
        .unwrap();
    assert!(store.gateway_serves("openai").await.unwrap());
    assert_eq!(
        store
            .gateway_provider("openai")
            .await
            .unwrap()
            .unwrap()
            .reason,
        "credentials resolved"
    );
    assert!(!store.gateway_serves("anthropic").await.unwrap());
    test.cleanup().await;
}

#[tokio::test]
async fn agent_memory_and_gpu_requirements_are_durable() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let image = image_fixture::image(&test.store).await;
    let settings = AgentSettings {
        memory_mib: Some(2048),
        gpu: Some(swarmy_core::GpuRequirement::Shared),
        route: None,
        ..Default::default()
    };
    let agent = test
        .store
        .create_agent(
            "memory",
            image,
            "",
            timestamp(0),
            Some(CreateAgentOptions {
                settings: Some(&settings),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    assert_eq!(agent.requirements.memory_mib, 2048);
    assert_eq!(agent.requirements.gpu, swarmy_core::GpuRequirement::Shared);
    let updated = test
        .store
        .set_agent(
            agent.agent_id,
            &AgentSettings {
                memory_mib: Some(3072),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.requirements.memory_mib, 3072);
    assert_eq!(
        updated.requirements.gpu,
        swarmy_core::GpuRequirement::Shared
    );
    assert_eq!(
        test.store.get_agent(agent.agent_id).await.unwrap(),
        Some(updated)
    );
    test.cleanup().await;
}
