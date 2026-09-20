use super::*;

#[tokio::test]
async fn turn_boundaries_are_atomic_and_fence_expired_and_replaced_workers() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let session = session();
    let id = session.session_id;
    store
        .create_session(
            &session,
            Timestamp::now(),
            image_fixture::image(store).await,
        )
        .await
        .unwrap();
    let expired = store
        .claim_lease(id, owner(), Timestamp::UNIX_EPOCH)
        .await
        .unwrap();
    let record = InflightRecord {
        session_id: id,
        seq: 1,
        provider: "fake".into(),
        key_id: String::new(),
    };
    let snapshot = SnapshotRef {
        seq: 1,
        object_key: "test-snapshot".into(),
    };
    {
        let lease = &expired;
        assert!(matches!(
            store.submit_inference(0, lease, &record, &"input").await,
            Err(StoreError::LeaseMismatch)
        ));
        assert!(matches!(
            store.finish_turn(id, 0, lease, &snapshot).await,
            Err(StoreError::LeaseMismatch)
        ));
    }
    store
        .reap_lease(id, &expired, Timestamp::now())
        .await
        .unwrap();
    let live = store
        .claim_lease(
            id,
            owner(),
            Timestamp::now()
                .checked_add(std::time::Duration::from_secs(60))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        store.submit_inference(0, &expired, &record, &"input").await,
        Err(StoreError::LeaseMismatch)
    ));
    assert!(matches!(
        store.finish_turn(id, 0, &expired, &snapshot).await,
        Err(StoreError::LeaseMismatch)
    ));
    assert!(store.read_events(id, 0, 64).await.unwrap().is_empty());
    assert!(store.scan_inflight(None, 64).await.unwrap().is_empty());
    assert!(
        store
            .fetch_session(id)
            .await
            .unwrap()
            .unwrap()
            .snapshot_ref
            .is_none()
    );
    store
        .release_lease(id, &live, Timestamp::now())
        .await
        .unwrap();
    test.cleanup().await;
}

#[tokio::test]
async fn submission_and_idle_commit_all_records_together() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let id = test.create().await;
    let live = store
        .claim_lease(
            id,
            owner(),
            Timestamp::now()
                .checked_add(std::time::Duration::from_secs(60))
                .unwrap(),
        )
        .await
        .unwrap();
    let record = InflightRecord {
        session_id: id,
        seq: 1,
        provider: "fake".into(),
        key_id: String::new(),
    };
    let snapshot = SnapshotRef {
        seq: 1,
        object_key: "test-snapshot".into(),
    };
    let event = store
        .submit_inference(0, &live, &record, &"input")
        .await
        .unwrap();
    assert_eq!(event.seq(), 1);
    let request = RequestId::for_step(id, 1);
    assert_eq!(
        store
            .get_inference_input::<String>(request)
            .await
            .unwrap()
            .as_deref(),
        Some("input")
    );
    assert_eq!(store.scan_inflight(None, 64).await.unwrap(), vec![record]);
    assert_eq!(
        store.fetch_session(id).await.unwrap().unwrap().state,
        SessionState::WaitingInference
    );
    assert!(matches!(
        store.finish_turn(id, 0, &live, &snapshot).await,
        Err(StoreError::LeaseMismatch)
    ));
    store
        .set_state(id, SessionState::Runnable, None, Timestamp::now())
        .await
        .unwrap();
    let next = store
        .claim_lease(
            id,
            owner(),
            Timestamp::now()
                .checked_add(std::time::Duration::from_secs(60))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        store.finish_turn(id, 0, &next, &snapshot).await,
        Err(StoreError::StaleSequence { .. })
    ));
    let snapshot = SnapshotRef { seq: 2, ..snapshot };
    let idle = store.finish_turn(id, 1, &next, &snapshot).await.unwrap();
    assert_eq!(idle.seq(), 2);
    let finished = store.fetch_session(id).await.unwrap().unwrap();
    assert_eq!(finished.state, SessionState::Idle);
    assert_eq!(finished.snapshot_ref, Some(snapshot));
    assert_eq!(finished.head_seq, 2);
    assert_eq!(store.read_events(id, 1, 64).await.unwrap(), vec![idle]);
    assert!(matches!(
        store.release_lease(id, &next, Timestamp::now()).await,
        Err(StoreError::LeaseMismatch)
    ));
    test.cleanup().await;
}

#[tokio::test]
async fn concurrent_user_appends_admit_one_message_and_index_it_atomically() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let session = SessionRecord {
        state: SessionState::Idle,
        ..session()
    };
    let id = session.session_id;
    store
        .create_session(
            &session,
            Timestamp::now(),
            image_fixture::image(store).await,
        )
        .await
        .unwrap();
    let Event::MessageAppended { message: first, .. } = event("first") else {
        unreachable!()
    };
    let Event::MessageAppended {
        message: second, ..
    } = event("second")
    else {
        unreachable!()
    };
    let (a, b) = tokio::join!(
        store.append_user_message(id, 0, &first),
        store.append_user_message(id, 0, &second)
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let winner = if a.is_ok() { first } else { second };
    let events = store.read_events(id, 0, 64).await.unwrap();
    assert_eq!(
        events,
        vec![Event::MessageAppended {
            seq: 1,
            message: winner.clone()
        }]
    );
    assert_eq!(store.turn_id(id).await.unwrap(), Some(winner.id));
    assert_eq!(
        store.fetch_session(id).await.unwrap().unwrap().state,
        SessionState::Runnable
    );
    let entries = store
        .scan_runnable(runnable_partition(id), None, 64)
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].session_id, id);
    let (_, claimed, turn, tail) = store
        .claim_step_with_tail(
            id,
            owner(),
            Timestamp::now()
                .checked_add(std::time::Duration::from_secs(30))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(claimed.head_seq, 1);
    assert_eq!(claimed.state, SessionState::Leased);
    assert_eq!(turn, Some(winner.id));
    assert_eq!(tail, store.read_events(id, 0, 64).await.unwrap());
    assert!(store.append_user_message(id, 1, &winner).await.is_err());
    test.cleanup().await;
}

#[tokio::test]
async fn tool_fold_and_inference_share_the_lease_fence_and_commit() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let id = test.create().await;
    let lease = store
        .claim_lease(
            id,
            owner(),
            Timestamp::now()
                .checked_add(std::time::Duration::from_secs(60))
                .unwrap(),
        )
        .await
        .unwrap();
    let mut folded = event("tool output");
    if let Event::MessageAppended { message, .. } = &mut folded {
        message.role = swarmy_core::MessageRole::Tool;
    }
    let record = InflightRecord {
        session_id: id,
        seq: 2,
        provider: "fake".into(),
        key_id: String::new(),
    };
    let stale = swarmy_core::Lease {
        owner: owner(),
        ..lease.clone()
    };
    assert!(matches!(
        store
            .submit_inference_after(0, &stale, &record, &"input", &[folded.clone()])
            .await,
        Err(StoreError::LeaseMismatch)
    ));
    assert!(store.read_events(id, 0, 64).await.unwrap().is_empty());
    assert!(store.scan_inflight(None, 64).await.unwrap().is_empty());
    let request = store
        .submit_inference_after(0, &lease, &record, &"input", &[folded.clone()])
        .await
        .unwrap();
    folded.set_seq(1);
    assert_eq!(
        store.read_events(id, 0, 64).await.unwrap(),
        vec![folded, request]
    );
    assert_eq!(
        store.fetch_session(id).await.unwrap().unwrap().state,
        SessionState::WaitingInference
    );
    assert_eq!(
        store
            .get_inference_input::<String>(RequestId::for_step(id, 2))
            .await
            .unwrap()
            .as_deref(),
        Some("input")
    );
    test.cleanup().await;
}

#[tokio::test]
async fn terminal_inference_commits_response_snapshot_and_idle_under_its_claim() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let id = test.create().await;
    let mut completion = terminal_completion(store, id).await;
    let snapshot = SnapshotRef {
        seq: 3,
        object_key: "terminal-snapshot".into(),
    };
    // Even a caller timestamp from before expiry must not authorize a terminal commit.
    assert!(matches!(
        store
            .complete_inference_and_idle(&completion, &"answer", &snapshot)
            .await,
        Err(StoreError::LeaseMismatch)
    ));
    completion.claim.owner = owner();
    completion.claim.expires_at = Timestamp::now()
        .checked_add(std::time::Duration::from_secs(60))
        .unwrap();
    completion.now = Timestamp::now();
    assert!(
        store
            .start_inference(&completion.claim, completion.now)
            .await
            .unwrap()
    );
    let stale = swarmy_store::InferenceCompletion {
        claim: swarmy_store::InferenceClaim {
            owner: owner(),
            ..completion.claim.clone()
        },
        expected_head: completion.expected_head,
        event: completion.event.clone(),
        now: completion.now,
    };
    assert!(matches!(
        store
            .complete_inference_and_idle(&stale, &"answer", &snapshot)
            .await,
        Err(StoreError::LeaseMismatch)
    ));
    assert_eq!(store.read_events(id, 0, 64).await.unwrap().len(), 1);
    assert!(
        store
            .complete_inference_and_idle(&completion, &"answer", &snapshot)
            .await
            .unwrap()
    );
    assert!(
        !store
            .complete_inference_and_idle(&completion, &"duplicate", &snapshot)
            .await
            .unwrap()
    );
    let finished = store.fetch_session(id).await.unwrap().unwrap();
    assert_eq!(finished.head_seq, 3);
    assert_eq!(finished.state, SessionState::Idle);
    assert_eq!(finished.snapshot_ref, Some(snapshot));
    assert_eq!(
        store
            .get_inference_result::<String>(completion.claim.request_id)
            .await
            .unwrap()
            .as_deref(),
        Some("answer")
    );
    let events = store.read_events(id, 1, 64).await.unwrap();
    assert_eq!(events.len(), 2);
    assert!(matches!(
        events[0],
        Event::InferenceCompleted { seq: 2, .. }
    ));
    assert!(matches!(
        events[1],
        Event::StateChanged {
            seq: 3,
            to: SessionState::Idle,
            ..
        }
    ));
    assert!(store.scan_inflight(None, 64).await.unwrap().is_empty());
    test.cleanup().await;
}

async fn terminal_completion(store: &Store, id: SessionId) -> swarmy_store::InferenceCompletion {
    let lease = store
        .claim_lease(
            id,
            owner(),
            Timestamp::now()
                .checked_add(std::time::Duration::from_secs(60))
                .unwrap(),
        )
        .await
        .unwrap();
    let record = InflightRecord {
        session_id: id,
        seq: 1,
        provider: "fake".into(),
        key_id: String::new(),
    };
    store
        .submit_inference(0, &lease, &record, &"input")
        .await
        .unwrap();
    let claim = swarmy_store::InferenceClaim {
        session_id: id,
        request_id: RequestId::for_step(id, 1),
        owner: owner(),
        expires_at: Timestamp::now()
            .checked_sub(std::time::Duration::from_secs(1))
            .unwrap(),
    };
    let now = claim
        .expires_at
        .checked_sub(std::time::Duration::from_secs(1))
        .unwrap();
    assert!(store.start_inference(&claim, now).await.unwrap());
    let Event::MessageAppended { mut message, .. } = event("answer") else {
        unreachable!()
    };
    message.role = swarmy_core::MessageRole::Assistant;
    swarmy_store::InferenceCompletion {
        expected_head: 1,
        event: Event::InferenceCompleted {
            provider: String::new(),
            model: String::new(),
            effort_used: None,
            usage: swarmy_core::TokenUsage::default(),
            cost_micros: 0,
            effort_requested: None,
            effort_clamped: false,
            seq: 0,
            request_id: claim.request_id,
            message,
        },
        claim,
        now,
    }
}
