use super::*;
use swarmy_store::SubmitInferenceOptions;

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
            swarmy_testkit::image(store).await,
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
            store
                .submit_inference::<_, ()>(0, lease, &record, &"input", None)
                .await,
            Err(StoreError::Fence(swarmy_store::FenceError::LeaseMismatch))
        ));
        assert!(matches!(
            store.finish_turn(id, 0, lease, &snapshot).await,
            Err(StoreError::Fence(swarmy_store::FenceError::LeaseMismatch))
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
        store
            .submit_inference::<_, ()>(0, &expired, &record, &"input", None)
            .await,
        Err(StoreError::Fence(swarmy_store::FenceError::LeaseMismatch))
    ));
    assert!(matches!(
        store.finish_turn(id, 0, &expired, &snapshot).await,
        Err(StoreError::Fence(swarmy_store::FenceError::LeaseMismatch))
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
        .set_state(id, SessionState::Runnable, Some(&live), Timestamp::now())
        .await
        .unwrap();
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
        .submit_inference::<_, ()>(0, &live, &record, &"input", None)
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
        Err(StoreError::Fence(swarmy_store::FenceError::LeaseMismatch))
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
        Err(StoreError::Fence(
            swarmy_store::FenceError::StaleSequence { .. }
        ))
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
        store
            .set_state(id, SessionState::Runnable, Some(&next), Timestamp::now())
            .await,
        Err(StoreError::Fence(swarmy_store::FenceError::LeaseMismatch))
    ));
}

#[tokio::test]
async fn concurrent_user_appends_admit_one_message_and_index_it_atomically() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let session = SessionRecord {
        interrupt_requested: false,
        state: SessionState::Idle,
        ..session()
    };
    let id = session.session_id;
    store
        .create_session(
            &session,
            Timestamp::now(),
            swarmy_testkit::image(store).await,
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
        message.role = MessageRole::Tool;
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
            .submit_inference::<_, ()>(
                0,
                &stale,
                &record,
                &"input",
                Some(SubmitInferenceOptions {
                    before: &[folded.clone()],
                    ..Default::default()
                })
            )
            .await,
        Err(StoreError::Fence(swarmy_store::FenceError::LeaseMismatch))
    ));
    assert!(store.read_events(id, 0, 64).await.unwrap().is_empty());
    assert!(store.scan_inflight(None, 64).await.unwrap().is_empty());
    let request = store
        .submit_inference::<_, ()>(
            0,
            &lease,
            &record,
            &"input",
            Some(SubmitInferenceOptions {
                before: &[folded.clone()],
                ..Default::default()
            }),
        )
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
        Err(StoreError::Fence(swarmy_store::FenceError::LeaseMismatch))
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
        entry: completion.entry.clone(),
        entry_kind: completion.entry_kind,
        quota_remaining: completion.quota_remaining.clone(),
        quota_resets: completion.quota_resets.clone(),
    };
    assert!(matches!(
        store
            .complete_inference_and_idle(&stale, &"answer", &snapshot)
            .await,
        Err(StoreError::Fence(swarmy_store::FenceError::LeaseMismatch))
    ));
    assert_eq!(store.read_events(id, 0, 64).await.unwrap().len(), 1);
    assert!(
        store
            .complete_inference_and_idle(&completion, &"answer", &snapshot)
            .await
            .unwrap()
    );
    let attribution = store
        .inference_usage_record(completion.claim.request_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(attribution.entry.as_deref(), Some("primary"));
    assert_eq!(attribution.provider, "");
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
        .submit_inference::<_, ()>(0, &lease, &record, &"input", None)
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
    message.role = MessageRole::Assistant;
    swarmy_store::InferenceCompletion {
        expected_head: 1,
        entry: Some("primary".into()),
        entry_kind: None,
        quota_remaining: std::collections::BTreeMap::new(),
        quota_resets: std::collections::BTreeMap::new(),
        event: Event::InferenceCompleted {
            seq: 0,
            request_id: claim.request_id,
            completion: swarmy_core::InferenceCompletion {
                message,
                provider: String::new(),
                model: String::new(),
                effort_used: None,
                usage: swarmy_core::TokenUsage::default(),
                cost_micros: 0,
                effort_requested: None,
                effort_clamped: false,
                entry: None,
                route: None,
                route_step: None,
            },
        },
        claim,
        now,
    }
}

#[tokio::test]
async fn queued_input_survives_a_claim_and_is_delivered_only_once() {
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
            swarmy_testkit::image(store).await,
        )
        .await
        .unwrap();
    let Event::MessageAppended { message: first, .. } = event("first") else {
        unreachable!()
    };
    store.append_user_message(id, 0, &first).await.unwrap();
    let (lease, _, _, _) = store
        .claim_step_with_tail(
            id,
            owner(),
            Timestamp::now()
                .checked_add(std::time::Duration::from_secs(60))
                .unwrap(),
        )
        .await
        .unwrap();
    let Event::MessageAppended { message, .. } = event("queued") else {
        unreachable!()
    };
    assert_eq!(
        store
            .queue_user_message_idempotent(id, &message, "queued-once")
            .await
            .unwrap(),
        swarmy_store::UserMessageAppend {
            sequence: 1,
            fresh: true,
            started: false
        }
    );
    assert_eq!(
        store
            .queue_user_message_idempotent(id, &message, "queued-once")
            .await
            .unwrap(),
        swarmy_store::UserMessageAppend {
            sequence: 1,
            fresh: false,
            started: false
        }
    );
    assert_eq!(store.read_events(id, 0, 10).await.unwrap().len(), 1);
    assert!(matches!(
        store
            .finish_turn(
                id,
                1,
                &lease,
                &SnapshotRef {
                    seq: 2,
                    object_key: "checkpoint".into()
                }
            )
            .await,
        Err(StoreError::Domain(
            swarmy_store::DomainError::QueuedInputPending
        ))
    ));
    // Simulate a worker exit and a new claim before the queued input is drained.
    store
        .set_state(id, SessionState::Runnable, Some(&lease), Timestamp::now())
        .await
        .unwrap();
    let (new_lease, _, _, _) = store
        .claim_step_with_tail(
            id,
            owner(),
            Timestamp::now()
                .checked_add(std::time::Duration::from_secs(60))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_ne!(lease, new_lease);
    assert_queued_pair_then_empty(store, id, &new_lease, 1).await;
}

async fn assert_queued_pair_then_empty(
    store: &Store,
    id: SessionId,
    lease: &swarmy_core::Lease,
    head: u64,
) {
    let delivered = store
        .deliver_queued(id, head, lease, &[])
        .await
        .unwrap()
        .events;
    assert!(matches!(
        &delivered[..],
        [Event::MessageQueued { .. }, Event::MessageAppended { .. }]
    ));
    assert_eq!(&store.read_events(id, head, 10).await.unwrap(), &delivered);
    assert!(
        store
            .deliver_queued(id, head + 2, lease, &[])
            .await
            .unwrap()
            .events
            .is_empty()
    );
}

#[tokio::test]
async fn queued_during_terminal_inference_starts_next_step_in_order() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let id = test.create().await;
    let mut completion = terminal_completion(store, id).await;
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
    let Event::MessageAppended { message, .. } = event("queued before answer") else {
        unreachable!()
    };
    store
        .queue_user_message_idempotent(id, &message, "during-answer")
        .await
        .unwrap();
    let snapshot = SnapshotRef {
        seq: 3,
        object_key: "terminal-snapshot".into(),
    };
    assert!(
        store
            .complete_inference_and_idle(&completion, &"answer", &snapshot)
            .await
            .unwrap()
    );
    let session = store.fetch_session(id).await.unwrap().unwrap();
    assert_eq!(session.state, SessionState::Runnable);
    assert_eq!(session.head_seq, 2);
    let (lease, _, _, _) = store
        .claim_step_with_tail(
            id,
            owner(),
            Timestamp::now()
                .checked_add(std::time::Duration::from_secs(60))
                .unwrap(),
        )
        .await
        .unwrap();
    let delivered = store
        .deliver_queued(id, 2, &lease, &[])
        .await
        .unwrap()
        .events;
    assert!(
        matches!(&delivered[..], [Event::MessageQueued { .. }, Event::MessageAppended { message: next, .. }] if next == &message)
    );
    assert!(
        store
            .deliver_queued(id, 4, &lease, &[])
            .await
            .unwrap()
            .events
            .is_empty()
    );
}

#[tokio::test]
async fn queued_rows_do_not_corrupt_session_listing_and_large_bodies_use_blobs() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let id = test.create().await;
    let Event::MessageAppended { message, .. } = event(&"large".repeat(25_000)) else {
        unreachable!()
    };
    store
        .queue_user_message_idempotent(id, &message, "large-queued")
        .await
        .unwrap();
    assert!(
        store
            .list_sessions(None, 64)
            .await
            .unwrap()
            .iter()
            .any(|item| item.session_id == id)
    );
    let (lease, _, _, _) = store
        .claim_step_with_tail(
            id,
            owner(),
            Timestamp::now()
                .checked_add(std::time::Duration::from_secs(60))
                .unwrap(),
        )
        .await
        .unwrap();
    let delivered = store
        .deliver_queued(id, 0, &lease, &[])
        .await
        .unwrap()
        .events;
    assert!(
        matches!(&delivered[1], Event::MessageAppended { message: next, .. } if next == &message)
    );
}

#[tokio::test]
async fn queued_input_survives_an_interrupt_before_newer_input() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let id = test.create().await;
    let Event::MessageAppended { message, .. } = event("before interrupt") else {
        unreachable!()
    };
    store
        .queue_user_message_idempotent(id, &message, "before-interrupt")
        .await
        .unwrap();
    store.interrupt_session(id).await.unwrap();
    assert!(store.finish_runnable_interrupt(id).await.unwrap());
    assert_eq!(
        store.fetch_session(id).await.unwrap().unwrap().state,
        SessionState::Runnable
    );
    let (lease, session, _, _) = store
        .claim_step_with_tail(
            id,
            owner(),
            Timestamp::now()
                .checked_add(std::time::Duration::from_secs(60))
                .unwrap(),
        )
        .await
        .unwrap();
    let events = store
        .deliver_queued(id, session.head_seq, &lease, &[])
        .await
        .unwrap()
        .events;
    assert!(matches!(&events[1], Event::MessageAppended { message: next, .. } if next == &message));
}

#[tokio::test]
async fn queued_input_is_delivered_in_bounded_ordered_batches() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let id = test.create().await;
    for index in 0..12 {
        let Event::MessageAppended { message, .. } =
            event(&format!("{index}:{}", "x".repeat(80_000)))
        else {
            unreachable!()
        };
        store
            .queue_user_message_idempotent(id, &message, &format!("batch-{index}"))
            .await
            .unwrap();
    }
    let (lease, session, _, _) = store
        .claim_step_with_tail(
            id,
            owner(),
            Timestamp::now()
                .checked_add(std::time::Duration::from_secs(60))
                .unwrap(),
        )
        .await
        .unwrap();
    let first = store
        .deliver_queued(id, session.head_seq, &lease, &[])
        .await
        .unwrap();
    assert!(first.events.len() < 24);
    let second = store
        .deliver_queued(id, first.head_seq, &lease, &[])
        .await
        .unwrap();
    assert!(!second.events.is_empty());
    let all = first
        .events
        .iter()
        .chain(&second.events)
        .filter_map(|event| {
            if let Event::MessageAppended { message, .. } = event {
                Some(message)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(all.len(), 12);
    for (index, message) in all.iter().enumerate() {
        assert!(
            matches!(&message.parts[0], Part::Text { text } if text.starts_with(&format!("{index}:")))
        );
    }
    assert!(
        store
            .deliver_queued(id, 24, &lease, &[])
            .await
            .unwrap()
            .events
            .is_empty()
    );
}

async fn live_lease(store: &Store, id: SessionId) -> swarmy_core::Lease {
    store
        .claim_lease(
            id,
            owner(),
            Timestamp::now()
                .checked_add(std::time::Duration::from_secs(60))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn drain_transferred_once(store: &Store, id: SessionId, expected: &Message) {
    // The successor carries waiting input as runnable state; draining it must
    // yield the queued marker plus the user message exactly once, then nothing.
    let session = store.fetch_session(id).await.unwrap().unwrap();
    assert_eq!(session.state, SessionState::Runnable);
    let lease = live_lease(store, id).await;
    let head = session.head_seq;
    let delivery = store.deliver_queued(id, head, &lease, &[]).await.unwrap();
    assert_eq!(delivery.head_seq, head + 2);
    let delivered = delivery.events;
    assert_eq!(delivered.len(), 2, "{delivered:?}");
    let (
        Event::MessageQueued {
            message: queued, ..
        },
        Event::MessageAppended {
            message: appended, ..
        },
    ) = (&delivered[0], &delivered[1])
    else {
        panic!("expected queued marker plus user message: {delivered:?}");
    };
    assert_eq!(queued, expected);
    assert_eq!(appended, expected);
    let follow_up = store
        .deliver_queued(id, head + 2, &lease, &[])
        .await
        .unwrap();
    assert_eq!(follow_up.head_seq, head + 2);
    assert!(follow_up.events.is_empty());
}

#[tokio::test]
async fn queued_message_survives_main_and_side_rollover_exactly_once() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    // Main rollover: waiting input queued while leased must move to the fresh
    // main, wake it runnable, and drain exactly once.
    let image = swarmy_testkit::image(store).await;
    let agent = store
        .create_agent("rollover", image, "", timestamp(0), None)
        .await
        .unwrap()
        .agent_id;
    let (main, _) = store.open_main_session(agent, timestamp(1)).await.unwrap();
    store.wake_session(main, timestamp(2)).await.unwrap();
    let lease = live_lease(store, main).await;
    let Event::MessageAppended {
        message: waiting, ..
    } = event("waiting during summary")
    else {
        unreachable!()
    };
    // A leased session files input in the queue instead of appending it: the
    // queue call returns the unchanged head, reports a new entry, and reports
    // no idle append.
    assert_eq!(
        store
            .queue_user_message_idempotent(main, &waiting, "rollover-once")
            .await
            .unwrap(),
        swarmy_store::UserMessageAppend {
            sequence: 0,
            fresh: true,
            started: false
        }
    );
    let Event::MessageAppended {
        message: summary, ..
    } = event("main summary")
    else {
        unreachable!()
    };
    let (next_main, _) = store
        .summarize_main_session(main, 0, &lease, &summary, &[])
        .await
        .unwrap();
    assert_eq!(
        store.fetch_session(main).await.unwrap().unwrap().state,
        SessionState::Completed
    );
    // Exactly once means the successor delivers the message and the archived
    // session never can: delivery needs a worker lease, which a Completed
    // session cannot hold.
    assert!(matches!(
        store.deliver_queued(main, 1, &lease, &[]).await,
        Err(StoreError::Fence(swarmy_store::FenceError::LeaseMismatch))
    ));
    drain_transferred_once(store, next_main, &waiting).await;
    // Side rollover: the same transfer runs for side sessions without moving
    // the main pointer, and also drains exactly once.
    let side_id = SessionId::from_ulid(Ulid::generate());
    store
        .create_agent_session(side_id, Some(agent), timestamp(3), None)
        .await
        .unwrap();
    store.wake_session(side_id, timestamp(4)).await.unwrap();
    let side_lease = live_lease(store, side_id).await;
    let Event::MessageAppended {
        message: side_waiting,
        ..
    } = event("side waiting during summary")
    else {
        unreachable!()
    };
    assert_eq!(
        store
            .queue_user_message_idempotent(side_id, &side_waiting, "side-rollover-once")
            .await
            .unwrap(),
        swarmy_store::UserMessageAppend {
            sequence: 0,
            fresh: true,
            started: false
        }
    );
    let Event::MessageAppended {
        message: side_summary,
        ..
    } = event("side summary")
    else {
        unreachable!()
    };
    let (next_side, _) = store
        .summarize_side_session(side_id, 0, &side_lease, &side_summary, &[])
        .await
        .unwrap();
    assert_eq!(
        store.fetch_session(side_id).await.unwrap().unwrap().state,
        SessionState::Completed
    );
    assert!(matches!(
        store.deliver_queued(side_id, 1, &side_lease, &[]).await,
        Err(StoreError::Fence(swarmy_store::FenceError::LeaseMismatch))
    ));
    drain_transferred_once(store, next_side, &side_waiting).await;
}
