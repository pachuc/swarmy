use super::*;
use serde_json::{Value, json};
use swarmy_core::{Lease, TimerRecord, TimerStatus, ToolCallId, ToolCallRecord, ToolResult};

async fn setup(store: &Store) -> (AgentId, SessionId, Lease) {
    let agent = store
        .create_agent(
            "tommy",
            image_fixture::image(store).await,
            "",
            Timestamp::now(),
        )
        .await
        .unwrap()
        .agent_id;
    let (id, _) = store
        .open_main_session(agent, Timestamp::now())
        .await
        .unwrap();
    store.wake_session(id, Timestamp::now()).await.unwrap();
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
    (agent, id, lease)
}

async fn invoke(
    store: &Store,
    id: SessionId,
    lease: &Lease,
    tool: &str,
    arguments: Value,
) -> ToolResult {
    let head = store.fetch_session(id).await.unwrap().unwrap().head_seq;
    let event = store
        .complete_timer_tool(
            id,
            head,
            lease,
            RequestId::for_step(id, head),
            &ToolCallRecord {
                call_id: ToolCallId("timer".into()),
                tool: tool.into(),
                arguments,
                result: None,
            },
        )
        .await
        .unwrap();
    let Event::ToolCallCompleted { result, .. } = event else {
        panic!("wrong event")
    };
    result
}

fn output(result: ToolResult) -> Value {
    let ToolResult::Completed { output, .. } = result else {
        panic!("{result:?}")
    };
    serde_json::from_str(&output).unwrap()
}

async fn set(store: &Store, id: SessionId, lease: &Lease) -> TimerRecord {
    serde_json::from_value(output(
        invoke(
            store,
            id,
            lease,
            "set_timer",
            json!({"at":"2026-01-01T00:00:00Z", "note":"remember violet"}),
        )
        .await,
    ))
    .unwrap()
}

#[tokio::test]
async fn timer_tools_set_list_cancel_and_fence_retries() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let (agent, id, lease) = setup(store).await;
    let timer = set(store, id, &lease).await;
    let listed = output(invoke(store, id, &lease, "list_timers", json!({})).await);
    assert_eq!(listed, json!([timer]));
    let args = json!({"timer_id":timer.timer_id});
    let cancelled = output(invoke(store, id, &lease, "cancel_timer", args.clone()).await);
    assert_eq!(cancelled["status"], "Cancelled");
    assert_eq!(
        output(invoke(store, id, &lease, "cancel_timer", args).await),
        cancelled
    );
    assert!(store.list_timers(agent).await.unwrap().is_empty());
    assert!(
        store
            .scan_due_timers(Timestamp::now(), None)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .fire_timer(agent, timer.timer_id, Timestamp::now())
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        store
            .complete_timer_tool(
                id,
                0,
                &lease,
                RequestId::for_step(id, 1),
                &ToolCallRecord {
                    call_id: ToolCallId("retry".into()),
                    tool: "set_timer".into(),
                    arguments: json!({"delay_seconds":1,"note":"duplicate"}),
                    result: None
                }
            )
            .await,
        Err(StoreError::StaleSequence { .. })
    ));
    assert!(matches!(
        invoke(
            store,
            id,
            &lease,
            "cancel_timer",
            json!({"timer_id":swarmy_core::TimerId::from_ulid(Ulid::generate())})
        )
        .await,
        ToolResult::Error { .. }
    ));
    assert!(store.list_timers(agent).await.unwrap().is_empty());
    test.cleanup().await;
}

#[tokio::test]
async fn due_timer_retries_busy_append_and_follows_summary_after_reopening_store() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let (agent, id, lease) = setup(store).await;
    let timer = set(store, id, &lease).await;
    let before = timer
        .due_at
        .checked_sub(std::time::Duration::from_nanos(1))
        .unwrap();
    assert!(
        store
            .fire_timer(agent, timer.timer_id, before)
            .await
            .unwrap()
            .is_none()
    );
    // The worker owns the session: the due note cannot be appended yet.
    assert!(
        store
            .fire_timer(agent, timer.timer_id, timer.due_at)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .get_timer(agent, timer.timer_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        TimerStatus::Pending
    );
    let opening = Message {
        id: MessageId::from_ulid(Ulid::generate()),
        role: MessageRole::System,
        parts: vec![Part::Text {
            text: "summary".into(),
        }],
    };
    let (main, _) = store
        .summarize_main_session(id, 1, &lease, &opening)
        .await
        .unwrap();
    // The fresh main is idle, so only the due-time fence can prevent early delivery.
    assert!(
        store
            .fire_timer(agent, timer.timer_id, before)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .scan_due_timers(before, None)
            .await
            .unwrap()
            .is_empty()
    );
    // A new store handle has no in-memory timer state, as after a scheduler restart.
    let restarted = Store::with_subspace(
        test.db.clone(),
        test.root.clone(),
        Arc::new(MemoryBlobStore::default()),
    );
    assert_eq!(
        restarted.scan_due_timers(timer.due_at, None).await.unwrap(),
        vec![timer.clone()]
    );
    let (first, second) = tokio::join!(
        restarted.fire_timer(agent, timer.timer_id, timer.due_at),
        store.fire_timer(agent, timer.timer_id, timer.due_at)
    );
    assert_eq!(
        usize::from(first.unwrap().is_some()) + usize::from(second.unwrap().is_some()),
        1
    );
    assert_delivery(store, main, &timer).await;
    assert!(
        restarted
            .fire_timer(agent, timer.timer_id, Timestamp::now())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        restarted
            .scan_due_timers(Timestamp::now(), None)
            .await
            .unwrap()
            .is_empty()
    );
    test.cleanup().await;
}

#[tokio::test]
async fn failed_append_leaves_no_receipt_and_next_tick_retries() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let (agent, id, lease) = setup(store).await;
    let timer = set(store, id, &lease).await;
    store
        .set_state(id, SessionState::Idle, Some(&lease), Timestamp::now())
        .await
        .unwrap();
    // Inject a transaction failure at the append boundary, then restore the header.
    let key = test
        .root
        .pack(&("session", id.as_ulid().to_bytes().as_slice()));
    let trx = test.db.create_trx().unwrap();
    let original = trx.get(&key, false).await.unwrap().unwrap();
    let mut header: (SessionId, AgentId, SessionState, u64, Option<u64>) =
        swarmy_core::decode(&original).unwrap();
    header.3 = u64::MAX;
    trx.set(&key, &encode(&header).unwrap());
    trx.commit().await.unwrap();
    assert!(matches!(
        store
            .fire_timer(agent, timer.timer_id, Timestamp::now())
            .await,
        Err(StoreError::SequenceOverflow)
    ));
    assert_eq!(
        store
            .get_timer(agent, timer.timer_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        TimerStatus::Pending
    );
    let trx = test.db.create_trx().unwrap();
    trx.set(&key, &original);
    trx.commit().await.unwrap();
    assert!(
        store
            .fire_timer(agent, timer.timer_id, Timestamp::now())
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(store.read_events(id, 0, 64).await.unwrap().len(), 2);
    test.cleanup().await;
}

#[tokio::test]
async fn timers_are_bounded_agent_scoped_and_retired_on_deletion() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let (agent, id, lease) = setup(store).await;
    for _ in 0..swarmy_core::MAX_AGENT_TIMERS {
        set(store, id, &lease).await;
    }
    assert!(matches!(
        invoke(
            store,
            id,
            &lease,
            "set_timer",
            json!({"delay_seconds":1,"note":"over limit"})
        )
        .await,
        ToolResult::Error { .. }
    ));
    let timers = store.list_timers(agent).await.unwrap();
    assert_eq!(timers.len(), swarmy_core::MAX_AGENT_TIMERS);
    let other = store
        .create_agent("other", "fixture:test", "", Timestamp::now())
        .await
        .unwrap()
        .agent_id;
    let (other_id, _) = store
        .open_main_session(other, Timestamp::now())
        .await
        .unwrap();
    store
        .wake_session(other_id, Timestamp::now())
        .await
        .unwrap();
    let other_lease = store
        .claim_lease(
            other_id,
            owner(),
            Timestamp::now()
                .checked_add(std::time::Duration::from_secs(60))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        invoke(
            store,
            other_id,
            &other_lease,
            "cancel_timer",
            json!({"timer_id":timers[0].timer_id})
        )
        .await,
        ToolResult::Error { .. }
    ));
    assert!(store.list_timers(other).await.unwrap().is_empty());
    store.delete_agent(agent).await.unwrap();
    for timer in timers {
        assert!(
            store
                .fire_timer(agent, timer.timer_id, Timestamp::now())
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .get_timer(agent, timer.timer_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            TimerStatus::Cancelled
        );
    }
    assert!(
        store
            .scan_due_timers(Timestamp::now(), None)
            .await
            .unwrap()
            .is_empty()
    );
    test.cleanup().await;
}

#[tokio::test]
async fn side_conversation_timer_opens_missing_main_conversation() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let agent = store
        .create_agent(
            "side-only",
            image_fixture::image(store).await,
            "",
            Timestamp::now(),
        )
        .await
        .unwrap()
        .agent_id;
    let side = SessionId::from_ulid(Ulid::generate());
    store
        .create_session_for_agent(side, Some(agent), None, Timestamp::now())
        .await
        .unwrap();
    store.wake_session(side, Timestamp::now()).await.unwrap();
    let lease = store
        .claim_lease(
            side,
            owner(),
            Timestamp::now()
                .checked_add(std::time::Duration::from_secs(60))
                .unwrap(),
        )
        .await
        .unwrap();
    let timer = set(store, side, &lease).await;
    assert!(
        store
            .get_agent(agent)
            .await
            .unwrap()
            .unwrap()
            .main_session
            .is_none()
    );
    let (main, event) = store
        .fire_timer(agent, timer.timer_id, timer.due_at)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(main, side);
    assert_eq!(event.seq(), 1);
    assert_eq!(
        store.get_agent(agent).await.unwrap().unwrap().main_session,
        Some(main)
    );
    assert_eq!(
        store.fetch_session(main).await.unwrap().unwrap().state,
        SessionState::Runnable
    );
    assert!(store.get_by_agent(agent).await.unwrap().is_none());
    test.cleanup().await;
}

async fn assert_delivery(store: &Store, main: SessionId, timer: &TimerRecord) {
    assert_eq!(
        store
            .get_timer(timer.agent_id, timer.timer_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        TimerStatus::Fired {
            session_id: main,
            seq: 2
        }
    );
    let events = store.read_events(main, 0, 64).await.unwrap();
    assert!(
        matches!(&events[1], Event::MessageAppended { message, .. } if message.role == MessageRole::System && message.parts == vec![Part::Text { text: timer.note.clone() }])
    );
    // Simulate a lost nudge: readiness is durable without publishing anything.
    assert_eq!(
        store
            .scan_runnable(runnable_partition(main), None, 64)
            .await
            .unwrap()[0]
            .session_id,
        main
    );
}
