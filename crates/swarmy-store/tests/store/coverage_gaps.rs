use super::*;
use swarmy_core::CredentialScope;

fn user_message(text: &str) -> Message {
    Message {
        id: MessageId::from_ulid(Ulid::generate()),
        role: MessageRole::User,
        parts: vec![Part::Text { text: text.into() }],
    }
}

fn opening_summary(text: &str) -> Message {
    user_message(text)
}

async fn named_main(store: &Store) -> (AgentId, SessionId, swarmy_core::Lease) {
    let agent = store
        .create_agent(
            "tommy",
            image_fixture::image(store).await,
            "",
            Timestamp::now(),
            None,
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

async fn drain_queued_once(store: &Store, id: SessionId, head: u64, expected: &Message) {
    // The successor carries waiting input as runnable state; draining it must
    // yield the queued marker plus the user message exactly once, then nothing.
    let session = store.fetch_session(id).await.unwrap().unwrap();
    assert_eq!(session.state, SessionState::Runnable);
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
    let delivered = store.deliver_queued(id, head, &lease, &[]).await.unwrap();
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
    assert!(
        store
            .deliver_queued(id, head + 2, &lease, &[])
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn queued_message_survives_main_and_side_rollover_exactly_once() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    // Main rollover: waiting input queued while leased must move to the fresh
    // main, wake it runnable, and drain exactly once.
    let (_agent, main, lease) = named_main(store).await;
    let waiting = user_message("waiting during summary");
    assert_eq!(
        store
            .queue_user_message_idempotent(main, &waiting, "rollover-once")
            .await
            .unwrap()
            .2,
        false
    );
    let (next_main, _) = store
        .summarize_main_session(main, 0, &lease, &opening_summary("main summary"), &[])
        .await
        .unwrap();
    assert_eq!(
        store.fetch_session(main).await.unwrap().unwrap().state,
        SessionState::Completed
    );
    drain_queued_once(store, next_main, 1, &waiting).await;
    // Side rollover: the same transfer runs for side sessions without moving
    // the main pointer, and also drains exactly once.
    let agent = store
        .fetch_session(next_main)
        .await
        .unwrap()
        .unwrap()
        .agent_id;
    let side_id = SessionId::from_ulid(Ulid::generate());
    store
        .create_agent_session(side_id, Some(agent), Timestamp::now(), None)
        .await
        .unwrap();
    store.wake_session(side_id, Timestamp::now()).await.unwrap();
    let side_lease = store
        .claim_lease(
            side_id,
            owner(),
            Timestamp::now()
                .checked_add(std::time::Duration::from_secs(60))
                .unwrap(),
        )
        .await
        .unwrap();
    let side_waiting = user_message("side waiting during summary");
    store
        .queue_user_message_idempotent(side_id, &side_waiting, "side-rollover-once")
        .await
        .unwrap();
    let (next_side, _) = store
        .summarize_side_session(
            side_id,
            0,
            &side_lease,
            &opening_summary("side summary"),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        store.fetch_session(side_id).await.unwrap().unwrap().state,
        SessionState::Completed
    );
    drain_queued_once(store, next_side, 1, &side_waiting).await;
    test.cleanup().await;
}

async fn inference_claim(store: &Store, id: SessionId) -> swarmy_store::InferenceClaim {
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
    let head = store.fetch_session(id).await.unwrap().unwrap().head_seq;
    let step = head + 1;
    let record = InflightRecord {
        session_id: id,
        seq: step,
        provider: "openai".into(),
        key_id: String::new(),
    };
    store
        .submit_inference::<_, ()>(head, &lease, &record, &"input", None)
        .await
        .unwrap();
    let claim = swarmy_store::InferenceClaim {
        session_id: id,
        request_id: RequestId::for_step(id, step),
        owner: owner(),
        expires_at: Timestamp::now()
            .checked_add(std::time::Duration::from_secs(60))
            .unwrap(),
    };
    assert!(
        store
            .start_inference(&claim, Timestamp::now())
            .await
            .unwrap()
    );
    claim
}

#[tokio::test]
async fn provider_retry_count_bounds_redelivery_and_release_reopens_the_claim() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let id = test.create().await;
    let claim = inference_claim(store, id).await;
    let now = Timestamp::now();
    // The gateway counts provider calls per request: recovery republishes with
    // delivery count one, so the durable counter is the only bound.
    assert_eq!(store.record_inference_retry(&claim, now).await.unwrap(), 1);
    assert_eq!(store.record_inference_retry(&claim, now).await.unwrap(), 2);
    // A fresh delivery of the same request must not bypass the backoff.
    let other = swarmy_store::InferenceClaim {
        owner: owner(),
        ..claim.clone()
    };
    assert!(!store.start_inference(&other, now).await.unwrap());
    // Releasing another owner's claim changes nothing; the holder still owns
    // the retry counter.
    store.release_inference(&other).await.unwrap();
    assert_eq!(store.record_inference_retry(&claim, now).await.unwrap(), 3);
    // Releasing the holder's claim clears the delivery without clearing the
    // backoff, so the next delivery still waits; a retry without a claim is
    // rejected as replaced.
    store.release_inference(&claim).await.unwrap();
    assert!(!store.start_inference(&other, now).await.unwrap());
    assert!(matches!(
        store.record_inference_retry(&claim, now).await,
        Err(StoreError::Fence(swarmy_store::FenceError::LeaseMismatch))
    ));
    test.cleanup().await;
}

#[tokio::test]
async fn leased_park_records_wait_history_once_per_failure_and_wakes() {
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
    let now = Timestamp::now();
    let wake_at = now.checked_add(std::time::Duration::from_secs(60)).unwrap();
    let failure = swarmy_store::InferenceFailureWait {
        seq: 1,
        reason: "openai/primary: quota reached",
        wake_at,
    };
    // Parking releases the worker lease and sleeps until the retry time.
    assert!(
        store
            .park_inference(
                id,
                &lease,
                &failure,
                now,
                std::time::Duration::from_secs(3600)
            )
            .await
            .unwrap()
    );
    assert_eq!(
        store.fetch_session(id).await.unwrap().unwrap().state,
        SessionState::Sleeping
    );
    let wait = store.inference_wait(id).await.unwrap().unwrap();
    assert_eq!(wait.attempts, 1);
    assert_eq!(wait.reasons, ["openai/primary: quota reached"]);
    // A stale lease cannot park; re-parking the same failure sequence records
    // no new attempt, while the next sequence extends the same wait.
    let stale = swarmy_core::Lease {
        owner: owner(),
        ..lease.clone()
    };
    assert!(matches!(
        store
            .park_inference(
                id,
                &stale,
                &failure,
                now,
                std::time::Duration::from_secs(3600)
            )
            .await,
        Err(StoreError::Fence(swarmy_store::FenceError::LeaseMismatch))
    ));
    // The session is already sleeping, so parking through the runnable entry
    // point refuses; the leased path below is what route failover uses.
    assert!(
        !store
            .park_runnable_for_breaker(
                id,
                "openai/primary: quota reached",
                wake_at,
                now,
                std::time::Duration::from_secs(3600)
            )
            .await
            .unwrap()
    );
    assert_eq!(
        store.scan_due_inference_waits(wake_at).await.unwrap(),
        vec![id]
    );
    assert!(
        store
            .scan_due_inference_waits(
                wake_at
                    .checked_sub(std::time::Duration::from_secs(1))
                    .unwrap()
            )
            .await
            .unwrap()
            .is_empty()
    );
    assert!(store.wake_inference_wait(id, wake_at).await.unwrap());
    assert_eq!(
        store.fetch_session(id).await.unwrap().unwrap().state,
        SessionState::Runnable
    );
    assert!(!store.wake_inference_wait(id, wake_at).await.unwrap());
    test.cleanup().await;
}

#[tokio::test]
async fn quota_writes_prefer_configured_limits_and_report_observed_quotas() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    // Without any write the view is empty: no limit, no remaining values.
    let empty = store.entry_quota("openai", "primary").await.unwrap();
    assert!(matches!(empty.source, swarmy_store::QuotaSource::Observed));
    assert!(empty.remaining.is_empty());
    assert_eq!(empty.requests_remaining, None);
    // Empty observations are ignored, so a caller that saw no headers writes
    // nothing durable.
    store
        .observe_entry_quota(
            "openai",
            "primary",
            &std::collections::BTreeMap::new(),
            &std::collections::BTreeMap::new(),
        )
        .await
        .unwrap();
    assert!(
        store
            .entry_quota("openai", "primary")
            .await
            .unwrap()
            .remaining
            .is_empty()
    );
    // Published headers record the latest remaining requests and tokens with
    // the smallest reset window; the gateway pool reads them without extra round trips.
    store
        .observe_entry_quota(
            "openai",
            "primary",
            &std::collections::BTreeMap::from([
                ("x-ratelimit-remaining-requests".into(), 97_u64),
                ("x-ratelimit-remaining-tokens".into(), 11_000_u64),
            ]),
            &std::collections::BTreeMap::from([
                ("x-ratelimit-reset-requests".into(), 60_u64),
                ("x-ratelimit-reset-tokens".into(), 300_u64),
            ]),
        )
        .await
        .unwrap();
    let observed = store.entry_quota("openai", "primary").await.unwrap();
    assert!(matches!(
        observed.source,
        swarmy_store::QuotaSource::Observed
    ));
    assert_eq!(observed.requests_remaining, Some(97));
    assert_eq!(observed.tokens_remaining, Some(11_000));
    assert_eq!(observed.window_seconds, Some(60));
    // An operator-configured limit takes precedence and counts completions
    // from rollups; with no usage the entry is fully free.
    store
        .set_entry_quota_config("openai", "primary", 1_000, 18_000)
        .await
        .unwrap();
    let configured = store.entry_quota("openai", "primary").await.unwrap();
    assert!(matches!(
        configured.source,
        swarmy_store::QuotaSource::Configured
    ));
    assert_eq!(configured.used, 0);
    assert_eq!(configured.free, Some(1_000));
    assert_eq!(configured.limit, Some(1_000));
    test.cleanup().await;
}

fn credential_record(access: &str, expires_at: Timestamp) -> swarmy_core::CredentialRecord {
    swarmy_core::CredentialRecord {
        bookkeeping: swarmy_core::CredentialBookkeeping::default(),
        kind: swarmy_core::CredentialKind::OAuth {
            access: access.into(),
            refresh: "refresh".into(),
            expires_at,
            extra: std::collections::BTreeMap::new(),
        },
        updated_at: Timestamp::now(),
    }
}

#[tokio::test]
async fn touch_records_last_use_and_ready_entries_skip_login_or_expiry() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let keyring = swarmy_config::Keyring::from_bytes([7; 32]);
    let credentials = store.credentials(keyring);
    let now = Timestamp::now();
    let ready_at = now
        .checked_add(std::time::Duration::from_secs(3600))
        .unwrap();
    let expired_at = now.checked_sub(std::time::Duration::from_secs(1)).unwrap();
    credentials
        .put_entry(
            CredentialScope::Cluster,
            "openai",
            "ready",
            &credential_record("ready-access", ready_at),
        )
        .await
        .unwrap();
    credentials
        .put_entry(
            CredentialScope::Cluster,
            "openai",
            "expired",
            &credential_record("old-access", expired_at),
        )
        .await
        .unwrap();
    // Touching records last use without changing the encrypted credential: a
    // subsequent read returns the same secret, while listings show the use.
    let before = credentials
        .get_entry(CredentialScope::Cluster, "openai", "ready")
        .await
        .unwrap()
        .unwrap();
    assert!(
        credentials
            .list_entries(CredentialScope::Cluster)
            .await
            .unwrap()
            .iter()
            .find(|entry| entry.label == "ready")
            .unwrap()
            .last_used_at
            .is_none()
    );
    credentials
        .touch_entry(CredentialScope::Cluster, "openai", "ready")
        .await
        .unwrap();
    let after = credentials
        .get_entry(CredentialScope::Cluster, "openai", "ready")
        .await
        .unwrap()
        .unwrap();
    assert!(before == after);
    assert!(
        credentials
            .list_entries(CredentialScope::Cluster)
            .await
            .unwrap()
            .iter()
            .find(|entry| entry.label == "ready")
            .unwrap()
            .last_used_at
            .is_some()
    );
    assert!(matches!(
        credentials
            .touch_entry(CredentialScope::Cluster, "openai", "missing")
            .await,
        Err(StoreError::Domain(
            swarmy_store::DomainError::CredentialMissing
        ))
    ));
    // The scheduler snapshot behind the gateway pool skips entries needing
    // login or past expiry without decrypting: with one ready entry present,
    // only it is offered.
    let snapshot = store
        .breaker_snapshot(CredentialScope::Cluster, "openai", now)
        .await
        .unwrap();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(
        snapshot[0].key,
        swarmy_store::CredentialKey::entry("openai", "ready")
    );
    test.cleanup().await;
}
