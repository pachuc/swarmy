use std::sync::{Arc, OnceLock};

use foundationdb::{Database, tuple::Subspace};
use jiff::Timestamp;
use swarmy_core::{
    AgentId, Event, IdempotencyRecord, IdempotencyState, InflightRecord, LeaseOwnerId, Message,
    MessageId, MessageRole, Part, RequestId, RunnableEntry, SessionId, SessionRecord, SessionState,
    SnapshotRef, encode,
};
use swarmy_store::{
    Store, StoreError,
    blob::{BlobStore, MemoryBlobStore, ObjectBlobStore},
    runnable_partition,
};
use ulid::Ulid;

fn timestamp(second: i64) -> Timestamp {
    Timestamp::new(second, 0).unwrap()
}
fn owner() -> LeaseOwnerId {
    LeaseOwnerId::from_ulid(Ulid::generate())
}
fn session() -> SessionRecord {
    SessionRecord {
        session_id: SessionId::from_ulid(Ulid::generate()),
        agent_id: AgentId::from_ulid(Ulid::generate()),
        state: SessionState::Runnable,
        head_seq: 0,
        snapshot_ref: None,
    }
}
fn event(text: &str) -> Event {
    Event::MessageAppended {
        seq: 999,
        message: Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role: MessageRole::User,
            parts: vec![Part::Text { text: text.into() }],
        },
    }
}
fn skip(variable: &str) {
    eprintln!("skipping integration test: {variable} is unset");
}

struct TestStore {
    store: Store,
    db: Arc<Database>,
    root: Subspace,
}
impl TestStore {
    fn new(blobs: Arc<dyn BlobStore>) -> Option<Self> {
        static NETWORK: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();
        let Ok(cluster) = std::env::var("SWARMY_FDB_CLUSTER_FILE") else {
            skip("SWARMY_FDB_CLUSTER_FILE");
            return None;
        };
        NETWORK.get_or_init(swarmy_store::boot);
        let db = Arc::new(Database::new(Some(&cluster)).unwrap());
        let root = Subspace::all().subspace(&("swarmy-store-tests", Ulid::generate().to_string()));
        let store = Store::with_subspace(db.clone(), root.clone(), blobs);
        Some(Self { store, db, root })
    }
    fn memory() -> Option<Self> {
        Self::new(Arc::new(MemoryBlobStore::default()))
    }
    async fn create(&self) -> SessionId {
        let record = session();
        self.store
            .create_session(&record, timestamp(0))
            .await
            .unwrap();
        record.session_id
    }
    async fn cleanup(self) {
        let root = &self.root;
        self.db
            .run(|trx, _| async move {
                let (begin, end) = root.range();
                trx.clear_range(&begin, &end);
                Ok(())
            })
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn waking_only_changes_idle_sessions_and_preserves_existing_schedules() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let mut record = session();
    record.state = SessionState::Idle;
    let id = record.session_id;
    test.store
        .create_session(&record, timestamp(0))
        .await
        .unwrap();
    let (first, second) = tokio::join!(
        test.store.wake_session(id, timestamp(1)),
        test.store.wake_session(id, timestamp(1)),
    );
    let states = [first.unwrap(), second.unwrap()];
    assert_eq!(
        states.iter().filter(|&&s| s == SessionState::Idle).count(),
        1
    );
    assert_eq!(
        states
            .iter()
            .filter(|&&s| s == SessionState::Runnable)
            .count(),
        1
    );
    let scheduled = RunnableEntry {
        session_id: id,
        priority: 7,
        wake_at: timestamp(100),
    };
    test.store.insert_runnable(&scheduled).await.unwrap();
    test.store.wake_session(id, timestamp(2)).await.unwrap();
    assert_eq!(
        test.store
            .scan_runnable(runnable_partition(id), None, 64)
            .await
            .unwrap(),
        [scheduled]
    );
    let lease = test
        .store
        .claim_lease(id, owner(), timestamp(50))
        .await
        .unwrap();
    assert_eq!(
        test.store.wake_session(id, timestamp(3)).await.unwrap(),
        SessionState::Leased
    );
    test.store
        .set_state(
            id,
            SessionState::WaitingInference,
            Some(&lease),
            timestamp(4),
        )
        .await
        .unwrap();
    assert_eq!(
        test.store.wake_session(id, timestamp(5)).await.unwrap(),
        SessionState::WaitingInference
    );
    assert!(
        test.store
            .scan_runnable(runnable_partition(id), None, 64)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        test.store
            .wake_session(SessionId::from_ulid(Ulid::generate()), timestamp(5))
            .await,
        Err(StoreError::SessionMissing)
    ));
    test.cleanup().await;
}

#[tokio::test]
async fn events_are_contiguous_and_stale_appends_write_nothing() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let id = test.create().await;
    assert_eq!(
        test.store
            .append_events(id, 0, &[event("a"), event("b"), event("c")])
            .await
            .unwrap(),
        3
    );
    assert!(matches!(
        test.store.append_events(id, 1, &[event("stale")]).await,
        Err(StoreError::StaleSequence {
            expected: 1,
            actual: 3
        })
    ));
    let tail = test.store.read_events(id, 0, 64).await.unwrap();
    assert_eq!(tail.iter().map(Event::seq).collect::<Vec<_>>(), [1, 2, 3]);
    assert_eq!(test.store.read_events(id, 1, 1).await.unwrap(), tail[1..2]);
    assert_eq!(test.store.read_events(id, 2, 64).await.unwrap(), tail[2..]);
    assert!(
        test.store
            .read_events(id, u64::MAX, 64)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        test.store
            .fetch_session(id)
            .await
            .unwrap()
            .unwrap()
            .head_seq,
        3
    );
    test.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_claims_have_exactly_one_winner() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let id = test.create().await;
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let mut tasks = Vec::new();
    for _ in 0..2 {
        let store = test.store.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            store.claim_lease(id, owner(), timestamp(100)).await
        }));
    }
    let mut winners = 0;
    for task in tasks {
        match task.await.unwrap() {
            Ok(_) => winners += 1,
            Err(StoreError::InvalidState) => {}
            other => panic!("unexpected claim: {other:?}"),
        }
    }
    assert_eq!(winners, 1);
    assert_eq!(
        test.store.fetch_session(id).await.unwrap().unwrap().state,
        SessionState::Leased
    );
    assert!(
        test.store
            .scan_runnable(runnable_partition(id), None, 64)
            .await
            .unwrap()
            .is_empty()
    );
    test.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_appends_do_not_overwrite_events() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let id = test.create().await;
    let left = [event("left")];
    let right = [event("right")];
    let (a, b) = tokio::join!(
        test.store.append_events(id, 0, &left),
        test.store.append_events(id, 0, &right)
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert!(matches!(
        a.err().or(b.err()),
        Some(StoreError::StaleSequence { .. })
    ));
    assert_eq!(test.store.read_events(id, 0, 64).await.unwrap().len(), 1);
    test.cleanup().await;
}

#[tokio::test]
async fn expiry_scan_reap_and_fresh_claim() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let id = test.create().await;
    let lease = test
        .store
        .claim_lease(id, owner(), timestamp(-1))
        .await
        .unwrap();
    let fresh_id = test.create().await;
    test.store
        .claim_lease(fresh_id, owner(), timestamp(100))
        .await
        .unwrap();
    let expired = test
        .store
        .scan_expired_leases(timestamp(0), None, 64)
        .await
        .unwrap();
    assert_eq!(expired, [(id, lease.clone())]);
    assert!(
        test.store
            .scan_expired_leases(timestamp(0), expired.last(), 64)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        test.store
            .renew_lease(id, &lease, timestamp(0), timestamp(100))
            .await
            .is_err()
    );
    test.store
        .reap_lease(id, &lease, timestamp(0))
        .await
        .unwrap();
    let fresh = test
        .store
        .claim_lease(id, owner(), timestamp(100))
        .await
        .unwrap();
    assert_ne!(fresh.owner, lease.owner);
    assert!(
        test.store
            .reap_lease(id, &lease, timestamp(200))
            .await
            .is_err()
    );
    assert!(
        test.store
            .release_lease(id, &lease, timestamp(0))
            .await
            .is_err()
    );
    assert!(
        test.store
            .scan_expired_leases(timestamp(0), None, 64)
            .await
            .unwrap()
            .is_empty()
    );
    test.cleanup().await;
}

#[tokio::test]
async fn large_event_uses_a_versioned_blob_pointer() {
    let blobs = Arc::new(MemoryBlobStore::default());
    let Some(test) = TestStore::new(blobs.clone()) else {
        return;
    };
    let id = test.create().await;
    let mut large = event(&"x".repeat(200 * 1024));
    test.store
        .append_events(id, 0, &[large.clone()])
        .await
        .unwrap();
    if let Event::MessageAppended { seq, .. } = &mut large {
        *seq = 1;
    }
    let bytes = encode(&large).unwrap();
    let key = format!("blobs/{}", blake3::hash(&bytes).to_hex());
    assert_eq!(blobs.get(&key).await.unwrap().as_ref(), bytes);
    assert_eq!(test.store.read_events(id, 0, 64).await.unwrap(), [large]);
    let key = test
        .root
        .pack(&("event", id.as_ulid().to_bytes().as_slice(), 1_u64));
    let trx = test.db.create_trx().unwrap();
    let value = trx.get(&key, false).await.unwrap().unwrap();
    assert_eq!(value[0], swarmy_core::STORAGE_VERSION);
    assert!(value.len() < 200);
    drop(trx);
    test.cleanup().await;
}

#[tokio::test]
async fn runnable_partitions_are_isolated_and_rescheduling_replaces_entries() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let id = test.create().await;
    let partition = runnable_partition(id);
    let mut expected = vec![id];
    for _ in 0..40 {
        let id = test.create().await;
        if runnable_partition(id) == partition {
            expected.push(id);
        }
    }
    let entry = RunnableEntry {
        session_id: id,
        priority: -10,
        wake_at: timestamp(-1),
    };
    test.store.insert_runnable(&entry).await.unwrap();
    let found = test.store.scan_runnable(partition, None, 64).await.unwrap();
    assert_eq!(found[0], entry);
    assert_eq!(found.len(), expected.len());
    for row in &found {
        assert_eq!(runnable_partition(row.session_id), partition);
        assert!(expected.contains(&row.session_id));
    }
    assert_eq!(
        test.store
            .scan_runnable(partition, Some(&found[0]), 64)
            .await
            .unwrap(),
        found[1..]
    );
    test.cleanup().await;
}

#[tokio::test]
async fn snapshots_requests_and_lease_transitions_round_trip() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let record = session();
    let id = record.session_id;
    test.store
        .create_session(&record, timestamp(0))
        .await
        .unwrap();
    assert_eq!(
        test.store.fetch_session(id).await.unwrap(),
        Some(record.clone())
    );
    assert!(matches!(
        test.store.create_session(&record, timestamp(0)).await,
        Err(StoreError::SessionExists)
    ));
    test.store
        .append_events(id, 0, &[event("one")])
        .await
        .unwrap();
    let snapshot = SnapshotRef {
        seq: 1,
        object_key: "snapshots/one".into(),
    };
    test.store.write_snapshot(id, &snapshot).await.unwrap();
    assert_eq!(
        test.store
            .fetch_session(id)
            .await
            .unwrap()
            .unwrap()
            .snapshot_ref,
        Some(snapshot)
    );
    let request = RequestId::for_step(id, 1);
    assert_eq!(test.store.get_idempotency(request).await.unwrap(), None);
    let idem = IdempotencyRecord {
        state: IdempotencyState::Completed,
        result_ref: Some("r".repeat(200 * 1024)),
    };
    test.store.put_idempotency(request, &idem).await.unwrap();
    assert_eq!(
        test.store.get_idempotency(request).await.unwrap(),
        Some(idem)
    );
    let inflight = InflightRecord {
        session_id: id,
        seq: 1,
        provider: "mock".into(),
        key_id: "test".into(),
    };
    test.store.put_inflight(request, &inflight).await.unwrap();
    assert_eq!(
        test.store.get_inflight(request).await.unwrap(),
        Some(inflight)
    );
    test.store.clear_inflight(request).await.unwrap();
    assert_eq!(test.store.get_inflight(request).await.unwrap(), None);
    test.cleanup().await;
}

#[tokio::test]
async fn lease_renewal_and_state_transitions_update_indexes() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let id = test.create().await;
    test.store
        .append_events(id, 0, &[event("one")])
        .await
        .unwrap();
    let lease = test
        .store
        .claim_lease(id, owner(), timestamp(10))
        .await
        .unwrap();
    assert_eq!(lease.seq, 2);
    let renewed = test
        .store
        .renew_lease(id, &lease, timestamp(0), timestamp(20))
        .await
        .unwrap();
    assert!(
        test.store
            .scan_expired_leases(timestamp(10), None, 64)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        test.store
            .reap_lease(id, &lease, timestamp(10))
            .await
            .is_err()
    );
    assert!(
        test.store
            .set_state(id, SessionState::Idle, None, timestamp(0))
            .await
            .is_err()
    );
    test.store
        .set_state(
            id,
            SessionState::WaitingInference,
            Some(&renewed),
            timestamp(0),
        )
        .await
        .unwrap();
    assert!(
        test.store
            .scan_expired_leases(timestamp(100), None, 64)
            .await
            .unwrap()
            .is_empty()
    );
    test.store
        .set_state(id, SessionState::Runnable, None, timestamp(1))
        .await
        .unwrap();
    let lease = test
        .store
        .claim_lease(id, owner(), timestamp(10))
        .await
        .unwrap();
    test.store
        .release_lease(id, &lease, timestamp(1))
        .await
        .unwrap();
    assert_eq!(
        test.store
            .scan_runnable(runnable_partition(id), None, 64)
            .await
            .unwrap()
            .len(),
        1
    );
    test.cleanup().await;
}

#[tokio::test]
async fn s3_blob_store_and_large_event_round_trip() {
    if std::env::var("SWARMY_S3_ENDPOINT").is_err() {
        skip("SWARMY_S3_ENDPOINT");
        return;
    }
    let blobs = Arc::new(ObjectBlobStore::from_env().unwrap());
    let key = format!("tests/{}/payload", Ulid::generate());
    let payload = bytes::Bytes::from(vec![42; 200 * 1024]);
    blobs.put(&key, payload.clone()).await.unwrap();
    assert_eq!(blobs.get(&key).await.unwrap(), payload);
    blobs.delete(&key).await.unwrap();
    let Some(test) = TestStore::new(blobs.clone()) else {
        return;
    };
    let id = test.create().await;
    let mut large = event(&"s".repeat(200 * 1024));
    test.store
        .append_events(id, 0, &[large.clone()])
        .await
        .unwrap();
    if let Event::MessageAppended { seq, .. } = &mut large {
        *seq = 1;
    }
    assert_eq!(
        test.store.read_events(id, 0, 64).await.unwrap(),
        [large.clone()]
    );
    let key = format!("blobs/{}", blake3::hash(&encode(&large).unwrap()).to_hex());
    test.cleanup().await;
    blobs.delete(&key).await.unwrap();
}

#[tokio::test]
async fn directory_roots_reopen_without_crossing_isolation_boundaries() {
    use foundationdb::directory::{Directory, DirectoryLayer};

    let Some(test) = TestStore::memory() else {
        return;
    };
    let cluster = std::env::var("SWARMY_FDB_CLUSTER_FILE").unwrap();
    let path = vec![format!("swarmy-store-test-{}", Ulid::generate())];
    let blobs = Arc::new(MemoryBlobStore::default());
    let store = Store::open(Some(&cluster), Some(&path), blobs.clone())
        .await
        .unwrap();
    let record = session();
    store.create_session(&record, timestamp(0)).await.unwrap();
    let reopened = Store::open(Some(&cluster), Some(&path), blobs)
        .await
        .unwrap();
    assert_eq!(
        reopened.fetch_session(record.session_id).await.unwrap(),
        Some(record.clone())
    );
    assert_eq!(
        test.store.fetch_session(record.session_id).await.unwrap(),
        None
    );
    test.db
        .run(|trx, _| {
            let path = &path;
            async move {
                DirectoryLayer::default().remove(&trx, path).await?;
                Ok(())
            }
        })
        .await
        .unwrap();
    test.cleanup().await;
}

#[tokio::test]
async fn oversized_batches_and_invalid_snapshots_preserve_the_session() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let id = test.create().await;
    let events = vec![event(&"x".repeat(79 * 1024)); 110];
    assert!(matches!(
        test.store.append_events(id, 0, &events).await,
        Err(StoreError::TooLarge)
    ));
    assert_eq!(
        test.store
            .fetch_session(id)
            .await
            .unwrap()
            .unwrap()
            .head_seq,
        0
    );
    assert!(test.store.read_events(id, 0, 64).await.unwrap().is_empty());
    let future = SnapshotRef {
        object_key: "snapshots/future".into(),
        seq: 1,
    };
    assert!(test.store.write_snapshot(id, &future).await.is_err());
    assert!(
        test.store
            .fetch_session(id)
            .await
            .unwrap()
            .unwrap()
            .snapshot_ref
            .is_none()
    );
    test.cleanup().await;
}

#[tokio::test]
async fn large_snapshot_metadata_survives_head_and_lease_updates() {
    let blobs = Arc::new(MemoryBlobStore::default());
    let Some(test) = TestStore::new(blobs.clone()) else {
        return;
    };
    let id = test.create().await;
    test.store
        .append_events(id, 0, &[event("before")])
        .await
        .unwrap();
    let snapshot = SnapshotRef {
        seq: 1,
        object_key: "s".repeat(200 * 1024),
    };
    test.store.write_snapshot(id, &snapshot).await.unwrap();
    test.store
        .append_events(id, 1, &[event("after")])
        .await
        .unwrap();
    let lease = test
        .store
        .claim_lease(id, owner(), timestamp(10))
        .await
        .unwrap();
    test.store
        .release_lease(id, &lease, timestamp(0))
        .await
        .unwrap();
    let fetched = test.store.fetch_session(id).await.unwrap().unwrap();
    assert_eq!(fetched.head_seq, 2);
    assert_eq!(fetched.snapshot_ref, Some(snapshot.clone()));
    let bytes = encode(&snapshot).unwrap();
    let key = format!("blobs/{}", blake3::hash(&bytes).to_hex());
    assert_eq!(blobs.get(&key).await.unwrap().as_ref(), bytes);
    test.cleanup().await;
}

#[tokio::test]
async fn inference_completion_is_atomic_fenced_and_idempotent() {
    use swarmy_store::{InferenceClaim, InferenceCompletion};

    let Some(test) = TestStore::memory() else {
        return;
    };
    let id = test.create().await;
    let lease = test
        .store
        .claim_lease(id, owner(), timestamp(10))
        .await
        .unwrap();
    test.store
        .set_state(
            id,
            SessionState::WaitingInference,
            Some(&lease),
            timestamp(0),
        )
        .await
        .unwrap();
    let request_id = RequestId::for_step(id, 1);
    let inflight = InflightRecord {
        session_id: id,
        seq: 1,
        provider: "fake".into(),
        key_id: "fake".into(),
    };
    test.store
        .put_inflight(request_id, &inflight)
        .await
        .unwrap();
    let claim = InferenceClaim {
        session_id: id,
        request_id,
        owner: owner(),
        expires_at: timestamp(10),
    };
    assert!(
        test.store
            .start_inference(&claim, timestamp(0))
            .await
            .unwrap()
    );
    let rival = InferenceClaim {
        owner: owner(),
        expires_at: timestamp(20),
        ..claim.clone()
    };
    assert!(
        !test
            .store
            .start_inference(&rival, timestamp(1))
            .await
            .unwrap()
    );
    assert!(
        test.store
            .start_inference(&rival, timestamp(11))
            .await
            .unwrap()
    );
    let mut completion = InferenceCompletion {
        claim,
        expected_head: 0,
        event: Event::InferenceFailed {
            seq: 999,
            request_id,
            error: "exhausted".into(),
        },
        now: timestamp(12),
    };
    let response = "large result".repeat(20_000);
    assert!(matches!(
        test.store.complete_inference(&completion, &response).await,
        Err(StoreError::LeaseMismatch)
    ));
    completion.claim = rival;
    completion.expected_head = 1;
    assert!(matches!(
        test.store.complete_inference(&completion, &response).await,
        Err(StoreError::StaleSequence { .. })
    ));
    assert_inference_pending(&test.store, id, request_id, inflight).await;
    completion.expected_head = 0;
    test.store
        .complete_inference(&completion, &response)
        .await
        .unwrap();
    test.store
        .complete_inference(&completion, &response)
        .await
        .unwrap();
    assert!(
        !test
            .store
            .start_inference(&completion.claim, timestamp(12))
            .await
            .unwrap()
    );
    assert_inference_completed(&test.store, id, request_id, response).await;
    test.cleanup().await;
}

async fn assert_inference_pending(
    store: &Store,
    id: SessionId,
    request_id: RequestId,
    inflight: InflightRecord,
) {
    assert!(store.read_events(id, 0, 64).await.unwrap().is_empty());
    assert_eq!(
        store.get_inflight(request_id).await.unwrap(),
        Some(inflight)
    );
    assert_eq!(
        store
            .get_idempotency(request_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        IdempotencyState::Requested
    );
    assert_eq!(
        store.fetch_session(id).await.unwrap().unwrap().state,
        SessionState::WaitingInference
    );
    assert!(
        store
            .get_inference_result::<String>(request_id)
            .await
            .unwrap()
            .is_none()
    );
}

async fn assert_inference_completed(
    store: &Store,
    id: SessionId,
    request_id: RequestId,
    response: String,
) {
    assert_eq!(store.read_events(id, 0, 64).await.unwrap().len(), 1);
    assert_eq!(store.read_events(id, 0, 64).await.unwrap()[0].seq(), 1);
    assert!(store.get_inflight(request_id).await.unwrap().is_none());
    assert_eq!(
        store
            .get_idempotency(request_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        IdempotencyState::Completed
    );
    assert_eq!(
        store.fetch_session(id).await.unwrap().unwrap().state,
        SessionState::Runnable
    );
    assert_eq!(
        store
            .scan_runnable(runnable_partition(id), None, 64)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .get_inference_result::<String>(request_id)
            .await
            .unwrap(),
        Some(response)
    );
}

#[tokio::test]
async fn inference_claim_rejects_work_without_matching_inflight() {
    use swarmy_store::InferenceClaim;

    let Some(test) = TestStore::memory() else {
        return;
    };
    let id = test.create().await;
    let lease = test
        .store
        .claim_lease(id, owner(), timestamp(10))
        .await
        .unwrap();
    test.store
        .set_state(
            id,
            SessionState::WaitingInference,
            Some(&lease),
            timestamp(0),
        )
        .await
        .unwrap();
    let request_id = RequestId::for_step(id, 1);
    let claim = InferenceClaim {
        session_id: id,
        request_id,
        owner: owner(),
        expires_at: timestamp(10),
    };
    assert!(matches!(
        test.store.start_inference(&claim, timestamp(0)).await,
        Err(StoreError::InvalidState)
    ));
    let mut inflight = InflightRecord {
        session_id: SessionId::from_ulid(Ulid::generate()),
        seq: 1,
        provider: "fake".into(),
        key_id: "fake".into(),
    };
    test.store
        .put_inflight(request_id, &inflight)
        .await
        .unwrap();
    assert!(matches!(
        test.store.start_inference(&claim, timestamp(0)).await,
        Err(StoreError::InvalidState)
    ));
    assert!(
        test.store
            .get_idempotency(request_id)
            .await
            .unwrap()
            .is_none()
    );
    inflight.session_id = id;
    test.store
        .put_inflight(request_id, &inflight)
        .await
        .unwrap();
    assert!(
        test.store
            .start_inference(&claim, timestamp(0))
            .await
            .unwrap()
    );
    test.cleanup().await;
}

#[tokio::test]
async fn worker_writes_are_fenced_after_renewal_expiry_and_replacement() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let id = test.create().await;
    let old = test
        .store
        .claim_lease(id, owner(), timestamp(10))
        .await
        .unwrap();
    let lease = test
        .store
        .renew_lease(id, &old, timestamp(1), timestamp(20))
        .await
        .unwrap();
    let request_id = RequestId::for_step(id, 1);
    let record = InflightRecord {
        session_id: id,
        seq: 1,
        provider: "fake".into(),
        key_id: String::new(),
    };
    for (token, now) in [(&old, timestamp(2)), (&lease, timestamp(20))] {
        assert!(matches!(
            test.store
                .append_events_leased(id, 0, &[event("stale")], token, now)
                .await,
            Err(StoreError::LeaseMismatch)
        ));
        assert!(matches!(
            test.store
                .put_inference_input(id, 0, token, now, &"stale")
                .await,
            Err(StoreError::LeaseMismatch)
        ));
        assert!(matches!(
            test.store
                .put_inflight_leased(request_id, &record, token, now)
                .await,
            Err(StoreError::LeaseMismatch)
        ));
    }
    test.store
        .put_inference_input(id, 0, &lease, timestamp(2), &"original prompt")
        .await
        .unwrap();
    test.store
        .append_events_leased(id, 0, &[event("valid")], &lease, timestamp(2))
        .await
        .unwrap();
    assert!(matches!(
        test.store
            .put_inference_input(id, 0, &lease, timestamp(2), &"changed prompt")
            .await,
        Err(StoreError::StaleSequence { .. })
    ));
    assert_eq!(
        test.store
            .get_inference_input::<String>(request_id)
            .await
            .unwrap()
            .as_deref(),
        Some("original prompt")
    );
    test.store
        .reap_lease(id, &lease, timestamp(21))
        .await
        .unwrap();
    let replacement = test
        .store
        .claim_lease(id, owner(), timestamp(40))
        .await
        .unwrap();
    assert_ne!(replacement.owner, lease.owner);
    assert!(matches!(
        test.store
            .append_events_leased(id, 1, &[event("stale")], &lease, timestamp(22))
            .await,
        Err(StoreError::LeaseMismatch)
    ));
    assert!(matches!(
        test.store
            .put_inflight_leased(request_id, &record, &lease, timestamp(22))
            .await,
        Err(StoreError::LeaseMismatch)
    ));
    test.store
        .put_inflight_leased(request_id, &record, &replacement, timestamp(22))
        .await
        .unwrap();
    assert_eq!(
        test.store.get_inflight(request_id).await.unwrap(),
        Some(record)
    );
    test.cleanup().await;
}

#[tokio::test]
async fn inflight_scan_pages_without_skips_or_duplicates() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let mut expected = Vec::new();
    for seq in 0..67 {
        let id = session().session_id;
        let record = InflightRecord {
            session_id: id,
            seq,
            provider: "fake".into(),
            key_id: String::new(),
        };
        test.store
            .put_inflight(RequestId::for_step(id, seq), &record)
            .await
            .unwrap();
        expected.push(record);
    }
    expected.sort_by_key(|record| *RequestId::for_step(record.session_id, record.seq).as_bytes());
    let first = test.store.scan_inflight(None, 64).await.unwrap();
    assert_eq!(first, expected[..64]);
    let last = first.last().unwrap();
    let cursor = RequestId::for_step(last.session_id, last.seq);
    let rest = test.store.scan_inflight(Some(cursor), 64).await.unwrap();
    assert_eq!(rest, expected[64..]);
    let last = rest.last().unwrap();
    assert!(
        test.store
            .scan_inflight(Some(RequestId::for_step(last.session_id, last.seq)), 64)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        test.store.scan_inflight(None, 0).await,
        Err(StoreError::InvalidLimit)
    ));
    test.cleanup().await;
}
