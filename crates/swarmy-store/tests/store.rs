#[path = "support/mod.rs"]
mod image_fixture;

use std::sync::{Arc, OnceLock};

use foundationdb::{Database, tuple::Subspace};
use jiff::Timestamp;
use swarmy_core::{
    AgentId, CHUNK_SIZE, ContentHash, Event, IdempotencyRecord, IdempotencyState, ImageTag,
    InflightRecord, LeaseOwnerId, ManifestHeader, ManifestId, Message, MessageId, MessageRole,
    Part, RequestId, RunnableEntry, SessionId, SessionRecord, SessionState, SnapshotRef, VolumeId,
    VolumeRecord, encode,
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
        kind: swarmy_core::SessionKind::Ephemeral,
        computer_deleted: false,
        plan: Vec::new(),
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
            .create_session(
                &record,
                timestamp(0),
                image_fixture::image(&self.store).await,
            )
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
        .create_session(
            &record,
            timestamp(0),
            image_fixture::image(&test.store).await,
        )
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
        .create_session(
            &record,
            timestamp(0),
            image_fixture::image(&test.store).await,
        )
        .await
        .unwrap();
    assert_eq!(
        test.store.fetch_session(id).await.unwrap(),
        Some(record.clone())
    );
    assert!(matches!(
        test.store
            .create_session(
                &record,
                timestamp(0),
                image_fixture::image(&test.store).await
            )
            .await,
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
    store
        .create_session(&record, timestamp(0), image_fixture::image(&store).await)
        .await
        .unwrap();
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
    assert_completion_published_once(&test.store, &mut completion, &response).await;
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

async fn assert_completion_published_once(
    store: &Store,
    completion: &mut swarmy_store::InferenceCompletion,
    response: &str,
) {
    assert!(
        store
            .complete_inference(completion, &response)
            .await
            .unwrap()
    );
    completion.expected_head = 123;
    completion.event = Event::InferenceFailed {
        seq: 0,
        request_id: completion.claim.request_id,
        error: "must not be published".into(),
    };
    assert!(
        !store
            .complete_inference(completion, &response)
            .await
            .unwrap()
    );
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
}

#[tokio::test]
async fn session_listing_pages_by_id_and_hydrates_snapshots() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    assert!(test.store.list_sessions(None, 2).await.unwrap().is_empty());
    for limit in [0, swarmy_store::MAX_SCAN_LIMIT + 1] {
        assert!(matches!(
            test.store.list_sessions(None, limit).await,
            Err(StoreError::InvalidLimit)
        ));
    }
    let mut expected = Vec::new();
    for _ in 0..5 {
        let id = test.create().await;
        test.store
            .append_events(id, 0, &[event("hello")])
            .await
            .unwrap();
        test.store
            .write_snapshot(
                id,
                &SnapshotRef {
                    object_key: "snapshots/".repeat(10_000),
                    seq: 1,
                },
            )
            .await
            .unwrap();
        expected.push(test.store.fetch_session(id).await.unwrap().unwrap());
    }
    expected.sort_by_key(|session| session.session_id);
    let mut actual = Vec::new();
    let mut after = None;
    loop {
        let page = test.store.list_sessions(after, 2).await.unwrap();
        assert!(page.len() <= 2);
        let Some(last) = page.last() else { break };
        after = Some(last.session_id);
        actual.extend(page);
    }
    assert_eq!(actual, expected);
    // A cursor need not refer to an existing session.
    let beyond = SessionId::from_ulid(Ulid::from(u128::MAX));
    assert!(
        test.store
            .list_sessions(Some(beyond), 2)
            .await
            .unwrap()
            .is_empty()
    );
    test.cleanup().await;
}

fn manifest_id() -> ManifestId {
    ManifestId::from_ulid(Ulid::generate())
}

fn volume_id() -> VolumeId {
    VolumeId::from_ulid(Ulid::generate())
}

fn manifest_header(size: u64) -> ManifestHeader {
    ManifestHeader {
        size,
        chunk_size: CHUNK_SIZE,
        root_hash: ContentHash([7; 32]),
    }
}

#[tokio::test]
async fn volume_records_images_and_immutable_headers_round_trip() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let manifest = manifest_id();
    let volume = volume_id();
    let header = manifest_header(32 * 1024 * 1024 * 1024);
    let tag = ImageTag("stable".into());
    assert_eq!(test.store.get_manifest(manifest).await.unwrap(), None);
    assert_eq!(test.store.get_volume(volume).await.unwrap(), None);
    assert_eq!(test.store.get_image("base", &tag).await.unwrap(), None);
    assert!(matches!(
        test.store.create_volume(volume, manifest).await,
        Err(StoreError::ManifestMissing)
    ));
    assert!(matches!(
        test.store.put_image("base", &tag, manifest).await,
        Err(StoreError::ManifestMissing)
    ));
    test.store.put_manifest(manifest, &header).await.unwrap();
    test.store.put_manifest(manifest, &header).await.unwrap();
    assert_eq!(
        test.store.get_manifest(manifest).await.unwrap(),
        Some(header.clone())
    );
    let conflicting = ManifestHeader {
        root_hash: ContentHash([8; 32]),
        ..header
    };
    assert!(matches!(
        test.store.put_manifest(manifest, &conflicting).await,
        Err(StoreError::ManifestExists)
    ));
    assert!(matches!(
        test.store
            .put_manifest(manifest_id(), &manifest_header(1))
            .await,
        Err(StoreError::InvalidManifest)
    ));
    test.store.put_image("base", &tag, manifest).await.unwrap();
    assert_eq!(
        test.store.get_image("base", &tag).await.unwrap(),
        Some(manifest)
    );
    assert_eq!(
        test.store
            .get_image("base", &ImageTag("other".into()))
            .await
            .unwrap(),
        None
    );
    assert_eq!(test.store.get_image("other", &tag).await.unwrap(), None);
    test.store.create_volume(volume, manifest).await.unwrap();
    assert_eq!(
        test.store.get_volume(volume).await.unwrap(),
        Some(VolumeRecord {
            head_manifest: manifest,
            writer_lease: None,
            parent: None
        })
    );
    assert!(matches!(
        test.store.create_volume(volume, manifest).await,
        Err(StoreError::VolumeExists)
    ));
    assert!(matches!(
        test.store.clone_volume(volume_id(), volume_id()).await,
        Err(StoreError::VolumeMissing)
    ));
    test.cleanup().await;
}

#[tokio::test]
async fn cloning_has_constant_metadata_cost_for_small_and_large_manifests() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    // Disk sizes differ by over 100 million blocks; the records have equal size.
    // Object storage is held separately and is never passed to the metadata store.
    let mut record_sizes = Vec::new();
    for size in [u64::from(CHUNK_SIZE), 32 * 1024 * 1024 * 1024 * 1024] {
        let manifest = manifest_id();
        let objects = Arc::new(object_store::memory::InMemory::new());
        let mut builder = swarmy_volume::ManifestBuilder::new(
            objects.clone(),
            swarmy_volume::Manifest::empty(size).unwrap(),
        );
        builder
            .set_chunk(size / u64::from(CHUNK_SIZE) - 1, ContentHash([1; 32]))
            .unwrap();
        let built = builder.build().await.unwrap();
        assert_eq!(
            built.leaf_hashes().len() as u64,
            (size / u64::from(CHUNK_SIZE)).div_ceil(4096)
        );
        test.store
            .put_manifest(manifest, built.header())
            .await
            .unwrap();
        // Dropping the only object store makes accidental object access impossible.
        drop(objects);
        let source = volume_id();
        test.store.create_volume(source, manifest).await.unwrap();
        let lease = test
            .store
            .acquire_writer_lease(source, owner(), timestamp(1), timestamp(10))
            .await
            .unwrap();
        let destination = volume_id();
        let started = std::time::Instant::now();
        test.store.clone_volume(source, destination).await.unwrap();
        eprintln!("clone of {size}-byte disk: {:?}", started.elapsed());
        let cloned = test.store.get_volume(destination).await.unwrap().unwrap();
        assert_eq!(
            cloned,
            VolumeRecord {
                head_manifest: manifest,
                writer_lease: None,
                parent: Some(source)
            }
        );
        record_sizes.push(encode(&cloned).unwrap().len());
        assert_eq!(
            test.store
                .get_volume(source)
                .await
                .unwrap()
                .unwrap()
                .writer_lease,
            Some(lease)
        );
        assert!(matches!(
            test.store.clone_volume(source, destination).await,
            Err(StoreError::VolumeExists)
        ));
        assert!(matches!(
            test.store.clone_volume(source, source).await,
            Err(StoreError::VolumeExists)
        ));
    }
    assert_eq!(record_sizes[0], record_sizes[1]);
    test.cleanup().await;
}

#[tokio::test]
async fn concurrent_volume_writers_have_one_winner_and_release_allows_reacquisition() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let manifest = manifest_id();
    test.store
        .put_manifest(manifest, &manifest_header(u64::from(CHUNK_SIZE)))
        .await
        .unwrap();
    let volume = volume_id();
    test.store.create_volume(volume, manifest).await.unwrap();
    let (a, b) = tokio::join!(
        test.store
            .acquire_writer_lease(volume, owner(), timestamp(1), timestamp(10)),
        test.store
            .acquire_writer_lease(volume, owner(), timestamp(1), timestamp(10)),
    );
    let (winner, loser) = match (a, b) {
        (Ok(lease), Err(error)) | (Err(error), Ok(lease)) => (lease, error),
        other => panic!("expected one winner, got {other:?}"),
    };
    assert!(matches!(loser, StoreError::LeaseMismatch));
    let mut wrong = winner.clone();
    wrong.owner = owner();
    assert!(matches!(
        test.store
            .release_writer_lease(volume, &wrong, timestamp(2))
            .await,
        Err(StoreError::LeaseMismatch)
    ));
    test.store
        .release_writer_lease(volume, &winner, timestamp(2))
        .await
        .unwrap();
    assert_eq!(
        test.store
            .get_volume(volume)
            .await
            .unwrap()
            .unwrap()
            .writer_lease,
        None
    );
    // Reusing owner and expiry still produces a distinct fencing token.
    let next = test
        .store
        .acquire_writer_lease(volume, winner.owner, timestamp(2), winner.expires_at)
        .await
        .unwrap();
    assert_eq!(next.seq, winner.seq + 1);
    assert!(matches!(
        test.store
            .release_writer_lease(volume, &winner, timestamp(3))
            .await,
        Err(StoreError::LeaseMismatch)
    ));
    assert!(matches!(
        test.store
            .release_writer_lease(volume, &next, timestamp(10))
            .await,
        Err(StoreError::LeaseMismatch)
    ));
    let replacement = test
        .store
        .acquire_writer_lease(volume, owner(), timestamp(10), timestamp(20))
        .await
        .unwrap();
    assert_eq!(replacement.seq, next.seq + 1);
    assert!(matches!(
        test.store
            .release_writer_lease(volume, &next, timestamp(11))
            .await,
        Err(StoreError::LeaseMismatch)
    ));
    test.store
        .release_writer_lease(volume, &replacement, timestamp(11))
        .await
        .unwrap();
    assert!(matches!(
        test.store
            .acquire_writer_lease(volume, owner(), timestamp(12), timestamp(12))
            .await,
        Err(StoreError::LeaseMismatch)
    ));
    assert!(matches!(
        test.store
            .acquire_writer_lease(volume_id(), owner(), timestamp(12), timestamp(20))
            .await,
        Err(StoreError::VolumeMissing)
    ));
    test.cleanup().await;
}

#[tokio::test]
async fn image_listing_pages_by_name_and_tag() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let manifest = ManifestId::from_ulid(Ulid::generate());
    test.store
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
    for (name, tag) in [("ubuntu", "v2"), ("base", "v1"), ("ubuntu", "v1")] {
        test.store
            .put_image(name, &ImageTag(tag.into()), manifest)
            .await
            .unwrap();
    }
    let mut after = None;
    let mut found = Vec::new();
    loop {
        let page = test
            .store
            .list_images(
                after
                    .as_ref()
                    .map(|(name, tag): &(String, ImageTag)| (name.as_str(), tag)),
                1,
            )
            .await
            .unwrap();
        assert!(page.len() <= 1);
        let Some(image) = page.into_iter().next() else {
            break;
        };
        assert_eq!(image.manifest_id, manifest);
        found.push((image.name.clone(), image.tag.0.clone()));
        after = Some((image.name, image.tag));
    }
    assert_eq!(
        found,
        [
            ("base".into(), "v1".into()),
            ("ubuntu".into(), "v1".into()),
            ("ubuntu".into(), "v2".into())
        ]
    );
    assert!(matches!(
        test.store.list_images(None, 0).await,
        Err(StoreError::InvalidLimit)
    ));
}

#[tokio::test]
async fn volume_publication_fences_writers_and_preserves_history() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let base = ManifestId::from_ulid(Ulid::generate());
    let next = ManifestId::from_ulid(Ulid::generate());
    let id = VolumeId::from_ulid(Ulid::generate());
    let header = ManifestHeader {
        size: u64::from(CHUNK_SIZE),
        chunk_size: CHUNK_SIZE,
        root_hash: ContentHash::ZERO,
    };
    store.put_manifest(base, &header).await.unwrap();
    store.create_volume(id, base).await.unwrap();
    let now = Timestamp::now();
    let lease = store
        .acquire_writer_lease(
            id,
            owner(),
            now,
            now.checked_add(std::time::Duration::from_secs(60)).unwrap(),
        )
        .await
        .unwrap();
    let before = store.get_volume(id).await.unwrap();
    let wrong = swarmy_core::Lease {
        owner: owner(),
        ..lease.clone()
    };
    assert!(matches!(
        store.advance_volume(id, &wrong, base, next, &header).await,
        Err(StoreError::LeaseMismatch)
    ));
    assert_eq!(store.get_volume(id).await.unwrap(), before);
    assert_eq!(store.get_manifest(next).await.unwrap(), None);
    assert_eq!(store.manifest_parent(next).await.unwrap(), None);
    let renewed = store
        .renew_writer_lease(
            id,
            &lease,
            lease
                .expires_at
                .checked_add(std::time::Duration::from_secs(30))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        store.advance_volume(id, &lease, base, next, &header).await,
        Err(StoreError::LeaseMismatch)
    ));
    store
        .advance_volume(id, &renewed, base, next, &header)
        .await
        .unwrap();
    store
        .advance_volume(id, &renewed, base, next, &header)
        .await
        .unwrap();
    assert_eq!(store.manifest_parent(next).await.unwrap(), Some(base));
    assert_eq!(store.manifest_parent(base).await.unwrap(), None);
    let stale = ManifestId::from_ulid(Ulid::generate());
    assert!(matches!(
        store
            .advance_volume(id, &renewed, base, stale, &header)
            .await,
        Err(StoreError::VolumeHeadMismatch)
    ));
    assert_eq!(store.get_manifest(stale).await.unwrap(), None);
    let clone = VolumeId::from_ulid(Ulid::generate());
    store.clone_volume(id, clone).await.unwrap();
    assert_volume_listing(store, next).await;
    store
        .release_writer_lease(id, &renewed, Timestamp::now())
        .await
        .unwrap();
    let replacement = store
        .acquire_writer_lease(id, owner(), now, renewed.expires_at)
        .await
        .unwrap();
    assert!(replacement.seq > renewed.seq);
    assert!(matches!(
        store
            .advance_volume(id, &renewed, next, stale, &header)
            .await,
        Err(StoreError::LeaseMismatch)
    ));
    test.cleanup().await;
}

async fn assert_volume_listing(store: &Store, head: ManifestId) {
    let mut listed = Vec::new();
    let mut after = None;
    loop {
        let page = store.list_volumes(after, 1).await.unwrap();
        if page.is_empty() {
            break;
        }
        after = page.last().map(|(id, _)| *id);
        listed.extend(page);
    }
    assert_eq!(listed.len(), 2);
    assert!(
        listed
            .iter()
            .all(|(_, record)| record.head_manifest == head)
    );
}

#[path = "store/tools.rs"]
mod tools;
#[path = "store/turns.rs"]
mod turns;

#[path = "store/placements.rs"]
mod placements;

#[tokio::test]
async fn turn_identity_survives_later_messages_and_failed_appends() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let id = test.create().await;
    let first = event("first turn");
    let Event::MessageAppended { message, .. } = &first else {
        unreachable!()
    };
    let turn = message.id;
    let request = RequestId::for_step(id, 2);
    test.store
        .append_events(
            id,
            0,
            &[
                first,
                Event::InferenceRequested {
                    seq: 0,
                    request_id: request,
                    step: 2,
                },
            ],
        )
        .await
        .unwrap();
    assert_eq!(test.store.turn_id(id).await.unwrap(), Some(turn));
    assert_eq!(
        test.store.request_turn_id(request).await.unwrap(),
        Some(turn)
    );
    assert!(
        test.store
            .append_events(id, 0, &[event("stale")])
            .await
            .is_err()
    );
    assert_eq!(test.store.turn_id(id).await.unwrap(), Some(turn));
    let next = event("next turn");
    let Event::MessageAppended { message, .. } = &next else {
        unreachable!()
    };
    let next_turn = message.id;
    test.store.append_events(id, 2, &[next]).await.unwrap();
    assert_eq!(test.store.turn_id(id).await.unwrap(), Some(next_turn));
    assert_eq!(
        test.store.request_turn_id(request).await.unwrap(),
        Some(turn)
    );
    test.cleanup().await;
}

#[tokio::test]
async fn creation_requires_a_registered_image_and_pins_it_atomically() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let record = session();
    let error = test
        .store
        .create_session(&record, timestamp(0), "missing:tag")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("registered images: (none)"));
    let image = image_fixture::image(&test.store).await;
    let manifest = test
        .store
        .get_image("fixture", &ImageTag("test".into()))
        .await
        .unwrap();
    // Cross a page boundary to verify the error lists every registration.
    for index in 0..65 {
        test.store
            .put_image("other", &ImageTag(format!("{index:02}")), manifest.unwrap())
            .await
            .unwrap();
    }
    for invalid in ["missing:tag", "fixture:unknown"] {
        let error = test
            .store
            .create_session(&record, timestamp(0), invalid)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(invalid) && error.contains("fixture:test") && error.contains("other:64"),
            "{error}"
        );
        assert!(
            test.store
                .fetch_session(record.session_id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            test.store
                .session_image(record.session_id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            test.store
                .scan_runnable(runnable_partition(record.session_id), None, 64)
                .await
                .unwrap()
                .is_empty()
        );
    }
    test.store
        .create_session(&record, timestamp(0), image)
        .await
        .unwrap();
    assert_eq!(
        test.store.session_image(record.session_id).await.unwrap(),
        manifest
    );
    let replacement = ManifestId::from_ulid(Ulid::generate());
    test.store
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
    test.store
        .put_image("fixture", &ImageTag("test".into()), replacement)
        .await
        .unwrap();
    assert_eq!(
        test.store.session_image(record.session_id).await.unwrap(),
        manifest
    );
    test.cleanup().await;
}

#[tokio::test]
async fn legacy_sessions_without_images_remain_readable() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let record = session();
    let key = test
        .root
        .pack(&("session", record.session_id.as_ulid().to_bytes().as_slice()));
    let bytes = encode(&(
        record.session_id,
        record.agent_id,
        record.state,
        record.head_seq,
        None::<u64>,
    ))
    .unwrap();
    test.db
        .run(|trx, _| {
            let key = &key;
            let bytes = &bytes;
            async move {
                trx.set(key, bytes);
                Ok(())
            }
        })
        .await
        .unwrap();
    assert_eq!(
        test.store.fetch_session(record.session_id).await.unwrap(),
        Some(record.clone())
    );
    assert_eq!(
        test.store.list_sessions(None, 64).await.unwrap(),
        vec![record.clone()]
    );
    assert!(
        test.store
            .session_image(record.session_id)
            .await
            .unwrap()
            .is_none()
    );
    test.cleanup().await;
}

#[path = "store/agents.rs"]
mod agents;

#[tokio::test]
async fn session_plan_replacement_is_atomic_fenced_and_validated() {
    use serde_json::json;
    use swarmy_core::{ToolCallId, ToolCallRecord, ToolResult};
    let Some(test) = TestStore::memory() else {
        return;
    };
    let id = test.create().await;
    let now = Timestamp::now();
    let lease = test
        .store
        .claim_lease(
            id,
            owner(),
            now.checked_add(std::time::Duration::from_secs(60)).unwrap(),
        )
        .await
        .unwrap();
    let call = |arguments| ToolCallRecord {
        call_id: ToolCallId("plan".into()),
        tool: "update_plan".into(),
        arguments,
        result: None,
    };
    let first = call(json!({"plan":[{"step":"First", "status":"in_progress"}]}));
    let result = test
        .store
        .complete_plan_tool(id, 0, &lease, RequestId::for_step(id, 1), &first)
        .await
        .unwrap();
    assert!(matches!(
        result,
        Event::ToolCallCompleted {
            result: ToolResult::Completed { .. },
            ..
        }
    ));
    let saved = test.store.fetch_session(id).await.unwrap().unwrap();
    assert_eq!(saved.plan[0].step, "First");
    assert_eq!(saved.head_seq, 1);
    let invalid = call(
        json!({"plan":[{"step":"One", "status":"in_progress"},{"step":"Two", "status":"in_progress"}]}),
    );
    let result = test
        .store
        .complete_plan_tool(id, 1, &lease, RequestId::for_step(id, 2), &invalid)
        .await
        .unwrap();
    assert!(matches!(
        result,
        Event::ToolCallCompleted {
            result: ToolResult::Error { .. },
            ..
        }
    ));
    assert_eq!(
        test.store.fetch_session(id).await.unwrap().unwrap().plan,
        saved.plan
    );
    let second = call(json!({"plan":[{"step":"Second", "status":"pending"}]}));
    test.store
        .complete_plan_tool(id, 2, &lease, RequestId::for_step(id, 3), &second)
        .await
        .unwrap();
    let saved = test.store.fetch_session(id).await.unwrap().unwrap();
    assert_eq!(saved.plan.len(), 1);
    assert_eq!(saved.plan[0].step, "Second");
    assert!(matches!(
        test.store
            .complete_plan_tool(id, 2, &lease, RequestId::for_step(id, 3), &first)
            .await,
        Err(StoreError::StaleSequence { .. })
    ));
    let mut stale = lease.clone();
    stale.owner = owner();
    assert!(matches!(
        test.store
            .complete_plan_tool(id, 3, &stale, RequestId::for_step(id, 4), &first)
            .await,
        Err(StoreError::LeaseMismatch)
    ));
    let empty = call(json!({"plan":[]}));
    let result = test
        .store
        .complete_plan_tool(id, 3, &lease, RequestId::for_step(id, 4), &empty)
        .await
        .unwrap();
    assert!(
        matches!(result, Event::ToolCallCompleted { result: ToolResult::Completed { output, .. }, .. } if output == "[]")
    );
    assert!(
        test.store
            .fetch_session(id)
            .await
            .unwrap()
            .unwrap()
            .plan
            .is_empty()
    );
    let events = test.store.read_events(id, 0, 10).await.unwrap();
    assert_eq!(events.len(), 4);
    test.cleanup().await;
}

#[path = "store/timers.rs"]
mod timers;
