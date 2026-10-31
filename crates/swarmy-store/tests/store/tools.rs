use super::*;
use std::time::Duration;
use swarmy_core::{
    BashArguments, BashResult, NodeCapacity, NodeId, NodeRecord, NodeRole, PlacedToolClaim,
    ToolCallId, ToolCallRecord, ToolJob,
};

async fn setup_with(
    store: &Store,
    command: &str,
) -> (
    SessionId,
    ManifestId,
    NodeRecord,
    Vec<ToolJob>,
    swarmy_core::PlacementRecord,
) {
    let mut session = session();
    session.state = SessionState::Idle;
    let id = session.session_id;
    let agent = session.agent_id;
    let image = ManifestId::from_ulid(Ulid::generate());
    store
        .put_manifest(
            image,
            &ManifestHeader {
                size: u64::from(CHUNK_SIZE),
                chunk_size: CHUNK_SIZE,
                root_hash: ContentHash([1; 32]),
            },
        )
        .await
        .unwrap();
    store
        .put_image("base", &ImageTag("test".into()), image, None)
        .await
        .unwrap();
    store
        .create_session(&session, Timestamp::now(), "base:test")
        .await
        .unwrap();
    let node = node();
    store.put_node(&node).await.unwrap();
    let placement = store
        .place(
            agent,
            node.node_id,
            Timestamp::now()
                .checked_add(Duration::from_secs(30))
                .unwrap(),
        )
        .await
        .unwrap();
    store.wake_session(id, Timestamp::now()).await.unwrap();
    let lease = store
        .claim_lease(
            id,
            owner(),
            Timestamp::now()
                .checked_add(Duration::from_secs(30))
                .unwrap(),
        )
        .await
        .unwrap();
    let (jobs, events) = tool_jobs(id, command);
    store
        .append_events_leased(id, 0, &events, &lease, Timestamp::now())
        .await
        .unwrap();
    let mut wrong = jobs.clone();
    wrong[0].arguments =
        swarmy_core::SandboxArguments::parse("bash", serde_json::json!({"command":"different"}))
            .unwrap();
    assert!(
        store
            .dispatch_placed_tool_jobs(id, &lease, &wrong, &placement)
            .await
            .is_err()
    );
    store
        .dispatch_placed_tool_jobs(id, &lease, &jobs, &placement)
        .await
        .unwrap();
    assert_eq!(
        store.fetch_session(id).await.unwrap().unwrap().state,
        SessionState::WaitingTools
    );
    assert_eq!(store.scan_tool_jobs(None, 64).await.unwrap().len(), 2);
    (id, image, node, jobs, placement)
}

async fn setup(
    store: &Store,
) -> (
    SessionId,
    ManifestId,
    NodeRecord,
    Vec<ToolJob>,
    swarmy_core::PlacementRecord,
) {
    setup_with(store, "echo test").await
}

fn tool_jobs(id: SessionId, command: &str) -> (Vec<ToolJob>, Vec<Event>) {
    let jobs: Vec<_> = (1..=2)
        .map(|step| ToolJob {
            session_id: id,
            step,
            request_id: RequestId::for_step(id, step),
            call_id: ToolCallId(format!("bash-{step}")),
            arguments: swarmy_core::SandboxArguments::Bash(BashArguments {
                command: command.into(),
                timeout_ms: 1000,
                yield_seconds: 10,
                output_budget_bytes: 32768,
            }),
        })
        .collect();
    let events: Vec<_> = jobs
        .iter()
        .map(|job| Event::ToolCallRequested {
            seq: 0,
            request_id: job.request_id,
            call: ToolCallRecord {
                call_id: job.call_id.clone(),
                tool: "bash".into(),
                arguments: job.arguments.parameters(),
                result: None,
            },
        })
        .collect();
    (jobs, events)
}

fn node() -> NodeRecord {
    NodeRecord {
        node_id: NodeId::from_ulid(Ulid::generate()),
        roles: vec![NodeRole::Sandbox],
        capacity: NodeCapacity {
            cpu_millis: 4000,
            memory_bytes: 1024 * 1024 * 1024,
            disk_bytes: 1024,
            sandboxes: 1,
        },
        last_heartbeat: Timestamp::now(),
        cached_images: vec![],
        gpu: None,
    }
}

#[tokio::test]
async fn agent_volume_starts_snapshot_history_and_can_publish() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let (session, image, node, _, placement) = setup(store).await;
    let volume = store.agent_volume(session, &placement).await.unwrap();
    assert_eq!(store.volume_snapshots(volume).await.unwrap(), vec![image]);
    assert_eq!(
        store.volume_live_manifests(volume).await.unwrap(),
        vec![image]
    );
    let lease = store
        .acquire_writer_lease(
            volume,
            LeaseOwnerId::from_ulid(node.node_id.as_ulid()),
            Timestamp::now()
                .checked_add(Duration::from_secs(30))
                .unwrap(),
        )
        .await
        .unwrap();
    let next = ManifestId::from_ulid(Ulid::generate());
    let header = store.get_manifest(image).await.unwrap().unwrap();
    store
        .advance_volume_retained(
            volume,
            &lease,
            image,
            next,
            &header,
            std::num::NonZeroUsize::new(10).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        store.volume_snapshots(volume).await.unwrap(),
        vec![next, image]
    );
    assert_eq!(
        store.volume_live_manifests(volume).await.unwrap(),
        vec![next, image]
    );
}

#[tokio::test]
async fn persistent_calls_fence_epochs_without_publishing_or_cloning() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let (session, image, node, jobs, placement) = setup(store).await;
    let agent = placement.agent_id;
    let expiry = || {
        Timestamp::now()
            .checked_add(Duration::from_secs(30))
            .unwrap()
    };
    let volume = store.agent_volume(session, &placement).await.unwrap();
    assert_eq!(volume.as_ulid(), agent.as_ulid());
    let writer = store
        .acquire_writer_lease(
            volume,
            LeaseOwnerId::from_ulid(node.node_id.as_ulid()),
            expiry(),
        )
        .await
        .unwrap();
    let before = store.list_volumes(None, 64).await.unwrap();
    let claim = PlacedToolClaim {
        job: jobs[0].clone(),
        owner: owner(),
        placement: placement.clone(),
        expires_at: expiry(),
    };
    check_tool_admission(store, &claim.job, &placement).await;
    assert!(store.claim_placed_tool(&claim).await.unwrap());
    assert!(store.claim_placed_tool(&claim).await.unwrap());
    assert!(
        !store
            .claim_placed_tool(&PlacedToolClaim {
                owner: owner(),
                ..claim.clone()
            })
            .await
            .unwrap()
    );
    store.renew(&placement, expiry()).await.unwrap();
    store.renew_placed_tool(&claim, expiry()).await.unwrap();
    let result = BashResult {
        stdout: "done".into(),
        stderr: String::new(),
        exit_code: 0,
        timed_out: false,
        manifest_id: image,
    };
    store
        .complete_placed_tool(&claim, 2, &result.tool_result())
        .await
        .unwrap();
    assert_eq!(store.list_volumes(None, 64).await.unwrap(), before);
    assert_eq!(
        store.tool_agent(&claim.job, node.node_id).await.unwrap(),
        None
    );
    let claim = PlacedToolClaim {
        job: jobs[1].clone(),
        owner: owner(),
        placement: placement.clone(),
        expires_at: expiry(),
    };
    assert!(store.claim_placed_tool(&claim).await.unwrap());
    store.release(&placement).await.unwrap();
    let next = store.place(agent, node.node_id, expiry()).await.unwrap();
    assert!(next.epoch > placement.epoch);
    assert_eq!(
        next.last_change_reason,
        swarmy_core::PlacementChangeReason::Eviction
    );
    assert!(matches!(
        store
            .complete_placed_tool(&claim, 3, &result.tool_result())
            .await,
        Err(StoreError::Fence(
            swarmy_store::FenceError::PlacementMismatch
        ))
    ));
    assert!(matches!(
        store.renew_placed_tool(&claim, expiry()).await,
        Err(StoreError::Fence(
            swarmy_store::FenceError::PlacementMismatch
        ))
    ));
    assert!(matches!(
        store.renew_writer_lease(volume, &writer, expiry()).await,
        Err(StoreError::Fence(
            swarmy_store::FenceError::PlacementMismatch
        ))
    ));
    check_stale_publication(store, volume, &writer, image).await;
    assert!(matches!(
        store.agent_volume(session, &next).await,
        Err(StoreError::Fence(
            swarmy_store::FenceError::PlacementMismatch
        ))
    ));
}

async fn check_tool_admission(
    store: &Store,
    job: &ToolJob,
    placement: &swarmy_core::PlacementRecord,
) {
    // Placed jobs already carry their dispatch fence; recovery re-resolves it.
    assert!(store.route_tool_job(job, placement).await.unwrap());
    assert_eq!(
        store.tool_agent(job, placement.node_id).await.unwrap(),
        Some(placement.agent_id)
    );
    assert!(matches!(
        store
            .tool_agent(job, NodeId::from_ulid(Ulid::generate()))
            .await,
        Err(StoreError::Fence(
            swarmy_store::FenceError::PlacementMismatch
        ))
    ));
}

async fn check_stale_publication(
    store: &Store,
    volume: VolumeId,
    writer: &swarmy_core::Lease,
    image: ManifestId,
) {
    let header = store.get_manifest(image).await.unwrap().unwrap();
    assert!(matches!(
        store
            .advance_volume(
                volume,
                writer,
                image,
                ManifestId::from_ulid(Ulid::generate()),
                &header
            )
            .await,
        Err(StoreError::Fence(
            swarmy_store::FenceError::PlacementMismatch
        ))
    ));
}

#[tokio::test]
async fn tool_requests_and_dispatch_commit_together_with_both_fences() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let id = test.create().await;
    let agent = store.fetch_session(id).await.unwrap().unwrap().agent_id;
    let node = node();
    store.put_node(&node).await.unwrap();
    let expiry = Timestamp::now()
        .checked_add(Duration::from_secs(60))
        .unwrap();
    let placement = store.place(agent, node.node_id, expiry).await.unwrap();
    let lease = store.claim_lease(id, owner(), expiry).await.unwrap();
    let calls: Vec<_> = (0..2)
        .map(|index| ToolCallRecord {
            call_id: ToolCallId(format!("call-{index}")),
            tool: "bash".into(),
            arguments: serde_json::json!({"command": "true", "timeout_ms": 1000}),
            result: None,
        })
        .collect();
    let replaced = swarmy_core::Lease {
        owner: owner(),
        ..lease.clone()
    };
    assert!(matches!(
        store
            .dispatch_tool_calls(id, 0, &replaced, &calls, &placement)
            .await,
        Err(StoreError::Fence(swarmy_store::FenceError::LeaseMismatch))
    ));
    let stale = swarmy_core::PlacementRecord {
        epoch: placement.epoch + 1,
        ..placement.clone()
    };
    assert!(matches!(
        store
            .dispatch_tool_calls(id, 0, &lease, &calls, &stale)
            .await,
        Err(StoreError::Fence(
            swarmy_store::FenceError::PlacementMismatch
        ))
    ));
    assert!(matches!(
        store
            .dispatch_tool_calls(id, 1, &lease, &calls, &placement)
            .await,
        Err(StoreError::Fence(
            swarmy_store::FenceError::StaleSequence { .. }
        ))
    ));
    assert!(store.read_events(id, 0, 64).await.unwrap().is_empty());
    assert!(store.scan_tool_jobs(None, 64).await.unwrap().is_empty());
    let (events, jobs) = store
        .dispatch_tool_calls(id, 0, &lease, &calls, &placement)
        .await
        .unwrap();
    assert_eq!(store.read_events(id, 0, 64).await.unwrap(), events);
    assert_eq!(events.len(), 2);
    assert_eq!(jobs.len(), 2);
    let session = store.fetch_session(id).await.unwrap().unwrap();
    assert_eq!(session.head_seq, 2);
    assert_eq!(session.state, SessionState::WaitingTools);
    for job in jobs {
        assert_eq!(
            store.tool_placement(job.request_id).await.unwrap(),
            Some(placement.clone())
        );
        assert_eq!(
            store.tool_agent(&job, node.node_id).await.unwrap(),
            Some(agent)
        );
    }
    assert!(matches!(
        store
            .set_state(id, SessionState::Runnable, Some(&lease), Timestamp::now())
            .await,
        Err(StoreError::Fence(swarmy_store::FenceError::LeaseMismatch))
    ));
}

#[tokio::test]
async fn tool_admission_reads_the_session_record_once() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let (_, _, node, jobs, placement) = setup(store).await;
    let before = store.session_record_read_count();
    assert_eq!(
        store.tool_agent(&jobs[0], node.node_id).await.unwrap(),
        Some(placement.agent_id)
    );
    assert_eq!(store.session_record_read_count() - before, 1);
}

/// Memory blobs that count downloads, so a test can prove a path reads no object storage.
#[derive(Default)]
struct CountingBlobs {
    inner: MemoryBlobStore,
    gets: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl BlobStore for CountingBlobs {
    async fn put(
        &self,
        key: &str,
        value: bytes::Bytes,
    ) -> Result<(), swarmy_store::blob::BlobError> {
        self.inner.put(key, value).await
    }
    async fn get(&self, key: &str) -> Result<bytes::Bytes, swarmy_store::blob::BlobError> {
        self.gets.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.get(key).await
    }
}

impl CountingBlobs {
    fn gets(&self) -> usize {
        self.gets.load(std::sync::atomic::Ordering::Relaxed)
    }
}

fn large_command(fill: char) -> String {
    std::iter::repeat_n(fill, 100 * 1024).collect()
}

#[tokio::test]
async fn large_tool_jobs_are_routed_and_claimed_without_object_storage_reads() {
    let blobs = Arc::new(CountingBlobs::default());
    let Some(test) = TestStore::new(blobs.clone()) else {
        return;
    };
    let store = &test.store;
    let (_, _, _, jobs, placement) = setup_with(store, &large_command('a')).await;
    let mut wrong = jobs[0].clone();
    wrong.arguments = swarmy_core::SandboxArguments::Bash(BashArguments {
        command: large_command('b'),
        timeout_ms: 1000,
        yield_seconds: 10,
        output_budget_bytes: 32768,
    });
    let expires_at = Timestamp::now()
        .checked_add(Duration::from_secs(30))
        .unwrap();
    let claim = |job: &ToolJob| PlacedToolClaim {
        job: job.clone(),
        owner: owner(),
        placement: placement.clone(),
        expires_at,
    };
    let before = blobs.gets();
    assert!(store.route_tool_job(&jobs[0], &placement).await.unwrap());
    assert!(matches!(
        store.route_tool_job(&wrong, &placement).await,
        Err(StoreError::Fence(swarmy_store::FenceError::ToolJobMismatch))
    ));
    assert!(matches!(
        store.claim_placed_tool(&claim(&wrong)).await,
        Err(StoreError::Fence(swarmy_store::FenceError::ToolJobMismatch))
    ));
    assert!(store.claim_placed_tool(&claim(&jobs[0])).await.unwrap());
    assert_eq!(
        blobs.gets(),
        before,
        "tool routing and claims must not read object storage"
    );
}

#[tokio::test]
async fn large_tool_job_fails_after_computer_deletion_without_object_storage_reads() {
    let blobs = Arc::new(CountingBlobs::default());
    let Some(test) = TestStore::new(blobs.clone()) else {
        return;
    };
    let store = &test.store;
    let (session, _, _, jobs, placement) = setup_with(store, &large_command('a')).await;
    store.delete_computer(placement.agent_id).await.unwrap();
    let before = blobs.gets();
    assert!(store.fail_deleted_computer_tool(&jobs[0]).await.unwrap());
    assert_eq!(
        blobs.gets(),
        before,
        "failing a deleted computer's call must not read object storage"
    );
    assert!(store.tool_completed(jobs[0].request_id).await.unwrap());
    assert_eq!(
        store.fetch_session(session).await.unwrap().unwrap().state,
        SessionState::WaitingTools
    );
}

#[tokio::test]
async fn redispatching_a_pending_job_writes_nothing() {
    use foundationdb::options::ConflictRangeType;
    let Some(test) = TestStore::memory() else {
        return;
    };
    let (_, _, _, jobs, placement) = setup(&test.store).await;
    let (begin, end) = test.root.range();
    let probe = test.root.pack(&("probe",));
    let trx = test.db.create_trx().unwrap();
    trx.get_read_version().await.unwrap();
    trx.add_conflict_range(&begin, &end, ConflictRangeType::Read)
        .unwrap();
    for _ in 0..2 {
        assert!(
            test.store
                .route_tool_job(&jobs[0], &placement)
                .await
                .unwrap()
        );
    }
    trx.set(&probe, b"ok");
    trx.commit().await.unwrap();

    let trx = test.db.create_trx().unwrap();
    trx.get_read_version().await.unwrap();
    trx.add_conflict_range(&begin, &end, ConflictRangeType::Read)
        .unwrap();
    let claim = PlacedToolClaim {
        job: jobs[0].clone(),
        owner: owner(),
        placement,
        expires_at: Timestamp::now()
            .checked_add(Duration::from_secs(30))
            .unwrap(),
    };
    assert!(test.store.claim_placed_tool(&claim).await.unwrap());
    trx.set(&probe, b"changed");
    assert_eq!(trx.commit().await.unwrap_err().code(), 1020);
}

#[tokio::test]
async fn claim_commits_on_first_attempt_while_redispatch_runs() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let (_, _, _, jobs, placement) = setup(&test.store).await;
    let expiry = Timestamp::now()
        .checked_add(Duration::from_secs(30))
        .unwrap();
    let claim = |job: &ToolJob| PlacedToolClaim {
        job: job.clone(),
        owner: owner(),
        placement: placement.clone(),
        expires_at: expiry,
    };
    assert_eq!(
        test.store
            .claim_placed_tool_racing(&claim(&jobs[0]), async {
                for _ in 0..2 {
                    assert!(
                        test.store
                            .route_tool_job(&jobs[0], &placement)
                            .await
                            .unwrap()
                    );
                }
            })
            .await
            .unwrap(),
        (true, 1)
    );
    assert_eq!(
        test.store
            .claim_placed_tool_racing(&claim(&jobs[1]), async {
                test.store
                    .renew(
                        &placement,
                        Timestamp::now()
                            .checked_add(Duration::from_secs(60))
                            .unwrap(),
                    )
                    .await
                    .unwrap();
            })
            .await
            .unwrap(),
        (true, 1)
    );
}

#[tokio::test]
async fn status_write_survives_renewal_and_fails_after_release() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let (_, _, node, _, placement) = setup(&test.store).await;
    let store = &test.store;
    let now = Timestamp::now();
    let status = swarmy_core::AgentCallStatus {
        agent_id: placement.agent_id,
        node_id: node.node_id,
        epoch: placement.epoch,
        holder_session_id: None,
        queued_calls: 0,
        observed_at: now,
        expires_at: now.checked_add(Duration::from_secs(30)).unwrap(),
    };
    assert_eq!(
        store
            .put_agent_call_status_racing(&status, async {
                store
                    .renew_hosted(
                        &placement,
                        now.checked_add(Duration::from_secs(60)).unwrap(),
                    )
                    .await
                    .unwrap();
            })
            .await
            .unwrap(),
        1
    );
    assert!(matches!(
        store
            .put_agent_call_status_racing(&status, async {
                store.release(&placement).await.unwrap();
            })
            .await,
        Err(StoreError::Fence(
            swarmy_store::FenceError::PlacementMismatch
        ))
    ));
}

#[tokio::test]
async fn deletion_racing_status_write_fences_on_computer_tombstone() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let (_, _, node, _, placement) = setup(&test.store).await;
    let now = Timestamp::now();
    let status = swarmy_core::AgentCallStatus {
        agent_id: placement.agent_id,
        node_id: node.node_id,
        epoch: placement.epoch,
        holder_session_id: None,
        queued_calls: 0,
        observed_at: now,
        expires_at: now.checked_add(Duration::from_secs(30)).unwrap(),
    };
    assert!(matches!(
        test.store
            .put_agent_call_status_racing(&status, async {
                test.store
                    .delete_computer(placement.agent_id)
                    .await
                    .unwrap();
            })
            .await,
        Err(StoreError::Domain(
            swarmy_store::DomainError::ComputerDeleted
        ))
    ));
}

#[tokio::test]
async fn takeover_racing_status_write_fences_on_placement_epoch() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let (_, _, node, _, placement) = setup(&test.store).await;
    // The two handles share the database but disagree on whether the old lease
    // has expired, so takeover can commit while the node's read sees it live.
    let live_now = Timestamp::now();
    let expired_now = placement
        .expires_at
        .checked_add(Duration::from_millis(1))
        .unwrap();
    let fenced = test.store.clone().with_clock(move || live_now);
    let racing = test.store.clone().with_clock(move || expired_now);
    let status = swarmy_core::AgentCallStatus {
        agent_id: placement.agent_id,
        node_id: node.node_id,
        epoch: placement.epoch,
        holder_session_id: None,
        queued_calls: 0,
        observed_at: live_now,
        expires_at: placement.expires_at,
    };
    assert!(matches!(
        fenced
            .put_agent_call_status_racing(&status, async {
                let next = racing
                    .take_over(
                        &placement,
                        node.node_id,
                        expired_now.checked_add(Duration::from_secs(60)).unwrap(),
                    )
                    .await
                    .unwrap();
                assert!(next.epoch > placement.epoch);
            })
            .await,
        Err(StoreError::Fence(
            swarmy_store::FenceError::PlacementMismatch
        ))
    ));
}

#[tokio::test]
async fn completion_survives_renewal_and_last_completion_wakes_the_session() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let (session, image, _, jobs, placement) = setup(store).await;
    let claim = |job: &ToolJob| PlacedToolClaim {
        job: job.clone(),
        owner: owner(),
        placement: placement.clone(),
        expires_at: Timestamp::now()
            .checked_add(Duration::from_secs(30))
            .unwrap(),
    };
    let result = BashResult {
        stdout: "done".into(),
        stderr: String::new(),
        exit_code: 0,
        timed_out: false,
        manifest_id: image,
    }
    .tool_result();
    let first = claim(&jobs[0]);
    assert!(store.claim_placed_tool(&first).await.unwrap());
    assert_eq!(
        store
            .complete_placed_tool_racing(&first, 2, &result, async {
                store
                    .renew_hosted(
                        &placement,
                        Timestamp::now()
                            .checked_add(Duration::from_secs(60))
                            .unwrap(),
                    )
                    .await
                    .unwrap();
            })
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store.fetch_session(session).await.unwrap().unwrap().state,
        SessionState::WaitingTools
    );
    let second = claim(&jobs[1]);
    assert!(store.claim_placed_tool(&second).await.unwrap());
    store
        .complete_placed_tool(&second, 3, &result)
        .await
        .unwrap();
    assert_eq!(
        store.fetch_session(session).await.unwrap().unwrap().state,
        SessionState::Runnable
    );
}

#[tokio::test]
async fn writer_lease_renewal_is_fenced_by_release() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let (session, _, node, _, placement) = setup(store).await;
    let volume = store.agent_volume(session, &placement).await.unwrap();
    let writer = store
        .acquire_writer_lease(
            volume,
            LeaseOwnerId::from_ulid(node.node_id.as_ulid()),
            Timestamp::now()
                .checked_add(Duration::from_secs(30))
                .unwrap(),
        )
        .await
        .unwrap();
    let renewed = store
        .renew_hosted(
            &placement,
            Timestamp::now()
                .checked_add(Duration::from_secs(60))
                .unwrap(),
        )
        .await
        .unwrap()
        .0;
    let writer = store
        .renew_writer_lease(
            volume,
            &writer,
            Timestamp::now()
                .checked_add(Duration::from_secs(60))
                .unwrap(),
        )
        .await
        .unwrap();
    store.release(&renewed).await.unwrap();
    assert!(matches!(
        store
            .renew_writer_lease(
                volume,
                &writer,
                Timestamp::now()
                    .checked_add(Duration::from_secs(90))
                    .unwrap()
            )
            .await,
        Err(StoreError::Fence(
            swarmy_store::FenceError::PlacementMismatch
        ))
    ));
}

fn tool_expiry() -> Timestamp {
    Timestamp::now()
        .checked_add(Duration::from_secs(60))
        .unwrap()
}

#[tokio::test]
async fn claim_repeat_after_commit_returns_true() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let (session, _, _, jobs, placement) = setup(&test.store).await;
    let claim = PlacedToolClaim {
        job: jobs[0].clone(),
        owner: owner(),
        placement,
        expires_at: tool_expiry(),
    };
    let store = &test.store;
    let (claimed, attempts) = store
        .claim_placed_tool_racing(&claim, async {
            assert!(store.claim_placed_tool(&claim).await.unwrap());
        })
        .await
        .unwrap();
    assert!(claimed);
    assert_eq!(attempts, 2);
    assert!(
        !store
            .claim_placed_tool(&PlacedToolClaim {
                owner: owner(),
                ..claim
            })
            .await
            .unwrap()
    );
    assert_eq!(
        store.fetch_session(session).await.unwrap().unwrap().state,
        SessionState::WaitingTools
    );
}

#[tokio::test]
async fn claim_renewal_extends_to_at_least() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let (_, image, _, jobs, placement) = setup(&test.store).await;
    let claim = PlacedToolClaim {
        job: jobs[0].clone(),
        owner: owner(),
        placement,
        expires_at: tool_expiry(),
    };
    let store = &test.store;
    assert!(store.claim_placed_tool(&claim).await.unwrap());
    let later = Timestamp::now()
        .checked_add(Duration::from_secs(120))
        .unwrap();
    assert_eq!(store.renew_placed_tool(&claim, later).await.unwrap(), later);
    assert_eq!(store.renew_placed_tool(&claim, later).await.unwrap(), later);
    assert_eq!(
        store
            .renew_placed_tool(&claim, claim.expires_at)
            .await
            .unwrap(),
        later
    );
    let result = BashResult {
        stdout: String::new(),
        stderr: String::new(),
        exit_code: 0,
        timed_out: false,
        manifest_id: image,
    };
    assert_eq!(
        store
            .complete_placed_tool(&claim, 2, &result.tool_result())
            .await
            .unwrap()
            .state,
        SessionState::WaitingTools
    );
}

#[tokio::test]
async fn completion_repeat_after_commit_answers_done() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let (_, image, _, jobs, placement) = setup(&test.store).await;
    let store = &test.store;
    let claim = PlacedToolClaim {
        job: jobs[0].clone(),
        owner: owner(),
        placement: placement.clone(),
        expires_at: tool_expiry(),
    };
    let result = BashResult {
        stdout: String::new(),
        stderr: String::new(),
        exit_code: 0,
        timed_out: false,
        manifest_id: image,
    }
    .tool_result();
    assert!(store.claim_placed_tool(&claim).await.unwrap());
    let raced = store
        .complete_placed_tool_racing(&claim, 2, &result, async {
            assert_eq!(
                store
                    .complete_placed_tool(&claim, 2, &result)
                    .await
                    .unwrap(),
                swarmy_store::PlacedToolCompletion {
                    head_seq: 3,
                    state: SessionState::WaitingTools
                }
            );
        })
        .await;
    assert!(matches!(
        raced,
        Err(StoreError::Fence(
            swarmy_store::FenceError::PlacedToolClaimMismatch
        ))
    ));
    assert!(store.tool_completed(jobs[0].request_id).await.unwrap());
    let second = PlacedToolClaim {
        job: jobs[1].clone(),
        owner: owner(),
        placement,
        expires_at: tool_expiry(),
    };
    assert!(store.claim_placed_tool(&second).await.unwrap());
    assert_eq!(
        store
            .complete_placed_tool(&second, 3, &result)
            .await
            .unwrap(),
        swarmy_store::PlacedToolCompletion {
            head_seq: 4,
            state: SessionState::Runnable
        }
    );
}

#[tokio::test]
async fn writer_acquire_repeats_for_the_bound_owner() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let (session, _, node, _, placement) = setup(&test.store).await;
    let store = &test.store;
    let volume = store.agent_volume(session, &placement).await.unwrap();
    let owner = LeaseOwnerId::from_ulid(node.node_id.as_ulid());
    let first = store
        .acquire_writer_lease(volume, owner, tool_expiry())
        .await
        .unwrap();
    let repeat = store
        .acquire_writer_lease(volume, owner, tool_expiry())
        .await
        .unwrap();
    assert_eq!(first.seq, repeat.seq);
    assert!(matches!(
        store
            .acquire_writer_lease(volume, super::owner(), tool_expiry())
            .await,
        Err(StoreError::Fence(
            swarmy_store::FenceError::VolumeLeaseMismatch
        ))
    ));
    store.release_writer_lease(volume, &first).await.unwrap();
    store.release_writer_lease(volume, &first).await.unwrap();
    let next = store
        .acquire_writer_lease(volume, owner, tool_expiry())
        .await
        .unwrap();
    assert_eq!(next.seq, first.seq + 1);
    assert!(matches!(
        store.release_writer_lease(volume, &first).await,
        Err(StoreError::Fence(
            swarmy_store::FenceError::VolumeLeaseMismatch
        ))
    ));
}

#[tokio::test]
async fn open_hosting_refuses_a_released_dispatch_placement() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let (_, _, node, jobs, placement) = setup(&test.store).await;
    test.store.release(&placement).await.unwrap();
    assert!(matches!(
        test.store
            .open_hosting(
                node.node_id,
                placement.agent_id,
                &jobs[0],
                None,
                Duration::from_secs(30)
            )
            .await,
        Err(StoreError::Fence(
            swarmy_store::FenceError::DispatchPlacementStale { .. }
        ))
    ));
    assert!(
        test.store
            .get_by_agent(placement.agent_id)
            .await
            .unwrap()
            .is_none()
    );
}
