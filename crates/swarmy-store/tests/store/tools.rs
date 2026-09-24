use super::*;
use std::time::Duration;
use swarmy_core::{
    BashArguments, BashResult, NodeCapacity, NodeId, NodeRecord, NodeRole, ToolCallId,
    ToolCallRecord, ToolClaim, ToolJob,
};

async fn setup(store: &Store) -> (SessionId, ManifestId, NodeRecord, Vec<ToolJob>) {
    let mut session = session();
    session.state = SessionState::Idle;
    let id = session.session_id;
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
        .put_image("base", &ImageTag("test".into()), image)
        .await
        .unwrap();
    store
        .create_session(&session, Timestamp::now(), "base:test")
        .await
        .unwrap();
    assert!(matches!(
        store.place_sandbox(id, Timestamp::now()).await,
        Err(StoreError::InvalidState)
    ));
    let node = node();
    store.put_node(&node).await.unwrap();
    let placement = store.place_sandbox(id, Timestamp::now()).await.unwrap();
    assert_eq!(placement.manifest_id, image);
    assert_eq!(
        placement,
        store.place_sandbox(id, Timestamp::now()).await.unwrap()
    );
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
    let jobs: Vec<_> = (1..=2)
        .map(|step| ToolJob {
            session_id: id,
            step,
            request_id: RequestId::for_step(id, step),
            call_id: ToolCallId(format!("bash-{step}")),
            arguments: swarmy_core::SandboxArguments::Bash(BashArguments {
                command: "echo test".into(),
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
    store
        .append_events_leased(id, 0, &events, &lease, Timestamp::now())
        .await
        .unwrap();
    let mut wrong = jobs.clone();
    wrong[0].arguments =
        swarmy_core::SandboxArguments::parse("bash", serde_json::json!({"command":"different"}))
            .unwrap();
    assert!(store.dispatch_tool_jobs(id, &lease, &wrong).await.is_err());
    store.dispatch_tool_jobs(id, &lease, &jobs).await.unwrap();
    assert_eq!(
        store.fetch_session(id).await.unwrap().unwrap().state,
        SessionState::WaitingTools
    );
    assert_eq!(store.scan_tool_jobs(None, 64).await.unwrap().len(), 2);
    (id, image, node, jobs)
}

fn claim(job: &ToolJob, node: NodeId) -> ToolClaim {
    ToolClaim {
        job: job.clone(),
        owner: owner(),
        node_id: node,
        expires_at: Timestamp::now()
            .checked_add(Duration::from_secs(30))
            .unwrap(),
        attempt_volume: VolumeId::from_ulid(Ulid::generate()),
    }
}

async fn flush(store: &Store, claim: &ToolClaim) -> BashResult {
    let previous = store
        .get_volume(claim.attempt_volume)
        .await
        .unwrap()
        .unwrap()
        .head_manifest;
    let lease = store
        .acquire_writer_lease(
            claim.attempt_volume,
            owner(),
            Timestamp::now(),
            Timestamp::now()
                .checked_add(Duration::from_secs(30))
                .unwrap(),
        )
        .await
        .unwrap();
    let manifest = ManifestId::from_ulid(Ulid::generate());
    store
        .advance_volume(
            claim.attempt_volume,
            &lease,
            previous,
            manifest,
            &ManifestHeader {
                size: u64::from(CHUNK_SIZE),
                chunk_size: CHUNK_SIZE,
                root_hash: ContentHash([2; 32]),
            },
        )
        .await
        .unwrap();
    store
        .release_writer_lease(claim.attempt_volume, &lease, Timestamp::now())
        .await
        .unwrap();
    BashResult {
        stdout: "test\n".into(),
        stderr: "diagnostic\n".into(),
        exit_code: 7,
        timed_out: false,
        manifest_id: manifest,
    }
}

#[tokio::test]
async fn tool_completion_fences_attempts_and_wakes_only_after_last_call() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let (id, image, node, jobs) = setup(store).await;
    let first = claim(&jobs[0], node.node_id);
    assert!(store.claim_tool(&first).await.unwrap());
    for job in &jobs {
        assert!(!store.claim_tool(&claim(job, node.node_id)).await.unwrap());
    }
    let result = flush(store, &first).await;
    // A flush alone must not advance either authoritative disk pointer.
    let sandbox = store.get_sandbox(id).await.unwrap().unwrap();
    assert_eq!(sandbox.manifest_id, image);
    assert_eq!(
        store
            .get_volume(sandbox.volume_id)
            .await
            .unwrap()
            .unwrap()
            .head_manifest,
        image
    );
    let mut stale = first.clone();
    stale.owner = owner();
    assert!(matches!(
        store.complete_tool(&stale, 2, &result).await,
        Err(StoreError::LeaseMismatch)
    ));
    assert!(matches!(
        store
            .renew_tool(
                &stale,
                Timestamp::now()
                    .checked_add(Duration::from_secs(60))
                    .unwrap()
            )
            .await,
        Err(StoreError::LeaseMismatch)
    ));
    assert!(matches!(
        store.complete_tool(&first, 1, &result).await,
        Err(StoreError::StaleSequence { .. })
    ));
    store
        .renew_tool(
            &first,
            Timestamp::now()
                .checked_add(Duration::from_secs(60))
                .unwrap(),
        )
        .await
        .unwrap();
    store.complete_tool(&first, 2, &result).await.unwrap();
    store.complete_tool(&first, 0, &result).await.unwrap();
    assert!(!store.claim_tool(&first).await.unwrap());
    assert_eq!(
        store.fetch_session(id).await.unwrap().unwrap().state,
        SessionState::WaitingTools
    );
    assert_eq!(
        store.get_sandbox(id).await.unwrap().unwrap().manifest_id,
        result.manifest_id
    );
    let second = claim(&jobs[1], node.node_id);
    assert!(store.claim_tool(&second).await.unwrap());
    assert_eq!(
        store
            .get_volume(second.attempt_volume)
            .await
            .unwrap()
            .unwrap()
            .head_manifest,
        result.manifest_id
    );
    let result = flush(store, &second).await;
    store.complete_tool(&second, 3, &result).await.unwrap();
    let session = store.fetch_session(id).await.unwrap().unwrap();
    assert_eq!(session.state, SessionState::Runnable);
    assert_eq!(session.head_seq, 4);
    assert!(store.scan_tool_jobs(None, 64).await.unwrap().is_empty());
    assert_eq!(
        store
            .scan_runnable(runnable_partition(id), None, 64)
            .await
            .unwrap()
            .len(),
        1
    );
    let events = store.read_events(id, 3, 64).await.unwrap();
    assert!(
        matches!(&events[0], Event::ToolCallCompleted { result: stored, .. } if *stored == result.tool_result())
    );
    test.cleanup().await;
}

#[tokio::test]
async fn expired_tool_relocates_and_discards_even_a_flushed_attempt() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let (id, image, node, jobs) = setup(store).await;
    let mut old = claim(&jobs[0], node.node_id);
    old.expires_at = Timestamp::now()
        .checked_add(Duration::from_secs(1))
        .unwrap();
    assert!(store.claim_tool(&old).await.unwrap());
    let uncommitted = flush(store, &old).await;
    let later = Timestamp::now()
        .checked_add(Duration::from_secs(31))
        .unwrap();
    let mut replacement = node.clone();
    replacement.node_id = NodeId::from_ulid(Ulid::generate());
    replacement.last_heartbeat = later;
    store.put_node(&replacement).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let placement = store.place_sandbox(id, later).await.unwrap();
    assert_eq!(placement.node_id, replacement.node_id);
    assert!(matches!(
        store.claim_tool(&claim(&jobs[0], node.node_id)).await,
        Err(StoreError::LeaseMismatch)
    ));
    let next = claim(&jobs[0], replacement.node_id);
    assert!(store.claim_tool(&next).await.unwrap());
    assert_eq!(
        store
            .get_volume(next.attempt_volume)
            .await
            .unwrap()
            .unwrap()
            .head_manifest,
        image
    );
    assert!(matches!(
        store.complete_tool(&old, 2, &uncommitted).await,
        Err(StoreError::LeaseMismatch)
    ));
    assert!(store.renew_tool(&old, later).await.is_err());
    let committed = flush(store, &next).await;
    store.complete_tool(&next, 2, &committed).await.unwrap();
    assert_eq!(
        store.get_sandbox(id).await.unwrap().unwrap().manifest_id,
        committed.manifest_id
    );
    test.cleanup().await;
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
    }
}

#[tokio::test]
async fn persistent_calls_fence_epochs_without_publishing_or_cloning() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let (session, image, node, jobs) = setup(store).await;
    let agent = store
        .fetch_session(session)
        .await
        .unwrap()
        .unwrap()
        .agent_id;
    let expiry = || {
        Timestamp::now()
            .checked_add(Duration::from_secs(30))
            .unwrap()
    };
    let placement = store.place(agent, node.node_id, expiry()).await.unwrap();
    let volume = store.agent_volume(session, &placement).await.unwrap();
    assert_eq!(volume.as_ulid(), agent.as_ulid());
    let writer = store
        .acquire_writer_lease(
            volume,
            LeaseOwnerId::from_ulid(node.node_id.as_ulid()),
            Timestamp::now(),
            expiry(),
        )
        .await
        .unwrap();
    let before = store.list_volumes(None, 64).await.unwrap();
    let claim = swarmy_core::PlacedToolClaim {
        job: jobs[0].clone(),
        owner: owner(),
        placement: placement.clone(),
        expires_at: expiry(),
    };
    check_tool_admission(store, &claim.job, &placement).await;
    assert!(store.claim_placed_tool(&claim).await.unwrap());
    assert!(!store.claim_placed_tool(&claim).await.unwrap());
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
    let claim = swarmy_core::PlacedToolClaim {
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
        Err(StoreError::LeaseMismatch)
    ));
    assert!(matches!(
        store.renew_placed_tool(&claim, expiry()).await,
        Err(StoreError::LeaseMismatch)
    ));
    assert!(matches!(
        store.renew_writer_lease(volume, &writer, expiry()).await,
        Err(StoreError::LeaseMismatch)
    ));
    check_stale_publication(store, volume, &writer, image).await;
    assert!(matches!(
        store.agent_volume(session, &next).await,
        Err(StoreError::LeaseMismatch)
    ));
    test.cleanup().await;
}

async fn check_tool_admission(
    store: &Store,
    job: &ToolJob,
    placement: &swarmy_core::PlacementRecord,
) {
    // Legacy jobs acquire their dispatch fence through recovery.
    assert!(store.route_tool_job(job, placement).await.unwrap());
    assert_eq!(
        store.tool_agent(job, placement.node_id).await.unwrap(),
        Some(placement.agent_id)
    );
    assert!(matches!(
        store
            .tool_agent(job, NodeId::from_ulid(Ulid::generate()))
            .await,
        Err(StoreError::LeaseMismatch)
    ));
}

async fn check_stale_publication(
    store: &Store,
    volume: swarmy_core::VolumeId,
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
        Err(StoreError::LeaseMismatch)
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
        Err(StoreError::LeaseMismatch)
    ));
    let stale = swarmy_core::PlacementRecord {
        epoch: placement.epoch + 1,
        ..placement.clone()
    };
    assert!(matches!(
        store
            .dispatch_tool_calls(id, 0, &lease, &calls, &stale)
            .await,
        Err(StoreError::LeaseMismatch)
    ));
    assert!(matches!(
        store
            .dispatch_tool_calls(id, 1, &lease, &calls, &placement)
            .await,
        Err(StoreError::StaleSequence { .. })
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
        store.release_lease(id, &lease, Timestamp::now()).await,
        Err(StoreError::LeaseMismatch)
    ));
    test.cleanup().await;
}
