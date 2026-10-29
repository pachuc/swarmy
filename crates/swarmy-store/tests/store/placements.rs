use super::*;
use std::time::Duration;
use swarmy_core::{
    NodeCapacity, NodeId, NodeRecord, NodeRole, PlacementChangeReason, PlacementRecord,
};

fn future(seconds: u64) -> Timestamp {
    Timestamp::now()
        .checked_add(Duration::from_secs(seconds))
        .unwrap()
}

async fn node(store: &Store, capacity: u32) -> NodeRecord {
    let record = NodeRecord {
        node_id: NodeId::from_ulid(Ulid::generate()),
        roles: vec![NodeRole::Sandbox],
        capacity: NodeCapacity {
            cpu_millis: 1000,
            memory_bytes: 4 * 1024 * 1024 * 1024,
            disk_bytes: 1024,
            sandboxes: capacity,
        },
        last_heartbeat: Timestamp::now(),
        cached_images: vec![],
        gpu: None,
    };
    store.put_node(&record).await.unwrap();
    record
}

// Expire the real stored lease without sleeping, so tests also exercise decoding
// and the production clock checks while remaining independent of machine speed.
async fn expire(test: &TestStore, record: &PlacementRecord) -> PlacementRecord {
    let mut expired = record.clone();
    expired.expires_at = timestamp(0);
    let value = encode(&expired).unwrap();
    test.db
        .run(|trx, _| {
            let value = &value;
            async move {
                trx.set(
                    &test
                        .root
                        .pack(&("placement", record.agent_id.as_ulid().to_bytes().as_slice())),
                    value,
                );
                trx.set(
                    &test.root.pack(&(
                        "placement_by_node",
                        record.node_id.as_ulid().to_bytes().as_slice(),
                        record.agent_id.as_ulid().to_bytes().as_slice(),
                    )),
                    value,
                );
                Ok(())
            }
        })
        .await
        .unwrap();
    expired
}

struct Renewed {
    test: TestStore,
    first: PlacementRecord,
    live: PlacementRecord,
}

async fn placed_renewed() -> Option<Renewed> {
    let test = TestStore::memory()?;
    let a = node(&test.store, 1).await.node_id;
    let agent = session().agent_id;
    let first = test.store.place(agent, a, future(60)).await.unwrap();
    test.store.claim_placement(&first).await.unwrap();
    let renewed = test.store.renew(&first, future(120)).await.unwrap();
    Some(Renewed {
        test,
        first,
        live: renewed,
    })
}

#[tokio::test]
async fn placement_initial_place_fences_a_second_live_placement() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let a = node(&test.store, 1).await.node_id;
    let b = node(&test.store, 1).await.node_id;
    let agent = session().agent_id;
    let first = test.store.place(agent, a, future(60)).await.unwrap();
    assert_eq!(first.epoch, 1, "initial placement starts at epoch 1");
    assert_eq!(
        first.last_change_reason,
        PlacementChangeReason::Initial,
        "initial placement reason"
    );
    assert_eq!(
        test.store.get_by_agent(agent).await.unwrap(),
        Some(first.clone()),
        "initial placement is readable"
    );
    assert!(
        matches!(
            test.store.place(agent, b, future(60)).await,
            Err(StoreError::Domain(
                swarmy_store::DomainError::PlacementExists
            ))
        ),
        "second placement while live is rejected"
    );
}

#[tokio::test]
async fn placement_claim_and_renew_keep_identity_and_fence_stale_expiry() {
    let Some(state) = placed_renewed().await else {
        return;
    };
    let Renewed {
        test,
        first,
        live: renewed,
    } = &state;
    assert_eq!(renewed.epoch, 1, "renew keeps the epoch");
    assert_eq!(
        renewed.last_changed_at, first.last_changed_at,
        "renew keeps the change timestamp"
    );
    assert_eq!(
        renewed.last_change_reason, first.last_change_reason,
        "renew keeps the change reason"
    );
    assert!(
        matches!(
            test.store.renew(renewed, renewed.expires_at).await,
            Err(StoreError::Fence(
                swarmy_store::FenceError::PlacementMismatch
            ))
        ),
        "renew at the current expiry is fenced"
    );
}

#[tokio::test]
async fn placement_impostor_node_operations_are_fenced() {
    let Some(state) = placed_renewed().await else {
        return;
    };
    let Renewed {
        test,
        live: renewed,
        ..
    } = &state;
    let b = node(&test.store, 1).await.node_id;
    let impostor = PlacementRecord {
        node_id: b,
        ..renewed.clone()
    };
    assert!(
        matches!(
            test.store.renew(&impostor, future(180)).await,
            Err(StoreError::Fence(
                swarmy_store::FenceError::PlacementMismatch
            ))
        ),
        "impostor node renew is fenced"
    );
    assert!(
        matches!(
            test.store.release(&impostor).await,
            Err(StoreError::Fence(
                swarmy_store::FenceError::PlacementMismatch
            ))
        ),
        "impostor node release is fenced"
    );
    assert!(
        matches!(
            test.store.take_over(renewed, b, future(60)).await,
            Err(StoreError::Fence(
                swarmy_store::FenceError::PlacementMismatch
            ))
        ),
        "takeover on a live placement is fenced"
    );
}

#[tokio::test]
async fn placement_expiry_hands_takeover_to_waiting_node_and_fences_stale_records() {
    let Some(state) = placed_renewed().await else {
        return;
    };
    let Renewed {
        test,
        live: renewed,
        ..
    } = &state;
    let a = renewed.node_id;
    let b = node(&test.store, 1).await.node_id;
    let expired = expire(test, renewed).await;
    assert!(
        matches!(
            test.store.renew(&expired, future(60)).await,
            Err(StoreError::Fence(
                swarmy_store::FenceError::PlacementMismatch
            ))
        ),
        "renew of an expired placement is fenced"
    );
    assert!(
        matches!(
            test.store.release(&expired).await,
            Err(StoreError::Fence(
                swarmy_store::FenceError::PlacementMismatch
            ))
        ),
        "release of an expired placement is fenced"
    );
    let next = test.store.take_over(&expired, b, future(60)).await.unwrap();
    assert_eq!(next.epoch, 2, "takeover after expiry starts epoch 2");
    assert_eq!(
        next.last_change_reason,
        PlacementChangeReason::Failure,
        "takeover reason is failure"
    );
    let first_changed_at = state.first.last_changed_at;
    assert!(
        next.last_changed_at >= first_changed_at,
        "takeover timestamp advances"
    );
    assert!(
        test.store
            .list_by_node(a, None, 64)
            .await
            .unwrap()
            .is_empty(),
        "old node lists nothing after takeover"
    );
    assert_eq!(
        test.store.list_by_node(b, None, 64).await.unwrap(),
        vec![next.clone()],
        "new node lists the takeover"
    );
    assert!(
        matches!(
            test.store.renew(renewed, future(180)).await,
            Err(StoreError::Fence(
                swarmy_store::FenceError::PlacementMismatch
            ))
        ),
        "stale pre-takeover renew is fenced"
    );
    assert!(
        matches!(
            test.store.release(renewed).await,
            Err(StoreError::Fence(
                swarmy_store::FenceError::PlacementMismatch
            ))
        ),
        "stale pre-takeover release is fenced"
    );
    assert!(
        matches!(
            test.store.take_over(&expired, a, future(60)).await,
            Err(StoreError::Fence(
                swarmy_store::FenceError::PlacementMismatch
            ))
        ),
        "takeover with the superseded record is fenced"
    );
}

#[tokio::test]
async fn placement_address_follows_its_epoch() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let first_node = node(&test.store, 1).await.node_id;
    let next_node = node(&test.store, 1).await.node_id;
    let agent = session().agent_id;
    let first = test
        .store
        .place(agent, first_node, future(60))
        .await
        .unwrap();
    let address = std::net::Ipv4Addr::new(10, 0, 2, 2);
    test.store
        .set_placement_address(&first, address)
        .await
        .unwrap();
    let renewed = test.store.renew(&first, future(120)).await.unwrap();
    assert_eq!(
        test.store.placement_address(&renewed).await.unwrap(),
        Some(address)
    );
    let expired = expire(&test, &renewed).await;
    let next = test
        .store
        .take_over(&expired, next_node, future(60))
        .await
        .unwrap();
    assert_eq!(test.store.placement_address(&next).await.unwrap(), None);
    assert!(matches!(
        test.store.set_placement_address(&first, address).await,
        Err(StoreError::Fence(
            swarmy_store::FenceError::PlacementMismatch
        ))
    ));
    test.store
        .set_placement_address(&next, address)
        .await
        .unwrap();
    test.store.release(&next).await.unwrap();
    assert_eq!(test.store.placement_address(&next).await.unwrap(), None);
}

#[tokio::test]
async fn placement_release_preserves_epoch_and_rejects_old_tokens() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let b = node(&test.store, 1).await.node_id;
    let agent = session().agent_id;
    let next = test.store.place(agent, b, future(60)).await.unwrap();
    test.store.release(&next).await.unwrap();
    assert!(test.store.get_by_agent(agent).await.unwrap().is_none());
    assert!(
        test.store
            .list_by_node(b, None, 64)
            .await
            .unwrap()
            .is_empty()
    );
    let rebuilt = test.store.place(agent, b, future(60)).await.unwrap();
    assert_eq!(rebuilt.epoch, 2);
    assert_eq!(rebuilt.last_change_reason, PlacementChangeReason::Eviction);
    assert!(matches!(
        test.store.renew(&next, future(180)).await,
        Err(StoreError::Fence(
            swarmy_store::FenceError::PlacementMismatch
        ))
    ));
    assert!(matches!(
        test.store.release(&next).await,
        Err(StoreError::Fence(
            swarmy_store::FenceError::PlacementMismatch
        ))
    ));
}

#[tokio::test]
async fn placement_capacity_and_index_are_atomic() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let a = node(&test.store, 1).await;
    let b = node(&test.store, 0).await.node_id;
    let agent = session().agent_id;
    assert!(matches!(
        test.store
            .place(agent, NodeId::from_ulid(Ulid::generate()), future(60))
            .await,
        Err(StoreError::Domain(swarmy_store::DomainError::NodeMissing))
    ));
    assert!(matches!(
        test.store.place(agent, b, future(60)).await,
        Err(StoreError::Domain(
            swarmy_store::DomainError::NodeAtCapacity { .. }
        ))
    ));
    assert!(matches!(
        test.store.place(agent, a.node_id, timestamp(0)).await,
        Err(StoreError::Fence(
            swarmy_store::FenceError::PlacementMismatch
        ))
    ));
    let first = test
        .store
        .place(agent, a.node_id, future(60))
        .await
        .unwrap();
    // Heartbeats must not reset occupancy.
    test.store.put_node(&a).await.unwrap();
    assert!(matches!(
        test.store
            .place(session().agent_id, a.node_id, future(60))
            .await,
        Err(StoreError::Domain(
            swarmy_store::DomainError::NodeAtCapacity { .. }
        ))
    ));
    let expired = expire(&test, &first).await;
    assert!(matches!(
        test.store.take_over(&expired, b, future(60)).await,
        Err(StoreError::Domain(
            swarmy_store::DomainError::NodeAtCapacity { .. }
        ))
    ));
    assert_eq!(
        test.store.get_by_agent(agent).await.unwrap(),
        Some(expired.clone())
    );
    assert!(matches!(
        test.store
            .place(session().agent_id, a.node_id, future(60))
            .await,
        Err(StoreError::Domain(
            swarmy_store::DomainError::NodeAtCapacity { .. }
        ))
    ));
    // A rebuild on the same node can reuse its occupied slot.
    let next = test
        .store
        .take_over(&expired, a.node_id, future(60))
        .await
        .unwrap();
    test.store.release(&next).await.unwrap();
    test.store
        .place(session().agent_id, a.node_id, future(60))
        .await
        .unwrap();
    let mut volume_only = node(&test.store, 1).await;
    volume_only.roles = vec![NodeRole::Volume];
    test.store.put_node(&volume_only).await.unwrap();
    assert!(matches!(
        test.store
            .place(agent, volume_only.node_id, future(60))
            .await,
        Err(StoreError::Domain(
            swarmy_store::DomainError::NodeNotSandbox
        ))
    ));
}

#[tokio::test]
async fn placement_takeover_race_has_exactly_one_winner() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let original = node(&test.store, 1).await.node_id;
    let first = test
        .store
        .place(session().agent_id, original, future(60))
        .await
        .unwrap();
    let expired = expire(&test, &first).await;
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let target = node(&test.store, 1).await.node_id;
        let store = test.store.clone();
        let expected = expired.clone();
        let barrier = barrier.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            store.take_over(&expected, target, future(60)).await
        });
    }
    let mut winners = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result.unwrap() {
            Ok(record) => winners.push(record),
            Err(error) => assert!(matches!(
                error,
                StoreError::Fence(swarmy_store::FenceError::PlacementMismatch)
            )),
        }
    }
    assert_eq!(winners.len(), 1);
    assert_eq!(winners[0].epoch, first.epoch + 1);
    assert_eq!(
        test.store
            .get_by_agent(first.agent_id)
            .await
            .unwrap()
            .as_ref(),
        winners.first()
    );
    assert!(
        test.store
            .list_by_node(original, None, 64)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn placement_capacity_race_and_paginated_node_listing() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let target = node(&test.store, 1).await.node_id;
    let (left, right) = tokio::join!(
        test.store.place(session().agent_id, target, future(60)),
        test.store.place(session().agent_id, target, future(60)),
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    assert!(matches!(
        left.as_ref().err().or(right.as_ref().err()),
        Some(StoreError::Domain(
            swarmy_store::DomainError::NodeAtCapacity { .. }
        ))
    ));
    let winner = left.or(right).unwrap();
    test.store.release(&winner).await.unwrap();
    let target = node(&test.store, 3).await.node_id;
    let mut records = Vec::new();
    for _ in 0..3 {
        records.push(
            test.store
                .place(session().agent_id, target, future(60))
                .await
                .unwrap(),
        );
    }
    records.sort_by_key(|record| record.agent_id);
    assert_eq!(
        test.store.list_by_node(target, None, 2).await.unwrap(),
        records[..2]
    );
    assert_eq!(
        test.store
            .list_by_node(target, Some(records[1].agent_id), 2)
            .await
            .unwrap(),
        records[2..]
    );
    assert!(
        test.store
            .list_by_node(target, Some(records[2].agent_id), 2)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        test.store.list_by_node(target, None, 0).await,
        Err(StoreError::Domain(swarmy_store::DomainError::InvalidLimit))
    ));
}

#[tokio::test]
async fn hosting_claims_and_renewals_distinguish_loss_from_unstarted_takeover() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let a = node(&test.store, 1).await.node_id;
    let b = node(&test.store, 1).await.node_id;
    let first = test
        .store
        .place(session().agent_id, a, future(60))
        .await
        .unwrap();
    // Renewing a grant alone does not prove that a node started hosting it.
    let first = test.store.renew(&first, future(120)).await.unwrap();
    let expired = expire(&test, &first).await;
    assert!(matches!(
        test.store.claim_placement(&expired).await,
        Err(StoreError::Fence(
            swarmy_store::FenceError::PlacementMismatch
        ))
    ));
    let unstarted = test.store.take_over(&expired, b, future(60)).await.unwrap();
    assert_eq!(
        unstarted.last_change_reason,
        PlacementChangeReason::Unstarted
    );
    assert_eq!(
        test.store
            .placement_failure_estimate(&unstarted)
            .await
            .unwrap(),
        None
    );
    assert!(matches!(
        test.store.claim_placement(&first).await,
        Err(StoreError::Fence(
            swarmy_store::FenceError::PlacementMismatch
        ))
    ));
    let impostor = PlacementRecord {
        node_id: a,
        ..unstarted.clone()
    };
    assert!(matches!(
        test.store.claim_placement(&impostor).await,
        Err(StoreError::Fence(
            swarmy_store::FenceError::PlacementMismatch
        ))
    ));
    test.store.claim_placement(&unstarted).await.unwrap();
    let before_renewal = Timestamp::now();
    let renewed = test.store.renew(&unstarted, future(120)).await.unwrap();
    let after_renewal = Timestamp::now();
    // An idempotent claim must not advance the last evidence of liveness.
    test.store.claim_placement(&unstarted).await.unwrap();
    let expired = expire(&test, &renewed).await;
    let recovered = test.store.take_over(&expired, a, future(60)).await.unwrap();
    assert_eq!(recovered.epoch, 3);
    assert_eq!(recovered.last_change_reason, PlacementChangeReason::Failure);
    let estimate = test
        .store
        .placement_failure_estimate(&recovered)
        .await
        .unwrap()
        .unwrap();
    assert!(estimate >= before_renewal && estimate <= after_renewal);
    assert!(estimate <= recovered.last_changed_at);
    // A later unstarted takeover clears the earlier failure estimate.
    let expired = expire(&test, &recovered).await;
    let retry = test.store.take_over(&expired, b, future(60)).await.unwrap();
    assert_eq!(retry.epoch, 4);
    assert_eq!(retry.last_change_reason, PlacementChangeReason::Unstarted);
    assert_eq!(
        test.store.placement_failure_estimate(&retry).await.unwrap(),
        None
    );
}

#[tokio::test]
async fn placement_memory_budget_is_atomic_and_released() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let mut record = node(&test.store, 8).await;
    // The default sandbox reserves 768 MiB. One fits, but a second does not.
    record.capacity.memory_bytes = 1280 * 1024 * 1024;
    test.store.put_node(&record).await.unwrap();
    let first = test
        .store
        .place(session().agent_id, record.node_id, future(60))
        .await
        .unwrap();
    assert_eq!(
        test.store.committed_memory(record.node_id).await.unwrap(),
        768 * 1024 * 1024
    );
    let second_agent = session().agent_id;
    assert!(matches!(
        test.store
            .place(second_agent, record.node_id, future(60))
            .await,
        Err(StoreError::Domain(
            swarmy_store::DomainError::NodeAtCapacity { .. }
        ))
    ));
    test.store.release(&first).await.unwrap();
    assert_eq!(
        test.store.committed_memory(record.node_id).await.unwrap(),
        0
    );
    test.store
        .place(second_agent, record.node_id, future(60))
        .await
        .unwrap();
}

#[tokio::test]
async fn sixteen_default_or_four_large_sandboxes_fill_standard_budget() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let mut record = node(&test.store, 32).await;
    record.capacity.memory_bytes = 12 * 1024 * 1024 * 1024;
    test.store.put_node(&record).await.unwrap();
    let mut defaults = Vec::new();
    for _ in 0..16 {
        defaults.push(
            test.store
                .place(session().agent_id, record.node_id, future(60))
                .await
                .unwrap(),
        );
    }
    assert_eq!(
        test.store.committed_memory(record.node_id).await.unwrap(),
        record.capacity.memory_bytes
    );
    assert!(matches!(
        test.store
            .place(session().agent_id, record.node_id, future(60))
            .await,
        Err(StoreError::Domain(
            swarmy_store::DomainError::NodeAtCapacity { .. }
        ))
    ));
    for placed in &defaults {
        test.store.release(placed).await.unwrap();
    }
    let mut large = Vec::new();
    for _ in 0..5 {
        let agent = session().agent_id;
        let key = test
            .root
            .pack(&("computer_memory", agent.as_ulid().to_bytes().as_slice()));
        let bytes = encode(&3072_u64).unwrap();
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
        large.push(agent);
    }
    for agent in large.iter().take(4) {
        test.store
            .place(*agent, record.node_id, future(60))
            .await
            .unwrap();
    }
    assert!(matches!(
        test.store.place(large[4], record.node_id, future(60)).await,
        Err(StoreError::Domain(
            swarmy_store::DomainError::NodeAtCapacity { .. }
        ))
    ));
}

#[tokio::test]
async fn gpu_change_requests_release_and_next_placement_records_it() {
    use swarmy_core::{AgentSettings, GpuRequirement};
    let Some(test) = TestStore::memory() else {
        return;
    };
    let image = swarmy_testkit::image(&test.store).await;
    let agent = test
        .store
        .create_agent("gpu-change", image, "", timestamp(0), None)
        .await
        .unwrap()
        .agent_id;
    let host = node(&test.store, 2).await.node_id;
    let first = test.store.place(agent, host, future(60)).await.unwrap();
    test.store.claim_placement(&first).await.unwrap();
    test.store
        .set_agent_with_resets(
            agent,
            &AgentSettings {
                gpu: Some(GpuRequirement::Shared),
                ..Default::default()
            },
            &[],
        )
        .await
        .unwrap();
    assert!(
        test.store
            .placement_release_requested(&first)
            .await
            .unwrap()
    );
    let renewed = test.store.renew(&first, future(120)).await.unwrap();
    assert!(
        test.store
            .placement_release_requested(&renewed)
            .await
            .unwrap()
    );
    assert!(matches!(
        test.store
            .set_agent_with_resets(
                agent,
                &AgentSettings {
                    memory_mib: Some(2048),
                    ..Default::default()
                },
                &[]
            )
            .await,
        Err(StoreError::Domain(
            swarmy_store::DomainError::ActiveSandboxRequirements
        ))
    ));
    test.store.release(&renewed).await.unwrap();
    let gpu_host = gpu_node(&test.store, 2, 1).await.node_id;
    let second = test
        .store
        .place(agent, gpu_host, future(180))
        .await
        .unwrap();
    assert_eq!(
        second.last_change_reason,
        PlacementChangeReason::RequirementChange
    );
    assert!(
        !test
            .store
            .placement_release_requested(&second)
            .await
            .unwrap()
    );
    test.store.release(&second).await.unwrap();
    let third = test
        .store
        .place(agent, gpu_host, future(180))
        .await
        .unwrap();
    assert_eq!(third.last_change_reason, PlacementChangeReason::Eviction);
}

#[tokio::test]
async fn requirement_change_without_placement_requests_nothing() {
    use swarmy_core::{AgentSettings, GpuRequirement};
    let Some(test) = TestStore::memory() else {
        return;
    };
    let image = swarmy_testkit::image(&test.store).await;
    let agent = test
        .store
        .create_agent("gpu-unplaced", image, "", timestamp(0), None)
        .await
        .unwrap()
        .agent_id;
    test.store
        .set_agent_with_resets(
            agent,
            &AgentSettings {
                gpu: Some(GpuRequirement::Shared),
                ..Default::default()
            },
            &[],
        )
        .await
        .unwrap();
    assert!(!test.store.request_placement_release(agent).await.unwrap());
}

async fn gpu_node(store: &Store, sandboxes: u32, cards: u32) -> NodeRecord {
    let mut record = node(store, sandboxes).await;
    record.capacity.memory_bytes = 16 * 1024 * 1024 * 1024;
    record.gpu = (cards > 0).then(|| swarmy_core::GpuCards {
        model: "NVIDIA L4".into(),
        memory_mib: 23034,
        count: cards,
    });
    store.put_node(&record).await.unwrap();
    record
}

async fn gpu_agent(store: &Store, name: &str, mode: swarmy_core::GpuRequirement) -> AgentId {
    let image = swarmy_testkit::image(store).await;
    let settings = swarmy_core::AgentSettings {
        gpu: Some(mode),
        ..Default::default()
    };
    store
        .create_agent(
            name,
            image,
            "",
            Timestamp::now(),
            Some(swarmy_store::CreateAgentOptions {
                settings: Some(&settings),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .agent_id
}

fn gpu_unavailable(error: &StoreError) {
    assert!(
        matches!(
            error,
            StoreError::Domain(swarmy_store::DomainError::GpuUnavailable { .. })
        ),
        "{error}"
    );
}

#[tokio::test]
async fn shared_gpu_sandboxes_share_a_card_node_and_never_land_without_one() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let card = gpu_node(&test.store, 4, 1).await;
    for name in ["s1", "s2", "s3"] {
        let agent = gpu_agent(&test.store, name, swarmy_core::GpuRequirement::Shared).await;
        let placement = test
            .store
            .place(agent, card.node_id, future(60))
            .await
            .unwrap();
        assert_eq!(placement.gpu, swarmy_core::GpuRequirement::Shared);
    }
    let cpu = test
        .store
        .place(session().agent_id, card.node_id, future(60))
        .await
        .unwrap();
    assert_eq!(cpu.gpu, swarmy_core::GpuRequirement::None);
    let empty = gpu_node(&test.store, 4, 0).await;
    let agent = gpu_agent(&test.store, "s4", swarmy_core::GpuRequirement::Shared).await;
    gpu_unavailable(
        &test
            .store
            .place(agent, empty.node_id, future(60))
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn dedicated_gpu_excludes_other_gpu_sandboxes_in_both_directions() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let node = gpu_node(&test.store, 4, 1).await.node_id;
    let dedicated = gpu_agent(&test.store, "d1", swarmy_core::GpuRequirement::Dedicated).await;
    let dedicated2 = gpu_agent(&test.store, "d2", swarmy_core::GpuRequirement::Dedicated).await;
    let shared = gpu_agent(&test.store, "s1", swarmy_core::GpuRequirement::Shared).await;
    let first = test.store.place(dedicated, node, future(60)).await.unwrap();
    gpu_unavailable(
        &test
            .store
            .place(shared, node, future(60))
            .await
            .unwrap_err(),
    );
    gpu_unavailable(
        &test
            .store
            .place(dedicated2, node, future(60))
            .await
            .unwrap_err(),
    );
    let cpu = test
        .store
        .place(session().agent_id, node, future(60))
        .await
        .unwrap();
    assert_eq!(cpu.gpu, swarmy_core::GpuRequirement::None);
    test.store.release(&first).await.unwrap();
    let shared_placement = test.store.place(shared, node, future(60)).await.unwrap();
    gpu_unavailable(
        &test
            .store
            .place(dedicated2, node, future(60))
            .await
            .unwrap_err(),
    );
    test.store.release(&shared_placement).await.unwrap();
    let second = test
        .store
        .place(dedicated2, node, future(60))
        .await
        .unwrap();
    assert_eq!(second.gpu, swarmy_core::GpuRequirement::Dedicated);
}

#[tokio::test]
async fn gpu_takeover_readmits_on_the_new_node() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let card = gpu_node(&test.store, 4, 1).await.node_id;
    let cpu = gpu_node(&test.store, 4, 0).await.node_id;
    let agent = gpu_agent(&test.store, "d1", swarmy_core::GpuRequirement::Dedicated).await;
    let first = test.store.place(agent, card, future(60)).await.unwrap();
    test.store.claim_placement(&first).await.unwrap();
    let expired = expire(&test, &first).await;
    gpu_unavailable(
        &test
            .store
            .take_over(&expired, cpu, future(60))
            .await
            .unwrap_err(),
    );
    let replacement = test
        .store
        .take_over(&expired, card, future(60))
        .await
        .unwrap();
    assert_eq!(replacement.gpu, swarmy_core::GpuRequirement::Dedicated);
}

#[tokio::test]
async fn gpu_rejection_leaves_counts_unchanged() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let node = gpu_node(&test.store, 2, 0).await.node_id;
    let agent = gpu_agent(&test.store, "s1", swarmy_core::GpuRequirement::Shared).await;
    gpu_unavailable(&test.store.place(agent, node, future(60)).await.unwrap_err());
    assert_eq!(test.store.committed_memory(node).await.unwrap(), 0);
    let cpu = test
        .store
        .place(session().agent_id, node, future(60))
        .await
        .unwrap();
    assert_eq!(cpu.gpu, swarmy_core::GpuRequirement::None);
    let another = test
        .store
        .place(session().agent_id, node, future(60))
        .await
        .unwrap();
    assert_eq!(another.gpu, swarmy_core::GpuRequirement::None);
}

async fn gpu_tool_session(
    store: &Store,
    name: &str,
    allow: bool,
) -> (AgentId, SessionId, swarmy_core::Lease) {
    use swarmy_core::AgentSettings;
    let agent = store
        .create_agent(
            name,
            swarmy_testkit::image(store).await,
            "",
            Timestamp::now(),
            Some(swarmy_store::CreateAgentOptions {
                settings: Some(&AgentSettings {
                    allow_gpu_request: Some(allow),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .agent_id;
    let (id, _) = store
        .open_main_session(agent, Timestamp::now())
        .await
        .unwrap();
    store.wake_session(id, Timestamp::now()).await.unwrap();
    let lease = store.claim_lease(id, owner(), future(60)).await.unwrap();
    (agent, id, lease)
}

async fn gpu_tool(
    store: &Store,
    id: SessionId,
    lease: &swarmy_core::Lease,
    tool: &str,
    args: serde_json::Value,
) -> swarmy_core::ToolResult {
    use swarmy_core::{Event, RequestId, ToolCallId, ToolCallRecord};
    let head = store.fetch_session(id).await.unwrap().unwrap().head_seq;
    let event = store
        .complete_gpu_tool(
            id,
            head,
            lease,
            RequestId::for_step(id, head),
            &ToolCallRecord {
                call_id: ToolCallId(tool.into()),
                tool: tool.into(),
                arguments: args,
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

fn gpu_output(result: swarmy_core::ToolResult) -> String {
    let swarmy_core::ToolResult::Completed { output, .. } = result else {
        panic!("{result:?}")
    };
    output
}

#[tokio::test]
async fn gpu_tool_request_sets_requirement_records_reason_and_requests_release() {
    use serde_json::json;
    use swarmy_core::GpuRequirement;
    let Some(test) = TestStore::memory() else {
        return;
    };
    let (agent, id, lease) = gpu_tool_session(&test.store, "gpu-request", true).await;
    let host = node(&test.store, 2).await.node_id;
    let placement = test.store.place(agent, host, future(60)).await.unwrap();
    test.store.claim_placement(&placement).await.unwrap();
    assert_eq!(
        gpu_output(
            gpu_tool(
                &test.store,
                id,
                &lease,
                "request_gpu",
                json!({"mode":"shared","reason":"  build the GPUI example  "})
            )
            .await
        ),
        swarmy_core::gpu_requested(swarmy_core::RequestedMode::Shared)
    );
    let record = test.store.get_agent(agent).await.unwrap().unwrap();
    assert_eq!(record.requirements.gpu, GpuRequirement::Shared);
    let cause = record.gpu_request.unwrap();
    assert_eq!(cause.reason, "build the GPUI example");
    assert_eq!(cause.session_id, id);
    assert!(
        test.store
            .placement_release_requested(&placement)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn gpu_tool_repeated_request_is_a_no_op() {
    use serde_json::json;
    let Some(test) = TestStore::memory() else {
        return;
    };
    let (agent, id, lease) = gpu_tool_session(&test.store, "gpu-repeat", true).await;
    gpu_tool(
        &test.store,
        id,
        &lease,
        "request_gpu",
        json!({"mode":"shared","reason":"first"}),
    )
    .await;
    let before = test.store.get_agent(agent).await.unwrap().unwrap();
    assert_eq!(
        gpu_output(
            gpu_tool(
                &test.store,
                id,
                &lease,
                "request_gpu",
                json!({"mode":"shared","reason":"second"})
            )
            .await
        ),
        swarmy_core::gpu_already(swarmy_core::GpuRequirement::Shared)
    );
    assert_eq!(test.store.get_agent(agent).await.unwrap().unwrap(), before);
}

#[tokio::test]
async fn gpu_tool_mode_change_records_the_new_reason() {
    use serde_json::json;
    use swarmy_core::{GpuRequirement, RequestedMode};
    let Some(test) = TestStore::memory() else {
        return;
    };
    let (agent, id, lease) = gpu_tool_session(&test.store, "gpu-mode-change", true).await;
    let cpu = node(&test.store, 2).await.node_id;
    let first = test.store.place(agent, cpu, future(60)).await.unwrap();
    test.store.claim_placement(&first).await.unwrap();
    assert_eq!(
        gpu_output(
            gpu_tool(
                &test.store,
                id,
                &lease,
                "request_gpu",
                json!({"mode":"shared","reason":"first"}),
            )
            .await
        ),
        swarmy_core::gpu_requested(RequestedMode::Shared)
    );
    test.store.release(&first).await.unwrap();
    let gpu = gpu_node(&test.store, 2, 1).await.node_id;
    let placement = test.store.place(agent, gpu, future(60)).await.unwrap();
    test.store.claim_placement(&placement).await.unwrap();
    assert_eq!(
        gpu_output(
            gpu_tool(
                &test.store,
                id,
                &lease,
                "request_gpu",
                json!({"mode":"dedicated","reason":"second"})
            )
            .await
        ),
        swarmy_core::gpu_requested(RequestedMode::Dedicated)
    );
    let record = test.store.get_agent(agent).await.unwrap().unwrap();
    assert_eq!(record.requirements.gpu, GpuRequirement::Dedicated);
    assert_eq!(record.gpu_request.unwrap().reason, "second");
    assert!(
        test.store
            .placement_release_requested(&placement)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn gpu_tool_release_resets_requirement_and_clears_reason() {
    use serde_json::json;
    let Some(test) = TestStore::memory() else {
        return;
    };
    let (agent, id, lease) = gpu_tool_session(&test.store, "gpu-release", true).await;
    gpu_tool(
        &test.store,
        id,
        &lease,
        "request_gpu",
        json!({"mode":"shared","reason":"test"}),
    )
    .await;
    assert_eq!(
        gpu_output(gpu_tool(&test.store, id, &lease, "release_gpu", json!({})).await),
        swarmy_core::GPU_RELEASED
    );
    let record = test.store.get_agent(agent).await.unwrap().unwrap();
    assert_eq!(record.requirements.gpu, swarmy_core::GpuRequirement::None);
    assert!(record.gpu_request.is_none());
    assert_eq!(
        gpu_output(gpu_tool(&test.store, id, &lease, "release_gpu", json!({})).await),
        swarmy_core::GPU_ALREADY_RELEASED
    );
}

#[tokio::test]
async fn gpu_tool_refused_without_the_flag() {
    use serde_json::json;
    let Some(test) = TestStore::memory() else {
        return;
    };
    let (agent, id, lease) = gpu_tool_session(&test.store, "gpu-refused", false).await;
    let host = node(&test.store, 2).await.node_id;
    let placement = test.store.place(agent, host, future(60)).await.unwrap();
    test.store.claim_placement(&placement).await.unwrap();
    for (tool, args) in [
        ("request_gpu", json!({"mode":"shared","reason":"x"})),
        ("release_gpu", json!({})),
    ] {
        assert_eq!(
            gpu_tool(&test.store, id, &lease, tool, args).await,
            swarmy_core::ToolResult::Error {
                error: swarmy_core::GPU_REQUEST_NOT_ALLOWED.into()
            }
        );
    }
    let record = test.store.get_agent(agent).await.unwrap().unwrap();
    assert_eq!(record.requirements.gpu, swarmy_core::GpuRequirement::None);
    assert!(record.gpu_request.is_none());
    assert!(
        !test
            .store
            .placement_release_requested(&placement)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn gpu_tool_invalid_arguments_change_nothing() {
    use serde_json::json;
    let Some(test) = TestStore::memory() else {
        return;
    };
    let (agent, id, lease) = gpu_tool_session(&test.store, "gpu-invalid", true).await;
    let before = test.store.get_agent(agent).await.unwrap().unwrap();
    for args in [
        json!({"mode":"none","reason":"x"}),
        json!({"mode":"shared","reason":" "}),
        json!({"mode":"shared","reason":"x".repeat(501)}),
    ] {
        assert!(matches!(
            gpu_tool(&test.store, id, &lease, "request_gpu", args).await,
            swarmy_core::ToolResult::Error { .. }
        ));
        assert_eq!(test.store.get_agent(agent).await.unwrap().unwrap(), before);
    }
}

#[tokio::test]
async fn gpu_tool_refused_for_ephemeral_sessions() {
    use serde_json::json;
    let Some(test) = TestStore::memory() else {
        return;
    };
    let id = SessionId::from_ulid(Ulid::generate());
    let image = swarmy_testkit::image(&test.store).await.to_owned();
    test.store
        .create_agent_session(
            id,
            None,
            Timestamp::now(),
            Some(swarmy_store::AgentSessionOptions {
                image: Some(&image),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    test.store.wake_session(id, Timestamp::now()).await.unwrap();
    let lease = test
        .store
        .claim_lease(id, owner(), future(60))
        .await
        .unwrap();
    assert_eq!(
        gpu_tool(
            &test.store,
            id,
            &lease,
            "request_gpu",
            json!({"mode":"shared","reason":"x"})
        )
        .await,
        swarmy_core::ToolResult::Error {
            error: swarmy_core::GPU_REQUEST_NEEDS_AGENT.into()
        }
    );
}

#[tokio::test]
async fn gpu_tool_stale_head_cannot_apply_twice() {
    use serde_json::json;
    use swarmy_core::{RequestId, ToolCallId, ToolCallRecord};
    let Some(test) = TestStore::memory() else {
        return;
    };
    let (_, id, lease) = gpu_tool_session(&test.store, "gpu-stale", true).await;
    let head = test
        .store
        .fetch_session(id)
        .await
        .unwrap()
        .unwrap()
        .head_seq;
    let call = ToolCallRecord {
        call_id: ToolCallId("gpu".into()),
        tool: "request_gpu".into(),
        arguments: json!({"mode":"shared","reason":"x"}),
        result: None,
    };
    test.store
        .complete_gpu_tool(id, head, &lease, RequestId::for_step(id, head), &call)
        .await
        .unwrap();
    assert!(
        test.store
            .complete_gpu_tool(id, head, &lease, RequestId::for_step(id, head), &call)
            .await
            .is_err()
    );
    assert_eq!(
        test.store
            .fetch_session(id)
            .await
            .unwrap()
            .unwrap()
            .head_seq,
        head + 1
    );
}

#[tokio::test]
async fn gpu_tool_operator_gpu_change_clears_the_reason() {
    use serde_json::json;
    use swarmy_core::{AgentSettings, GpuRequirement};
    let Some(test) = TestStore::memory() else {
        return;
    };
    let (agent, id, lease) = gpu_tool_session(&test.store, "gpu-operator", true).await;
    gpu_output(
        gpu_tool(
            &test.store,
            id,
            &lease,
            "request_gpu",
            json!({"mode":"shared","reason":"x"}),
        )
        .await,
    );
    let before = test
        .store
        .get_agent(agent)
        .await
        .unwrap()
        .unwrap()
        .gpu_request
        .expect("request_gpu records a reason");
    let still = test
        .store
        .set_agent_with_resets(
            agent,
            &AgentSettings {
                allow_gpu_request: Some(true),
                ..Default::default()
            },
            &[],
        )
        .await
        .unwrap();
    assert_eq!(still.gpu_request, Some(before));
    let changed = test
        .store
        .set_agent_with_resets(
            agent,
            &AgentSettings {
                gpu: Some(GpuRequirement::Dedicated),
                ..Default::default()
            },
            &[],
        )
        .await
        .unwrap();
    assert!(changed.gpu_request.is_none());
}

async fn ledger_gpu_node(test: &TestStore, group: &str) -> (swarmy_core::MachineId, NodeId) {
    use swarmy_core::MachineTransition;
    let machine = machines::machine(group);
    let mut host = gpu_node(&test.store, 5, 1).await;
    host.node_id = machine.machine_id.joined_node_id();
    test.store.put_node(&host).await.unwrap();
    test.store
        .create_machine(&machine, timestamp(1))
        .await
        .unwrap();
    test.store
        .transition_machine(
            machine.machine_id,
            &MachineTransition::Creating,
            timestamp(2),
        )
        .await
        .unwrap();
    test.store
        .transition_machine(
            machine.machine_id,
            &MachineTransition::Created {
                address: "address".into(),
            },
            timestamp(3),
        )
        .await
        .unwrap();
    test.store
        .transition_machine(
            machine.machine_id,
            &MachineTransition::Provisioned,
            timestamp(4),
        )
        .await
        .unwrap();
    (machine.machine_id, host.node_id)
}

#[tokio::test]
async fn gpu_placements_on_a_joined_machine_record_assignment_intervals() {
    use swarmy_core::{GpuRequirement, MachineTransition};
    let Some(test) = TestStore::memory() else {
        return;
    };
    let (machine, host) = ledger_gpu_node(&test, "gpu-assignment").await;
    let a = gpu_agent(&test.store, "gpu-assignment-a", GpuRequirement::Shared).await;
    let b = gpu_agent(&test.store, "gpu-assignment-b", GpuRequirement::Shared).await;
    let c = gpu_agent(&test.store, "gpu-assignment-c", GpuRequirement::None).await;
    let first = test.store.place(a, host, future(60)).await.unwrap();
    test.store.place(c, host, future(60)).await.unwrap();
    assert!(
        test.store
            .open_machine_assignments(machine)
            .await
            .unwrap()
            .is_empty()
    );
    test.store
        .transition_machine(
            machine,
            &MachineTransition::Joined { node_id: host },
            timestamp(5),
        )
        .await
        .unwrap();
    assert_eq!(
        test.store
            .open_machine_assignments(machine)
            .await
            .unwrap()
            .iter()
            .map(|row| row.agent)
            .collect::<Vec<_>>(),
        [a]
    );
    let second = test.store.place(b, host, future(60)).await.unwrap();
    let open = test.store.open_machine_assignments(machine).await.unwrap();
    assert_eq!(open.len(), 2);
    assert!(open.iter().any(|row| row.agent == a));
    assert!(open.iter().any(|row| row.agent == b));
    test.store.release(&second).await.unwrap();
    assert!(
        test.store
            .machine_assignments(machine)
            .await
            .unwrap()
            .iter()
            .any(|row| row.agent == b && row.to.is_some())
    );
    let old = expire(&test, &first).await;
    let other = gpu_node(&test.store, 2, 1).await.node_id;
    let moved = test.store.take_over(&old, other, future(60)).await.unwrap();
    assert!(
        test.store
            .machine_assignments(machine)
            .await
            .unwrap()
            .iter()
            .any(|row| row.agent == a && row.to.is_some())
    );
    assert!(
        test.store
            .open_machine_assignments(other.ledger_machine_id())
            .await
            .unwrap()
            .is_empty()
    );
    let old_from = test
        .store
        .machine_assignments(machine)
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.agent == a)
        .unwrap()
        .from;
    let expired = expire(&test, &moved).await;
    test.store
        .take_over(&expired, host, future(60))
        .await
        .unwrap();
    let open = test.store.open_machine_assignments(machine).await.unwrap();
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].agent, a);
    assert!(open[0].from > old_from);
}

#[tokio::test]
async fn joined_with_a_foreign_node_id_opens_no_intervals() {
    use swarmy_core::{GpuRequirement, MachineTransition};
    let Some(test) = TestStore::memory() else {
        return;
    };
    let (machine, derived_node) = ledger_gpu_node(&test, "gpu-foreign-join").await;
    let foreign_node = gpu_node(&test.store, 2, 1).await.node_id;
    assert_ne!(foreign_node, derived_node);
    let agent = gpu_agent(
        &test.store,
        "gpu-foreign-join-agent",
        GpuRequirement::Shared,
    )
    .await;
    test.store
        .place(agent, foreign_node, future(60))
        .await
        .unwrap();
    test.store
        .transition_machine(
            machine,
            &MachineTransition::Joined {
                node_id: foreign_node,
            },
            timestamp(5),
        )
        .await
        .unwrap();
    assert!(
        test.store
            .open_machine_assignments(machine)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn deleting_a_gpu_computer_closes_its_interval() {
    use swarmy_core::{GpuRequirement, MachineTransition};
    let Some(test) = TestStore::memory() else {
        return;
    };
    let (machine, host) = ledger_gpu_node(&test, "gpu-deleted").await;
    test.store
        .transition_machine(
            machine,
            &MachineTransition::Joined { node_id: host },
            timestamp(5),
        )
        .await
        .unwrap();
    let agent = gpu_agent(&test.store, "gpu-deleted-agent", GpuRequirement::Shared).await;
    test.store.place(agent, host, future(60)).await.unwrap();
    test.store.delete_agent(agent).await.unwrap();
    assert!(
        test.store
            .open_machine_assignments(machine)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        test.store
            .machine_assignments(machine)
            .await
            .unwrap()
            .iter()
            .any(|row| row.agent == agent && row.to.is_some())
    );
}
