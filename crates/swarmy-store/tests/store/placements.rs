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
            memory_bytes: 1024,
            disk_bytes: 1024,
            sandboxes: capacity,
        },
        last_heartbeat: Timestamp::now(),
        cached_images: vec![],
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

#[tokio::test]
async fn placement_lifecycle_fences_holders() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let a = node(&test.store, 1).await.node_id;
    let b = node(&test.store, 1).await.node_id;
    let agent = session().agent_id;
    let first = test.store.place(agent, a, future(60)).await.unwrap();
    assert_eq!(first.epoch, 1);
    assert_eq!(first.last_change_reason, PlacementChangeReason::Initial);
    assert_eq!(
        test.store.get_by_agent(agent).await.unwrap(),
        Some(first.clone())
    );
    assert!(matches!(
        test.store.place(agent, b, future(60)).await,
        Err(StoreError::PlacementExists)
    ));
    test.store.claim_placement(&first).await.unwrap();
    let renewed = test.store.renew(&first, future(120)).await.unwrap();
    assert_eq!(renewed.epoch, first.epoch);
    assert_eq!(renewed.last_changed_at, first.last_changed_at);
    assert_eq!(renewed.last_change_reason, first.last_change_reason);
    assert!(matches!(
        test.store.renew(&renewed, renewed.expires_at).await,
        Err(StoreError::LeaseMismatch)
    ));
    let impostor = PlacementRecord {
        node_id: b,
        ..renewed.clone()
    };
    assert!(matches!(
        test.store.renew(&impostor, future(180)).await,
        Err(StoreError::LeaseMismatch)
    ));
    assert!(matches!(
        test.store.release(&impostor).await,
        Err(StoreError::LeaseMismatch)
    ));
    assert!(matches!(
        test.store.take_over(&first, b, future(60)).await,
        Err(StoreError::LeaseMismatch)
    ));
    let expired = expire(&test, &renewed).await;
    assert!(matches!(
        test.store.renew(&expired, future(60)).await,
        Err(StoreError::LeaseMismatch)
    ));
    assert!(matches!(
        test.store.release(&expired).await,
        Err(StoreError::LeaseMismatch)
    ));
    let next = test.store.take_over(&expired, b, future(60)).await.unwrap();
    assert_eq!(next.epoch, 2);
    assert_eq!(next.last_change_reason, PlacementChangeReason::Failure);
    assert!(next.last_changed_at >= first.last_changed_at);
    assert!(
        test.store
            .list_by_node(a, None, 64)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        test.store.list_by_node(b, None, 64).await.unwrap(),
        vec![next.clone()]
    );
    assert!(matches!(
        test.store.renew(&first, future(180)).await,
        Err(StoreError::LeaseMismatch)
    ));
    assert!(matches!(
        test.store.release(&first).await,
        Err(StoreError::LeaseMismatch)
    ));
    assert!(matches!(
        test.store.take_over(&expired, a, future(60)).await,
        Err(StoreError::LeaseMismatch)
    ));
    test.cleanup().await;
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
        Err(StoreError::LeaseMismatch)
    ));
    test.store
        .set_placement_address(&next, address)
        .await
        .unwrap();
    test.store.release(&next).await.unwrap();
    assert_eq!(test.store.placement_address(&next).await.unwrap(), None);
    test.cleanup().await;
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
        Err(StoreError::LeaseMismatch)
    ));
    assert!(matches!(
        test.store.release(&next).await,
        Err(StoreError::LeaseMismatch)
    ));
    test.cleanup().await;
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
        Err(StoreError::NodeMissing)
    ));
    assert!(matches!(
        test.store.place(agent, b, future(60)).await,
        Err(StoreError::NodeAtCapacity)
    ));
    assert!(matches!(
        test.store.place(agent, a.node_id, timestamp(0)).await,
        Err(StoreError::LeaseMismatch)
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
        Err(StoreError::NodeAtCapacity)
    ));
    let expired = expire(&test, &first).await;
    assert!(matches!(
        test.store.take_over(&expired, b, future(60)).await,
        Err(StoreError::NodeAtCapacity)
    ));
    assert_eq!(
        test.store.get_by_agent(agent).await.unwrap(),
        Some(expired.clone())
    );
    assert!(matches!(
        test.store
            .place(session().agent_id, a.node_id, future(60))
            .await,
        Err(StoreError::NodeAtCapacity)
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
        Err(StoreError::InvalidState)
    ));
    test.cleanup().await;
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
            Err(error) => assert!(matches!(error, StoreError::LeaseMismatch)),
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
    test.cleanup().await;
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
        Some(StoreError::NodeAtCapacity)
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
        Err(StoreError::InvalidLimit)
    ));
    test.cleanup().await;
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
        Err(StoreError::LeaseMismatch)
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
        Err(StoreError::LeaseMismatch)
    ));
    let impostor = PlacementRecord {
        node_id: a,
        ..unstarted.clone()
    };
    assert!(matches!(
        test.store.claim_placement(&impostor).await,
        Err(StoreError::LeaseMismatch)
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
    test.cleanup().await;
}

#[tokio::test]
async fn legacy_placements_without_hosting_metadata_still_report_loss() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let node = node(&test.store, 1).await.node_id;
    let first = test
        .store
        .place(session().agent_id, node, future(60))
        .await
        .unwrap();
    // Legacy placement and dispatch records retain their original postcard schema.
    let key = test.root.pack(&(
        "placement_hosting",
        first.agent_id.as_ulid().to_bytes().as_slice(),
    ));
    test.db
        .run(|trx, _| {
            let key = &key;
            async move {
                trx.clear(key);
                Ok(())
            }
        })
        .await
        .unwrap();
    let expired = expire(&test, &first).await;
    let recovered = test
        .store
        .take_over(&expired, node, future(60))
        .await
        .unwrap();
    assert_eq!(recovered.last_change_reason, PlacementChangeReason::Failure);
    assert_eq!(
        test.store
            .placement_failure_estimate(&recovered)
            .await
            .unwrap(),
        None
    );
    test.cleanup().await;
}
