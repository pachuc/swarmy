#![deny(clippy::disallowed_methods)]
use jiff::Timestamp;
use std::sync::{Arc, Mutex};
use swarmy_core::{ElasticGroupDefinition, HourlyPrice, LeaseOwnerId};
use swarmy_store::{FenceError, StoreError, blob::MemoryBlobStore};

#[tokio::test]
async fn lease_window_and_reconcile_are_fenced() {
    let Some(stack) = swarmy_testkit::Stack::load("provisioner") else {
        return;
    };
    let (store, _guard) = stack.open_store(Arc::new(MemoryBlobStore::default())).await;
    let clock = Arc::new(Mutex::new(Timestamp::from_second(100_000).unwrap()));
    let store = store.with_clock({
        let clock = clock.clone();
        move || *clock.lock().unwrap()
    });
    let first = LeaseOwnerId::from_ulid(1u128.into());
    let second = LeaseOwnerId::from_ulid(2u128.into());
    let ttl = std::time::Duration::from_mins(15);
    let old = store.acquire_provisioner_lease(first, ttl).await.unwrap();
    assert!(matches!(
        store.acquire_provisioner_lease(second, ttl).await,
        Err(StoreError::Fence(FenceError::ProvisionerLeaseMismatch))
    ));
    let renewed = store.renew_provisioner_lease(&old, ttl).await.err();
    assert!(matches!(
        renewed,
        Some(StoreError::Fence(FenceError::ProvisionerLeaseMismatch))
    ));
    *clock.lock().unwrap() += std::time::Duration::from_secs(1);
    let live = store.renew_provisioner_lease(&old, ttl).await.unwrap();
    let group = ElasticGroupDefinition {
        swarm: "fleet".into(),
        group: "gpu".into(),
        module: "scaleway".into(),
        size: "L4-1-24G".into(),
        location: "fr-par-2".into(),
        sandboxes: 1,
        local_storage: "dir:/tmp".into(),
        max: 6,
        hourly_price: HourlyPrice::parse("0.75", "EUR").unwrap(),
        primary_address: "192.0.2.1".parse().unwrap(),
        service_user: "swarmy".into(),
        autoscale: false,
    };
    store.put_elastic_group(&group).await.unwrap();
    store
        .set_elastic_desired("gpu", 2, &swarmy_core::MachineCause::from_reason("test"))
        .await
        .unwrap();
    let grant = store.reserve_elastic_creations(&live, 6).await.unwrap();
    assert_eq!((grant.granted, grant.used), (4, 4));
    assert_eq!(
        store
            .reserve_elastic_creations(&live, 1)
            .await
            .unwrap()
            .granted,
        0
    );
    assert!(grant.next_free_at.is_some());
    assert!(matches!(
        store.reserve_elastic_creations(&old, 1).await,
        Err(StoreError::Fence(FenceError::ProvisionerLeaseMismatch))
    ));
    assert_eq!(store.elastic_creation_slots().await.unwrap().used, 4);
    let unchanged = store.get_elastic_group("gpu").await.unwrap().unwrap();
    assert_eq!((unchanged.applied, unchanged.reconciled_generation), (0, 0));
    assert!(matches!(
        store
            .record_elastic_reconcile(&old, "gpu", 1, 1, true)
            .await,
        Err(StoreError::Fence(FenceError::ProvisionerLeaseMismatch))
    ));
    assert_eq!(store.elastic_creation_slots().await.unwrap().used, 4);
    let unchanged = store.get_elastic_group("gpu").await.unwrap().unwrap();
    assert_eq!((unchanged.applied, unchanged.reconciled_generation), (0, 0));
    let record = store
        .record_elastic_reconcile(&live, "gpu", 1, 1, false)
        .await
        .unwrap();
    assert_eq!((record.applied, record.reconciled_generation), (1, 0));
    let record = store
        .record_elastic_reconcile(&live, "gpu", 2, 1, true)
        .await
        .unwrap();
    assert_eq!(record.reconciled_generation, 1);
    let record = store
        .record_elastic_reconcile(&live, "gpu", 0, 0, true)
        .await
        .unwrap();
    assert_eq!(record.reconciled_generation, 1);
    *clock.lock().unwrap() += std::time::Duration::from_secs(3601);
    assert!(matches!(
        store.renew_provisioner_lease(&live, ttl).await,
        Err(StoreError::Fence(FenceError::ProvisionerLeaseMismatch))
    ));
    let new = store.acquire_provisioner_lease(second, ttl).await.unwrap();
    assert!(new.seq > live.seq);
    let grant = store.reserve_elastic_creations(&new, 4).await.unwrap();
    assert_eq!(grant.granted, 4);
    store.release_provisioner_lease(&live).await.unwrap();
    assert!(matches!(
        store.acquire_provisioner_lease(first, ttl).await,
        Err(StoreError::Fence(FenceError::ProvisionerLeaseMismatch))
    ));
    store.release_provisioner_lease(&new).await.unwrap();
    store.acquire_provisioner_lease(first, ttl).await.unwrap();
}
