#![deny(clippy::disallowed_methods)]
use std::sync::Arc;
use swarmy_core::{ElasticGroupDefinition, HourlyPrice};
use swarmy_store::{DomainError, StoreError, blob::MemoryBlobStore};

fn definition(group: &str) -> ElasticGroupDefinition {
    ElasticGroupDefinition {
        swarm: "fleet".into(),
        group: group.into(),
        module: "hetzner-cloud".into(),
        size: "cpx41".into(),
        location: "fsn1".into(),
        sandboxes: 64,
        local_storage: "dir:/srv/swarmy-local".into(),
        max: 2,
        hourly_price: HourlyPrice::parse("0.75", "EUR").unwrap(),
        primary_address: "192.0.2.1".parse().unwrap(),
        service_user: "swarmy".into(),
        autoscale: false,
    }
}
#[tokio::test]
async fn register_and_set_desired() {
    let Some(stack) = swarmy_testkit::Stack::load("elastic") else {
        return;
    };
    let (store, _guard) = stack.open_store(Arc::new(MemoryBlobStore::default())).await;
    assert!(store.get_elastic_group("a").await.unwrap().is_none());
    let first = store.put_elastic_group(&definition("b")).await.unwrap();
    assert_eq!(
        (
            first.desired,
            first.applied,
            first.generation,
            first.reconciled_generation
        ),
        (0, 0, 0, 0)
    );
    store.put_elastic_group(&definition("a")).await.unwrap();
    assert_eq!(
        store
            .list_elastic_groups()
            .await
            .unwrap()
            .iter()
            .map(|r| r.definition.group.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    let updated = store
        .set_elastic_desired("b", 1, &swarmy_core::MachineCause::from_reason("manual"))
        .await
        .unwrap();
    assert_eq!(updated.generation, 1);
    assert!(!updated.settled());
    assert_eq!(
        store
            .set_elastic_desired("b", 1, &swarmy_core::MachineCause::from_reason("again"))
            .await
            .unwrap(),
        updated
    );
    assert!(matches!(
        store
            .set_elastic_desired("b", 3, &swarmy_core::MachineCause::from_reason("manual"))
            .await,
        Err(StoreError::Domain(DomainError::DesiredAboveMax { .. }))
    ));
    assert!(matches!(
        store
            .set_elastic_desired(
                "missing",
                1,
                &swarmy_core::MachineCause::from_reason("manual")
            )
            .await,
        Err(StoreError::Domain(DomainError::ElasticGroupMissing { .. }))
    ));
    let mut changed = definition("b");
    changed.size = "different".into();
    assert!(matches!(
        store.put_elastic_group(&changed).await,
        Err(StoreError::Domain(DomainError::ElasticGroupBusy { .. }))
    ));
    changed = definition("b");
    changed.max = 0;
    assert!(matches!(
        store.put_elastic_group(&changed).await,
        Err(StoreError::Domain(DomainError::InvalidElasticGroup(_)))
    ));
    changed.max = 1;
    changed.hourly_price = HourlyPrice::parse("0.50", "EUR").unwrap();
    assert_eq!(
        store
            .put_elastic_group(&changed)
            .await
            .unwrap()
            .definition
            .hourly_price
            .amount(),
        "0.5"
    );
    assert!(matches!(
        store
            .set_elastic_desired("b", 2, &swarmy_core::MachineCause::from_reason("manual"))
            .await,
        Err(StoreError::Domain(DomainError::DesiredAboveMax { .. }))
    ));
    store.put_elastic_group(&definition("b")).await.unwrap();
    store
        .set_elastic_desired("b", 2, &swarmy_core::MachineCause::from_reason("manual"))
        .await
        .unwrap();
    changed.max = 1;
    assert!(matches!(
        store.put_elastic_group(&changed).await,
        Err(StoreError::Domain(DomainError::DesiredAboveMax { .. }))
    ));
}

#[tokio::test]
async fn changing_identity_waits_for_reconciliation_even_when_empty() {
    let Some(stack) = swarmy_testkit::Stack::load("elastic") else {
        return;
    };
    let (store, _guard) = stack.open_store(Arc::new(MemoryBlobStore::default())).await;
    store.put_elastic_group(&definition("race")).await.unwrap();
    store
        .set_elastic_desired("race", 1, &swarmy_core::MachineCause::from_reason("manual"))
        .await
        .unwrap();
    let unsettled = store
        .set_elastic_desired("race", 0, &swarmy_core::MachineCause::from_reason("manual"))
        .await
        .unwrap();
    assert_eq!((unsettled.desired, unsettled.applied), (0, 0));
    assert!(!unsettled.settled());
    let mut changed_race = definition("race");
    changed_race.size = "different".into();
    assert!(matches!(
        store.put_elastic_group(&changed_race).await,
        Err(StoreError::Domain(DomainError::ElasticGroupBusy { .. }))
    ));
    changed_race = definition("race");
    changed_race.hourly_price = HourlyPrice::parse("0.50", "EUR").unwrap();
    assert_eq!(
        store
            .put_elastic_group(&changed_race)
            .await
            .unwrap()
            .definition
            .hourly_price
            .amount(),
        "0.5"
    );
}
