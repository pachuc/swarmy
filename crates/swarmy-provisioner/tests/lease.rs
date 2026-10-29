#![deny(clippy::disallowed_methods)]
use foundationdb::{Database, tuple::Subspace};
use jiff::Timestamp;
use object_store::memory::InMemory;
use std::sync::{Arc, Mutex};
use swarmy_config::Keyring;
use swarmy_core::{
    CredentialKind, CredentialRecord, CredentialScope, ElasticGroupDefinition, HourlyPrice,
};
use swarmy_infra::Tofu;
use swarmy_provisioner::{Holder, Reconciler, Tick, role::LEASE_TTL};
use swarmy_store::{Store, blob::MemoryBlobStore};
#[tokio::test]
async fn only_the_current_holder_applies() {
    let Some(stack) = swarmy_testkit::Stack::load("provisioner-lease") else {
        return;
    };
    let db = Arc::new(Database::new(Some(&stack.cluster)).unwrap());
    let root = Subspace::all().subspace(&(stack.prefix.clone(),));
    let clock = Arc::new(Mutex::new(Timestamp::from_second(100_000).unwrap()));
    let store = Store::with_subspace(db, root, Arc::new(MemoryBlobStore::default())).with_clock({
        let clock = clock.clone();
        move || *clock.lock().unwrap()
    });
    let _guard = swarmy_testkit::StackGuard::new(&stack);
    let credentials = store.credentials(Keyring::from_bytes([7; 32]));
    let env = [
        ("SCW_ACCESS_KEY".into(), "test-access".into()),
        ("SCW_SECRET_KEY".into(), "test-secret".into()),
        (
            "SCW_DEFAULT_PROJECT_ID".into(),
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".into(),
        ),
    ]
    .into();
    credentials
        .put_entry(
            CredentialScope::Cluster,
            "scaleway",
            "default",
            &CredentialRecord {
                kind: CredentialKind::MachineProvider { env },
                bookkeeping: swarmy_core::CredentialBookkeeping::default(),
                updated_at: store.now(),
            },
        )
        .await
        .unwrap();
    store
        .put_elastic_group(&ElasticGroupDefinition {
            swarm: "fleet".into(),
            group: "gpu".into(),
            module: "scaleway".into(),
            size: "L4-1-24G".into(),
            location: "fr-par-2".into(),
            sandboxes: 1,
            local_storage: "dir:/tmp".into(),
            max: 2,
            hourly_price: HourlyPrice::parse("0.75", "EUR").unwrap(),
        })
        .await
        .unwrap();
    store.set_elastic_desired("gpu", 1, "test").await.unwrap();
    let temp = tempfile::tempdir().unwrap();
    let link = temp.path().join("fake-tofu");
    std::os::unix::fs::symlink(
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/fake-tofu"),
        &link,
    )
    .unwrap();
    let tofu = Tofu::from_binary(link, &temp.path().join("cache"));
    let make = || {
        Reconciler::new(
            store.clone(),
            Some(credentials.clone()),
            Arc::new(InMemory::new()),
            temp.path().join("data"),
            temp.path().join("cache"),
        )
        .with_tofu(tofu.clone())
    };
    let first = make();
    let second = make();
    let mut one = Holder::new();
    let mut two = Holder::new();
    assert!(
        matches!(first.tick(&mut one).await.unwrap(), Tick::Reconciled(outcomes) if outcomes.len() == 1)
    );
    assert!(matches!(
        second.tick(&mut two).await.unwrap(),
        Tick::Standby
    ));
    assert_eq!(
        std::fs::read_to_string(temp.path().join("tofu.log"))
            .unwrap()
            .matches("apply machine_count")
            .count(),
        1
    );
    *clock.lock().unwrap() += LEASE_TTL + std::time::Duration::from_secs(1);
    assert!(matches!(
        second.tick(&mut two).await.unwrap(),
        Tick::Reconciled(_)
    ));
    assert!(matches!(first.tick(&mut one).await.unwrap(), Tick::Standby));
}
