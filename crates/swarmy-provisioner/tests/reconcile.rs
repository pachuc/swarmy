#![deny(clippy::disallowed_methods)]
use foundationdb::{Database, tuple::Subspace};
use jiff::Timestamp;
use object_store::{ObjectStore, memory::InMemory, path::Path};
use std::{
    fs,
    sync::{Arc, Mutex},
};
use swarmy_config::Keyring;
use swarmy_core::{
    CredentialKind, CredentialRecord, CredentialScope, ElasticGroupDefinition, HourlyPrice,
    MachineState,
};
use swarmy_infra::Tofu;
use swarmy_provisioner::{Holder, Reconciler, Tick};
use swarmy_store::{MachineFilter, Store, blob::MemoryBlobStore};

struct Fixture {
    store: Store,
    reconciler: Reconciler,
    objects: Arc<InMemory>,
    temp: tempfile::TempDir,
    clock: Arc<Mutex<Timestamp>>,
    _guard: swarmy_testkit::StackGuard,
}
impl Fixture {
    async fn new(with_credentials: bool, max: u32) -> Option<Self> {
        let stack = swarmy_testkit::Stack::load("provisioner-reconcile")?;
        let db = Arc::new(Database::new(Some(&stack.cluster)).unwrap());
        let root = Subspace::all().subspace(&(stack.prefix.clone(),));
        let clock = Arc::new(Mutex::new(Timestamp::from_second(100_000).unwrap()));
        let store = Store::with_subspace(db, root, Arc::new(MemoryBlobStore::default()))
            .with_clock({
                let clock = clock.clone();
                move || *clock.lock().unwrap()
            });
        let temp = tempfile::tempdir().unwrap();
        let link = temp.path().join("fake-tofu");
        std::os::unix::fs::symlink(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/fake-tofu"),
            &link,
        )
        .unwrap();
        let credentials = store.credentials(Keyring::from_bytes([7; 32]));
        if with_credentials {
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
                        bookkeeping: Default::default(),
                        updated_at: store.now(),
                    },
                )
                .await
                .unwrap();
        }
        let objects = Arc::new(InMemory::new());
        let reconciler = Reconciler::new(
            store.clone(),
            Some(credentials),
            objects.clone(),
            temp.path().join("data"),
            temp.path().join("cache"),
        )
        .with_tofu(Tofu::from_binary(link, &temp.path().join("cache")));
        store
            .put_elastic_group(&ElasticGroupDefinition {
                swarm: "fleet".into(),
                group: "gpu".into(),
                module: "scaleway".into(),
                size: "L4-1-24G".into(),
                location: "fr-par-2".into(),
                sandboxes: 1,
                local_storage: "dir:/tmp".into(),
                max,
                hourly_price: HourlyPrice::parse("0.75", "EUR").unwrap(),
            })
            .await
            .unwrap();
        Some(Self {
            store,
            reconciler,
            objects,
            temp,
            clock,
            _guard: swarmy_testkit::StackGuard::new(&stack),
        })
    }
    async fn desired(&self, n: u32) {
        self.store
            .set_elastic_desired("gpu", n, "tasky 01TEST")
            .await
            .unwrap();
    }
    fn log(&self) -> String {
        fs::read_to_string(self.temp.path().join("tofu.log")).unwrap_or_default()
    }
    async fn records(&self) -> Vec<swarmy_core::MachineRecord> {
        self.store
            .list_machines(Some("gpu"), MachineFilter::All)
            .await
            .unwrap()
    }
}
#[tokio::test]
async fn scale_up_down_and_backup() {
    let Some(f) = Fixture::new(true, 3).await else {
        return;
    };
    f.desired(2).await;
    let mut holder = Holder::new();
    let Tick::Reconciled(outcomes) = f.reconciler.tick(&mut holder).await.unwrap() else {
        panic!("not leader")
    };
    assert_eq!(outcomes.len(), 1);
    assert_eq!(
        (outcomes[0].before, outcomes[0].after, outcomes[0].limited),
        (0, 2, false)
    );
    assert!(f.log().contains("apply machine_count=2"));
    assert!(f.log().contains("env SCW_ACCESS_KEY"));
    assert!(
        f.store
            .get_elastic_group("gpu")
            .await
            .unwrap()
            .unwrap()
            .settled()
    );
    let rows = f.records().await;
    assert_eq!(rows.len(), 2);
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row.state, MachineState::Provisioning);
        assert_eq!(row.created_at, Some(*f.clock.lock().unwrap()));
        assert_eq!(row.provisioned_at, None);
        assert_eq!(row.cause.reason, "tasky 01TEST");
        assert_eq!(
            row.address.as_deref(),
            Some(format!("203.0.113.{}", index + 11).as_str())
        );
    }
    let local = fs::read(f.temp.path().join("data/tofu/gpu/terraform.tfstate")).unwrap();
    assert_eq!(
        f.objects
            .get(&Path::from("tofu-state/gpu/terraform.tfstate"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        local
    );
    let count = f.log().matches("apply machine_count").count();
    f.reconciler.tick(&mut holder).await.unwrap();
    assert_eq!(f.log().matches("apply machine_count").count(), count);
    f.desired(1).await;
    f.reconciler.tick(&mut holder).await.unwrap();
    assert_eq!(f.records().await[1].state, MachineState::Destroyed);
    f.desired(0).await;
    f.reconciler.tick(&mut holder).await.unwrap();
    assert!(
        f.records()
            .await
            .iter()
            .all(|r| r.state == MachineState::Destroyed)
    );
    assert!(
        f.store
            .get_elastic_group("gpu")
            .await
            .unwrap()
            .unwrap()
            .settled()
    );
}
#[tokio::test]
async fn creation_limit_and_retry() {
    let Some(f) = Fixture::new(true, 6).await else {
        return;
    };
    f.desired(6).await;
    let mut holder = Holder::new();
    let Tick::Reconciled(outcomes) = f.reconciler.tick(&mut holder).await.unwrap() else {
        panic!("not leader")
    };
    assert_eq!((outcomes[0].after, outcomes[0].limited), (4, true));
    let before = f.log();
    f.reconciler.tick(&mut holder).await.unwrap();
    assert_eq!(f.log(), before);
    *f.clock.lock().unwrap() += std::time::Duration::from_secs(61 * 60);
    f.reconciler.tick(&mut holder).await.unwrap();
    assert!(f.log().contains("apply machine_count=6"));
    assert!(
        f.store
            .get_elastic_group("gpu")
            .await
            .unwrap()
            .unwrap()
            .settled()
    );
}
#[tokio::test]
async fn missing_credentials_do_not_start_tofu() {
    let Some(f) = Fixture::new(false, 2).await else {
        return;
    };
    f.desired(1).await;
    let mut holder = Holder::new();
    assert!(
        matches!(f.reconciler.tick(&mut holder).await.unwrap(), Tick::Reconciled(outcomes) if outcomes.is_empty())
    );
    let error = f
        .reconciler
        .reconcile(
            holder.lease().unwrap(),
            &f.store.get_elastic_group("gpu").await.unwrap().unwrap(),
        )
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("swarmy --swarm fleet auth set scaleway --from-env")
    );
    assert!(f.log().is_empty());
}
#[tokio::test]
async fn failed_apply_backoff_and_reuse_name() {
    let Some(f) = Fixture::new(true, 2).await else {
        return;
    };
    f.desired(1).await;
    fs::write(f.temp.path().join("fail-apply"), "").unwrap();
    let mut holder = Holder::new();
    f.reconciler.tick(&mut holder).await.unwrap();
    assert_eq!(f.records().await[0].state, MachineState::Failed);
    let before = f.log();
    f.reconciler.tick(&mut holder).await.unwrap();
    assert_eq!(before, f.log());
    fs::remove_file(f.temp.path().join("fail-apply")).unwrap();
    *f.clock.lock().unwrap() += std::time::Duration::from_secs(5 * 60);
    f.reconciler.tick(&mut holder).await.unwrap();
    let records = f.records().await;
    assert_eq!(records.len(), 2);
    assert!(
        records
            .iter()
            .any(|r| r.state == MachineState::Provisioning)
    );
}
#[tokio::test]
async fn partial_failure_records_observed_state() {
    let Some(f) = Fixture::new(true, 2).await else {
        return;
    };
    f.desired(2).await;
    fs::write(f.temp.path().join("fail-apply-partial"), "").unwrap();
    let mut holder = Holder::new();
    f.reconciler.tick(&mut holder).await.unwrap();
    let records = f.records().await;
    assert_eq!(records[0].state, MachineState::Provisioning);
    assert_eq!(records[1].state, MachineState::Failed);
    assert_eq!(
        f.store
            .get_elastic_group("gpu")
            .await
            .unwrap()
            .unwrap()
            .applied,
        1
    );
}

async fn seed(f: &Fixture, n: u32, state: MachineState) -> swarmy_core::MachineRecord {
    use swarmy_core::{BillingGranularity, MachineCause, MachineId, MachineTransition, NewMachine};
    let def = f
        .store
        .get_elastic_group("gpu")
        .await
        .unwrap()
        .unwrap()
        .definition;
    let record = f
        .store
        .create_machine(
            &NewMachine {
                machine_id: MachineId::from_ulid(ulid::Ulid::generate()),
                group: "gpu".into(),
                name: format!("fleet-gpu-{n}"),
                module: def.module,
                size: def.size,
                location: def.location,
                cause: MachineCause {
                    agent: None,
                    session: None,
                    reason: "seed".into(),
                },
                hourly_price: def.hourly_price,
                billing: BillingGranularity::default(),
            },
            f.store.now(),
        )
        .await
        .unwrap();
    if state != MachineState::Requested {
        f.store
            .transition_machine(
                record.machine_id,
                &MachineTransition::Creating,
                f.store.now(),
            )
            .await
            .unwrap();
    }
    if matches!(state, MachineState::Provisioning | MachineState::Draining) {
        f.store
            .transition_machine(
                record.machine_id,
                &MachineTransition::Created {
                    address: format!("203.0.113.{}", n + 10),
                },
                f.store.now(),
            )
            .await
            .unwrap();
    }
    if state == MachineState::Draining {
        f.store
            .transition_machine(
                record.machine_id,
                &MachineTransition::DrainRequested,
                f.store.now(),
            )
            .await
            .unwrap();
    }
    record
}
fn state(f: &Fixture, count: u32) {
    let dir = f.temp.path().join("data/tofu/gpu");
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("terraform.tfstate"),
        format!("{{\"count\":{count}}}"),
    )
    .unwrap();
}
#[tokio::test]
async fn restart_reuses_creating_record_without_state() {
    let Some(f) = Fixture::new(true, 3).await else {
        return;
    };
    f.desired(1).await;
    let old = seed(&f, 1, MachineState::Creating).await;
    f.reconciler.tick(&mut Holder::new()).await.unwrap();
    let records = f.records().await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].machine_id, old.machine_id);
    assert_eq!(records[0].state, MachineState::Provisioning);
}
#[tokio::test]
async fn restart_completes_creating_from_existing_state_without_apply() {
    let Some(f) = Fixture::new(true, 3).await else {
        return;
    };
    f.desired(1).await;
    let old = seed(&f, 1, MachineState::Creating).await;
    state(&f, 1);
    f.reconciler.tick(&mut Holder::new()).await.unwrap();
    assert!(!f.log().contains("apply machine_count"));
    assert_eq!(f.records().await[0].machine_id, old.machine_id);
    assert_eq!(f.records().await[0].state, MachineState::Provisioning);
    assert!(
        f.store
            .get_elastic_group("gpu")
            .await
            .unwrap()
            .unwrap()
            .settled()
    );
}
#[tokio::test]
async fn restart_finishes_draining_without_apply() {
    let Some(f) = Fixture::new(true, 3).await else {
        return;
    };
    f.desired(1).await;
    seed(&f, 1, MachineState::Draining).await;
    let mut holder = Holder::new();
    // Record that the last apply held one machine before the zero request.
    let lease = f
        .store
        .acquire_provisioner_lease(
            swarmy_core::LeaseOwnerId::from_ulid(5u128.into()),
            std::time::Duration::from_secs(900),
        )
        .await
        .unwrap();
    f.store
        .record_elastic_reconcile(&lease, "gpu", 1, 1, true)
        .await
        .unwrap();
    f.store.release_provisioner_lease(&lease).await.unwrap();
    f.desired(0).await;
    state(&f, 0);
    f.reconciler.tick(&mut holder).await.unwrap();
    assert_eq!(f.records().await[0].state, MachineState::Destroyed);
    assert!(!f.log().contains("apply machine_count"));
}
#[tokio::test]
async fn restart_adopts_state_without_ledger() {
    let Some(f) = Fixture::new(true, 3).await else {
        return;
    };
    f.desired(1).await;
    state(&f, 1);
    f.reconciler.tick(&mut Holder::new()).await.unwrap();
    let rows = f.records().await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].state, MachineState::Provisioning);
    assert!(
        rows[0]
            .cause
            .reason
            .starts_with("adopted from OpenTofu state")
    );
}
#[tokio::test]
async fn missing_state_orphans_machine_and_blocks_growth() {
    let Some(f) = Fixture::new(true, 3).await else {
        return;
    };
    f.desired(1).await;
    seed(&f, 1, MachineState::Provisioning).await;
    let lease = f
        .store
        .acquire_provisioner_lease(
            swarmy_core::LeaseOwnerId::from_ulid(6u128.into()),
            std::time::Duration::from_secs(900),
        )
        .await
        .unwrap();
    let error = f
        .reconciler
        .reconcile(
            &lease,
            &f.store.get_elastic_group("gpu").await.unwrap().unwrap(),
        )
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("remove it before the group can grow")
    );
    assert_eq!(f.records().await[0].state, MachineState::Orphaned);
}
#[tokio::test]
async fn draining_finishes_before_growth() {
    let Some(f) = Fixture::new(true, 3).await else {
        return;
    };
    f.desired(3).await;
    seed(&f, 1, MachineState::Provisioning).await;
    seed(&f, 2, MachineState::Draining).await;
    state(&f, 2);
    let mut holder = Holder::new();
    f.reconciler.tick(&mut holder).await.unwrap();
    assert!(f.log().contains("apply machine_count=1"));
    assert_eq!(f.records().await[1].state, MachineState::Destroyed);
    f.reconciler.tick(&mut holder).await.unwrap();
    assert!(f.log().contains("apply machine_count=3"));
    assert_eq!(f.records().await.len(), 4);
}
