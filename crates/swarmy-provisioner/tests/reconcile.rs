#![deny(clippy::disallowed_methods)]
use foundationdb::{Database, tuple::Subspace};
use jiff::Timestamp;
use object_store::{ObjectStore, memory::InMemory, path::Path};
use std::time::Duration;
use std::{
    fs,
    sync::{Arc, Mutex},
};
use swarmy_cloud::{
    provision::Machine,
    ssh::{Drain, LocalPrimary},
};
use swarmy_config::Keyring;
use swarmy_core::{
    AgentId, CredentialKind, CredentialRecord, CredentialScope, ElasticGroupDefinition,
    GpuRequirement, HourlyPrice, LeaseOwnerId, MachineState, MachineTransition, NodeCapacity,
    NodeId, NodeRecord, NodeRole, SessionId, SessionRecord, SessionState,
};
use swarmy_infra::Tofu;
use swarmy_provisioner::{Holder, Nodes, Reconciler, Tick};
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
        Self::build(with_credentials, max, None, false).await
    }
    async fn with_nodes(max: u32, nodes: Arc<FakeNodes>) -> Option<Self> {
        Self::build(true, max, Some(nodes), false).await
    }
    async fn autoscaled(max: u32, nodes: Arc<FakeNodes>) -> Option<Self> {
        *nodes.card.lock().unwrap() = true;
        Self::build(true, max, Some(nodes), true).await
    }
    async fn build(
        with_credentials: bool,
        max: u32,
        nodes: Option<Arc<FakeNodes>>,
        autoscale: bool,
    ) -> Option<Self> {
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
                        bookkeeping: swarmy_core::CredentialBookkeeping::default(),
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
        let reconciler = if let Some(nodes) = nodes {
            nodes.set_store(store.clone(), temp.path().join("tofu.log"));
            reconciler.with_nodes(nodes, None)
        } else {
            reconciler
        };
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
                primary_address: "192.0.2.1".parse().unwrap(),
                service_user: "swarmy".into(),
                autoscale,
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
            .set_elastic_desired(
                "gpu",
                n,
                &swarmy_core::MachineCause::from_reason("tasky 01TEST"),
            )
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
#[derive(Default)]
struct FakeNodes {
    store: Mutex<Option<Store>>,
    log: Mutex<Option<std::path::PathBuf>>,
    calls: Mutex<Vec<String>>,
    join_error: Mutex<Option<String>>,
    drain_fails: Mutex<bool>,
    card: Mutex<bool>,
}
impl FakeNodes {
    fn set_store(&self, store: Store, log: std::path::PathBuf) {
        *self.store.lock().unwrap() = Some(store);
        *self.log.lock().unwrap() = Some(log);
    }
    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
    fn applies(&self) -> usize {
        let path = self.log.lock().unwrap().clone().unwrap();
        fs::read_to_string(path)
            .unwrap_or_default()
            .matches("apply machine_count")
            .count()
    }
}
fn sandbox_node(id: NodeId, now: Timestamp) -> NodeRecord {
    NodeRecord {
        node_id: id,
        roles: vec![NodeRole::Sandbox],
        capacity: NodeCapacity {
            cpu_millis: 1000,
            memory_bytes: 1024,
            disk_bytes: 1024,
            sandboxes: 1,
        },
        last_heartbeat: now,
        cached_images: vec![],
        gpu: None,
    }
}
#[async_trait::async_trait]
impl Nodes for FakeNodes {
    async fn join(&self, node: &Machine, _primary: &LocalPrimary) -> anyhow::Result<()> {
        self.calls.lock().unwrap().push(format!(
            "join {} {} {} sandboxes={} storage={} node={}",
            node.name,
            node.public_ip,
            node.ssh_user,
            node.sandboxes,
            node.local_storage,
            node.node_id.unwrap()
        ));
        if let Some(error) = self.join_error.lock().unwrap().clone() {
            anyhow::bail!("{error}")
        }
        let store = self.store.lock().unwrap().clone().unwrap();
        let mut record = sandbox_node(node.node_id.unwrap(), store.now());
        if *self.card.lock().unwrap() {
            record.capacity.sandboxes = node.sandboxes;
            record.gpu = Some(swarmy_core::GpuCards {
                model: "NVIDIA L4".into(),
                memory_mib: 23034,
                count: 1,
            });
            record.capacity.memory_bytes = 16 * 1024 * 1024 * 1024;
        }
        store.put_node(&record).await?;
        Ok(())
    }
    async fn drain(&self, node: &Machine, timeout: Duration) -> anyhow::Result<Drain> {
        self.calls.lock().unwrap().push(format!(
            "drain {} {} applies={}",
            node.name,
            timeout.as_secs(),
            self.applies()
        ));
        if *self.drain_fails.lock().unwrap() {
            anyhow::bail!("unreachable")
        }
        Ok(Drain::default())
    }
    async fn revoke(&self, node: &Machine, _primary: &LocalPrimary) -> anyhow::Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("revoke {} applies={}", node.name, self.applies()));
        Ok(())
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
    assert_eq!(f.log().matches("apply machine_count=2").count(), 1);
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
        assert_eq!(row.cause.agent, None);
        assert_eq!(row.cause.session, None);
        assert_eq!(row.hourly_price, HourlyPrice::parse("0.75", "EUR").unwrap());
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
    assert_eq!(f.log().matches("apply machine_count=1").count(), 1);
    let rows = f.records().await;
    assert_eq!(rows[0].state, MachineState::Provisioning);
    assert_eq!(rows[1].state, MachineState::Destroyed);
    assert!(rows[1].drain_requested_at.is_some());
    assert!(rows[1].destroyed_at.is_some());
    f.desired(0).await;
    f.reconciler.tick(&mut holder).await.unwrap();
    assert!(
        f.records()
            .await
            .iter()
            .all(|r| r.state == MachineState::Destroyed)
    );
    let group = f.store.get_elastic_group("gpu").await.unwrap().unwrap();
    assert!(group.settled());
    assert_eq!(group.applied, 0);
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
    assert_eq!(f.log().matches("apply machine_count=4").count(), 1);
    assert!(
        !f.store
            .get_elastic_group("gpu")
            .await
            .unwrap()
            .unwrap()
            .settled()
    );
    let before = f.log();
    f.reconciler.tick(&mut holder).await.unwrap();
    assert_eq!(f.log(), before);
    *f.clock.lock().unwrap() += Duration::from_mins(61);
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
    let failed = &f.records().await[0];
    assert_eq!(failed.state, MachineState::Failed);
    assert!(!failed.is_active());
    let failed_id = failed.machine_id;
    let group = f.store.get_elastic_group("gpu").await.unwrap().unwrap();
    assert_eq!(group.applied, 0);
    assert!(!group.settled());
    let before = f.log();
    f.reconciler.tick(&mut holder).await.unwrap();
    assert_eq!(before, f.log());
    fs::remove_file(f.temp.path().join("fail-apply")).unwrap();
    *f.clock.lock().unwrap() += Duration::from_mins(5);
    f.reconciler.tick(&mut holder).await.unwrap();
    let records = f.records().await;
    assert_eq!(records.len(), 2);
    let replacement = records
        .iter()
        .find(|r| r.state == MachineState::Provisioning)
        .unwrap();
    assert_eq!(replacement.name, "fleet-gpu-1");
    assert_ne!(replacement.machine_id, failed_id);
}
#[tokio::test]
async fn output_error_after_apply_is_not_settled() {
    let Some(f) = Fixture::new(true, 2).await else {
        return;
    };
    let mut holder = Holder::new();
    f.reconciler.tick(&mut holder).await.unwrap();
    f.desired(1).await;
    fs::write(f.temp.path().join("fail-output"), "").unwrap();
    let error = f
        .reconciler
        .reconcile(
            holder.lease().unwrap(),
            &f.store.get_elastic_group("gpu").await.unwrap().unwrap(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("reading OpenTofu outputs"));
    assert!(
        !f.store
            .get_elastic_group("gpu")
            .await
            .unwrap()
            .unwrap()
            .settled()
    );
    assert_eq!(f.records().await[0].state, MachineState::Creating);
    fs::remove_file(f.temp.path().join("fail-output")).unwrap();
    f.reconciler.tick(&mut holder).await.unwrap();
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
async fn partial_failure_records_observed_state() {
    let Some(f) = Fixture::new(true, 2).await else {
        return;
    };
    let mut holder = Holder::new();
    f.reconciler.tick(&mut holder).await.unwrap();
    f.desired(2).await;
    fs::write(f.temp.path().join("fail-apply-partial"), "").unwrap();
    let error = f
        .reconciler
        .reconcile(
            holder.lease().unwrap(),
            &f.store.get_elastic_group("gpu").await.unwrap().unwrap(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("OpenTofu apply"));
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
    let status = std::process::Command::new(f.temp.path().join("fake-tofu"))
        .args(["write-state", "fleet", "gpu", &count.to_string()])
        .current_dir(&dir)
        .status()
        .unwrap();
    assert!(status.success());
}
#[tokio::test]
async fn a_failed_first_apply_leaves_resources_that_scale_to_zero_destroys() {
    let Some(f) = Fixture::new(true, 2).await else {
        return;
    };
    let state_path = f.temp.path().join("data/tofu/gpu/terraform.tfstate");
    f.desired(1).await;
    fs::write(f.temp.path().join("fail-apply-leak"), "").unwrap();
    let mut holder = Holder::new();
    f.reconciler.tick(&mut holder).await.unwrap();
    assert!(
        fs::read_to_string(&state_path)
            .unwrap()
            .contains("scaleway_instance_ip")
    );
    assert!(
        f.records()
            .await
            .iter()
            .all(|r| r.state == MachineState::Failed)
    );
    // The next attempt reads that state as zero machines and reaches the apply,
    // instead of failing on the missing machines output.
    let error = f
        .reconciler
        .reconcile(
            holder.lease().unwrap(),
            &f.store.get_elastic_group("gpu").await.unwrap().unwrap(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("OpenTofu apply"), "{error:#}");
    fs::remove_file(f.temp.path().join("fail-apply-leak")).unwrap();
    f.desired(0).await;
    f.reconciler.tick(&mut holder).await.unwrap();
    assert_eq!(f.log().matches("call gpu destroy").count(), 1);
    let state: serde_json::Value = serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
    assert_eq!(state["resources"], serde_json::json!([]));
    let group = f.store.get_elastic_group("gpu").await.unwrap().unwrap();
    assert!(group.settled());
    assert_eq!(group.applied, 0);
    assert!(f.records().await.iter().all(|r| !r.is_active()));
    f.reconciler.tick(&mut holder).await.unwrap();
    assert_eq!(f.log().matches("call gpu destroy").count(), 1);
}

#[tokio::test]
async fn an_emptied_state_settles_at_zero_without_reading_outputs() {
    let Some(f) = Fixture::new(true, 2).await else {
        return;
    };
    let dir = f.temp.path().join("data/tofu/gpu");
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("terraform.tfstate"),
        r#"{"version":4,"resources":[],"outputs":{}}"#,
    )
    .unwrap();
    f.desired(1).await;
    settled(&f, 1).await;
    f.desired(0).await;
    f.reconciler.tick(&mut Holder::new()).await.unwrap();
    let group = f.store.get_elastic_group("gpu").await.unwrap().unwrap();
    assert!(group.settled());
    assert_eq!(group.applied, 0);
    let log = f.log();
    assert!(!log.contains("call gpu output"), "{log}");
    assert!(!log.contains("call gpu destroy"), "{log}");
    assert!(!log.contains("apply machine_count"), "{log}");
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
            LeaseOwnerId::from_ulid(5u128.into()),
            Duration::from_mins(15),
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
async fn adopted_machines_keep_the_desired_cause() {
    let Some(f) = Fixture::new(true, 3).await else {
        return;
    };
    let cause = swarmy_core::MachineCause {
        agent: Some(AgentId::from_ulid(ulid::Ulid::from(1_u128))),
        session: Some(SessionId::from_ulid(ulid::Ulid::from(2_u128))),
        reason: "tasky 01X: y".into(),
    };
    f.store.set_elastic_desired("gpu", 1, &cause).await.unwrap();
    state(&f, 1);
    f.reconciler.tick(&mut Holder::new()).await.unwrap();
    let rows = f.records().await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].cause.agent, cause.agent);
    assert_eq!(rows[0].cause.session, cause.session);
    assert_eq!(
        rows[0].cause.reason,
        "adopted from OpenTofu state; tasky 01X: y"
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
            LeaseOwnerId::from_ulid(6u128.into()),
            Duration::from_mins(15),
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

#[tokio::test]
async fn new_generation_bypasses_failure_backoff() {
    let Some(f) = Fixture::new(true, 3).await else {
        return;
    };
    f.desired(1).await;
    fs::write(f.temp.path().join("fail-apply"), "").unwrap();
    let mut holder = Holder::new();
    f.reconciler.tick(&mut holder).await.unwrap();
    let before = f.log();
    fs::remove_file(f.temp.path().join("fail-apply")).unwrap();
    f.reconciler.tick(&mut holder).await.unwrap();
    assert_eq!(before, f.log());
    f.desired(2).await;
    f.reconciler.tick(&mut holder).await.unwrap();
    assert!(f.log().contains("apply machine_count=2"));
    assert_eq!(f.records().await.len(), 3);
}

fn test_session() -> SessionRecord {
    SessionRecord {
        interrupt_requested: false,
        session_id: SessionId::from_ulid(ulid::Ulid::generate()),
        agent_id: AgentId::from_ulid(ulid::Ulid::generate()),
        state: SessionState::Runnable,
        head_seq: 0,
        snapshot_ref: None,
        inference: swarmy_core::InferenceSelection::default(),
        route: None,
        route_step: 0,
        kind: swarmy_core::SessionKind::Ephemeral,
        computer_deleted: false,
        plan: Vec::new(),
    }
}
#[tokio::test]
async fn new_machine_joins_and_wakes_capacity_waits() {
    let nodes = Arc::new(FakeNodes::default());
    let Some(f) = Fixture::with_nodes(1, nodes.clone()).await else {
        return;
    };
    let session = test_session();
    f.store
        .create_session(
            &session,
            f.store.now(),
            swarmy_testkit::image(&f.store).await,
        )
        .await
        .unwrap();
    let lease = f
        .store
        .claim_lease(
            session.session_id,
            LeaseOwnerId::from_ulid(ulid::Ulid::generate()),
            f.store.now() + Duration::from_hours(1),
        )
        .await
        .unwrap();
    f.store
        .park_for_capacity(
            session.session_id,
            &lease,
            GpuRequirement::Shared,
            f.store.now(),
            Duration::from_hours(1),
        )
        .await
        .unwrap();
    f.desired(1).await;
    f.reconciler.tick(&mut Holder::new()).await.unwrap();
    let row = &f.records().await[0];
    assert_eq!(
        nodes.calls(),
        vec![format!(
            "join fleet-gpu-1 203.0.113.11 root sandboxes=1 storage=dir:/tmp node={}",
            row.machine_id.joined_node_id()
        )]
    );
    assert_eq!(row.state, MachineState::Joined);
    assert!(row.provisioned_at.is_some() && row.joined_at.is_some());
    assert_eq!(row.node_id, Some(row.machine_id.joined_node_id()));
    assert_eq!(
        f.store
            .fetch_session(session.session_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        SessionState::Runnable
    );
}
#[tokio::test]
async fn join_failures_retry_until_the_timeout_then_the_machine_is_replaced() {
    let nodes = Arc::new(FakeNodes::default());
    *nodes.join_error.lock().unwrap() = Some("ssh refused".into());
    let Some(f) = Fixture::with_nodes(1, nodes.clone()).await else {
        return;
    };
    f.desired(1).await;
    let mut holder = Holder::new();
    f.reconciler.tick(&mut holder).await.unwrap();
    assert_eq!(f.records().await[0].state, MachineState::Provisioning);
    assert_eq!(nodes.calls().len(), 1);
    *f.clock.lock().unwrap() += Duration::from_mins(14);
    f.reconciler.tick(&mut holder).await.unwrap();
    assert_eq!(nodes.calls().len(), 2);
    assert_eq!(f.records().await[0].state, MachineState::Provisioning);
    *f.clock.lock().unwrap() += Duration::from_mins(1);
    f.reconciler.tick(&mut holder).await.unwrap();
    let failed = &f.records().await[0];
    assert_eq!(failed.state, MachineState::Failed);
    assert!(
        failed
            .failure
            .as_ref()
            .unwrap()
            .contains("did not join within 15 minutes")
    );
    assert!(failed.failure.as_ref().unwrap().contains("ssh refused"));
    f.reconciler.tick(&mut holder).await.unwrap();
    assert!(
        nodes
            .calls()
            .iter()
            .any(|call| call == "drain fleet-gpu-1 600 applies=1")
    );
    assert!(
        nodes
            .calls()
            .iter()
            .any(|call| call == "revoke fleet-gpu-1 applies=1")
    );
    assert!(f.log().contains("call gpu destroy"));
    assert_eq!(f.records().await[0].state, MachineState::Destroyed);
    f.reconciler.tick(&mut holder).await.unwrap();
    assert_eq!(f.log().matches("apply machine_count=1").count(), 2);
    assert!(
        f.records()
            .await
            .iter()
            .any(|r| r.name == "fleet-gpu-1" && r.state == MachineState::Provisioning)
    );
}
#[tokio::test]
async fn provisioned_machine_joins_after_restart_without_reprovisioning() {
    let nodes = Arc::new(FakeNodes::default());
    let Some(f) = Fixture::with_nodes(1, nodes.clone()).await else {
        return;
    };
    f.desired(1).await;
    let machine = seed(&f, 1, MachineState::Provisioning).await;
    f.store
        .transition_machine(
            machine.machine_id,
            &MachineTransition::Provisioned,
            f.store.now(),
        )
        .await
        .unwrap();
    state(&f, 1);
    let lease = f
        .store
        .acquire_provisioner_lease(
            LeaseOwnerId::from_ulid(5u128.into()),
            Duration::from_mins(15),
        )
        .await
        .unwrap();
    f.store
        .record_elastic_reconcile(&lease, "gpu", 1, 1, true)
        .await
        .unwrap();
    f.store.release_provisioner_lease(&lease).await.unwrap();
    f.store
        .put_node(&sandbox_node(
            machine.machine_id.joined_node_id(),
            f.store.now(),
        ))
        .await
        .unwrap();
    f.reconciler.tick(&mut Holder::new()).await.unwrap();
    assert_eq!(f.records().await[0].state, MachineState::Joined);
    assert!(nodes.calls().is_empty());
}
async fn scale_down_case(unreachable: bool) {
    let nodes = Arc::new(FakeNodes::default());
    let Some(f) = Fixture::with_nodes(1, nodes.clone()).await else {
        return;
    };
    f.desired(1).await;
    let mut holder = Holder::new();
    f.reconciler.tick(&mut holder).await.unwrap();
    *nodes.drain_fails.lock().unwrap() = unreachable;
    f.desired(0).await;
    f.reconciler.tick(&mut holder).await.unwrap();
    let calls = nodes.calls();
    assert_eq!(
        &calls[calls.len() - 2..],
        [
            "drain fleet-gpu-1 600 applies=1",
            "revoke fleet-gpu-1 applies=1"
        ]
    );
    assert!(f.log().contains("call gpu destroy"));
    let row = &f.records().await[0];
    assert_eq!(row.state, MachineState::Destroyed);
    assert!(row.requested_at <= f.store.now());
    assert!(row.created_at.is_some() && row.provisioned_at.is_some() && row.joined_at.is_some());
    assert!(row.drain_requested_at.is_some() && row.destroyed_at.is_some());
    assert_eq!(row.cost_micros(f.store.now()), 750_000);
}
#[tokio::test]
async fn scale_down_drains_and_revokes_before_destroying() {
    scale_down_case(false).await;
}
#[tokio::test]
async fn unreachable_node_is_still_destroyed() {
    scale_down_case(true).await;
}

async fn settled(f: &Fixture, applied: u32) {
    let lease = f
        .store
        .acquire_provisioner_lease(
            LeaseOwnerId::from_ulid(ulid::Ulid::generate()),
            Duration::from_mins(15),
        )
        .await
        .unwrap();
    let generation = f
        .store
        .get_elastic_group("gpu")
        .await
        .unwrap()
        .unwrap()
        .generation;
    f.store
        .record_elastic_reconcile(&lease, "gpu", applied, generation, true)
        .await
        .unwrap();
    f.store.release_provisioner_lease(&lease).await.unwrap();
}
async fn provisioned(f: &Fixture, machine: &swarmy_core::MachineRecord) {
    f.store
        .transition_machine(
            machine.machine_id,
            &MachineTransition::Provisioned,
            f.store.now(),
        )
        .await
        .unwrap();
}
async fn joined(f: &Fixture, machine: &swarmy_core::MachineRecord) {
    provisioned(f, machine).await;
    f.store
        .transition_machine(
            machine.machine_id,
            &MachineTransition::Joined {
                node_id: machine.machine_id.joined_node_id(),
            },
            f.store.now(),
        )
        .await
        .unwrap();
}
#[tokio::test]
async fn failed_machine_missing_from_state_is_destroyed_and_replaced() {
    let nodes = Arc::new(FakeNodes::default());
    *nodes.join_error.lock().unwrap() = Some("ssh refused".into());
    let Some(f) = Fixture::with_nodes(1, nodes).await else {
        return;
    };
    f.desired(1).await;
    let failed = seed(&f, 1, MachineState::Provisioning).await;
    f.store
        .transition_machine(
            failed.machine_id,
            &MachineTransition::Failed { detail: "x".into() },
            f.store.now(),
        )
        .await
        .unwrap();
    state(&f, 0);
    settled(&f, 1).await;
    let mut holder = Holder::new();
    f.reconciler.tick(&mut holder).await.unwrap();
    assert_eq!(
        f.records()
            .await
            .iter()
            .find(|r| r.machine_id == failed.machine_id)
            .unwrap()
            .state,
        MachineState::Destroyed
    );
    f.reconciler.tick(&mut holder).await.unwrap();
    assert!(f.log().contains("apply machine_count=1"));
    assert!(f.records().await.iter().any(|r| r.name == "fleet-gpu-1"
        && r.machine_id != failed.machine_id
        && r.state == MachineState::Provisioning));
}
#[tokio::test]
async fn missing_output_is_orphaned_before_drain() {
    let nodes = Arc::new(FakeNodes::default());
    let Some(f) = Fixture::with_nodes(2, nodes.clone()).await else {
        return;
    };
    f.desired(2).await;
    seed(&f, 1, MachineState::Provisioning).await;
    let second = seed(&f, 2, MachineState::Provisioning).await;
    joined(&f, &second).await;
    state(&f, 1);
    settled(&f, 2).await;
    f.desired(1).await;
    f.reconciler.tick(&mut Holder::new()).await.unwrap();
    assert_eq!(
        f.records()
            .await
            .iter()
            .find(|r| r.machine_id == second.machine_id)
            .unwrap()
            .state,
        MachineState::Orphaned
    );
    assert!(
        !nodes
            .calls()
            .iter()
            .any(|call| call.starts_with("drain fleet-gpu-2"))
    );
}
#[tokio::test]
async fn registered_node_joins_even_after_deadline() {
    let nodes = Arc::new(FakeNodes::default());
    let Some(f) = Fixture::with_nodes(1, nodes.clone()).await else {
        return;
    };
    f.desired(1).await;
    let machine = seed(&f, 1, MachineState::Provisioning).await;
    provisioned(&f, &machine).await;
    state(&f, 1);
    settled(&f, 1).await;
    f.store
        .put_node(&sandbox_node(
            machine.machine_id.joined_node_id(),
            f.store.now(),
        ))
        .await
        .unwrap();
    *f.clock.lock().unwrap() += Duration::from_mins(20);
    f.reconciler.tick(&mut Holder::new()).await.unwrap();
    assert_eq!(f.records().await[0].state, MachineState::Joined);
    assert!(nodes.calls().is_empty());
}
#[tokio::test]
async fn failed_lower_machine_removes_healthy_ones_above_then_regrows() {
    let nodes = Arc::new(FakeNodes::default());
    let Some(f) = Fixture::with_nodes(2, nodes.clone()).await else {
        return;
    };
    f.desired(2).await;
    let first = seed(&f, 1, MachineState::Provisioning).await;
    f.store
        .transition_machine(
            first.machine_id,
            &MachineTransition::Failed { detail: "x".into() },
            f.store.now(),
        )
        .await
        .unwrap();
    let second = seed(&f, 2, MachineState::Provisioning).await;
    joined(&f, &second).await;
    state(&f, 2);
    settled(&f, 2).await;
    let mut holder = Holder::new();
    f.reconciler.tick(&mut holder).await.unwrap();
    assert_eq!(
        nodes.calls(),
        [
            "drain fleet-gpu-2 600 applies=0",
            "revoke fleet-gpu-2 applies=0",
            "drain fleet-gpu-1 600 applies=0",
            "revoke fleet-gpu-1 applies=0"
        ]
    );
    assert!(f.log().contains("call gpu destroy"));
    assert!(
        f.records()
            .await
            .iter()
            .all(|r| r.state == MachineState::Destroyed)
    );
    f.reconciler.tick(&mut holder).await.unwrap();
    assert!(f.log().contains("apply machine_count=2"));
}

#[tokio::test]
async fn machines_carry_the_desired_cause() {
    let Some(f) = Fixture::new(true, 2).await else {
        return;
    };
    let cause = swarmy_core::MachineCause {
        agent: Some(AgentId::from_ulid(ulid::Ulid::from(1_u128))),
        session: Some(SessionId::from_ulid(ulid::Ulid::from(2_u128))),
        reason: "tasky 01X: y".into(),
    };
    f.store.set_elastic_desired("gpu", 1, &cause).await.unwrap();
    f.reconciler.tick(&mut Holder::new()).await.unwrap();
    assert_eq!(f.records().await[0].cause, cause);
}

async fn gpu_waiter(f: &Fixture, name: &str, mode: &str, reason: &str) -> (AgentId, SessionId) {
    use swarmy_core::{AgentSettings, Event, RequestId, ToolCallId, ToolCallRecord};
    let agent = f
        .store
        .create_agent(
            name,
            swarmy_testkit::image(&f.store).await,
            "",
            f.store.now(),
            Some(swarmy_store::CreateAgentOptions {
                settings: Some(&AgentSettings {
                    allow_gpu_request: Some(true),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .agent_id;
    let (id, _) = f
        .store
        .open_main_session(agent, f.store.now())
        .await
        .unwrap();
    f.store.wake_session(id, f.store.now()).await.unwrap();
    let lease = f
        .store
        .claim_lease(
            id,
            LeaseOwnerId::from_ulid(ulid::Ulid::generate()),
            f.store.now() + Duration::from_hours(1),
        )
        .await
        .unwrap();
    let head = f.store.fetch_session(id).await.unwrap().unwrap().head_seq;
    let result = f
        .store
        .complete_gpu_tool(
            id,
            head,
            &lease,
            RequestId::for_step(id, head),
            &ToolCallRecord {
                call_id: ToolCallId("request_gpu".into()),
                tool: "request_gpu".into(),
                arguments: serde_json::json!({ "mode": mode, "reason": reason }),
                result: None,
            },
        )
        .await
        .unwrap();
    assert!(matches!(result, Event::ToolCallCompleted { .. }));
    let requirement = match mode {
        "shared" => GpuRequirement::Shared,
        "dedicated" => GpuRequirement::Dedicated,
        _ => panic!("bad mode"),
    };
    f.store
        .park_for_capacity(
            id,
            &lease,
            requirement,
            f.store.now(),
            Duration::from_hours(1),
        )
        .await
        .unwrap();
    (agent, id)
}
async fn desired_count(f: &Fixture) -> u32 {
    f.store.list_elastic_groups().await.unwrap()[0].desired
}
#[tokio::test]
async fn a_gpu_wait_buys_a_machine_named_after_its_cause() {
    let Some(f) = Fixture::autoscaled(1, Arc::new(FakeNodes::default())).await else {
        return;
    };
    let (agent, session) = gpu_waiter(&f, "w1", "shared", "tasky 01M63TEST: shader demo").await;
    f.reconciler.tick(&mut Holder::new()).await.unwrap();
    assert_eq!(desired_count(&f).await, 1);
    let record = &f.records().await[0];
    assert_eq!(record.state, MachineState::Joined);
    assert_eq!(record.cause.agent, Some(agent));
    assert_eq!(record.cause.session, Some(session));
    assert!(
        record
            .cause
            .reason
            .starts_with("tasky 01M63TEST: shader demo (autoscale: 1 session(s)")
    );
    assert_eq!(
        f.store.fetch_session(session).await.unwrap().unwrap().state,
        SessionState::Runnable
    );
    f.store
        .place(
            agent,
            record.machine_id.joined_node_id(),
            f.store.now() + Duration::from_hours(1),
        )
        .await
        .unwrap();
    f.reconciler.tick(&mut Holder::new()).await.unwrap();
    assert_eq!(desired_count(&f).await, 1);
    assert_eq!(
        f.store
            .open_machine_assignments(record.machine_id)
            .await
            .unwrap()[0]
            .agent,
        agent
    );
}
#[tokio::test]
async fn an_idle_machine_is_removed_after_thirty_minutes() {
    let nodes = Arc::new(FakeNodes::default());
    let Some(f) = Fixture::autoscaled(1, nodes.clone()).await else {
        return;
    };
    let mut holder = Holder::new();
    let (agent, session) = gpu_waiter(&f, "w1", "shared", "tasky 01M63TEST: shader demo").await;
    f.reconciler.tick(&mut holder).await.unwrap();
    let machine = f.records().await[0].machine_id;
    let placement = f
        .store
        .place(
            agent,
            machine.joined_node_id(),
            f.store.now() + Duration::from_hours(1),
        )
        .await
        .unwrap();
    f.store.clear_session_wait(session).await.unwrap();
    f.store.release(&placement).await.unwrap();
    *f.clock.lock().unwrap() += Duration::from_mins(29);
    f.reconciler.tick(&mut holder).await.unwrap();
    assert_eq!(desired_count(&f).await, 1);
    assert!(!nodes.calls().iter().any(|c| c.starts_with("drain")));
    *f.clock.lock().unwrap() += Duration::from_mins(1);
    f.reconciler.tick(&mut holder).await.unwrap();
    assert_eq!(desired_count(&f).await, 0);
    assert!(
        f.store.list_elastic_groups().await.unwrap()[0]
            .desired_cause
            .reason
            .starts_with("autoscale: lowered from 1 to 0")
    );
    assert!(nodes.calls().iter().any(|c| c.starts_with("drain")));
    assert!(nodes.calls().iter().any(|c| c.starts_with("revoke")));
    assert!(f.log().contains("call gpu destroy"));
    assert_eq!(f.records().await[0].state, MachineState::Destroyed);
}
#[tokio::test]
async fn dedicated_waits_beyond_max_keep_waiting_and_block_lowering() {
    let Some(f) = Fixture::autoscaled(1, Arc::new(FakeNodes::default())).await else {
        return;
    };
    let (a, _) = gpu_waiter(&f, "a", "dedicated", "tasky first").await;
    let (_, b) = gpu_waiter(&f, "b", "dedicated", "tasky second").await;
    f.reconciler.tick(&mut Holder::new()).await.unwrap();
    assert_eq!(desired_count(&f).await, 1);
    assert_eq!(f.records().await.len(), 1);
    let machine = f.records().await[0].machine_id;
    let placement = f
        .store
        .place(
            a,
            machine.joined_node_id(),
            f.store.now() + Duration::from_hours(1),
        )
        .await
        .unwrap();
    f.reconciler.tick(&mut Holder::new()).await.unwrap();
    assert_eq!(desired_count(&f).await, 1);
    assert!(f.store.session_wait(b).await.unwrap().is_some());
    f.store.release(&placement).await.unwrap();
    *f.clock.lock().unwrap() += Duration::from_mins(31);
    f.reconciler.tick(&mut Holder::new()).await.unwrap();
    assert_eq!(desired_count(&f).await, 1);
}
#[tokio::test]
async fn aws_group_whose_location_equals_the_stored_region_applies() {
    let Some(f) = Fixture::new(false, 1).await else {
        return;
    };
    let credentials = f.store.credentials(Keyring::from_bytes([7; 32]));
    let env = [
        ("AWS_ACCESS_KEY_ID".into(), "test-access".into()),
        ("AWS_SECRET_ACCESS_KEY".into(), "test-secret".into()),
        ("AWS_REGION".into(), "us-west-2".into()),
    ]
    .into();
    credentials
        .put_entry(
            CredentialScope::Cluster,
            "aws",
            "default",
            &CredentialRecord {
                kind: CredentialKind::MachineProvider { env },
                bookkeeping: swarmy_core::CredentialBookkeeping::default(),
                updated_at: f.store.now(),
            },
        )
        .await
        .unwrap();
    let definition = f
        .store
        .get_elastic_group("gpu")
        .await
        .unwrap()
        .unwrap()
        .definition;
    f.store
        .put_elastic_group(&ElasticGroupDefinition {
            module: "aws".into(),
            size: "t3.large".into(),
            location: "us-west-2".into(),
            ..definition
        })
        .await
        .unwrap();
    f.desired(1).await;
    let Tick::Reconciled(outcomes) = f.reconciler.tick(&mut Holder::new()).await.unwrap() else {
        panic!("standby")
    };
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].after, 1);
    assert!(f.log().contains("apply machine_count=1"));
    assert!(f.log().contains("env AWS_REGION"));
}

#[tokio::test]
async fn autoscale_respects_the_creation_limit() {
    let Some(f) = Fixture::autoscaled(6, Arc::new(FakeNodes::default())).await else {
        return;
    };
    for i in 0..5 {
        gpu_waiter(&f, &format!("w{i}"), "dedicated", "test demand").await;
    }
    let Tick::Reconciled(outcomes) = f.reconciler.tick(&mut Holder::new()).await.unwrap() else {
        panic!("standby")
    };
    assert_eq!(desired_count(&f).await, 5);
    assert!(outcomes.iter().any(|o| o.group == "gpu" && o.limited));
    assert!(f.log().contains("apply machine_count=4"));
    assert!(!f.log().contains("apply machine_count=5"));
}
#[tokio::test]
async fn groups_without_autoscale_ignore_waits() {
    let Some(f) = Fixture::with_nodes(1, Arc::new(FakeNodes::default())).await else {
        return;
    };
    gpu_waiter(&f, "w1", "shared", "test").await;
    f.reconciler.tick(&mut Holder::new()).await.unwrap();
    assert_eq!(desired_count(&f).await, 0);
    assert!(!f.log().contains("apply machine_count"));
}
#[tokio::test]
async fn manual_scale_on_an_autoscaled_group_is_a_starting_point() {
    let Some(f) = Fixture::autoscaled(1, Arc::new(FakeNodes::default())).await else {
        return;
    };
    let mut holder = Holder::new();
    f.store
        .set_elastic_desired(
            "gpu",
            1,
            &swarmy_core::MachineCause::from_reason("swarmy swarm scale"),
        )
        .await
        .unwrap();
    f.reconciler.tick(&mut holder).await.unwrap();
    assert_eq!(f.records().await[0].state, MachineState::Joined);
    assert_eq!(f.records().await[0].cause.reason, "swarmy swarm scale");
    *f.clock.lock().unwrap() += Duration::from_mins(29);
    f.reconciler.tick(&mut holder).await.unwrap();
    assert_eq!(desired_count(&f).await, 1);
    *f.clock.lock().unwrap() += Duration::from_mins(1);
    f.reconciler.tick(&mut holder).await.unwrap();
    assert_eq!(desired_count(&f).await, 0);
}
