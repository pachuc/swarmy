use super::*;
use swarmy_core::{
    BillingGranularity, HourlyPrice, MachineCause, MachineId, MachineRecord, MachineState,
    MachineTransition, NewMachine, NodeId,
};
use swarmy_store::{DomainError, MachineFilter};

pub(super) fn machine(group: &str) -> NewMachine {
    NewMachine {
        machine_id: MachineId::from_ulid(Ulid::generate()),
        group: group.into(),
        name: format!("{group}-1"),
        module: "scaleway".into(),
        size: "L4".into(),
        location: "par".into(),
        cause: MachineCause {
            agent: None,
            session: None,
            reason: "task".into(),
        },
        hourly_price: HourlyPrice {
            micros: 750_000,
            currency: "EUR".into(),
        },
        billing: BillingGranularity::PerStartedHour,
    }
}
async fn joined(store: &Store, record: &NewMachine) {
    store.create_machine(record, timestamp(1)).await.unwrap();
    store
        .transition_machine(
            record.machine_id,
            &MachineTransition::Creating,
            timestamp(2),
        )
        .await
        .unwrap();
    store
        .transition_machine(
            record.machine_id,
            &MachineTransition::Created {
                address: "address".into(),
            },
            timestamp(3),
        )
        .await
        .unwrap();
    store
        .transition_machine(
            record.machine_id,
            &MachineTransition::Provisioned,
            timestamp(4),
        )
        .await
        .unwrap();
    store
        .transition_machine(
            record.machine_id,
            &MachineTransition::Joined {
                node_id: NodeId::from_ulid(Ulid::generate()),
            },
            timestamp(5),
        )
        .await
        .unwrap();
}
#[tokio::test]
async fn machine_create_rejects_duplicates_and_bad_input() {
    let Some(f) = TestStore::memory() else { return };
    let mut m = machine("create");
    let row = f.store.create_machine(&m, timestamp(1)).await.unwrap();
    assert_eq!(f.store.get_machine(m.machine_id).await.unwrap(), Some(row));
    assert!(matches!(
        f.store.create_machine(&m, timestamp(2)).await,
        Err(StoreError::Domain(DomainError::MachineExists))
    ));
    m.machine_id = MachineId::from_ulid(Ulid::generate());
    m.hourly_price.currency = "eur".into();
    assert!(matches!(
        f.store.create_machine(&m, timestamp(2)).await,
        Err(StoreError::Domain(DomainError::InvalidMachine(_)))
    ));
    m.hourly_price.currency = "EUR".into();
    m.cause.reason.clear();
    assert!(matches!(
        f.store.create_machine(&m, timestamp(2)).await,
        Err(StoreError::Domain(DomainError::InvalidMachine(_)))
    ));
}
async fn assert_stored(store: &Store, expected: &MachineRecord) {
    assert_eq!(
        store.get_machine(expected.machine_id).await.unwrap(),
        Some(expected.clone())
    );
}
#[tokio::test]
async fn machine_lifecycle_persists_each_transition() {
    let Some(f) = TestStore::memory() else { return };
    let m = machine("lifecycle");
    let mut row = f.store.create_machine(&m, timestamp(1)).await.unwrap();
    assert_eq!(row.state, MachineState::Requested);
    assert_stored(&f.store, &row).await;
    row = f
        .store
        .transition_machine(m.machine_id, &MachineTransition::Creating, timestamp(2))
        .await
        .unwrap();
    assert_eq!(row.state, MachineState::Creating);
    assert_stored(&f.store, &row).await;
    row = f
        .store
        .transition_machine(
            m.machine_id,
            &MachineTransition::Created {
                address: "address".into(),
            },
            timestamp(3),
        )
        .await
        .unwrap();
    assert_eq!(row.state, MachineState::Provisioning);
    assert_eq!(row.created_at, Some(timestamp(3)));
    assert_eq!(row.address.as_deref(), Some("address"));
    assert_stored(&f.store, &row).await;
    row = f
        .store
        .transition_machine(m.machine_id, &MachineTransition::Provisioned, timestamp(4))
        .await
        .unwrap();
    assert_eq!(row.provisioned_at, Some(timestamp(4)));
    assert_stored(&f.store, &row).await;
    let node_id = NodeId::from_ulid(Ulid::generate());
    row = f
        .store
        .transition_machine(
            m.machine_id,
            &MachineTransition::Joined { node_id },
            timestamp(5),
        )
        .await
        .unwrap();
    assert_eq!(row.state, MachineState::Joined);
    assert_eq!(row.joined_at, Some(timestamp(5)));
    assert_eq!(row.node_id, Some(node_id));
    assert_stored(&f.store, &row).await;
    row = f
        .store
        .transition_machine(
            m.machine_id,
            &MachineTransition::DrainRequested,
            timestamp(6),
        )
        .await
        .unwrap();
    assert_eq!(row.state, MachineState::Draining);
    assert_eq!(row.drain_requested_at, Some(timestamp(6)));
    assert_stored(&f.store, &row).await;
    row = f
        .store
        .transition_machine(m.machine_id, &MachineTransition::Destroyed, timestamp(7))
        .await
        .unwrap();
    assert_eq!(row.state, MachineState::Destroyed);
    assert_eq!(row.destroyed_at, Some(timestamp(7)));
    assert_stored(&f.store, &row).await;
}
#[tokio::test]
async fn rejected_transition_leaves_record_unchanged() {
    let Some(f) = TestStore::memory() else { return };
    let m = machine("rejected");
    let original = f.store.create_machine(&m, timestamp(1)).await.unwrap();
    assert!(matches!(
        f.store
            .transition_machine(
                m.machine_id,
                &MachineTransition::Joined {
                    node_id: NodeId::from_ulid(Ulid::generate())
                },
                timestamp(2)
            )
            .await,
        Err(StoreError::Domain(DomainError::InvalidMachineTransition(_)))
    ));
    assert_eq!(
        f.store.get_machine(m.machine_id).await.unwrap(),
        Some(original)
    );
    assert!(matches!(
        f.store
            .transition_machine(
                MachineId::from_ulid(Ulid::generate()),
                &MachineTransition::Creating,
                timestamp(2)
            )
            .await,
        Err(StoreError::Domain(DomainError::MachineMissing))
    ));
}
#[tokio::test]
async fn assignments_open_and_close_as_intervals() {
    let Some(f) = TestStore::memory() else { return };
    let m = machine("assign");
    let agent = AgentId::from_ulid(Ulid::generate());
    f.store.create_machine(&m, timestamp(1)).await.unwrap();
    assert!(matches!(
        f.store
            .open_machine_assignment(m.machine_id, agent, timestamp(10))
            .await,
        Err(StoreError::Domain(DomainError::MachineNotJoined))
    ));
    f.store
        .transition_machine(m.machine_id, &MachineTransition::Creating, timestamp(2))
        .await
        .unwrap();
    f.store
        .transition_machine(
            m.machine_id,
            &MachineTransition::Created {
                address: "address".into(),
            },
            timestamp(3),
        )
        .await
        .unwrap();
    assert!(matches!(
        f.store
            .open_machine_assignment(m.machine_id, agent, timestamp(10))
            .await,
        Err(StoreError::Domain(DomainError::MachineNotJoined))
    ));
    f.store
        .transition_machine(m.machine_id, &MachineTransition::Provisioned, timestamp(4))
        .await
        .unwrap();
    f.store
        .transition_machine(
            m.machine_id,
            &MachineTransition::Joined {
                node_id: NodeId::from_ulid(Ulid::generate()),
            },
            timestamp(5),
        )
        .await
        .unwrap();
    f.store
        .open_machine_assignment(m.machine_id, agent, timestamp(100))
        .await
        .unwrap();
    f.store
        .open_machine_assignment(m.machine_id, agent, timestamp(150))
        .await
        .unwrap();
    assert!(
        f.store
            .close_machine_assignment(m.machine_id, agent, timestamp(200))
            .await
            .unwrap()
    );
    assert!(
        !f.store
            .close_machine_assignment(m.machine_id, agent, timestamp(201))
            .await
            .unwrap()
    );
    f.store
        .open_machine_assignment(m.machine_id, agent, timestamp(300))
        .await
        .unwrap();
    f.store
        .close_machine_assignment(m.machine_id, agent, timestamp(400))
        .await
        .unwrap();
    let rows = f.store.machine_assignments(m.machine_id).await.unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| (row.from, row.to))
            .collect::<Vec<_>>(),
        vec![
            (timestamp(100), Some(timestamp(200))),
            (timestamp(300), Some(timestamp(400)))
        ]
    );
    let second = machine("second");
    joined(&f.store, &second).await;
    f.store
        .open_machine_assignment(second.machine_id, agent, timestamp(250))
        .await
        .unwrap();
    assert_eq!(
        f.store
            .agent_machine_assignments(agent)
            .await
            .unwrap()
            .iter()
            .map(|row| row.from)
            .collect::<Vec<_>>(),
        vec![timestamp(100), timestamp(250), timestamp(300)]
    );
}
#[tokio::test]
async fn destroying_a_machine_closes_open_assignments() {
    let Some(f) = TestStore::memory() else { return };
    let m = machine("destroy");
    joined(&f.store, &m).await;
    let agents = [
        AgentId::from_ulid(Ulid::generate()),
        AgentId::from_ulid(Ulid::generate()),
    ];
    for agent in agents {
        f.store
            .open_machine_assignment(m.machine_id, agent, timestamp(100))
            .await
            .unwrap();
    }
    assert_eq!(
        f.store
            .open_machine_assignments(m.machine_id)
            .await
            .unwrap()
            .len(),
        2
    );
    f.store
        .transition_machine(
            m.machine_id,
            &MachineTransition::DrainRequested,
            timestamp(400),
        )
        .await
        .unwrap();
    f.store
        .transition_machine(m.machine_id, &MachineTransition::Destroyed, timestamp(500))
        .await
        .unwrap();
    assert_eq!(
        f.store
            .machine_assignments(m.machine_id)
            .await
            .unwrap()
            .iter()
            .map(|row| row.to)
            .collect::<Vec<_>>(),
        vec![Some(timestamp(500)); 2]
    );
    assert!(
        f.store
            .open_machine_assignments(m.machine_id)
            .await
            .unwrap()
            .is_empty()
    );
    for agent in agents {
        assert!(
            !f.store
                .close_machine_assignment(m.machine_id, agent, timestamp(600))
                .await
                .unwrap()
        );
    }
}
async fn fail_after_creation(store: &Store, machine: &NewMachine) {
    store.create_machine(machine, timestamp(1)).await.unwrap();
    store
        .transition_machine(
            machine.machine_id,
            &MachineTransition::Creating,
            timestamp(2),
        )
        .await
        .unwrap();
    store
        .transition_machine(
            machine.machine_id,
            &MachineTransition::Created {
                address: "address".into(),
            },
            timestamp(3),
        )
        .await
        .unwrap();
    store
        .transition_machine(
            machine.machine_id,
            &MachineTransition::Failed {
                detail: "failed".into(),
            },
            timestamp(4),
        )
        .await
        .unwrap();
}
#[tokio::test]
async fn list_machines_filters_by_state_and_activity() {
    let Some(f) = TestStore::memory() else { return };
    let mut requested = machine("requested");
    requested.machine_id = MachineId::from_ulid(1_u128.into());
    f.store
        .create_machine(&requested, timestamp(1))
        .await
        .unwrap();
    let mut joined_m = machine("joined");
    joined_m.machine_id = MachineId::from_ulid(2_u128.into());
    joined(&f.store, &joined_m).await;
    let mut destroyed = machine("destroyed");
    destroyed.machine_id = MachineId::from_ulid(3_u128.into());
    f.store
        .create_machine(&destroyed, timestamp(1))
        .await
        .unwrap();
    f.store
        .transition_machine(
            destroyed.machine_id,
            &MachineTransition::Destroyed,
            timestamp(2),
        )
        .await
        .unwrap();
    let mut failed_before = machine("failed_before");
    failed_before.machine_id = MachineId::from_ulid(4_u128.into());
    f.store
        .create_machine(&failed_before, timestamp(1))
        .await
        .unwrap();
    f.store
        .transition_machine(
            failed_before.machine_id,
            &MachineTransition::Failed {
                detail: "failed".into(),
            },
            timestamp(2),
        )
        .await
        .unwrap();
    let mut failed_after = machine("failed_after");
    failed_after.machine_id = MachineId::from_ulid(5_u128.into());
    fail_after_creation(&f.store, &failed_after).await;
    let all = f
        .store
        .list_machines(None, MachineFilter::All)
        .await
        .unwrap();
    assert_eq!(
        all.iter().map(|row| row.machine_id).collect::<Vec<_>>(),
        vec![
            requested.machine_id,
            joined_m.machine_id,
            destroyed.machine_id,
            failed_before.machine_id,
            failed_after.machine_id
        ]
    );
    let active: Vec<_> = f
        .store
        .list_machines(None, MachineFilter::Active)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.group)
        .collect();
    assert_eq!(active.len(), 3);
    for group in ["requested", "joined", "failed_after"] {
        assert!(active.contains(&group.to_owned()));
    }
    let joined_only = f
        .store
        .list_machines(None, MachineFilter::State(MachineState::Joined))
        .await
        .unwrap();
    assert_eq!(joined_only.len(), 1);
    assert_eq!(joined_only[0].machine_id, joined_m.machine_id);
}

#[tokio::test]
async fn machine_names_are_unique_while_active_and_group_list_is_name_ordered() {
    let Some(f) = TestStore::memory() else { return };
    let mut a2 = machine("a");
    a2.name = "a-2".into();
    let mut a1 = machine("a");
    a1.name = "a-1".into();
    let b = machine("b");
    f.store.create_machine(&a2, timestamp(1)).await.unwrap();
    f.store.create_machine(&a1, timestamp(2)).await.unwrap();
    f.store.create_machine(&b, timestamp(3)).await.unwrap();
    let group_a = f
        .store
        .list_machines(Some("a"), MachineFilter::All)
        .await
        .unwrap();
    assert_eq!(
        group_a
            .iter()
            .map(|row| row.name.as_str())
            .collect::<Vec<_>>(),
        vec!["a-1", "a-2"]
    );
    assert_eq!(
        f.store
            .list_machines(Some("b"), MachineFilter::All)
            .await
            .unwrap()
            .len(),
        1
    );
    let mut replacement = machine("a");
    replacement.name = "a-1".into();
    assert!(matches!(
        f.store.create_machine(&replacement, timestamp(4)).await,
        Err(StoreError::Domain(DomainError::MachineNameInUse))
    ));
    f.store
        .transition_machine(a1.machine_id, &MachineTransition::Destroyed, timestamp(5))
        .await
        .unwrap();
    let row = f
        .store
        .create_machine(&replacement, timestamp(6))
        .await
        .unwrap();
    assert_eq!(row.name, "a-1");
    assert_eq!(
        f.store
            .list_machines(Some("a"), MachineFilter::Active)
            .await
            .unwrap()
            .len(),
        2
    );
}
