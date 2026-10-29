use super::*;
use swarmy_core::{
    BillingGranularity, MachineCause, MachineId, MachineState, MachineTransition, NewMachine,
    NodeId,
};
use swarmy_store::{DomainError, MachineFilter};

fn machine(group: &str) -> NewMachine {
    NewMachine {
        machine_id: MachineId::from_ulid(Ulid::generate()),
        group: group.into(),
        module: "scaleway".into(),
        size: "L4".into(),
        location: "par".into(),
        cause: MachineCause {
            agent: None,
            session: None,
            reason: "task".into(),
        },
        hourly_price_micros: 750_000,
        currency: "EUR".into(),
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
    m.currency = "eur".into();
    assert!(matches!(
        f.store.create_machine(&m, timestamp(2)).await,
        Err(StoreError::Domain(DomainError::InvalidMachine(_)))
    ));
    m.currency = "EUR".into();
    m.cause.reason.clear();
    assert!(matches!(
        f.store.create_machine(&m, timestamp(2)).await,
        Err(StoreError::Domain(DomainError::InvalidMachine(_)))
    ));
}
#[tokio::test]
async fn machine_lifecycle_persists_each_transition() {
    let Some(f) = TestStore::memory() else { return };
    let m = machine("lifecycle");
    joined(&f.store, &m).await;
    let row = f.store.get_machine(m.machine_id).await.unwrap().unwrap();
    assert_eq!(row.state, MachineState::Joined);
    assert_eq!(row.created_at, Some(timestamp(3)));
    assert_eq!(row.provisioned_at, Some(timestamp(4)));
    assert_eq!(row.joined_at, Some(timestamp(5)));
    f.store
        .transition_machine(
            m.machine_id,
            &MachineTransition::DrainRequested,
            timestamp(6),
        )
        .await
        .unwrap();
    let row = f
        .store
        .transition_machine(m.machine_id, &MachineTransition::Destroyed, timestamp(7))
        .await
        .unwrap();
    assert_eq!(row.destroyed_at, Some(timestamp(7)));
    assert_eq!(f.store.get_machine(m.machine_id).await.unwrap(), Some(row));
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
    for _ in 0..2 {
        f.store
            .open_machine_assignment(
                m.machine_id,
                AgentId::from_ulid(Ulid::generate()),
                timestamp(100),
            )
            .await
            .unwrap();
    }
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
}
#[tokio::test]
async fn list_machines_filters_by_state_and_activity() {
    let Some(f) = TestStore::memory() else { return };
    let requested = machine("requested");
    f.store
        .create_machine(&requested, timestamp(1))
        .await
        .unwrap();
    let joined_m = machine("joined");
    joined(&f.store, &joined_m).await;
    let destroyed = machine("destroyed");
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
    let failed_before = machine("failed_before");
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
    let failed_after = machine("failed_after");
    f.store
        .create_machine(&failed_after, timestamp(1))
        .await
        .unwrap();
    f.store
        .transition_machine(
            failed_after.machine_id,
            &MachineTransition::Creating,
            timestamp(2),
        )
        .await
        .unwrap();
    f.store
        .transition_machine(
            failed_after.machine_id,
            &MachineTransition::Created {
                address: "address".into(),
            },
            timestamp(3),
        )
        .await
        .unwrap();
    f.store
        .transition_machine(
            failed_after.machine_id,
            &MachineTransition::Failed {
                detail: "failed".into(),
            },
            timestamp(4),
        )
        .await
        .unwrap();
    assert_eq!(
        f.store
            .list_machines(MachineFilter::All)
            .await
            .unwrap()
            .len(),
        5
    );
    let active: Vec<_> = f
        .store
        .list_machines(MachineFilter::Active)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.group)
        .collect();
    assert_eq!(active.len(), 3);
    for group in ["requested", "joined", "failed_after"] {
        assert!(active.contains(&group.to_owned()));
    }
    assert_eq!(
        f.store
            .list_machines(MachineFilter::State(MachineState::Joined))
            .await
            .unwrap()[0]
            .group,
        "joined"
    );
}
