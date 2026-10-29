//! Transactional elastic-machine ledger and assignment intervals.
use crate::{DomainError, Result, Store, StoreError, read, scan_all, write};
use foundationdb::{Transaction, tuple::Subspace};
use jiff::Timestamp;
use swarmy_core::{
    AgentId, MachineAssignment, MachineId, MachineRecord, MachineState, MachineTransition,
    NewMachine, decode,
};

/// Which machine records a listing returns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MachineFilter {
    All,
    Active,
    State(MachineState),
}

fn invalid(field: &str) -> StoreError {
    StoreError::Domain(DomainError::InvalidMachine(field.to_owned()))
}
impl MachineFilter {
    fn matches(self, record: &MachineRecord) -> bool {
        match self {
            Self::All => true,
            Self::Active => record.is_active(),
            Self::State(state) => record.state == state,
        }
    }
}
fn open_assignment(
    space: &Subspace,
    machine: MachineId,
    key: &[u8],
    value: &[u8],
) -> Result<MachineAssignment> {
    let (bytes,): (Vec<u8>,) = space.unpack(key)?;
    Ok(MachineAssignment {
        machine,
        agent: crate::keys::agent_id(&bytes)?,
        from: decode(value)?,
        to: None,
    })
}
impl Store {
    /// Create a Requested record, validating its descriptive fields and currency.
    /// # Errors
    /// Duplicate ids, invalid fields, or storage errors.
    pub async fn create_machine(
        &self,
        machine: &NewMachine,
        at: Timestamp,
    ) -> Result<MachineRecord> {
        for (name, value) in [
            ("group", &machine.group),
            ("name", &machine.name),
            ("module", &machine.module),
            ("size", &machine.size),
            ("location", &machine.location),
            ("reason", &machine.cause.reason),
        ] {
            if value.trim().is_empty() {
                return Err(invalid(name));
            }
        }
        if !machine.hourly_price.currency_is_valid() {
            return Err(invalid("currency"));
        }
        self.transaction(|trx| async move {
            let key = self.keys().machine(machine.machine_id);
            if read::<MachineRecord>(&trx, &key).await?.is_some() {
                return Err(StoreError::Domain(DomainError::MachineExists));
            }
            let space = self
                .keys()
                .machine_by_group_name_space(&machine.group, &machine.name);
            for (index_key, _) in scan_all(&trx, space.range()).await? {
                let (bytes,): (Vec<u8>,) = space.unpack(&index_key)?;
                let existing = read::<MachineRecord>(
                    &trx,
                    &self.keys().machine(crate::keys::machine_id(&bytes)?),
                )
                .await?
                .ok_or(crate::StorageError::Corrupt)?;
                if existing.is_active() {
                    return Err(StoreError::Domain(DomainError::MachineNameInUse));
                }
            }
            let record = machine.record(at);
            write(&trx, &key, &record)?;
            write(
                &trx,
                &self
                    .keys()
                    .machine_by_group(&machine.group, &machine.name, machine.machine_id),
                &(),
            )?;
            Ok(record)
        })
        .await
    }
    /// Apply one lifecycle transition, closing all open assignments on destruction.
    /// # Errors
    /// Missing machine, invalid transition, or storage errors.
    pub async fn transition_machine(
        &self,
        id: MachineId,
        transition: &MachineTransition,
        at: Timestamp,
    ) -> Result<MachineRecord> {
        self.transaction(|trx| async move {
            let key = self.keys().machine(id);
            let mut record: MachineRecord = read(&trx, &key)
                .await?
                .ok_or(StoreError::Domain(DomainError::MachineMissing))?;
            record.apply(transition, at).map_err(DomainError::from)?;
            if matches!(transition, MachineTransition::Destroyed) {
                self.close_open_assignments_in(&trx, id, at).await?;
            }
            write(&trx, &key, &record)?;
            Ok(record)
        })
        .await
    }
    /// Read one record.
    /// # Errors
    /// Storage or decoding errors.
    pub async fn get_machine(&self, id: MachineId) -> Result<Option<MachineRecord>> {
        self.transaction(|trx| async move { read(&trx, &self.keys().machine(id)).await })
            .await
    }
    /// List records by request id across all groups, or by name within one group.
    /// # Errors
    /// Storage or decoding errors.
    pub async fn list_machines(
        &self,
        group: Option<&str>,
        filter: MachineFilter,
    ) -> Result<Vec<MachineRecord>> {
        let mut records = Vec::new();
        if let Some(group) = group {
            let space = self.keys().machine_by_group_space(group);
            let (begin, end) = space.range();
            for (key, _) in self.scan_all_pages(begin, end).await? {
                let (_, bytes): (String, Vec<u8>) = space.unpack(&key)?;
                let record = self
                    .get_machine(crate::keys::machine_id(&bytes)?)
                    .await?
                    .ok_or(crate::StorageError::Corrupt)?;
                records.push(record);
            }
        } else {
            let (begin, end) = self.keys().machine_space().range();
            for (_, value) in self.scan_all_pages(begin, end).await? {
                records.push(decode(&value)?);
            }
        }
        Ok(records
            .into_iter()
            .filter(|record| filter.matches(record))
            .collect())
    }
    /// Open a new interval only for a joined machine; an existing open interval is unchanged.
    /// # Errors
    /// Missing or non-joined machine, or storage errors.
    pub async fn open_machine_assignment(
        &self,
        machine: MachineId,
        agent: AgentId,
        at: Timestamp,
    ) -> Result<()> {
        self.transaction(|trx| async move {
            let record: MachineRecord = read(&trx, &self.keys().machine(machine))
                .await?
                .ok_or(StoreError::Domain(DomainError::MachineMissing))?;
            if record.state != MachineState::Joined {
                return Err(StoreError::Domain(DomainError::MachineNotJoined));
            }
            let open = self.keys().machine_assignment_open(machine, agent);
            if read::<Timestamp>(&trx, &open).await?.is_some() {
                return Ok(());
            }
            write(
                &trx,
                &self.keys().machine_assignment(machine, agent, at),
                &MachineAssignment {
                    machine,
                    agent,
                    from: at,
                    to: None,
                },
            )?;
            write(
                &trx,
                &self.keys().machine_assignment_by_agent(agent, machine, at),
                &(),
            )?;
            write(&trx, &open, &at)
        })
        .await
    }
    async fn close_assignment_in(
        &self,
        trx: &Transaction,
        machine: MachineId,
        agent: AgentId,
        from: Timestamp,
        at: Timestamp,
    ) -> Result<()> {
        let key = self.keys().machine_assignment(machine, agent, from);
        let mut interval: MachineAssignment =
            read(trx, &key).await?.ok_or(crate::StorageError::Corrupt)?;
        interval.to = Some(at.max(from));
        write(trx, &key, &interval)?;
        trx.clear(&self.keys().machine_assignment_open(machine, agent));
        Ok(())
    }
    async fn close_open_assignments_in(
        &self,
        trx: &Transaction,
        machine: MachineId,
        at: Timestamp,
    ) -> Result<()> {
        let space = self.keys().machine_assignment_open_space(machine);
        for (key, value) in scan_all(trx, space.range()).await? {
            let row = open_assignment(&space, machine, &key, &value)?;
            self.close_assignment_in(trx, machine, row.agent, row.from, at)
                .await?;
        }
        Ok(())
    }
    /// Every currently open interval on a machine, ordered by start and agent.
    /// # Errors
    /// Storage or decoding errors.
    pub async fn open_machine_assignments(
        &self,
        machine: MachineId,
    ) -> Result<Vec<MachineAssignment>> {
        let space = self.keys().machine_assignment_open_space(machine);
        let (begin, end) = space.range();
        let mut rows = Vec::new();
        for (key, value) in self.scan_all_pages(begin, end).await? {
            rows.push(open_assignment(&space, machine, &key, &value)?);
        }
        rows.sort_by_key(|row| (row.from, row.agent));
        Ok(rows)
    }
    /// Close an open interval. Returns false when nothing was open.
    /// # Errors
    /// Storage or decoding errors.
    pub async fn close_machine_assignment(
        &self,
        machine: MachineId,
        agent: AgentId,
        at: Timestamp,
    ) -> Result<bool> {
        self.transaction(|trx| async move {
            let Some(from) =
                read(&trx, &self.keys().machine_assignment_open(machine, agent)).await?
            else {
                return Ok(false);
            };
            self.close_assignment_in(&trx, machine, agent, from, at)
                .await?;
            Ok(true)
        })
        .await
    }
    /// Every interval on a machine, ordered by start, then agent.
    /// # Errors
    /// Storage or decoding errors.
    pub async fn machine_assignments(&self, machine: MachineId) -> Result<Vec<MachineAssignment>> {
        let (begin, end) = self.keys().machine_assignment_space(machine).range();
        let mut rows: Vec<MachineAssignment> = self
            .scan_all_pages(begin, end)
            .await?
            .into_iter()
            .map(|(_, value)| decode(&value))
            .collect::<std::result::Result<_, _>>()?;
        rows.sort_by_key(|row| (row.from, row.agent));
        Ok(rows)
    }
    /// Every interval for an agent across machines, ordered by start.
    /// # Errors
    /// Storage or decoding errors.
    pub async fn agent_machine_assignments(
        &self,
        agent: AgentId,
    ) -> Result<Vec<MachineAssignment>> {
        let space = self.keys().machine_assignment_by_agent_space(agent);
        let (begin, end) = space.range();
        let mut rows = Vec::new();
        for (key, _) in self.scan_all_pages(begin, end).await? {
            let (bytes, (seconds, nanos)): (Vec<u8>, (i64, i32)) = space.unpack(&key)?;
            let machine = crate::keys::machine_id(&bytes)?;
            let from = Timestamp::new(seconds, nanos).map_err(|_| crate::StorageError::Corrupt)?;
            let row = self
                .transaction(|trx| async move {
                    read::<MachineAssignment>(
                        &trx,
                        &self.keys().machine_assignment(machine, agent, from),
                    )
                    .await
                })
                .await?
                .ok_or(crate::StorageError::Corrupt)?;
            rows.push(row);
        }
        rows.sort_by_key(|row| (row.from, row.machine));
        Ok(rows)
    }
}
