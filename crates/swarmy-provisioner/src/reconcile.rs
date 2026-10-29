//! Reconcile `OpenTofu` state and the durable machine ledger before each apply.
//!
//! | Rule | Ledger | In state | Action |
//! |---|---|---|---|
//! | S1 | Requested | yes | Creating, Created |
//! | S2 | Creating | yes | Created |
//! | S3 | Requested or Creating | no | Reuse or fail after target selection |
//! | S4 | Provisioning or Joined | no | Orphaned |
//! | S5 | Draining | no | Destroyed |
//! | S6 | Provisioning, Joined, Draining, Failed or Orphaned | yes | None |
//! | S7 | Failed or Orphaned | no | None |
//! | S8 | No active record | yes | Adopt as Creating, Created |
//!
//! Replaying these rules against state before applying makes every interrupted step recoverable.
//! Reservations and group results check the lease in their transactions. Ledger transitions
//! are not fenced: the binary runs as a single unit on the primary, and invalid transitions fail.
use anyhow::{Context, Result, anyhow, bail};
use jiff::Timestamp;
use object_store::{ObjectStore, path::Path};
use std::{
    collections::BTreeMap,
    path::{Path as FilePath, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use swarmy_cloud::provision::groups::{self, GroupInputs};
use swarmy_config::{
    cloud::{CloudCredential, CloudCredentials},
    swarm::Module,
};
use swarmy_core::{
    BillingGranularity, CredentialKind, CredentialScope, ElasticGroupDefinition,
    ElasticGroupRecord, Lease, LeaseOwnerId, MachineCause, MachineId, MachineRecord, MachineState,
    MachineTransition, NewMachine,
};
use swarmy_infra::{Machine, Tofu, Workspace};
use swarmy_store::{
    FenceError, MAX_ELASTIC_CREATIONS_PER_HOUR, MachineFilter, Store, StoreError,
    credentials::CredentialStore,
};

pub const RETRY_AFTER_FAILURE: Duration = Duration::from_mins(5);
pub struct Reconciler {
    store: Store,
    credentials: Option<CredentialStore>,
    objects: Arc<dyn ObjectStore>,
    data_dir: PathBuf,
    cache: PathBuf,
    tofu: tokio::sync::OnceCell<Tofu>,
    retry: Mutex<BTreeMap<String, (u64, Timestamp)>>,
}
pub struct Holder {
    owner: LeaseOwnerId,
    lease: Option<Lease>,
}
impl Holder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            owner: LeaseOwnerId::from_ulid(ulid::Ulid::generate()),
            lease: None,
        }
    }
    #[must_use]
    pub fn lease(&self) -> Option<&Lease> {
        self.lease.as_ref()
    }
}
impl Default for Holder {
    fn default() -> Self {
        Self::new()
    }
}
#[derive(Debug)]
pub enum Tick {
    Standby,
    Reconciled(Vec<Outcome>),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Outcome {
    pub group: String,
    pub before: u32,
    pub after: u32,
    pub desired: u32,
    pub limited: bool,
}
impl Reconciler {
    #[must_use]
    pub fn new(
        store: Store,
        credentials: Option<CredentialStore>,
        objects: Arc<dyn ObjectStore>,
        data_dir: PathBuf,
        cache: PathBuf,
    ) -> Self {
        Self {
            store,
            credentials,
            objects,
            data_dir,
            cache,
            tofu: tokio::sync::OnceCell::new(),
            retry: Mutex::new(BTreeMap::new()),
        }
    }
    #[must_use]
    pub fn with_tofu(mut self, tofu: Tofu) -> Self {
        self.tofu = tokio::sync::OnceCell::from(tofu);
        self
    }
    #[must_use]
    pub fn store(&self) -> &Store {
        &self.store
    }
    /// Renew before every group; a replaced holder never starts an apply.
    /// # Errors
    /// Returns store or runtime failures.
    /// # Panics
    /// Only if the claimed lease unexpectedly disappears within this tick.
    pub async fn tick(&self, holder: &mut Holder) -> Result<Tick> {
        let lease = match &holder.lease {
            Some(lease) if lease.expires_at <= self.store.now() => {
                self.store
                    .acquire_provisioner_lease(holder.owner, crate::role::LEASE_TTL)
                    .await
            }
            Some(lease) if lease.expires_at >= self.store.now() + crate::role::LEASE_TTL => {
                Ok(lease.clone())
            }
            Some(lease) => {
                self.store
                    .renew_provisioner_lease(lease, crate::role::LEASE_TTL)
                    .await
            }
            None => {
                self.store
                    .acquire_provisioner_lease(holder.owner, crate::role::LEASE_TTL)
                    .await
            }
        };
        holder.lease = match lease {
            Ok(lease) => Some(lease),
            Err(StoreError::Fence(FenceError::ProvisionerLeaseMismatch)) => {
                holder.lease = None;
                return Ok(Tick::Standby);
            }
            Err(error) => return Err(error.into()),
        };
        let mut outcomes = Vec::new();
        for record in self.store.list_elastic_groups().await? {
            if record.settled()
                || self
                    .retry
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(&record.definition.group)
                    .is_some_and(|(generation, at)| {
                        *generation == record.generation && self.store.now() < *at
                    })
            {
                continue;
            }
            let lease = if holder.lease.as_ref().expect("claimed lease").expires_at
                >= self.store.now() + crate::role::LEASE_TTL
            {
                holder.lease.as_ref().expect("claimed lease").clone()
            } else {
                match self
                    .store
                    .renew_provisioner_lease(
                        holder.lease.as_ref().expect("claimed lease"),
                        crate::role::LEASE_TTL,
                    )
                    .await
                {
                    Ok(lease) => lease,
                    Err(StoreError::Fence(FenceError::ProvisionerLeaseMismatch)) => {
                        holder.lease = None;
                        return Ok(Tick::Standby);
                    }
                    Err(error) => return Err(error.into()),
                }
            };
            holder.lease = Some(lease.clone());
            match self.reconcile(&lease, &record).await {
                Ok(outcome) => {
                    self.retry
                        .lock()
                        .expect("retry mutex poisoned")
                        .remove(&record.definition.group);
                    outcomes.push(outcome);
                }
                Err(error) => {
                    tracing::error!(group = %record.definition.group, error = %swarmy_core::error_chain(error.as_ref()), "elastic reconcile failed");
                    self.retry
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(
                            record.definition.group.clone(),
                            (record.generation, self.store.now() + RETRY_AFTER_FAILURE),
                        );
                }
            }
        }
        Ok(Tick::Reconciled(outcomes))
    }
    /// The single reconcile path shared by manual and future autoscaling.
    /// # Errors
    /// Returns invalid state, missing credentials, apply, or store failures.
    pub async fn reconcile(&self, lease: &Lease, record: &ElasticGroupRecord) -> Result<Outcome> {
        let def = &record.definition;
        if record.desired > record.applied
            && self.store.elastic_creation_slots().await?.used >= MAX_ELASTIC_CREATIONS_PER_HOUR
        {
            tracing::debug!(group = %def.group, "creation window full");
            return Ok(Outcome {
                group: def.group.clone(),
                before: record.applied,
                after: record.applied,
                desired: record.desired,
                limited: true,
            });
        }
        let (workspace, key) = self.workspace(def, record.applied).await?;
        let outputs = self.outputs(&workspace, def).await?;
        let actual = u32::try_from(outputs.len())?;
        self.sync(def, &record.desired_reason, &outputs, None)
            .await?;
        let active = self
            .store
            .list_machines(Some(&def.group), MachineFilter::Active)
            .await?;
        let (target, limited) = self
            .target(lease, record, actual, &outputs, &active)
            .await?;
        self.fail_unneeded(def, &outputs, target, &active).await?;
        if target == actual {
            self.store
                .record_elastic_reconcile(
                    lease,
                    &def.group,
                    actual,
                    record.generation,
                    actual == record.desired,
                )
                .await?;
            return Ok(Outcome {
                group: def.group.clone(),
                before: actual,
                after: actual,
                desired: record.desired,
                limited,
            });
        }
        self.prepare_ledger(def, &record.desired_reason, actual, target)
            .await?;
        workspace
            .set_variables(&variables(def, &key, target))
            .await?;
        self.apply_and_record(lease, record, &workspace, actual, target)
            .await?;
        Ok(Outcome {
            group: def.group.clone(),
            before: actual,
            after: target,
            desired: record.desired,
            limited,
        })
    }
    async fn credential_env(
        &self,
        def: &ElasticGroupDefinition,
    ) -> Result<swarmy_infra::CredentialEnv> {
        let module = Module::try_from(def.module.clone()).map_err(|error| anyhow!(error))?;
        let provider = module
            .machine_provider()
            .context("elastic group must use a built-in machine module")?;
        let creds = self
            .credentials
            .as_ref()
            .context("the provisioner needs the cluster keyring to decrypt cloud credentials")?;
        let entry = creds
            .get_entry(CredentialScope::Cluster, provider.name(), "default")
            .await?;
        let Some(CredentialKind::MachineProvider { env }) = entry.map(|entry| entry.kind) else {
            bail!(
                "elastic group {} needs {} credentials in the swarm: run swarmy --swarm {} auth set {} --from-env",
                def.group,
                provider,
                def.swarm,
                provider
            );
        };
        let mut credentials = CloudCredentials::default();
        credentials.set(CloudCredential::from_env(provider, |name| {
            env.get(name).cloned()
        })?);
        Ok(groups::module_credential_env(&credentials, &module, false)?)
    }
    async fn workspace(
        &self,
        def: &ElasticGroupDefinition,
        applied: u32,
    ) -> Result<(Workspace<'_>, String)> {
        let env = self.credential_env(def).await?;
        let key = groups::group_key(&self.data_dir, &def.swarm, &def.group).await?;
        let tofu = self
            .tofu
            .get_or_try_init(|| Tofu::resolve(&self.cache))
            .await?;
        let workspace = tofu.workspace(groups::group_dir(&self.data_dir, &def.group), env);
        workspace
            .prepare(
                swarmy_infra::modules::find(&def.module)
                    .context("module is not built in")?
                    .files,
                &variables(def, &key, applied),
            )
            .await?;
        workspace.init().await?;
        Ok((workspace, key))
    }
    async fn target(
        &self,
        lease: &Lease,
        record: &ElasticGroupRecord,
        actual: u32,
        outputs: &[Machine],
        active: &[MachineRecord],
    ) -> Result<(u32, bool)> {
        let def = &record.definition;
        let draining = active
            .iter()
            .filter(|r| {
                r.state == MachineState::Draining && outputs.iter().any(|m| m.name == r.name)
            })
            .filter_map(|r| number(def, &r.name))
            .min();
        if let Some(n) = draining {
            return Ok((record.desired.min(n - 1), false));
        }
        if record.desired <= actual {
            return Ok((record.desired, false));
        }
        let wanted = record.desired - actual;
        let grant = self.store.reserve_elastic_creations(lease, wanted).await?;
        if grant.granted < wanted {
            tracing::warn!(group = %def.group, wanted, granted = grant.granted, used = grant.used, limit = MAX_ELASTIC_CREATIONS_PER_HOUR, next_free_at = ?grant.next_free_at, "creation limit reached: at most 4 machine creations per hour per swarm; the rest wait for the window");
        }
        Ok((actual + grant.granted, grant.granted < wanted))
    }
    async fn apply_and_record(
        &self,
        lease: &Lease,
        record: &ElasticGroupRecord,
        workspace: &Workspace<'_>,
        actual: u32,
        target: u32,
    ) -> Result<()> {
        let def = &record.definition;
        let applied = workspace.apply().await;
        self.backup(workspace, &def.group).await;
        let after = self.outputs(workspace, def).await;
        let length = if let Ok(ref machines) = after {
            u32::try_from(machines.len())?
        } else {
            record.applied
        };
        if let Ok(ref machines) = after {
            self.sync(
                def,
                &record.desired_reason,
                machines,
                Some((
                    actual,
                    target,
                    applied
                        .as_ref()
                        .err()
                        .map(|error| swarmy_core::error_chain(error)),
                )),
            )
            .await?;
        } else if let Err(ref error) = applied {
            self.fail_pending(def, actual, target, &swarmy_core::error_chain(error))
                .await?;
        }
        // Task 6b joins machines left in Provisioning here: provision over SSH, then Provisioned and Joined.
        self.store
            .record_elastic_reconcile(
                lease,
                &def.group,
                length,
                record.generation,
                applied.is_ok() && length == record.desired,
            )
            .await?;
        applied.with_context(|| format!("OpenTofu apply for elastic group {}", def.group))?;
        if length != target {
            bail!(
                "OpenTofu reported {length} machines after applying machine_count = {target} for elastic group {}",
                def.group
            );
        }
        Ok(())
    }
    async fn outputs(
        &self,
        workspace: &Workspace<'_>,
        def: &ElasticGroupDefinition,
    ) -> Result<Vec<Machine>> {
        if !workspace.dir().join("terraform.tfstate").exists() {
            return Ok(Vec::new());
        }
        let machines = workspace.outputs().await?.machines;
        for (i, machine) in machines.iter().enumerate() {
            if machine.name != name(def, u32::try_from(i)? + 1) {
                bail!(
                    "OpenTofu state for elastic group {} lists {:?}, expected {} to {}; the module or its state was changed outside swarmy",
                    def.group,
                    machines.iter().map(|m| &m.name).collect::<Vec<_>>(),
                    name(def, 1),
                    name(def, u32::try_from(machines.len())?)
                );
            }
        }
        Ok(machines)
    }
    async fn transition(&self, record: &MachineRecord, change: MachineTransition) -> Result<()> {
        self.store
            .transition_machine(record.machine_id, &change, self.store.now())
            .await?;
        Ok(())
    }
    async fn create(
        &self,
        def: &ElasticGroupDefinition,
        n: u32,
        reason: &str,
    ) -> Result<MachineRecord> {
        let machine = NewMachine {
            machine_id: MachineId::from_ulid(ulid::Ulid::generate()),
            group: def.group.clone(),
            name: name(def, n),
            module: def.module.clone(),
            size: def.size.clone(),
            location: def.location.clone(),
            cause: MachineCause {
                agent: None,
                session: None,
                reason: reason.into(),
            },
            hourly_price: def.hourly_price.clone(),
            billing: BillingGranularity::default(),
        };
        match self.store.create_machine(&machine, self.store.now()).await {
            Ok(record) => Ok(record),
            Err(StoreError::Domain(swarmy_store::DomainError::MachineNameInUse)) => {
                let state = self
                    .store
                    .list_machines(Some(&def.group), MachineFilter::Active)
                    .await?
                    .into_iter()
                    .find(|record| record.name == machine.name)
                    .map_or_else(|| "active".to_owned(), |record| record.state.to_string());
                bail!(
                    "elastic group {}: machine {} is {} in the ledger and missing from OpenTofu state; it may still exist and cost money; check the provider and remove it before the group can grow past {} machines",
                    def.group,
                    machine.name,
                    state,
                    n - 1
                )
            }
            Err(error) => Err(error.into()),
        }
    }
    async fn sync(
        &self,
        def: &ElasticGroupDefinition,
        reason: &str,
        outputs: &[Machine],
        after: Option<(u32, u32, Option<String>)>,
    ) -> Result<()> {
        let active = self
            .store
            .list_machines(Some(&def.group), MachineFilter::Active)
            .await?;
        for record in &active {
            self.sync_record(def, record, outputs, after.as_ref())
                .await?;
        }
        for (i, machine) in outputs.iter().enumerate() {
            if !active.iter().any(|r| r.name == machine.name) {
                let record = self
                    .create(
                        def,
                        u32::try_from(i)? + 1,
                        &format!("adopted from OpenTofu state; {reason}"),
                    )
                    .await?;
                tracing::warn!(name = %machine.name, "adopted machine; billing starts now");
                self.transition(&record, MachineTransition::Creating)
                    .await?;
                self.transition(
                    &record,
                    MachineTransition::Created {
                        address: address(machine)?,
                    },
                )
                .await?;
            }
        }
        Ok(())
    }
    async fn sync_record(
        &self,
        def: &ElasticGroupDefinition,
        record: &MachineRecord,
        outputs: &[Machine],
        after: Option<&(u32, u32, Option<String>)>,
    ) -> Result<()> {
        let Some(n) = number(def, &record.name) else {
            tracing::warn!(name = %record.name, "invalid machine name in ledger");
            return Ok(());
        };
        let machine = outputs.iter().find(|m| m.name == record.name);
        match (record.state, machine) {
            (MachineState::Requested, Some(machine)) => {
                self.transition(record, MachineTransition::Creating).await?;
                self.transition(record, MachineTransition::Created { address: address(machine)? }).await?;
            }
            (MachineState::Creating, Some(machine)) => self.transition(record, MachineTransition::Created { address: address(machine)? }).await?,
            (MachineState::Draining, None) => self.transition(record, MachineTransition::Destroyed).await?,
            (MachineState::Provisioning | MachineState::Joined, None) => self.transition(record, MachineTransition::Orphaned { detail: format!("machine {} is no longer in OpenTofu state, but swarmy did not remove it; it may still exist and cost money; check the provider", record.name) }).await?,
            (MachineState::Requested | MachineState::Creating | MachineState::Failed | MachineState::Orphaned, None) => self.sync_missing(record, n, after).await?,
            _ => {},
        }
        Ok(())
    }
    async fn sync_missing(
        &self,
        record: &MachineRecord,
        n: u32,
        after: Option<&(u32, u32, Option<String>)>,
    ) -> Result<()> {
        let Some((actual, target, error)) = after else {
            return Ok(());
        };
        if matches!(
            record.state,
            MachineState::Requested | MachineState::Creating
        ) && n > *actual
            && n <= *target
        {
            let detail = error.clone().unwrap_or_else(|| {
                format!(
                    "OpenTofu applied machine_count = {target} but did not report {}",
                    record.name
                )
            });
            self.transition(record, MachineTransition::Failed { detail })
                .await?;
        } else if matches!(record.state, MachineState::Failed | MachineState::Orphaned)
            && n > *target
            && n <= *actual
        {
            self.transition(record, MachineTransition::Destroyed)
                .await?;
        }
        Ok(())
    }
    async fn fail_unneeded(
        &self,
        def: &ElasticGroupDefinition,
        outputs: &[Machine],
        target: u32,
        active: &[MachineRecord],
    ) -> Result<()> {
        let actual = u32::try_from(outputs.len())?;
        for record in active {
            if matches!(
                record.state,
                MachineState::Requested | MachineState::Creating
            ) && !outputs.iter().any(|m| m.name == record.name)
                && !number(def, &record.name).is_some_and(|n| n > actual && n <= target)
            {
                self.transition(
                    record,
                    MachineTransition::Failed {
                        detail: "not created: the target count changed before OpenTofu created it"
                            .into(),
                    },
                )
                .await?;
            }
        }
        Ok(())
    }
    async fn fail_pending(
        &self,
        def: &ElasticGroupDefinition,
        actual: u32,
        target: u32,
        detail: &str,
    ) -> Result<()> {
        for record in self
            .store
            .list_machines(Some(&def.group), MachineFilter::Active)
            .await?
        {
            if matches!(
                record.state,
                MachineState::Requested | MachineState::Creating
            ) && number(def, &record.name).is_some_and(|n| n > actual && n <= target)
            {
                self.transition(
                    &record,
                    MachineTransition::Failed {
                        detail: detail.into(),
                    },
                )
                .await?;
            }
        }
        Ok(())
    }
    async fn prepare_ledger(
        &self,
        def: &ElasticGroupDefinition,
        reason: &str,
        actual: u32,
        target: u32,
    ) -> Result<()> {
        let active = self
            .store
            .list_machines(Some(&def.group), MachineFilter::Active)
            .await?;
        for n in actual + 1..=target {
            let record = if let Some(record) = active.iter().find(|r| {
                r.name == name(def, n)
                    && matches!(r.state, MachineState::Requested | MachineState::Creating)
            }) {
                record.clone()
            } else {
                self.create(def, n, reason).await?
            };
            if record.state == MachineState::Requested {
                self.transition(&record, MachineTransition::Creating)
                    .await?;
            }
        }
        for n in target + 1..=actual {
            if let Some(record) = active.iter().find(|r| {
                r.name == name(def, n)
                    && matches!(r.state, MachineState::Provisioning | MachineState::Joined)
            }) {
                self.transition(record, MachineTransition::DrainRequested)
                    .await?;
            }
        }
        Ok(())
    }
    async fn backup(&self, workspace: &Workspace<'_>, group: &str) {
        let file = workspace.dir().join("terraform.tfstate");
        if FilePath::new(&file).exists() {
            let result: Result<()> = async {
                let bytes = tokio::fs::read(&file).await?;
                self.objects
                    .put(
                        &Path::from(format!("tofu-state/{group}/terraform.tfstate")),
                        bytes.into(),
                    )
                    .await?;
                Ok(())
            }
            .await;
            if let Err(error) = result {
                tracing::error!(group, error = %swarmy_core::error_chain(error.as_ref()), "OpenTofu state backup failed");
            }
        }
    }
}
fn name(def: &ElasticGroupDefinition, n: u32) -> String {
    format!("{}-{}-{n}", def.swarm, def.group)
}
fn number(def: &ElasticGroupDefinition, value: &str) -> Option<u32> {
    value
        .strip_prefix(&format!("{}-{}-", def.swarm, def.group))?
        .parse()
        .ok()
        .filter(|n| *n > 0)
}
fn address(machine: &Machine) -> Result<String> {
    machine
        .public_ipv4
        .map(|ip| ip.to_string())
        .or_else(|| machine.public_ipv6.map(|ip| ip.to_string()))
        .context("machine has no public address")
}

fn variables(
    def: &ElasticGroupDefinition,
    key: &str,
    machine_count: u32,
) -> serde_json::Map<String, serde_json::Value> {
    groups::group_variables(&GroupInputs {
        swarm: &def.swarm,
        group: &def.group,
        machine_count,
        size: Some(&def.size),
        location: Some(&def.location),
        ssh_public_key: key,
        bucket: "",
        network: "",
    })
}
