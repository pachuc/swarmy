//! Reconcile OpenTofu state and the durable machine ledger before each apply.
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

pub const RETRY_AFTER_FAILURE: Duration = Duration::from_secs(5 * 60);
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
    pub async fn tick(&self, holder: &mut Holder) -> Result<Tick> {
        let lease = match &holder.lease {
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
                    .expect("retry mutex poisoned")
                    .get(&record.definition.group)
                    .is_some_and(|(generation, at)| {
                        *generation == record.generation && self.store.now() < *at
                    })
            {
                continue;
            }
            let lease = match self
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
                    self.retry.lock().expect("retry mutex poisoned").insert(
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
        let missing = || {
            anyhow!(
                "elastic group {} needs {} credentials in the swarm: run swarmy --swarm {} auth set {} --from-env",
                def.group,
                provider,
                def.swarm,
                provider
            )
        };
        let Some(CredentialKind::MachineProvider { env }) = entry.map(|entry| entry.kind) else {
            return Err(missing());
        };
        let mut credentials = CloudCredentials::default();
        credentials.set(CloudCredential::from_env(provider, |name| {
            env.get(name).cloned()
        })?);
        let env = groups::module_credential_env(&credentials, &module, false)?;
        let key = groups::group_key(&self.data_dir, &def.swarm, &def.group).await?;
        let tofu = self
            .tofu
            .get_or_try_init(|| Tofu::resolve(&self.cache))
            .await?;
        let workspace = tofu.workspace(groups::group_dir(&self.data_dir, &def.group), env);
        let vars = |count| {
            groups::group_variables(&GroupInputs {
                swarm: &def.swarm,
                group: &def.group,
                machine_count: count,
                size: Some(&def.size),
                location: Some(&def.location),
                ssh_public_key: &key,
                bucket: "",
                network: "",
            })
        };
        workspace
            .prepare(
                swarmy_infra::modules::find(&def.module)
                    .context("module is not built in")?
                    .files,
                &vars(record.applied),
            )
            .await?;
        workspace.init().await?;
        let outputs = self.outputs(&workspace, def).await?;
        let actual = u32::try_from(outputs.len())?;
        self.sync(def, &record.desired_reason, &outputs, None)
            .await?;
        let active = self
            .store
            .list_machines(Some(&def.group), MachineFilter::Active)
            .await?;
        let draining = active
            .iter()
            .filter(|r| {
                r.state == MachineState::Draining && outputs.iter().any(|m| m.name == r.name)
            })
            .filter_map(|r| number(def, &r.name))
            .min();
        let (target, limited) = if let Some(n) = draining {
            (record.desired.min(n - 1), false)
        } else if record.desired > actual {
            let grant = self
                .store
                .reserve_elastic_creations(lease, record.desired - actual)
                .await?;
            if grant.granted < record.desired - actual {
                tracing::warn!(group = %def.group, wanted = record.desired - actual, granted = grant.granted, used = grant.used, limit = MAX_ELASTIC_CREATIONS_PER_HOUR, next_free_at = ?grant.next_free_at, "creation limit reached: at most 4 machine creations per hour per swarm; the rest wait for the window");
            }
            (
                actual + grant.granted,
                grant.granted < record.desired - actual,
            )
        } else {
            (record.desired, false)
        };
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
        workspace.set_variables(&vars(target)).await?;
        let applied = workspace.apply().await;
        self.backup(&workspace, &def.group).await;
        let after = self.outputs(&workspace, def).await;
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
        Ok(Outcome {
            group: def.group.clone(),
            before: actual,
            after: length,
            desired: record.desired,
            limited,
        })
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
        self.store.create_machine(&machine, self.store.now()).await.map_err(|error| {
            if matches!(error, StoreError::Domain(swarmy_store::DomainError::MachineNameInUse)) {
                anyhow!("elastic group {}: machine {} is active in the ledger and missing from OpenTofu state; it may still exist and cost money; check the provider and remove it before the group can grow past {} machines", def.group, machine.name, n - 1)
            } else { error.into() }
        })
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
            let Some(n) = number(def, &record.name) else {
                tracing::warn!(name = %record.name, "invalid machine name in ledger");
                continue;
            };
            let machine = outputs.iter().find(|m| m.name == record.name);
            match (record.state, machine) {
                (MachineState::Requested, Some(machine)) => { self.transition(record, MachineTransition::Creating).await?; self.transition(record, MachineTransition::Created { address: address(machine)? }).await?; }
                (MachineState::Creating, Some(machine)) => self.transition(record, MachineTransition::Created { address: address(machine)? }).await?,
                (MachineState::Draining, None) => self.transition(record, MachineTransition::Destroyed).await?,
                (MachineState::Provisioning | MachineState::Joined, None) => self.transition(record, MachineTransition::Orphaned { detail: format!("machine {} is no longer in OpenTofu state, but swarmy did not remove it; it may still exist and cost money; check the provider", record.name) }).await?,
                (MachineState::Requested | MachineState::Creating, None) if after.is_some() => {
                    let (actual, target, error) = after.as_ref().expect("post-apply sync");
                    if n > *actual && n <= *target { self.transition(record, MachineTransition::Failed { detail: error.clone().unwrap_or_else(|| format!("OpenTofu applied machine_count = {target} but did not report {}", record.name)) }).await?; }
                },
                (MachineState::Failed | MachineState::Orphaned, None) if after.as_ref().is_some_and(|(actual, target, _)| n > *target && n <= *actual) => self.transition(record, MachineTransition::Destroyed).await?,
                _ => {},
            }
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
    async fn fail_unneeded(
        &self,
        def: &ElasticGroupDefinition,
        outputs: &[Machine],
        target: u32,
        active: &[MachineRecord],
    ) -> Result<()> {
        for record in active {
            if matches!(
                record.state,
                MachineState::Requested | MachineState::Creating
            ) && !outputs.iter().any(|m| m.name == record.name)
                && !number(def, &record.name)
                    .is_some_and(|n| n > outputs.len() as u32 && n <= target)
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
