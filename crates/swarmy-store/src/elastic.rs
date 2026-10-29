//! Runtime truth for elastic machine groups.
use crate::{DomainError, Result, StorageError, Store, StoreError, read, write};
use std::time::Duration;
use swarmy_core::{ElasticGroupDefinition, ElasticGroupRecord, MAX_ELASTIC_MACHINES};

const CREATION_WINDOW: Duration = Duration::from_hours(1);

impl Store {
    /// Register or update a definition without changing the desired machine count.
    /// # Errors
    /// Rejects an invalid maximum, lowering it below desired, or changing a busy group's identity.
    pub async fn put_elastic_group(
        &self,
        definition: &ElasticGroupDefinition,
    ) -> Result<ElasticGroupRecord> {
        if !(1..=MAX_ELASTIC_MACHINES).contains(&definition.max) {
            return Err(
                DomainError::InvalidElasticGroup("max must be between 1 and 50".into()).into(),
            );
        }
        self.transaction(|trx| async move {
            let key = self.keys().elastic_group(&definition.group);
            let old: Option<ElasticGroupRecord> = read(&trx, &key).await?;
            let mut record = if let Some(mut record) = old {
                if definition.max < record.desired {
                    return Err(DomainError::DesiredAboveMax {
                        desired: record.desired,
                        max: definition.max,
                    }
                    .into());
                }
                let previous = &record.definition;
                if (previous.swarm != definition.swarm
                    || previous.module != definition.module
                    || previous.size != definition.size
                    || previous.location != definition.location)
                    && (record.desired > 0 || record.applied > 0 || !record.settled())
                {
                    return Err(DomainError::ElasticGroupBusy {
                        group: definition.group.clone(),
                    }
                    .into());
                }
                record.definition = definition.clone();
                record
            } else {
                ElasticGroupRecord {
                    definition: definition.clone(),
                    desired: 0,
                    desired_reason: "registered".into(),
                    generation: 0,
                    applied: 0,
                    reconciled_generation: 0,
                    updated_at: self.now(),
                }
            };
            record.updated_at = self.now();
            write(&trx, &key, &record)?;
            Ok(record)
        })
        .await
    }
    /// Read a registered group.
    /// # Errors
    /// Returns storage failures.
    pub async fn get_elastic_group(&self, group: &str) -> Result<Option<ElasticGroupRecord>> {
        self.transaction(|trx| async move { read(&trx, &self.keys().elastic_group(group)).await })
            .await
    }
    /// List every registered group in name order.
    /// # Errors
    /// Returns storage failures.
    pub async fn list_elastic_groups(&self) -> Result<Vec<ElasticGroupRecord>> {
        let space = self.keys().elastic_group_space();
        let (begin, end) = space.range();
        let mut records = Vec::new();
        for (key, value) in self.scan_all_pages(begin, end).await? {
            let (name,): (String,) = space.unpack(&key)?;
            let record: ElasticGroupRecord = swarmy_core::decode(&value)?;
            if record.definition.group != name {
                return Err(StoreError::Storage(StorageError::Corrupt));
            }
            records.push(record);
        }
        Ok(records)
    }
    /// Change desired count atomically, bumping generation only for a new value.
    /// # Errors
    /// Rejects an unknown group or a count above max.
    pub async fn set_elastic_desired(
        &self,
        group: &str,
        desired: u32,
        reason: &str,
    ) -> Result<ElasticGroupRecord> {
        self.transaction(|trx| async move {
            let key = self.keys().elastic_group(group);
            let Some(mut record): Option<ElasticGroupRecord> = read(&trx, &key).await? else {
                return Err(DomainError::ElasticGroupMissing {
                    group: group.into(),
                }
                .into());
            };
            if desired > record.definition.max {
                return Err(DomainError::DesiredAboveMax {
                    desired,
                    max: record.definition.max,
                }
                .into());
            }
            if record.desired != desired {
                record.desired = desired;
                record.desired_reason = reason.into();
                record.generation = record
                    .generation
                    .checked_add(1)
                    .ok_or(StorageError::Corrupt)?;
                record.updated_at = self.now();
                write(&trx, &key, &record)?;
            }
            Ok(record)
        })
        .await
    }
}

/// A per-swarm safety interlock, not a spending budget.
pub const MAX_ELASTIC_CREATIONS_PER_HOUR: u32 = 4;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreationGrant {
    pub granted: u32,
    pub used: u32,
    pub next_free_at: Option<jiff::Timestamp>,
}
impl Store {
    /// Read the creation window without reserving.
    /// # Errors
    /// Returns storage failures.
    pub async fn elastic_creation_slots(&self) -> Result<CreationGrant> {
        self.transaction(|trx| async move {
            let now = self.now();
            let times = live_window(
                read(&trx, &self.keys().elastic_creations())
                    .await?
                    .unwrap_or_default(),
                now,
            );
            Ok(grant(&times, 0))
        })
        .await
    }
    /// Reserve creation slots, including slots used by failed applies.
    /// # Errors
    /// Returns `ProvisionerLeaseMismatch` when the token is stale.
    pub async fn reserve_elastic_creations(
        &self,
        lease: &swarmy_core::Lease,
        wanted: u32,
    ) -> Result<CreationGrant> {
        self.transaction(|trx| async move {
            self.check_provisioner(&trx, lease).await?;
            let now = self.now();
            let key = self.keys().elastic_creations();
            let mut times = live_window(read(&trx, &key).await?.unwrap_or_default(), now);
            let count = wanted.min(
                MAX_ELASTIC_CREATIONS_PER_HOUR
                    .saturating_sub(u32::try_from(times.len()).unwrap_or(u32::MAX)),
            );
            times.extend(std::iter::repeat_n(now, count as usize));
            write(&trx, &key, &times)?;
            Ok(grant(&times, count))
        })
        .await
    }
    /// Record observed count without changing desired or generation.
    /// # Errors
    /// Returns `ProvisionerLeaseMismatch` or `ElasticGroupMissing`.
    pub async fn record_elastic_reconcile(
        &self,
        lease: &swarmy_core::Lease,
        group: &str,
        applied: u32,
        generation: u64,
        settled: bool,
    ) -> Result<ElasticGroupRecord> {
        self.transaction(|trx| async move {
            self.check_provisioner(&trx, lease).await?;
            let key = self.keys().elastic_group(group);
            let Some(mut record): Option<ElasticGroupRecord> = read(&trx, &key).await? else {
                return Err(DomainError::ElasticGroupMissing {
                    group: group.into(),
                }
                .into());
            };
            record.applied = applied;
            if settled && generation == record.generation {
                record.reconciled_generation = record.reconciled_generation.max(generation);
            }
            record.updated_at = self.now();
            write(&trx, &key, &record)?;
            Ok(record)
        })
        .await
    }
    async fn check_provisioner(
        &self,
        trx: &foundationdb::Transaction,
        lease: &swarmy_core::Lease,
    ) -> Result<()> {
        if !crate::singleton_lease::is_current(
            trx,
            &self.keys().provisioner_lease(),
            lease,
            self.now(),
        )
        .await?
        {
            return Err(crate::FenceError::ProvisionerLeaseMismatch.into());
        }
        Ok(())
    }
}
fn live_window(mut times: Vec<jiff::Timestamp>, now: jiff::Timestamp) -> Vec<jiff::Timestamp> {
    times.retain(|at| *at + CREATION_WINDOW > now);
    times
}
fn grant(times: &[jiff::Timestamp], granted: u32) -> CreationGrant {
    CreationGrant {
        granted,
        used: u32::try_from(times.len()).unwrap_or(u32::MAX),
        next_free_at: if times.len() >= MAX_ELASTIC_CREATIONS_PER_HOUR as usize {
            times.first().map(|at| *at + CREATION_WINDOW)
        } else {
            None
        },
    }
}
