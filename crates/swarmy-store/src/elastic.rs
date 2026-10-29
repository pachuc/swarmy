//! Runtime truth for elastic machine groups.
use crate::{DomainError, Result, StorageError, Store, StoreError, read, write};
use swarmy_core::{ElasticGroupDefinition, ElasticGroupRecord, MAX_ELASTIC_MACHINES};

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
                    && (record.desired > 0 || record.applied > 0)
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
