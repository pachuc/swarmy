//! Service health is advisory; leases and epochs remain the authority for work.
use crate::{Result, Store, read, write};
use foundationdb::RangeOption;
use futures::TryStreamExt;
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use swarmy_core::{NodeRecord, decode};

/// A service is considered stale after 90 seconds without a heartbeat.
pub const SERVICE_STALE_SECONDS: i64 = 90;
/// Stale service rows are retained for ten minutes for diagnostics.
pub const SERVICE_EXPIRE_SECONDS: i64 = 600;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceRole {
    Scheduler,
    Worker,
    Gateway,
    Api,
    Node,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServiceDetail {
    Partitions(Vec<u16>),
    Providers(Vec<String>),
    Capacity(swarmy_core::NodeCapacity),
    None,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceHeartbeat {
    pub role: ServiceRole,
    pub instance_id: String,
    pub version: String,
    pub host: String,
    pub started_at: Timestamp,
    pub last_seen: Timestamp,
    pub detail: ServiceDetail,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceHealth {
    pub heartbeat: ServiceHeartbeat,
    pub alive: bool,
}

impl Store {
    fn service_key(&self, role: &ServiceRole, id: &str) -> Vec<u8> {
        self.root
            .pack(&("service_heartbeat", format!("{role:?}"), id))
    }

    /// Refresh an instance's health. Older delayed writes cannot replace newer health.
    /// # Errors
    /// Returns storage or encoding errors.
    pub async fn put_service_heartbeat(&self, record: &ServiceHeartbeat) -> Result<()> {
        self.transaction(|trx| async move {
            let key = self.service_key(&record.role, &record.instance_id);
            if read::<ServiceHeartbeat>(&trx, &key)
                .await?
                .is_some_and(|old| old.last_seen > record.last_seen)
            {
                return Ok(());
            }
            write(&trx, &key, record)
        })
        .await
    }

    /// List all service instances, including stale ones and existing node records.
    /// Liveness is evaluated at `now`; a heartbeat is alive through 90 seconds
    /// after its last observation. Node records predate service metadata, so their
    /// version and host are reported as unknown and their first observed time is
    /// used as started-at.
    /// # Errors
    /// Returns storage or decoding errors.
    pub async fn list_services(&self) -> Result<Vec<ServiceHealth>> {
        self.list_services_at(Timestamp::now()).await
    }

    /// Evaluate health at a supplied time, useful for deterministic monitoring tests.
    /// # Errors
    /// Returns storage or decoding errors.
    pub async fn list_services_at(&self, now: Timestamp) -> Result<Vec<ServiceHealth>> {
        let rows = self
            .transaction(|trx| async move {
                let mut result = Vec::new();
                for (space, node) in [("service_heartbeat", false), ("node", true)] {
                    let range = self.root.subspace(&(space,)).range();
                    let values: Vec<_> = trx
                        .get_ranges_keyvalues(RangeOption::from(range), false)
                        .map_ok(|kv| kv.value().to_vec())
                        .try_collect()
                        .await?;
                    for bytes in values {
                        let record = if node {
                            let n: NodeRecord = decode(&bytes)?;
                            ServiceHeartbeat {
                                role: ServiceRole::Node,
                                instance_id: n.node_id.to_string(),
                                version: "unknown".into(),
                                host: "unknown".into(),
                                started_at: n.last_heartbeat,
                                last_seen: n.last_heartbeat,
                                detail: ServiceDetail::Capacity(n.capacity),
                            }
                        } else {
                            decode(&bytes)?
                        };
                        result.push(record);
                    }
                }
                Ok(result)
            })
            .await?;
        let since = now
            .checked_sub(jiff::Span::new().seconds(SERVICE_STALE_SECONDS))
            .unwrap_or(Timestamp::MIN);
        Ok(rows
            .into_iter()
            .map(|heartbeat| ServiceHealth {
                alive: heartbeat.last_seen >= since,
                heartbeat,
            })
            .collect())
    }

    /// Remove non-node services last seen more than ten minutes ago.
    /// Node records are managed by the node lifecycle and are never deleted here.
    /// # Errors
    /// Returns storage or decoding errors.
    pub async fn expire_services(&self) -> Result<usize> {
        self.expire_services_at(Timestamp::now()).await
    }

    /// Expire at a supplied time for deterministic tests.
    /// # Errors
    /// Returns storage or decoding errors.
    pub async fn expire_services_at(&self, now: Timestamp) -> Result<usize> {
        let since = now
            .checked_sub(jiff::Span::new().seconds(SERVICE_EXPIRE_SECONDS))
            .unwrap_or(Timestamp::MIN);
        self.transaction(|trx| async move {
            let range = self.root.subspace(&("service_heartbeat",)).range();
            let values: Vec<_> = trx
                .get_ranges_keyvalues(RangeOption::from(range), false)
                .map_ok(|kv| (kv.key().to_vec(), kv.value().to_vec()))
                .try_collect()
                .await?;
            let mut count = 0;
            for (key, value) in values {
                let record: ServiceHeartbeat = decode(&value)?;
                if record.last_seen < since {
                    trx.clear(&key);
                    count += 1;
                }
            }
            Ok(count)
        })
        .await
    }
}
