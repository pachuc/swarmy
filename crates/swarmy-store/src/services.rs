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

/// Identity and behaviour for [`Store::heartbeat_loop`]: what this instance
/// reports, and whether its tick also expires stale service rows. Only the
/// scheduler expires; the worker, API, and gateway only report themselves,
/// so a bare `bool` at the call site would hide which service cleans up.
#[derive(Clone, Debug)]
pub struct HeartbeatSpec {
    pub role: ServiceRole,
    pub instance_id: String,
    pub version: String,
    pub started_at: Timestamp,
    pub detail: ServiceDetail,
    pub expire_stale: bool,
}

impl Store {
    fn service_key(&self, role: &ServiceRole, id: &str) -> Vec<u8> {
        self.keys().service_heartbeat(&format!("{role:?}"), id)
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
    /// version and host are reported as unknown and their latest heartbeat is
    /// used as started-at because the original start time was not recorded.
    /// # Errors
    /// Returns storage or decoding errors.
    pub async fn list_services(&self) -> Result<Vec<ServiceHealth>> {
        self.list_services_at(self.now()).await
    }

    /// Evaluate health at a supplied time, useful for deterministic monitoring tests.
    /// # Errors
    /// Returns storage or decoding errors.
    pub async fn list_services_at(&self, now: Timestamp) -> Result<Vec<ServiceHealth>> {
        let rows = self
            .transaction(|trx| async move {
                let mut result = Vec::new();
                // Both key spaces hold heartbeat-shaped rows; node rows predate
                // service metadata and decode through the legacy record.
                let spaces = [
                    (self.keys().service_heartbeat_space().range(), false),
                    (self.keys().node_space().range(), true),
                ];
                for (range, node) in spaces {
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
        self.expire_services_at(self.now()).await
    }

    /// Expire at a supplied time for deterministic tests.
    /// # Errors
    /// Returns storage or decoding errors.
    pub async fn expire_services_at(&self, now: Timestamp) -> Result<usize> {
        let since = now
            .checked_sub(jiff::Span::new().seconds(SERVICE_EXPIRE_SECONDS))
            .unwrap_or(Timestamp::MIN);
        self.transaction(|trx| async move {
            let range = self.keys().service_heartbeat_space().range();
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

    /// Report this instance on the shared service tick until the process ends.
    /// Heartbeat and expiry failures only warn; the next tick retries. The
    /// worker, API, and scheduler share this instead of repeating the loop;
    /// the scheduler also expires stale rows on each tick.
    pub async fn heartbeat_loop(&self, spec: HeartbeatSpec) {
        let mut ticks = tokio::time::interval(swarmy_config::SERVICE_HEALTH_INTERVAL);
        loop {
            ticks.tick().await;
            let record = ServiceHeartbeat {
                role: spec.role.clone(),
                instance_id: spec.instance_id.clone(),
                version: spec.version.clone(),
                host: swarmy_config::service_hostname(),
                started_at: spec.started_at,
                last_seen: Timestamp::now(),
                detail: spec.detail.clone(),
            };
            if let Err(error) = self.put_service_heartbeat(&record).await {
                tracing::warn!(%error, role = ?spec.role, "service health heartbeat failed");
            }
            if spec.expire_stale
                && let Err(error) = self.expire_services().await
            {
                tracing::warn!(%error, "service health expiry failed");
            }
        }
    }
}
