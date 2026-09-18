use foundationdb::Transaction;
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use swarmy_core::{
    AgentId, NodeId, NodeRecord, NodeRole, PlacementChangeReason, PlacementRecord, decode,
};

use crate::{Result, Store, StoreError, check_limit, read, scan, write};

// Keep new metadata separate so existing postcard placement records and nested
// dispatch fences remain readable without changing their binary schema.
#[derive(Default, Serialize, Deserialize)]
pub(crate) struct PlacementHosting {
    claimed: Option<Timestamp>,
    last_renewed: Option<Timestamp>,
    pub(crate) failure_estimate: Option<Timestamp>,
}

impl Store {
    pub(crate) async fn read_placement_hosting(
        &self,
        trx: &Transaction,
        placement: &PlacementRecord,
    ) -> Result<Option<PlacementHosting>> {
        read(
            trx,
            &self.placement_key("placement_hosting", placement.agent_id),
        )
        .await
    }

    /// Read the failure estimate for this epoch. Legacy placements have no estimate.
    /// # Errors
    /// Rejects replaced placements and storage failures.
    pub async fn placement_failure_estimate(
        &self,
        expected: &PlacementRecord,
    ) -> Result<Option<Timestamp>> {
        self.transaction(|trx| async move {
            self.checked_placement(&trx, expected).await?;
            Ok(self
                .read_placement_hosting(&trx, expected)
                .await?
                .and_then(|hosting| hosting.failure_estimate))
        })
        .await
    }

    /// Claim a live epoch before creating its sandbox or executing any tools.
    /// Recording this before local work conservatively treats interrupted boots as
    /// possible computer loss, while grants that no node claimed remain silent.
    /// # Errors
    /// Rejects stale or expired placements and storage failures.
    pub async fn claim_placement(&self, expected: &PlacementRecord) -> Result<()> {
        self.transaction(|trx| async move {
            self.check_live_placement(&trx, expected).await?;
            let mut hosting = self
                .read_placement_hosting(&trx, expected)
                .await?
                .unwrap_or_default();
            hosting.claimed.get_or_insert_with(Timestamp::now);
            write(
                &trx,
                &self.placement_key("placement_hosting", expected.agent_id),
                &hosting,
            )
        })
        .await
    }

    fn placement_key(&self, kind: &str, agent: AgentId) -> Vec<u8> {
        self.root
            .pack(&(kind, agent.as_ulid().to_bytes().as_slice()))
    }

    fn placement_node_key(&self, node: NodeId, agent: AgentId) -> Vec<u8> {
        self.root.pack(&(
            "placement_by_node",
            node.as_ulid().to_bytes().as_slice(),
            agent.as_ulid().to_bytes().as_slice(),
        ))
    }

    fn placement_count_key(&self, node: NodeId) -> Vec<u8> {
        self.root
            .pack(&("placement_count", node.as_ulid().to_bytes().as_slice()))
    }

    async fn reserve_computer(&self, trx: &Transaction, node: NodeId) -> Result<()> {
        let registered: NodeRecord = read(trx, &self.node_key(node))
            .await?
            .ok_or(StoreError::NodeMissing)?;
        if !registered.roles.contains(&NodeRole::Sandbox) {
            return Err(StoreError::InvalidState);
        }
        let key = self.placement_count_key(node);
        let count: u32 = read(trx, &key).await?.unwrap_or(0);
        if count >= registered.capacity.sandboxes {
            return Err(StoreError::NodeAtCapacity);
        }
        write(trx, &key, &(count + 1))
    }

    async fn free_computer(&self, trx: &Transaction, node: NodeId) -> Result<()> {
        let key = self.placement_count_key(node);
        let count: u32 = read(trx, &key).await?.ok_or(StoreError::Corrupt)?;
        write(trx, &key, &count.checked_sub(1).ok_or(StoreError::Corrupt)?)
    }

    fn write_placement(&self, trx: &Transaction, record: &PlacementRecord) -> Result<()> {
        write(
            trx,
            &self.placement_key("placement", record.agent_id),
            record,
        )?;
        write(
            trx,
            &self.placement_node_key(record.node_id, record.agent_id),
            record,
        )?;
        write(
            trx,
            &self.placement_key("placement_epoch", record.agent_id),
            &record.epoch,
        )
    }

    pub(crate) async fn checked_placement(
        &self,
        trx: &Transaction,
        expected: &PlacementRecord,
    ) -> Result<PlacementRecord> {
        self.check_computer(trx, expected.agent_id).await?;
        let current: PlacementRecord =
            read(trx, &self.placement_key("placement", expected.agent_id))
                .await?
                .ok_or(StoreError::LeaseMismatch)?;
        if current.node_id != expected.node_id || current.epoch != expected.epoch {
            return Err(StoreError::LeaseMismatch);
        }
        Ok(current)
    }

    pub(crate) async fn check_live_placement(
        &self,
        trx: &Transaction,
        expected: &PlacementRecord,
    ) -> Result<()> {
        if self.checked_placement(trx, expected).await?.expires_at <= Timestamp::now() {
            return Err(StoreError::LeaseMismatch);
        }
        Ok(())
    }

    /// Validate the current epoch before local execution.
    /// # Errors
    /// Rejects stale or expired placements and storage failures.
    pub async fn validate_placement(&self, expected: &PlacementRecord) -> Result<()> {
        self.transaction(|trx| async move { self.check_live_placement(&trx, expected).await })
            .await
    }

    /// Place an absent computer, advancing its retained epoch counter.
    /// Checking absence and the counter in one transaction prevents duplicate grants.
    /// Recreating after release records Eviction; the first placement records Initial.
    /// Expired placements still consume capacity until released or taken over.
    /// # Errors
    /// Rejects existing placements, invalid expiry, missing or full nodes,
    /// nodes without the sandbox role, epoch overflow, and storage errors.
    pub async fn place(
        &self,
        agent: AgentId,
        node: NodeId,
        expires_at: Timestamp,
    ) -> Result<PlacementRecord> {
        self.transaction(|trx| async move {
            self.check_computer(&trx, agent).await?;
            let now = Timestamp::now();
            if expires_at <= now {
                return Err(StoreError::LeaseMismatch);
            }
            if read::<PlacementRecord>(&trx, &self.placement_key("placement", agent))
                .await?
                .is_some()
            {
                return Err(StoreError::PlacementExists);
            }
            let epoch: u64 = read(&trx, &self.placement_key("placement_epoch", agent))
                .await?
                .unwrap_or(0);
            self.reserve_computer(&trx, node).await?;
            let record = PlacementRecord {
                agent_id: agent,
                node_id: node,
                epoch: epoch.checked_add(1).ok_or(StoreError::SequenceOverflow)?,
                expires_at,
                last_change_reason: if epoch == 0 {
                    PlacementChangeReason::Initial
                } else {
                    PlacementChangeReason::Eviction
                },
                last_changed_at: now,
            };
            self.write_placement(&trx, &record)?;
            write(
                &trx,
                &self.placement_key("placement_hosting", agent),
                &PlacementHosting::default(),
            )?;
            Ok(record)
        })
        .await
    }

    /// Extend a live lease held by the supplied node and epoch. The stored expiry
    /// is authoritative, so an earlier renewal response remains a valid epoch token.
    /// The clock is checked again on every transaction retry.
    /// # Errors
    /// Rejects absent, stale, expired leases and non-increasing expiry times.
    pub async fn renew(
        &self,
        expected: &PlacementRecord,
        expires_at: Timestamp,
    ) -> Result<PlacementRecord> {
        self.transaction(|trx| async move {
            let mut current = self.checked_placement(&trx, expected).await?;
            let now = Timestamp::now();
            if current.expires_at <= now || expires_at <= current.expires_at {
                return Err(StoreError::LeaseMismatch);
            }
            let mut hosting = self
                .read_placement_hosting(&trx, &current)
                .await?
                // A legacy holder may already have a resident computer.
                .unwrap_or(PlacementHosting {
                    claimed: Some(now),
                    ..Default::default()
                });
            hosting.last_renewed = Some(now);
            write(
                &trx,
                &self.placement_key("placement_hosting", current.agent_id),
                &hosting,
            )?;
            current.expires_at = expires_at;
            self.write_placement(&trx, &current)?;
            Ok(current)
        })
        .await
    }

    /// Clear a live placement and its node index, returning its capacity.
    /// The epoch counter survives release so old tokens can never become valid again.
    /// # Errors
    /// Rejects absent, expired, or replaced leases and storage errors.
    pub async fn release(&self, expected: &PlacementRecord) -> Result<()> {
        self.transaction(|trx| async move {
            let current = self.checked_placement(&trx, expected).await?;
            if current.expires_at <= Timestamp::now() {
                return Err(StoreError::LeaseMismatch);
            }
            self.free_computer(&trx, current.node_id).await?;
            trx.clear(&self.placement_key("placement", current.agent_id));
            trx.clear(&self.placement_key("placement_hosting", current.agent_id));
            trx.clear(&self.placement_node_key(current.node_id, current.agent_id));
            Ok(())
        })
        .await
    }

    /// Replace the observed epoch only after expiry. Only claimed or legacy placements
    /// record Failure; an expired unclaimed grant records Unstarted.
    /// Competing takeovers conflict on the placement and only one can commit.
    /// # Errors
    /// Rejects live or replaced placements, invalid expiry, unavailable capacity,
    /// missing nodes, epoch overflow, and storage errors.
    pub async fn take_over(
        &self,
        expected: &PlacementRecord,
        node: NodeId,
        expires_at: Timestamp,
    ) -> Result<PlacementRecord> {
        self.transaction(|trx| async move {
            let current = self.checked_placement(&trx, expected).await?;
            let now = Timestamp::now();
            if current.expires_at > now || expires_at <= now {
                return Err(StoreError::LeaseMismatch);
            }
            self.free_computer(&trx, current.node_id).await?;
            self.reserve_computer(&trx, node).await?;
            let hosting = self.read_placement_hosting(&trx, &current).await?;
            // Missing metadata predates claim tracking, so do not assume that
            // an existing resident computer was never started.
            let lost_computer = hosting.as_ref().is_none_or(|h| h.claimed.is_some());
            let estimated_failure_at = hosting.and_then(|h| {
                h.claimed
                    .map(|claimed| h.last_renewed.unwrap_or(claimed).max(claimed))
            });
            let record = PlacementRecord {
                node_id: node,
                epoch: current
                    .epoch
                    .checked_add(1)
                    .ok_or(StoreError::SequenceOverflow)?,
                expires_at,
                last_change_reason: if lost_computer {
                    PlacementChangeReason::Failure
                } else {
                    PlacementChangeReason::Unstarted
                },
                last_changed_at: now,
                ..current
            };
            trx.clear(&self.placement_node_key(current.node_id, current.agent_id));
            self.write_placement(&trx, &record)?;
            write(
                &trx,
                &self.placement_key("placement_hosting", current.agent_id),
                &PlacementHosting {
                    failure_estimate: estimated_failure_at,
                    ..Default::default()
                },
            )?;
            Ok(record)
        })
        .await
    }

    /// Read placement metadata, including an expired lease. A read grants no authority.
    /// # Errors
    /// Returns storage or decoding errors.
    pub async fn get_by_agent(&self, agent: AgentId) -> Result<Option<PlacementRecord>> {
        self.transaction(
            |trx| async move { read(&trx, &self.placement_key("placement", agent)).await },
        )
        .await
    }

    /// List placements (including expired leases) by agent id, strictly after the cursor.
    /// Pages are independent transaction views; the final agent id is the next cursor.
    /// # Errors
    /// Rejects invalid limits and returns storage or decoding errors.
    pub async fn list_by_node(
        &self,
        node: NodeId,
        after: Option<AgentId>,
        limit: usize,
    ) -> Result<Vec<PlacementRecord>> {
        check_limit(limit)?;
        self.transaction(|trx| async move {
            let (mut begin, end) = self
                .root
                .subspace(&("placement_by_node", node.as_ulid().to_bytes().as_slice()))
                .range();
            if let Some(agent) = after {
                begin = self.placement_node_key(node, agent);
                begin.push(0);
            }
            scan(&trx, (begin, end), limit)
                .await?
                .into_iter()
                .map(|(_, value)| decode(&value).map_err(Into::into))
                .collect()
        })
        .await
    }
}

impl Store {
    pub(crate) async fn release_deleted_computer(
        &self,
        trx: &Transaction,
        agent: AgentId,
    ) -> Result<()> {
        if let Some(current) =
            read::<PlacementRecord>(trx, &self.placement_key("placement", agent)).await?
        {
            self.free_computer(trx, current.node_id).await?;
            trx.clear(&self.placement_key("placement", agent));
            trx.clear(&self.placement_key("placement_hosting", agent));
            trx.clear(&self.placement_node_key(current.node_id, agent));
        }
        Ok(())
    }
}
