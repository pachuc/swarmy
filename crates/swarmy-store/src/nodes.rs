use crate::{Result, Store, check_limit, read, scan, write};
use swarmy_core::{NodeId, NodeRecord, decode};

impl Store {
    /// Register or refresh a node's advertised capacity and heartbeat.
    /// # Errors
    /// Returns storage or encoding errors.
    pub async fn put_node(&self, record: &NodeRecord) -> Result<()> {
        self.transaction(|trx| async move {
            let key = self.keys().node(record.node_id);
            // A delayed heartbeat must not move liveness backwards.
            if read::<NodeRecord>(&trx, &key)
                .await?
                .is_some_and(|old| old.last_heartbeat > record.last_heartbeat)
            {
                return Ok(());
            }
            write(&trx, &key, record)
        })
        .await
    }

    /// # Errors
    /// Returns storage or decoding errors.
    pub async fn get_node(&self, id: NodeId) -> Result<Option<NodeRecord>> {
        self.transaction(|trx| async move { read(&trx, &self.keys().node(id)).await })
            .await
    }

    /// Scan live nodes in id order, strictly after the supplied cursor.
    /// The limit counts examined records, including expired ones. The returned
    /// cursor advances over expired records so callers can scan every page.
    /// # Errors
    /// Returns invalid-limit, storage, or decoding errors.
    pub async fn scan_live_nodes(
        &self,
        after: Option<NodeId>,
        since: jiff::Timestamp,
        limit: usize,
    ) -> Result<(Vec<NodeRecord>, Option<NodeId>)> {
        check_limit(limit)?;
        self.transaction(|trx| async move {
            let (mut begin, end) = self.keys().node_space().range();
            if let Some(id) = after {
                begin = self.keys().node(id);
                begin = crate::next_cursor(&begin);
            }
            let mut live = Vec::new();
            let mut cursor = None;
            for (_, value) in scan(&trx, (begin, end), limit).await? {
                let record: NodeRecord = decode(&value)?;
                cursor = Some(record.node_id);
                if record.last_heartbeat >= since {
                    live.push(record);
                }
            }
            Ok((live, cursor))
        })
        .await
    }
}
