use swarmy_core::{AgentId, SessionId, UsageTotals};

use crate::{Result, Store, read};

impl Store {
    /// Read billed totals committed with successful inference completions.
    /// # Errors
    /// Returns database or decoding errors.
    pub async fn session_usage(&self, id: SessionId) -> Result<UsageTotals> {
        self.transaction(|trx| async move {
            Ok(read(
                &trx,
                &self
                    .root
                    .pack(&("usage", id.as_ulid().to_bytes().as_slice())),
            )
            .await?
            .unwrap_or_default())
        })
        .await
    }

    /// Read billed totals across all of an agent's sessions, including archives.
    /// # Errors
    /// Returns database or decoding errors.
    pub async fn agent_usage(&self, id: AgentId) -> Result<UsageTotals> {
        self.transaction(|trx| async move {
            Ok(read(
                &trx,
                &self
                    .root
                    .pack(&("usage_by_agent", id.as_ulid().to_bytes().as_slice())),
            )
            .await?
            .unwrap_or_default())
        })
        .await
    }
}

impl Store {
    pub(crate) async fn record_usage(
        &self,
        trx: &foundationdb::Transaction,
        session: SessionId,
        agent: AgentId,
        usage: &swarmy_core::TokenUsage,
        cost_micros: u64,
    ) -> Result<()> {
        let session_key = self
            .root
            .pack(&("usage", session.as_ulid().to_bytes().as_slice()));
        let agent_key = self
            .root
            .pack(&("usage_by_agent", agent.as_ulid().to_bytes().as_slice()));
        let (session, agent) = futures::try_join!(
            read::<UsageTotals>(trx, &session_key),
            read::<UsageTotals>(trx, &agent_key)
        )?;
        for (key, totals) in [(session_key, session), (agent_key, agent)] {
            let mut totals = totals.unwrap_or_default();
            totals.add(usage, cost_micros);
            crate::write(trx, &key, &totals)?;
        }
        Ok(())
    }
}
