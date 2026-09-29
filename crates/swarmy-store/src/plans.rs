use crate::{Result, Store, StoreError};
use swarmy_core::{
    Event, Lease, RequestId, SessionId, ToolCallRecord, ToolResult, UpdatePlanArguments,
};

impl Store {
    /// Replace a session plan and append its tool result in one leased transaction.
    /// The head fence prevents a retried completion from overwriting a newer plan.
    /// Invalid arguments produce an error result without changing the plan.
    /// # Errors
    /// Rejects a stale head or lease, wrong tool name, or a storage failure.
    pub async fn complete_plan_tool(
        &self,
        id: SessionId,
        expected_head: u64,
        lease: &Lease,
        request_id: RequestId,
        call: &ToolCallRecord,
    ) -> Result<Event> {
        if call.tool != "update_plan" {
            return Err(StoreError::Domain(crate::DomainError::InvalidToolCall));
        }
        let parsed = UpdatePlanArguments::parse(call.arguments.clone());
        let result = match &parsed {
            Ok(arguments) => ToolResult::Completed {
                title: "update_plan".into(),
                output: serde_json::to_string(&arguments.plan)
                    .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?,
                metadata: std::collections::BTreeMap::new(),
            },
            Err(error) => ToolResult::Error {
                error: error.clone(),
            },
        };
        let seq = expected_head
            .checked_add(1)
            .ok_or(StoreError::Storage(crate::StorageError::SequenceOverflow))?;
        let event = Event::ToolCallCompleted {
            seq,
            request_id,
            call_id: call.call_id.clone(),
            result,
        };
        let value = self.prepare(&event).await?;
        self.transaction(|trx| {
            let value = &value;
            let parsed = &parsed;
            async move {
                self.check_worker_lease(&trx, id, lease, self.now()).await?;
                let mut session = self.session(&trx, id).await?;
                crate::check_head(session.head_seq, expected_head)?;
                if let Ok(arguments) = parsed {
                    session.plan.clone_from(&arguments.plan);
                }
                trx.set(&self.event_key(id, seq), value);
                session.head_seq = seq;
                self.write_session(&trx, &session)
            }
        })
        .await?;
        Ok(event)
    }
}
