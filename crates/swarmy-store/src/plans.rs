use crate::{Result, Store, StoreError, write};
use swarmy_core::{
    Event, Lease, RequestId, SessionId, ToolCallRecord, ToolResult, UpdatePlanArguments,
};

impl Store {
    pub(crate) fn session_plan_key(&self, id: SessionId) -> Vec<u8> {
        self.root
            .pack(&("session_plan", id.as_ulid().to_bytes().as_slice()))
    }

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
            return Err(StoreError::InvalidState);
        }
        let parsed = UpdatePlanArguments::parse(call.arguments.clone());
        let result = match &parsed {
            Ok(arguments) => ToolResult::Completed {
                title: "update_plan".into(),
                output: serde_json::to_string(&arguments.plan).map_err(|_| StoreError::Corrupt)?,
                metadata: std::collections::BTreeMap::new(),
            },
            Err(error) => ToolResult::Error {
                error: error.clone(),
            },
        };
        let seq = expected_head
            .checked_add(1)
            .ok_or(StoreError::SequenceOverflow)?;
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
                self.check_worker_lease(&trx, id, lease, jiff::Timestamp::now())
                    .await?;
                let mut session = self.session(&trx, id).await?;
                if session.head_seq != expected_head {
                    return Err(StoreError::StaleSequence {
                        expected: expected_head,
                        actual: session.head_seq,
                    });
                }
                if let Ok(arguments) = parsed {
                    write(&trx, &self.session_plan_key(id), &arguments.plan)?;
                }
                trx.set(&self.event_space(id).pack(&(seq,)), value);
                session.head_seq = seq;
                write(&trx, &self.session_key(id), &session)
            }
        })
        .await?;
        Ok(event)
    }
}
