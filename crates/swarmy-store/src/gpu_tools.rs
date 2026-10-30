//! Agent GPU changes and their tool results commit under the worker's lease and head fence.
use crate::{Result, Store, StoreError};
use foundationdb::Transaction;
use jiff::Timestamp;
use swarmy_core::{
    AgentId, Event, GPU_ALREADY_RELEASED, GPU_RELEASED, GPU_REQUEST_NEEDS_AGENT,
    GPU_REQUEST_NOT_ALLOWED, GpuRequest, GpuRequirement, GpuToolCall, Lease, RequestId, SessionId,
    ToolCallRecord, ToolResult, gpu_already, gpu_requested, is_gpu_tool,
};

impl Store {
    /// Apply a GPU tool and append its result in one leased transaction.
    /// # Errors
    /// Rejects non-GPU names, stale heads or leases, deleted computers and storage failures.
    pub async fn complete_gpu_tool(
        &self,
        id: SessionId,
        expected_head: u64,
        lease: &Lease,
        request_id: RequestId,
        call: &ToolCallRecord,
    ) -> Result<Event> {
        if !is_gpu_tool(&call.tool) {
            return Err(StoreError::Domain(crate::DomainError::InvalidToolCall(
                "expected request_gpu or release_gpu".into(),
            )));
        }
        let parsed = GpuToolCall::parse(&call.tool, call.arguments.clone());
        let now = self.now();
        self.transaction(|trx| {
            let parsed = parsed.clone();
            async move {
                self.check_worker_lease(&trx, id, lease, self.now()).await?;
                let mut session = self.session(&trx, id).await?;
                crate::check_head(session.head_seq, expected_head)?;
                self.check_computer(&trx, session.agent_id).await?;
                let result = match parsed {
                    Ok(change) => {
                        self.gpu_tool_in(&trx, session.agent_id, id, change, now)
                            .await?
                    }
                    Err(error) => Err(error),
                };
                let result = match result {
                    Ok(output) => ToolResult::Completed {
                        title: call.tool.clone(),
                        output,
                        metadata: std::collections::BTreeMap::new(),
                    },
                    Err(error) => ToolResult::Error { error },
                };
                let seq = crate::seq_after(expected_head, 1)?;
                let event = Event::ToolCallCompleted {
                    seq,
                    request_id,
                    call_id: call.call_id.clone(),
                    result,
                };
                let value = self.prepare(&event).await?;
                trx.set(&self.keys().event(id, seq), &value);
                session.head_seq = seq;
                self.write_session(&trx, &session)?;
                Ok(event)
            }
        })
        .await
    }

    async fn gpu_tool_in(
        &self,
        trx: &Transaction,
        agent_id: AgentId,
        session_id: SessionId,
        change: GpuToolCall,
        now: Timestamp,
    ) -> Result<std::result::Result<String, String>> {
        let Some(mut agent) = self.read_agent(trx, agent_id).await? else {
            return Ok(Err(GPU_REQUEST_NEEDS_AGENT.into()));
        };
        if !agent.allow_gpu_request {
            return Ok(Err(GPU_REQUEST_NOT_ALLOWED.into()));
        }
        let previous = agent.requirements;
        let output = match change {
            GpuToolCall::Request { mode, .. } if GpuRequirement::from(mode) == previous.gpu => {
                return Ok(Ok(gpu_already(mode.into())));
            }
            GpuToolCall::Request { mode, reason } => {
                agent.requirements.gpu = mode.into();
                agent.gpu_request = Some(GpuRequest {
                    reason,
                    requested_at: now,
                    session_id: Some(session_id),
                });
                gpu_requested(mode)
            }
            GpuToolCall::Release if previous.gpu == GpuRequirement::None => {
                return Ok(Ok(GPU_ALREADY_RELEASED.into()));
            }
            GpuToolCall::Release => {
                agent.requirements.gpu = GpuRequirement::None;
                agent.gpu_request = None;
                GPU_RELEASED.into()
            }
        };
        self.write_agent_requirements_in(trx, &agent, previous)
            .await?;
        Ok(Ok(output))
    }
}
