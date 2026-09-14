//! Deterministic provider for control-plane tests without external quota.
use crate::{Delta, Error, Provider, ProviderStream, Request, Response, StopReason, TokenUsage};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use swarmy_core::Part;

#[derive(Default)]
pub struct FakeProvider {
    pub latency: Duration,
    /// Turns are zero based and assigned when `request` is called.
    pub responses: BTreeMap<usize, Response>,
    /// Tool calls take precedence over a response for the same turn.
    pub tool_calls: Option<BTreeMap<usize, Vec<Part>>>,
    calls: AtomicUsize,
}

impl FakeProvider {
    #[must_use]
    pub fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl Provider for FakeProvider {
    fn request(&self, _request: Request) -> ProviderStream {
        let turn = self.calls.fetch_add(1, Ordering::SeqCst);
        let response = self
            .tool_calls
            .as_ref()
            .and_then(|script| script.get(&turn))
            .map(|parts| Response {
                parts: parts.clone(),
                stop_reason: StopReason::ToolCalls,
                usage: TokenUsage::default(),
            })
            .or_else(|| self.responses.get(&turn).cloned());
        let latency = self.latency;
        Box::pin(async_stream::try_stream! {
            tokio::time::sleep(latency).await;
            let response = response.ok_or(Error::UnscriptedTurn(turn))?;
            for (output_index, part) in response.parts.iter().enumerate() {
                yield Delta::PartDone { output_index, part: part.clone() };
            }
            yield Delta::Completed(response);
        })
    }
}
