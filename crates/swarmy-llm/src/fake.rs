//! Deterministic provider for control-plane tests without external quota.
use crate::{Delta, Error, Provider, ProviderStream, Request, Response, StopReason, TokenUsage};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use swarmy_core::Part;

#[derive(Clone, Debug, serde::Deserialize)]
pub struct FakeFailure {
    pub status: u16,
    pub message: String,
    #[serde(default)]
    pub retry_after_seconds: Option<u64>,
}

impl FakeFailure {
    #[must_use]
    pub fn error(&self) -> Error {
        Error::ProviderResponse {
            status: reqwest::StatusCode::from_u16(self.status)
                .unwrap_or(reqwest::StatusCode::INTERNAL_SERVER_ERROR),
            message: self.message.clone(),
            retry_after: self.retry_after_seconds.map(Duration::from_secs),
        }
    }
}

#[derive(Default)]
pub struct FakeProvider {
    pub latency: Duration,
    /// Turns are zero based and assigned when `request` is called.
    pub responses: BTreeMap<usize, Response>,
    pub failures: BTreeMap<usize, FakeFailure>,
    /// Tool calls take precedence over a response for the same turn.
    pub tool_calls: Option<BTreeMap<usize, Vec<Part>>>,
    calls: AtomicUsize,
    requests: std::sync::Mutex<Vec<Request>>,
}

impl FakeProvider {
    /// Returns requests captured before the provider scripted its responses.
    ///
    /// # Panics
    /// Panics if a prior test poisoned the request lock.
    #[must_use]
    pub fn requests(&self) -> Vec<Request> {
        self.requests
            .lock()
            .expect("fake request lock poisoned")
            .clone()
    }

    #[must_use]
    pub fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl Provider for FakeProvider {
    fn request(&self, request: Request) -> ProviderStream {
        self.requests
            .lock()
            .expect("fake request lock poisoned")
            .push(request);
        let turn = self.calls.fetch_add(1, Ordering::SeqCst);
        let response = self
            .tool_calls
            .as_ref()
            .and_then(|script| script.get(&turn))
            .map(|parts| Response {
                parts: parts.clone(),
                stop_reason: StopReason::ToolCalls,
                usage: TokenUsage::default(),
                quota_remaining: BTreeMap::new(),
                quota_resets: BTreeMap::new(),
            })
            .or_else(|| self.responses.get(&turn).cloned());
        let failure = self.failures.get(&turn).cloned();
        let latency = self.latency;
        Box::pin(async_stream::try_stream! {
            tokio::time::sleep(latency).await;
            if let Some(failure) = failure { Err(failure.error())?; }
            let response = response.ok_or(Error::UnscriptedTurn(turn))?;
            for (output_index, part) in response.parts.iter().enumerate() {
                yield Delta::PartDone { output_index, part: part.clone() };
            }
            yield Delta::Completed(response);
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn scripted_failure_precedes_a_successful_second_call() {
        let mut fake = FakeProvider::default();
        fake.failures.insert(
            0,
            FakeFailure {
                status: 429,
                message: "quota reached".into(),
                retry_after_seconds: Some(2),
            },
        );
        fake.responses.insert(
            1,
            Response {
                parts: vec![Part::Text {
                    text: "done".into(),
                }],
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::default(),
                quota_remaining: BTreeMap::new(),
                quota_resets: BTreeMap::new(),
            },
        );
        let request = Request {
            system_prompt: String::new(),
            messages: Vec::new(),
            tools: Vec::new(),
            settings: crate::GenerationSettings::default(),
        };
        let first = fake.request(request.clone()).next().await.unwrap();
        assert!(
            matches!(first, Err(Error::ProviderResponse { status, retry_after: Some(delay), .. })
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS && delay == Duration::from_secs(2))
        );
        assert!(fake.request(request).next().await.unwrap().is_ok());
        assert_eq!(fake.call_count(), 2);
    }
    #[tokio::test]
    async fn captures_image_inputs() {
        let fake = FakeProvider::default();
        let mut request = Request {
            system_prompt: String::new(),
            messages: Vec::new(),
            tools: Vec::new(),
            settings: crate::GenerationSettings::default(),
        };
        request.messages.push(swarmy_core::Message {
            id: swarmy_core::MessageId::from_ulid(ulid::Ulid::nil()),
            role: swarmy_core::MessageRole::User,
            parts: vec![Part::Image {
                media_type: "image/png".into(),
                bytes: vec![1, 2, 3],
                object_key: None,
                detail: None,
            }],
        });
        let _ = fake.request(request.clone()).next().await;
        assert_eq!(fake.requests(), vec![request]);
    }
}
