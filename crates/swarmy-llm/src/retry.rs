//! Bounded retries for requests that have not started emitting output.

use std::{future::Future, time::Duration};

use crate::Error;

#[derive(Clone, Debug)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(60),
        }
    }
}

/// Retry transient HTTP statuses, preferring the server's delay to backoff.
///
/// # Errors
/// Returns the final attempt's error, or a non-retryable error immediately.
pub async fn with_retry<T, F, Fut>(policy: &RetryPolicy, mut f: F) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error>>,
{
    let mut delay = policy.initial_delay.min(policy.max_delay);
    let mut attempt = 1;
    loop {
        let result = f().await;
        let retry_after = match &result {
            Err(Error::Retryable {
                status,
                retry_after,
            }) if retryable(*status) => *retry_after,
            Err(Error::Status(status)) if retryable(*status) => None,
            _ => return result,
        };
        if attempt >= policy.max_attempts.max(1) {
            return result;
        }
        tokio::time::sleep(retry_after.unwrap_or(delay).min(policy.max_delay)).await;
        delay = delay.saturating_mul(2).min(policy.max_delay);
        attempt += 1;
    }
}

pub(crate) fn retryable(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 409 | 429) || status.is_server_error()
}
