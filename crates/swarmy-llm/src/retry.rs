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
    let mut attempt = 1;
    loop {
        let result = f().await;
        let retry_after = match &result {
            Err(error) => {
                let class = error.classify();
                if !class.retryable {
                    return result;
                }
                class.retry_after
            }
            _ => return result,
        };
        if attempt >= policy.max_attempts.max(1) {
            return result;
        }
        tokio::time::sleep(
            retry_after
                .unwrap_or_else(|| swarmy_core::backoff(policy.initial_delay, attempt, 5))
                .min(policy.max_delay),
        )
        .await;
        attempt += 1;
    }
}

pub(crate) fn retryable(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 409 | 429) || status.is_server_error()
}

/// Parse a Retry-After value as seconds or an HTTP date.
#[must_use]
pub(crate) fn retry_after(value: &str) -> Option<Duration> {
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let date = jiff::fmt::rfc2822::parse(value).ok()?.timestamp();
    let seconds = date
        .as_second()
        .saturating_sub(jiff::Timestamp::now().as_second());
    Some(Duration::from_secs(u64::try_from(seconds).unwrap_or(0)))
}

/// Read Retry-After from a response.
#[must_use]
pub(crate) fn retry_after_header(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()
        .and_then(retry_after)
}

#[cfg(test)]
mod header_tests {
    use super::*;
    #[test]
    fn seconds_dates_and_invalid_values() {
        assert_eq!(retry_after("12"), Some(Duration::from_secs(12)));
        assert_eq!(
            retry_after("Wed, 21 Oct 2015 07:28:00 GMT"),
            Some(Duration::ZERO)
        );
        assert!(retry_after("Wed, 21 Oct 2099 07:28:00 GMT").is_some());
        assert_eq!(retry_after("invalid"), None);
    }
}
