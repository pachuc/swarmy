//! Retries before a provider begins streaming. Partial streams are never replayed.

use std::{future::Future, time::Duration};

use crate::Error;

#[derive(Clone, Debug)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(60),
        }
    }
}

/// Retry transient HTTP failures with capped exponential backoff.
///
/// # Errors
/// Returns the final failure when retries are exhausted, or immediately for a
/// non-retryable failure, including context overflow and protocol errors.
pub async fn with_retry<T, F, Fut>(policy: &RetryPolicy, mut f: F) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error>>,
{
    let mut delay = policy.initial_delay.min(policy.max_delay);
    let mut attempt = 0;
    loop {
        let error = match f().await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        let (status, retry_after) = match &error {
            Error::RetryableStatus {
                status,
                retry_after,
                ..
            } => (Some(*status), *retry_after),
            Error::Status(status) => (Some(*status), None),
            _ => (None, None),
        };
        if attempt >= policy.max_retries || !status.is_some_and(is_retryable) {
            return Err(match error {
                Error::RetryableStatus { source, .. } => *source,
                other => other,
            });
        }
        tokio::time::sleep(retry_after.unwrap_or(delay).min(policy.max_delay)).await;
        delay = delay.saturating_mul(2).min(policy.max_delay);
        attempt += 1;
    }
}

pub(crate) fn is_retryable(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 409 | 429) || status.is_server_error()
}

/// Classify the shared context-overflow phrases before deciding to retry.
pub(crate) fn provider_error(message: String) -> Error {
    let lower = message.to_ascii_lowercase();
    if [
        "maximum context length",
        "context_length_exceeded",
        "exceeds the context window",
    ]
    .iter()
    .any(|phrase| lower.contains(phrase))
    {
        Error::ContextOverflow(message)
    } else {
        Error::Protocol(message)
    }
}

pub(crate) fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let value = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let date = jiff::fmt::rfc2822::parse(value).ok()?.timestamp();
    let seconds = date
        .as_second()
        .saturating_sub(jiff::Timestamp::now().as_second());
    Some(Duration::from_secs(u64::try_from(seconds).unwrap_or(0)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::{
        StatusCode,
        header::{HeaderMap, HeaderValue, RETRY_AFTER},
    };

    #[test]
    fn retry_after_supports_seconds_http_dates_and_invalid_values() {
        let mut headers = HeaderMap::new();
        assert_eq!(retry_after(&headers), None);
        headers.insert(RETRY_AFTER, HeaderValue::from_static("12"));
        assert_eq!(retry_after(&headers), Some(Duration::from_secs(12)));
        headers.insert(
            RETRY_AFTER,
            HeaderValue::from_static("Sun, 06 Nov 1994 08:49:37 GMT"),
        );
        assert_eq!(retry_after(&headers), Some(Duration::ZERO));
        headers.insert(RETRY_AFTER, HeaderValue::from_static("invalid"));
        assert_eq!(retry_after(&headers), None);
    }

    #[tokio::test]
    async fn retry_policy_bounds_attempts_and_rejects_permanent_errors() {
        let policy = RetryPolicy {
            max_retries: 2,
            initial_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
        };
        let mut attempts = 0;
        let error = with_retry(&policy, || {
            attempts += 1;
            std::future::ready(Err::<(), _>(Error::Status(StatusCode::SERVICE_UNAVAILABLE)))
        })
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            Error::Status(StatusCode::SERVICE_UNAVAILABLE)
        ));
        assert_eq!(attempts, 3);
        attempts = 0;
        let error = with_retry(&policy, || {
            attempts += 1;
            std::future::ready(Err::<(), _>(Error::Status(StatusCode::UNAUTHORIZED)))
        })
        .await
        .unwrap_err();
        assert!(matches!(error, Error::Status(StatusCode::UNAUTHORIZED)));
        assert_eq!(attempts, 1);
    }
}
