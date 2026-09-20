//! Bounded retries before streaming starts, shared by protocol clients.
use crate::Error;
use std::{future::Future, time::Duration};

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

/// Retry transient request failures. Callers must not retry a consumed stream.
/// # Errors
/// Returns the last error after exhaustion, or a permanent error immediately.
pub async fn with_retry<T, F, Fut>(policy: &RetryPolicy, mut f: F) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error>>,
{
    let mut delay = policy.initial_delay.min(policy.max_delay);
    for attempt in 0..=policy.max_retries {
        match f().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                let retry_after = match &error {
                    Error::Status(status) if retryable(*status) => Some(None),
                    Error::RetryableStatus {
                        status,
                        retry_after,
                    } if retryable(*status) => Some(*retry_after),
                    Error::Http(error) if error.is_connect() || error.is_timeout() => Some(None),
                    _ => None,
                };
                let Some(retry_after) = retry_after.filter(|_| attempt < policy.max_retries) else {
                    return Err(error);
                };
                tokio::time::sleep(retry_after.unwrap_or(delay).min(policy.max_delay)).await;
                delay = delay.saturating_mul(2).min(policy.max_delay);
            }
        }
    }
    unreachable!("every final attempt returns")
}

fn retryable(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 409 | 429 | 500..=599)
}

pub(crate) async fn check_response(
    response: reqwest::Response,
) -> Result<reqwest::Response, Error> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let retry_after = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(retry_after);
    let body = response.text().await?;
    if status == reqwest::StatusCode::PAYLOAD_TOO_LARGE || is_context_overflow(&body) {
        return Err(Error::ContextOverflow(body));
    }
    if retryable(status) {
        return Err(Error::RetryableStatus {
            status,
            retry_after,
        });
    }
    Err(Error::Status(status))
}

fn retry_after(value: &str) -> Option<Duration> {
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let date = jiff::fmt::strtime::parse("%a, %d %b %Y %H:%M:%S GMT", value).ok()?;
    let then = date
        .to_datetime()
        .ok()?
        .to_zoned(jiff::tz::TimeZone::UTC)
        .ok()?
        .timestamp();
    Some(Duration::from_secs(
        u64::try_from(then.as_second() - jiff::Timestamp::now().as_second()).unwrap_or(0),
    ))
}

/// Vendor error codes and phrases used for context-window failures.
#[must_use]
pub fn is_context_overflow(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    if ["rate limit", "rate_limit", "too many requests", "throttl"]
        .iter()
        .any(|phrase| message.contains(phrase))
    {
        return false;
    }
    [
        "context_length_exceeded",
        "context length exceeded",
        "maximum context length",
        "exceeds the context window",
        "maximum prompt length",
        "prompt is too long",
        "prompt too long",
        "input is too long",
        "request_too_large",
        "too many tokens",
        "token limit exceeded",
        "exceeded model token limit",
        "reduce the length of the messages",
        "exceeds the available context size",
        "greater than the context length",
        "maximum allowed input length",
    ]
    .iter()
    .any(|phrase| message.contains(phrase))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_after_accepts_seconds_and_http_dates() {
        assert_eq!(retry_after("12"), Some(Duration::from_secs(12)));
        assert_eq!(
            retry_after("Wed, 21 Oct 2015 07:28:00 GMT"),
            Some(Duration::ZERO)
        );
        assert!(retry_after("Wed, 21 Oct 2099 07:28:00 GMT").is_some());
        assert!(retry_after("invalid").is_none());
        assert!(!is_context_overflow("rate limit: too many tokens"));
    }
}
