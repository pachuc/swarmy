//! Shared provider response errors with wire-specific body extraction.
use crate::Error;
use reqwest::StatusCode;
use serde_json::Value;

/// Extract the provider explanation while retaining a useful fallback for non-JSON bodies.
#[must_use]
pub(crate) fn provider_error(status: StatusCode, body: &str) -> Error {
    let message = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|value| value["error"]["message"].as_str().map(str::to_owned))
        .unwrap_or_else(|| body.trim().chars().take(600).collect());
    if message.is_empty() {
        Error::Status(status)
    } else {
        Error::Protocol(format!("provider error ({status}): {message}"))
    }
}

/// Classify a provider-specific extracted message, including context exhaustion.
#[must_use]
pub(crate) fn message_error(message: String) -> Error {
    if is_context_overflow(&message) {
        Error::ContextOverflow(message)
    } else {
        Error::Protocol(message)
    }
}

/// Map streamed provider error objects, including envelopes and bare errors.
pub(crate) fn stream_error(value: &Value) -> Error {
    let error = value.get("error").unwrap_or(value);
    let message = error["message"]
        .as_str()
        .or_else(|| error.as_str())
        .unwrap_or("provider returned an error");
    let code = error["code"]
        .as_str()
        .or_else(|| error["type"].as_str())
        .unwrap_or_default();
    message_error(if code.is_empty() {
        message.into()
    } else {
        format!("{code}: {message}")
    })
}

/// Vendor error codes and phrases used for context-window failures.
pub(crate) fn is_context_overflow(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    if ["rate limit", "rate_limit", "too many requests", "throttl"]
        .iter()
        .any(|phrase| message.contains(phrase))
    {
        return false;
    }
    [
        "context_length_exceeded",
        "context overflow",
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
        "input token count exceeds the maximum",
    ]
    .iter()
    .any(|phrase| message.contains(phrase))
}

/// Classify a failed HTTP request before retrying it.
pub(crate) fn classify_http_failure(
    status: StatusCode,
    body: &str,
    retry_after: Option<std::time::Duration>,
) -> Error {
    if status == StatusCode::PAYLOAD_TOO_LARGE || is_context_overflow(body) {
        return Error::ContextOverflow(body.to_owned());
    }
    // Subscription usage limits can be reported as 403, not only 429.
    if status == StatusCode::FORBIDDEN
        && crate::classify_provider_failure(body) == crate::ProviderFailureReason::Quota
    {
        return Error::ProviderResponse {
            status,
            reason: crate::ProviderFailureReason::Quota,
            message: body.to_owned(),
            retry_after,
        };
    }
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        return Error::Authentication(body.to_owned());
    }
    if status == StatusCode::BAD_REQUEST || status == StatusCode::NOT_FOUND {
        return Error::BadRequest(body.to_owned());
    }
    if crate::retry::retryable(status) {
        return Error::ProviderResponse {
            status,
            reason: crate::classify_provider_failure(body),
            message: body.to_owned(),
            retry_after,
        };
    }
    provider_error(status, body)
}

/// Pass a successful provider response through; turn a failed one into a
/// classified error that carries the server's Retry-After delay, so the
/// retry loop can honour it.
///
/// # Errors
/// Returns the classified HTTP failure, or the transport error from reading its body.
pub(crate) async fn check_response(
    response: reqwest::Response,
) -> Result<reqwest::Response, Error> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let retry_after = crate::retry::retry_after_header(response.headers());
    let body = response.text().await?;
    Err(classify_http_failure(status, &body, retry_after))
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    // Vendor phrases are classified below the public Error API; the rate-limit
    // exclusion below is the edge case the recorded streams cannot reach cheaply.
    #[test]
    fn context_overflow_classification() {
        assert!(super::is_context_overflow(
            "maximum context length exceeded"
        ));
        assert!(!super::is_context_overflow("rate limit: too many tokens"));
    }
    #[test]
    fn forbidden_usage_limit_is_retryable_but_bad_credentials_are_not() {
        let quota = super::classify_http_failure(
            reqwest::StatusCode::FORBIDDEN,
            "usage_limit_reached",
            None,
        );
        assert!(quota.classify().retryable);
        let auth =
            super::classify_http_failure(reqwest::StatusCode::FORBIDDEN, "invalid key", None);
        assert!(auth.classify().permanent);
    }
}
