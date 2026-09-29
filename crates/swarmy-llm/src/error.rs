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

pub(crate) fn response_event_error(value: &Value) -> Error {
    let message = format!(
        "provider error ({}): {}",
        value["code"]
            .as_str()
            .or_else(|| value["type"].as_str())
            .unwrap_or("unknown"),
        value["message"].as_str().unwrap_or("request failed")
    );
    message_error(message)
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
