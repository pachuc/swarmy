//! Shared provider response errors with wire-specific body extraction.
use crate::Error;
use reqwest::StatusCode;
use serde_json::Value;

/// Extract the provider explanation while retaining a useful fallback for non-JSON bodies.
#[must_use]
pub fn provider_error(status: StatusCode, body: &str) -> Error {
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
pub fn message_error(message: String) -> Error {
    if crate::responses::is_context_overflow(&message) {
        Error::ContextOverflow(message)
    } else {
        Error::Protocol(message)
    }
}
