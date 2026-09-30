//! The one shape of an API error response.
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use swarmy_api_types as api;

/// An HTTP status plus the JSON `ApiError` body. Every handler error is one
/// of these; nothing else builds an `ApiError`.
#[derive(Debug)]
pub(crate) struct ApiFailure {
    pub(crate) status: StatusCode,
    pub(crate) body: api::ApiError,
}

impl ApiFailure {
    /// A failure the client can act on: a stable machine-readable `code`
    /// and a human `message` that says what was wrong.
    pub(crate) fn new(status: StatusCode, code: &str, message: impl Into<String>) -> Self {
        Self {
            status,
            body: api::ApiError {
                code: code.into(),
                message: message.into(),
                provider_text: None,
            },
        }
    }

    /// A failure with an underlying cause. The HTTP body cannot carry the
    /// source, so the full chain is logged here before answering.
    pub(crate) fn caused(
        status: StatusCode,
        code: &str,
        message: impl Into<String>,
        cause: &(dyn std::error::Error + 'static),
    ) -> Self {
        tracing::warn!(
            error = %swarmy_core::error_chain(cause),
            code,
            "request failed"
        );
        Self::new(status, code, message)
    }

    /// Attach the provider's original error text.
    #[must_use]
    pub(crate) fn with_provider_text(mut self, text: String) -> Self {
        self.body.provider_text = Some(text);
        self
    }
}

impl IntoResponse for ApiFailure {
    fn into_response(self) -> Response {
        (self.status, Json(self.body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;

    /// A failing pipe captures one log line so the test pins the cause
    /// chain in the log without touching the global subscriber.
    #[derive(Clone)]
    struct SharedBuffer(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for SharedBuffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .map_err(|_| std::io::Error::other("log buffer poisoned"))?
                .write(bytes)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for SharedBuffer {
        type Writer = Self;

        fn make_writer(&'writer self) -> Self::Writer {
            self.clone()
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("outer failed")]
    struct Outer {
        #[source]
        source: std::io::Error,
    }

    #[test]
    fn caused_logs_the_cause_chain_and_keeps_code_and_message() {
        let outer = Outer {
            source: std::io::Error::other("disk full"),
        };
        let buffer = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_writer(SharedBuffer(buffer.clone()))
            .finish();
        let failure = tracing::subscriber::with_default(subscriber, || {
            ApiFailure::caused(
                StatusCode::INTERNAL_SERVER_ERROR,
                "storage_error",
                "storage operation failed",
                &outer,
            )
        });
        assert_eq!(failure.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(failure.body.code, "storage_error");
        assert_eq!(failure.body.message, "storage operation failed");
        let logged = String::from_utf8(
            buffer
                .lock()
                .map(|guard| guard.clone())
                .unwrap_or_default(),
        )
        .unwrap_or_default();
        assert!(
            logged.contains("outer failed: disk full"),
            "log must carry the full cause chain, got: {logged}"
        );
    }
}
