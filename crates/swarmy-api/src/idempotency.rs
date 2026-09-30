//! Idempotency keys, checked once at the API boundary, and the one
//! replay path every mutating handler shares.
use super::{ApiFailure, ApiResult, AppState, storage};
use axum::{Json, http::StatusCode};

/// A client idempotency key, checked once at the API boundary.
pub(crate) struct IdempotencyKey(String);

impl IdempotencyKey {
    /// Accept a key of 1 to 256 bytes.
    ///
    /// # Errors
    ///
    /// `invalid_idempotency_key` (400) for an empty or longer key.
    pub(crate) fn parse(raw: &str) -> Result<Self, ApiFailure> {
        if raw.is_empty() || raw.len() > 256 {
            return Err(ApiFailure::new(
                StatusCode::BAD_REQUEST,
                "invalid_idempotency_key",
                "idempotency key must be 1 to 256 bytes",
            ));
        }
        Ok(Self(raw.into()))
    }

    /// The replay-row key: `{scope}:{key}`.
    #[must_use]
    pub(crate) fn scoped(&self, scope: &str) -> String {
        format!("{scope}:{}", self.0)
    }

    #[must_use]
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// The stored response for `key`, if any. The caller holds
/// `state.mutation_guard()`.
pub(crate) async fn replayed<T: serde::de::DeserializeOwned>(
    state: &AppState,
    key: &str,
) -> Result<Option<T>, ApiFailure> {
    if let Some(value) = state.store.api_replay(key).await.map_err(storage)? {
        return serde_json::from_value(value)
            .map(Some)
            .map_err(|cause| {
                ApiFailure::caused(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "corrupt_replay",
                    "stored replay response is corrupt",
                    &cause,
                )
            });
    }
    Ok(None)
}

/// Store `value` under `key` and return the stored JSON. The caller holds
/// `state.mutation_guard()`.
pub(crate) async fn record_replay<T: serde::Serialize>(
    state: &AppState,
    key: &str,
    value: &T,
) -> Result<serde_json::Value, ApiFailure> {
    let stored = serde_json::to_value(value).map_err(|cause| {
        ApiFailure::caused(
            StatusCode::INTERNAL_SERVER_ERROR,
            "encoding_error",
            "response could not be encoded",
            &cause,
        )
    })?;
    state
        .store
        .put_api_replay(key, stored.clone())
        .await
        .map_err(storage)?;
    Ok(stored)
}

/// Run `operation` once per key, holding the mutation guard throughout.
pub(crate) async fn replay<T: serde::Serialize + serde::de::DeserializeOwned>(
    state: &AppState,
    key: &IdempotencyKey,
    scope: &str,
    operation: impl Future<Output = ApiResult<T>>,
) -> ApiResult<T> {
    let _guard = state.mutation_guard().await;
    let key = key.scoped(scope);
    if let Some(done) = replayed(state, &key).await? {
        return Ok(Json(done));
    }
    let Json(result) = operation.await?;
    record_replay(state, &key, &result).await?;
    Ok(Json(result))
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;

    #[test]
    fn parse_checks_the_key_once() {
        let key = IdempotencyKey::parse("abc").unwrap_or_else(|failure| {
            panic!("a short key parses, got {}", failure.body.code)
        });
        assert_eq!(key.as_str(), "abc");
        assert_eq!(key.scoped("gc:runs"), "gc:runs:abc");
        let longest = "k".repeat(256);
        assert_eq!(IdempotencyKey::parse(&longest).unwrap().as_str(), longest);
        let longer = "k".repeat(257);
        for raw in ["", longer.as_str()] {
            let failure = IdempotencyKey::parse(raw).unwrap_err();
            assert_eq!(failure.status, StatusCode::BAD_REQUEST);
            assert_eq!(failure.body.code, "invalid_idempotency_key");
        }
    }
}
