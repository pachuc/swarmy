use crate::{
    BUILD_HEADER, DEFAULT_DEADLINE, Failure, NodeRefusal, PLACEMENTS_PATH, PlacementsResponse,
    REGISTER_PATH, RegisterRequest, RegisterResponse,
};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::time::Instant;
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_BACKOFF: Duration = Duration::from_secs(2);
const BUILD_RETRY: Duration = Duration::from_secs(5);
/// One HTTP/2 connection pool and one bearer identity. Never debug-print this value.
#[derive(Clone)]
pub struct NodeClient {
    http: reqwest::Client,
    base: String,
    bearer: String,
    node: swarmy_core::NodeId,
}
/// A final node API error.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("node API rejected this node's token")]
    Unauthorized,
    #[error("node build {node:?} does not match node API build {server:?}")]
    BuildMismatch { server: String, node: String },
    #[error("node API refused the request: {0}")]
    Refused(NodeRefusal),
    #[error("node API rejected the request: {0}")]
    Rejected(String),
    #[error(
        "node API gave no answer for {path} before the deadline after {attempts} attempts; last: {last}"
    )]
    Deadline {
        path: &'static str,
        attempts: u32,
        last: String,
    },
    #[error("node API answer for {path} did not decode")]
    Decode {
        path: &'static str,
        #[source]
        source: serde_json::Error,
    },
    #[error("read node token file {path}")]
    TokenFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("node token file is malformed")]
    Token(#[source] swarmy_core::NodeTokenError),
    #[error("build node API HTTP client")]
    Http(#[source] reqwest::Error),
}
fn retry_delay(attempt: u32) -> Duration {
    swarmy_core::backoff(Duration::from_millis(100), attempt, 5).min(MAX_BACKOFF)
}
#[derive(Debug)]
enum Outcome {
    Success(Vec<u8>),
    Retry(String),
    BuildMismatch { server: String, node: String },
    Fail(ClientError),
}
fn classify(status: reqwest::StatusCode, body: &[u8]) -> Outcome {
    if status.is_success() {
        return Outcome::Success(body.to_vec());
    }
    if status == reqwest::StatusCode::UNAUTHORIZED {
        return Outcome::Fail(ClientError::Unauthorized);
    }
    if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
        return Outcome::Retry(format!("HTTP {status}"));
    }
    match serde_json::from_slice::<Failure>(body) {
        Ok(Failure::BuildMismatch { server, node }) => Outcome::BuildMismatch { server, node },
        Ok(Failure::Refused { refusal }) => Outcome::Fail(ClientError::Refused(refusal)),
        Ok(Failure::Rejected { message } | Failure::Internal { message }) => {
            Outcome::Fail(ClientError::Rejected(message))
        }
        Ok(Failure::Unavailable { message }) => Outcome::Retry(message),
        _ => Outcome::Fail(ClientError::Rejected(format!(
            "HTTP {status}: invalid failure body"
        ))),
    }
}
impl NodeClient {
    /// Build an HTTP/2 client for an API root without a trailing slash.
    /// # Errors
    /// HTTP client configuration failure.
    pub fn new(base: &str, token: &swarmy_core::NodeToken) -> Result<Self, ClientError> {
        let http = reqwest::Client::builder()
            .http2_prior_knowledge()
            .http2_keep_alive_interval(Duration::from_secs(10))
            .http2_keep_alive_timeout(Duration::from_secs(5))
            .http2_keep_alive_while_idle(true)
            .connect_timeout(Duration::from_secs(5))
            .build()
            .map_err(ClientError::Http)?;
        Ok(Self {
            http,
            base: base.trim_end_matches('/').into(),
            bearer: format!("Bearer {}", token.reveal()),
            node: token.node(),
        })
    }
    /// Read a token file, ignoring surrounding whitespace.
    /// # Errors
    /// File, token or HTTP client errors.
    pub fn from_token_file(base: &str, path: &Path) -> Result<Self, ClientError> {
        let text = std::fs::read_to_string(path).map_err(|source| ClientError::TokenFile {
            path: path.into(),
            source,
        })?;
        Self::new(
            base,
            &swarmy_core::NodeToken::parse(&text).map_err(ClientError::Token)?,
        )
    }
    #[must_use]
    pub const fn node(&self) -> swarmy_core::NodeId {
        self.node
    }
    /// Register this node.
    /// # Errors
    /// Final server answers and exhausted deadline.
    pub async fn register(
        &self,
        request: &RegisterRequest,
    ) -> Result<RegisterResponse, ClientError> {
        self.call(
            reqwest::Method::POST,
            REGISTER_PATH,
            Some(request),
            Instant::now() + DEFAULT_DEADLINE,
        )
        .await
    }
    /// Send one sync before the caller's earliest local lease deadline.
    /// # Errors
    /// As `register`.
    pub async fn sync(
        &self,
        request: &crate::SyncRequest,
        deadline: Instant,
    ) -> Result<crate::SyncResponse, ClientError> {
        self.call(
            reqwest::Method::POST,
            crate::SYNC_PATH,
            Some(request),
            deadline,
        )
        .await
    }
    /// Send route tool before the caller's deadline.
    /// # Errors
    /// As `register`.
    pub async fn route_tool(
        &self,
        request: &crate::ToolRouteRequest,
        deadline: Instant,
    ) -> Result<crate::ToolRouteResponse, ClientError> {
        self.call(
            reqwest::Method::POST,
            crate::TOOL_ROUTE_PATH,
            Some(request),
            deadline,
        )
        .await
    }
    /// Claim a tool before the caller's deadline.
    /// # Errors
    /// As `register`.
    pub async fn claim_tool(
        &self,
        request: &crate::ToolClaimRequest,
        deadline: Instant,
    ) -> Result<crate::ToolClaimResponse, ClientError> {
        self.call(
            reqwest::Method::POST,
            crate::TOOL_CLAIM_PATH,
            Some(request),
            deadline,
        )
        .await
    }
    /// Complete a tool before the caller's deadline.
    /// # Errors
    /// As `register`.
    pub async fn complete_tool(
        &self,
        request: &crate::ToolCompleteRequest,
        deadline: Instant,
    ) -> Result<crate::ToolCompleteResponse, ClientError> {
        self.call(
            reqwest::Method::POST,
            crate::TOOL_COMPLETE_PATH,
            Some(request),
            deadline,
        )
        .await
    }
    /// Send open hosting before the caller's deadline.
    /// # Errors
    /// As `register`.
    pub async fn open_hosting(
        &self,
        request: &crate::HostingOpenRequest,
        deadline: Instant,
    ) -> Result<crate::HostingOpenResponse, ClientError> {
        self.call(
            reqwest::Method::POST,
            crate::HOSTING_OPEN_PATH,
            Some(request),
            deadline,
        )
        .await
    }
    /// Send hosting ready before the caller's deadline.
    /// # Errors
    /// As `register`.
    pub async fn hosting_ready(
        &self,
        request: &crate::HostingReadyRequest,
        deadline: Instant,
    ) -> Result<crate::Ack, ClientError> {
        self.call(
            reqwest::Method::POST,
            crate::HOSTING_READY_PATH,
            Some(request),
            deadline,
        )
        .await
    }
    /// Send close hosting before the caller's deadline.
    /// # Errors
    /// As `register`.
    pub async fn close_hosting(
        &self,
        request: &crate::HostingCloseRequest,
        deadline: Instant,
    ) -> Result<crate::Ack, ClientError> {
        self.call(
            reqwest::Method::POST,
            crate::HOSTING_CLOSE_PATH,
            Some(request),
            deadline,
        )
        .await
    }
    /// Send publish volume before the caller's deadline.
    /// # Errors
    /// As `register`.
    pub async fn publish_volume(
        &self,
        request: &crate::VolumePublishRequest,
        deadline: Instant,
    ) -> Result<crate::Ack, ClientError> {
        self.call(
            reqwest::Method::POST,
            crate::VOLUME_PUBLISH_PATH,
            Some(request),
            deadline,
        )
        .await
    }
    /// Send github token before the caller's deadline.
    /// # Errors
    /// As `register`.
    pub async fn github_token(
        &self,
        request: &crate::GithubTokenRequest,
        deadline: Instant,
    ) -> Result<crate::GithubTokenResponse, ClientError> {
        self.call(
            reqwest::Method::POST,
            crate::GITHUB_TOKEN_PATH,
            Some(request),
            deadline,
        )
        .await
    }
    /// Protect chunks in bounded sequential requests, retaining deletion order.
    /// # Errors
    /// As `register`.
    pub async fn protect_chunks(
        &self,
        hashes: &[swarmy_core::ContentHash],
        deadline: Instant,
    ) -> Result<Vec<swarmy_core::ContentHash>, ClientError> {
        let mut deleting = Vec::new();
        for chunk in hashes.chunks(crate::MAX_PROTECT_HASHES) {
            let response: crate::VolumeProtectResponse = self
                .call(
                    reqwest::Method::POST,
                    crate::VOLUME_PROTECT_PATH,
                    Some(&crate::VolumeProtectRequest {
                        hashes: chunk.to_vec(),
                    }),
                    deadline,
                )
                .await?;
            deleting.extend(response.deleting);
        }
        Ok(deleting)
    }
    /// List this node's placements.
    /// # Errors
    /// Final server answers and exhausted deadline.
    pub async fn placements(&self) -> Result<PlacementsResponse, ClientError> {
        self.call::<(), _>(
            reqwest::Method::GET,
            PLACEMENTS_PATH,
            None,
            Instant::now() + DEFAULT_DEADLINE,
        )
        .await
    }
    async fn call<B: Serialize, R: DeserializeOwned>(
        &self,
        method: reqwest::Method,
        path: &'static str,
        body: Option<&B>,
        deadline: Instant,
    ) -> Result<R, ClientError> {
        let bytes = body
            .map(serde_json::to_vec)
            .transpose()
            .map_err(|source| ClientError::Decode { path, source })?;
        let start = Instant::now();
        let mut attempts = 0_u32;
        let result = loop {
            if Instant::now() >= deadline {
                break Err(ClientError::Deadline {
                    path,
                    attempts,
                    last: "deadline elapsed".into(),
                });
            }
            attempts += 1;
            let mut request = self
                .http
                .request(method.clone(), format!("{}{path}", self.base))
                .header(reqwest::header::AUTHORIZATION, &self.bearer)
                .header(BUILD_HEADER, swarmy_version::IDENTITY);
            if let Some(ref bytes) = bytes {
                request = request
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(bytes.clone());
            }
            let answer = tokio::time::timeout(
                ATTEMPT_TIMEOUT.min(deadline.saturating_duration_since(Instant::now())),
                async {
                    let response = request.send().await?;
                    let status = response.status();
                    let body = response.bytes().await?;
                    Ok::<_, reqwest::Error>((status, body))
                },
            )
            .await;
            let outcome = match answer {
                Ok(Ok((status, body))) => classify(status, &body),
                Ok(Err(error)) => Outcome::Retry(swarmy_core::error_chain(&error)),
                Err(_) => Outcome::Retry("attempt timed out".into()),
            };
            match outcome {
                Outcome::Success(body) => {
                    break serde_json::from_slice(&body)
                        .map_err(|source| ClientError::Decode { path, source });
                }
                Outcome::Fail(error) => break Err(error),
                Outcome::BuildMismatch { server, node } => {
                    tracing::warn!(server, node, path, "node API build differs; retrying");
                    if Instant::now() + BUILD_RETRY >= deadline {
                        break Err(ClientError::BuildMismatch { server, node });
                    }
                    tokio::time::sleep(BUILD_RETRY).await;
                }
                Outcome::Retry(last) => {
                    let delay = retry_delay(attempts);
                    if Instant::now() + delay >= deadline {
                        break Err(ClientError::Deadline {
                            path,
                            attempts,
                            last,
                        });
                    }
                    tokio::time::sleep(delay).await;
                }
            }
        };
        let outcome = match &result {
            Ok(_) => "ok",
            Err(ClientError::Unauthorized) => "unauthorized",
            Err(ClientError::BuildMismatch { .. }) => "build_mismatch",
            Err(ClientError::Refused(_)) => "refused",
            Err(ClientError::Deadline { .. }) => "deadline",
            Err(ClientError::Decode { .. }) => "decode",
            Err(_) => "rejected",
        };
        let elapsed_ms = start.elapsed().as_millis();
        tracing::info!(path, attempts, elapsed_ms, outcome, "node request");
        result
    }
}
#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    #[test]
    fn backoff_is_bounded() {
        for (attempt, expected) in [100, 200, 400, 800, 1600, 2000, 2000]
            .into_iter()
            .enumerate()
        {
            assert_eq!(
                retry_delay(u32::try_from(attempt + 1).unwrap()),
                Duration::from_millis(expected)
            );
        }
    }
    #[test]
    fn classify_answers() {
        use reqwest::StatusCode;
        assert!(matches!(
            classify(StatusCode::OK, b"{}"),
            Outcome::Success(_)
        ));
        assert!(matches!(
            classify(StatusCode::SERVICE_UNAVAILABLE, b""),
            Outcome::Retry(_)
        ));
        assert!(matches!(
            classify(StatusCode::UNAUTHORIZED, b""),
            Outcome::Fail(ClientError::Unauthorized)
        ));
        let cases = [
            (
                Failure::BuildMismatch {
                    server: "a".into(),
                    node: "b".into(),
                },
                StatusCode::CONFLICT,
            ),
            (
                Failure::Refused {
                    refusal: NodeRefusal::AlreadyDone,
                },
                StatusCode::CONFLICT,
            ),
            (
                Failure::Rejected {
                    message: "bad".into(),
                },
                StatusCode::BAD_REQUEST,
            ),
            (
                Failure::Internal {
                    message: "internal".into(),
                },
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
        ];
        for (failure, status) in cases {
            let outcome = classify(status, &serde_json::to_vec(&failure).unwrap());
            match failure {
                Failure::BuildMismatch { .. } => {
                    assert!(matches!(outcome, Outcome::BuildMismatch { .. }));
                }
                Failure::Refused { .. } => {
                    assert!(matches!(outcome, Outcome::Fail(ClientError::Refused(_))));
                }
                _ => assert!(matches!(outcome, Outcome::Fail(ClientError::Rejected(_)))),
            }
        }
        assert!(matches!(
            classify(StatusCode::BAD_GATEWAY, b"<html>broken</html>"),
            Outcome::Fail(ClientError::Rejected(_))
        ));
    }
    #[test]
    fn token_file() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let token = swarmy_core::NodeToken::new(
            swarmy_core::NodeId::from_ulid(ulid::Ulid::generate()),
            [7; 32],
        );
        std::fs::write(file.path(), format!("{}\n", token.reveal())).unwrap();
        assert_eq!(
            NodeClient::from_token_file("http://localhost", file.path())
                .unwrap()
                .node(),
            token.node()
        );
        std::fs::write(file.path(), "garbage").unwrap();
        assert!(matches!(
            NodeClient::from_token_file("http://localhost", file.path()),
            Err(ClientError::Token(_))
        ));
    }
}
