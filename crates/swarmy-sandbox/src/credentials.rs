//! Per-sandbox credential endpoint. The socket determines identity, never the request.
use crate::SandboxMetadata;
use std::{os::unix::fs::PermissionsExt, path::Path, sync::Arc, time::Duration};
use swarmy_core::{AgentId, ignore_best_effort};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixListener,
    task::JoinHandle,
};

pub(crate) struct Credentials(JoinHandle<()>);

impl Drop for Credentials {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Credentials {
    pub(crate) fn start(
        path: &Path,
        metadata: Arc<dyn SandboxMetadata>,
        agent: AgentId,
    ) -> std::io::Result<Self> {
        let listener = UnixListener::bind(path)?;
        // The containing bundle is private to the node; only this agent sees
        // the bind mount. Both root and the agent user can ask for its token.
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o666))?;
        // The per-sandbox credential socket loop; drop aborts it.
        // ast-grep-ignore: no-spawn-in-libraries
        Ok(Self(tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                // Bound stalled clients without keeping credentials in a cache.
                // Log both timeouts and inner I/O failures; `timeout` nests
                // the inner result, so flatten it before the best-effort log.
                match tokio::time::timeout(Duration::from_secs(5), async {
                    let mut request = [0; 13];
                    socket.read_exact(&mut request).await?;
                    if &request != b"github-token\n" {
                        return Ok::<_, std::io::Error>(());
                    }
                    let response = match metadata.github_token(agent).await {
                        Ok(Some(token)) => serde_json::json!({"token": token}),
                        // Do not log database errors alongside secret-bearing data.
                        _ => serde_json::json!({"error": "GitHub credential unavailable"}),
                    };
                    let mut bytes = serde_json::to_vec(&response)?;
                    bytes.push(b'\n');
                    socket.write_all(&bytes).await
                })
                .await
                {
                    Err(error) => {
                        ignore_best_effort::<(), _>(Err(error), "answer credential request");
                    }
                    Ok(inner) => {
                        ignore_best_effort(inner, "answer credential request");
                    }
                }
            }
        })))
    }
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use swarmy_core::VolumeId;
    use swarmy_volume::{MetadataError, WriterGrant};
    use tokio::net::UnixStream;

    struct FakeMetadata {
        token: Result<Option<String>, MetadataError>,
    }

    #[async_trait::async_trait]
    impl SandboxMetadata for FakeMetadata {
        async fn open_writer(&self, _: VolumeId) -> Result<WriterGrant, MetadataError> {
            Err(MetadataError::VolumeMissing)
        }
        async fn github_token(&self, _: AgentId) -> Result<Option<String>, MetadataError> {
            match &self.token {
                Ok(token) => Ok(token.clone()),
                Err(_) => Err(MetadataError::Failed(Box::new(std::io::Error::other(
                    "lookup failed",
                )))),
            }
        }
    }

    async fn answer(token: Result<Option<String>, MetadataError>, request: &[u8]) -> Vec<u8> {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("github.sock");
        let _credentials = Credentials::start(
            &path,
            Arc::new(FakeMetadata { token }),
            AgentId::from_ulid(ulid::Ulid::generate()),
        )
        .unwrap();
        let mut socket = UnixStream::connect(path).await.unwrap();
        socket.write_all(request).await.unwrap();
        let mut response = Vec::new();
        socket.read_to_end(&mut response).await.unwrap();
        response
    }

    #[tokio::test]
    async fn socket_answers_the_agent_token() {
        assert_eq!(
            answer(Ok(Some("example-token".into())), b"github-token\n").await,
            b"{\"token\":\"example-token\"}\n"
        );
    }

    #[tokio::test]
    async fn socket_hides_lookup_failures() {
        assert_eq!(
            answer(Err(MetadataError::VolumeMissing), b"github-token\n").await,
            b"{\"error\":\"GitHub credential unavailable\"}\n"
        );
    }

    #[tokio::test]
    async fn socket_ignores_other_requests() {
        assert!(
            answer(Ok(Some("example-token".into())), b"other-request")
                .await
                .is_empty()
        );
    }
}
