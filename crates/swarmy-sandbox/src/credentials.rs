//! Per-sandbox credential endpoint. The socket determines identity, never the request.
use std::{os::unix::fs::PermissionsExt, path::Path, time::Duration};
use swarmy_core::AgentId;
use swarmy_store::Store;
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
    pub(crate) fn start(path: &Path, store: Store, agent: AgentId) -> std::io::Result<Self> {
        let listener = UnixListener::bind(path)?;
        // The containing bundle is private to the node; only this agent sees
        // the bind mount. Both root and the agent user can ask for its token.
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o666))?;
        Ok(Self(tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                // Bound stalled clients without keeping credentials in a cache.
                let _ = tokio::time::timeout(Duration::from_secs(5), async {
                    let mut request = [0; 13];
                    socket.read_exact(&mut request).await?;
                    if &request != b"github-token\n" {
                        return Ok::<_, std::io::Error>(());
                    }
                    let response = match store.agent_github_token(agent).await {
                        Ok(Some(token)) => serde_json::json!({"token": token}),
                        // Do not log database errors alongside secret-bearing data.
                        _ => serde_json::json!({"error": "GitHub credential unavailable"}),
                    };
                    let mut bytes = serde_json::to_vec(&response)?;
                    bytes.push(b'\n');
                    socket.write_all(&bytes).await
                })
                .await;
            }
        })))
    }
}
