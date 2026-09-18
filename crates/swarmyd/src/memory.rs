use anyhow::{Context, Result, ensure};
use std::{collections::HashMap, sync::Arc};
use swarmy_core::{ManifestId, MemoryRequest, NodeId, Sandbox, VolumeId};
use swarmy_sandbox::{ExecRequest, RuncRuntime};
use swarmy_store::Store;
use tokio::sync::Mutex;

type CacheKey = (swarmy_core::AgentId, u64, ManifestId, u64, String, usize);

pub fn spawn(
    bus: swarmy_bus::Bus,
    store: Store,
    runtime: Arc<RuncRuntime>,
    node: NodeId,
) -> tokio::task::JoinHandle<Result<(), swarmy_bus::Error>> {
    let memory = Memory::new(store, runtime, node);
    tokio::spawn(async move {
        bus.serve_memory(node, |request| async {
            memory
                .read(request)
                .await
                .map_err(|error| error.to_string())
        })
        .await
    })
}

struct Memory {
    store: Store,
    runtime: Arc<RuncRuntime>,
    node: NodeId,
    cache: Mutex<HashMap<CacheKey, String>>,
}

impl Memory {
    fn new(store: Store, runtime: Arc<RuncRuntime>, node: NodeId) -> Self {
        Self {
            store,
            runtime,
            node,
            cache: Mutex::default(),
        }
    }

    async fn check(&self, request: &MemoryRequest) -> Result<()> {
        let placement = self
            .store
            .get_by_agent(request.agent_id)
            .await?
            .context("computer is not placed")?;
        ensure!(
            placement.node_id == self.node
                && placement.epoch == request.epoch
                && placement.expires_at > jiff::Timestamp::now(),
            "memory placement expired or changed"
        );
        Ok(())
    }

    async fn read(&self, request: MemoryRequest) -> Result<String> {
        self.check(&request).await?;
        let volume = self
            .store
            .get_volume(VolumeId::from_ulid(request.agent_id.as_ulid()))
            .await?;
        let Some(volume) = volume else {
            return Ok(String::new());
        };
        let fingerprint = self
            .runtime
            .memory_fingerprint(request.agent_id, &request.directory)
            .await?;
        let key = (
            request.agent_id,
            request.epoch,
            volume.head_manifest,
            fingerprint,
            request.directory.clone(),
            request.max_bytes,
        );
        if let Some(text) = self.cache.lock().await.get(&key) {
            return Ok(text.clone());
        }
        tracing::info!(agent_id = %request.agent_id, "reading agent memory with sandbox exec");
        let (exit, text, error) = crate::tools::exec(
            &self.runtime,
            &Sandbox {
                agent_id: request.agent_id,
            },
            ExecRequest {
                args: vec![
                    "/usr/bin/python3".into(),
                    "-c".into(),
                    include_str!("memory.py").into(),
                    request.directory.clone(),
                    request.max_bytes.to_string(),
                ],
                timeout_ms: 10_000,
                stdin: Vec::new(),
            },
        )
        .await?;
        ensure!(
            exit.exit_code == 0 && !exit.timed_out,
            "memory read failed: {error}"
        );
        self.check(&request).await?;
        // Do not cache an observation that overlapped a write or rebuild.
        if fingerprint
            == self
                .runtime
                .memory_fingerprint(request.agent_id, &request.directory)
                .await?
        {
            let mut cache = self.cache.lock().await;
            if cache.len() >= 128 {
                cache.clear();
            }
            cache.insert(key, text.clone());
        }
        Ok(text)
    }
}

#[cfg(test)]
mod tests {
    fn read(path: &std::path::Path, cap: usize) -> String {
        let output = std::process::Command::new("python3")
            .args(["-c", include_str!("memory.py")])
            .arg(path)
            .arg(cap.to_string())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    }

    #[test]
    fn memory_is_sorted_bounded_and_skips_non_files() {
        let dir = tempfile::tempdir().unwrap();
        assert!(read(&dir.path().join("missing"), 100).is_empty());
        std::fs::write(dir.path().join("z.txt"), "last").unwrap();
        std::fs::write(dir.path().join("a.txt"), "first 🌍").unwrap();
        std::fs::create_dir(dir.path().join("directory")).unwrap();
        std::os::unix::fs::symlink("a.txt", dir.path().join("link")).unwrap();
        let text = read(dir.path(), 100);
        assert!(text.find("first").unwrap() < text.find("last").unwrap());
        assert!(!text.contains("link"));
        assert!(!text.contains("directory"));
        for cap in [1, 16, 23] {
            let text = read(dir.path(), cap);
            let (body, _) = text.split_once("\n[Agent memory truncated").unwrap();
            assert!(body.len() <= cap);
        }
    }
}
