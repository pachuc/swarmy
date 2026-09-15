use crate::process::{Kind, Process};
use anyhow::{Context, Result, ensure};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use swarmy_core::{AgentId, Event, ManifestId, NodeId, SessionId, ToolResult, VolumeId};
use swarmy_sandbox::{
    BlockDevice, ExecOutput, ExecRequest, RuncRuntime, SandboxRuntime, SandboxSpec,
};
use swarmy_store::Store;
use swarmy_volume::server::ServerConfig;

pub async fn kill_mid_command(processes: &mut [Process], files: &Path) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            if let Ok(bundles) = std::fs::read_dir(files.join(".swarmy/node/bundles")) {
                for bundle in bundles.flatten() {
                    if std::fs::read(bundle.path().join("rootfs/root/swarmy-lines"))
                        .ok()
                        .as_deref()
                        == Some(b"swarmy\n")
                    {
                        let node = processes
                            .iter_mut()
                            .find(|process| process.kind == Kind::Node)
                            .context("node process missing")?;
                        node.restart().await?;
                        tracing::info!(
                            "killed swarmyd after the file write; waiting for one clean retry"
                        );
                        return Ok::<_, anyhow::Error>(());
                    }
                }
            }
            for process in &mut *processes {
                process.check()?;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .context("command did not reach its write before the kill deadline")?
}

pub async fn verify(
    store: &Store,
    events: &[Event],
    id: SessionId,
    image: ManifestId,
    files: &Path,
) -> Result<()> {
    let mut manifests = Vec::new();
    for event in events {
        if let Event::ToolCallCompleted {
            result: ToolResult::Completed {
                metadata, title, ..
            },
            ..
        } = event
            && title == "bash"
        {
            let manifest: ManifestId = serde_json::from_value(metadata["manifest_id"].clone())?;
            ensure!(
                manifest != image,
                "tool manifest still points to the base image"
            );
            manifests.push(manifest);
        }
    }
    let final_manifest = *manifests.last().context("bash completion missing")?;
    let record = store.get_sandbox(id).await?.context("sandbox missing")?;
    ensure!(
        record.manifest_id == final_manifest,
        "sandbox and log disagree"
    );
    ensure!(
        store
            .get_volume(record.volume_id)
            .await?
            .context("disk missing")?
            .head_manifest
            == final_manifest,
        "volume and log disagree"
    );
    let volume_id = VolumeId::from_ulid(ulid::Ulid::generate());
    store.create_volume(volume_id, final_manifest).await?;
    let objects = objects()?;
    let root = files.join(".swarmy/verify");
    let _cleanup = Cleanup(root.clone());
    let runtime = RuncRuntime::open(
        root,
        ServerConfig {
            directory: files.join(".swarmy/verify-volumes"),
            node: NodeId::from_ulid(ulid::Ulid::generate()),
            store: store.clone(),
            objects,
        },
    )
    .await?;
    let result = async {
        let sandbox = runtime
            .create(
                SandboxSpec {
                    agent_id: AgentId::from_ulid(ulid::Ulid::generate()),
                },
                BlockDevice { volume_id },
            )
            .await?;
        let (send, mut receive) = tokio::sync::mpsc::channel(16);
        let collect = async {
            let mut stdout = Vec::new();
            while let Some(output) = receive.recv().await {
                if let ExecOutput::Stdout(bytes) = output {
                    stdout.extend(bytes);
                }
            }
            stdout
        };
        let (exit, output) = tokio::join!(
            runtime.exec(
                &sandbox,
                ExecRequest {
                    args: vec!["/bin/cat".into(), "/root/swarmy-lines".into()],
                    timeout_ms: 30_000,
                },
                send
            ),
            collect
        );
        ensure!(exit?.exit_code == 0, "clone could not read the file");
        ensure!(
            output == b"swarmy\n".repeat(manifests.len()),
            "clone contains missing or repeated writes"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let cleanup = runtime.shutdown().await;
    result?;
    cleanup?;
    tracing::info!(session_id = %id, %final_manifest, "clone contains exactly the committed writes");
    Ok(())
}

pub struct Cleanup(pub PathBuf);
impl Drop for Cleanup {
    fn drop(&mut self) {
        cleanup(&self.0);
    }
}

/// Remove only containers and mounts owned by this isolated run, even after a
/// failed assertion or a killed daemon. The device is captured before unmount.
pub fn cleanup(root: &Path) {
    use std::process::{Command, Stdio};
    if let Ok(bundles) = std::fs::read_dir(root.join("bundles")) {
        for bundle in bundles.flatten() {
            let _ = Command::new("runc")
                .arg("--root")
                .arg(root.join("runc"))
                .args(["delete", "--force"])
                .arg(bundle.file_name())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
            let mount = bundle.path().join("rootfs");
            let source = Command::new("findmnt")
                .args(["--noheadings", "--output", "SOURCE", "--mountpoint"])
                .arg(&mount)
                .output()
                .ok()
                .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned());
            let _ = Command::new("umount")
                .arg(&mount)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
            if let Some(source) = source.filter(|source| {
                source
                    .strip_prefix("/dev/nbd")
                    .is_some_and(|suffix| suffix.parse::<u32>().is_ok())
            }) {
                let _ = Command::new("nbd-client")
                    .args(["-d", &source])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }
        }
    }
}

fn objects() -> Result<Arc<dyn object_store::ObjectStore>> {
    let settings = swarmy_config::Settings::load()?.settings;
    let objects = Arc::new(
        object_store::aws::AmazonS3Builder::new()
            .with_endpoint(&settings.s3_endpoint)
            .with_access_key_id(&settings.s3_access_key)
            .with_secret_access_key(&settings.s3_secret_key)
            .with_bucket_name(&settings.s3_bucket)
            .with_region(&settings.s3_region)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .build()?,
    );
    Ok(objects)
}
