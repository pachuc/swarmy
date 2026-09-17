//! Configure control-plane services without implicitly exporting provider credentials.
use std::{
    path::{Path, PathBuf},
    process::Stdio,
};

use anyhow::{Context, Result, ensure};
use swarmy_config::{RemoteNode, RemoteServices, Settings};
use tokio::io::AsyncWriteExt;

pub struct Options<'a> {
    pub recipe: Option<&'a Path>,
    pub credential: Option<PathBuf>,
    config: String,
    fake_script: Option<Vec<u8>>,
}

#[cfg(test)]
impl<'a> From<Option<&'a Path>> for Options<'a> {
    fn from(recipe: Option<&'a Path>) -> Self {
        Self {
            recipe,
            credential: None,
            config: String::new(),
            fake_script: None,
        }
    }
}

impl<'a> Options<'a> {
    pub fn new(settings: &Settings, copy: bool, recipe: Option<&'a Path>) -> Result<Self> {
        ensure!(
            !copy || settings.remote.services == RemoteServices::Node,
            "--copy-credential requires --services node (or remote.services = 'node')"
        );
        let credential = if copy {
            let path = PathBuf::from(&settings.credential_file);
            ensure!(
                path.is_file(),
                "configure credential_file before using --copy-credential"
            );
            Some(path)
        } else {
            None
        };
        if settings.remote.services == RemoteServices::Node {
            ensure!(
                settings.provider == "fake" || credential.is_some(),
                "node gateway requires --copy-credential for ChatGPT; this explicitly acknowledges the credential leaves the laptop"
            );
        }
        // Copy only service options. Local paths, cloud secrets, endpoints, and
        // the selected tunnel profile must never become node configuration.
        let remote = Settings {
            provider: settings.provider.clone(),
            model: settings.model.clone(),
            reasoning_effort: settings.reasoning_effort.clone(),
            system_prompt: settings.system_prompt.clone(),
            store_directory: settings.store_directory.clone(),
            bus_prefix: settings.bus_prefix.clone(),
            s3_prefix: settings.s3_prefix.clone(),
            credential_file: "/etc/swarmy/auth.json".into(),
            fake: swarmy_config::Fake {
                script: "/etc/swarmy/fake.json".into(),
                call_log: "/home/ubuntu/swarmy/.swarmy/calls.log".into(),
            },
            ..Settings::default()
        };
        let fake_script = if settings.remote.services == RemoteServices::Node
            && settings.provider == "fake"
        {
            Some(match std::fs::read(&settings.fake.script) {
                    Ok(script) => script,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound =>
                        br#"{"request_based":{"steps":1,"tool_steps":[],"final_answer":"Hello from swarmy!"}}"#.to_vec(),
                    Err(error) => return Err(error).context("read fake provider script for node gateway"),
                })
        } else {
            None
        };
        Ok(Self {
            recipe,
            credential,
            config: remote.to_toml()?,
            fake_script,
        })
    }
}

pub async fn install(node: &RemoteNode, address: &str, options: &Options<'_>) -> Result<()> {
    upload(
        node,
        address,
        "/home/ubuntu/swarmy/.swarmy/config.toml",
        options.config.as_bytes(),
    )
    .await?;
    if let Some(script) = &options.fake_script {
        upload(node, address, "/etc/swarmy/fake.json", script).await?;
    }
    if let Some(path) = &options.credential {
        eprintln!(
            "WARNING: --copy-credential sends your ChatGPT credential file to node {} over SSH. Its refresh credentials will leave this laptop; do not run another gateway refreshing the same account.",
            node.name
        );
        upload(
            node,
            address,
            "/etc/swarmy/auth.json",
            &std::fs::read(path)?,
        )
        .await?;
    }
    let status = super::ssh::command(node)?
        .arg(address)
        .arg("cd swarmy && bash scripts/remote-services.sh")
        .status()
        .await?;
    ensure!(status.success(), "start node services failed with {status}");
    Ok(())
}

async fn upload(node: &RemoteNode, address: &str, path: &str, bytes: &[u8]) -> Result<()> {
    // Data travels on stdin, never in a shell argument, diagnostic, or process listing.
    // The remote file is private from creation, including on interrupted writes.
    let script = format!(
        "umask 077; cat > {path}.tmp && chown ubuntu:ubuntu {path}.tmp && chmod 600 {path}.tmp && mv {path}.tmp {path}"
    );
    let mut child = super::ssh::command(node)?
        .arg(address)
        .arg(format!("sudo -n sh -c {}", shell_words::quote(&script)))
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()?;
    child
        .stdin
        .take()
        .context("SSH stdin missing")?
        .write_all(bytes)
        .await?;
    ensure!(
        child.wait().await?.success(),
        "copy node service file failed"
    );
    Ok(())
}
