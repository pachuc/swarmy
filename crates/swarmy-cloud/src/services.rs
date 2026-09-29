//! Configure control-plane services without implicitly exporting provider credentials.
use std::{
    path::{Path, PathBuf},
    process::Stdio,
};

use crate::Result;
use swarmy_config::{RemoteNode, RemoteServices, Settings};
use tokio::io::AsyncWriteExt;

pub struct Options<'a> {
    pub recipe: Option<&'a Path>,
    pub credential: Option<PathBuf>,
    pub keyring: Option<PathBuf>,
    config: String,
    fake_script: Option<Vec<u8>>,
}

#[cfg(test)]
impl<'a> From<Option<&'a Path>> for Options<'a> {
    fn from(recipe: Option<&'a Path>) -> Self {
        Self {
            recipe,
            credential: None,
            keyring: None,
            config: String::new(),
            fake_script: None,
        }
    }
}

impl<'a> Options<'a> {
    /// Resolve service installation options, loading secrets only when the
    /// caller explicitly acknowledged copying them to the node.
    ///
    /// # Errors
    ///
    /// Rejects credential copying without node services, and reports missing
    /// credential files and unreadable keyrings.
    pub fn new(settings: &Settings, copy: bool, recipe: Option<&'a Path>) -> Result<Self> {
        let keyring = if copy {
            Some(swarmy_config::Keyring::path()?)
        } else {
            None
        };
        Self::with_keyring(settings, copy, recipe, keyring)
    }

    pub(super) fn with_keyring(
        settings: &Settings,
        copy: bool,
        recipe: Option<&'a Path>,
        keyring: Option<PathBuf>,
    ) -> Result<Self> {
        crate::Error::ensure(
            !copy || settings.remote.services == RemoteServices::Node,
            "--copy-credential requires --services node (or remote.services = 'node')",
        )?;
        if let Some(path) = &keyring {
            swarmy_config::Keyring::read(path)?;
        }
        let credential = if copy {
            let path = settings.selection.credential_file.clone();
            crate::Error::ensure(
                path.is_file(),
                "configure credential_file before using --copy-credential",
            )?;
            Some(path)
        } else {
            None
        };
        crate::Error::ensure(
            settings.remote.services != RemoteServices::Node
                || settings.selection.provider == "fake"
                || credential.is_some(),
            "node gateway requires --copy-credential for ChatGPT; this explicitly acknowledges the credential leaves the laptop",
        )?;
        // Copy only service options. Local paths, cloud secrets, endpoints, and
        // the selected tunnel profile must never become node configuration.
        swarmy_config::validate_service_user(&settings.remote.service_user)?;
        let mut remote = Settings {
            api: settings.api.clone(),
            selection: swarmy_config::SelectionSettings {
                provider: settings.selection.provider.clone(),
                model: settings.selection.model.clone(),
                effort: settings.selection.effort,
                credential_file: "/etc/swarmy/auth.json".into(),
                ..swarmy_config::SelectionSettings::default()
            },
            context: swarmy_config::ContextSettings {
                system_prompt: settings.context.system_prompt.clone(),
                ..swarmy_config::ContextSettings::default()
            },
            store: swarmy_config::StoreSettings {
                directory: settings.store.directory.clone(),
                ..Default::default()
            },
            bus: swarmy_config::BusSettings {
                prefix: settings.bus.prefix.clone(),
                ..swarmy_config::BusSettings::default()
            },
            s3: swarmy_config::S3Settings {
                prefix: settings.s3.prefix.clone(),
                ..swarmy_config::S3Settings::default()
            },
            fake: swarmy_config::Fake {
                script: "/etc/swarmy/fake.json".into(),
                // Relative to the gateway unit's working directory, which the
                // host resolves to the service checkout. The laptop cannot
                // render an absolute node path: the service home comes from
                // the host's passwd entry, so a laptop-side `/home/<user>`
                // guess would point at the wrong checkout whenever the laptop
                // login differs from the node's service user.
                call_log: ".swarmy/calls.log".into(),
            },
            ..Settings::default()
        };
        if settings.remote.services == RemoteServices::Node && remote.api.token.is_empty() {
            // Control nodes serve the API, which rejects every request while its
            // token is empty. Provision a token here with the same generator
            // `dev up` uses. Only fill when empty so a reinstall never rotates
            // an existing token out from under connected profiles.
            remote.api.token = ulid::Ulid::generate().to_string();
        }
        let fake_script = if settings.remote.services == RemoteServices::Node
            && settings.selection.provider == "fake"
        {
            Some(match std::fs::read(&settings.fake.script) {
                    Ok(script) => script,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound =>
                        br#"{"request_based":{"steps":1,"tool_steps":[],"final_answer":"Hello from swarmy!"}}"#.to_vec(),
                    Err(error) => return Err(error.into()),
                })
        } else {
            None
        };
        Ok(Self {
            recipe,
            credential,
            keyring,
            config: remote.to_toml()?,
            fake_script,
        })
    }

    /// The TOML uploaded to the node's `.swarmy/config.toml`.
    #[cfg(all(test, feature = "remote"))]
    pub(crate) fn config_toml(&self) -> &str {
        &self.config
    }
}

pub(crate) async fn install(node: &RemoteNode, address: &str, options: &Options<'_>) -> Result<()> {
    let user = node.service_user().to_owned();
    swarmy_config::validate_service_user(&user)?;
    // Bash resolves `~user` through the passwd entry; Rust passes only the login.
    let repo = format!("~{user}/swarmy");
    let home = format!("~{user}");
    upload(
        node,
        address,
        &format!("{repo}/.swarmy/config.toml"),
        options.config.as_bytes(),
    )
    .await?;
    if let Some(script) = &options.fake_script {
        upload(node, address, "/etc/swarmy/fake.json", script).await?;
    }
    if let Some(path) = &options.credential {
        cloud_err!(
            "WARNING: --copy-credential sends your ChatGPT credential file and cluster keyring to node {} over SSH. Legacy file credentials share one refresh chain; import into the cluster before running multiple gateways.",
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
    if let Some(path) = &options.keyring {
        swarmy_config::Keyring::read(path)?;
        upload(
            node,
            address,
            &format!("{home}/.swarmy/keyring"),
            &std::fs::read(path)?,
        )
        .await?;
    }
    let status = super::ssh::command(node)?
        .arg(address)
        .arg(format!(
            "cd {repo} && bash scripts/remote-services.sh {}",
            shell_words::quote(&user),
        ))
        .status()
        .await
        .map_err(crate::Error::ssh("start node services"))?;
    if !status.success() {
        return Err(crate::Error::SshStatus {
            command: "start node services".to_owned(),
            status,
        });
    }
    Ok(())
}

async fn upload(node: &RemoteNode, address: &str, path: &str, bytes: &[u8]) -> Result<()> {
    // Data travels on stdin, never in a shell argument, diagnostic, or process listing.
    // The remote file is private from creation, including on interrupted writes.
    let user = node.service_user();
    swarmy_config::validate_service_user(user)?;
    let script = format!(
        "umask 077; (test -d $(dirname {path}) || install -d -o {user} -g {user} -m 700 $(dirname {path})) && cat > {path}.tmp && chown {user}:{user} {path}.tmp && chmod 600 {path}.tmp && mv {path}.tmp {path}"
    );
    upload_with(node, address, &script, bytes).await
}

/// Upload bytes to a root-owned 0600 path over SSH stdin. Data travels on
/// stdin, never in a shell argument, diagnostic, or process listing.
pub(crate) async fn upload_root(
    node: &RemoteNode,
    address: &str,
    path: &str,
    bytes: &[u8],
) -> Result<()> {
    let script = format!(
        "umask 077; (test -d $(dirname {path}) || install -d -m 755 $(dirname {path})) && cat > {path}.tmp && chmod 600 {path}.tmp && mv {path}.tmp {path}"
    );
    upload_with(node, address, &script, bytes).await
}

/// Copy static S3 keys to the node when the remote uses them. Keys travel on
/// SSH stdin into a root-owned 0600 file; the provisioning script appends
/// them to the node environment without printing them.
pub(crate) async fn upload_bucket_keys(node: &RemoteNode, address: &str) -> Result<()> {
    let Some(spec) = node.bucket_spec() else {
        return Ok(());
    };
    let swarmy_config::BucketCredentials::StaticKeys {
        access_key,
        secret_key,
    } = &spec.credentials
    else {
        return Ok(());
    };
    // A newline would escape the env-file line format; reject it loudly
    // instead of writing a corrupt file.
    crate::Error::ensure(
        !access_key.contains('\n') && !secret_key.contains('\n'),
        "static S3 keys must not contain newlines",
    )?;
    let bytes = format!("SWARMY_S3_ACCESS_KEY={access_key}\nSWARMY_S3_SECRET_KEY={secret_key}\n");
    upload_root(node, address, "/etc/swarmy/s3-keys.env", bytes.as_bytes()).await
}

async fn upload_with(node: &RemoteNode, address: &str, script: &str, bytes: &[u8]) -> Result<()> {
    let command = "copy node service file".to_owned();
    let mut child = super::ssh::command(node)?
        .arg(address)
        .arg(format!("sudo -n sh -c {}", shell_words::quote(&script)))
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .map_err(crate::Error::ssh(&command))?;
    let Some(mut stdin) = child.stdin.take() else {
        return Err(crate::Error::other("SSH stdin missing"));
    };
    stdin
        .write_all(bytes)
        .await
        .map_err(crate::Error::ssh(&command))?;
    let status = child.wait().await.map_err(crate::Error::ssh(&command))?;
    if !status.success() {
        return Err(crate::Error::SshStatus { command, status });
    }
    Ok(())
}
