use crate::{Error, ModuleOutputs, Result, opentofu, parse_outputs};
use std::{
    fmt,
    path::{Component, Path, PathBuf},
    process::Stdio,
};
use tokio::{fs, process::Command};
pub const VARIABLES_FILE: &str = "swarmy.tfvars.json";
const PASSTHROUGH: &[&str] = &[
    "PATH",
    "HOME",
    "TMPDIR",
    "LANG",
    "LC_ALL",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "HTTPS_PROXY",
    "https_proxy",
    "HTTP_PROXY",
    "http_proxy",
    "NO_PROXY",
    "no_proxy",
];
const LOCK_TIMEOUT: &str = "-lock-timeout=120s";
const CLOSED_AWS: [(&str, &str); 3] = [
    ("AWS_SHARED_CREDENTIALS_FILE", "/dev/null"),
    ("AWS_CONFIG_FILE", "/dev/null"),
    ("AWS_EC2_METADATA_DISABLED", "true"),
];
#[derive(Clone, Default)]
pub struct CredentialEnv {
    vars: Vec<(String, String)>,
}
impl CredentialEnv {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    /// Keep the AWS provider off shared files and instance metadata.
    #[must_use]
    pub fn closed() -> Self {
        CLOSED_AWS
            .iter()
            .fold(Self::new(), |env, (name, value)| env.with(*name, *value))
    }
    #[must_use]
    pub fn with(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.vars.push((name.into(), value.into()));
        self
    }
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.vars.iter().map(|(name, _)| name.as_str())
    }
    fn found_in(&self, bytes: &[u8]) -> Option<&str> {
        self.vars
            .iter()
            .find(|(name, value)| {
                secret_name(name)
                    && !value.is_empty()
                    && bytes
                        .windows(value.len())
                        .any(|window| window == value.as_bytes())
            })
            .map(|(name, _)| name.as_str())
    }
    fn redact(&self, text: &str) -> String {
        self.vars
            .iter()
            .filter(|(name, value)| secret_name(name) && !value.is_empty())
            .fold(text.to_owned(), |text, (_, value)| {
                text.replace(value, "<redacted>")
            })
    }
}
fn secret_name(name: &str) -> bool {
    !CLOSED_AWS.iter().any(|(closed, _)| *closed == name)
}
impl fmt::Debug for CredentialEnv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CredentialEnv")
            .field("names", &self.names().collect::<Vec<_>>())
            .finish()
    }
}
pub struct ModuleFile<'a> {
    pub path: &'a str,
    pub contents: &'a [u8],
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanOutcome {
    NoChanges,
    Changes,
}
#[derive(Clone, Debug)]
pub struct Tofu {
    binary: PathBuf,
    plugin_cache: PathBuf,
}
impl Tofu {
    /// # Errors
    /// Returns installation or platform errors.
    pub async fn install(cache: &Path, source: &opentofu::Source) -> Result<Self> {
        Ok(Self {
            binary: opentofu::ensure(cache, source, opentofu::Platform::host()?).await?,
            plugin_cache: opentofu::plugin_cache_dir(cache),
        })
    }
    #[must_use]
    pub fn from_binary(binary: impl Into<PathBuf>, cache: &Path) -> Self {
        Self {
            binary: binary.into(),
            plugin_cache: opentofu::plugin_cache_dir(cache),
        }
    }
    #[must_use]
    pub fn binary(&self) -> &Path {
        &self.binary
    }
    #[must_use]
    pub fn plugin_cache(&self) -> &Path {
        &self.plugin_cache
    }
    #[must_use]
    pub fn workspace(&self, dir: impl Into<PathBuf>, credentials: CredentialEnv) -> Workspace<'_> {
        Workspace {
            tofu: self,
            dir: dir.into(),
            credentials,
        }
    }
}
#[derive(Debug)]
pub struct Workspace<'a> {
    tofu: &'a Tofu,
    dir: PathBuf,
    credentials: CredentialEnv,
}
fn io(action: &'static str, path: &Path, source: std::io::Error) -> Error {
    Error::Io {
        action,
        path: path.into(),
        source,
    }
}
impl Workspace<'_> {
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }
    /// Write module and non-secret variables after validating all input.
    /// # Errors
    /// Rejects unsafe paths, embedded credentials, and filesystem failures.
    pub async fn prepare(
        &self,
        module: &[ModuleFile<'_>],
        variables: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<()> {
        for file in module {
            let path = Path::new(file.path);
            if path.components().next().is_none()
                || !path
                    .components()
                    .all(|part| matches!(part, Component::Normal(_)))
                || file.path == VARIABLES_FILE
            {
                return Err(Error::ModulePath(file.path.into()));
            }
        }
        if let Some(name) = module
            .iter()
            .find_map(|file| self.credentials.found_in(file.contents))
        {
            return Err(Error::SecretInFiles { name: name.into() });
        }
        self.variable_bytes(variables)?;
        fs::create_dir_all(&self.dir)
            .await
            .map_err(|e| io("creating", &self.dir, e))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&self.dir, std::fs::Permissions::from_mode(0o700))
                .await
                .map_err(|e| io("securing", &self.dir, e))?;
        }
        let mut entries = fs::read_dir(&self.dir)
            .await
            .map_err(|e| io("reading", &self.dir, e))?;
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| io("reading", &self.dir, e))?
        {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".tf") || name.ends_with(".tf.json") {
                let path = entry.path();
                if entry
                    .file_type()
                    .await
                    .map_err(|e| io("reading", &path, e))?
                    .is_file()
                {
                    fs::remove_file(&path)
                        .await
                        .map_err(|e| io("removing", &path, e))?;
                }
            }
        }
        for file in module {
            let path = self.dir.join(file.path);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)
                    .await
                    .map_err(|e| io("creating", parent, e))?;
            }
            fs::write(&path, file.contents)
                .await
                .map_err(|e| io("writing", &path, e))?;
        }
        self.set_variables(variables).await
    }
    /// Rewrite only the variables file, checking that it contains no credentials.
    /// # Errors
    /// Returns embedded credentials, serialization or filesystem failures.
    pub async fn set_variables(
        &self,
        variables: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<()> {
        let bytes = self.variable_bytes(variables)?;
        let path = self.dir.join(VARIABLES_FILE);
        fs::write(&path, bytes)
            .await
            .map_err(|e| io("writing", &path, e))?;
        Ok(())
    }
    fn variable_bytes(
        &self,
        variables: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<Vec<u8>> {
        let bytes = serde_json::to_vec_pretty(variables).map_err(Error::Variables)?;
        if let Some(name) = self.credentials.found_in(&bytes) {
            return Err(Error::SecretInFiles { name: name.into() });
        }
        Ok(bytes)
    }
    /// # Errors
    /// Returns process failures.
    pub async fn init(&self) -> Result<()> {
        self.run(Subcommand::Init).await.map(|_| ())
    }
    /// # Errors
    /// Returns process failures.
    pub async fn plan(&self) -> Result<PlanOutcome> {
        self.plan_text().await.map(|(outcome, _)| outcome)
    }
    /// Plan and return `OpenTofu`'s human-readable plan with credential values redacted.
    /// # Errors
    /// Returns process failures.
    pub async fn plan_text(&self) -> Result<(PlanOutcome, String)> {
        let (text, code) = self.run(Subcommand::Plan).await?;
        let outcome = if code == Some(2) {
            PlanOutcome::Changes
        } else {
            PlanOutcome::NoChanges
        };
        Ok((
            outcome,
            self.credentials.redact(&String::from_utf8_lossy(&text)),
        ))
    }
    /// # Errors
    /// Returns process failures.
    pub async fn apply(&self) -> Result<()> {
        self.run(Subcommand::Apply).await.map(|_| ())
    }
    /// Apply only the named resources, without creating unrelated missing resources.
    /// # Errors
    /// Returns process failures.
    pub async fn apply_targets(&self, targets: &[&str]) -> Result<()> {
        self.run(Subcommand::ApplyTargets(targets))
            .await
            .map(|_| ())
    }
    /// # Errors
    /// Returns process failures.
    pub async fn destroy(&self) -> Result<()> {
        self.run(Subcommand::Destroy).await.map(|_| ())
    }
    /// # Errors
    /// Returns process and contract failures.
    pub async fn outputs(&self) -> Result<ModuleOutputs> {
        let (bytes, _) = self.run(Subcommand::Output).await?;
        Ok(parse_outputs(&String::from_utf8_lossy(&bytes))?)
    }
    async fn run(&self, sub: Subcommand<'_>) -> Result<(Vec<u8>, Option<i32>)> {
        fs::create_dir_all(&self.tofu.plugin_cache)
            .await
            .map_err(|e| io("creating", &self.tofu.plugin_cache, e))?;
        let mut command = Command::new(&self.tofu.binary);
        command.args(sub.args()).current_dir(&self.dir).env_clear();
        if let Subcommand::ApplyTargets(targets) = sub {
            for target in targets {
                command.arg(format!("-target={target}"));
            }
        }
        for key in PASSTHROUGH {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        for (key, value) in &self.credentials.vars {
            command.env(key, value);
        }
        command
            .env("TF_IN_AUTOMATION", "1")
            .env("TF_INPUT", "0")
            .env("TF_PLUGIN_CACHE_DIR", &self.tofu.plugin_cache)
            .stdin(Stdio::null());
        tracing::info!(command = sub.name(), dir = %self.dir.display(), "running tofu");
        let output = command.output().await.map_err(|source| Error::Spawn {
            binary: self.tofu.binary.clone(),
            source,
        })?;
        let code = output.status.code();
        if !(output.status.success() || matches!(sub, Subcommand::Plan) && code == Some(2)) {
            return Err(Error::Failed {
                command: sub.name(),
                dir: self.dir.clone(),
                status: output.status,
                // Redact before truncating so a cut cannot leave part of a secret.
                stderr: stderr_summary(
                    &self
                        .credentials
                        .redact(&String::from_utf8_lossy(&output.stderr)),
                ),
            });
        }
        Ok((output.stdout, code))
    }
}
#[derive(Clone, Copy)]
enum Subcommand<'a> {
    Init,
    Plan,
    Apply,
    ApplyTargets(&'a [&'a str]),
    Destroy,
    Output,
}
impl Subcommand<'_> {
    const fn name(self) -> &'static str {
        match self {
            Self::Init => "init",
            Self::Plan => "plan",
            Self::Apply | Self::ApplyTargets(_) => "apply",
            Self::Destroy => "destroy",
            Self::Output => "output",
        }
    }
    const fn args(self) -> &'static [&'static str] {
        match self {
            Self::Init => &["init", "-input=false", "-no-color"],
            Self::Plan => &[
                "plan",
                "-input=false",
                "-no-color",
                "-detailed-exitcode",
                LOCK_TIMEOUT,
                "-var-file=swarmy.tfvars.json",
            ],
            Self::Apply | Self::ApplyTargets(_) => &[
                "apply",
                "-input=false",
                "-no-color",
                "-auto-approve",
                LOCK_TIMEOUT,
                "-var-file=swarmy.tfvars.json",
            ],
            Self::Destroy => &[
                "destroy",
                "-input=false",
                "-no-color",
                "-auto-approve",
                LOCK_TIMEOUT,
                "-var-file=swarmy.tfvars.json",
            ],
            Self::Output => &["output", "-no-color", "-json"],
        }
    }
}
pub(crate) fn stderr_summary(stderr: &str) -> String {
    let lines: Vec<&str> = stderr
        .lines()
        .map(|line| {
            line.trim()
                .trim_matches(|c: char| matches!(c, '│' | '╷' | '╵'))
                .trim()
        })
        .filter(|line| !line.is_empty())
        .collect();
    let selected = if let Some(index) = lines.iter().position(|line| line.starts_with("Error:")) {
        &lines[index..lines.len().min(index + 40)]
    } else {
        &lines[lines.len().saturating_sub(20)..]
    };
    let mut text = selected.join("\n");
    if text.len() > 4096 {
        let mut end = 4096;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
    }
    text
}
#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    #[test]
    fn summaries() {
        assert_eq!(
            stderr_summary("╷\n│ Error: boom\n│ details\n╵"),
            "Error: boom\ndetails"
        );
        let text = (0..30)
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(stderr_summary(&text).lines().count(), 20);
        assert_eq!(stderr_summary(&text).lines().next(), Some("10"));
        assert!(stderr_summary(&"é".repeat(3000)).len() <= 4096);
    }
    #[test]
    fn redaction() {
        let env = CredentialEnv::new()
            .with("EMPTY", "")
            .with("TOKEN", "secret");
        assert_eq!(env.redact("secret secret"), "<redacted> <redacted>");
        assert!(!format!("{env:?}").contains("secret"));
    }
}
