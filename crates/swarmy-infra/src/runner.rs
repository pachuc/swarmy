use crate::{Error, ModuleOutputs, Result, opentofu, parse_outputs};
use std::{
    fmt,
    path::{Component, Path, PathBuf},
    process::Stdio,
};
use tokio::{fs, process::Command};
pub const VARIABLES_FILE: &str = "swarmy.tfvars.json";
pub const STATE_FILE: &str = "terraform.tfstate";
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
const CLOSED: [(&str, &str); 4] = [
    ("AWS_SHARED_CREDENTIALS_FILE", "/dev/null"),
    ("AWS_CONFIG_FILE", "/dev/null"),
    ("AWS_EC2_METADATA_DISABLED", "true"),
    ("SCW_CONFIG_PATH", "/nonexistent/swarmy/scw-config.yaml"),
];
#[derive(Clone)]
struct Var {
    name: String,
    value: String,
    secret: bool,
}
#[derive(Clone, Default)]
pub struct CredentialEnv {
    vars: Vec<Var>,
}
impl CredentialEnv {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    /// Keep the AWS and Scaleway providers off shared files, local CLI profiles and instance metadata.
    #[must_use]
    pub fn closed() -> Self {
        CLOSED.iter().fold(Self::new(), |env, (name, value)| {
            env.with_setting(*name, *value)
        })
    }
    /// Add a secret: passed to `OpenTofu`, refused in its files, redacted from its output.
    #[must_use]
    pub fn with(self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.push(name.into(), value.into(), true)
    }
    /// Add a plain setting, such as a region: passed to `OpenTofu` and allowed anywhere.
    #[must_use]
    pub fn with_setting(self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.push(name.into(), value.into(), false)
    }
    fn push(mut self, name: String, value: String, secret: bool) -> Self {
        self.vars.push(Var {
            name,
            value,
            secret,
        });
        self
    }
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.vars.iter().map(|var| var.name.as_str())
    }
    fn secrets(&self) -> impl Iterator<Item = &Var> {
        self.vars
            .iter()
            .filter(|var| var.secret && !var.value.is_empty())
    }
    fn found_in(&self, bytes: &[u8]) -> Option<&str> {
        self.secrets()
            .find(|var| {
                bytes
                    .windows(var.value.len())
                    .any(|w| w == var.value.as_bytes())
            })
            .map(|var| var.name.as_str())
    }
    fn redact(&self, text: &str) -> String {
        self.secrets().fold(text.to_owned(), |text, var| {
            text.replace(&var.value, "<redacted>")
        })
    }
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
    /// The test override when set and non-empty, otherwise the pinned verified download.
    /// # Errors
    /// Returns `TofuBinMissing` for a missing override, or download errors.
    pub async fn resolve(cache: &Path) -> Result<Self> {
        Self::resolve_with(std::env::var_os("SWARMY_TOFU_BIN").as_deref(), cache).await
    }
    /// Resolve with an explicit override so tests do not mutate process environment.
    /// # Errors
    /// Returns `TofuBinMissing` for a missing override, or download errors.
    pub async fn resolve_with(
        override_bin: Option<&std::ffi::OsStr>,
        cache: &Path,
    ) -> Result<Self> {
        if let Some(path) = override_bin.filter(|value| !value.is_empty()) {
            let path = PathBuf::from(path);
            if !path.is_file() {
                return Err(Error::TofuBinMissing(path));
            }
            return Ok(Self::from_binary(path, cache));
        }
        Self::install(cache, &opentofu::Source::pinned()).await
    }

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
#[derive(serde::Deserialize)]
struct StateFile {
    #[serde(default)]
    resources: Vec<StateResource>,
}
#[derive(serde::Deserialize)]
struct StateResource {
    mode: String,
    #[serde(rename = "type")]
    kind: String,
    name: String,
    #[serde(default)]
    module: Option<String>,
    #[serde(default)]
    instances: Vec<StateInstance>,
}
#[derive(serde::Deserialize)]
struct StateInstance {
    #[serde(default)]
    index_key: Option<serde_json::Value>,
}
/// Managed resource instances in an `OpenTofu` state file, as addresses such as
/// `scaleway_instance_ip.this[0]`. Data sources are left out because destroying
/// the group does not remove them and they cost nothing.
fn managed_instances(json: &[u8]) -> serde_json::Result<Vec<String>> {
    let state: StateFile = serde_json::from_slice(json)?;
    Ok(state
        .resources
        .iter()
        .filter(|resource| resource.mode == "managed")
        .flat_map(|resource| {
            let base = match &resource.module {
                Some(module) => format!("{module}.{}.{}", resource.kind, resource.name),
                None => format!("{}.{}", resource.kind, resource.name),
            };
            resource
                .instances
                .iter()
                .map(move |instance| match &instance.index_key {
                    Some(key) => format!("{base}[{key}]"),
                    None => base.clone(),
                })
        })
        .collect())
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
    /// Managed resource instances recorded in this workspace's local state.
    /// Empty when there is no state file yet.
    /// # Errors
    /// Returns filesystem failures and a state file that is not `OpenTofu` JSON.
    pub async fn state_resources(&self) -> Result<Vec<String>> {
        let path = self.dir.join(STATE_FILE);
        let bytes = match fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(io("reading", &path, error)),
        };
        managed_instances(&bytes).map_err(|source| Error::State { path, source })
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
        for var in &self.credentials.vars {
            command.env(&var.name, &var.value);
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
    #[test]
    fn state_lists_managed_instances_only() {
        let state = br#"{"version":4,"resources":[
            {"mode":"data","type":"aws_ssm_parameter","name":"ubuntu","instances":[{}]},
            {"mode":"managed","type":"scaleway_instance_security_group","name":"this","instances":[{"schema_version":0,"attributes":{}}]},
            {"mode":"managed","type":"scaleway_instance_ip","name":"this","instances":[{"index_key":0},{"index_key":1}]},
            {"mode":"managed","module":"module.net","type":"x","name":"y","instances":[{"index_key":"a"}]}
        ]}"#;
        assert_eq!(
            managed_instances(state).unwrap(),
            [
                "scaleway_instance_security_group.this",
                "scaleway_instance_ip.this[0]",
                "scaleway_instance_ip.this[1]",
                "module.net.x.y[\"a\"]",
            ]
        );
        assert!(
            managed_instances(br#"{"version":4,"resources":[]}"#)
                .unwrap()
                .is_empty()
        );
        assert!(managed_instances(b"state").is_err());
    }

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
            .with("TOKEN", "secret")
            .with_setting("AWS_REGION", "us-west-2");
        assert_eq!(env.redact("secret secret"), "<redacted> <redacted>");
        assert_eq!(env.redact("us-west-2 secret"), "us-west-2 <redacted>");
        assert_eq!(env.found_in(b"location us-west-2"), None);
        assert_eq!(env.found_in(b"x secret y"), Some("TOKEN"));
        assert_eq!(CredentialEnv::closed().found_in(b"/dev/null true"), None);
        assert!(!format!("{env:?}").contains("secret"));
    }
}
