use swarmy_cloud::ssh as remote_ssh;
mod process;

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use clap::Subcommand;
use fs2::FileExt;
use swarmy_config::Settings;
use tokio::{
    process::{Child, Command as Process},
    time::{sleep, timeout},
};

use process::Identity;

const SERVICES: [(&str, &str); 4] = [
    ("scheduler", "scheduler started"),
    ("worker", "worker ready"),
    ("gateway", "gateway ready"),
    ("api", "api ready"),
];
/// The backing tool installer is compiled in so it needs no checkout.
/// The script remains the source for make, remote provisioning, and image setup.
const INSTALL_DEV_TOOLS: &str = include_str!("../../../scripts/install-dev-tools.sh");
/// The backing stack script is compiled in so `dev` needs no checkout; it runs
/// with `SWARMY_DEV_DIR`. The file remains the source for make, CI, and nodes.
const DEV_STACK: &str = include_str!("../../../scripts/dev-stack.sh");
const START_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Subcommand)]
pub(crate) enum Command {
    /// Start the backing stack and supervised services in the background
    Up {
        /// Run this swarmy-core instead of the matching release
        #[arg(long, env = "SWARMY_CORE_BIN", value_name = "PATH")]
        core_bin: Option<PathBuf>,
    },
    /// Stop the services, supervisor, and backing stack, preserving data
    Down,
    /// Show process ids and uptime
    Status,
    /// Follow all service logs, or one service's log
    Logs {
        #[arg(value_enum)]
        service: Option<DevService>,
    },
    /// Install `FoundationDB`, NATS, and `SeaweedFS` for the local stack (Linux x86-64)
    InstallTools {
        /// Install root: tools go to PREFIX/bin and PREFIX/lib (default ~/.local)
        #[arg(long)]
        prefix: Option<PathBuf>,
    },
    #[command(hide = true)]
    Supervise { state: PathBuf, core: PathBuf },
}

/// One supervised backing service, parsed once by clap instead of matched as a string.
#[derive(Clone, Copy, clap::ValueEnum)]
pub(crate) enum DevService {
    Scheduler,
    Worker,
    Gateway,
    Api,
    Supervisor,
}

struct Layout {
    /// File dev up creates or updates.
    config: PathBuf,
    /// Anchor for relative paths and services' cwd.
    root: PathBuf,
    /// Resolved state directory containing dev/ and core/.
    state_dir: PathBuf,
    /// Supervised service state under `state_dir/dev`.
    state: PathBuf,
    /// Backing stack directory passed as `SWARMY_DEV_DIR`.
    stack: PathBuf,
}

impl Layout {
    fn discover() -> Result<Self> {
        let loaded = Settings::load_base()?;
        let cwd = std::env::current_dir()?;
        let environment = [
            "HOME",
            "XDG_CONFIG_HOME",
            "SWARMY_DEV_DIR",
            "SWARMY_STATE_DIR",
        ]
        .into_iter()
        .filter_map(|key| std::env::var(key).ok().map(|value| (key.to_owned(), value)))
        .collect();
        Self::resolve(loaded, &cwd, &environment)
    }

    fn resolve(
        loaded: swarmy_config::Loaded,
        cwd: &Path,
        environment: &BTreeMap<String, String>,
    ) -> Result<Self> {
        let repo = cwd
            .ancestors()
            .find(|dir| dir.join("scripts/dev-stack.sh").is_file())
            .map(Path::canonicalize)
            .transpose()?;
        let found = loaded.path.is_some();
        let (config, root) = if let Some(path) = loaded.path {
            (path, loaded.root)
        } else if let Some(repo) = &repo {
            (repo.join(".swarmy/config.toml"), repo.clone())
        } else {
            let config = Settings::user_config_path(environment).context(
                "set HOME (or XDG_CONFIG_HOME), or run swarmy dev inside a checkout or a directory with .swarmy/config.toml",
            )?;
            let home = environment
                .get("HOME")
                .context("set HOME for swarmy dev state")?;
            (config, PathBuf::from(home))
        };
        let state_dir = if found || environment.contains_key("SWARMY_STATE_DIR") {
            loaded.settings.state_dir
        } else {
            root.join(Settings::default().state_dir)
        };
        let state_dir = if state_dir.is_absolute() {
            state_dir
        } else {
            root.join(state_dir)
        };
        let stack = environment.get("SWARMY_DEV_DIR").map_or_else(
            || repo.map_or_else(|| state_dir.join("stack"), |repo| repo.join(".dev")),
            |dir| {
                let path = PathBuf::from(dir);
                if path.is_absolute() {
                    path
                } else {
                    cwd.join(path)
                }
            },
        );
        Ok(Self {
            config,
            root,
            state: state_dir.join("dev"),
            state_dir,
            stack,
        })
    }

    fn lock(&self) -> Result<File> {
        fs::create_dir_all(&self.state)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(self.state.join("lock"))?;
        file.try_lock_exclusive()
            .context("another swarmy dev up/down is in progress")?;
        Ok(file)
    }
}

pub(crate) async fn run(command: Command, json: bool, allow_version_mismatch: bool) -> Result<()> {
    // One match owns every variant. The supervisor is spawned without flags
    // by `dev up`, so it never takes the human-output path; every other arm
    // rejects `--json` and discovers the layout through one helper.
    match command {
        Command::Supervise { state, core } => supervise(&state, &core).await,
        Command::Up { core_bin } => {
            let layout = local(json)?;
            let _lock = layout.lock()?;
            up(&layout, allow_version_mismatch, core_bin).await
        }
        Command::Down => {
            let layout = local(json)?;
            let _lock = layout.lock()?;
            stop_services(&layout.state).await?;
            let remote = layout.state.join("remote").exists()
                || Settings::load_base()?.settings.remote.profile.is_some();
            if remote {
                println!("services: down; remote stack preserved");
            } else {
                stack(&layout, "stop").await?;
                println!("services and stack: down");
            }
            Ok(())
        }
        Command::Status => status(&local(json)?).await,
        Command::InstallTools { prefix } => {
            reject_json(json)?;
            install_tools(prefix.as_deref()).await
        }
        Command::Logs { service } => {
            logs(
                &local(json)?.state,
                service
                    .map(|service| crate::cost_command::value_name(&service))
                    .as_deref(),
            )
            .await
        }
    }
}

/// Reject `--json` and discover the dev layout for human-output `dev`
/// commands. `dev` manages local processes with progress lines, so it has no
/// JSON rendering; the explicit rejection keeps `--json dev` from silently
/// printing text.
fn reject_json(json: bool) -> Result<()> {
    if json {
        bail!("--json is not supported for dev commands; run without it for human-readable output");
    }
    Ok(())
}

fn local(json: bool) -> Result<Layout> {
    reject_json(json)?;
    Layout::discover()
}

fn write_private(path: &Path, content: &str) -> Result<()> {
    let temporary = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(&temporary)?;
    file.write_all(content.as_bytes())?;
    file.sync_all()?;
    fs::rename(temporary, path)?;
    Ok(())
}

/// `bash -c SCRIPT NAME`: run a compiled-in script; NAME becomes its $0.
fn embedded(script: &str, name: &str) -> Process {
    let mut command = Process::new("bash");
    command.arg("-c").arg(script).arg(name);
    command
}

async fn stack(layout: &Layout, action: &str) -> Result<String> {
    let output = embedded(DEV_STACK, "swarmy dev stack")
        .arg(action)
        .env("SWARMY_DEV_DIR", &layout.stack)
        .env("PATH", crate::tools::search_path()?)
        .stdin(Stdio::null())
        .process_group(0)
        .output()
        .await
        .context("cannot run bash for the dev stack")?;
    ensure!(
        output.status.success(),
        "dev stack {action} failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?)
}

pub(crate) enum CoreSource {
    Explicit(PathBuf),
    Release(swarmy_cloud::release::Release),
    Development,
}
impl CoreSource {
    pub(crate) fn select(core_bin: Option<PathBuf>) -> Result<Self> {
        if let Some(path) = core_bin {
            return Ok(Self::Explicit(path));
        }
        Ok(swarmy_cloud::release::Release::for_client()?.map_or(Self::Development, Self::Release))
    }
    pub(crate) const fn label(&self) -> &'static str {
        match self {
            Self::Explicit(_) => "explicit",
            Self::Release(_) => "release",
            Self::Development => "development",
        }
    }
}
pub(crate) struct ResolvedCore {
    pub(crate) path: PathBuf,
    pub(crate) source: &'static str,
    pub(crate) fix: String,
}
/// `<state_dir>/core` for the layout `swarmy dev` uses.
pub(crate) fn core_cache() -> Result<PathBuf> {
    Ok(Layout::discover()?.state_dir.join("core"))
}
pub(crate) async fn resolve_core(
    core_bin: Option<PathBuf>,
    target: Option<swarmy_cloud::release::Target>,
) -> Result<ResolvedCore> {
    use swarmy_cloud::release::{Component, Target};
    let source = CoreSource::select(core_bin)?;
    let label = source.label();
    let (path, fix) = match source {
        CoreSource::Explicit(path) => {
            let canonical = path.canonicalize().with_context(|| {
                format!(
                    "--core-bin/SWARMY_CORE_BIN {} does not exist",
                    path.display()
                )
            })?;
            let fix = format!(
                "Rebuild {} at commit {} (cargo build -p swarmy-core-bin) or unset --core-bin/SWARMY_CORE_BIN",
                path.display(),
                swarmy_version::GIT_COMMIT
            );
            (canonical, fix)
        }
        CoreSource::Release(release) => {
            let cache = core_cache()?;
            let fix = format!(
                "Remove {}/{} and run swarmy dev up again to download {}",
                cache.display(),
                release.tag(),
                release.tag()
            );
            (
                release
                    .ensure(
                        &cache,
                        Component::Core,
                        target.map_or_else(Target::host, Ok)?,
                    )
                    .await?
                    .join("swarmy-core"),
                fix,
            )
        }
        CoreSource::Development => (
            service_binary("swarmy-core")?,
            format!("Reinstall from the CLI checkout: {REINSTALL}"),
        ),
    };
    Ok(ResolvedCore {
        path,
        source: label,
        fix,
    })
}
pub(crate) async fn check_versions(
    core: &ResolvedCore,
    allow_version_mismatch: bool,
) -> Result<()> {
    if let Err(error) = version_check(&core.path, "swarmy-core").await {
        let message = format!("{error}. {}", core.fix);
        if !allow_version_mismatch {
            bail!("{message}\nUse --allow-version-mismatch to override.");
        }
        eprintln!("warning: {message} (--allow-version-mismatch)");
    }
    Ok(())
}

async fn node_services(layout: &Layout) -> Result<bool> {
    let base = Settings::load_base()?.settings;
    if let Some(name) = &base.remote.profile {
        let node: swarmy_config::RemoteNode = serde_json::from_slice(&fs::read(
            swarmy_config::remote_path(Path::new(&base.state_dir), name, "json")?,
        )?)?;
        if node
            .launch_settings
            .as_ref()
            .is_some_and(|settings| settings.services == swarmy_config::RemoteServices::Node)
        {
            stop_services(&layout.state).await?;
            prepare_stack(layout, Some(name)).await?;
            println!("services: running on node {name}; no local services started");
            return Ok(true);
        }
    }
    Ok(false)
}

fn ensure_keyring() -> Result<()> {
    let keyring_path = swarmy_config::Keyring::path()?;
    match swarmy_config::Keyring::read(&keyring_path) {
        Ok(_) => (),
        Err(swarmy_config::Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
            match swarmy_config::Keyring::generate() {
                Ok(_) => println!(
                    "Generated cluster keyring at {} (mode 600)",
                    keyring_path.display()
                ),
                Err(swarmy_config::Error::Io(e))
                    if e.kind() == std::io::ErrorKind::AlreadyExists =>
                {
                    swarmy_config::Keyring::load()?;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

async fn up(
    layout: &Layout,
    allow_version_mismatch: bool,
    core_bin: Option<PathBuf>,
) -> Result<()> {
    if node_services(layout).await? {
        return Ok(());
    }
    ensure_keyring()?;
    let core = resolve_core(core_bin, None).await?;
    check_versions(&core, allow_version_mismatch).await?;
    fs::create_dir_all(layout.state.join("logs"))?;
    let survivors: Vec<_> = ["supervisor", "scheduler", "worker", "gateway", "api"]
        .into_iter()
        .filter(|name| Identity::read(&layout.state.join(format!("{name}.pid"))).is_some())
        .collect();
    if !survivors.is_empty() {
        println!("replacing recorded processes: {}", survivors.join(", "));
    }
    stop_services(&layout.state).await?;
    let remote = Settings::load_base()?.settings.remote.profile;
    prepare_stack(layout, remote.as_deref()).await?;
    let settings = prepare_settings(layout, remote.is_some())?;
    let ready = layout.state.join("ready");
    if ready.exists() {
        fs::remove_file(&ready)?;
    }
    let log = File::create(layout.state.join("logs/supervisor.log"))?;
    let mut supervisor = Process::new(std::env::current_exe()?)
        .arg("dev")
        .arg("supervise")
        .arg(&layout.state)
        .arg(&core.path)
        .current_dir(&layout.root)
        .envs(settings.environment())
        .env("RUST_LOG", "info")
        .env(
            "TOKIO_WORKER_THREADS",
            std::env::var("TOKIO_WORKER_THREADS").unwrap_or_else(|_| "2".into()),
        )
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log)
        .process_group(0)
        .spawn()?;
    let pid = supervisor
        .id()
        .context("supervisor exited before startup")?;
    Identity::current(pid)
        .context("supervisor exited before its pid could be recorded")?
        .save(&layout.state.join("supervisor.pid"))?;
    let result = timeout(START_TIMEOUT, async {
        loop {
            if let Some(exit) = supervisor.try_wait()? {
                bail!(
                    "supervisor exited: {exit}; see {}",
                    layout.state.join("logs/supervisor.log").display()
                );
            }
            if ready.is_file() {
                return Ok::<_, anyhow::Error>(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("services did not become ready within 30 seconds")
    .and_then(std::convert::identity);
    if let Err(error) = result {
        let cleanup = stop_services(&layout.state).await;
        if supervisor.try_wait()?.is_none() {
            supervisor.kill().await?;
        }
        cleanup?;
        return Err(error);
    }
    let stack_pids = ["fdb", "nats", "seaweed"]
        .into_iter()
        .filter_map(|name| {
            Identity::read(&layout.stack.join(format!("{name}.pid")))
                .map(|id| format!("{name}={}", id.pid))
        })
        .collect::<Vec<_>>();
    if let Some(name) = remote {
        println!("stack: remote {name}");
    } else {
        println!("stack: ready ({})", stack_pids.join(", "));
    }
    for (name, _) in SERVICES {
        let identity = Identity::read(&layout.state.join(format!("{name}.pid")))
            .context("service exited during startup")?;
        println!("{name}: ready (pid {})", identity.pid);
    }
    println!(
        "supervisor: running (pid {pid}); logs: {}",
        layout.state.join("logs").display()
    );
    Ok(())
}

async fn prepare_stack(layout: &Layout, remote: Option<&str>) -> Result<()> {
    if remote.is_none() {
        // CI and manual users may already have started the backing stack.
        let running = stack(layout, "status").await?;
        if running
            .lines()
            .filter(|line| line.contains(": up (pid "))
            .count()
            != 3
            || !layout.stack.join("env").is_file()
        {
            stack(layout, "start").await?;
        }
    }
    let marker = layout.state.join("remote");
    if let Some(name) = remote {
        let profile = swarmy_config::RemoteProfile::read(
            Path::new(&Settings::load_base()?.settings.state_dir),
            name,
        )?;
        ensure!(
            remote_ssh::healthy(&profile).await,
            "remote tunnel is down; run swarmy remote connect {name}"
        );
        write_private(&marker, name)?;
    } else if marker.exists() {
        fs::remove_file(&marker)?;
    }
    Ok(())
}

fn prepare_settings(layout: &Layout, remote: bool) -> Result<Settings> {
    let mut settings = if layout.config.exists() {
        Settings::read(&layout.config)?
    } else {
        Settings::default()
    };
    if !remote {
        let exports = swarmy_config::parse_exports(&fs::read_to_string(layout.stack.join("env"))?)?;
        settings.apply_environment(&exports)?;
    }
    if settings.selection.credential_file.as_os_str().is_empty() {
        settings.selection.credential_file = std::env::var_os("HOME")
            .map_or_else(|| layout.root.clone(), PathBuf::from)
            .join(".swarmy/auth.json");
    }
    settings.resolve_paths(&layout.root);
    if settings.api.token.is_empty() {
        // A usable API must reject unauthenticated traffic, including on loopback.
        settings.api.token = ulid::Ulid::generate().to_string();
    }
    if let Some(parent) = layout.config.parent() {
        fs::create_dir_all(parent)?;
    }
    write_private(&layout.config, &settings.to_toml()?)?;
    // Load again so environment overrides win, without persisting those overrides.
    settings = Settings::load()?.settings;
    if settings.selection.provider == "fake" {
        let script = &settings.fake.script;
        if let Some(parent) = script.parent() {
            fs::create_dir_all(parent)?;
        }
        if !script.exists() {
            write_private(
                script,
                r#"{"request_based":{"steps":1,"tool_steps":[],"final_answer":"Hello from swarmy!"}}"#,
            )?;
        }
        if let Some(parent) = Path::new(&settings.fake.call_log).parent() {
            fs::create_dir_all(parent)?;
        }
    }
    Ok(settings)
}

// Keep this command aligned with docs/DEV.md's no-root installation workflow.
pub(crate) const REINSTALL: &str = "for crate in swarmy-cli swarmy-core-bin; do SWARMY_FDB_LIB_DIR=\"$HOME/.local/lib\" cargo install --locked --path \"crates/$crate\"; done";

pub(crate) fn service_binary(name: &str) -> Result<PathBuf> {
    let executable =
        crate::tools::find(name).unwrap_or(std::env::current_exe()?.with_file_name(name));
    executable.canonicalize().with_context(|| {
        format!(
            "missing {name} at {}; reinstall: {REINSTALL}",
            executable.display()
        )
    })
}

pub(crate) async fn version_check(executable: &Path, name: &str) -> Result<String> {
    let output = timeout(
        Duration::from_secs(5),
        Process::new(executable)
            .arg("--version")
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .with_context(|| format!("{name} version check timed out"))?
    .with_context(|| format!("cannot run {name} --version"))?;
    ensure!(
        output.status.success(),
        "{name} version check failed ({})",
        output.status
    );
    let reported = String::from_utf8(output.stdout)
        .with_context(|| format!("{name} reported a non-UTF-8 version"))?;
    let reported = reported.trim();
    let identity = reported
        .strip_prefix(name)
        .and_then(|rest| rest.strip_prefix(' '))
        .unwrap_or(reported);
    ensure!(
        identity == swarmy_version::IDENTITY,
        "{name} version mismatch: found {identity:?}, CLI expects {}",
        swarmy_version::IDENTITY
    );
    Ok(format!("{}: {identity}", executable.display()))
}

async fn stop_services(state: &Path) -> Result<()> {
    process::stop(&state.join("supervisor.pid"), Duration::from_secs(40)).await?;
    for (name, _) in SERVICES.into_iter().rev() {
        process::stop(&state.join(format!("{name}.pid")), Duration::from_secs(10)).await?;
    }
    let ready = state.join("ready");
    if ready.exists() {
        fs::remove_file(ready)?;
    }
    Ok(())
}

async fn supervise(state: &Path, core: &Path) -> Result<()> {
    // Register signal handlers before spawning so down during startup also cleans up.
    let stop = swarmy_config::shutdown_signal();
    let mut children = Vec::new();
    let result = tokio::select! {
        result = serve(state, core, &mut children) => result,
        () = stop => Ok(()),
    };
    for (name, child) in children.iter_mut().rev() {
        if !state.join(format!("{name}.pid")).exists() && child.try_wait()?.is_none() {
            child.start_kill()?;
        }
        process::stop(&state.join(format!("{name}.pid")), Duration::from_secs(10)).await?;
        child.wait().await?;
    }
    for name in ["ready", "supervisor.pid"] {
        let path = state.join(name);
        if path.exists() {
            fs::remove_file(path)?;
        }
    }
    result
}

async fn serve(state: &Path, core: &Path, children: &mut Vec<(&'static str, Child)>) -> Result<()> {
    // Use the checked absolute paths even when the supervisor changes directory.
    for (name, _) in SERVICES {
        let log = File::create(state.join(format!("logs/{name}.log")))?;
        let child = Process::new(core)
            .arg(name)
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .env("RUST_LOG", "info")
            .env("NO_COLOR", "1")
            .kill_on_drop(true)
            .spawn()?;
        let pid = child.id().context("child has no pid")?;
        children.push((name, child));
        Identity::current(pid)
            .context("service exited before its pid could be recorded")?
            .save(&state.join(format!("{name}.pid")))?;
    }
    timeout(START_TIMEOUT, async {
        loop {
            check_children(children)?;
            if SERVICES.iter().all(|(name, message)| {
                fs::read_to_string(state.join(format!("logs/{name}.log")))
                    .is_ok_and(|log| log.contains(message))
            }) {
                write_private(&state.join("ready"), "ready\n")?;
                return Ok::<_, anyhow::Error>(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("service readiness timed out; inspect service logs")??;
    loop {
        check_children(children)?;
        sleep(Duration::from_millis(200)).await;
    }
}

fn check_children(children: &mut [(&str, Child)]) -> Result<()> {
    for (name, child) in children {
        if let Some(exit) = child.try_wait()? {
            bail!("{name} exited: {exit}; inspect its service log");
        }
    }
    Ok(())
}

async fn status(layout: &Layout) -> Result<()> {
    let ticks = process::clock_ticks().await?;
    let stack = ["fdb", "nats", "seaweed"]
        .into_iter()
        .filter_map(|name| {
            Identity::read(&layout.stack.join(format!("{name}.pid")))
                .map(|identity| (name, identity))
        })
        .collect::<Vec<_>>();
    if let Ok(name) = fs::read_to_string(layout.state.join("remote")) {
        println!("stack: remote {name}");
    } else if stack.is_empty() {
        println!("stack: down");
    } else {
        let mut entries = Vec::new();
        for (name, identity) in stack {
            entries.push(format!(
                "{name} pid {} uptime {}s",
                identity.pid,
                process::uptime(identity, ticks)?
            ));
        }
        println!("stack: {}", entries.join(", "));
    }
    for name in ["scheduler", "worker", "gateway", "api", "supervisor"] {
        if let Some(identity) = Identity::read(&layout.state.join(format!("{name}.pid"))) {
            println!(
                "{name}: up (pid {}, uptime {}s)",
                identity.pid,
                process::uptime(identity, ticks)?
            );
        } else {
            println!("{name}: down");
        }
    }
    Ok(())
}

async fn logs(state: &Path, service: Option<&str>) -> Result<()> {
    let names = service.map_or_else(
        || SERVICES.iter().map(|(name, _)| *name).collect(),
        |name| vec![name],
    );
    let mut command = Process::new("tail");
    command.args(["-n", "30", "-F"]);
    for name in names {
        let path = state.join(format!("logs/{name}.log"));
        ensure!(path.exists(), "no log for {name}; run swarmy dev up first");
        command.arg(path);
    }
    let mut child = command.kill_on_drop(true).spawn()?;
    tokio::select! {
        result = child.wait() => { ensure!(result?.success(), "tail failed"); },
        result = tokio::signal::ctrl_c() => { result?; child.kill().await?; },
    }
    Ok(())
}

/// Run the embedded installer with bash, inheriting stdio so download progress shows.
async fn install_tools(prefix: Option<&Path>) -> Result<()> {
    let mut command = embedded(INSTALL_DEV_TOOLS, "swarmy dev install-tools");
    if let Some(prefix) = prefix {
        command.arg("--prefix").arg(prefix);
    }
    let status = command
        .status()
        .await
        .context("cannot run bash for the backing tool installer")?;
    ensure!(
        status.success(),
        "backing tool installation failed ({status})"
    );
    Ok(())
}

#[cfg(test)]
mod layout_tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    fn loaded(path: Option<PathBuf>, root: &Path, state_dir: PathBuf) -> swarmy_config::Loaded {
        let mut settings = Settings::default();
        settings.state_dir = state_dir;
        swarmy_config::Loaded {
            path,
            root: root.to_owned(),
            settings,
        }
    }
    #[test]
    fn checkout_without_config_keeps_repo_files() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path();
        fs::create_dir_all(repo.join("scripts")).unwrap();
        fs::write(repo.join("scripts/dev-stack.sh"), "").unwrap();
        let cwd = repo.join("crates/x");
        fs::create_dir_all(&cwd).unwrap();
        let layout = Layout::resolve(
            loaded(None, &cwd, cwd.join(".swarmy")),
            &cwd,
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(layout.config, repo.join(".swarmy/config.toml"));
        assert_eq!(layout.root, repo);
        assert_eq!(layout.state, repo.join(".swarmy/dev"));
        assert_eq!(layout.stack, repo.join(".dev"));
    }
    #[test]
    fn no_checkout_uses_user_config_and_home_state() {
        let temp = tempfile::tempdir().unwrap();
        let cwd = temp.path().join("cwd");
        let home = temp.path().join("home");
        let mut env = BTreeMap::from([("HOME".into(), home.display().to_string())]);
        let make = |env: &BTreeMap<_, _>| {
            Layout::resolve(loaded(None, &cwd, cwd.join(".swarmy")), &cwd, env).unwrap()
        };
        let layout = make(&env);
        assert_eq!(layout.config, home.join(".config/swarmy/config.toml"));
        assert_eq!(layout.root, home);
        assert_eq!(layout.state_dir, home.join(".swarmy"));
        assert_eq!(layout.stack, home.join(".swarmy/stack"));
        env.insert(
            "XDG_CONFIG_HOME".into(),
            temp.path().join("xdg").display().to_string(),
        );
        assert_eq!(
            make(&env).config,
            temp.path().join("xdg/swarmy/config.toml")
        );
    }
    #[test]
    fn discovered_config_wins_and_supplies_state_dir() {
        let temp = tempfile::tempdir().unwrap();
        let cwd = temp.path().join("cwd");
        let root = temp.path().join("project");
        let state = temp.path().join("custom");
        let layout = Layout::resolve(
            loaded(Some(root.join(".swarmy/config.toml")), &root, state.clone()),
            &cwd,
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(layout.root, root);
        assert_eq!(layout.state_dir, state);
    }
    #[test]
    fn state_dir_env_applies_without_a_file() {
        let temp = tempfile::tempdir().unwrap();
        let cwd = temp.path();
        let state = cwd.join("other");
        let env = BTreeMap::from([
            ("HOME".into(), cwd.display().to_string()),
            ("SWARMY_STATE_DIR".into(), state.display().to_string()),
        ]);
        assert_eq!(
            Layout::resolve(loaded(None, cwd, state.clone()), cwd, &env)
                .unwrap()
                .state_dir,
            state
        );
    }
    #[test]
    fn dev_dir_override_is_absolute() {
        let temp = tempfile::tempdir().unwrap();
        let cwd = temp.path();
        let env = BTreeMap::from([
            ("HOME".into(), cwd.display().to_string()),
            ("SWARMY_DEV_DIR".into(), "elsewhere".into()),
        ]);
        assert_eq!(
            Layout::resolve(loaded(None, cwd, cwd.join(".swarmy")), cwd, &env)
                .unwrap()
                .stack,
            cwd.join("elsewhere")
        );
    }
    #[test]
    fn no_home_no_checkout_no_config_errors() {
        let temp = tempfile::tempdir().unwrap();
        let cwd = temp.path();
        let error = Layout::resolve(
            loaded(None, cwd, cwd.join(".swarmy")),
            cwd,
            &BTreeMap::new(),
        )
        .err()
        .unwrap();
        assert!(error.to_string().contains("HOME"));
    }
}
