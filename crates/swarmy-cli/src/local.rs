//! Local swarms: the backing stack and four control services on this machine.
//! Local swarms share fixed ports, so one runs at a time.
pub(crate) mod process;
use anyhow::{Context, Result, bail, ensure};
use process::Identity;
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use swarmy_config::Settings;
use tokio::{
    process::{Child, Command as Process},
    time::{sleep, timeout},
};
pub(crate) const SERVICES: [(&str, &str); 4] = [
    ("scheduler", "scheduler started"),
    ("worker", "worker ready"),
    ("gateway", "gateway ready"),
    ("api", "api ready"),
];
const START_TIMEOUT: Duration = Duration::from_secs(30);

/// One supervised backing service, parsed once by clap instead of matched as a string.
#[derive(Clone, Copy, clap::ValueEnum)]
pub(crate) enum Service {
    Scheduler,
    Worker,
    Gateway,
    Api,
    Supervisor,
}

/// `bash -c SCRIPT NAME`: run a compiled-in script; NAME becomes its $0.
pub(crate) fn embedded(script: &str, name: &str) -> Process {
    let mut command = Process::new("bash");
    command.arg("-c").arg(script).arg(name);
    command
}

pub(crate) async fn stack(dir: &Path, action: &str) -> Result<String> {
    // The same compiled-in script is used by the node and local dev stack.
    let output = embedded(swarmy_cloud::node_files::DEV_STACK, "swarmy local stack")
        .arg(action)
        .env("SWARMY_DEV_DIR", dir)
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

pub(crate) fn ensure_keyring() -> Result<()> {
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

pub(crate) async fn launch(
    state: &Path,
    root: &Path,
    core: &Path,
    settings: &Settings,
) -> Result<u32> {
    let ready = state.join("ready");
    if ready.exists() {
        fs::remove_file(&ready)?;
    }
    let log = File::create(state.join("logs/supervisor.log"))?;
    let mut supervisor = Process::new(std::env::current_exe()?)
        .arg("swarm")
        .arg("supervise")
        .arg(state)
        .arg(core)
        .current_dir(root)
        .env_remove("SWARMY_API_URL")
        .env_remove("SWARMY_SWARM")
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
        .save(&state.join("supervisor.pid"))?;
    let result = timeout(START_TIMEOUT, async {
        loop {
            if let Some(exit) = supervisor.try_wait()? {
                bail!(
                    "supervisor exited: {exit}; see {}",
                    state.join("logs/supervisor.log").display()
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
        let cleanup = stop_services(state).await;
        if supervisor.try_wait()?.is_none() {
            supervisor.kill().await?;
        }
        cleanup?;
        return Err(error);
    }
    Ok(pid)
}

pub(crate) async fn ensure_stack(dir: &Path) -> Result<()> {
    let running = stack(dir, "status").await?;
    if running
        .lines()
        .filter(|line| line.contains(": up (pid "))
        .count()
        != 3
        || !dir.join("env").is_file()
    {
        stack(dir, "start").await?;
    }
    Ok(())
}

pub(crate) async fn stop_services(state: &Path) -> Result<()> {
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

pub(crate) async fn supervise(state: &Path, core: &Path) -> Result<()> {
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
                swarmy_config::write_private(&state.join("ready"), b"ready\n")?;
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

pub(crate) async fn status(stack_dir: &Path, state: &Path) -> Result<()> {
    let ticks = process::clock_ticks().await?;
    let stack = ["fdb", "nats", "seaweed"]
        .into_iter()
        .filter_map(|name| {
            Identity::read(&stack_dir.join(format!("{name}.pid"))).map(|identity| (name, identity))
        })
        .collect::<Vec<_>>();
    if stack.is_empty() {
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
        if let Some(identity) = Identity::read(&state.join(format!("{name}.pid"))) {
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

pub(crate) async fn logs(state: &Path, service: Option<Service>) -> Result<()> {
    let service = service.map(|s| crate::cost_command::value_name(&s));
    let names = service.map_or_else(
        || {
            SERVICES
                .iter()
                .map(|(name, _)| (*name).to_owned())
                .collect()
        },
        |name| vec![name],
    );
    let mut command = Process::new("tail");
    command.args(["-n", "30", "-F"]);
    for name in names {
        let path = state.join(format!("logs/{name}.log"));
        ensure!(path.exists(), "no log for {name}; start the services first");
        command.arg(path);
    }
    let mut child = command.kill_on_drop(true).spawn()?;
    tokio::select! {
        result = child.wait() => { ensure!(result?.success(), "tail failed"); },
        result = tokio::signal::ctrl_c() => { result?; child.kill().await?; },
    }
    Ok(())
}

pub(crate) fn survivors(state: &Path) -> Vec<&'static str> {
    ["supervisor", "scheduler", "worker", "gateway", "api"]
        .into_iter()
        .filter(|name| Identity::read(&state.join(format!("{name}.pid"))).is_some())
        .collect()
}
pub(crate) fn print_started(stack: &Path, state: &Path, pid: u32) -> Result<()> {
    let pids = ["fdb", "nats", "seaweed"]
        .into_iter()
        .filter_map(|name| {
            Identity::read(&stack.join(format!("{name}.pid")))
                .map(|id| format!("{name}={}", id.pid))
        })
        .collect::<Vec<_>>();
    println!("stack: ready ({})", pids.join(", "));
    for (name, _) in SERVICES {
        let id = Identity::read(&state.join(format!("{name}.pid")))
            .context("service exited during startup")?;
        println!("{name}: ready (pid {})", id.pid);
    }
    println!(
        "supervisor: running (pid {pid}); logs: {}",
        state.join("logs").display()
    );
    Ok(())
}

use fs2::FileExt;
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;
use swarmy_config::{
    create_private_dir,
    paths::Dirs,
    swarm::{Lifecycle, Registry, SwarmSpec},
};

pub(crate) struct LocalSwarm {
    pub(crate) name: String,
    pub(crate) lifecycle: Lifecycle,
    pub(crate) record_dir: PathBuf,
    pub(crate) state: PathBuf,
    pub(crate) stack: PathBuf,
    pub(crate) owned_stack: bool,
}
impl LocalSwarm {
    pub(crate) fn new(registry: &Registry, spec: &SwarmSpec) -> Result<Self> {
        let group = spec.local_group().with_context(|| {
            format!(
                "swarm \"{}\" is not a local swarm; only local swarms can be brought up so far",
                spec.name
            )
        })?;
        let record_dir = registry.swarm_dir(&spec.name)?;
        Ok(Self {
            name: spec.name.clone(),
            lifecycle: spec.lifecycle,
            state: registry.state_dir(&spec.name)?,
            stack: group
                .stack_dir
                .clone()
                .unwrap_or_else(|| record_dir.join("stack")),
            owned_stack: group.stack_dir.is_none(),
            record_dir,
        })
    }
    fn running(&self) -> (bool, bool) {
        (
            !survivors(&self.state).is_empty(),
            ["fdb", "nats", "seaweed"]
                .iter()
                .any(|name| Identity::read(&self.stack.join(format!("{name}.pid"))).is_some()),
        )
    }
}
pub(crate) struct Start {
    pub(crate) core_bin: Option<PathBuf>,
    pub(crate) register_image: bool,
    pub(crate) require_record: bool,
    pub(crate) allow_version_mismatch: bool,
}
fn xdg_environment() -> BTreeMap<String, String> {
    [
        "HOME",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "XDG_STATE_HOME",
        "XDG_CACHE_HOME",
    ]
    .into_iter()
    .filter_map(|key| std::env::var(key).ok().map(|value| (key.into(), value)))
    .collect()
}
fn checkout_stack(cwd: &Path) -> Option<PathBuf> {
    cwd.ancestors()
        .find(|p| p.join("scripts/dev-stack.sh").is_file())
        .and_then(|p| p.canonicalize().ok())
        .map(|p| p.join(".dev"))
}
struct Tuning {
    path: Option<PathBuf>,
    root: Option<PathBuf>,
    project: bool,
}
impl Tuning {
    fn discover(cwd: &Path, environment: &BTreeMap<String, String>) -> Self {
        let path = Settings::discover_from(cwd, environment);
        let project = path
            .as_deref()
            .and_then(swarmy_config::project_root)
            .is_some();
        let root = path.as_deref().and_then(|p| {
            swarmy_config::project_root(p)
                .or_else(|| p.parent())
                .map(Path::to_path_buf)
        });
        Self {
            path,
            root,
            project,
        }
    }
}
fn service_settings(
    tuning: &Tuning,
    state: &Path,
    exports: &BTreeMap<String, String>,
    environment: &BTreeMap<String, String>,
    token: Option<String>,
) -> Result<Settings> {
    let mut settings = tuning
        .path
        .as_ref()
        .map_or_else(|| Ok(Settings::default()), |path| Settings::read(path))?;
    settings.state_dir = state.to_path_buf();
    let root = tuning.root.as_deref().unwrap_or(state);
    settings.fill_default_locations(root, tuning.project, environment);
    let defaults = swarmy_config::Fake::default();
    if settings.fake.script == defaults.script {
        settings.fake.script = state.join("fake.json");
    }
    if settings.fake.call_log == defaults.call_log {
        settings.fake.call_log = state.join("calls.log");
    }
    settings.resolve_paths(root);
    settings.apply_environment(exports)?;
    settings.state_dir = state.to_path_buf();
    if let Some(token) = token.filter(|t| !t.is_empty()) {
        settings.api.token = token;
    }
    if settings.api.token.is_empty() {
        settings.api.token = ulid::Ulid::generate().to_string();
    }
    Ok(settings)
}
fn api_url(settings: &Settings) -> String {
    settings
        .api
        .url
        .clone()
        .unwrap_or_else(|| format!("http://{}", settings.api.listen))
}
fn prepare_fake(settings: &Settings) -> Result<()> {
    if settings.selection.provider == "fake" {
        if let Some(parent) = settings.fake.script.parent() {
            fs::create_dir_all(parent)?;
        }
        if !settings.fake.script.exists() {
            swarmy_config::write_private(&settings.fake.script, br#"{"request_based":{"steps":1,"tool_steps":[],"final_answer":"Hello from swarmy!"}}"#)?;
        }
        if let Some(parent) = settings.fake.call_log.parent() {
            fs::create_dir_all(parent)?;
        }
    }
    Ok(())
}
fn lock(dirs: &Dirs) -> Result<File> {
    create_private_dir(&dirs.state)?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(dirs.state.join("local.lock"))?;
    file.try_lock_exclusive()
        .context("another local swarm lifecycle command is in progress")?;
    Ok(file)
}
fn spec(registry: &Registry, name: &str) -> Result<SwarmSpec> {
    registry.read_spec(name)?.with_context(|| {
        format!(
            "no swarm spec named \"{name}\"; create one with swarmy swarm create {name} --local"
        )
    })
}
fn ensure_only_running(registry: &Registry, swarm: &LocalSwarm) -> Result<()> {
    for name in registry.list()? {
        if name == swarm.name {
            continue;
        }
        if let Some(other) = registry
            .read_spec(&name)?
            .and_then(|s| LocalSwarm::new(registry, &s).ok())
        {
            let (services, stack) = other.running();
            if services || (other.stack != swarm.stack && stack) {
                bail!(
                    "local swarm \"{name}\" is running; stop it first with swarmy swarm stop {name} (local swarms share fixed ports, so one runs at a time)"
                );
            }
        }
    }
    Ok(())
}
pub(crate) fn create(
    registry: &Registry,
    name: &str,
    persistent: bool,
    stack_dir: Option<PathBuf>,
    json: bool,
) -> Result<()> {
    if registry.read_spec(name)?.is_some() || registry.read_record(name)?.is_some() {
        bail!("swarm \"{name}\" already exists; remove it with swarmy swarm rm {name}");
    }
    let cwd = std::env::current_dir()?;
    let checkout = checkout_stack(&cwd);
    let detected = stack_dir.is_none() && checkout.is_some();
    let chosen = stack_dir
        .map(|p| if p.is_absolute() { p } else { cwd.join(p) })
        .or(checkout);
    let lifecycle = if persistent {
        Lifecycle::Persistent
    } else {
        Lifecycle::Ephemeral
    };
    let spec = SwarmSpec::local(name, lifecycle, chosen.clone())?;
    registry.write_spec(&spec)?;
    let current = registry.current()?.is_none();
    if current {
        registry.set_current(name)?;
    }
    let effective = chosen.unwrap_or(registry.swarm_dir(name)?.join("stack"));
    let path = registry.spec_path(name)?;
    if json {
        println!(
            "{}",
            serde_json::json!({"created":name,"spec":path,"stack_dir":effective,"current":current})
        );
    } else {
        println!(
            "created swarm {name} (local, {}): {}",
            if persistent {
                "persistent"
            } else {
                "ephemeral"
            },
            path.display()
        );
        println!(
            "backing stack: {}{}",
            effective.display(),
            if detected {
                " (the checkout's .dev, shared with scripts/dev-stack.sh)"
            } else {
                ""
            }
        );
        println!("start it with swarmy swarm up {name}");
    }
    Ok(())
}
fn save_settings(swarm: &LocalSwarm, settings: &Settings) -> Result<()> {
    swarmy_config::write_private(
        &swarm.record_dir.join("services.toml"),
        settings.to_toml()?.as_bytes(),
    )?;
    prepare_fake(settings)
}
async fn register_default_image(
    registry: &Registry,
    swarm: &LocalSwarm,
    tuning: &Tuning,
    settings: &Settings,
    core: &Path,
    exports: &BTreeMap<String, String>,
    environment: &BTreeMap<String, String>,
) -> Result<Option<u32>> {
    if let Some(default) = settings.selection.default_image.as_deref() {
        if crate::image::other_release_image(default) {
            println!(
                "note: default image {default} is from another release; run swarmy image pull base-ubuntu --set-default and swarmy swarm up {} to use {}",
                swarm.name,
                crate::image::release_image(swarmy_version::RELEASE_TAG)
            );
        }
        return Ok(None);
    }
    match crate::image::pull_release_image(
        Some((&api_url(settings), &settings.api.token)),
        &crate::image::image_cache()?,
        swarmy_version::RELEASE_TAG,
        swarmy_cloud::release::Target::host()?,
        false,
    )
    .await
    {
        Ok(Some(_)) => {
            let image = crate::image::release_image(swarmy_version::RELEASE_TAG);
            let path = tuning
                .path
                .clone()
                .or_else(|| Settings::user_config_path(environment))
                .context("HOME or XDG_CONFIG_HOME is required to save the default image")?;
            crate::image::set_default_image(&path, &image)?;
            println!("default image: {image}; restarting services");
            stop_services(&swarm.state).await?;
            let tuning = Tuning::discover(&std::env::current_dir()?, environment);
            let settings = service_settings(
                &tuning,
                &swarm.state,
                exports,
                environment,
                Some(settings.api.token.clone()),
            )?;
            save_settings(swarm, &settings)?;
            registry.set_api(&swarm.name, &api_url(&settings), &settings.api.token)?;
            Ok(Some(
                launch(
                    &swarm.state,
                    tuning.root.as_deref().unwrap_or(&swarm.state),
                    core,
                    &settings,
                )
                .await?,
            ))
        }
        Ok(None) => {
            println!(
                "default image: none (development build); build one from a checkout with swarmy image build images/base-ubuntu and set selection.default_image in your tuning file"
            );
            Ok(None)
        }
        Err(error) => {
            eprintln!(
                "warning: could not register the release base image: {error:#}\nRetry with swarmy image pull base-ubuntu --set-default, then swarmy swarm up {}",
                swarm.name
            );
            Ok(None)
        }
    }
}
pub(crate) async fn up(registry: &Registry, name: &str, start: Start) -> Result<()> {
    let swarm = LocalSwarm::new(registry, &spec(registry, name)?)?;
    if start.require_record && registry.read_record(name)?.is_none() {
        bail!("swarm \"{name}\" has not been brought up; run swarmy swarm up {name}");
    }
    let _guard = lock(registry.dirs())?;
    ensure_only_running(registry, &swarm)?;
    ensure_keyring()?;
    let core = crate::core_command::resolve_core(start.core_bin, None).await?;
    crate::core_command::check_versions(&core, start.allow_version_mismatch).await?;
    create_private_dir(&swarm.record_dir)?;
    create_private_dir(&swarm.state)?;
    fs::create_dir_all(swarm.state.join("logs"))?;
    let live = survivors(&swarm.state);
    if !live.is_empty() {
        println!("replacing recorded processes: {}", live.join(", "));
    }
    stop_services(&swarm.state).await?;
    ensure_stack(&swarm.stack).await?;
    let exports = swarmy_config::parse_exports(&fs::read_to_string(swarm.stack.join("env"))?)?;
    let environment = xdg_environment();
    let tuning = Tuning::discover(&std::env::current_dir()?, &environment);
    let token = registry
        .read_record(name)?
        .and_then(|r| r.api.token)
        .filter(|t| !t.is_empty());
    let settings = service_settings(&tuning, &swarm.state, &exports, &environment, token)?;
    save_settings(&swarm, &settings)?;
    let url = api_url(&settings);
    registry.set_api(name, &url, &settings.api.token)?;
    let mut pid = launch(
        &swarm.state,
        tuning.root.as_deref().unwrap_or(&swarm.state),
        &core.path,
        &settings,
    )
    .await?;
    if start.register_image
        && let Some(new_pid) = register_default_image(
            registry,
            &swarm,
            &tuning,
            &settings,
            &core.path,
            &exports,
            &environment,
        )
        .await?
    {
        pid = new_pid;
    }
    print_started(&swarm.stack, &swarm.state, pid)?;
    let current = registry.current()?.is_none();
    if current {
        registry.set_current(name)?;
    }
    println!(
        "swarm {name}: API {url}{}",
        if current {
            " (current)".to_owned()
        } else {
            format!("; select it with --swarm {name} or swarmy swarm use {name}")
        }
    );
    Ok(())
}
pub(crate) async fn stop(registry: &Registry, name: &str) -> Result<()> {
    let swarm = LocalSwarm::new(registry, &spec(registry, name)?)?;
    let _guard = lock(registry.dirs())?;
    stop_services(&swarm.state).await?;
    if swarm.stack.exists() {
        stack(&swarm.stack, "stop").await?;
    }
    println!("services and stack: stopped; data kept");
    Ok(())
}
pub(crate) async fn down(registry: &Registry, name: &str, destroy: bool) -> Result<()> {
    let swarm = LocalSwarm::new(registry, &spec(registry, name)?)?;
    if swarm.lifecycle == Lifecycle::Persistent && !destroy {
        bail!(
            "swarm \"{name}\" is persistent: swarmy swarm down deletes its data. Pass --destroy to delete it, or run swarmy swarm stop {name} to keep it"
        );
    }
    let _guard = lock(registry.dirs())?;
    stop_services(&swarm.state).await?;
    if swarm.stack.exists() {
        stack(&swarm.stack, "stop").await?;
    }
    println!("services and stack: down");
    for path in [&swarm.record_dir, &swarm.state] {
        if path.exists() {
            fs::remove_dir_all(path)?;
        }
    }
    println!(
        "swarm {name}: data deleted; the spec stays (swarmy swarm up {name} starts a fresh swarm)"
    );
    if !swarm.owned_stack {
        println!(
            "stack directory {} kept: stack_dir is outside the swarm",
            swarm.stack.display()
        );
    }
    Ok(())
}
pub(crate) async fn show_status(registry: &Registry, name: &str) -> Result<()> {
    let swarm = LocalSwarm::new(registry, &spec(registry, name)?)?;
    println!(
        "swarm: {name} (local, {})",
        if swarm.lifecycle == Lifecycle::Persistent {
            "persistent"
        } else {
            "ephemeral"
        }
    );
    println!(
        "endpoint: {}",
        registry
            .read_record(name)?
            .and_then(|r| r.api.url)
            .unwrap_or_else(|| "not up".into())
    );
    status(&swarm.stack, &swarm.state).await
}
pub(crate) async fn follow_logs(
    registry: &Registry,
    name: &str,
    service: Option<Service>,
) -> Result<()> {
    let swarm = LocalSwarm::new(registry, &spec(registry, name)?)?;
    logs(&swarm.state, service).await
}
pub(crate) fn refuse_running(registry: &Registry, spec: &SwarmSpec) -> Result<()> {
    if spec.local_group().is_some() {
        let swarm = LocalSwarm::new(registry, spec)?;
        let (services, stack) = swarm.running();
        if services || (swarm.owned_stack && stack) {
            bail!(
                "swarm \"{}\" is running on this machine; stop it first with swarmy swarm down {} or swarmy swarm stop {}",
                spec.name,
                spec.name,
                spec.name
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    #[test]
    fn checkout_stack_finds_the_checkout() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        fs::create_dir_all(repo.join("scripts")).unwrap();
        fs::write(repo.join("scripts/dev-stack.sh"), "").unwrap();
        fs::create_dir_all(repo.join("a/b")).unwrap();
        assert_eq!(
            checkout_stack(&repo.join("a/b")),
            Some(repo.canonicalize().unwrap().join(".dev"))
        );
        assert_eq!(checkout_stack(temp.path()), None);
    }
    #[test]
    fn service_settings_use_the_swarm_locations() {
        let temp = tempfile::tempdir().unwrap();
        let state = temp.path().join("state");
        let env = BTreeMap::from([("HOME".into(), temp.path().display().to_string())]);
        let exports = BTreeMap::from([(
            "SWARMY_FDB_CLUSTER_FILE".into(),
            temp.path().join("fdb.cluster").display().to_string(),
        )]);
        let tuning = Tuning::discover(temp.path(), &env);
        let settings = service_settings(&tuning, &state, &exports, &env, Some("t".into())).unwrap();
        assert_eq!(settings.state_dir, state);
        assert_eq!(settings.fake.script, state.join("fake.json"));
        assert_eq!(settings.fake.call_log, state.join("calls.log"));
        assert_eq!(settings.store.cluster_file, temp.path().join("fdb.cluster"));
        assert_eq!(settings.api.token, "t");
        assert!(
            !service_settings(&tuning, &state, &exports, &env, None)
                .unwrap()
                .api
                .token
                .is_empty()
        );
        let path = temp.path().join(".swarmy/config.toml");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, "state_dir = '/elsewhere'\n[api]\ntoken = 'file'\n").unwrap();
        let tuning = Tuning::discover(temp.path(), &env);
        let settings = service_settings(&tuning, &state, &exports, &env, None).unwrap();
        assert_eq!(settings.state_dir, state);
        assert_eq!(settings.api.token, "file");
    }
    #[test]
    fn api_url_prefers_the_configured_url() {
        let mut settings = Settings::default();
        settings.api.url = Some("https://example.test".into());
        assert_eq!(api_url(&settings), "https://example.test");
    }
}
