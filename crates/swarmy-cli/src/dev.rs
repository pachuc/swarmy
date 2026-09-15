mod process;

use std::{
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

const SERVICES: [(&str, &str); 3] = [
    ("scheduler", "scheduler started"),
    ("worker", "worker ready"),
    ("gateway", "gateway ready"),
];
const START_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Subcommand)]
pub enum Command {
    /// Start the backing stack and supervised services in the background
    Up,
    /// Stop the services, supervisor, and backing stack, preserving data
    Down,
    /// Show process ids and uptime
    Status,
    /// Follow all service logs, or one service's log
    Logs {
        #[arg(value_parser = ["scheduler", "worker", "gateway", "supervisor"])]
        service: Option<String>,
    },
    #[command(hide = true)]
    Supervise { state: PathBuf },
}

struct Layout {
    config: PathBuf,
    root: PathBuf,
    state: PathBuf,
    repo: PathBuf,
}

impl Layout {
    fn discover() -> Result<Self> {
        let loaded = Settings::load()?;
        let cwd = std::env::current_dir()?;
        let repo = cwd
            .ancestors()
            .find(|dir| dir.join("scripts/dev-stack.sh").is_file())
            .map_or_else(
                || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.."),
                Path::to_owned,
            )
            .canonicalize()
            .context("cannot locate the checkout containing scripts/dev-stack.sh")?;
        let root = if loaded.path.is_some() {
            loaded.root
        } else if cwd.starts_with(&repo) {
            repo.clone()
        } else {
            cwd
        };
        let config = loaded
            .path
            .unwrap_or_else(|| root.join(".swarmy/config.toml"));
        let state = config
            .parent()
            .context("configuration has no parent")?
            .join("dev");
        Ok(Self {
            config,
            root,
            state,
            repo,
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

pub async fn run(command: Command) -> Result<()> {
    if let Command::Supervise { state } = command {
        return supervise(&state).await;
    }
    let layout = Layout::discover()?;
    match command {
        Command::Up => {
            let _lock = layout.lock()?;
            up(&layout).await
        }
        Command::Down => {
            let _lock = layout.lock()?;
            stop_services(&layout.state).await?;
            stack(&layout, "stop").await?;
            println!("services and stack: down");
            Ok(())
        }
        Command::Status => status(&layout).await,
        Command::Logs { service } => logs(&layout.state, service.as_deref()).await,
        Command::Supervise { .. } => unreachable!(),
    }
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

async fn stack(layout: &Layout, action: &str) -> Result<String> {
    let output = Process::new(layout.repo.join("scripts/dev-stack.sh"))
        .process_group(0)
        .env("PATH", crate::tools::search_path()?)
        .arg(action)
        .output()
        .await?;
    ensure!(
        output.status.success(),
        "dev stack {action} failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?)
}

async fn up(layout: &Layout) -> Result<()> {
    fs::create_dir_all(layout.state.join("logs"))?;
    let survivors: Vec<_> = ["supervisor", "scheduler", "worker", "gateway"]
        .into_iter()
        .filter(|name| Identity::read(&layout.state.join(format!("{name}.pid"))).is_some())
        .collect();
    if !survivors.is_empty() {
        println!("replacing recorded processes: {}", survivors.join(", "));
    }
    stop_services(&layout.state).await?;
    // CI and manual users may already have started the backing stack.
    let running = stack(layout, "status").await?;
    if running
        .lines()
        .filter(|line| line.contains(": up (pid "))
        .count()
        != 3
        || !layout.repo.join(".dev/env").is_file()
    {
        stack(layout, "start").await?;
    }
    let settings = prepare_settings(layout)?;
    for (name, _) in SERVICES {
        let executable = binary(name)?;
        ensure!(
            executable.is_file(),
            "missing {}; run cargo build --workspace --locked first",
            executable.display()
        );
    }
    let ready = layout.state.join("ready");
    if ready.exists() {
        fs::remove_file(&ready)?;
    }
    let log = File::create(layout.state.join("logs/supervisor.log"))?;
    let mut supervisor = Process::new(std::env::current_exe()?)
        .arg("dev")
        .arg("supervise")
        .arg(&layout.state)
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
            Identity::read(&layout.repo.join(format!(".dev/{name}.pid")))
                .map(|id| format!("{name}={}", id.pid))
        })
        .collect::<Vec<_>>();
    println!("stack: ready ({})", stack_pids.join(", "));
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

fn prepare_settings(layout: &Layout) -> Result<Settings> {
    let exports = swarmy_config::parse_exports(&fs::read_to_string(layout.repo.join(".dev/env"))?)?;
    let mut settings = if layout.config.exists() {
        Settings::read(&layout.config)?
    } else {
        Settings::default()
    };
    settings.apply_environment(&exports)?;
    if settings.credential_file.is_empty() {
        settings.credential_file = std::env::var_os("HOME")
            .map_or_else(|| layout.root.clone(), PathBuf::from)
            .join(".swarmy/auth.json")
            .to_string_lossy()
            .into_owned();
    }
    settings.resolve_paths(&layout.root);
    write_private(&layout.config, &settings.to_toml()?)?;
    // Load again so environment overrides win, without persisting those overrides.
    settings = Settings::load()?.settings;
    if settings.provider == "fake" {
        let script = Path::new(&settings.fake.script);
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

fn binary(name: &str) -> Result<PathBuf> {
    Ok(std::env::current_exe()?
        .parent()
        .context("executable has no parent")?
        .join(format!("swarmy-{name}")))
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

async fn supervise(state: &Path) -> Result<()> {
    // Register signal handlers before spawning so down during startup also cleans up.
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut children = Vec::new();
    let result = tokio::select! {
        result = serve(state, &mut children) => result,
        _ = interrupt.recv() => Ok(()),
        _ = terminate.recv() => Ok(()),
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

async fn serve(state: &Path, children: &mut Vec<(&'static str, Child)>) -> Result<()> {
    for (name, _) in SERVICES {
        let log = File::create(state.join(format!("logs/{name}.log")))?;
        let child = Process::new(binary(name)?)
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
            Identity::read(&layout.repo.join(format!(".dev/{name}.pid")))
                .map(|identity| (name, identity))
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
    for name in ["scheduler", "worker", "gateway", "supervisor"] {
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
