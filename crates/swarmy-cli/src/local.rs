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
        .save(state.join("supervisor.pid"))?;
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
            Identity::read(stack_dir.join(format!("{name}.pid"))).map(|identity| (name, identity))
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
        if let Some(identity) = Identity::read(state.join(format!("{name}.pid"))) {
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
        || SERVICES.iter().map(|(name, _)| *name).collect(),
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
