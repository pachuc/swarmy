mod check;
mod config;
mod process;

use std::{
    ffi::OsString,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use clap::Parser;
use foundationdb::{
    Database,
    directory::{Directory, DirectoryLayer},
};
use futures::future::try_join_all;
use jiff::Timestamp;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use swarmy_bus::{Bus, Config as BusConfig, SubjectToken};
use swarmy_core::{
    AgentId, Event, Message, MessageId, MessageRole, Part, SessionId, SessionRecord, SessionState,
    WakeReply,
};
use swarmy_store::{MAX_SCAN_LIMIT, Store, blob::ObjectBlobStore, runnable_partition};
use tempfile::TempDir;
use tokio::time::{Instant, sleep, timeout};
use ulid::Ulid;

use config::Config;
use process::{Kind, Process};

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let config = Config::parse();
    config.validate()?;
    if !config.no_start_stack {
        return config::with_stack();
    }
    let binaries = config.binaries()?;
    let seed = config.seed.unwrap_or_else(rand::random);
    tracing::info!(
        seed,
        ?config,
        "starting chaos run; use --seed to replay the kill schedule"
    );
    let _network = swarmy_store::boot();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?
        .block_on(run(&config, &binaries, seed))
}

struct Fixture {
    store: Store,
    bus: Bus,
    prefix: String,
    files: TempDir,
    processes: Vec<Process>,
    sessions: Vec<SessionId>,
}

impl Fixture {
    async fn new() -> Result<Self> {
        let prefix = format!("chaos_{}", Ulid::generate());
        let store = Store::open(
            Some(&swarmy_config::Settings::load()?.settings.fdb_cluster_file),
            Some(std::slice::from_ref(&prefix)),
            Arc::new(ObjectBlobStore::from_env()?),
        )
        .await?;
        let bus = Bus::connect(
            &swarmy_config::Settings::load()?.settings.nats_url,
            BusConfig {
                prefix: Some(SubjectToken::new(&prefix)?),
                ack_wait: Duration::from_millis(1200),
                max_deliver: 1000,
            },
        )
        .await?;
        Ok(Self {
            store,
            bus,
            prefix,
            files: tempfile::tempdir()?,
            processes: Vec::new(),
            sessions: Vec::new(),
        })
    }

    async fn start(&mut self, config: &Config, binaries: &Path) -> Result<()> {
        self.bus.setup(&[]).await?;
        std::fs::write(self.files.path().join("calls"), "")?;
        std::fs::write(
            self.files.path().join("script.json"),
            serde_json::to_vec(&serde_json::json!({
                "latency_ms": config.latency_ms,
                "request_based": {"steps": config.steps, "tool_steps": (0..config.steps-1).collect::<Vec<_>>(),
                    "final_answer": check::ANSWER}
            }))?,
        )?;
        let mut environment: Vec<(OsString, OsString)> = [
            ("SWARMY_PROVIDER", "fake"),
            ("SWARMY_STORE_DIRECTORY", &self.prefix),
            ("SWARMY_BUS_PREFIX", &self.prefix),
            ("SWARMY_WORKER_PARTITIONS", "0-3"),
            ("SWARMY_SCHEDULER_PARTITIONS", "0-3"),
            ("SWARMY_SCHEDULER_SCAN_INTERVAL_MS", "50"),
            ("SWARMY_SCHEDULER_RESEND_INTERVAL_MS", "100"),
            ("SWARMY_WORKER_LEASE_MS", "600"),
            ("SWARMY_WORKER_RECOVERY_INTERVAL_MS", "200"),
            ("SWARMY_BUS_ACK_WAIT_MS", "1200"),
            ("SWARMY_BUS_MAX_DELIVER", "1000"),
            ("SWARMY_GATEWAY_CONCURRENCY", "1"),
            ("TOKIO_WORKER_THREADS", "2"),
            ("RUST_LOG", "info"),
        ]
        .into_iter()
        .map(|(key, value)| (key.into(), value.into()))
        .collect();
        let mut shared = swarmy_config::Settings::load()?
            .settings
            .environment()
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect::<Vec<(OsString, OsString)>>();
        shared.append(&mut environment);
        environment = shared;
        environment.extend([
            (
                "SWARMY_FAKE_SCRIPT".into(),
                self.files.path().join("script.json").into_os_string(),
            ),
            (
                "SWARMY_FAKE_CALL_LOG".into(),
                self.files.path().join("calls").into_os_string(),
            ),
        ]);
        for (kind, count) in [
            (Kind::Scheduler, config.schedulers),
            (Kind::Worker, config.workers),
            (Kind::Gateway, config.gateways),
        ] {
            for index in 0..count {
                self.processes.push(Process::start(
                    kind,
                    index,
                    binaries,
                    self.files.path(),
                    &environment,
                )?);
            }
        }
        timeout(Duration::from_secs(30), async {
            loop {
                for process in &mut self.processes {
                    process.check()?;
                }
                if matches!(
                    self.bus
                        .request_wake(
                            SessionId::from_ulid(Ulid::generate()),
                            Duration::from_millis(200)
                        )
                        .await,
                    Ok(WakeReply::NotFound)
                ) {
                    return Ok::<_, anyhow::Error>(());
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .context("scheduler startup timed out")??;
        self.create_sessions(config.sessions).await
    }

    async fn create_sessions(&mut self, count: usize) -> Result<()> {
        for _ in 0..count {
            let id = loop {
                let id = SessionId::from_ulid(Ulid::generate());
                if runnable_partition(id) < 4 {
                    break id;
                }
            };
            self.sessions.push(id);
            self.store
                .create_session(
                    &SessionRecord {
                        session_id: id,
                        agent_id: AgentId::from_ulid(Ulid::generate()),
                        state: SessionState::Idle,
                        head_seq: 0,
                        snapshot_ref: None,
                    },
                    Timestamp::now(),
                )
                .await?;
            self.store
                .append_events(
                    id,
                    0,
                    &[Event::MessageAppended {
                        seq: 0,
                        message: Message {
                            id: MessageId::from_ulid(Ulid::generate()),
                            role: MessageRole::User,
                            parts: vec![Part::Text {
                                text: "Read the clock at each step, then finish.".into(),
                            }],
                        },
                    }],
                )
                .await?;
        }
        Ok(())
    }

    async fn exercise(&mut self, config: &Config, seed: u64) -> Result<usize> {
        let finished = AtomicBool::new(false);
        let sessions = async {
            try_join_all(
                self.sessions
                    .iter()
                    .map(|id| watch(&self.store, &self.bus, *id, config)),
            )
            .await?;
            finished.store(true, Ordering::SeqCst);
            Ok::<_, anyhow::Error>(())
        };
        let injection = inject(
            &mut self.processes,
            self.files.path(),
            config,
            seed,
            &finished,
        );
        let ((), gateway_kills) = tokio::try_join!(sessions, injection)?;
        // Stop consumers before taking the final call count, including any unexpected late retry.
        for process in &mut self.processes {
            process.check()?;
            process.stop().await?;
        }
        for id in &self.sessions {
            let session = self
                .store
                .fetch_session(*id)
                .await?
                .context("session disappeared")?;
            let mut events = Vec::new();
            read_through(&self.store, *id, &mut events, session.head_seq).await?;
            check::finished(&session, &events, config.steps)?;
        }
        let calls = call_count(self.files.path())?;
        check::calls(calls, config.sessions * config.steps, gateway_kills)?;
        tracing::info!(calls, gateway_kills, "all session and charge checks passed");
        Ok(gateway_kills)
    }

    async fn cleanup(&mut self) -> Result<()> {
        for process in &mut self.processes {
            process.stop().await?;
        }
        let blobs = ObjectBlobStore::from_env()?;
        for id in &self.sessions {
            if let Some(session) = self.store.fetch_session(*id).await?
                && let Some(snapshot) = session.snapshot_ref
            {
                blobs.delete(&snapshot.object_key).await?;
            }
        }
        // Like the service integration fixtures, remove only this run's directory and streams.
        let db = Database::new(Some(
            &swarmy_config::Settings::load()?.settings.fdb_cluster_file,
        ))?;
        let path = vec![self.prefix.clone()];
        db.run(|trx, _| {
            let path = &path;
            async move {
                DirectoryLayer::default()
                    .remove_if_exists(&trx, path)
                    .await?;
                Ok(())
            }
        })
        .await?;
        let context = async_nats::jetstream::new(
            async_nats::connect(swarmy_config::Settings::load()?.settings.nats_url).await?,
        );
        for stream in ["INFER_REQ", "SCHED_RUNNABLE", "TOOL_REMOTE", "TOOL_NODE"] {
            context
                .delete_stream(format!("{}_{stream}", self.prefix))
                .await?;
        }
        Ok(())
    }
}

async fn run(config: &Config, binaries: &Path, seed: u64) -> Result<()> {
    let started = Instant::now();
    let mut fixture = timeout(Duration::from_secs(30), Fixture::new())
        .await
        .context("stack connection timed out")??;
    tracing::info!(prefix = %fixture.prefix, logs = %fixture.files.path().display(), "isolated run created");
    let result = tokio::select! {
        result = async {
            timeout(Duration::from_secs(60), fixture.start(config, binaries)).await.context("session setup timed out")??;
            fixture.exercise(config, seed).await
        } => result,
        result = tokio::signal::ctrl_c() => { result?; Err(anyhow::anyhow!("interrupted")) }
    };
    let cleanup = timeout(Duration::from_secs(30), fixture.cleanup())
        .await
        .context("cleanup timed out")
        .and_then(std::convert::identity);
    if result.is_err() || cleanup.is_err() {
        let path = fixture.files.keep();
        tracing::error!(seed, logs = %path.display(), "chaos failed; retained script, call log, and service logs");
    }
    if let Err(error) = cleanup {
        tracing::error!(%error, "cleanup failed");
        result?;
        return Err(error);
    }
    result?;
    tracing::info!(
        seed,
        sessions = config.sessions,
        steps = config.steps,
        kills = config.kills,
        elapsed_secs = started.elapsed().as_secs_f64(),
        "chaos passed"
    );
    Ok(())
}

fn call_count(files: &Path) -> Result<usize> {
    let log = std::fs::read_to_string(files.join("calls"))?;
    ensure!(
        log.lines().all(|line| line == "call") && (log.is_empty() || log.ends_with('\n')),
        "malformed provider call log"
    );
    Ok(log.lines().count())
}

async fn inject(
    processes: &mut [Process],
    files: &Path,
    config: &Config,
    seed: u64,
    finished: &AtomicBool,
) -> Result<usize> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut gateway_kills = 0;
    // Do not spend the kill budget on process startup before inference begins.
    while config.kills > 0 && call_count(files)? == 0 {
        ensure!(
            !finished.load(Ordering::SeqCst),
            "sessions finished before failure injection began"
        );
        for process in &mut *processes {
            process.check()?;
        }
        sleep(Duration::from_millis(20)).await;
    }
    for kill in 1..=config.kills {
        let interval_ms = rng.random_range(config.min_interval_ms..=config.max_interval_ms);
        sleep(Duration::from_millis(interval_ms)).await;
        ensure!(
            !finished.load(Ordering::SeqCst),
            "sessions finished before kill {kill}/{}; increase latency or shorten kill intervals",
            config.kills
        );
        for process in &mut *processes {
            process.check()?;
        }
        let victim = rng.random_range(0..processes.len());
        let process = &mut processes[victim];
        process.restart().await?;
        gateway_kills += usize::from(process.kind == Kind::Gateway);
        tracing::info!(kill, victim = %process.name, interval_ms, "SIGKILL and restart");
    }
    while !finished.load(Ordering::SeqCst) {
        for process in &mut *processes {
            process.check()?;
        }
        sleep(Duration::from_millis(25)).await;
    }
    Ok(gateway_kills)
}

async fn watch(store: &Store, bus: &Bus, id: SessionId, config: &Config) -> Result<()> {
    let mut events = Vec::new();
    let mut last_state = None;
    let result = timeout(Duration::from_secs(config.session_timeout_secs), async {
        loop {
            match bus.request_wake(id, Duration::from_millis(200)).await {
                Ok(WakeReply::Runnable | WakeReply::Unchanged(_)) => break,
                Ok(reply) => bail!("session {id}: scheduler rejected wake: {reply:?}"),
                Err(_) => sleep(Duration::from_millis(25)).await,
            }
        }
        loop {
            let session = store
                .fetch_session(id)
                .await?
                .context("session disappeared")?;
            last_state = Some(session.state);
            read_through(store, id, &mut events, session.head_seq).await?;
            check::log(id, &events)?;
            if session.state == SessionState::Idle {
                check::finished(&session, &events, config.steps)?;
                return Ok::<_, anyhow::Error>(());
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    let tail = &events[events.len().saturating_sub(8)..];
    match result {
        Ok(result) => result.with_context(|| {
            format!("session {id}; last state={last_state:?}; last events={tail:?}")
        }),
        Err(_) => bail!(
            "session {id} timed out after {}s; last state={last_state:?}; last events={tail:?}",
            config.session_timeout_secs
        ),
    }
}

async fn read_through(
    store: &Store,
    id: SessionId,
    events: &mut Vec<Event>,
    head: u64,
) -> Result<()> {
    let mut after = events.last().map_or(0, Event::seq);
    while after < head {
        let page = store.read_events(id, after, MAX_SCAN_LIMIT).await?;
        ensure!(
            !page.is_empty(),
            "session {id}: log ended before head {head}"
        );
        let before = after;
        for event in page.into_iter().take_while(|event| event.seq() <= head) {
            after = event.seq();
            events.push(event);
        }
        ensure!(
            after > before,
            "session {id}: log page made no progress before head {head}"
        );
    }
    Ok(())
}
