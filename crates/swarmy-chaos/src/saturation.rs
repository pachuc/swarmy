//! Rate-limit saturation acceptance: one HTTP fake with two rate-limited API keys behind one route, named agents chatting, and checks on conversations, leases, breakers, and metering.
use crate::{Config, Fixture, Kind, Process};
use anyhow::{Context, Result, ensure};
use futures::future::try_join_all;
use jiff::Timestamp;
use serde_json::json;
use std::fmt::Write;
use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use swarmy_bus::Bus;
use swarmy_client::Client;
use swarmy_core::{
    AgentId, CredentialBookkeeping, CredentialEntryKind, CredentialKind, CredentialRecord,
    CredentialScope, Event, Message, MessageRole, Part, RouteStep, SessionId, SessionState,
};
use swarmy_store::Store;
use tokio::time::{interval, timeout};
use ulid::Ulid;
use wiremock::{
    Mock, MockServer, Request, ResponseTemplate,
    matchers::{method, path},
};

const ANSWER: &str = "saturation turn complete";
const ROUTE: &str = "sat";
const MODEL: &str = "sat-model";
const WINDOW_MS: i64 = 2_000;
const LIMIT: u64 = 2;
const RESPONSE_DELAY: Duration = Duration::from_millis(50);
const TOKEN_BUDGET: u64 = 10_000_000;

#[derive(Clone, Debug)]
struct Arrival {
    at_ms: i64,
    status: u16,
    retry_after_secs: u64,
    requests_remaining: u64,
    tokens_remaining: u64,
    input_tokens: u64,
    output_tokens: u64,
}
struct Limiter {
    key: String,
    state: Mutex<LimiterState>,
}
struct LimiterState {
    window: i64,
    used: u64,
    tokens_left: u64,
    log: Vec<Arrival>,
}
impl Limiter {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let messages = request
            .body_json::<serde_json::Value>()
            .ok()
            .and_then(|v| v["messages"].as_array().map(Vec::len))
            .unwrap_or(0);
        let mut state = self.state.lock().expect("limiter lock poisoned");
        let now_ms = Timestamp::now().as_millisecond();
        let window = now_ms.div_euclid(WINDOW_MS);
        let reset_ms = (window + 1) * WINDOW_MS - now_ms;
        if state.window != window {
            state.window = window;
            state.used = 0;
        }
        let reset = format!("{reset_ms}ms");
        if state.used >= LIMIT {
            let retry = u64::try_from(reset_ms)
                .expect("positive reset")
                .div_ceil(1000)
                .max(1);
            let tokens = state.tokens_left;
            state.log.push(Arrival {
                at_ms: now_ms,
                status: 429,
                retry_after_secs: retry,
                requests_remaining: 0,
                tokens_remaining: tokens,
                input_tokens: 0,
                output_tokens: 0,
            });
            let body = json!({"error": {
                "message": "Rate limit reached for requests",
                "type": "requests",
                "code": "rate_limit_exceeded"
            }});
            return ResponseTemplate::new(429)
                .insert_header("retry-after", retry.to_string())
                .insert_header("x-ratelimit-limit-requests", LIMIT.to_string())
                .insert_header("x-ratelimit-remaining-requests", "0")
                .insert_header("x-ratelimit-reset-requests", reset)
                .set_body_json(body);
        }
        state.used += 1;
        let input = u64::try_from(messages).expect("message count fits u64") * 10;
        let output = 5;
        state.tokens_left = state.tokens_left.saturating_sub(input + output);
        let remaining = LIMIT - state.used;
        let tokens = state.tokens_left;
        state.log.push(Arrival {
            at_ms: now_ms,
            status: 200,
            retry_after_secs: 0,
            requests_remaining: remaining,
            tokens_remaining: tokens,
            input_tokens: input,
            output_tokens: output,
        });
        ResponseTemplate::new(200)
            .insert_header("x-ratelimit-limit-requests", LIMIT.to_string())
            .insert_header("x-ratelimit-remaining-requests", remaining.to_string())
            .insert_header("x-ratelimit-reset-requests", reset)
            .insert_header("x-ratelimit-remaining-tokens", tokens.to_string())
            .insert_header("x-ratelimit-reset-tokens", "60s")
            .set_body_raw(sse_body(input, output), "text/event-stream")
            .set_delay(RESPONSE_DELAY)
    }
}
struct Fake {
    label: &'static str,
    server: Arc<MockServer>,
    limiter: Arc<Limiter>,
}
impl Fake {
    fn new(label: &'static str, server: Arc<MockServer>) -> Self {
        Self {
            label,
            server,
            limiter: Arc::new(Limiter {
                key: format!("{label}-secret"),
                state: Mutex::new(LimiterState {
                    window: -1,
                    used: 0,
                    tokens_left: TOKEN_BUDGET,
                    log: Vec::new(),
                }),
            }),
        }
    }
    fn base_url(&self) -> String {
        format!("{}/v1", self.server.uri())
    }
    fn entry(&self) -> String {
        format!("sat/{}", self.label)
    }
    fn log(&self) -> Vec<Arrival> {
        let mut log = self
            .limiter
            .state
            .lock()
            .expect("limiter lock poisoned")
            .log
            .clone();
        log.sort_by_key(|a| a.at_ms);
        log
    }
}
async fn start_fakes() -> [Fake; 2] {
    let server = Arc::new(MockServer::start().await);
    let fakes = [
        Fake::new("a-key", Arc::clone(&server)),
        Fake::new("b-key", server),
    ];
    let limiters = fakes
        .iter()
        .map(|fake| Arc::clone(&fake.limiter))
        .collect::<Vec<_>>();
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(move |request: &Request| {
            let auth = request
                .headers
                .get("authorization")
                .and_then(|h| h.to_str().ok());
            limiters
                .iter()
                .find(|limiter| auth == Some(format!("Bearer {}", limiter.key).as_str()))
                .map_or_else(
                    || {
                        ResponseTemplate::new(401)
                            .set_body_json(json!({"error":{"message":"wrong key"}}))
                    },
                    |limiter| limiter.respond(request),
                )
        })
        .mount(&fakes[0].server)
        .await;
    fakes
}
fn sse_body(input: u64, output: u64) -> String {
    let frames = [
        json!({"choices":[{"index":0,"delta":{"content":ANSWER},"finish_reason":null}]}),
        json!({"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}),
        json!({"choices": [], "usage": {
            "prompt_tokens": input,
            "completion_tokens": output,
            "total_tokens": input + output
        }}),
    ];
    let mut body = String::new();
    for frame in frames {
        write!(body, "data: {frame}\n\n").expect("writing to String cannot fail");
    }
    body.push_str("data: [DONE]\n\n");
    body
}
struct Api {
    url: String,
    listen: String,
    token: String,
}
impl Api {
    fn new() -> Result<Self> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        drop(listener);
        Ok(Self {
            url: format!("http://127.0.0.1:{port}"),
            listen: format!("127.0.0.1:{port}"),
            token: Ulid::generate().to_string(),
        })
    }
}
async fn configure(f: &Fixture, fakes: &[Fake]) -> Result<()> {
    let keyring = swarmy_config::Keyring::generate_at(&f.files.path().join("keyring"))?;
    for fake in fakes {
        f.store
            .credentials(keyring.clone())
            .put_entry(
                CredentialScope::Cluster,
                ROUTE,
                fake.label,
                &CredentialRecord {
                    bookkeeping: CredentialBookkeeping::default(),
                    kind: CredentialKind::ApiKey {
                        key: fake.limiter.key.clone(),
                        extra: BTreeMap::new(),
                    },
                    updated_at: Timestamp::now(),
                },
            )
            .await?;
    }
    f.store
        .put_route(
            ROUTE,
            &fakes
                .iter()
                .map(|fake| RouteStep {
                    provider: ROUTE.into(),
                    entry: fake.label.into(),
                    model: None,
                })
                .collect::<Vec<_>>(),
        )
        .await?;
    Ok(())
}
fn environment(
    f: &Fixture,
    config: &Config,
    fakes: &[Fake],
    api: &Api,
) -> Result<Vec<(OsString, OsString)>> {
    let mut env = f.service_environment(config)?;
    let providers = json!({"sat":{"api":"OpenAiCompletions","base_url":fakes[0].base_url()}});
    let models = json!([{"provider":ROUTE,"id":MODEL,"cost":{"input":1.0,"output":2.0}}]);
    for (key, value) in [
        ("SWARMY_PROVIDER", ROUTE.into()),
        ("SWARMY_MODEL", MODEL.into()),
        ("SWARMY_PROVIDERS", ROUTE.into()),
        ("SWARMY_CUSTOM_PROVIDERS", providers.to_string()),
        ("SWARMY_MODELS", models.to_string()),
        ("SWARMY_REASONING_EFFORT", "none".into()),
        (
            "SWARMY_KEYRING",
            f.files.path().join("keyring").display().to_string(),
        ),
        ("SWARMY_INFERENCE_DEFAULT_ROUTE", ROUTE.into()),
        ("SWARMY_INFERENCE_MAX_WAIT_SECONDS", "600".into()),
        ("SWARMY_WORKER_PARTITIONS", "0-255".into()),
        ("SWARMY_SCHEDULER_PARTITIONS", "0-255".into()),
        ("SWARMY_GATEWAY_CONCURRENCY", "1".into()),
        ("SWARMY_API_LISTEN", api.listen.clone()),
        ("SWARMY_API_TOKEN", api.token.clone()),
    ] {
        env.push((key.into(), value.into()));
    }
    Ok(env)
}
async fn start_services(
    f: &mut Fixture,
    config: &Config,
    binaries: &Path,
    environment: Vec<(OsString, OsString)>,
) -> Result<()> {
    for (kind, count) in [
        (Kind::Scheduler, config.schedulers),
        (Kind::Worker, config.workers),
        (Kind::Gateway, config.gateways),
        (Kind::Api, 1),
    ] {
        for index in 0..count {
            f.processes.push(Process::start(
                kind,
                index,
                binaries,
                f.files.path(),
                &environment,
            )?);
        }
    }
    f.ready().await?;
    f.environment = environment;
    Ok(())
}
async fn create_agents(f: &mut Fixture, count: usize) -> Result<Vec<(AgentId, SessionId)>> {
    let mut agents = Vec::with_capacity(count);
    for i in 0..count {
        let agent = f
            .store
            .create_agent(
                &format!("saturation-{i}"),
                "chaos:test",
                "Chat through a saturated route",
                Timestamp::now(),
                None,
            )
            .await?
            .agent_id;
        let (id, _) = f.store.open_main_session(agent, Timestamp::now()).await?;
        f.sessions.push(id);
        agents.push((agent, id));
    }
    Ok(agents)
}
async fn chat(
    store: &Store,
    bus: &Bus,
    client: &Client,
    index: usize,
    id: SessionId,
    turns: usize,
) -> Result<()> {
    for turn in 0..turns {
        let head = crate::send_user(
            store,
            bus,
            id,
            &format!("Agent {index}, turn {turn}: answer in one line."),
        )
        .await?;
        let session = client
            .wait_idle(&id.to_string(), head, 120_000)
            .await
            .with_context(|| format!("agent {index} turn {turn} session {id}"))?;
        ensure!(
            session.state == swarmy_api_types::SessionState::Idle,
            "agent {index}: session {id} ended {:?}; this scenario expects one session per agent",
            session.state
        );
    }
    Ok(())
}
async fn audit(
    store: &Store,
    processes: &mut [Process],
    sessions: &[SessionId],
    finished: &AtomicBool,
) -> Result<(usize, usize)> {
    let mut ticker = interval(Duration::from_millis(100));
    let (mut samples, mut waiting) = (0, 0);
    while !finished.load(Ordering::SeqCst) {
        ticker.tick().await;
        for process in &mut *processes {
            process.check()?;
        }
        for row in store.lease_audit(sessions).await? {
            ensure!(
                !(row.state == SessionState::Sleeping && row.leased),
                "session {} waits for inference while holding a worker lease",
                row.session_id
            );
            waiting += usize::from(row.state == SessionState::Sleeping && row.waiting);
        }
        samples += 1;
    }
    Ok((samples, waiting))
}
async fn poll_until<T>(
    label: &str,
    budget: Duration,
    mut probe: impl AsyncFnMut() -> Result<Option<T>>,
) -> Result<T> {
    timeout(budget, async {
        let mut ticker = interval(Duration::from_millis(50));
        loop {
            ticker.tick().await;
            if let Some(found) = probe().await? {
                return Ok(found);
            }
        }
    })
    .await
    .unwrap_or_else(|_| Err(anyhow::anyhow!("timed out waiting for {label}")))
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Totals {
    completions: u64,
    input: u64,
    output: u64,
    cost_micros: u64,
}
impl std::ops::Add for Totals {
    type Output = Self;
    fn add(self, rhs: Self) -> Self {
        Self {
            completions: self.completions + rhs.completions,
            input: self.input + rhs.input,
            output: self.output + rhs.output,
            cost_micros: self.cost_micros + rhs.cost_micros,
        }
    }
}
fn shows_rate_limit(message: &Message) -> bool {
    message.parts.iter().any(|part| {
        let (Part::Text { text } | Part::Notice { text, .. } | Part::Reasoning { text, .. }) = part
        else {
            return false;
        };
        let text = text.to_ascii_lowercase();
        [
            "rate limit",
            "rate_limit",
            "429",
            "too many requests",
            "retry-after",
            "quota",
        ]
        .iter()
        .any(|word| text.contains(word))
    })
}
async fn verify_conversations(
    store: &Store,
    agents: &[(AgentId, SessionId)],
    turns: usize,
) -> Result<BTreeMap<AgentId, Totals>> {
    let mut totals = BTreeMap::new();
    let mut steps = [0_usize; 2];
    let mut retries = 0;
    for &(agent, id) in agents {
        let session = store.fetch_session(id).await?.context("session missing")?;
        ensure!(session.state == SessionState::Idle, "session {id} not idle");
        let mut events = Vec::new();
        crate::read_through(store, id, &mut events, session.head_seq).await?;
        crate::check::log(id, &events)?;
        let mut users = 0;
        let mut completions = 0;
        let mut sum = Totals::default();
        for event in &events {
            match event {
                Event::MessageAppended { message, .. } => {
                    ensure!(!shows_rate_limit(message), "session {id} showed rate limit");
                    users += usize::from(message.role == MessageRole::User);
                }
                Event::InferenceCompleted { completion, .. } => {
                    ensure!(
                        completion.message.parts
                            == [Part::Text {
                                text: ANSWER.into()
                            }],
                        "session {id} answered unexpectedly"
                    );
                    ensure!(
                        !shows_rate_limit(&completion.message),
                        "session {id} showed rate limit"
                    );
                    if let Some(step @ 0..=1) = completion.route_step {
                        steps[usize::try_from(step)?] += 1;
                    }
                    completions += 1;
                    sum = sum
                        + Totals {
                            completions: 1,
                            input: completion.usage.input_tokens,
                            output: completion.usage.output_tokens,
                            cost_micros: completion.cost_micros,
                        };
                }
                Event::InferenceFailed { retryable, .. } => {
                    ensure!(*retryable, "session {id} has terminal inference failure");
                    retries += 1;
                }
                _ => {}
            }
        }
        ensure!(
            users == turns && completions == turns,
            "session {id}: {users} users and {completions} completions, expected {turns}"
        );
        totals.insert(agent, sum);
    }
    ensure!(
        steps.iter().all(|&count| count > 0),
        "route did not use both entries: {steps:?}"
    );
    ensure!(retries > 0, "no retryable failure observed");
    Ok(totals)
}
/// One gateway slot holds its permit through the breaker write, and its `open_until`
/// is never earlier than the fake's arrival plus Retry-After. `claim_entry`
/// refuses claims while `probe_until` is set. A request claimed before the
/// breaker write must reach the fake within one second to count as a racer.
fn check_breaker(fake: &Fake, slots: usize) -> Result<usize> {
    let log = fake.log();
    let first = log.iter().find(|a| a.status == 429).with_context(|| {
        format!(
            "{} never answered 429; the scenario did not saturate it",
            fake.entry()
        )
    })?;
    ensure!(
        log.iter().any(|a| a.status == 200 && a.at_ms > first.at_ms),
        "{} never recovered",
        fake.entry()
    );
    let mut probes = BTreeSet::new();
    for rejected in log.iter().filter(|a| a.status == 429) {
        let until = rejected.at_ms + i64::try_from(rejected.retry_after_secs)? * 1000;
        let inside = log
            .iter()
            .filter(|a| a.at_ms > rejected.at_ms && a.at_ms < until)
            .count();
        ensure!(
            inside < slots,
            "{}: {inside} requests reached the fake while its breaker was open after the 429 at {}ms; at most {} may race the breaker write",
            fake.entry(),
            rejected.at_ms,
            slots - 1
        );
        if let Some(probe) = log.iter().find(|a| a.at_ms >= until) {
            probes.insert(probe.at_ms);
            if probe.status == 200 {
                let probe_window_ms = i64::try_from(RESPONSE_DELAY.as_millis())?;
                let during = log
                    .iter()
                    .filter(|a| a.at_ms > probe.at_ms && a.at_ms < probe.at_ms + probe_window_ms)
                    .count();
                ensure!(
                    during == 0,
                    "{}: {during} requests arrived while its probe was in flight",
                    fake.entry()
                );
            }
        }
    }
    Ok(probes.len())
}
fn expected(fake: &Fake) -> Totals {
    fake.log()
        .iter()
        .filter(|a| a.status == 200)
        .fold(Totals::default(), |sum, a| {
            sum + Totals {
                completions: 1,
                input: a.input_tokens,
                output: a.output_tokens,
                cost_micros: a.input_tokens + 2 * a.output_tokens,
            }
        })
}
async fn check_usage(
    client: &Client,
    by: &str,
    key: Option<&str>,
    from: &str,
    to: &str,
    expected: Totals,
) -> Result<()> {
    let usage = client.usage(by, key, from, to, "day").await?;
    let got = Totals {
        completions: usage.total.completions,
        input: usage.total.input_tokens,
        output: usage.total.output_tokens,
        cost_micros: usage.total.cost_micros,
    };
    ensure!(
        got == expected,
        "usage {by}/{key:?}: got {got:?}, expected {expected:?}"
    );
    ensure!(
        usage
            .groups
            .iter()
            .map(|g| g.totals.completions)
            .sum::<u64>()
            == got.completions,
        "usage group sum differs from total for {by}/{key:?}"
    );
    Ok(())
}
async fn verify_usage(
    client: &Client,
    fakes: &[Fake],
    agents: &BTreeMap<AgentId, Totals>,
    from: &str,
    to: &str,
) -> Result<()> {
    let mut all = Totals::default();
    for fake in fakes {
        let sum = expected(fake);
        check_usage(client, "entry", Some(&fake.entry()), from, to, sum).await?;

        all = all + sum;
    }
    check_usage(client, "provider", Some(ROUTE), from, to, all).await?;
    for (agent, sum) in agents {
        check_usage(client, "agent", Some(&agent.to_string()), from, to, *sum).await?;
    }
    ensure!(
        agents
            .values()
            .copied()
            .fold(Totals::default(), |a, b| a + b)
            == all,
        "agent totals differ from fake logs"
    );
    check_usage(client, "agent", None, from, to, all).await?;
    check_usage(
        client,
        "kind",
        Some(CredentialEntryKind::ApiKey.as_str()),
        from,
        to,
        all,
    )
    .await
}
async fn verify_quota(client: &Client, fakes: &[Fake]) -> Result<()> {
    let quotas = client.quotas().await?;
    for fake in fakes {
        let row = quotas
            .iter()
            .find(|row| row.provider == ROUTE && row.label == fake.label)
            .with_context(|| format!("quota row missing for {}", fake.entry()))?;
        ensure!(
            row.quota.source == "observed",
            "{} quota is not observed",
            fake.entry()
        );
        // Commit order, not arrival order, determines the last observation
        // written. The decreasing token budget identifies one 200 response.
        let successes = fake.log();
        ensure!(
            successes
                .iter()
                .filter(|a| a.status == 200)
                .any(
                    |a| Some(a.requests_remaining) == row.quota.requests_remaining
                        && Some(a.tokens_remaining) == row.quota.tokens_remaining
                ),
            "{} quota differs from fake log",
            fake.entry()
        );
    }
    Ok(())
}
fn hour_bound(timestamp: Timestamp, offset: i64) -> Result<String> {
    let seconds = timestamp.as_second();
    Ok(Timestamp::from_second(seconds - seconds.rem_euclid(3600) + offset)?.to_string())
}
pub(crate) async fn exercise(f: &mut Fixture, config: &Config, binaries: &Path) -> Result<()> {
    let started = Timestamp::now();
    f.prepare(config).await?;
    let fakes = start_fakes().await;
    configure(f, &fakes).await?;
    let api = Api::new()?;
    let env = environment(f, config, &fakes, &api)?;
    start_services(f, config, binaries, env).await?;
    let client = Client::new(&api.url, api.token.clone())?;
    poll_until("api health", Duration::from_secs(30), async || {
        Ok(client.health().await.ok())
    })
    .await?;
    poll_until("gateway serves sat", Duration::from_secs(30), async || {
        Ok(f.store.gateway_serves(ROUTE).await?.then_some(()))
    })
    .await?;
    let agents = create_agents(f, config.sessions).await?;
    let sessions = agents.iter().map(|(_, id)| *id).collect::<Vec<_>>();
    let finished = AtomicBool::new(false);
    let drive = async {
        try_join_all(
            agents
                .iter()
                .enumerate()
                .map(|(i, (_, id))| chat(&f.store, &f.bus, &client, i, *id, config.steps)),
        )
        .await?;
        finished.store(true, Ordering::SeqCst);
        Ok::<_, anyhow::Error>(())
    };
    let audit = audit(&f.store, &mut f.processes, &sessions, &finished);
    let ((), (samples, waiting)) =
        timeout(Duration::from_secs(config.session_timeout_secs), async {
            tokio::try_join!(drive, audit)
        })
        .await
        .context("saturation drive timed out")??;
    ensure!(
        waiting > 0,
        "no session was ever observed waiting; the route was not saturated"
    );
    for process in &mut f.processes {
        if process.kind != Kind::Api {
            process.stop().await?;
        }
    }
    let totals = verify_conversations(&f.store, &agents, config.steps).await?;
    let probes = fakes
        .iter()
        .map(|fake| check_breaker(fake, config.gateways))
        .collect::<Result<Vec<_>>>()?;
    verify_usage(
        &client,
        &fakes,
        &totals,
        &hour_bound(started, -3600)?,
        &hour_bound(Timestamp::now(), 7200)?,
    )
    .await?;
    verify_quota(&client, &fakes).await?;
    tracing::info!(
        served_a = expected(&fakes[0]).completions,
        served_b = expected(&fakes[1]).completions,
        rejected_a = fakes[0].log().iter().filter(|a| a.status == 429).count(),
        rejected_b = fakes[1].log().iter().filter(|a| a.status == 429).count(),
        probes_a = probes[0],
        probes_b = probes[1],
        audit_samples = samples,
        waiting_samples = waiting,
        elapsed_seconds = Timestamp::now().duration_since(started).as_secs_f64(),
        "saturation passed"
    );
    Ok(())
}
