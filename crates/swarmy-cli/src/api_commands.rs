//! Commands whose state is read or mutated through the control-plane API.
use anyhow::{Context, Result, ensure};
use std::fmt::Write as _;
use swarmy_client::Client;

use ulid::Ulid;

use crate::{agent_command, auth_command, cost_command, image_command, session_command};

use swarmy_client::api_client::call as request;
// Keep the volume image label rule here so the client does not link libfdb_c.
fn validate_label(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte)),
        "names and tags must contain 1-128 ASCII letters, digits, dots, dashes, or underscores"
    );
    Ok(())
}

fn print<T: serde::Serialize>(value: &T, text: &str, json: bool) {
    if json {
        println!(
            "{}",
            serde_json::to_string(value).expect("API response serializes")
        );
    } else {
        println!("{text}");
    }
}
/// Run one control-plane API command. Each entry connects on its own so the
/// top-level dispatch owns every CLI variant without a shared dispatcher.
pub async fn session_command(command: session_command::Command, json: bool) -> Result<()> {
    let (client, endpoint) = swarmy_client::api_client::connect()?;
    session(&client, &endpoint, command, json).await
}

/// Run one agent API command, validating `set` flags before connecting.
pub async fn agent_command(command: agent_command::Command, json: bool) -> Result<()> {
    if let agent_command::Command::Set {
        inference,
        github_token,
        clear_github_token,
        ..
    } = &command
    {
        ensure!(
            inference.system_prompt.is_some()
                || inference.system_prompt_file.is_some()
                || inference.provider.is_some()
                || inference.model.is_some()
                || inference.effort.is_some()
                || inference.route.is_some()
                || inference.memory.is_some()
                || inference.gpu.is_some()
                || github_token.is_some()
                || *clear_github_token,
            "agent set requires --system-prompt, --system-prompt-file, --provider, --model, --effort, \
                  --route, --memory, --gpu, --github-token, or --clear-github-token"
        );
    }
    let (client, endpoint) = swarmy_client::api_client::connect()?;
    agent(&client, &endpoint, command, json).await
}

/// Run one cost API command.
pub async fn cost_command(args: cost_command::Args, json: bool) -> Result<()> {
    let (client, endpoint) = swarmy_client::api_client::connect()?;
    cost(&client, &endpoint, args, json).await
}

/// Run one image read through the API. Builds run locally in `crate::image`.
pub async fn image_command(command: image_command::Command, json: bool) -> Result<()> {
    // Validate the reference before connecting so argument errors report
    // without needing API configuration.
    if let image_command::Command::Show { image } = &command {
        let (name, tag) = image.split_once(':').context("expected NAME:TAG")?;
        validate_label(name)?;
        validate_label(tag)?;
    }
    let (client, endpoint) = swarmy_client::api_client::connect()?;
    image(&client, &endpoint, command, json).await
}

/// Run one credential API command. Logins and imports run through the
/// `swarmy-auth` helper instead.
pub async fn auth_command(command: auth_command::Command, json: bool) -> Result<()> {
    let (client, endpoint) = swarmy_client::api_client::connect()?;
    auth(&client, &endpoint, command, json).await
}
/// Parse a `--since` or `--until` bound, defaulting to `default` when the
/// flag is absent. The client resolves relative spans and calendar words
/// locally so the API only ever sees absolute bounds.
fn cost_bound(
    value: Option<&str>,
    default: &str,
    flag: &str,
    now: jiff::Timestamp,
) -> Result<jiff::Timestamp> {
    let text = value.unwrap_or(default);
    swarmy_core::time::parse_bound(text, now).with_context(|| {
        format!(
            "--{flag} must be a date like 2026-09-01, an RFC 3339 timestamp, now, \
             a span like 7d, 3mo, or 1y, or a calendar word like month or 2months"
        )
    })
}

/// Resolve one key filter to the rollup key the API reads. Agent names
/// resolve to ids; sessions must already be ids.
async fn cost_key(client: &Client, endpoint: &str, dimension: &str, value: &str) -> Result<String> {
    match dimension {
        "agent" => Ok(request(endpoint, client.agent(value)).await?.id),
        "session" => {
            value.parse::<Ulid>().context("invalid session id")?;
            Ok(value.to_owned())
        }
        _ => Ok(value.to_owned()),
    }
}

/// Pick the `(dimension, key)` series for `swarmy cost`. An explicit `--by`
/// reads its key from the matching filter and aggregates when the filter
/// is absent; without `--by` a lone filter implies its dimension and no
/// filter reads the fleet-wide agent series.
async fn cost_series(
    client: &Client,
    endpoint: &str,
    args: &cost_command::Args,
) -> Result<(String, Option<String>)> {
    let filters = [
        ("session", args.session.as_deref()),
        ("agent", args.agent.as_deref()),
        ("provider", args.provider.as_deref()),
        ("entry", args.entry.as_deref()),
        ("kind", args.kind.as_deref()),
        ("model", args.model.as_deref()),
    ];
    let present: Vec<(&str, &str)> = filters
        .iter()
        .filter_map(|(dimension, value)| value.map(|value| (*dimension, value)))
        .collect();
    match (args.by, present.as_slice()) {
        (Some(by), []) => Ok((by.as_str().into(), None)),
        (Some(by), [(dimension, value)]) if *dimension == by.as_str() => Ok((
            by.as_str().into(),
            Some(cost_key(client, endpoint, dimension, value).await?),
        )),
        (Some(_), _) => anyhow::bail!(
            "--by DIMENSION reads its key from the matching filter only; pass one of \
             --agent, --session, --provider, --entry, --kind, or --model for that dimension"
        ),
        (None, []) => Ok(("agent".into(), None)),
        (None, [(dimension, value)]) => Ok((
            (*dimension).into(),
            Some(cost_key(client, endpoint, dimension, value).await?),
        )),
        (None, _) => anyhow::bail!(
            "pass only one of --agent, --session, --provider, --entry, --kind, or --model"
        ),
    }
}

/// One printed cost row: the group bounds with its token, cost, and count totals.
struct UsageRow<'a> {
    start: &'a str,
    end: &'a str,
    input: u64,
    cached: u64,
    cache_write: u64,
    output: u64,
    reasoning: u64,
    total: u64,
    cost_dollars: &'a str,
    completions: u64,
}

impl UsageRow<'_> {
    fn render(&self) -> String {
        format!(
            "{} {} input={} cached={} cache_write={} output={} reasoning={} total={} cost=${} completions={}",
            self.start,
            self.end,
            self.input,
            self.cached,
            self.cache_write,
            self.output,
            self.reasoning,
            self.total,
            self.cost_dollars,
            self.completions,
        )
    }
}

fn print_usage(response: &swarmy_api_types::UsageResponse, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string(response)?);
        return Ok(());
    }
    for group in &response.groups {
        println!(
            "{}",
            UsageRow {
                start: &group.start,
                end: &group.end,
                input: group.totals.input_tokens,
                cached: group.totals.cached_input_tokens,
                cache_write: group.totals.cache_write_input_tokens,
                output: group.totals.output_tokens,
                reasoning: group.totals.reasoning_output_tokens,
                total: group.totals.total_tokens,
                cost_dollars: &group.totals.cost_dollars,
                completions: group.totals.completions,
            }
            .render()
        );
    }
    let total = &response.total;
    println!(
        "{}",
        UsageRow {
            start: "total",
            end: "",
            input: total.input_tokens,
            cached: total.cached_input_tokens,
            cache_write: total.cache_write_input_tokens,
            output: total.output_tokens,
            reasoning: total.reasoning_output_tokens,
            total: total.total_tokens,
            cost_dollars: &total.cost_dollars,
            completions: total.completions,
        }
        .render()
    );
    Ok(())
}

async fn cost(client: &Client, endpoint: &str, args: cost_command::Args, json: bool) -> Result<()> {
    let (by, key) = cost_series(client, endpoint, &args).await?;
    let now = jiff::Timestamp::now();
    let until = cost_bound(args.until.as_deref(), "now", "until", now)?;
    let since = cost_bound(args.since.as_deref(), "30d", "since", now)?;
    ensure!(until > since, "--until must be after --since");
    let response = request(
        endpoint,
        client.usage(
            &by,
            key.as_deref(),
            &since.to_string(),
            &until.to_string(),
            args.group.as_str(),
        ),
    )
    .await?;
    print_usage(&response, json)
}

async fn close_session(
    client: &Client,
    endpoint: &str,
    id: &str,
) -> Result<swarmy_api_types::SessionClosed> {
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        client.close_session(
            id,
            &swarmy_api_types::CloseSession {
                idempotency_key: Ulid::generate().to_string(),
            },
        ),
    )
    .await
    .context("session close timed out")?
    .map_err(|error| match &error {
        swarmy_client::Error::Api { body, .. } if body.code == "main_session_close" => {
            anyhow::anyhow!("cannot close an agent main session; use swarmy agent delete")
        }
        _ => swarmy_client::api_client::api_error(error, endpoint),
    })
}

async fn session(
    client: &Client,
    endpoint: &str,
    command: session_command::Command,
    json: bool,
) -> Result<()> {
    match command {
        session_command::Command::List => {
            let mut after = None;
            loop {
                let page = request(endpoint, client.sessions(after.as_deref(), 256)).await?;
                if page.is_empty() {
                    break;
                }
                for row in page {
                    let selection = row
                        .resolved
                        .as_ref()
                        .context("session resolution missing")?;
                    let kind = row.kind.as_str();
                    let state = row.state.as_str().to_owned();
                    print(
                        &row,
                        &format!(
                            "{} {state} {}/{} route={} kind={kind} agent={} head={} computer_deleted={} archived={} main={} previous_session={}",
                            row.id,
                            selection.provider,
                            selection.model,
                            row.route.as_deref().unwrap_or("(swarm default)"),
                            row.agent_name.as_deref().unwrap_or("-"),
                            row.head_sequence,
                            row.computer_deleted,
                            row.next_session.is_some(),
                            row.main,
                            row.previous_session.as_deref().unwrap_or("-")
                        ),
                        json,
                    );
                    after = Some(row.id);
                }
            }
        }
        session_command::Command::Show { session_id } => {
            show_session(client, endpoint, session_id, json).await?;
        }
        session_command::Command::Close { session_id } => {
            let id = session_id.to_string();
            let closed = close_session(client, endpoint, &id).await?;
            print(&closed, &format!("Closed session {id}"), json);
        }
        session_command::Command::Interrupt { session_id } => {
            let id = session_id.to_string();
            let outcome = request(
                endpoint,
                client.interrupt(
                    &id,
                    &swarmy_api_types::InterruptSession {
                        idempotency_key: Ulid::generate().to_string(),
                    },
                ),
            )
            .await?;
            let message = match outcome.result {
                swarmy_api_types::InterruptStatus::Finished => {
                    format!("Interrupted session {id}")
                }
                swarmy_api_types::InterruptStatus::Requested => {
                    format!("Interrupt requested for session {id}")
                }
            };
            print(&outcome, &message, json);
        }
        session_command::Command::Metrics { session_id } => {
            session_metrics(client, endpoint, session_id, json).await?;
        }
    }
    Ok(())
}
/// Print durable per-turn records. Human output renders missing latencies as
/// `-` rather than `None` so columns stay readable. JSON output is a single
/// array across all pages so `jq` sees one document.
async fn session_metrics(
    client: &Client,
    endpoint: &str,
    session_id: ulid::Ulid,
    json: bool,
) -> Result<()> {
    let mut after: Option<String> = None;
    let mut rows = Vec::new();
    loop {
        let page = request(
            endpoint,
            client.session_metrics(&session_id.to_string(), after.as_deref(), 64),
        )
        .await?;
        if page.is_empty() {
            break;
        }
        after = page.last().map(|row| row.turn_id.clone());
        let last = page.len() < 64;
        if json {
            rows.extend(page);
        } else {
            for row in &page {
                println!(
                    "{} append_to_first_token_ms={} inference_ms={} append_to_idle_ms={} tools={} error={}",
                    row.turn_id,
                    ms_or_dash(row.append_to_first_token_ms),
                    ms_or_dash(row.inference_duration_ms),
                    ms_or_dash(row.append_to_idle_ms),
                    row.tools.len(),
                    row.error.as_deref().unwrap_or("-"),
                );
            }
        }
        if last {
            break;
        }
    }
    if json {
        println!("{}", serde_json::to_string(&rows)?);
    }
    Ok(())
}
fn ms_or_dash(value: Option<f64>) -> String {
    value.map_or_else(|| "-".into(), |ms| format!("{ms:.1}"))
}
/// Read a session's full history across event pages. The API clamps one
/// page to its scan limit, so `session show` follows the sequence cursor to
/// the empty page instead of printing a truncated log.
async fn session_events(
    client: &Client,
    endpoint: &str,
    id: &str,
) -> Result<Vec<swarmy_api_types::Event>> {
    let mut events = Vec::new();
    let mut after = 0;
    loop {
        let page = request(endpoint, client.events(id, after, 256)).await?;
        if page.is_empty() {
            break;
        }
        after = page.last().map_or(after, |event| event.sequence);
        events.extend(page);
    }
    Ok(events)
}

async fn show_session(
    client: &Client,
    endpoint: &str,
    session_id: ulid::Ulid,
    json: bool,
) -> Result<()> {
    let record = request(endpoint, client.session(&session_id.to_string())).await?;
    let selection = record
        .resolved
        .clone()
        .context("session resolution missing")?;
    let id = session_id.to_string();
    if json {
        println!("{}", serde_json::to_string(&record)?);
    } else {
        let inherited = |present: bool| if present { "" } else { " (inherited)" };
        let scratch_node = record
            .scratch
            .as_ref()
            .map_or("-".to_owned(), |scratch| scratch.node_id.clone());
        let scratch_bytes = record.scratch.as_ref().map_or(0, |scratch| scratch.bytes);
        let sandbox_memory = record.requirements.as_ref().map_or_else(
            || "-".into(),
            |requirements| requirements.memory_mib.to_string(),
        );
        let sandbox_gpu = record
            .requirements
            .as_ref()
            .map_or("-", |requirements| requirements.gpu.as_str());
        println!(
            "Session {id}: {}, interrupt_requested={} provider={}{} model={}{} effort={}{} route={} scratch_node={} scratch_bytes={} sandbox_memory_mib={} sandbox_gpu={} sandbox_address={}",
            record.state.as_str(),
            record.interrupt_requested,
            selection.provider,
            inherited(record.provider.is_some()),
            selection.model,
            inherited(record.model.is_some()),
            selection.effort.as_str(),
            inherited(record.effort.is_some()),
            record.route.as_deref().unwrap_or("(swarm default)"),
            scratch_node,
            scratch_bytes,
            sandbox_memory,
            sandbox_gpu,
            record.sandbox_address.as_deref().unwrap_or("-")
        );
        if let Some(usage) = &record.usage {
            println!(
                "Usage: input={} cached={} cache_write={} output={} reasoning={} total={} cost=${}",
                usage.input_tokens,
                usage.cached_input_tokens,
                usage.cache_write_input_tokens,
                usage.output_tokens,
                usage.reasoning_output_tokens,
                usage.total_tokens,
                usage.cost_dollars
            );
        }
        for entry in &record.entries {
            println!(
                "entry {} cost=${} input={} output={} total={} completions={}",
                entry.entry,
                entry.totals.cost_dollars,
                entry.totals.input_tokens,
                entry.totals.output_tokens,
                entry.totals.total_tokens,
                entry.totals.completions
            );
        }
        if !record.providers.is_empty() {
            println!("providers={}", record.providers.join(","));
        }
        if record.state == swarmy_api_types::SessionState::Sleeping
            && let Some(wait) = &record.waiting
        {
            println!(
                "WaitingForInference until {}: {}",
                wait.wake_at.as_deref().unwrap_or("-"),
                wait.reasons.join("; ")
            );
        }
    }
    for event in &session_events(client, endpoint, &id).await? {
        let swarmy_api_types::EventPayload::StoreRecord {
            record: swarmy_api_types::RecordBody::Event(inner),
        } = &event.payload
        else {
            continue;
        };
        if json {
            println!("{}", serde_json::to_string(inner)?);
        } else {
            println!("{} {}", event.sequence, serde_json::to_string(inner)?);
        }
    }
    Ok(())
}

async fn image(
    client: &Client,
    endpoint: &str,
    command: image_command::Command,
    json: bool,
) -> Result<()> {
    match command {
        image_command::Command::Ls => {
            let mut after = None;
            loop {
                let page = request(endpoint, client.images(after.as_deref(), 256)).await?;
                if page.is_empty() {
                    break;
                }
                for image in page {
                    print(
                        &image,
                        &format!("{}:{} {}", image.name, image.tag, image.manifest_id),
                        json,
                    );
                    after = Some(format!("{}:{}", image.name, image.tag));
                }
            }
        }
        image_command::Command::Show { image } => {
            // The reference was validated before connecting.
            let (name, tag) = image.split_once(':').context("expected NAME:TAG")?;
            let value = client.image(name, tag).await.map_err(|error| {
                if matches!(&error, swarmy_client::Error::Api { body, .. } if body.code == "image_not_found") {
                    anyhow::anyhow!("image not found")
                } else {
                    swarmy_client::api_client::api_error(error, endpoint)
                }
            })?;
            let header = value.header.as_ref().context("image header missing")?;
            print(
                &value,
                &format!(
                    "{image} {}\nsize={} chunk_size={} root_hash={} scratch={}",
                    value.manifest_id,
                    header.size,
                    header.chunk_size,
                    header.root_hash,
                    value
                        .scratch
                        .as_ref()
                        .context("image scratch missing")?
                        .join(",")
                ),
                json,
            );
        }
        image_command::Command::Build { .. } => {
            anyhow::bail!("image build runs locally, not through the API");
        }
    }
    Ok(())
}

fn settings_text(agent: &swarmy_api_types::Agent) -> String {
    format!(
        "\nprovider={}\nsystem_prompt={}\nmodel={}\nreasoning_effort={}\nroute={}\nsandbox_memory_mib={}\nsandbox_gpu={}",
        agent.provider.as_deref().unwrap_or("(stack default)"),
        agent.system_prompt.as_deref().unwrap_or("(stack default)"),
        agent.model.as_deref().unwrap_or("(stack default)"),
        agent.effort.map_or("(stack default)", |v| v.as_str()),
        agent.route.as_deref().unwrap_or("(stack default)"),
        agent.requirements.memory_mib,
        agent.requirements.gpu.as_str(),
    )
}
struct AgentFlags {
    provider: Option<String>,
    model: Option<String>,
    effort: Option<swarmy_api_types::ReasoningEffort>,
    route: Option<String>,
    system_prompt: Option<String>,
    memory_mib: Option<u64>,
    gpu: Option<swarmy_api_types::GpuMode>,
    resets: Vec<swarmy_api_types::AgentReset>,
}
fn inference(args: agent_command::InferenceArgs, update: bool) -> Result<AgentFlags> {
    use swarmy_api_types::AgentReset;
    let mut resets = Vec::new();
    let mut override_or_reset = |value: Option<String>, field| -> Result<Option<String>> {
        if value.as_deref() == Some("default") {
            ensure!(update, "default clears an override only with agent set");
            resets.push(field);
            Ok(None)
        } else {
            Ok(value)
        }
    };
    let provider = override_or_reset(args.provider, AgentReset::Provider)?;
    let model = override_or_reset(args.model, AgentReset::Model)?;
    let effort = override_or_reset(args.effort, AgentReset::Effort)?
        .map(|value| {
            value
                .parse::<swarmy_core::ReasoningEffort>()
                .map(Into::into)
        })
        .transpose()?;
    let route = override_or_reset(args.route, AgentReset::Route)?;
    let system_prompt = if let Some(path) = args.system_prompt_file {
        Some(
            std::fs::read_to_string(&path)
                .with_context(|| format!("read system prompt {}", path.display()))?,
        )
    } else {
        args.system_prompt
    };
    let gpu = args.gpu.map(crate::agent_command::GpuArg::into_api);
    Ok(AgentFlags {
        provider,
        model,
        effort,
        route,
        system_prompt,
        memory_mib: args.memory,
        gpu,
        resets,
    })
}
fn agent_text(agent: &swarmy_api_types::Agent, detail: bool) -> String {
    // List rows are summaries: the list endpoint serves no placement,
    // scratch, usage, or sessions, so `agent ls` prints none of those
    // columns instead of placeholders. Detail hydrates them on show.
    let mut text = format!(
        "{} {} image={}:{} created={} main_session={}",
        agent.name,
        agent.id,
        agent.image.name,
        agent.image.tag,
        agent.created_at,
        agent.main_session_id.as_deref().unwrap_or("-"),
    );
    if detail {
        let _ = write!(
            text,
            "\nnode={} scratch_node={} scratch_bytes={}",
            agent.node_id.as_deref().unwrap_or("-"),
            agent
                .scratch
                .as_ref()
                .map_or("-", |scratch| scratch.node_id.as_str()),
            agent.scratch.as_ref().map_or(0, |scratch| scratch.bytes),
        );
        text.push_str(&settings_text(agent));
        if let Some(usage) = &agent.usage {
            let _ = write!(
                text,
                "\nUsage: input={} cached={} cache_write={} output={} reasoning={} total={} cost=${}",
                usage.input_tokens,
                usage.cached_input_tokens,
                usage.cache_write_input_tokens,
                usage.output_tokens,
                usage.reasoning_output_tokens,
                usage.total_tokens,
                usage.cost_dollars
            );
        }
        for entry in &agent.entries {
            let _ = write!(
                text,
                "\nentry {} cost=${} input={} output={} total={} completions={}",
                entry.entry,
                entry.totals.cost_dollars,
                entry.totals.input_tokens,
                entry.totals.output_tokens,
                entry.totals.total_tokens,
                entry.totals.completions
            );
        }
        let _ = write!(text, "\nproviders={}", agent.providers.join(","));
        let _ = write!(
            text,
            "\ndescription={}\nplacement_epoch={}\nsandbox_address={}\nsandbox_state={}\nlast_snapshot={} age_seconds={}",
            agent.description,
            agent
                .placement
                .as_ref()
                .map_or_else(|| "-".into(), |placement| placement.epoch.to_string()),
            agent.sandbox_address.as_deref().unwrap_or("-"),
            agent.sandbox_state.map_or("-", |state| state.as_str()),
            agent.last_snapshot_at.as_deref().unwrap_or("-"),
            agent
                .last_snapshot_age_seconds
                .map_or_else(|| "-".into(), |v| v.to_string())
        );
        if let Some(status) = &agent.call_status {
            let _ = write!(
                text,
                "\ncall_holder={} queued_calls={} observed_at={} expires_at={} node={} epoch={}",
                status.holder_session_id.as_deref().unwrap_or("-"),
                status.queued_calls,
                status.observed_at,
                status.expires_at,
                status.node_id,
                status.epoch
            );
        }
        for session in &agent.sessions {
            let _ = write!(
                text,
                "\nsession={} state={} computer_deleted={} main={} archived={}",
                session.id,
                session.state.as_str(),
                session.computer_deleted,
                agent.main_session_id.as_deref() == Some(session.id.as_str()),
                session.next_session.is_some()
            );
        }
        if let Some(count) = agent.session_count {
            let _ = write!(text, "\nsessions={count}");
        }
    }
    text
}
async fn update_agent(
    client: &Client,
    endpoint: &str,
    name: &str,
    body: &swarmy_api_types::UpdateAgent,
) -> Result<swarmy_api_types::Agent> {
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        client.update_agent(name, body),
    )
    .await
    .context("agent update timed out")?
    .map_err(|error| match &error {
        swarmy_client::Error::Api { body, .. } if body.code == "agent_computer_placed" => {
            anyhow::anyhow!("the agent's computer is placed; retry after it is released")
        }
        _ => swarmy_client::api_client::api_error(error, endpoint),
    })
}

async fn agent_create(
    client: &Client,
    endpoint: &str,
    args: agent_command::CreateArgs,
    json: bool,
) -> Result<()> {
    let agent_command::CreateArgs {
        name,
        image,
        description,
        inference: flags,
        github_token,
        github_token_stdin,
    } = args;
    let token = if github_token_stdin {
        use std::io::Read as _;
        let mut token = String::new();
        std::io::stdin().read_to_string(&mut token)?;
        Some(token.trim_end_matches(['\r', '\n']).to_owned())
    } else {
        github_token
    };
    let flags = inference(flags, false)?;
    let image = image.or_else(|| swarmy_config::Settings::load().ok()?.settings.default_image);
    let image = match image {
        Some(image) => image,
        None => request(endpoint, client.doctor())
            .await?
            .default_image
            .context("no default image configured")?,
    };
    let (image_name, image_tag) = image.split_once(':').context("expected image NAME:TAG")?;
    let created = request(
        endpoint,
        client.create_agent(&swarmy_api_types::CreateAgent {
            idempotency_key: Ulid::generate().to_string(),
            name: name.clone(),
            description,
            image: swarmy_api_types::ImageRef {
                name: image_name.into(),
                tag: image_tag.into(),
            },
            provider: flags.provider,
            model: flags.model,
            effort: flags.effort,
            system_prompt: flags.system_prompt,
            route: flags.route,
            memory_mib: flags.memory_mib,
            gpu: flags.gpu,
            github_token: token,
        }),
    )
    .await?;
    // Create returns the detail shape, so render it without a second fetch.
    print(
        &created,
        &format!(
            "Created agent {} {} image={}:{}{}",
            created.name,
            created.id,
            created.image.name,
            created.image.tag,
            settings_text(&created)
        ),
        json,
    );
    Ok(())
}

async fn agent(
    client: &Client,
    endpoint: &str,
    command: agent_command::Command,
    json: bool,
) -> Result<()> {
    match command {
        agent_command::Command::Ls => {
            let mut after = None;
            loop {
                let page = request(endpoint, client.agents(after.as_deref(), 256)).await?;
                if page.is_empty() {
                    break;
                }
                for row in page {
                    print(&row, &agent_text(&row, false), json);
                    after = Some(row.id.clone());
                }
            }
        }
        agent_command::Command::Show { name } => {
            let row = request(endpoint, client.agent(&name)).await?;
            print(&row, &agent_text(&row, true), json);
        }
        agent_command::Command::Create(args) => {
            agent_create(client, endpoint, args, json).await?;
        }
        agent_command::Command::Set {
            name,
            inference: flags,
            github_token,
            clear_github_token,
        } => {
            let flags = inference(flags, true)?;
            let updated = update_agent(
                client,
                endpoint,
                &name,
                &swarmy_api_types::UpdateAgent {
                    idempotency_key: Ulid::generate().to_string(),
                    description: None,
                    provider: flags.provider,
                    model: flags.model,
                    effort: flags.effort,
                    system_prompt: flags.system_prompt,
                    route: flags.route,
                    memory_mib: flags.memory_mib,
                    gpu: flags.gpu,
                    resets: flags.resets,
                    github_token,
                    clear_github_token,
                },
            )
            .await?;
            print(
                &updated,
                &format!(
                    "Updated agent {} {}{}",
                    updated.name,
                    updated.id,
                    settings_text(&updated)
                ),
                json,
            );
        }
        agent_command::Command::Delete { name, yes } => {
            delete_agent(client, endpoint, &name, yes, json).await?;
        }
        agent_command::Command::Metrics { name } => {
            agent_metrics(client, endpoint, &name, json).await?;
        }
    }
    Ok(())
}
/// Print the rollup for an agent's main session. Missing throughput renders
/// as `-` rather than `None`.
async fn agent_metrics(client: &Client, endpoint: &str, name: &str, json: bool) -> Result<()> {
    let record = request(endpoint, client.agent_metrics(name, 200, None)).await?;
    if json {
        println!("{}", serde_json::to_string(&record)?);
    } else {
        println!(
            "agent={} turns={} input_tokens={} output_tokens={} mean_tps={} errors={} retries={} cost_micros={}",
            name,
            record.turns,
            record.input_tokens,
            record.output_tokens,
            record
                .mean_output_tokens_per_second
                .map_or_else(|| "-".into(), |tps| format!("{tps:.1}")),
            record.errors,
            record.retries,
            record.cost_micros
        );
        let mut names: Vec<_> = record.latencies.keys().collect();
        names.sort();
        for latency in names {
            let percentiles = &record.latencies[latency];
            println!(
                "latency_{latency}_ms p50={:.1} p95={:.1}",
                percentiles.p50_ms, percentiles.p95_ms
            );
        }
    }
    Ok(())
}

async fn delete_agent(
    client: &Client,
    endpoint: &str,
    name: &str,
    yes: bool,
    json: bool,
) -> Result<()> {
    let current = request(endpoint, client.agent(name)).await?;
    if !yes {
        use std::io::{IsTerminal as _, Write as _};
        ensure!(
            std::io::stdin().is_terminal(),
            "agent delete requires confirmation; pass --yes for noninteractive deletion"
        );
        eprint!("Delete agent {} and its computer? [y/N] ", current.name);
        std::io::stderr().flush()?;
        let mut answer = String::new();
        std::io::stdin().read_line(&mut answer)?;
        ensure!(
            matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes"),
            "agent deletion cancelled"
        );
    }
    request(
        endpoint,
        client.delete_agent(name, &Ulid::generate().to_string()),
    )
    .await?;
    print(
        &current,
        &format!("Deleted agent {} {}", current.name, current.id),
        json,
    );

    Ok(())
}

async fn key_from_source(
    client: &Client,
    endpoint: &str,
    args: &auth_command::Set,
    provider: &str,
) -> Result<String> {
    if let Some(key) = &args.source.api_key {
        return Ok(key.clone());
    }
    if let Some(path) = &args.source.file {
        return Ok(std::fs::read_to_string(path)
            .context("read API key file")?
            .trim()
            .to_owned());
    }
    let providers = swarmy_client::api_client::call(endpoint, client.providers()).await?;
    let names = providers
        .iter()
        .find(|row| row.id == provider)
        .map(crate::provider_report::credential_env_keys)
        .unwrap_or_default();
    if names.is_empty() {
        anyhow::bail!("no API key environment mapping for {provider}; use --api-key or --file");
    }
    names
        .iter()
        .find_map(|name| std::env::var(name).ok().filter(|value| !value.is_empty()))
        .with_context(|| format!("set {} before using --from-env", names.join(" or ")))
}
#[derive(serde::Serialize)]
struct AuthEvent<'a> {
    event: &'a str,
    provider: &'a str,
}

fn auth_report(event: &str, provider: &str, json: bool) {
    print(
        &AuthEvent { event, provider },
        &format!("{provider}: {event}"),
        json,
    );
}

async fn set_credential_key(
    client: &Client,
    endpoint: &str,
    args: &auth_command::Set,
    provider: &str,
    json: bool,
) -> Result<()> {
    let key = key_from_source(client, endpoint, args, provider).await?;
    ensure!(!key.trim().is_empty(), "API key must not be empty");
    let mut extra: std::collections::BTreeMap<String, String> =
        args.extra.clone().into_iter().collect();
    if provider == "azure" && args.source.from_env {
        for (env, name) in [
            ("AZURE_OPENAI_BASE_URL", "base_url"),
            ("AZURE_RESOURCE_NAME", "resource_name"),
        ] {
            if let Ok(value) = std::env::var(env)
                && !value.is_empty()
            {
                extra.entry(name.into()).or_insert(value);
            }
        }
    }
    request(
        endpoint,
        client.set_credential(&swarmy_api_types::CreateCredential {
            idempotency_key: Ulid::generate().to_string(),
            provider: provider.to_owned(),
            kind: swarmy_api_types::CredentialKind::ApiKey,
            label: args.label.clone().unwrap_or_else(|| "default".into()),
            secret: key,
            extra,
        }),
    )
    .await?;
    auth_report("saved", provider, json);
    Ok(())
}

async fn set_entry_quota(
    client: &Client,
    endpoint: &str,
    provider: &str,
    label: Option<&str>,
    limit: u64,
    window: &str,
) -> Result<()> {
    let window_seconds = swarmy_core::quota::parse_window(window)
        .with_context(|| "--window must look like 30m, 5h, or 7d")?;
    let label = label.unwrap_or("default");
    request(
        endpoint,
        client.set_entry_quota(
            provider,
            label,
            &swarmy_api_types::SetEntryQuota {
                idempotency_key: Ulid::generate().to_string(),
                limit,
                window_seconds,
            },
        ),
    )
    .await?;
    Ok(())
}
#[derive(serde::Serialize)]
struct CredentialWithExpiry<'a> {
    #[serde(flatten)]
    credential: &'a swarmy_api_types::Credential,
    expires_in_seconds: Option<i64>,
}

fn auth_display(summary: &swarmy_api_types::Credential, json: bool, expiry: bool) {
    let seconds = summary
        .expires_at
        .as_deref()
        .and_then(|at| at.parse::<jiff::Timestamp>().ok())
        .map(|at| at.as_second() - jiff::Timestamp::now().as_second());
    if json {
        if expiry {
            println!(
                "{}",
                serde_json::to_string(&CredentialWithExpiry {
                    credential: summary,
                    expires_in_seconds: seconds,
                })
                .expect("credential serializes")
            );
        } else {
            println!(
                "{}",
                serde_json::to_string(summary).expect("credential serializes")
            );
        }
    } else {
        println!(
            "{}\t{}\t{}\t{}\t{}{}",
            summary.provider,
            summary.kind.as_str(),
            summary.label,
            summary.status.as_str(),
            summary.updated_at,
            if expiry {
                seconds.map_or_else(String::new, |n| format!("\texpires in {n}s"))
            } else {
                String::new()
            }
        );
    }
}
async fn auth_set(
    client: &Client,
    endpoint: &str,
    args: auth_command::Set,
    json: bool,
) -> Result<()> {
    let provider = args
        .provider_flag
        .as_ref()
        .or(args.provider.as_ref())
        .context("auth set requires a provider (positional or --provider)")?;
    ensure!(
        !provider.is_empty()
            && provider
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
        "invalid provider id"
    );
    validate_auth_set_sources(&args)?;
    let has_source = auth_set_has_source(&args);
    let has_quota = args.limit.is_some() || args.window.is_some();
    if has_source {
        ensure!(
            provider != "chatgpt",
            "chatgpt requires OAuth; use auth login chatgpt"
        );
        set_credential_key(client, endpoint, &args, provider, json).await?;
    }
    if let (Some(limit), Some(window)) = (args.limit, args.window) {
        set_entry_quota(
            client,
            endpoint,
            provider,
            args.label.as_deref(),
            limit,
            &window,
        )
        .await?;
    }
    if !has_source && has_quota {
        auth_report("saved", provider, json);
    }
    Ok(())
}

fn auth_set_has_source(args: &auth_command::Set) -> bool {
    args.source.api_key.is_some()
        || args.source.from_env
        || args.source.file.is_some()
        || !args.extra.is_empty()
}

fn validate_auth_set_sources(args: &auth_command::Set) -> Result<()> {
    let has_source = auth_set_has_source(args);
    let has_quota = args.limit.is_some() || args.window.is_some();
    ensure!(
        has_source || has_quota,
        "auth set requires a key source (--api-key, --from-env, --file, --extra) or quota flags (--limit, --window)"
    );
    if let (Some(limit), Some(window)) = (args.limit, args.window.clone()) {
        ensure!(limit > 0, "--limit must be positive");
        ensure!(
            swarmy_core::quota::parse_window(&window).is_some(),
            "--window must look like 30m, 5h, or 7d"
        );
    } else {
        ensure!(
            args.limit.is_none() && args.window.is_none(),
            "--limit and --window must be set together"
        );
    }
    Ok(())
}

async fn auth(
    client: &Client,
    endpoint: &str,
    command: auth_command::Command,
    json: bool,
) -> Result<()> {
    match command {
        auth_command::Command::Set(args) => {
            auth_set(client, endpoint, args, json).await?;
        }
        auth_command::Command::Ls => {
            for summary in request(endpoint, client.credentials()).await? {
                auth_display(&summary, json, false);
            }
        }
        auth_command::Command::Check { provider, label } => {
            let rows: Vec<_> = request(endpoint, client.credentials())
                .await?
                .into_iter()
                .filter(|row| {
                    provider
                        .as_ref()
                        .is_none_or(|provider| row.provider == *provider)
                        && label.as_ref().is_none_or(|label| row.label == *label)
                })
                .collect();
            ensure!(!rows.is_empty(), "credential does not exist");
            let ready = rows
                .iter()
                .all(|row| row.status == swarmy_api_types::CredentialStatus::Ready);
            let expired_bedrock = rows.iter().any(|row| {
                row.provider == "amazon-bedrock"
                    && row.status == swarmy_api_types::CredentialStatus::Expired
            });
            for summary in rows {
                auth_display(&summary, json, true);
            }
            ensure!(
                ready,
                if expired_bedrock {
                    "Bedrock console API keys expire after twelve hours and are for development only; use an IAM identity for long-lived use"
                } else {
                    "one or more credentials are expired or need login"
                }
            );
        }
        auth_command::Command::Rm { provider, label } => {
            request(
                endpoint,
                client.remove_credential_entry(&provider, &label, &Ulid::generate().to_string()),
            )
            .await?;
            auth_report("removed", &provider, json);
        }
        auth_command::Command::Routes { command } => {
            routes(client, endpoint, command, json).await?;
        }
        auth_command::Command::Quota {
            entry,
            group,
            since,
            until,
        } => {
            quota(
                client,
                endpoint,
                entry.as_deref(),
                group,
                since.as_deref(),
                until.as_deref(),
                json,
            )
            .await?;
        }
        auth_command::Command::Login { .. } | auth_command::Command::Import { .. } => {
            anyhow::bail!("login and import run through the swarmy-auth helper");
        }
    }
    Ok(())
}

fn quota_line(entry: &swarmy_api_types::QuotaEntry) -> String {
    let quota = &entry.quota;
    let mut line = format!(
        "{}/{} {} {} used={} free={} window={} observed={}",
        entry.provider,
        entry.label,
        entry.kind,
        quota.source,
        quota.used,
        quota
            .free
            .map_or_else(|| "-".into(), |free| free.to_string()),
        quota
            .window_seconds
            .map_or_else(|| "-".into(), |window| window.to_string()),
        quota.observed_at.as_deref().unwrap_or("-"),
    );
    if let Some(requests) = quota.requests_remaining {
        let _ = write!(line, " requests={requests}");
    }
    if let Some(tokens) = quota.tokens_remaining {
        let _ = write!(line, " tokens={tokens}");
    }
    if let Some(limit) = quota.limit {
        let _ = write!(line, " limit={limit}");
    }
    line
}

/// List every entry's quota, or show one entry's quota with its usage
/// series. Observed entries report the provider's latest published
/// remaining values; configured entries count completions from the
/// entry rollups over their window.
async fn quota(
    client: &Client,
    endpoint: &str,
    entry: Option<&str>,
    group: crate::cost_command::UsageGroup,
    since: Option<&str>,
    until: Option<&str>,
    json: bool,
) -> Result<()> {
    // One request lists every entry's quota; the per-entry usage series
    // below is the only second call, and only for `--entry`.
    let views = request(endpoint, client.quotas()).await?;
    if let Some(entry) = entry {
        let (provider, label) = entry.split_once('/').context("expected PROVIDER/LABEL")?;
        let view = views
            .iter()
            .find(|view| view.provider == provider && view.label == label)
            .with_context(|| format!("unknown entry {entry}"))?;
        let now = jiff::Timestamp::now();
        let end = cost_bound(until, "now", "until", now)?;
        let start = cost_bound(since, "30d", "since", now)?;
        ensure!(end > start, "--until must be after --since");
        let response = request(
            endpoint,
            client.usage(
                "entry",
                Some(entry),
                &start.to_string(),
                &end.to_string(),
                group.as_str(),
            ),
        )
        .await?;
        if json {
            println!(
                "{}",
                serde_json::to_string(&swarmy_api_types::EntryQuotaDetail {
                    quota: view.clone(),
                    usage: response,
                })?
            );
        } else {
            println!("{}", quota_line(view));
            print_usage(&response, false)?;
        }
        return Ok(());
    }
    if json {
        println!("{}", serde_json::to_string(&views)?);
    } else {
        for view in &views {
            println!("{}", quota_line(view));
        }
    }
    Ok(())
}

fn route_text(route: &swarmy_api_types::Route) -> String {
    let steps = route
        .steps
        .iter()
        .map(|step| match step.model.as_deref() {
            Some(model) if !model.is_empty() => {
                format!("{}/{}={model}", step.provider, step.entry)
            }
            _ => format!("{}/{}", step.provider, step.entry),
        })
        .collect::<Vec<_>>()
        .join(" ");
    format!("{} {steps}", route.name)
}

async fn routes(
    client: &Client,
    endpoint: &str,
    command: auth_command::RoutesCommand,
    json: bool,
) -> Result<()> {
    match command {
        auth_command::RoutesCommand::Ls => {
            for row in request(endpoint, client.routes()).await? {
                print(&row, &route_text(&row), json);
            }
        }
        auth_command::RoutesCommand::Show { name } => {
            let row = request(endpoint, client.route(&name)).await?;
            print(&row, &route_text(&row), json);
        }
        auth_command::RoutesCommand::Set { name, steps } => {
            ensure!(!steps.is_empty(), "a route needs at least one step");
            let mut parsed = Vec::with_capacity(steps.len());
            for step in &steps {
                let step = swarmy_core::route::parse_step(step)
                    .map_err(|message| anyhow::anyhow!("{message}"))?;
                parsed.push(swarmy_api_types::RouteStep {
                    provider: step.provider,
                    entry: step.entry,
                    model: step.model,
                });
            }
            let saved = request(
                endpoint,
                client.set_route(
                    &name,
                    &swarmy_api_types::SetRoute {
                        idempotency_key: Ulid::generate().to_string(),
                        steps: parsed,
                    },
                ),
            )
            .await?;
            print(&saved, &format!("{name}: saved"), json);
        }
        auth_command::RoutesCommand::Rm { name } => {
            let removed = request(
                endpoint,
                client.remove_route(&name, &Ulid::generate().to_string()),
            )
            .await?;
            print(&removed, &format!("{name}: removed"), json);
        }
    }
    Ok(())
}
