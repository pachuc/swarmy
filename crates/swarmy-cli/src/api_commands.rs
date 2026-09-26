//! Commands whose state is read or mutated through the control-plane API.
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::fmt::Write as _;
use swarmy_client::Client;

use ulid::Ulid;

use crate::{Command, agent_command, auth_command, cost_command, image_command, session_command};

use crate::api_client::call as request;
async fn projection<T: serde::Serialize>(
    endpoint: &str,
    future: impl std::future::Future<Output = Result<T, swarmy_client::Error>>,
) -> Result<Value> {
    Ok(serde_json::to_value(request(endpoint, future).await?)?)
}

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

fn print(value: &Value, text: &str, json: bool) {
    if json {
        println!("{value}");
    } else {
        println!("{text}");
    }
}
fn str_field<'a>(value: &'a Value, key: &str) -> &'a str {
    value[key].as_str().unwrap_or("-")
}
fn display_state(value: &Value) -> String {
    str_field(value, "state")
        .split('_')
        .map(|word| {
            let mut chars = word.chars();
            chars.next().map_or_else(String::new, |first| {
                first.to_uppercase().collect::<String>() + chars.as_str()
            })
        })
        .collect()
}

pub async fn run(command: Command, json: bool) -> Result<()> {
    if let Command::Image {
        command: image_command::Command::Show { image },
    } = &command
    {
        let (name, tag) = image.split_once(':').context("expected NAME:TAG")?;
        validate_label(name)?;
        validate_label(tag)?;
    }
    if let Command::Agent {
        command:
            agent_command::Command::Set {
                inference,
                github_token,
                clear_github_token,
                ..
            },
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
    let (client, endpoint) = crate::api_client::connect()?;
    match command {
        Command::Session { command } => session(&client, &endpoint, command, json).await?,
        Command::Agent { command } => agent(&client, &endpoint, command, json).await?,
        Command::Cost { args } => cost(&client, &endpoint, args, json).await?,
        Command::Image { command } => image(&client, &endpoint, command, json).await?,
        Command::Auth { command, .. } => auth(&client, &endpoint, command, json).await?,
        _ => unreachable!("only API commands reach this dispatcher"),
    }
    Ok(())
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
    match (args.by.as_deref(), present.as_slice()) {
        (Some(by), []) => Ok((by.into(), None)),
        (Some(by), [(dimension, value)]) if *dimension == by => Ok((
            by.into(),
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
            &args.group,
        ),
    )
    .await?;
    print_usage(&response, json)
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
                let page = request(endpoint, client.cli_sessions(after.as_deref(), 256))
                    .await?
                    .into_iter()
                    .map(serde_json::to_value)
                    .collect::<Result<Vec<_>, _>>()?;
                if page.is_empty() {
                    break;
                }
                for row in page {
                    let id = str_field(&row, "session_id");
                    let selection = &row["resolved_inference"];
                    let kind = if row["kind"].is_string() {
                        "ephemeral"
                    } else {
                        "named"
                    };
                    let name = str_field(&row, "agent_name");
                    print(
                        &row,
                        &format!(
                            "{id} {} {}/{} route={} kind={kind} agent={name} head={} computer_deleted={} archived={} main={}",
                            display_state(&row),
                            str_field(selection, "provider"),
                            str_field(selection, "model"),
                            row["route"].as_str().unwrap_or("(swarm default)"),
                            row["head_seq"],
                            row["computer_deleted"],
                            row["archived"],
                            row["main"]
                        ),
                        json,
                    );
                    after = Some(id.to_owned());
                }
            }
        }
        session_command::Command::Show { session_id } => {
            show_session(client, endpoint, session_id, json).await?;
        }
        session_command::Command::Close { session_id } => {
            let id = session_id.to_string();
            request(
                endpoint,
                client.close_session(
                    &id,
                    &swarmy_api_types::CloseSession {
                        idempotency_key: Ulid::generate().to_string(),
                    },
                ),
            )
            .await?;
            print(
                &json!({"event":"session_closed","session_id":id}),
                &format!("Closed session {id}"),
                json,
            );
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
            let (status, message) = match outcome.result {
                swarmy_api_types::InterruptStatus::Finished => {
                    ("finished", format!("Interrupted session {id}"))
                }
                swarmy_api_types::InterruptStatus::Requested => {
                    ("requested", format!("Interrupt requested for session {id}"))
                }
            };
            print(
                &json!({"event":"session_interrupt","session_id":id,"result":status}),
                &message,
                json,
            );
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
async fn show_session(
    client: &Client,
    endpoint: &str,
    session_id: ulid::Ulid,
    json: bool,
) -> Result<()> {
    let details = projection(endpoint, client.cli_session(&session_id.to_string())).await?;
    let record = &details["session"];
    let selection = &details["resolved"];
    let id = session_id.to_string();
    let value = json!({"event":"session_selection","session_id":id,"state":record["state"],
                "interrupt_requested":record["interrupt_requested"],"inference":record["inference"],"resolved":selection,
                "scratch":details["scratch"],"sandbox_requirements":details["requirements"],
                "memory_limit_mib":details["requirements"]["memory_mib"],"placement":details["placement"],
                "sandbox_address":details["address"],"sandbox_status":if details["placement"].is_null() {"waiting_for_capacity_or_first_tool"} else {"placed"}});
    let inherited = |field: &str| {
        if record["inference"][field].is_null() {
            " (inherited)"
        } else {
            ""
        }
    };
    let scratch = &details["scratch"];
    let requirements = &details["requirements"];
    let gpu = match str_field(requirements, "gpu") {
        "none" => "None",
        "shared" => "Shared",
        "dedicated" => "Dedicated",
        other => other,
    };
    print(
        &value,
        &format!(
            "Session {id}: {}, interrupt_requested={} provider={}{} model={}{} effort={}{} route={} scratch_node={} scratch_bytes={} sandbox_memory_mib={} sandbox_gpu={gpu} sandbox_address={}",
            display_state(record),
            record["interrupt_requested"],
            str_field(selection, "provider"),
            inherited("provider"),
            str_field(selection, "model"),
            inherited("model"),
            str_field(selection, "effort"),
            inherited("effort"),
            record["route"].as_str().unwrap_or("(swarm default)"),
            str_field(scratch, "node_id"),
            scratch["bytes"].as_u64().unwrap_or(0),
            requirements["memory_mib"],
            text_value_or_dash(&details["address"])
        ),
        json,
    );
    let usage = &details["usage"];
    print_session_usage(usage, &details["cost_dollars"], &details, json);
    if record["state"] == "sleeping" && !details["wait"].is_null() {
        let wait = &details["wait"];
        let reasons = wait["reasons"]
            .as_array()
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join("; ")
            })
            .unwrap_or_default();
        print(
            &json!({"state":"waiting_for_inference","wake_at":wait["wake_at"],"reasons":wait["reasons"]}),
            &format!(
                "WaitingForInference until {}: {reasons}",
                text_value(&wait["wake_at"])
            ),
            json,
        );
    }
    for event in details["events"]
        .as_array()
        .context("invalid event response")?
    {
        if json {
            println!("{event}");
        } else {
            println!(
                "{} {event}",
                event
                    .as_object()
                    .and_then(|map| map.values().next())
                    .and_then(|inner| inner["seq"].as_u64())
                    .unwrap_or(0)
            );
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
                    let value = json!({"name":image.name,"tag":image.tag,"manifest_id":image.id});
                    print(
                        &value,
                        &format!("{}:{} {}", image.name, image.tag, image.id),
                        json,
                    );
                    after = Some(format!("{}:{}", image.name, image.tag));
                }
            }
        }
        image_command::Command::Show { image } => {
            let (name, tag) = image.split_once(':').context("expected NAME:TAG")?;
            validate_label(name)?;
            validate_label(tag)?;
            let value = projection(endpoint, client.cli_image(name, tag))
                .await
                .map_err(|error| {
                    if error.to_string().contains("image_not_found") {
                        anyhow::anyhow!("image not found")
                    } else {
                        error
                    }
                })?;
            print(
                &value,
                &format!(
                    "{image} {}\nsize={} chunk_size={} root_hash={} scratch={}",
                    value["manifest_id"],
                    value["header"]["size"],
                    value["header"]["chunk_size"],
                    value["header"]["root_hash"],
                    value["scratch"]
                        .as_array()
                        .map(|items| items
                            .iter()
                            .filter_map(Value::as_str)
                            .collect::<Vec<_>>()
                            .join(","))
                        .unwrap_or_default()
                ),
                json,
            );
        }
        image_command::Command::Build { .. } => unreachable!(),
    }
    Ok(())
}

fn settings_text(agent: &Value) -> String {
    let fallback = |key| agent[key].as_str().unwrap_or("(stack default)");
    format!(
        "\nprovider={}\nsystem_prompt={}\nmodel={}\nreasoning_effort={}\nroute={}\nsandbox_memory_mib={}\nsandbox_gpu={}",
        fallback("provider"),
        fallback("system_prompt"),
        fallback("model"),
        fallback("reasoning_effort"),
        fallback("route"),
        agent["requirements"]["memory_mib"],
        match str_field(&agent["requirements"], "gpu") {
            "none" => "None",
            "shared" => "Shared",
            "dedicated" => "Dedicated",
            other => other,
        }
    )
}
fn inference(args: agent_command::InferenceArgs, update: bool) -> Result<Value> {
    let mut value = json!({});
    for (name, setting) in [
        ("provider", args.provider),
        ("model", args.model),
        ("effort", args.effort),
        ("route", args.route),
    ] {
        if let Some(setting) = setting {
            if setting == "default" {
                ensure!(update, "default clears an override only with agent set");
                if !value["resets"].is_array() {
                    value["resets"] = json!([]);
                }
                value["resets"]
                    .as_array_mut()
                    .expect("array initialized")
                    .push(json!(name));
            } else {
                value[name] = json!(setting);
            }
        }
    }
    let prompt = if let Some(path) = args.system_prompt_file {
        Some(
            std::fs::read_to_string(&path)
                .with_context(|| format!("read system prompt {}", path.display()))?,
        )
    } else {
        args.system_prompt
    };
    if let Some(prompt) = prompt {
        value["system_prompt"] = json!(prompt);
    }
    if let Some(memory) = args.memory {
        value["memory"] = json!(memory);
    }
    if let Some(gpu) = args.gpu {
        value["gpu"] = json!(gpu);
    }
    Ok(value)
}
fn text_value(value: &Value) -> String {
    value
        .as_str()
        .map_or_else(|| value.to_string(), str::to_owned)
}
fn text_value_or_dash(value: &Value) -> String {
    if value.is_null() {
        "-".into()
    } else {
        text_value(value)
    }
}
fn print_entry_breakdown(details: &Value, json: bool) {
    if !json && let Some(entries) = entries_text(details) {
        println!("{entries}");
    }
}

/// Print one session's billed totals with the entries behind them. The
/// JSON line carries the same entries so scripts see what the text shows.
fn print_session_usage(usage: &Value, cost_dollars: &Value, details: &Value, json: bool) {
    let tokens = &usage["usage"];
    print(
        &json!({"session_usage":usage,"cost_dollars":cost_dollars,"entries":details["entries"],"providers":details["providers"]}),
        &format!(
            "Usage: input={} cached={} cache_write={} output={} reasoning={} total={} cost=${}",
            tokens["input_tokens"],
            tokens["cached_input_tokens"],
            tokens["cache_write_input_tokens"],
            tokens["output_tokens"],
            tokens["reasoning_output_tokens"],
            tokens["total_tokens"],
            optional_text(cost_dollars),
        ),
        json,
    );
    print_entry_breakdown(details, json);
}

/// Render one owner's per-entry cost shares with the providers involved.
/// The server reads these from the entry rollups, so they survive the raw
/// completion record retention window.
fn entries_text(value: &Value) -> Option<String> {
    let entries = value["entries"].as_array()?;
    let mut text = String::new();
    for entry in entries {
        let _ = write!(
            text,
            "\nentry {} cost=${} input={} output={} total={} completions={}",
            str_field(entry, "entry"),
            optional_text(&entry["cost_dollars"]),
            entry["input_tokens"],
            entry["output_tokens"],
            entry["total_tokens"],
            entry["completions"],
        );
    }
    if let Some(providers) = value["providers"].as_array() {
        let names: Vec<&str> = providers.iter().filter_map(Value::as_str).collect();
        let _ = write!(text, "\nproviders={}", names.join(","));
    }
    Some(text)
}

fn agent_text(agent: &Value, detail: bool) -> String {
    let name = str_field(agent, "name");
    let id = str_field(agent, "agent_id");
    let image = &agent["image"];
    let mut text = format!(
        "{name} {id} image={}:{} node={} scratch_node={} scratch_bytes={} sessions={} created={} main_session={}",
        str_field(image, "name"),
        str_field(image, "tag"),
        str_field(agent, "node_id"),
        str_field(&agent["scratch"], "node_id"),
        agent["scratch"]["bytes"].as_u64().unwrap_or(0),
        agent["session_count"],
        str_field(agent, "created_at"),
        str_field(agent, "main_session")
    );
    if detail {
        text.push_str(&settings_text(agent));
        let usage = &agent["usage"]["usage"];
        let _ = write!(
            text,
            "\nUsage: input={} cached={} cache_write={} output={} reasoning={} total={} cost=${}",
            usage["input_tokens"],
            usage["cached_input_tokens"],
            usage["cache_write_input_tokens"],
            usage["output_tokens"],
            usage["reasoning_output_tokens"],
            usage["total_tokens"],
            text_value(&agent["cost_dollars"])
        );
        if let Some(entries) = entries_text(agent) {
            text.push_str(&entries);
        }
        let last = if agent["last_snapshot_at"].is_null() {
            "-".into()
        } else {
            text_value(&agent["last_snapshot_at"])
        };
        let age = if agent["last_snapshot_age_seconds"].is_null() {
            "-".into()
        } else {
            agent["last_snapshot_age_seconds"].to_string()
        };
        let _ = write!(
            text,
            "\ndescription={}\nplacement_epoch={}\nsandbox_address={}\nsandbox_state={}\nlast_snapshot={} age_seconds={}",
            str_field(agent, "description"),
            text_value_or_dash(&agent["placement"]["epoch"]),
            text_value_or_dash(&agent["sandbox_address"]),
            str_field(agent, "sandbox_state"),
            last,
            age
        );
        if let Some(status) = agent["call_status"].as_object() {
            let _ = write!(
                text,
                "\ncall_holder={} queued_calls={} observed_at={} expires_at={} node={} epoch={}",
                text_value_or_dash(&status["holder_session_id"]),
                status["queued_calls"],
                text_value(&status["observed_at"]),
                text_value(&status["expires_at"]),
                text_value(&status["node_id"]),
                status["epoch"]
            );
        }
        if let Some(sessions) = agent["sessions"].as_array() {
            for session in sessions {
                let _ = write!(
                    text,
                    "\nsession={} state={} computer_deleted={} main={} archived={}",
                    str_field(session, "session_id"),
                    display_state(session),
                    session["computer_deleted"],
                    agent["main_session"] == session["session_id"],
                    session["archived"]
                );
            }
        }
    }
    text
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
                let page = request(endpoint, client.cli_agents(after.as_deref(), 256))
                    .await?
                    .into_iter()
                    .map(serde_json::to_value)
                    .collect::<Result<Vec<_>, _>>()?;
                if page.is_empty() {
                    break;
                }
                for row in page {
                    print(&row, &agent_text(&row, false), json);
                    after = Some(str_field(&row, "agent_id").to_owned());
                }
            }
        }
        agent_command::Command::Show { name } => {
            let row = projection(endpoint, client.cli_agent(&name)).await?;
            print(&row, &agent_text(&row, true), json);
        }
        agent_command::Command::Create {
            name,
            image,
            description,
            inference: flags,
            github_token,
            github_token_stdin,
        } => {
            let token = if github_token_stdin {
                use std::io::Read as _;
                let mut token = String::new();
                std::io::stdin().read_to_string(&mut token)?;
                Some(token.trim_end_matches(['\r', '\n']).to_owned())
            } else {
                github_token
            };
            let mut body = inference(flags, false)?;
            body["name"] = json!(name);
            body["description"] = json!(description);
            body["image"] = json!(
                image.or_else(|| swarmy_config::Settings::load().ok()?.settings.default_image)
            );
            body["github_token"] = json!(token);
            body["idempotency_key"] = json!(Ulid::generate().to_string());
            let created = projection(
                endpoint,
                client.cli_create_agent(&serde_json::from_value(body)?),
            )
            .await?;
            print(
                &created,
                &format!(
                    "Created agent {} {} image={}:{}{}",
                    str_field(&created, "name"),
                    str_field(&created, "agent_id"),
                    str_field(&created["image"], "name"),
                    str_field(&created["image"], "tag"),
                    settings_text(&created)
                ),
                json,
            );
        }
        agent_command::Command::Set {
            name,
            inference: flags,
            github_token,
            clear_github_token,
        } => {
            let mut body = inference(flags, true)?;
            body["github_token"] = json!(github_token.clone());
            body["clear_github_token"] = json!(clear_github_token);
            body["idempotency_key"] = json!(Ulid::generate().to_string());
            let updated = projection(
                endpoint,
                client.cli_update_agent(&name, &serde_json::from_value(body)?),
            )
            .await?;
            print(
                &updated,
                &format!(
                    "Updated agent {} {}{}",
                    str_field(&updated, "name"),
                    str_field(&updated, "agent_id"),
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
    let current = projection(endpoint, client.cli_agent(name)).await?;
    if !yes {
        use std::io::{IsTerminal as _, Write as _};
        ensure!(
            std::io::stdin().is_terminal(),
            "agent delete requires confirmation; pass --yes for noninteractive deletion"
        );
        eprint!(
            "Delete agent {} and its computer? [y/N] ",
            str_field(&current, "name")
        );
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
    let value =
        json!({"event":"agent_deleted","agent_id":current["agent_id"],"name":current["name"]});
    print(
        &value,
        &format!(
            "Deleted agent {} {}",
            str_field(&current, "name"),
            str_field(&current, "agent_id")
        ),
        json,
    );

    Ok(())
}

fn key_from_source(args: &auth_command::Set, provider: &str) -> Result<String> {
    if let Some(key) = &args.source.api_key {
        return Ok(key.clone());
    }
    if let Some(path) = &args.source.file {
        return Ok(std::fs::read_to_string(path)
            .context("read API key file")?
            .trim()
            .to_owned());
    }
    let names: &[&str] = match provider {
        "anthropic" => &["ANTHROPIC_API_KEY"],
        "openai" => &["OPENAI_API_KEY"],
        "xai" => &["XAI_API_KEY"],
        "meta" => &["META_MODEL_API_KEY"],
        "openrouter" => &["OPENROUTER_API_KEY"],
        "azure" => &["AZURE_API_KEY", "AZURE_OPENAI_API_KEY"],
        "amazon-bedrock" => &["AWS_BEARER_TOKEN_BEDROCK"],
        "google" => &["GEMINI_API_KEY", "GOOGLE_API_KEY"],
        "google-vertex" | "google-vertex-anthropic" => &["GOOGLE_CLOUD_API_KEY"],
        provider => {
            anyhow::bail!("no API key environment mapping for {provider}; use --api-key or --file")
        }
    };
    names
        .iter()
        .find_map(|name| std::env::var(name).ok().filter(|value| !value.is_empty()))
        .with_context(|| format!("set {} before using --from-env", names.join(" or ")))
}
fn auth_report(event: &str, provider: &str, json: bool) {
    print(
        &json!({"event":event,"provider":provider}),
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
    let key = key_from_source(args, provider)?;
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
    let record = swarmy_core::CredentialRecord {
        kind: swarmy_core::CredentialKind::ApiKey { key, extra },
        updated_at: jiff::Timestamp::now(),
    };
    let body = json!({"idempotency_key":Ulid::generate().to_string(),"provider":provider,"label":args.label,"record":record});
    projection(
        endpoint,
        client.cli_set_credential(&serde_json::from_value(body)?),
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
            &serde_json::from_value(json!({
                "idempotency_key": Ulid::generate().to_string(),
                "limit": limit,
                "window_seconds": window_seconds,
            }))?,
        ),
    )
    .await?;
    Ok(())
}
fn auth_display(summary: &Value, json: bool, expiry: bool) {
    let mut value = summary.clone();
    let seconds = summary["expires_at"]
        .as_str()
        .and_then(|at| at.parse::<jiff::Timestamp>().ok())
        .map(|at| at.as_second() - jiff::Timestamp::now().as_second());
    if expiry {
        value["expires_in_seconds"] = json!(seconds);
    }
    if json {
        println!("{value}");
    } else {
        print!(
            "{}\t{}\t{}\t{}\t{}",
            str_field(summary, "provider"),
            str_field(summary, "kind"),
            str_field(summary, "label"),
            str_field(summary, "status"),
            str_field(summary, "updated_at")
        );
        if expiry && let Some(seconds) = seconds {
            print!("\texpires in {seconds}s");
        }
        println!();
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
            for summary in request(endpoint, client.cli_credentials())
                .await?
                .into_iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()?
            {
                auth_display(&summary, json, false);
            }
        }
        auth_command::Command::Check { provider, label } => {
            let rows: Vec<_> = request(endpoint, client.cli_credentials())
                .await?
                .into_iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .filter(|row| {
                    provider
                        .as_ref()
                        .is_none_or(|provider| row["provider"] == *provider)
                        && label.as_ref().is_none_or(|label| row["label"] == *label)
                })
                .collect();
            ensure!(!rows.is_empty(), "credential does not exist");
            let ready = rows.iter().all(|row| row["status"] == "ready");
            let expired_bedrock = rows
                .iter()
                .any(|row| row["provider"] == "amazon-bedrock" && row["status"] == "expired");
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
                &group,
                since.as_deref(),
                until.as_deref(),
                json,
            )
            .await?;
        }
        _ => unreachable!("login and import remain local"),
    }
    Ok(())
}

fn optional_text(value: &Value) -> String {
    if value.is_null() {
        "-".into()
    } else {
        text_value(value)
    }
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
    group: &str,
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
                group,
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

fn route_text(name: &str, steps: &[Value]) -> String {
    let steps = steps
        .iter()
        .map(|step| {
            let model = step["model"].as_str().unwrap_or("");
            if model.is_empty() {
                format!(
                    "{}/{}",
                    str_field(step, "provider"),
                    str_field(step, "entry")
                )
            } else {
                format!(
                    "{}/{}={model}",
                    str_field(step, "provider"),
                    str_field(step, "entry")
                )
            }
        })
        .collect::<Vec<_>>()
        .join(" ");
    format!("{name} {steps}")
}

async fn routes(
    client: &Client,
    endpoint: &str,
    command: auth_command::RoutesCommand,
    json: bool,
) -> Result<()> {
    match command {
        auth_command::RoutesCommand::Ls => {
            for row in request(endpoint, client.cli_routes())
                .await?
                .into_iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()?
            {
                let steps = row["steps"].as_array().cloned().unwrap_or_default();
                print(&row, &route_text(str_field(&row, "name"), &steps), json);
            }
        }
        auth_command::RoutesCommand::Show { name } => {
            let row = projection(endpoint, client.cli_route(&name)).await?;
            let steps = row["steps"].as_array().cloned().unwrap_or_default();
            print(&row, &route_text(str_field(&row, "name"), &steps), json);
        }
        auth_command::RoutesCommand::Set { name, steps } => {
            ensure!(!steps.is_empty(), "a route needs at least one step");
            let mut parsed = Vec::with_capacity(steps.len());
            for step in &steps {
                let step = swarmy_core::route::parse_step(step)
                    .map_err(|message| anyhow::anyhow!("{message}"))?;
                parsed.push(serde_json::json!({
                    "provider": step.provider,
                    "entry": step.entry,
                    "model": step.model,
                }));
            }
            projection(
                endpoint,
                client.cli_set_route(&serde_json::from_value(serde_json::json!({
                    "idempotency_key": Ulid::generate().to_string(),
                    "name": name,
                    "steps": parsed,
                }))?),
            )
            .await?;
            print(
                &serde_json::json!({"event":"saved","route":name}),
                &format!("{name}: saved"),
                json,
            );
        }
        auth_command::RoutesCommand::Rm { name } => {
            request(
                endpoint,
                client.cli_remove_route(&name, &Ulid::generate().to_string()),
            )
            .await?;
            print(
                &serde_json::json!({"event":"removed","route":name}),
                &format!("{name}: removed"),
                json,
            );
        }
    }
    Ok(())
}
