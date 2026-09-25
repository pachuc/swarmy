//! Commands whose state is read or mutated through the control-plane API.
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::fmt::Write as _;
use swarmy_client::Client;

use ulid::Ulid;

use crate::{Command, agent_command, auth_command, image_command, session_command};

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
                || inference.memory.is_some()
                || inference.gpu.is_some()
                || github_token.is_some()
                || *clear_github_token,
            "agent set requires --system-prompt, --system-prompt-file, --provider, --model, --effort, \
                 --memory, --gpu, --github-token, or --clear-github-token"
        );
    }
    let (client, endpoint) = crate::api_client::connect()?;
    match command {
        Command::Session { command } => session(&client, &endpoint, command, json).await?,
        Command::Agent { command } => agent(&client, &endpoint, command, json).await?,
        Command::Image { command } => image(&client, &endpoint, command, json).await?,
        Command::Auth { command, .. } => auth(&client, &endpoint, command, json).await?,
        _ => unreachable!("only API commands reach this dispatcher"),
    }
    Ok(())
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
                            "{id} {} {}/{} kind={kind} agent={name} head={} computer_deleted={} archived={} main={}",
                            display_state(&row),
                            str_field(selection, "provider"),
                            str_field(selection, "model"),
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
        session_command::Command::Metrics { session_id } => {
            session_metrics(client, endpoint, session_id, json).await?;
        }
        _ => unreachable!("session close and interrupt run in swarmy-session"),
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
            "Session {id}: {}, interrupt_requested={} provider={}{} model={}{} effort={}{} scratch_node={} scratch_bytes={} sandbox_memory_mib={} sandbox_gpu={gpu} sandbox_address={}",
            display_state(record),
            record["interrupt_requested"],
            str_field(selection, "provider"),
            inherited("provider"),
            str_field(selection, "model"),
            inherited("model"),
            str_field(selection, "effort"),
            inherited("effort"),
            str_field(scratch, "node_id"),
            scratch["bytes"].as_u64().unwrap_or(0),
            requirements["memory_mib"],
            text_value_or_dash(&details["address"])
        ),
        json,
    );
    let usage = &details["usage"]["usage"];
    print(
        &json!({"session_usage":details["usage"],"cost_dollars":details["cost_dollars"]}),
        &format!(
            "Usage: input={} cached={} cache_write={} output={} reasoning={} total={} cost=${}",
            usage["input_tokens"],
            usage["cached_input_tokens"],
            usage["cache_write_input_tokens"],
            usage["output_tokens"],
            usage["reasoning_output_tokens"],
            usage["total_tokens"],
            details["cost_dollars"]
        ),
        json,
    );
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
        "\nprovider={}\nsystem_prompt={}\nmodel={}\nreasoning_effort={}\nsandbox_memory_mib={}\nsandbox_gpu={}",
        fallback("provider"),
        fallback("system_prompt"),
        fallback("model"),
        fallback("reasoning_effort"),
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

fn key_from_source(args: &auth_command::Set) -> Result<String> {
    if let Some(key) = &args.source.api_key {
        return Ok(key.clone());
    }
    if let Some(path) = &args.source.file {
        return Ok(std::fs::read_to_string(path)
            .context("read API key file")?
            .trim()
            .to_owned());
    }
    let names: &[&str] = match args.provider.as_str() {
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
            "{}\t{}\t{}\t{}",
            str_field(summary, "provider"),
            str_field(summary, "kind"),
            str_field(summary, "status"),
            str_field(summary, "updated_at")
        );
        if expiry && let Some(seconds) = seconds {
            print!("\texpires in {seconds}s");
        }
        println!();
    }
}
async fn auth(
    client: &Client,
    endpoint: &str,
    command: auth_command::Command,
    json: bool,
) -> Result<()> {
    match command {
        auth_command::Command::Set(args) => {
            ensure!(
                !args.provider.is_empty()
                    && args
                        .provider
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
                "invalid provider id"
            );
            ensure!(
                args.provider != "chatgpt",
                "chatgpt requires OAuth; use auth login chatgpt"
            );
            let key = key_from_source(&args)?;
            ensure!(!key.trim().is_empty(), "API key must not be empty");
            let mut extra: std::collections::BTreeMap<String, String> =
                args.extra.into_iter().collect();
            if args.provider == "azure" && args.source.from_env {
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
            let body = json!({"idempotency_key":Ulid::generate().to_string(),"provider":args.provider,"record":record});
            projection(
                endpoint,
                client.cli_set_credential(&serde_json::from_value(body)?),
            )
            .await?;
            auth_report("saved", &args.provider, json);
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
        auth_command::Command::Check { provider } => {
            let rows = if let Some(provider) = provider {
                let summary = request(endpoint, client.cli_credential(&provider))
                    .await
                    .with_context(|| format!("credential for {provider} does not exist"))?;
                vec![serde_json::to_value(summary)?]
            } else {
                request(endpoint, client.cli_credentials())
                    .await?
                    .into_iter()
                    .map(serde_json::to_value)
                    .collect::<Result<Vec<_>, _>>()?
            };
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
        auth_command::Command::Rm { provider } => {
            request(
                endpoint,
                client.remove_credential(&provider, &Ulid::generate().to_string()),
            )
            .await?;
            auth_report("removed", &provider, json);
        }
        _ => unreachable!("login and import remain local"),
    }
    Ok(())
}
