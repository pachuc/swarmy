use std::{
    fmt::Write as _,
    io::{IsTerminal, Write as _},
};

use anyhow::{Context, Result, ensure};
use jiff::Timestamp;
use swarmy_core::{AgentId, AgentRecord, AgentSettings, SessionRecord, VolumeId};
use swarmy_store::{MAX_SCAN_LIMIT, Store};

use crate::{
    agent_command::{Command, InferenceArgs},
    conversation::store,
    vol::output,
};

pub async fn resolve(store: &Store, name: &str) -> Result<AgentRecord> {
    // Prefer a literal name, including names that happen to parse as a ULID.
    if let Some(agent) = store.get_agent_by_name(name).await? {
        return Ok(agent);
    }
    if let Ok(id) = name.parse::<ulid::Ulid>()
        && let Some(agent) = store.get_agent(AgentId::from_ulid(id)).await?
    {
        return Ok(agent);
    }
    anyhow::bail!("agent {name} not found")
}

async fn sessions(store: &Store, id: AgentId) -> Result<Vec<SessionRecord>> {
    let mut records = Vec::new();
    let mut after = None;
    loop {
        let page = store
            .list_sessions_by_agent(id, after, MAX_SCAN_LIMIT)
            .await?;
        if page.is_empty() {
            break;
        }
        after = page.last().map(|session| session.session_id);
        records.extend(page);
    }
    Ok(records)
}

pub async fn run(command: Command, json: bool) -> Result<()> {
    let store = store().await?;
    match command {
        Command::Create {
            name,
            image,
            description,
            inference,
            github_token,
        } => {
            let overrides = inference_settings(inference)?;
            let settings = swarmy_config::Settings::load()?.settings;
            let agent = store
                .create_agent_with(
                    &name,
                    settings.session_image(image.as_deref())?,
                    &description,
                    &overrides,
                    github_token.as_deref(),
                    Timestamp::now(),
                )
                .await?;
            output(
                &serde_json::to_value(&agent)?,
                &format!(
                    "Created agent {} {} image={}:{}{}",
                    agent.name,
                    agent.agent_id,
                    agent.image.name,
                    agent.image.tag.0,
                    settings_text(&agent)
                ),
                json,
            )?;
        }
        Command::Set {
            name,
            inference,
            github_token,
            clear_github_token,
        } => {
            let settings = inference_settings(inference)?;
            let token_change = github_token.is_some() || clear_github_token;
            ensure!(
                settings != AgentSettings::default() || token_change,
                "agent set requires --system-prompt, --system-prompt-file, --model, --effort, \
                 --github-token, or --clear-github-token"
            );
            let mut agent = resolve(&store, &name).await?;
            if settings != AgentSettings::default() {
                agent = store.set_agent(agent.agent_id, &settings).await?;
            }
            if token_change {
                store
                    .set_agent_github_token(agent.agent_id, github_token.as_deref())
                    .await?;
            }
            output(
                &serde_json::to_value(&agent)?,
                &format!(
                    "Updated agent {} {}{}",
                    agent.name,
                    agent.agent_id,
                    settings_text(&agent)
                ),
                json,
            )?;
        }
        Command::Ls => {
            let mut after = None;
            loop {
                let page = store.list_agents(after, MAX_SCAN_LIMIT).await?;
                if page.is_empty() {
                    break;
                }
                for agent in page {
                    after = Some(agent.agent_id);
                    show(&store, &agent, false, json).await?;
                }
            }
        }
        Command::Show { name } => show(&store, &resolve(&store, &name).await?, true, json).await?,
        Command::Delete { name, yes } => {
            let agent = resolve(&store, &name).await?;
            if !yes {
                confirm(&agent.name)?;
            }
            store.delete_agent(agent.agent_id).await?;
            output(
                &serde_json::json!({"event": "agent_deleted", "agent_id": agent.agent_id, "name": agent.name}),
                &format!("Deleted agent {} {}", agent.name, agent.agent_id),
                json,
            )?;
        }
    }
    Ok(())
}

fn inference_settings(args: InferenceArgs) -> Result<AgentSettings> {
    let system_prompt = match args.system_prompt_file {
        Some(path) => Some(
            std::fs::read_to_string(&path)
                .with_context(|| format!("read system prompt {}", path.display()))?,
        ),
        None => args.system_prompt,
    };
    Ok(AgentSettings {
        system_prompt,
        model: args.model,
        reasoning_effort: args.effort,
    })
}

fn settings_text(agent: &AgentRecord) -> String {
    format!(
        "\nsystem_prompt={}\nmodel={}\nreasoning_effort={}",
        agent.system_prompt.as_deref().unwrap_or("(stack default)"),
        agent.model.as_deref().unwrap_or("(stack default)"),
        agent
            .reasoning_effort
            .map_or("(stack default)", swarmy_core::ReasoningEffort::as_str),
    )
}

fn confirm(name: &str) -> Result<()> {
    ensure!(
        std::io::stdin().is_terminal(),
        "agent delete requires confirmation; pass --yes for noninteractive deletion"
    );
    eprint!("Delete agent {name} and its computer? [y/N] ");
    std::io::stderr().flush()?;
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer)?;
    ensure!(
        matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes"),
        "agent deletion cancelled"
    );
    Ok(())
}

async fn show(store: &Store, agent: &AgentRecord, detail: bool, json: bool) -> Result<()> {
    let sessions = sessions(store, agent.agent_id).await?;
    let placement = store.get_by_agent(agent.agent_id).await?;
    let node = placement.as_ref().map(|record| record.node_id);
    let mut value = serde_json::to_value(agent)?;
    value["node_id"] = serde_json::to_value(node)?;
    value["session_count"] = sessions.len().into();
    let mut text = format!(
        "{} {} image={}:{} node={} sessions={} created={} main_session={}",
        agent.name,
        agent.agent_id,
        agent.image.name,
        agent.image.tag.0,
        node.map_or_else(|| "-".into(), |id| id.to_string()),
        sessions.len(),
        agent.created_at,
        agent
            .main_session
            .map_or_else(|| "-".into(), |id| id.to_string())
    );
    if detail {
        text.push_str(&settings_text(agent));
        let totals = store.agent_usage(agent.agent_id).await?;
        value["usage"] = serde_json::to_value(&totals)?;
        value["cost_dollars"] = totals.dollars().into();
        write!(
            text,
            "\nUsage: input={} cached={} cache_write={} output={} reasoning={} total={} cost=${}",
            totals.usage.input_tokens,
            totals.usage.cached_input_tokens,
            totals.usage.cache_write_input_tokens,
            totals.usage.output_tokens,
            totals.usage.reasoning_output_tokens,
            totals.usage.total_tokens,
            totals.dollars()
        )?;
        let volume = store
            .get_volume(VolumeId::from_ulid(agent.agent_id.as_ulid()))
            .await?;
        // Manifest ULIDs supply the same snapshot timestamp used in recovery notices.
        let snapshot_at = volume
            .as_ref()
            .map(|volume| {
                Timestamp::from_millisecond(
                    i64::try_from(volume.head_manifest.as_ulid().timestamp_ms())
                        .context("snapshot timestamp overflow")?,
                )
                .map_err(anyhow::Error::from)
            })
            .transpose()?;
        let age = snapshot_at.map(|time| Timestamp::now().duration_since(time).as_secs().max(0));
        let status = store.agent_call_status(agent.agent_id).await?;
        let state = call_status(&mut value, &mut text, status.as_ref())?;
        value["placement"] = serde_json::to_value(&placement)?;
        value["last_snapshot_at"] = serde_json::to_value(snapshot_at)?;
        value["last_snapshot_age_seconds"] = serde_json::to_value(age)?;
        let mut listed = Vec::new();
        for session in &sessions {
            let mut value = serde_json::to_value(session)?;
            let next = store.next_session(session.session_id).await?;
            value["archived"] = next.is_some().into();
            value["next_session"] = serde_json::to_value(next)?;
            listed.push(value);
        }
        value["sessions"] = listed.into();
        write!(
            text,
            "\ndescription={}\nplacement_epoch={}\nsandbox_state={state}\nlast_snapshot={} age_seconds={}",
            agent.description,
            placement
                .as_ref()
                .map_or_else(|| "-".into(), |record| record.epoch.to_string()),
            snapshot_at.map_or_else(|| "-".into(), |time| time.to_string()),
            age.map_or_else(|| "-".into(), |age| age.to_string())
        )?;
        for session in sessions {
            write!(
                text,
                "\nsession={} state={:?} computer_deleted={} main={} archived={}",
                session.session_id,
                session.state,
                session.computer_deleted,
                agent.main_session == Some(session.session_id),
                store.next_session(session.session_id).await?.is_some()
            )?;
        }
    }
    output(&value, &text, json)
}

fn call_status(
    value: &mut serde_json::Value,
    text: &mut String,
    status: Option<&swarmy_core::AgentCallStatus>,
) -> Result<&'static str> {
    let state = match status {
        Some(status) if status.holder_session_id.is_some() || status.queued_calls > 0 => "busy",
        Some(_) => "idle",
        None => "unknown",
    };
    value["sandbox_state"] = state.into();
    value["sandbox_state_reason"] = if status.is_some() {
        "sampled node call occupancy"
    } else {
        "no current node call observation"
    }
    .into();
    value["call_status"] = serde_json::to_value(status)?;
    if let Some(status) = status {
        write!(
            text,
            "\ncall_holder={} queued_calls={} observed_at={} expires_at={} node={} epoch={}",
            status
                .holder_session_id
                .map_or_else(|| "-".into(), |id| id.to_string()),
            status.queued_calls,
            status.observed_at,
            status.expires_at,
            status.node_id,
            status.epoch,
        )?;
    }
    Ok(state)
}
