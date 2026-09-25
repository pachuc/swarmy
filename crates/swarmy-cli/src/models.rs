use anyhow::{Context, ensure};
use clap::Subcommand;
use serde_json::Value;

fn model_row(row: swarmy_api_types::Model) -> Result<Value, serde_json::Error> {
    let mut value = serde_json::to_value(row)?;
    value
        .as_object_mut()
        .expect("model object")
        .remove("provider_id");
    value
        .as_object_mut()
        .expect("model object")
        .remove("context_window");
    Ok(value)
}
fn provider_row(row: swarmy_api_types::Provider) -> Result<Value, serde_json::Error> {
    let mut value = serde_json::to_value(row)?;
    value
        .as_object_mut()
        .expect("provider object")
        .remove("name");
    value
        .as_object_mut()
        .expect("provider object")
        .remove("status");
    Ok(value)
}

use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

#[derive(Subcommand)]
pub enum Command {
    /// List available models.
    Ls {
        #[arg(long)]
        provider: Option<String>,
        #[arg(long)]
        reasoning: bool,
    },
    /// Show one model by PROVIDER/MODEL.
    Show { model: String },
    /// Search models by name or identifier.
    Search { pattern: String },
    /// List providers and their authentication methods.
    Providers,
    /// Probe a model with a live request.
    Probe(crate::models_probe_command::Args),
}

pub async fn run(command: Command, json: bool) -> anyhow::Result<()> {
    // Probes verify providers directly from the CLI and need no API.
    if let Command::Probe(args) = command {
        return crate::models_probe::run(args, json).await;
    }
    let (client, endpoint) = crate::api_client::connect()?;
    match command {
        Command::Probe(_) => unreachable!("probes run without the API"),
        Command::Ls {
            provider,
            reasoning,
        } => {
            if let Some(id) = &provider {
                known_provider(&client, &endpoint, id).await?;
            }
            let rows = crate::api_client::call(
                &endpoint,
                client.cli_models(None, provider.as_deref(), reasoning),
            )
            .await?;
            print_models(
                &rows
                    .into_iter()
                    .map(model_row)
                    .collect::<Result<Vec<_>, _>>()?,
                json,
            )?;
        }
        Command::Show { model } => {
            let (provider, id) = model.split_once('/').context("expected PROVIDER/MODEL")?;
            known_provider(&client, &endpoint, provider).await?;
            let rows = crate::api_client::call(
                &endpoint,
                client.cli_models(Some(id), Some(provider), false),
            )
            .await?;
            let rows = rows
                .into_iter()
                .map(model_row)
                .collect::<Result<Vec<_>, _>>()?;
            let row = rows
                .iter()
                .find(|row| row["key"] == model)
                .with_context(|| format!("unknown model: {model}"))?;
            if json {
                println!("{}", serde_json::to_string(row)?);
            } else {
                println!("{}", serde_json::to_string_pretty(row)?);
            }
        }
        Command::Search { pattern } => {
            let rows =
                crate::api_client::call(&endpoint, client.cli_models(Some(&pattern), None, false))
                    .await?;
            ensure!(!rows.is_empty(), "no models found matching {pattern:?}");
            print_models(
                &rows
                    .into_iter()
                    .map(model_row)
                    .collect::<Result<Vec<_>, _>>()?,
                json,
            )?;
        }
        Command::Providers => {
            let rows = crate::api_client::call(&endpoint, client.cli_providers()).await?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string(
                        &rows
                            .into_iter()
                            .map(provider_row)
                            .collect::<Result<Vec<_>, _>>()?
                    )?
                );
            } else {
                for row in rows {
                    let row = provider_row(row)?;
                    let api: swarmy_llm::catalog::Api = serde_json::from_value(row["api"].clone())?;
                    line(&format!(
                        "{}  {:?}  credential: {}",
                        text(&row["id"]),
                        api,
                        text(&row["credential"])
                    ));
                    line(&format!("  Auth: {}", joined(&row["auth_kinds"])));
                    let env = joined(&row["env_keys"]);
                    line(&format!(
                        "  Env: {}",
                        if env.is_empty() { "-" } else { &env }
                    ));
                }
            }
        }
    }
    Ok(())
}
async fn known_provider(
    client: &swarmy_client::Client,
    endpoint: &str,
    id: &str,
) -> anyhow::Result<()> {
    let providers = crate::api_client::call(endpoint, client.cli_providers()).await?;
    ensure!(
        providers.iter().any(|provider| provider.id == id),
        "unknown provider: {id}"
    );
    Ok(())
}
fn text(value: &Value) -> String {
    value
        .as_str()
        .map_or_else(|| value.to_string(), str::to_owned)
}
fn joined(value: &Value) -> String {
    value
        .as_array()
        .map(|items| items.iter().map(text).collect::<Vec<_>>().join(", "))
        .unwrap_or_default()
}
fn print_models(rows: &[Value], json: bool) -> anyhow::Result<()> {
    if json {
        println!("{}", serde_json::to_string(rows)?);
        return Ok(());
    }
    for row in rows {
        line(&text(&row["key"]));
        line(&format!("  {}", text(&row["name"])));
        line("  CONTEXT     OUTPUT      INPUT $/M    OUTPUT $/M");
        line(&format!(
            "  {:<11} {:<11} {:<12} {}",
            row["limit"]["context"],
            if row["limit"]["output"].is_null() {
                "unknown".into()
            } else {
                row["limit"]["output"].to_string()
            },
            row["cost"]["input"],
            row["cost"]["output"]
        ));
        line(&format!("  Efforts: {}", joined(&row["supported_efforts"])));
    }
    Ok(())
}
// Keep long catalog ids and environment lists readable on narrow terminals.
fn line(value: &str) {
    let width = crossterm::terminal::size()
        .map_or(80, |(width, _)| usize::from(width))
        .clamp(20, 100);
    let mut output = String::new();
    let mut column = 0;
    for word in value.split_inclusive(' ') {
        if column > 2 && column + word.width() > width {
            output.push_str("\n  ");
            column = 2;
        }
        for ch in word.chars() {
            let size = ch.width().unwrap_or(0);
            if column + size > width {
                output.push_str("\n  ");
                column = 2;
            }
            output.push(ch);
            column += size;
        }
    }
    println!("{output}");
}
