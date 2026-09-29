use anyhow::{Context, ensure};
use clap::Subcommand;
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
    // Probes use the control plane credential resolver, not laptop credentials.
    if let Command::Probe(args) = command {
        return crate::models_probe::run(args, json).await;
    }
    let (client, endpoint) = swarmy_client::api_client::connect()?;
    match command {
        Command::Probe(_) => unreachable!("probes dispatch before listing"),
        Command::Ls {
            provider,
            reasoning,
        } => {
            if let Some(id) = &provider {
                known_provider(&client, &endpoint, id).await?;
            }
            let rows = swarmy_client::api_client::call(
                &endpoint,
                client.models_filtered(None, provider.as_deref(), reasoning),
            )
            .await?;
            print_models(&rows, json)?;
        }
        Command::Show { model } => {
            let (provider, id) = model.split_once('/').context("expected PROVIDER/MODEL")?;
            known_provider(&client, &endpoint, provider).await?;
            let rows = swarmy_client::api_client::call(
                &endpoint,
                client.models_filtered(Some(id), Some(provider), false),
            )
            .await?;
            let row = rows
                .iter()
                .find(|row| row.key == model)
                .with_context(|| format!("unknown model: {model}"))?;
            if json {
                println!("{}", serde_json::to_string(row)?);
            } else {
                println!("{}", serde_json::to_string_pretty(row)?);
            }
        }
        Command::Search { pattern } => {
            let rows = swarmy_client::api_client::call(
                &endpoint,
                client.models_filtered(Some(&pattern), None, false),
            )
            .await?;
            ensure!(!rows.is_empty(), "no models found matching {pattern:?}");
            print_models(&rows, json)?;
        }
        Command::Providers => {
            let rows = swarmy_client::api_client::call(&endpoint, client.providers()).await?;
            if json {
                println!("{}", serde_json::to_string(&rows)?);
            } else {
                for row in rows {
                    line(&format!(
                        "{}  {}  credential: {}",
                        row.id, row.api, row.credential
                    ));
                    line(&format!("  Auth: {}", row.auth_kinds.join(", ")));
                    let env = row.env_keys.join(", ");
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
    let providers = swarmy_client::api_client::call(endpoint, client.providers()).await?;
    ensure!(
        providers.iter().any(|provider| provider.id == id),
        "unknown provider: {id}"
    );
    Ok(())
}
fn print_models(rows: &[swarmy_api_types::Model], json: bool) -> anyhow::Result<()> {
    if json {
        println!("{}", serde_json::to_string(rows)?);
        return Ok(());
    }
    for row in rows {
        line(&row.key);
        line(&format!("  {}", row.name));
        line("  CONTEXT     OUTPUT      INPUT $/M    OUTPUT $/M");
        line(&format!(
            "  {:<11} {:<11} {:<12} {}",
            row.limit.context,
            row.limit
                .output
                .map_or_else(|| "unknown".into(), |n| n.to_string()),
            row.cost.input,
            row.cost.output
        ));
        line(&format!(
            "  Efforts: {}",
            row.supported_efforts
                .iter()
                .map(|effort| format!("{effort:?}").to_lowercase())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    Ok(())
}
// Keep long catalog ids and environment lists readable on narrow terminals.
fn line(value: &str) {
    #[cfg(feature = "chat")]
    let width = crossterm::terminal::size()
        .map_or(80, |(width, _)| usize::from(width))
        .clamp(20, 100);
    #[cfg(not(feature = "chat"))]
    let width = 80;
    let mut output = String::new();
    let mut column = 0;
    for word in value.split_inclusive(' ') {
        if column > 2 && column + display_width(word) > width {
            output.push_str("\n  ");
            column = 2;
        }
        for ch in word.chars() {
            let size = char_width(ch);
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

#[cfg(feature = "chat")]
fn display_width(value: &str) -> usize {
    unicode_width::UnicodeWidthStr::width(value)
}
#[cfg(not(feature = "chat"))]
fn display_width(value: &str) -> usize {
    value.chars().count()
}
#[cfg(feature = "chat")]
fn char_width(value: char) -> usize {
    unicode_width::UnicodeWidthChar::width(value).unwrap_or(0)
}
#[cfg(not(feature = "chat"))]
fn char_width(_value: char) -> usize {
    1
}
