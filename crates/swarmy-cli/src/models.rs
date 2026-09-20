use anyhow::{Context, ensure};
use clap::Subcommand;
use serde::Serialize;
use swarmy_core::ReasoningEffort;
use swarmy_llm::catalog::{Api, ModelInfo, ProviderInfo};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

#[derive(Subcommand)]
pub enum Command {
    /// List models, ordered by provider and model id
    Ls {
        #[arg(long)]
        provider: Option<String>,
        /// Include only models supporting a reasoning effort other than none
        #[arg(long)]
        reasoning: bool,
    },
    /// Show all metadata and compatibility flags for PROVIDER/MODEL
    Show { model: String },
    /// Find case-insensitive substrings in provider/model ids
    Search { pattern: String },
    /// List provider protocols, authentication kinds, and environment variables
    Providers,
    /// Send a small request directly to a provider, bypassing gateway routing
    Probe(crate::models_probe_command::Args),
}

#[derive(Serialize)]
struct ModelRow<'a> {
    key: String,
    provider: &'a str,
    effective_api: Api,
    effective_base_url: &'a str,
    supported_efforts: Vec<ReasoningEffort>,
    #[serde(flatten)]
    model: &'a ModelInfo,
}

impl<'a> ModelRow<'a> {
    fn new(provider: &'a ProviderInfo, model: &'a ModelInfo) -> Self {
        Self {
            key: format!("{}/{}", provider.id, model.id),
            provider: &provider.id,
            effective_api: model.api.unwrap_or(provider.api),
            effective_base_url: model.base_url.as_deref().unwrap_or(&provider.base_url),
            supported_efforts: model.supported_efforts(),
            model,
        }
    }
}

#[derive(Serialize)]
struct ProviderRow<'a> {
    id: &'a str,
    api: Api,
    auth_kinds: &'a [String],
    env_keys: &'a [String],
    credential: &'static str,
}

pub fn run(command: Command, json: bool) -> anyhow::Result<()> {
    let catalog = swarmy_config::Settings::load()?.settings.catalog()?;
    match command {
        Command::Probe(_) => unreachable!("probes run in swarmy-session"),
        Command::Ls {
            provider,
            reasoning,
        } => {
            if let Some(id) = &provider {
                ensure!(catalog.provider(id).is_some(), "unknown provider: {id}");
            }
            let rows = catalog
                .find("")
                .into_iter()
                .filter(|(info, model)| {
                    provider.as_ref().is_none_or(|id| *id == info.id)
                        && (!reasoning
                            || model
                                .supported_efforts()
                                .iter()
                                .any(|effort| *effort != ReasoningEffort::None))
                })
                .map(|(provider, model)| ModelRow::new(provider, model))
                .collect::<Vec<_>>();
            print_models(&rows, json)?;
        }
        Command::Show { model } => {
            let (provider_id, id) = model.split_once('/').context("expected PROVIDER/MODEL")?;
            let provider = catalog
                .provider(provider_id)
                .with_context(|| format!("unknown provider: {provider_id}"))?;
            let info = catalog
                .model(provider_id, id)
                .with_context(|| format!("unknown model: {model}"))?;
            let row = ModelRow::new(provider, info);
            if json {
                println!("{}", serde_json::to_string(&row)?);
            } else {
                println!("{}", serde_json::to_string_pretty(&row)?);
            }
        }
        Command::Search { pattern } => {
            let rows = catalog
                .find(&pattern)
                .into_iter()
                .map(|(provider, model)| ModelRow::new(provider, model))
                .collect::<Vec<_>>();
            ensure!(!rows.is_empty(), "no models found matching {pattern:?}");
            print_models(&rows, json)?;
        }
        Command::Providers => {
            let rows = catalog
                .providers()
                .map(|provider| ProviderRow {
                    id: &provider.id,
                    api: provider.api,
                    auth_kinds: &provider.auth_kinds,
                    env_keys: &provider.env_keys,
                    credential: "unknown",
                })
                .collect::<Vec<_>>();
            if json {
                println!("{}", serde_json::to_string(&rows)?);
            } else {
                for row in rows {
                    line(&format!(
                        "{}  {:?}  credential: {}",
                        row.id, row.api, row.credential
                    ));
                    line(&format!("  Auth: {}", row.auth_kinds.join(", ")));
                    line(&format!(
                        "  Env: {}",
                        if row.env_keys.is_empty() {
                            "-".into()
                        } else {
                            row.env_keys.join(", ")
                        }
                    ));
                }
            }
        }
    }
    Ok(())
}

fn print_models(rows: &[ModelRow<'_>], json: bool) -> anyhow::Result<()> {
    if json {
        println!("{}", serde_json::to_string(rows)?);
        return Ok(());
    }
    for row in rows {
        line(&row.key);
        line(&format!("  {}", row.model.name));
        line("  CONTEXT     OUTPUT      INPUT $/M    OUTPUT $/M");
        line(&format!(
            "  {:<11} {:<11} {:<12} {}",
            row.model.limit.context,
            row.model
                .limit
                .output
                .map_or_else(|| "unknown".into(), |limit| limit.to_string()),
            row.model.cost.input,
            row.model.cost.output
        ));
        line(&format!(
            "  Efforts: {}",
            row.supported_efforts
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ));
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
