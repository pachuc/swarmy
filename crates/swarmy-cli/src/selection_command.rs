use clap::Args;
use swarmy_core::{InferenceSelection, ReasoningEffort};

#[derive(Args, Default)]
pub struct SelectionArgs {
    /// Provider for a new ephemeral session
    #[arg(long, conflicts_with = "agent")]
    pub provider: Option<String>,
    /// Model id or provider/model
    #[arg(long, conflicts_with = "agent")]
    pub model: Option<String>,
    /// Reasoning effort: none, minimal, low, medium, high, xhigh, max
    #[arg(long, conflicts_with = "agent")]
    pub effort: Option<ReasoningEffort>,
}

impl From<SelectionArgs> for InferenceSelection {
    fn from(args: SelectionArgs) -> Self {
        Self {
            provider: args.provider,
            model: args.model,
            effort: args.effort,
        }
    }
}

pub fn effort_or_default(value: &str) -> Result<String, String> {
    if value != "default" {
        value
            .parse::<ReasoningEffort>()
            .map_err(|e| e.to_string())?;
    }
    Ok(value.to_owned())
}
