use std::path::PathBuf;

use clap::{Args, Subcommand};
use swarmy_core::ReasoningEffort;

#[derive(Args)]
pub struct InferenceArgs {
    /// Override the stack system prompt (preserved verbatim)
    #[arg(long, conflicts_with = "system_prompt_file")]
    pub system_prompt: Option<String>,
    /// Read the system prompt from a UTF-8 file
    #[arg(long)]
    pub system_prompt_file: Option<PathBuf>,
    /// Override the stack model
    #[arg(long)]
    pub model: Option<String>,
    /// Reasoning effort: none, minimal, low, medium, high, or xhigh
    #[arg(long)]
    pub effort: Option<ReasoningEffort>,
}

#[derive(Subcommand)]
pub enum Command {
    /// Create a named agent and pin its computer's image
    Create {
        #[arg(value_parser = agent_name)]
        name: String,
        #[arg(long)]
        image: Option<String>,
        #[arg(long, default_value = "")]
        description: String,
        #[command(flatten)]
        inference: InferenceArgs,
        /// GitHub token, stored only in `FoundationDB`
        #[arg(long)]
        github_token: Option<String>,
    },
    /// Change inference settings or rotate the GitHub token for an agent
    Set {
        name: String,
        #[command(flatten)]
        inference: InferenceArgs,
        /// GitHub token, stored only in `FoundationDB`
        #[arg(long, conflicts_with = "clear_github_token")]
        github_token: Option<String>,
        /// Remove the stored GitHub token
        #[arg(long)]
        clear_github_token: bool,
    },
    /// List named agents
    #[command(alias = "list")]
    Ls,
    /// Inspect an agent and its sessions
    Show { name: String },
    /// Delete an agent and its computer, retaining transcripts
    Delete {
        name: String,
        #[arg(long)]
        yes: bool,
    },
}

fn agent_name(name: &str) -> Result<String, String> {
    swarmy_config::validate_remote_name(name).map_err(|_| {
        "agent names must contain 1-64 letters, digits, hyphens, or underscores".to_owned()
    })?;
    Ok(name.to_owned())
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    #[test]
    fn agent_inference_flags_validate_without_connecting_to_the_stack() {
        for command in ["create", "set"] {
            for effort in ["none", "minimal", "low", "medium", "high", "xhigh"] {
                assert!(
                    crate::Cli::try_parse_from([
                        "swarmy", "agent", command, "tommy", "--effort", effort
                    ])
                    .is_ok()
                );
            }
            for effort in ["", "unknown", "HIGH", "high "] {
                let error = crate::Cli::try_parse_from([
                    "swarmy", "agent", command, "tommy", "--effort", effort,
                ])
                .err()
                .unwrap();
                assert!(
                    error
                        .to_string()
                        .contains("reasoning effort must be one of")
                );
            }
            assert!(
                crate::Cli::try_parse_from([
                    "swarmy",
                    "agent",
                    command,
                    "tommy",
                    "--system-prompt",
                    "inline",
                    "--system-prompt-file",
                    "prompt.txt"
                ])
                .is_err()
            );
        }
    }
}
