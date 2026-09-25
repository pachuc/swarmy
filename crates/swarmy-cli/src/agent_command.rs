use std::path::PathBuf;

use clap::{Args, Subcommand};

#[derive(Args)]
pub struct InferenceArgs {
    /// Override the stack system prompt (preserved verbatim)
    #[arg(long, conflicts_with = "system_prompt_file")]
    pub system_prompt: Option<String>,
    /// Read the system prompt from a UTF-8 file
    #[arg(long)]
    pub system_prompt_file: Option<PathBuf>,
    /// Override the stack provider
    #[arg(long)]
    pub provider: Option<String>,
    /// Override the stack model
    #[arg(long)]
    pub model: Option<String>,
    /// Reasoning effort: none, minimal, low, medium, high, xhigh, or max
    #[arg(long, value_parser = crate::selection_command::effort_or_default)]
    pub effort: Option<String>,
    /// Sandbox memory limit in MiB
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    pub memory: Option<u64>,
    /// GPU requirement for placement
    #[arg(long, value_parser = ["none", "shared", "dedicated"])]
    pub gpu: Option<String>,
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
        /// Read the GitHub token from standard input, keeping it out of process arguments
        #[arg(long, conflicts_with = "github_token")]
        github_token_stdin: bool,
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
    /// Roll up durable metrics from an agent's main session
    Metrics { name: String },
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
            for effort in ["none", "minimal", "low", "medium", "high", "xhigh", "max"] {
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
        for command in ["create", "set"] {
            assert!(
                crate::Cli::try_parse_from([
                    "swarmy", "agent", command, "tommy", "--memory", "2048", "--gpu", "shared"
                ])
                .is_ok()
            );
            assert!(
                crate::Cli::try_parse_from(["swarmy", "agent", command, "tommy", "--memory", "0"])
                    .is_err()
            );
            assert!(
                crate::Cli::try_parse_from([
                    "swarmy", "agent", command, "tommy", "--gpu", "unknown"
                ])
                .is_err()
            );
        }
        assert!(
            crate::Cli::try_parse_from([
                "swarmy",
                "agent",
                "create",
                "tommy",
                "--github-token-stdin"
            ])
            .is_ok()
        );
        assert!(
            crate::Cli::try_parse_from([
                "swarmy",
                "agent",
                "create",
                "tommy",
                "--github-token-stdin",
                "--github-token",
                "secret"
            ])
            .is_err()
        );
    }
}
