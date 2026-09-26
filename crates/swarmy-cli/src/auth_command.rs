use clap::{Args, Subcommand};
use std::path::PathBuf;

#[derive(Subcommand)]
pub enum Command {
    /// Sign in with `ChatGPT`, `OpenRouter`, or Azure
    Login {
        #[arg(default_value = "chatgpt")]
        provider: String,
        /// Entry label. Without one the provider's default entry is replaced;
        /// pass `--label` to keep a second entry.
        #[arg(long)]
        label: Option<String>,
        #[arg(long)]
        resource: Option<String>,
        #[arg(long)]
        scope: Option<String>,
    },
    /// Store a provider API key in the cluster
    Set(Set),
    /// List credential metadata without printing secrets
    Ls,
    /// Remove a credential
    Rm { provider: String, label: String },
    /// Decrypt and inspect one or all credentials
    Check {
        provider: Option<String>,
        #[arg(long)]
        label: Option<String>,
    },
    /// Import the configured `ChatGPT` credential file without deleting it
    Import {
        #[arg(long)]
        file: Option<PathBuf>,
        /// Entry label. Without one the provider's default entry is replaced;
        /// pass `--label` to keep a second entry.
        #[arg(long)]
        label: Option<String>,
    },
    /// Manage named inference failover routes
    Routes {
        #[command(subcommand)]
        command: RoutesCommand,
    },
    /// Show quota per auth entry, or one entry's usage over time
    Quota {
        /// One entry in PROVIDER/LABEL form; without it every entry is listed
        #[arg(long)]
        entry: Option<String>,
        /// Calendar grouping for the entry's usage rows
        #[arg(long, default_value = "day", value_parser = ["day", "week", "month", "year"])]
        group: String,
        /// Range start: an absolute date or timestamp, or a relative span like 7d, 3mo, or 1y
        #[arg(long)]
        since: Option<String>,
        /// Range end: an absolute date or timestamp, a relative span, or now
        #[arg(long)]
        until: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum RoutesCommand {
    /// List named routes with their steps in order
    Ls,
    /// Show one route's steps in order
    Show { name: String },
    /// Replace a route's steps in `PROVIDER/LABEL[=MODEL]` order
    Set {
        name: String,
        #[arg(num_args(1..), required = true)]
        steps: Vec<String>,
    },
    /// Remove a named route; assigned sessions fall back
    Rm { name: String },
}

#[derive(Args)]
#[group(required = false, multiple = false)]
pub struct Source {
    #[arg(long)]
    pub api_key: Option<String>,
    #[arg(long)]
    pub from_env: bool,
    #[arg(long)]
    pub file: Option<PathBuf>,
}

#[derive(Args)]
pub struct Set {
    /// Provider id (also accepted as --provider).
    pub provider: Option<String>,
    #[arg(long = "provider", conflicts_with = "provider")]
    pub provider_flag: Option<String>,
    /// Entry label. Without one the provider's default entry is replaced;
    /// pass `--label` to keep a second entry.
    #[arg(long)]
    pub label: Option<String>,
    #[command(flatten)]
    pub source: Source,
    /// Provider setting in name=value form (repeatable)
    #[arg(long, value_parser = extra)]
    pub extra: Vec<(String, String)>,
    /// Configured quota limit for entries without published quotas.
    #[arg(long)]
    pub limit: Option<u64>,
    /// Configured quota window like `5h`, `30m`, or `7d`.
    #[arg(long)]
    pub window: Option<String>,
}

fn extra(value: &str) -> Result<(String, String), String> {
    let (name, value) = value
        .split_once('=')
        .filter(|(name, _)| !name.is_empty())
        .ok_or("expected name=value")?;
    if name == "needs_login" {
        return Err("needs_login is reserved".into());
    }
    Ok((name.into(), value.into()))
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    #[test]
    fn routes_subcommands_parse_without_connecting_to_the_stack() {
        assert!(crate::Cli::try_parse_from(["swarmy", "auth", "routes", "ls"]).is_ok());
        assert!(
            crate::Cli::try_parse_from(["swarmy", "auth", "routes", "show", "fallback"]).is_ok()
        );
        assert!(
            crate::Cli::try_parse_from([
                "swarmy",
                "auth",
                "routes",
                "set",
                "fallback",
                "chatgpt/default",
                "openai/work-key",
                "azure/prod=gpt-5.5",
            ])
            .is_ok()
        );
        assert!(crate::Cli::try_parse_from(["swarmy", "auth", "routes", "rm", "fallback"]).is_ok());
        assert!(
            crate::Cli::try_parse_from(["swarmy", "auth", "routes", "set", "fallback"]).is_err()
        );
    }

    #[test]
    fn quota_flags_parse_without_connecting_to_the_stack() {
        assert!(crate::Cli::try_parse_from(["swarmy", "auth", "quota"]).is_ok());
        assert!(
            crate::Cli::try_parse_from([
                "swarmy",
                "auth",
                "quota",
                "--entry",
                "openai/main",
                "--group",
                "month",
                "--since",
                "1y",
            ])
            .is_ok()
        );
    }
}
