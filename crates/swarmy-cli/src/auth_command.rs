use anyhow::{Context, Result, ensure};
use clap::{Args, Subcommand};
use std::path::PathBuf;

#[derive(Subcommand)]
pub(crate) enum Command {
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
        #[arg(long, default_value = "day", value_enum)]
        group: crate::cost_command::UsageGroup,
        /// Range start: an absolute date or timestamp, a relative span like 7d, 3mo, or 1y,
        /// or a calendar word like month or 2months for the start of this or last month
        #[arg(long)]
        since: Option<String>,
        /// Range end: an absolute date or timestamp, a relative span, a calendar word, or now
        #[arg(long)]
        until: Option<String>,
    },
}

#[derive(Subcommand)]
pub(crate) enum RoutesCommand {
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
pub(crate) struct Source {
    #[arg(long)]
    pub api_key: Option<String>,
    #[arg(long)]
    pub from_env: bool,
    #[arg(long)]
    pub file: Option<PathBuf>,
}

#[derive(Args)]
pub(crate) struct Set {
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

/// Run one credential command. One match owns every variant: logins and
/// imports shell out to the `swarmy-auth` helper without connecting, while
/// every other arm connects on its own path.
pub(crate) async fn run(command: Command, auth_file: Option<PathBuf>, json: bool) -> Result<()> {
    match command {
        Command::Login {
            provider,
            label,
            resource,
            scope,
        } => {
            let mut argv: Vec<std::ffi::OsString> = vec!["login".into(), provider.into()];
            if let Some(label) = label {
                argv.push("--label".into());
                argv.push(label.into());
            }
            if let Some(resource) = resource {
                argv.push("--resource".into());
                argv.push(resource.into());
            }
            if let Some(scope) = scope {
                argv.push("--scope".into());
                argv.push(scope.into());
            }
            helper(auth_file, json, &argv)
        }
        Command::Import { file, label } => {
            let mut argv: Vec<std::ffi::OsString> = vec!["import".into()];
            if let Some(file) = file {
                argv.push("--file".into());
                argv.push(file.into());
            }
            if let Some(label) = label {
                argv.push("--label".into());
                argv.push(label.into());
            }
            helper(auth_file, json, &argv)
        }
        Command::Set(args) => {
            let (client, endpoint) = swarmy_client::api_client::connect()?;
            crate::api_commands::auth_set(&client, &endpoint, args, json).await
        }
        Command::Ls => list(json).await,
        Command::Check { provider, label } => check(provider, label, json).await,
        Command::Rm { provider, label } => remove(provider, label, json).await,
        Command::Routes { command } => {
            let (client, endpoint) = swarmy_client::api_client::connect()?;
            crate::api_commands::routes(&client, &endpoint, command, json).await
        }
        Command::Quota {
            entry,
            group,
            since,
            until,
        } => {
            let (client, endpoint) = swarmy_client::api_client::connect()?;
            crate::api_commands::quota(
                &client,
                &endpoint,
                entry.as_deref(),
                group,
                since.as_deref(),
                until.as_deref(),
                json,
            )
            .await
        }
    }
}

/// List credential metadata without printing secrets.
async fn list(json: bool) -> Result<()> {
    let (client, endpoint) = swarmy_client::api_client::connect()?;
    for summary in swarmy_client::api_client::call(&endpoint, client.credentials()).await? {
        crate::api_commands::auth_display(&summary, json, false);
    }
    Ok(())
}

/// Decrypt and inspect one or all credentials, failing when none is ready.
async fn check(provider: Option<String>, label: Option<String>, json: bool) -> Result<()> {
    let (client, endpoint) = swarmy_client::api_client::connect()?;
    let rows: Vec<_> = swarmy_client::api_client::call(&endpoint, client.credentials())
        .await?
        .into_iter()
        .filter(|row| {
            provider
                .as_ref()
                .is_none_or(|provider| row.provider == *provider)
                && label.as_ref().is_none_or(|label| row.label == *label)
        })
        .collect();
    ensure!(!rows.is_empty(), "credential does not exist");
    let ready = rows
        .iter()
        .all(|row| row.status == swarmy_api_types::CredentialStatus::Ready);
    let expired_bedrock = rows.iter().any(|row| {
        row.provider == "amazon-bedrock"
            && row.status == swarmy_api_types::CredentialStatus::Expired
    });
    for summary in rows {
        crate::api_commands::auth_display(&summary, json, true);
    }
    ensure!(
        ready,
        if expired_bedrock {
            swarmy_core::BEDROCK_CONSOLE_KEY_EXPIRED
        } else {
            "one or more credentials are expired or need login"
        }
    );
    Ok(())
}

/// Remove one labelled credential.
async fn remove(provider: String, label: String, json: bool) -> Result<()> {
    let (client, endpoint) = swarmy_client::api_client::connect()?;
    swarmy_client::api_client::call(
        &endpoint,
        client.remove_credential_entry(&provider, &label, &ulid::Ulid::generate().to_string()),
    )
    .await
    .with_context(|| format!("removing credential {provider}/{label}"))?;
    crate::api_commands::auth_report("removed", &provider, json);
    Ok(())
}

/// Shell out to the `swarmy-auth` helper so terminal OAuth flows stay out of
/// this binary. One function builds the helper command, appends the caller's
/// subcommand arguments, and checks its exit status.
fn helper(auth_file: Option<PathBuf>, json: bool, argv: &[std::ffi::OsString]) -> Result<()> {
    let sibling = std::env::current_exe()?.with_file_name("swarmy-auth");
    let helper = if sibling.is_file() {
        sibling.into_os_string()
    } else {
        "swarmy-auth".into()
    };
    let mut process = std::process::Command::new(helper);
    if json {
        process.arg("--json");
    }
    if let Some(file) = auth_file {
        process.arg("--auth-file").arg(file);
    }
    process.args(argv);
    let status = process.status().map_err(|error| {
        anyhow::anyhow!("swarmy-auth helper unavailable; run make install-client or cargo install --path crates/swarmy-devtools: {error}")
    })?;
    ensure!(status.success(), "swarmy-auth failed: {status}");
    Ok(())
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use clap::Parser;

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
