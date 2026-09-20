use clap::{Args, Subcommand};
use std::path::PathBuf;

#[derive(Subcommand)]
pub enum Command {
    /// Sign in with the existing file-based `ChatGPT` device flow
    Login {
        #[arg(default_value = "chatgpt")]
        provider: String,
    },
    /// Store a provider API key in the cluster
    Set(Set),
    /// List credential metadata without printing secrets
    Ls,
    /// Remove a credential
    Rm { provider: String },
    /// Decrypt and inspect one or all credentials
    Check { provider: Option<String> },
    /// Import the configured `ChatGPT` credential file without deleting it
    Import {
        #[arg(long)]
        file: Option<PathBuf>,
    },
}

#[derive(Args)]
#[group(required = true, multiple = false)]
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
    pub provider: String,
    #[command(flatten)]
    pub source: Source,
    /// Provider setting in name=value form (repeatable)
    #[arg(long, value_parser = extra)]
    pub extra: Vec<(String, String)>,
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
