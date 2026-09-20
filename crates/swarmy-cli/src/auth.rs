use anyhow::{Context, Result, bail, ensure};
use jiff::Timestamp;
use std::{path::PathBuf, sync::Arc};
use swarmy_config::{Keyring, Settings};
use swarmy_core::{CredentialKind, CredentialRecord, CredentialScope, CredentialStatus};
use swarmy_llm::auth::{CredentialStore as _, FileCredentialStore};
use swarmy_store::{Store, blob::MemoryBlobStore, credentials::CredentialSummary};

use crate::auth_command::{Command, Set};

pub async fn run(command: Command, auth_file: Option<PathBuf>, json: bool) -> Result<()> {
    let settings = Settings::load()?.settings;
    let keyring =
        Keyring::load().context("load cluster keyring; run swarmy dev up to create one")?;
    let directory: Vec<_> = settings
        .store_directory
        .split('/')
        .map(str::to_owned)
        .collect();
    let store = Store::open(
        Some(&settings.fdb_cluster_file),
        Some(&directory),
        Arc::new(MemoryBlobStore::default()),
    )
    .await?
    .credentials(keyring);
    let scope = CredentialScope::Cluster;
    match command {
        Command::Set(args) => {
            let (provider, record) = api_key(args)?;
            store.put_credential(scope, &provider, &record).await?;
            report("saved", &provider, json);
        }
        Command::Ls => {
            for summary in store.list_credentials(scope).await? {
                display(&summary, json, false)?;
            }
        }
        Command::Rm { provider } => {
            store.delete_credential(scope, &provider).await?;
            report("removed", &provider, json);
        }
        Command::Check { provider } => {
            let summaries = if let Some(provider) = provider {
                let record = store
                    .get_credential(scope, &provider)
                    .await?
                    .with_context(|| format!("credential for {provider} does not exist"))?;
                vec![CredentialSummary::new(provider, &record, Timestamp::now())]
            } else {
                store.list_credentials(scope).await?
            };
            let ready = summaries
                .iter()
                .all(|s| s.status == CredentialStatus::Ready);
            for summary in summaries {
                display(&summary, json, true)?;
            }
            ensure!(ready, "one or more credentials are expired or need login");
        }
        Command::Import { file } => {
            let path = file
                .or(auth_file)
                .unwrap_or_else(|| PathBuf::from(settings.credential_file));
            let credentials = FileCredentialStore::new(path).load().await?;
            store
                .put_credential(scope, "chatgpt", &credentials.to_record()?)
                .await?;
            report("imported", "chatgpt", json);
        }
        Command::Login {
            provider,
            resource,
            scope: login_scope,
        } => {
            ensure!(
                provider == "azure" || (resource.is_none() && login_scope.is_none()),
                "--resource and --scope are Azure options"
            );
            let login = swarmy_llm::auth::login_for(
                &provider,
                resource.as_deref(),
                login_scope.as_deref(),
            )?;
            let kind = login.login(&TerminalUi { json }).await?;
            store
                .put_credential(
                    scope,
                    login.provider(),
                    &CredentialRecord {
                        kind,
                        updated_at: Timestamp::now(),
                    },
                )
                .await?;
            report("saved", login.provider(), json);
        }
    }
    Ok(())
}

fn api_key(args: Set) -> Result<(String, CredentialRecord)> {
    ensure!(
        !args.provider.is_empty()
            && args
                .provider
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
        "invalid provider id"
    );
    ensure!(
        args.provider != "chatgpt",
        "chatgpt requires OAuth; use auth login chatgpt"
    );
    let key = if let Some(key) = args.source.api_key {
        key
    } else if let Some(path) = args.source.file {
        std::fs::read_to_string(path)
            .context("read API key file")?
            .trim()
            .to_owned()
    } else {
        environment_key(&args.provider)?
    };
    ensure!(!key.trim().is_empty(), "API key must not be empty");
    Ok((
        args.provider,
        CredentialRecord {
            kind: CredentialKind::ApiKey {
                key,
                extra: args.extra.into_iter().collect(),
            },
            updated_at: Timestamp::now(),
        },
    ))
}

fn environment_key(provider: &str) -> Result<String> {
    let names: &[&str] = match provider {
        "anthropic" => &["ANTHROPIC_API_KEY"],
        "openai" => &["OPENAI_API_KEY"],
        "xai" => &["XAI_API_KEY"],
        "meta" => &["META_MODEL_API_KEY"],
        "openrouter" => &["OPENROUTER_API_KEY"],
        "azure" => &["AZURE_API_KEY", "AZURE_OPENAI_API_KEY"],
        "amazon-bedrock" => &["AWS_BEARER_TOKEN_BEDROCK"],
        "google" => &["GEMINI_API_KEY", "GOOGLE_API_KEY"],
        "google-vertex" | "google-vertex-anthropic" => &["GOOGLE_CLOUD_API_KEY"],
        _ => bail!("no API key environment mapping for {provider}; use --api-key or --file"),
    };
    names
        .iter()
        .find_map(|name| std::env::var(name).ok().filter(|s| !s.is_empty()))
        .with_context(|| format!("set {} before using --from-env", names.join(" or ")))
}

fn report(event: &str, provider: &str, json: bool) {
    if json {
        println!(
            "{}",
            serde_json::json!({"event": event, "provider": provider})
        );
    } else {
        println!("{provider}: {event}");
    }
}

fn display(summary: &CredentialSummary, json: bool, expiry: bool) -> Result<()> {
    let mut value = serde_json::to_value(summary)?;
    let seconds = summary
        .expires_at
        .map(|at| at.as_second() - Timestamp::now().as_second());
    if expiry {
        value["expires_in_seconds"] = serde_json::json!(seconds);
    }
    if json {
        println!("{value}");
    } else {
        let status = value["status"].as_str().unwrap_or_default();
        print!(
            "{}\t{}\t{status}\t{}",
            summary.provider, summary.kind, summary.updated_at
        );
        if expiry && let Some(seconds) = seconds {
            print!("\texpires in {seconds}s");
        }
        println!();
    }
    Ok(())
}

struct TerminalUi {
    json: bool,
}

#[async_trait::async_trait]
impl swarmy_llm::auth::LoginUi for TerminalUi {
    async fn notify_url(&self, url: &str) -> Result<(), swarmy_llm::Error> {
        if self.json {
            println!("{}", serde_json::json!({"event":"auth_url", "url":url}));
        } else {
            println!("Open {url}");
        }
        // Opening a browser is best effort; the printed URL also works over SSH.
        let opener = if cfg!(target_os = "macos") {
            "open"
        } else {
            "xdg-open"
        };
        let _ = tokio::process::Command::new(opener)
            .arg(url)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();
        Ok(())
    }
    async fn notify_device_code(&self, url: &str, code: &str) -> Result<(), swarmy_llm::Error> {
        use std::io::Write as _;
        if self.json {
            println!(
                "{}",
                serde_json::json!({"event":"device_code", "url":url, "code":code})
            );
        } else {
            println!("Open {url} and enter code {code}");
        }
        std::io::stdout().flush()?;
        Ok(())
    }
    async fn prompt_secret(&self, prompt: &str) -> Result<String, swarmy_llm::Error> {
        eprintln!("{prompt}:");
        tokio::task::spawn_blocking(read_secret).await?
    }
    async fn prompt_choice(
        &self,
        prompt: &str,
        choices: &[&str],
    ) -> Result<usize, swarmy_llm::Error> {
        eprintln!("{prompt}:");
        for (index, choice) in choices.iter().enumerate() {
            eprintln!("{}: {choice}", index + 1);
        }
        let count = choices.len();
        tokio::task::spawn_blocking(move || {
            let mut line = String::new();
            std::io::stdin().read_line(&mut line)?;
            line.trim()
                .parse::<usize>()
                .ok()
                .and_then(|n| n.checked_sub(1))
                .filter(|n| *n < count)
                .ok_or(swarmy_llm::Error::Credentials("invalid login choice"))
        })
        .await?
    }
}

fn read_secret() -> Result<String, swarmy_llm::Error> {
    use std::io::IsTerminal as _;
    struct RawMode;
    impl Drop for RawMode {
        fn drop(&mut self) {
            let _ = crossterm::terminal::disable_raw_mode();
        }
    }
    if !std::io::stdin().is_terminal() {
        let mut line = String::new();
        std::io::stdin().read_line(&mut line)?;
        return Ok(line.trim().into());
    }
    crossterm::terminal::enable_raw_mode()?;
    let _raw = RawMode;
    let mut secret = String::new();
    loop {
        use crossterm::event::{Event, KeyCode, KeyModifiers};
        if let Event::Key(key) = crossterm::event::read()? {
            match key.code {
                KeyCode::Enter => return Ok(secret),
                KeyCode::Backspace => {
                    secret.pop();
                }
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    return Err(swarmy_llm::Error::Credentials("login cancelled"));
                }
                KeyCode::Char(c) => secret.push(c),
                _ => (),
            }
        }
    }
}
