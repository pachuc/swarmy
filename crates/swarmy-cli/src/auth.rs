use crate::auth_command::Command;
use anyhow::{Result, ensure};
use std::path::PathBuf;
use swarmy_config::Settings;
use swarmy_core::CredentialRecord;
use swarmy_llm::auth::{CredentialStore as _, FileCredentialStore};

pub async fn run(command: Command, auth_file: Option<PathBuf>, json: bool) -> Result<()> {
    let settings = Settings::load()?.settings;
    let (client, endpoint) = crate::api_client::connect()?;
    match command {
        Command::Import { file } => {
            let path = file
                .or(auth_file)
                .unwrap_or_else(|| PathBuf::from(settings.credential_file));
            let credentials = FileCredentialStore::new(path).load().await?;
            let record = credentials.to_record()?;
            submit(&client, &endpoint, "chatgpt", &record).await?;
            report("imported", "chatgpt", json);
        }
        Command::Login {
            provider,
            resource,
            scope,
        } => {
            ensure!(
                provider == "azure" || (resource.is_none() && scope.is_none()),
                "--resource and --scope are Azure options"
            );
            let login =
                swarmy_llm::auth::login_for(&provider, resource.as_deref(), scope.as_deref())?;
            let kind = login.login(&TerminalUi { json }).await?;
            let record = CredentialRecord {
                kind,
                updated_at: jiff::Timestamp::now(),
            };
            submit(&client, &endpoint, login.provider(), &record).await?;
            report("saved", login.provider(), json);
        }
        _ => anyhow::bail!("use swarmy for auth set, ls, rm, and check"),
    }
    Ok(())
}
async fn submit(
    client: &swarmy_client::Client,
    endpoint: &str,
    provider: &str,
    record: &CredentialRecord,
) -> Result<()> {
    let body = serde_json::json!({"idempotency_key": ulid::Ulid::generate().to_string(),
        "provider":provider,"record":record});
    crate::api_client::call(
        endpoint,
        client.cli_set_credential(&serde_json::from_value(body)?),
    )
    .await?;
    Ok(())
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
