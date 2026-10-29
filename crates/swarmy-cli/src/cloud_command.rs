use anyhow::bail;
use clap::Subcommand;
use std::io::{self, IsTerminal, Read, Write};
use swarmy_config::cloud::{CloudCredential, CloudCredentials, CloudProvider};

#[derive(Subcommand)]
pub(crate) enum Command {
    /// Store cloud API keys for creating machines (hetzner-cloud, hetzner-s3, aws, scaleway); secrets are read from stdin
    Login {
        #[arg(value_parser = parse_provider)]
        provider: CloudProvider,
        /// aws only: default region
        #[arg(long)]
        region: Option<String>,
    },
    /// Delete one provider's stored cloud keys
    Logout {
        #[arg(value_parser = parse_provider)]
        provider: CloudProvider,
    },
    /// List stored cloud providers and which fields are set, never their values
    Ls,
}

fn parse_provider(value: &str) -> Result<CloudProvider, swarmy_config::Error> {
    value.parse()
}

pub(crate) fn run(command: Command, json: bool) -> anyhow::Result<()> {
    let path = CloudCredentials::path(&swarmy_config::paths::Dirs::from_process()?);
    match command {
        Command::Login { provider, region } => {
            if region.is_some() && provider.option_field() != Some("region") {
                bail!("--region applies only to aws");
            }
            let mut store = CloudCredentials::load(&path)?;
            let credential = CloudCredential::new(provider, read_secrets(provider)?, region)?;
            store.set(credential);
            store.save(&path)?;
            if json {
                println!(
                    "{}",
                    serde_json::json!({"provider":provider.name(),"saved":true,"path":path})
                );
            } else {
                println!("saved {provider} credentials in {}", path.display());
            }
        }
        Command::Logout { provider } => {
            let mut store = CloudCredentials::load(&path)?;
            let removed = store.remove(provider);
            if removed {
                store.save(&path)?;
            }
            if json {
                println!(
                    "{}",
                    serde_json::json!({"provider":provider.name(),"removed":removed})
                );
            } else if removed {
                println!("removed {provider} credentials");
            } else {
                println!("no {provider} credentials were stored");
            }
        }
        Command::Ls => {
            let store = CloudCredentials::load(&path)?;
            if json {
                let entries: Vec<_> = CloudProvider::ALL
                    .iter()
                    .map(|&provider| {
                        let entry = store.get(provider);
                        let set = entry.is_some();
                        let fields = entry.map_or_else(Vec::new, |c| c.set_fields());
                        serde_json::json!({"provider":provider.name(),"set":set,"fields":fields})
                    })
                    .collect();
                println!("{}", serde_json::json!(entries));
            } else {
                for provider in CloudProvider::ALL {
                    let fields = store
                        .get(provider)
                        .map_or_else(|| "not set".into(), |c| c.set_fields().join(", "));
                    println!("{:<14}{}", provider.name(), fields);
                }
            }
        }
    }
    Ok(())
}

fn read_secrets(provider: CloudProvider) -> anyhow::Result<Vec<String>> {
    if io::stdin().is_terminal() {
        provider
            .secret_fields()
            .iter()
            .map(|field| read_hidden(provider, field))
            .collect()
    } else {
        let mut text = String::new();
        io::stdin().read_to_string(&mut text)?;
        split_stdin(&text, provider)
    }
}

fn split_stdin(text: &str, provider: CloudProvider) -> anyhow::Result<Vec<String>> {
    let mut lines: Vec<String> = text.lines().map(|line| line.trim().to_owned()).collect();
    while lines.last().is_some_and(String::is_empty) {
        lines.pop();
    }
    let expected = provider.secret_fields();
    if lines.len() != expected.len() {
        bail!(
            "expected {} lines on stdin ({}), got {}",
            expected.len(),
            expected.join(", "),
            lines.len()
        );
    }
    Ok(lines)
}

struct EchoGuard {
    saved: rustix::termios::Termios,
}

impl Drop for EchoGuard {
    fn drop(&mut self) {
        swarmy_core::ignore_best_effort(
            rustix::termios::tcsetattr(
                io::stdin(),
                rustix::termios::OptionalActions::Now,
                &self.saved,
            ),
            "restore terminal echo",
        );
    }
}

fn read_hidden(provider: CloudProvider, field: &str) -> anyhow::Result<String> {
    use rustix::termios::{LocalModes, OptionalActions, tcgetattr, tcsetattr};
    let fd = io::stdin();
    let saved = tcgetattr(&fd)?;
    let mut quiet = saved.clone();
    quiet.local_modes.remove(LocalModes::ECHO);
    tcsetattr(&fd, OptionalActions::Now, &quiet)?;
    let guard = EchoGuard { saved };
    eprint!("{provider} {field}: ");
    io::stderr().flush()?;
    let mut line = String::new();
    let result = fd.read_line(&mut line);
    eprintln!();
    drop(guard);
    if result? == 0 {
        bail!("no input for {provider} {field}");
    }
    Ok(line.trim_end_matches(['\r', '\n']).to_owned())
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    #[test]
    fn stdin_lines() {
        for text in ["t\n", "t", "t\r\n", "t\n\n"] {
            assert_eq!(
                split_stdin(text, CloudProvider::HetznerCloud).unwrap(),
                ["t"]
            );
        }
        assert_eq!(
            split_stdin("a\nb\n", CloudProvider::HetznerS3).unwrap(),
            ["a", "b"]
        );
        for (text, count) in [("", 0), ("a\n", 1), ("a\nb\nc\n", 3)] {
            assert_eq!(
                split_stdin(text, CloudProvider::HetznerS3)
                    .unwrap_err()
                    .to_string(),
                format!("expected 2 lines on stdin (access_key, secret_key), got {count}")
            );
        }
    }
    #[test]
    fn clap_rejects_secrets_and_unknown_providers() {
        use clap::Parser;
        #[derive(Parser)]
        struct Args {
            #[command(subcommand)]
            command: Command,
        }
        assert!(
            Args::try_parse_from(["swarmy", "login", "hetzner-cloud", "--token", "x"]).is_err()
        );
        assert!(Args::try_parse_from(["swarmy", "login", "digitalocean"]).is_err());
        assert!(
            Args::try_parse_from(["swarmy", "login", "aws", "--region", "eu-central-1"]).is_ok()
        );
    }
}
