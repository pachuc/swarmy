mod agent_command;
mod api_commands;
mod auth_command;
mod bench_command;
mod client_bench;
use swarmy_chat::client_conversation;
mod client_commands;
mod cloud_command;
mod core_command;
mod cost_command;
mod dev;
mod doctor;
mod gc;
mod image;
mod image_command;
mod models;
mod models_probe;
mod models_probe_command;
mod provider_report;
mod selection_command;
mod session_command;
mod swarm_command;
mod target;
mod tools;
mod version_guard;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "swarmy",
    version,
    about = "Operate a Swarmy cluster: agents, sessions, volumes, images"
)]
struct Cli {
    /// Emit compact machine-readable JSON
    #[arg(long, global = true)]
    json: bool,
    /// Talk to this swarm (overrides `SWARMY_SWARM`, `SWARMY_API_URL`, and the current swarm)
    #[arg(long, global = true, value_name = "NAME", value_parser = swarm_command::parse_name)]
    swarm: Option<String>,
    /// Talk to, or start, a swarmy-core whose version differs from this client
    #[arg(long, global = true)]
    allow_version_mismatch: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// List, show, select, attach, and forget swarms
    Swarm {
        #[command(subcommand)]
        command: swarm_command::Command,
    },
    /// Browse the configured provider and model catalog
    Models {
        #[command(subcommand)]
        command: models::Command,
    },
    /// Create and manage named agents
    Agent {
        #[command(subcommand)]
        command: agent_command::Command,
    },
    /// Measure conversation latency
    Bench {
        #[command(subcommand)]
        command: bench_command::Command,
    },
    /// Launch, connect to, and inspect remote development stacks
    Remote {
        #[command(subcommand)]
        command: swarmy_cloud::Command,
    },
    /// Collect unreferenced chunks older than the configured grace window
    Gc {
        #[arg(long)]
        dry_run: bool,
        /// Override the service's grace window for this run only, in seconds
        #[arg(long)]
        grace_seconds: Option<u64>,
    },
    /// Build and inspect base filesystem images
    Image {
        #[command(subcommand)]
        command: image_command::Command,
    },
    /// Fetch, verify, and locate the swarmy-core build matching this client
    Core {
        #[command(subcommand)]
        command: core_command::Command,
    },
    /// Start and operate the local development system
    Dev {
        #[command(subcommand)]
        command: dev::Command,
    },
    /// Check installation, configuration, credentials, and local connectivity
    Doctor,
    /// Print the version of this CLI
    Version {
        /// Print the release tag (`v<version>`) whose archives match this client
        #[arg(long)]
        release: bool,
    },
    /// Start a conversation and stream its output until idle
    Run {
        #[command(flatten)]
        args: client_commands::RunArgs,
    },
    /// Open a terminal conversation, or resume a session
    Chat {
        #[command(flatten)]
        args: client_commands::ChatArgs,
    },
    /// Inspect stored sessions
    Session {
        #[command(subcommand)]
        command: session_command::Command,
    },
    /// Show billed token and cost totals from the metering rollups
    Cost {
        #[command(flatten)]
        args: cost_command::Args,
    },
    /// Manage cloud API keys stored on this machine for creating swarm machines
    Cloud {
        #[command(subcommand)]
        command: cloud_command::Command,
    },
    /// Manage encrypted provider credentials
    Auth {
        /// Swarmy's credential file (never defaults to Codex's auth.json)
        #[arg(long, env = "SWARMY_CHATGPT_AUTH", global = true)]
        auth_file: Option<PathBuf>,
        #[command(subcommand)]
        command: auth_command::Command,
    },
}

fn json_core_ensure(cli: &Cli) -> bool {
    cli.json && matches!(cli.command, Command::Core { .. })
}

fn main() -> anyhow::Result<()> {
    let cli = swarmy_version::parse::<Cli>("swarmy")?;
    if json_core_ensure(&cli) {
        swarmy_cloud::set_output_sink(|message, stderr| {
            if stderr {
                eprintln!("{message}");
            }
        });
    } else {
        swarmy_cloud::set_output_sink(|message, stderr| {
            if stderr {
                eprintln!("{message}");
            } else {
                println!("{message}");
            }
        });
    }
    target::select(cli.swarm.clone())?;
    // Background service logs must not overwrite the full-screen transcript.
    let writer = if matches!(cli.command, Command::Chat { .. }) {
        tracing_subscriber::fmt::writer::BoxMakeWriter::new(std::io::sink)
    } else {
        tracing_subscriber::fmt::writer::BoxMakeWriter::new(std::io::stderr)
    };
    tracing_subscriber::fmt()
        .with_writer(writer)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();
    // One match owns every command and calls into its module. Each arm
    // connects on its own path, except the local `dev` supervisor and the
    // `swarmy-auth` helpers and cloud credentials which never touch the API.
    let runtime = tokio::runtime::Runtime::new()?;
    let json = cli.json;
    match cli.command {
        Command::Swarm { command } => runtime.block_on(swarm_command::run(command, json)),
        Command::Dev { command } => {
            runtime.block_on(dev::run(command, json, cli.allow_version_mismatch))
        }
        Command::Core { command } => {
            runtime.block_on(core_command::run(command, json, cli.allow_version_mismatch))
        }
        Command::Auth { command, auth_file } => {
            runtime.block_on(auth_command::run(command, auth_file, json))
        }
        Command::Cloud { command } => cloud_command::run(command, json),
        Command::Session { command } => {
            runtime.block_on(api_commands::session_command(command, json))
        }
        Command::Agent { command } => runtime.block_on(api_commands::agent_command(command, json)),
        Command::Cost { args } => runtime.block_on(api_commands::cost_command(args, json)),
        Command::Image { command } => runtime.block_on(image::run(command, json)),
        Command::Models { command } => runtime.block_on(models::run(command, json)),
        Command::Bench { command } => {
            runtime.block_on(run_bench(command, json, cli.allow_version_mismatch))
        }
        Command::Run { args } => {
            runtime.block_on(run_prompt(args, json, cli.allow_version_mismatch))
        }
        Command::Chat { args } => {
            runtime.block_on(run_chat(args, json, cli.allow_version_mismatch))
        }
        #[cfg(feature = "remote")]
        Command::Remote { command } => runtime.block_on(remote(command, json)),
        #[cfg(not(feature = "remote"))]
        Command::Remote { .. } => {
            anyhow::bail!("swarmy was built without remote support");
        }
        Command::Gc {
            dry_run,
            grace_seconds,
        } => runtime.block_on(gc::run(dry_run, grace_seconds, json)),
        Command::Doctor => runtime.block_on(run_doctor(json)),
        Command::Version { release: true } => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({"tag": swarmy_version::RELEASE_TAG, "version": swarmy_version::VERSION, "release_build": swarmy_version::RELEASE_BUILD})
                );
            } else {
                println!("{}", swarmy_version::RELEASE_TAG);
            }
            Ok(())
        }
        Command::Version { release: false } => {
            swarmy_version::print("swarmy", json).map_err(anyhow::Error::from)
        }
    }
}

/// Connect to the API and wait for a healthy response. Every database-backed
/// command runs through the control plane, so the client links no database,
/// message bus, or object store library.
async fn connect_client(
    allow_version_mismatch: bool,
) -> anyhow::Result<(swarmy_client::Client, String)> {
    let (client, endpoint) = target::connect().await?;
    let health = swarmy_client::api_client::call(&endpoint, client.health()).await?;
    version_guard::enforce(&health, allow_version_mismatch)?;
    Ok((client, endpoint))
}

async fn run_bench(
    command: bench_command::Command,
    json: bool,
    allow_version_mismatch: bool,
) -> anyhow::Result<()> {
    let (client, endpoint) = connect_client(allow_version_mismatch).await?;
    client_bench::run(client, endpoint, command, json).await
}

async fn run_prompt(
    args: client_commands::RunArgs,
    json: bool,
    allow_version_mismatch: bool,
) -> anyhow::Result<()> {
    let (client, endpoint) = connect_client(allow_version_mismatch).await?;
    wait_for_services(&client, &endpoint, args.selection.provider.as_deref()).await?;
    client_commands::run(client, endpoint, args, json).await
}

async fn run_chat(
    args: client_commands::ChatArgs,
    json: bool,
    allow_version_mismatch: bool,
) -> anyhow::Result<()> {
    let (client, endpoint) = connect_client(allow_version_mismatch).await?;
    wait_for_services(&client, &endpoint, args.selection.provider.as_deref()).await?;
    if json {
        client_commands::chat(client, endpoint, args, json).await
    } else {
        #[cfg(feature = "chat")]
        {
            let route = args.selection.route.clone();
            let open = client_conversation::OpenArgs {
                id: args.session_id.map(|id| id.to_string()),
                image: args.image,
                agent: args.agent,
                new: args.new,
                selection: args.selection.into(),
                route,
            };
            swarmy_chat::client_chat::run(client, endpoint, open, &mut |message| {
                eprintln!("{message}");
            })
            .await?;
            Ok(())
        }
        #[cfg(not(feature = "chat"))]
        {
            anyhow::bail!(
                "interactive chat is unavailable in this headless build; use --json or install with --features chat"
            );
        }
    }
}

async fn wait_for_services(
    client: &swarmy_client::Client,
    endpoint: &str,
    provider: Option<&str>,
) -> anyhow::Result<()> {
    client_conversation::wait_healthy(client, endpoint, provider, &mut |message| {
        eprintln!("{message}");
    })
    .await?;
    Ok(())
}

async fn run_doctor(json: bool) -> anyhow::Result<()> {
    if !doctor::run(json).await? {
        std::process::exit(1);
    }
    Ok(())
}

/// Run a `swarmy remote` subcommand. The library reports what `remote down`
/// would delete and what `remote tag` would adopt; only the CLI prints the
/// confirmation wording and prompts.
#[cfg(feature = "remote")]
async fn remote(command: swarmy_cloud::Command, json: bool) -> anyhow::Result<()> {
    let name = remote_name(&command);
    let advises = matches!(
        &command,
        swarmy_cloud::Command::Down { .. } | swarmy_cloud::Command::Tag { .. }
    );
    let retry = command.clone();
    let outcome = run_once(command, json, false, name, advises).await?;
    match outcome {
        swarmy_cloud::RunOutcome::Completed => Ok(()),
        swarmy_cloud::RunOutcome::NeedsConfirmation { plan } => {
            confirm_deletion(&plan, json)?;
            run_once(retry, json, true, name, advises).await?;
            Ok(())
        }
        swarmy_cloud::RunOutcome::NeedsTagConfirmation { node, targets } => {
            confirm_tag(&targets, &node)?;
            run_once(retry, json, true, name, advises).await?;
            Ok(())
        }
    }
}

/// One `swarmy remote` attempt: the library error plus the command context.
/// Permission advice prints only for `down` and `tag`, on every attempt.
#[cfg(feature = "remote")]
async fn run_once(
    command: swarmy_cloud::Command,
    json: bool,
    confirmed: bool,
    name: &'static str,
    advises: bool,
) -> anyhow::Result<swarmy_cloud::RunOutcome> {
    use anyhow::Context as _;
    Box::pin(swarmy_cloud::run(command, json, confirmed))
        .await
        .map_err(|error| {
            if advises && matches!(error, swarmy_cloud::Error::MissingPermission { .. }) {
                eprintln!(
                    "AWS denied the named permission; nothing was deleted by this operation. Grant it and retry; local remote state is retained"
                );
            }
            error
        })
        .with_context(|| format!("swarmy remote {name} failed"))
}

/// The subcommand name for edge error context.
#[cfg(feature = "remote")]
fn remote_name(command: &swarmy_cloud::Command) -> &'static str {
    match command {
        swarmy_cloud::Command::Up { .. } => "up",
        swarmy_cloud::Command::Adopt { .. } => "adopt",
        swarmy_cloud::Command::AddNode { .. } => "add-node",
        swarmy_cloud::Command::Upgrade { .. } => "upgrade",
        swarmy_cloud::Command::Down { .. } => "down",
        swarmy_cloud::Command::Tag { .. } => "tag",
        swarmy_cloud::Command::Connect { .. } => "connect",
        swarmy_cloud::Command::Disconnect { .. } => "disconnect",
        swarmy_cloud::Command::Logs { .. } => "logs",
        swarmy_cloud::Command::Ls => "ls",
    }
}

/// Print what `remote down` would delete and read one confirmation.
/// The caller reruns the original command with `confirmed` set.
#[cfg(feature = "remote")]
fn confirm_deletion(plan: &swarmy_cloud::DeletionPlan, json: bool) -> anyhow::Result<()> {
    use std::io::IsTerminal;
    if json {
        anyhow::bail!("remote down --json requires --yes to delete owned resources");
    }
    if !std::io::stdin().is_terminal() {
        anyhow::bail!("remote down requires --yes without a terminal");
    }
    println!("Permanently delete these owned resources and their data:");
    if let Some(bucket) = &plan.bucket {
        println!("  bucket {bucket} (all objects and versions)");
    }
    if let Some(profile) = &plan.profile {
        println!("  instance profile {profile}");
    }
    if let Some(role) = &plan.role {
        println!("  role {role}");
    }
    let answer = read_confirmation("Continue? [y/N] ")?;
    if answer.trim() != "y" && answer.trim() != "yes" {
        anyhow::bail!("remote down cancelled");
    }
    Ok(())
}

/// Adoption is deliberately interactive and requires typing every exact
/// resource name. Shares the confirmation reader with `remote down`.
#[cfg(feature = "remote")]
fn confirm_tag(targets: &[(String, String)], node: &str) -> anyhow::Result<()> {
    use std::io::IsTerminal;
    if !std::io::stdin().is_terminal() {
        anyhow::bail!("remote tag requires a terminal");
    }
    for (kind, name) in targets {
        let answer = read_confirmation(&format!(
            "Type the exact {kind} name {name} to adopt it for remote {node}: "
        ))?;
        if answer.trim() != name {
            anyhow::bail!("remote tag cancelled");
        }
    }
    Ok(())
}

/// One confirmation reader for every remote prompt: print the wording,
/// flush, and return the operator's answer.
#[cfg(feature = "remote")]
fn read_confirmation(message: &str) -> anyhow::Result<String> {
    use std::io::Write;
    print!("{message}");
    std::io::stdout().flush()?;
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer)?;
    Ok(answer)
}

/// Documented `swarmy ...` commands resolve against the real clap tree.
#[cfg(test)]
mod docs_command_tests {
    #![deny(clippy::disallowed_methods)]
    #[test]
    fn markdown_swarmy_commands_match_the_clap_tree() {
        use clap::CommandFactory;
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../");
        let command = crate::Cli::command();
        swarmy_testkit::check_docs_commands(&root, &[("swarmy", &command)])
            .expect("documented swarmy commands match the CLI tree");
    }

    #[test]
    fn a_binary_the_docs_never_invoke_fails_the_check() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../");
        let command = clap::Command::new("swarmy-undocumented");
        let error =
            swarmy_testkit::check_docs_commands(&root, &[("swarmy-undocumented", &command)])
                .expect_err("a walk that extracts no commands must fail");
        assert!(
            error.contains("no documented commands extracted for swarmy-undocumented"),
            "{error}"
        );
    }
}
