mod agent_command;
use swarmy_client::api_client;
mod api_commands;
mod auth_command;
mod bench_command;
mod client_bench;
use swarmy_chat::client_conversation;
mod client_commands;
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
mod tools;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "swarmy",
    version,
    about = "Operate a Swarmy cluster: agents, sessions, volumes, images, channels"
)]
struct Cli {
    /// Emit compact machine-readable JSON
    #[arg(long, global = true)]
    json: bool,
    /// Use a saved remote tunnel profile
    #[arg(long, global = true)]
    remote: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
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
    /// Start and operate the local development system
    Dev {
        #[command(subcommand)]
        command: dev::Command,
    },
    /// Check installation, configuration, credentials, and local connectivity
    Doctor,
    /// Print the version of this CLI
    Version,
    /// Start a conversation and stream its output until idle
    Run {
        prompt: String,
        #[arg(long)]
        image: Option<String>,
        /// Resume the main session on a named agent (name or agent id)
        #[arg(long, conflicts_with = "image")]
        agent: Option<String>,
        /// Create a side conversation on the named agent
        #[arg(long, requires = "agent")]
        new: bool,
        /// Continue an existing session by id instead of an agent's main one
        #[arg(long, conflicts_with_all = ["agent", "image", "new"])]
        session: Option<ulid::Ulid>,
        /// Deliver after the current tool call without interrupting the turn.
        #[arg(long, requires = "session")]
        queue: bool,
        #[command(flatten)]
        selection: selection_command::SelectionArgs,
    },
    /// Open a terminal conversation, or resume a session
    Chat {
        #[arg(conflicts_with_all = ["provider", "model", "effort"])]
        session_id: Option<ulid::Ulid>,
        /// Base image in NAME:TAG form; otherwise use `default_image`.
        #[arg(long, conflicts_with = "session_id")]
        image: Option<String>,
        /// Resume the main session on a named agent (name or agent id)
        #[arg(long, conflicts_with_all = ["image", "session_id"])]
        agent: Option<String>,
        /// Create a side conversation on the named agent
        #[arg(long, requires = "agent")]
        new: bool,
        #[command(flatten)]
        selection: selection_command::SelectionArgs,
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
    /// Manage encrypted provider credentials
    Auth {
        /// Swarmy's credential file (never defaults to Codex's auth.json)
        #[arg(long, env = "SWARMY_CHATGPT_AUTH", global = true)]
        auth_file: Option<PathBuf>,
        #[command(subcommand)]
        command: auth_command::Command,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = swarmy_version::parse::<Cli>("swarmy")?;
    #[cfg(not(feature = "remote"))]
    if matches!(cli.command, Command::Remote { .. }) {
        anyhow::bail!("swarmy was built without remote support");
    }
    swarmy_cloud::set_output_sink(|message, stderr| {
        if stderr {
            eprintln!("{message}");
        } else {
            println!("{message}");
        }
    });
    #[cfg(feature = "remote")]
    swarmy_cloud::set_prompt_sink(|message| {
        use std::io::Write;
        print!("{message}");
        std::io::stdout().flush()?;
        let mut answer = String::new();
        std::io::stdin().read_line(&mut answer)?;
        Ok(answer)
    });
    swarmy_cloud::select(cli.remote.as_deref())?;
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
    if let Command::Dev { command } = cli.command {
        return tokio::runtime::Runtime::new()?.block_on(dev::run(command));
    }
    if matches!(
        &cli.command,
        Command::Auth {
            command: auth_command::Command::Login { .. } | auth_command::Command::Import { .. },
            ..
        }
    ) {
        let Command::Auth { command, auth_file } = cli.command else {
            unreachable!()
        };
        return run_auth_tool(command, auth_file, cli.json);
    }
    // Every database-backed command runs through the control-plane API, so
    // the client links no database, message bus, or object store library.
    if matches!(
        &cli.command,
        Command::Session { .. }
            | Command::Agent { .. }
            | Command::Cost { .. }
            | Command::Image {
                command: image_command::Command::Ls | image_command::Command::Show { .. }
            }
            | Command::Auth { .. }
    ) {
        return tokio::runtime::Runtime::new()?.block_on(api_commands::run(cli.command, cli.json));
    }
    tokio::runtime::Runtime::new()?.block_on(run(cli))
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    match cli.command {
        Command::Models { command } => models::run(command, cli.json).await?,
        Command::Bench { command } => {
            let (client, endpoint) = api_client::connect()?;
            api_client::call(&endpoint, client.health()).await?;
            client_bench::run(client, command, cli.json).await?;
        }
        Command::Run {
            prompt,
            image,
            agent,
            new,
            session,
            queue,
            selection,
        } => {
            let (client, endpoint) = api_client::connect()?;
            api_client::call(&endpoint, client.health()).await?;
            client_commands::run(
                client, prompt, image, agent, new, session, queue, selection, cli.json,
            )
            .await?;
        }
        Command::Chat {
            session_id,
            image,
            agent,
            new,
            selection,
        } => {
            let (client, endpoint) = api_client::connect()?;
            api_client::call(&endpoint, client.health()).await?;
            client_conversation::wait_healthy(&client, &endpoint, selection.provider.as_deref())
                .await?;
            if cli.json {
                client_commands::chat(client, session_id, image, agent, new, selection, true)
                    .await?;
            } else {
                #[cfg(feature = "chat")]
                swarmy_chat::client_chat::run(
                    client,
                    session_id,
                    image,
                    agent,
                    new,
                    selection.clone().into(),
                    selection.route,
                )
                .await?;
                #[cfg(not(feature = "chat"))]
                anyhow::bail!(
                    "interactive chat is unavailable in this headless build; use --json or install with --features chat"
                );
            }
        }
        #[cfg(feature = "remote")]
        Command::Remote { command } => remote(command, cli.json).await?,
        #[cfg(not(feature = "remote"))]
        Command::Remote { .. } => unreachable!("remote commands are rejected before dispatch"),
        Command::Dev { .. } => unreachable!("dev commands run without the database network"),
        Command::Agent { .. } | Command::Session { .. } | Command::Cost { .. } => {
            unreachable!()
        }
        Command::Image { command } => image::run(command, cli.json).await?,
        Command::Gc {
            dry_run,
            grace_seconds,
        } => gc::run(dry_run, grace_seconds, cli.json).await?,
        Command::Doctor => {
            if !doctor::run(cli.json).await? {
                std::process::exit(1);
            }
        }
        Command::Auth { .. } => unreachable!("auth commands run through the API"),
        Command::Version => swarmy_version::print("swarmy", cli.json)?,
    }
    Ok(())
}

// `run` and `chat` exist only in this binary: `swarmy-session` shares
// `auth_command` but serves database commands instead, so the session-route
// parse test lives here rather than in the shared module.
fn run_auth_tool(
    command: auth_command::Command,
    file: Option<PathBuf>,
    json: bool,
) -> anyhow::Result<()> {
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
    if let Some(file) = file {
        process.arg("--auth-file").arg(file);
    }
    match command {
        auth_command::Command::Import { file, label } => {
            process.arg("import");
            if let Some(file) = file {
                process.arg("--file").arg(file);
            }
            if let Some(label) = label {
                process.arg("--label").arg(label);
            }
        }
        auth_command::Command::Login {
            provider,
            label,
            resource,
            scope,
        } => {
            process.arg("login").arg(provider);
            if let Some(label) = label {
                process.arg("--label").arg(label);
            }
            if let Some(resource) = resource {
                process.arg("--resource").arg(resource);
            }
            if let Some(scope) = scope {
                process.arg("--scope").arg(scope);
            }
        }
        _ => unreachable!("only interactive auth commands use the helper"),
    }
    let status = process.status().map_err(|error| {
        anyhow::anyhow!("swarmy-auth helper unavailable; run make install-client or cargo install --path crates/swarmy-devtools: {error}")
    })?;
    anyhow::ensure!(status.success(), "swarmy-auth failed: {status}");
    Ok(())
}

/// Run a `swarmy remote` subcommand. The library reports what `remote down`
/// would delete; only the CLI prints the confirmation wording and prompts.
#[cfg(feature = "remote")]
async fn remote(command: swarmy_cloud::Command, json: bool) -> anyhow::Result<()> {
    use anyhow::Context as _;
    let name = remote_name(&command);
    // Keep the deletion target before the command moves into the first run.
    let down = match &command {
        swarmy_cloud::Command::Down {
            name, keep_bucket, ..
        } => Some((name.clone(), *keep_bucket)),
        _ => None,
    };
    match Box::pin(swarmy_cloud::run(command, json))
        .await
        .with_context(|| format!("swarmy remote {name} failed"))?
    {
        swarmy_cloud::RunOutcome::Completed => Ok(()),
        swarmy_cloud::RunOutcome::NeedsConfirmation(plan) => {
            let Some((node, keep_bucket)) = down else {
                anyhow::bail!("remote down confirmation expired; rerun the command");
            };
            confirm_deletion(&plan, json).await?;
            Box::pin(swarmy_cloud::run(
                swarmy_cloud::Command::Down {
                    name: node,
                    keep_bucket,
                    yes: true,
                },
                json,
            ))
            .await
            .with_context(|| format!("swarmy remote {name} failed"))?;
            Ok(())
        }
    }
}

/// The subcommand name for edge error context.
#[cfg(feature = "remote")]
fn remote_name(command: &swarmy_cloud::Command) -> &'static str {
    match command {
        swarmy_cloud::Command::Up { .. } => "up",
        swarmy_cloud::Command::AddNode { .. } => "add-node",
        swarmy_cloud::Command::Upgrade { .. } => "upgrade",
        swarmy_cloud::Command::Down { .. } => "down",
        swarmy_cloud::Command::Tag { .. } => "tag",
        swarmy_cloud::Command::Connect { .. } => "connect",
        swarmy_cloud::Command::Disconnect { .. } => "disconnect",
        swarmy_cloud::Command::Logs { .. } => "logs",
        swarmy_cloud::Command::Status => "status",
    }
}

/// Print what `remote down` would delete and prompt, then rerun confirmed.
/// The rerun cannot need confirmation again: it passes `--yes`.
#[cfg(feature = "remote")]
async fn confirm_deletion(plan: &swarmy_cloud::DeletionPlan, json: bool) -> anyhow::Result<()> {
    use std::io::{IsTerminal, Write};
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
    print!("Continue? [y/N] ");
    std::io::stdout().flush()?;
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer)?;
    if answer.trim() != "y" && answer.trim() != "yes" {
        anyhow::bail!("remote down cancelled");
    }
    Ok(())
}
