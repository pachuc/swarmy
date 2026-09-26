mod agent_command;
mod api_client;
mod api_commands;
mod auth;
mod auth_command;
mod bench_command;
mod client_bench;
mod client_chat;
mod client_commands;
mod client_conversation;
mod cost_command;
mod dev;
mod doctor;
mod gc;
mod image;
mod image_command;
mod input;
mod models;
mod models_probe;
mod models_probe_command;
mod provider_report;
mod provider_runtime;
#[cfg(feature = "remote")]
mod remote;
mod remote_command;
#[cfg(not(feature = "remote"))]
#[allow(dead_code)] // The node CLI only uses tunnel health checks from this shared module.
#[path = "remote/ssh.rs"]
mod remote_ssh;
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
        command: remote_command::Command,
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
    remote_command::select(cli.remote.as_deref())?;
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
        return tokio::runtime::Runtime::new()?.block_on(auth::run(command, auth_file, cli.json));
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
            selection,
        } => {
            let (client, endpoint) = api_client::connect()?;
            api_client::call(&endpoint, client.health()).await?;
            client_commands::run(
                client, prompt, image, agent, new, session, selection, cli.json,
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
                client_chat::run(client, session_id, image, agent, new, selection).await?;
            }
        }
        #[cfg(feature = "remote")]
        Command::Remote { command } => Box::pin(remote::run(command, cli.json)).await?,
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
#[cfg(test)]
mod session_route_tests {
    use clap::Parser;

    #[test]
    fn run_and_chat_accept_a_session_route() {
        assert!(crate::Cli::try_parse_from(["swarmy", "run", "--route", "fallback", "hi"]).is_ok());
        assert!(crate::Cli::try_parse_from(["swarmy", "chat", "--route", "fallback"]).is_ok());
        // The route overrides one session only, so it stays available with an agent.
        assert!(
            crate::Cli::try_parse_from([
                "swarmy", "chat", "--agent", "tommy", "--route", "fallback"
            ])
            .is_ok()
        );
        assert!(
            crate::Cli::try_parse_from([
                "swarmy",
                "run",
                "--agent",
                "tommy",
                "--provider",
                "openai",
                "--route",
                "fallback",
                "hi",
            ])
            .is_err()
        );
    }
}
