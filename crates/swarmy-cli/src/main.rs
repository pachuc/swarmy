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
mod doctor;
mod gc;
mod image;
mod image_command;
mod local;
mod machines;
mod models;
mod models_probe;
mod models_probe_command;
mod provider_report;
mod selection_command;
mod session_command;
mod swarm_command;
mod swarm_hosts;
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
    /// Install `FoundationDB`, NATS, and `SeaweedFS` for local swarms (Linux x86-64)
    InstallTools {
        /// Install root: tools go to PREFIX/bin and PREFIX/lib (default ~/.local)
        #[arg(long)]
        prefix: Option<PathBuf>,
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

fn json_progress_on_stderr(cli: &Cli) -> bool {
    cli.json && matches!(cli.command, Command::Core { .. } | Command::Swarm { .. })
}

fn main() -> anyhow::Result<()> {
    let cli = swarmy_version::parse::<Cli>("swarmy")?;
    if json_progress_on_stderr(&cli) {
        swarmy_cloud::set_output_sink(|message, _stderr| {
            eprintln!("{message}");
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
    // connects on its own path, except the local swarm supervisor, install-tools,
    // auth helpers, and cloud credentials which never touch the API.
    let runtime = tokio::runtime::Runtime::new()?;
    let json = cli.json;
    match cli.command {
        Command::Swarm { command } => runtime.block_on(swarm_command::run(
            command,
            json,
            cli.allow_version_mismatch,
        )),
        Command::InstallTools { prefix } => {
            if json {
                anyhow::bail!("--json is not supported for install-tools; run without it");
            }
            runtime.block_on(tools::install(prefix.as_deref()))
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
