//! Shared build identity and version output for Swarmy executables.

use std::io::{self, Write};

use clap::Parser;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const GIT_COMMIT: &str = env!("SWARMY_GIT_COMMIT");
pub const IDENTITY: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("SWARMY_GIT_COMMIT"),
    ")"
);

/// Print the same identity in human and machine readable CLI output.
///
/// # Errors
/// Returns an error if stdout cannot be written.
pub fn print(binary: &str, json: bool) -> io::Result<()> {
    let mut output = io::stdout().lock();
    if json {
        writeln!(
            output,
            "{}",
            serde_json::json!({
                "binary": binary, "version": VERSION, "git_commit": GIT_COMMIT,
                "identity": IDENTITY,
            })
        )
    } else {
        writeln!(output, "{binary} {IDENTITY}")
    }
}

/// Parse arguments, handling standalone version queries before service startup.
///
/// # Errors
/// Returns an error if version output cannot be written.
pub fn parse<P: Parser>(binary: &'static str) -> io::Result<P> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    // Clap's built-in version action exits as soon as it sees --version, before
    // it can see a following --json. Only intercept standalone queries so a
    // prompt or an argument after -- is never mistaken for a version request.
    if !args.is_empty()
        && args
            .iter()
            .all(|arg| arg == "--version" || arg == "-V" || arg == "--json")
        && args.iter().any(|arg| arg == "--version" || arg == "-V")
    {
        print(binary, args.iter().any(|arg| arg == "--json"))?;
        std::process::exit(0);
    }
    let matches = P::command().name(binary).version(IDENTITY).get_matches();
    Ok(P::from_arg_matches(&matches).unwrap_or_else(|error| error.exit()))
}

/// Services take configuration from files and the environment.
#[derive(Parser)]
pub struct ServiceArgs {}
