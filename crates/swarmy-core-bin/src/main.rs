//! Dispatch one executable to the selected service role.
use anyhow::{Result, bail};
use std::ffi::OsString;
mod vol;

// CLI output belongs to the executable, not the volume command module.
fn print_volume_output(value: impl std::fmt::Display) {
    println!("{value}");
}

fn main() -> Result<()> {
    let mut args = std::env::args_os().skip(1);
    let role = args.next();
    let rest: Vec<OsString> = args.collect();
    match role.as_deref().and_then(std::ffi::OsStr::to_str) {
        Some("scheduler") => swarmy_scheduler::role::run(rest),
        Some("worker") => swarmy_worker::role::run(rest),
        Some("gateway") => Ok(swarmy_gateway::role::run(rest)?),
        Some("api") => Ok(swarmy_api::role::run(rest)?),
        Some("node-api") => Ok(swarmy_node_api::role::run(rest)?),
        Some("node-credential") => Ok(swarmy_node_api::role::credential(rest)?),
        Some("vol") => vol::run(rest),
        Some(role) if role.starts_with('-') => {
            swarmy_version::parse_from::<swarmy_version::ServiceArgs>(
                "swarmy-core",
                std::iter::once(OsString::from(role)).chain(rest).collect(),
            )?;
            Ok(())
        }
        None => bail!(
            "usage: swarmy-core <ROLE> [ARGS]...; roles: scheduler, worker, gateway, api, node-api, vol; commands: node-credential"
        ),
        Some(role) => bail!(
            "unknown role {role:?}; expected one of: scheduler, worker, gateway, api, node-api, vol, node-credential"
        ),
    }
}
