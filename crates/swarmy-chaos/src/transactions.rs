//! Store transaction timings from the node's service log, for the latency suite.
use std::path::Path;

use anyhow::{Context, Result, ensure};

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Timing {
    pub caller: String,
    pub attempts: u32,
    pub elapsed_ms: u64,
}

pub(crate) fn parse(line: &str) -> Option<Timing> {
    if !line.contains(" swarmy_store::transaction: ") {
        return None;
    }
    let (mut caller, mut attempts, mut elapsed_ms) = (None, None, None);
    for word in line.split_whitespace() {
        if let Some(value) = word.strip_prefix("caller=") {
            caller = Some(value.to_owned());
        } else if let Some(value) = word.strip_prefix("attempts=") {
            attempts = value.parse().ok();
        } else if let Some(value) = word.strip_prefix("elapsed_ms=") {
            elapsed_ms = value.parse().ok();
        }
    }
    Some(Timing {
        caller: caller?,
        attempts: attempts?,
        elapsed_ms: elapsed_ms?,
    })
}

pub(crate) fn check_node_logs(files: &Path, max_ms: u64) -> Result<()> {
    let mut timings = Vec::new();
    for entry in std::fs::read_dir(files).context("read the service log directory")? {
        let path = entry?.path();
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        if name.starts_with("node-")
            && path
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("log"))
        {
            let log = std::fs::read_to_string(&path)
                .with_context(|| format!("read {}", path.display()))?;
            timings.extend(log.lines().filter_map(parse));
        }
    }
    ensure!(
        !timings.is_empty(),
        "the node logged no store transactions; it must run with swarmy_store::transaction=debug"
    );
    let slowest = timings
        .iter()
        .map(|timing| timing.elapsed_ms)
        .max()
        .unwrap_or(0);
    tracing::info!(
        transactions = timings.len(),
        slowest_ms = slowest,
        max_ms,
        "node transaction timings"
    );
    let slow: Vec<String> = timings
        .iter()
        .filter(|timing| timing.elapsed_ms > max_ms)
        .map(|timing| {
            format!(
                "{} ({} ms, {} attempts)",
                timing.caller, timing.elapsed_ms, timing.attempts
            )
        })
        .collect();
    ensure!(
        slow.is_empty(),
        "{} node store transactions took longer than {max_ms} ms: {}",
        slow.len(),
        slow.join("; ")
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;

    const LINE: &str = "2026-10-31T10:00:00.000000Z DEBUG swarmy_store::transaction: store transaction caller=crates/swarmy-store/src/placed_tools.rs:95:9 attempts=2 elapsed_ms=812 ok=true";

    #[test]
    fn parses_the_store_transaction_line() {
        assert_eq!(
            parse(LINE),
            Some(Timing {
                caller: "crates/swarmy-store/src/placed_tools.rs:95:9".into(),
                attempts: 2,
                elapsed_ms: 812,
            })
        );
    }

    #[test]
    fn parses_warn_lines_inside_spans_and_ignores_other_lines() {
        let warn = "2026-10-31T10:00:00Z  WARN node{id=1}: swarmy_store::transaction: slow store transaction caller=a.rs:1:1 attempts=1 elapsed_ms=1500 ok=false";
        assert_eq!(parse(warn).map(|timing| timing.elapsed_ms), Some(1500));
        assert_eq!(
            parse("2026-10-31T10:00:00Z  INFO swarmyd: node registered"),
            None
        );
    }

    #[test]
    fn node_logs_fail_on_a_slow_transaction_and_name_it() {
        let files = tempfile::tempdir().unwrap();
        std::fs::write(files.path().join("node-0.log"), format!("{LINE}\n")).unwrap();
        std::fs::write(files.path().join("worker-0.log"), "elapsed_ms=99999\n").unwrap();
        assert!(check_node_logs(files.path(), 900).is_ok());
        let error = check_node_logs(files.path(), 800).unwrap_err().to_string();
        assert!(error.contains("placed_tools.rs:95:9"), "{error}");
    }

    #[test]
    fn node_logs_without_transaction_lines_fail() {
        let files = tempfile::tempdir().unwrap();
        std::fs::write(files.path().join("node-0.log"), "nothing here\n").unwrap();
        assert!(check_node_logs(files.path(), 800).is_err());
    }
}
