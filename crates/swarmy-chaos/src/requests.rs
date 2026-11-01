//! Node API request timings from the node's service log, for the latency suite.
use std::path::Path;

use anyhow::{Context, Result, ensure};

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Timing {
    pub path: String,
    pub attempts: u32,
    pub elapsed_ms: u64,
    pub outcome: String,
}

/// One `node request` line written by `NodeClient`, or `None`.
pub(crate) fn parse(line: &str) -> Option<Timing> {
    let (_, message) = line.split_once(" swarmy_node_protocol::client: ")?;
    let fields = message.strip_prefix("node request ")?;
    let (mut path, mut attempts, mut elapsed_ms, mut outcome) = (None, None, None, None);
    for word in fields.split_whitespace() {
        if let Some(value) = word.strip_prefix("path=") {
            path = Some(value.trim_matches('"').to_owned());
        } else if let Some(value) = word.strip_prefix("attempts=") {
            attempts = value.parse().ok();
        } else if let Some(value) = word.strip_prefix("elapsed_ms=") {
            elapsed_ms = value.parse().ok();
        } else if let Some(value) = word.strip_prefix("outcome=") {
            outcome = Some(value.trim_matches('"').to_owned());
        }
    }
    Some(Timing {
        path: path?,
        attempts: attempts?,
        elapsed_ms: elapsed_ms?,
        outcome: outcome?,
    })
}

/// Read node logs, require tool requests, and reject slow non-registration requests.
///
/// # Errors
/// Unreadable logs, missing tool requests, or slow requests.
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
    for action in ["claim", "complete"] {
        let path = format!("/node/v1/tool/{action}");
        ensure!(
            timings.iter().any(|timing| timing.path == path),
            "the node made no tool/{action} request; the node log has no `node request` line for {path}"
        );
    }
    let measured = timings
        .iter()
        .filter(|timing| timing.path != "/node/v1/register")
        .collect::<Vec<_>>();
    let slowest = measured
        .iter()
        .map(|timing| timing.elapsed_ms)
        .max()
        .unwrap_or(0);
    tracing::info!(
        requests = timings.len(),
        slowest_ms = slowest,
        max_ms,
        "node request timings"
    );
    let slow = measured
        .into_iter()
        .filter(|timing| timing.elapsed_ms > max_ms)
        .map(|timing| {
            format!(
                "{} ({} ms, {} attempts, {})",
                timing.path, timing.elapsed_ms, timing.attempts, timing.outcome
            )
        })
        .collect::<Vec<_>>();
    ensure!(
        slow.is_empty(),
        "{} node API requests took longer than {max_ms} ms: {}",
        slow.len(),
        slow.join(", ")
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;

    const LINE: &str = "2026-11-01T10:00:00.123456Z  INFO swarmy_node_protocol::client: node request path=\"/node/v1/tool/claim\" attempts=1 elapsed_ms=183 outcome=\"ok\"";
    fn line(path: &str, elapsed: u64) -> String {
        LINE.replace("/node/v1/tool/claim", path)
            .replace("elapsed_ms=183", &format!("elapsed_ms={elapsed}"))
    }

    #[test]
    fn parses_the_node_request_line() {
        assert_eq!(
            parse(LINE),
            Some(Timing {
                path: "/node/v1/tool/claim".into(),
                attempts: 1,
                elapsed_ms: 183,
                outcome: "ok".into()
            })
        );
    }

    #[test]
    fn ignores_other_lines() {
        assert_eq!(
            parse(
                "2026-11-01T10:00:00Z DEBUG swarmy_store::transaction: store transaction attempts=1 elapsed_ms=183"
            ),
            None
        );
        assert_eq!(
            parse(&LINE.replace("swarmy_node_protocol::client", "swarmyd")),
            None
        );
        assert_eq!(parse(&LINE.replace("elapsed_ms=183 ", "")), None);
    }

    #[test]
    fn fails_on_a_slow_request_and_names_it() {
        let files = tempfile::tempdir().unwrap();
        std::fs::write(
            files.path().join("node-0.log"),
            format!(
                "{LINE}\n{}\n{}\n",
                line("/node/v1/tool/complete", 120),
                line("/node/v1/sync", 900)
            ),
        )
        .unwrap();
        std::fs::write(
            files.path().join("worker-0.log"),
            line("/node/v1/sync", 99_999),
        )
        .unwrap();
        assert!(check_node_logs(files.path(), 900).is_ok());
        let error = check_node_logs(files.path(), 800).unwrap_err().to_string();
        assert!(error.contains("/node/v1/sync (900 ms"), "{error}");
    }

    #[test]
    fn register_is_not_timed() {
        let files = tempfile::tempdir().unwrap();
        std::fs::write(
            files.path().join("node-0.log"),
            format!(
                "{}\n{LINE}\n{}\n",
                line("/node/v1/register", 30_000),
                line("/node/v1/tool/complete", 120)
            ),
        )
        .unwrap();
        assert!(check_node_logs(files.path(), 500).is_ok());
    }

    #[test]
    fn requires_claim_and_complete() {
        let files = tempfile::tempdir().unwrap();
        let log = files.path().join("node-0.log");
        std::fs::write(&log, line("/node/v1/sync", 100)).unwrap();
        assert!(
            check_node_logs(files.path(), 500)
                .unwrap_err()
                .to_string()
                .contains("tool/claim")
        );
        std::fs::write(&log, LINE).unwrap();
        assert!(
            check_node_logs(files.path(), 500)
                .unwrap_err()
                .to_string()
                .contains("tool/complete")
        );
    }
}
