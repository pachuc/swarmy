//! Detect the NVIDIA cards this node offers to GPU sandboxes.
use anyhow::{Result, bail};
use std::{io::ErrorKind, process::Stdio, time::Duration};
use swarmy_core::GpuCards;
use tokio::process::Command;

pub(crate) const QUERY: [&str; 2] = [
    "--query-gpu=name,memory.total",
    "--format=csv,noheader,nounits",
];

/// The production card query.
pub(crate) fn nvidia_smi() -> Command {
    let mut query = Command::new("nvidia-smi");
    query.args(QUERY);
    query
}

/// The fake wins without running the query. Otherwise any failure means no cards.
pub(crate) async fn detect(fake: Option<&GpuCards>, query: Command) -> Option<GpuCards> {
    if let Some(fake) = fake {
        return Some(fake.clone());
    }
    let stdout = query_output(query).await?;
    let cards = match std::str::from_utf8(&stdout)
        .map_err(anyhow::Error::from)
        .and_then(parse_cards)
    {
        Ok(cards) => cards,
        Err(error) => {
            tracing::warn!(error = %swarmy_core::error_chain(&*error), "GPU query output is malformed");
            return None;
        }
    };
    if let Some(cards) = &cards {
        tracing::info!(model = %cards.model, memory_mib = cards.memory_mib, count = cards.count, "GPU cards detected");
    }
    cards
}

/// Run the bounded command and report why a node cannot advertise cards.
async fn query_output(mut query: Command) -> Option<Vec<u8>> {
    query.stdin(Stdio::null()).kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_secs(10), query.output())
        .await
        .map_err(|_| QueryFailure::Timeout)
        .and_then(|result| result.map_err(QueryFailure::Spawn));
    let output = match output {
        Ok(output) => output,
        Err(error) => {
            report_query_failure(&error);
            return None;
        }
    };
    if !output.status.success() {
        tracing::warn!(status = %output.status, stderr = %String::from_utf8_lossy(&output.stderr).trim(), "GPU query failed");
        return None;
    }
    Some(output.stdout)
}

enum QueryFailure {
    Timeout,
    Spawn(std::io::Error),
}

fn report_query_failure(failure: &QueryFailure) {
    match failure {
        QueryFailure::Timeout => tracing::warn!("GPU query timed out"),
        QueryFailure::Spawn(error) if error.kind() == ErrorKind::NotFound => {
            tracing::debug!("nvidia-smi not installed; no GPU cards");
        }
        QueryFailure::Spawn(error) => {
            tracing::warn!(error = %swarmy_core::error_chain(error), "GPU query failed to start");
        }
    }
}

/// One card per line, "NAME, `MEMORY_MIB`". Blank output means no cards.
pub(crate) fn parse_cards(stdout: &str) -> Result<Option<GpuCards>> {
    let mut first: Option<GpuCards> = None;
    let mut ignored = 0_u32;
    for line in stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let Some((model, memory)) = line.rsplit_once(',') else {
            bail!("malformed nvidia-smi line {line:?}");
        };
        let model = model.trim();
        let Ok(memory_mib) = memory.trim().parse::<u64>() else {
            bail!("malformed nvidia-smi line {line:?}");
        };
        if model.is_empty() {
            bail!("malformed nvidia-smi line {line:?}");
        }
        match &mut first {
            None => {
                first = Some(GpuCards {
                    model: model.into(),
                    memory_mib,
                    count: 1,
                });
            }
            Some(card) if card.model == model && card.memory_mib == memory_mib => card.count += 1,
            Some(_) => ignored += 1,
        }
    }
    if ignored > 0 {
        tracing::warn!(
            ignored,
            "node has mixed GPU cards; advertising only those matching the first"
        );
    }
    Ok(first)
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    fn l4(count: u32) -> GpuCards {
        GpuCards {
            model: "NVIDIA L4".into(),
            memory_mib: 23034,
            count,
        }
    }
    #[test]
    fn parse_cards_reads_one_card() {
        assert_eq!(parse_cards("NVIDIA L4, 23034\n").unwrap(), Some(l4(1)));
    }
    #[test]
    fn parse_cards_counts_identical_cards() {
        assert_eq!(
            parse_cards("NVIDIA L4, 23034\nNVIDIA L4, 23034\n").unwrap(),
            Some(l4(2))
        );
    }
    #[test]
    fn parse_cards_mixed_cards_advertise_the_first_kind() {
        assert_eq!(
            parse_cards("NVIDIA L4, 23034\nA100, 40000\n").unwrap(),
            Some(l4(1))
        );
    }
    #[test]
    fn parse_cards_blank_output_is_no_cards() {
        assert_eq!(parse_cards("").unwrap(), None);
        assert_eq!(parse_cards("\n \n").unwrap(), None);
    }
    #[test]
    fn parse_cards_rejects_malformed_lines() {
        for line in ["NVIDIA L4, [N/A]", "garbage", ", 100"] {
            assert!(parse_cards(line).is_err());
        }
    }
    #[tokio::test]
    async fn detect_missing_binary_is_no_cards() {
        assert_eq!(
            detect(None, Command::new("/nonexistent/nvidia-smi")).await,
            None
        );
    }
    #[tokio::test]
    async fn detect_failing_query_is_no_cards() {
        let mut q = Command::new("sh");
        q.args(["-c", "echo 'NVIDIA-SMI has failed' >&2; exit 9"]);
        assert_eq!(detect(None, q).await, None);
    }
    #[tokio::test]
    async fn detect_reads_query_output() {
        let mut q = Command::new("sh");
        q.args(["-c", "printf 'NVIDIA L4, 23034\n'"]);
        assert_eq!(detect(None, q).await, Some(l4(1)));
    }
    #[tokio::test]
    async fn detect_fake_wins_without_running_the_query() {
        let fake = GpuCards {
            model: "Fake L4".into(),
            memory_mib: 1000,
            count: 2,
        };
        let mut q = Command::new("sh");
        q.args(["-c", "printf 'NVIDIA L4, 23034\n'"]);
        assert_eq!(detect(Some(&fake), q).await, Some(fake));
    }
}
