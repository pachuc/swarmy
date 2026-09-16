use jiff::Timestamp;

use crate::PlacementChangeReason;

/// Explain the durable restore point without implying that prior tool writes survived.
#[must_use]
pub fn computer_rebuilt_message(
    reason: PlacementChangeReason,
    snapshot: Timestamp,
    changed_at: Timestamp,
) -> Option<String> {
    let seconds = changed_at.duration_since(snapshot).as_secs().max(0);
    match reason {
        PlacementChangeReason::Initial => None,
        PlacementChangeReason::Failure => Some(format!(
            "Your computer was rebuilt from the snapshot at {snapshot}, which was {seconds} seconds before the failure. Running processes and file changes after that snapshot were lost. The interrupted tool call failed; check external side effects before retrying."
        )),
        PlacementChangeReason::Eviction => Some(format!(
            "Your computer was evicted while idle and rebuilt from its final checkpoint at {snapshot}, which was {seconds} seconds before the rebuild. Running processes and file changes after that checkpoint were lost. Any interrupted tool call failed; check external side effects before retrying."
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_wording() {
        assert_eq!(
            computer_rebuilt_message(
                PlacementChangeReason::Failure,
                "2026-09-16T10:00:00Z".parse().unwrap(),
                "2026-09-16T10:02:03Z".parse().unwrap(),
            )
            .unwrap(),
            "Your computer was rebuilt from the snapshot at 2026-09-16T10:00:00Z, which was 123 seconds before the failure. Running processes and file changes after that snapshot were lost. The interrupted tool call failed; check external side effects before retrying."
        );
    }

    #[test]
    fn eviction_wording() {
        assert_eq!(
            computer_rebuilt_message(
                PlacementChangeReason::Eviction,
                "2026-09-16T10:00:00Z".parse().unwrap(),
                "2026-09-16T10:00:02Z".parse().unwrap(),
            )
            .unwrap(),
            "Your computer was evicted while idle and rebuilt from its final checkpoint at 2026-09-16T10:00:00Z, which was 2 seconds before the rebuild. Running processes and file changes after that checkpoint were lost. Any interrupted tool call failed; check external side effects before retrying."
        );
    }

    #[test]
    fn initial_grant_has_no_notice_and_clock_skew_cannot_report_negative_loss() {
        let snapshot = "2026-09-16T10:00:01Z".parse().unwrap();
        let changed = "2026-09-16T10:00:00Z".parse().unwrap();
        assert_eq!(
            computer_rebuilt_message(PlacementChangeReason::Initial, snapshot, changed),
            None
        );
        assert!(
            computer_rebuilt_message(PlacementChangeReason::Failure, snapshot, changed)
                .unwrap()
                .contains("0 seconds")
        );
    }
}
