use jiff::Timestamp;

use crate::PlacementChangeReason;

/// Explain the durable restore point without implying that prior tool writes survived.
#[must_use]
pub fn computer_rebuilt_message(
    reason: PlacementChangeReason,
    snapshot: Timestamp,
    recovery_started_at: Timestamp,
    estimated_failure_at: Option<Timestamp>,
) -> Option<String> {
    let seconds = recovery_started_at
        .duration_since(snapshot)
        .as_secs()
        .max(0);
    match reason {
        PlacementChangeReason::Initial | PlacementChangeReason::Unstarted => None,
        PlacementChangeReason::Failure => {
            let estimate = estimated_failure_at.map_or_else(String::new, |time| {
                format!(" The estimated failure time is {time}, based on the lost computer's last successful placement claim or lease renewal; the actual failure time is unknown.")
            });
            Some(format!(
                "Your computer recovery began at {recovery_started_at} from the snapshot at {snapshot}, which was {seconds} seconds before recovery.{estimate} Running processes and file changes after that snapshot were lost. The interrupted tool call failed; check external side effects before retrying."
            ))
        }
        PlacementChangeReason::Eviction => Some(format!(
            "Your computer was evicted while idle. Recovery began at {recovery_started_at} from its final checkpoint at {snapshot}, which was {seconds} seconds before recovery. Running processes and file changes after that checkpoint were lost. Any interrupted tool call failed; check external side effects before retrying."
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
                None,
            )
            .unwrap(),
            "Your computer recovery began at 2026-09-16T10:02:03Z from the snapshot at 2026-09-16T10:00:00Z, which was 123 seconds before recovery. Running processes and file changes after that snapshot were lost. The interrupted tool call failed; check external side effects before retrying."
        );
    }

    #[test]
    fn failure_estimate_is_distinct_from_recovery_time() {
        let message = computer_rebuilt_message(
            PlacementChangeReason::Failure,
            "2026-09-16T10:00:00Z".parse().unwrap(),
            "2026-09-16T10:07:54Z".parse().unwrap(),
            Some("2026-09-16T10:05:53Z".parse().unwrap()),
        )
        .unwrap();
        assert!(message.contains("474 seconds before recovery"));
        assert!(message.contains("recovery began at 2026-09-16T10:07:54Z"));
        assert!(message.contains("estimated failure time is 2026-09-16T10:05:53Z"));
        assert!(message.contains(
            "last successful placement claim or lease renewal; the actual failure time is unknown"
        ));
        assert!(!message.contains("seconds before the failure"));
    }

    #[test]
    fn unstarted_takeover_has_no_notice() {
        assert_eq!(
            computer_rebuilt_message(
                PlacementChangeReason::Unstarted,
                Timestamp::UNIX_EPOCH,
                Timestamp::now(),
                None,
            ),
            None
        );
    }

    #[test]
    fn eviction_wording() {
        assert_eq!(
            computer_rebuilt_message(
                PlacementChangeReason::Eviction,
                "2026-09-16T10:00:00Z".parse().unwrap(),
                "2026-09-16T10:00:02Z".parse().unwrap(),
                None,
            )
            .unwrap(),
            "Your computer was evicted while idle. Recovery began at 2026-09-16T10:00:02Z from its final checkpoint at 2026-09-16T10:00:00Z, which was 2 seconds before recovery. Running processes and file changes after that checkpoint were lost. Any interrupted tool call failed; check external side effects before retrying."
        );
    }

    #[test]
    fn initial_grant_has_no_notice_and_clock_skew_cannot_report_negative_loss() {
        let snapshot = "2026-09-16T10:00:01Z".parse().unwrap();
        let changed = "2026-09-16T10:00:00Z".parse().unwrap();
        assert_eq!(
            computer_rebuilt_message(PlacementChangeReason::Initial, snapshot, changed, None),
            None
        );
        assert!(
            computer_rebuilt_message(PlacementChangeReason::Failure, snapshot, changed, None)
                .unwrap()
                .contains("0 seconds")
        );
    }
}
